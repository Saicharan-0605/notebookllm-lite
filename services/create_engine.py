import uuid
from fastapi import status,HTTPException
from google.api_core.exceptions import AlreadyExists,NotFound
from services.datastore_service import _create_data_store
from services.gcs_service import _create_gcs_bucket,_delete_gcs_bucket_and_files
from services.database import get_engine_from_db,init_database,save_engine_to_db,delete_documents_by_engine,delete_engine_from_db,get_other_engines_using_datastore
from google.cloud.discoveryengine_v1 import (
    Engine, 
    EngineServiceClient,DataStoreServiceClient)
from utils.settings import settings




def _create_enterprise_engine_logic(
    engine_name: str
) -> dict:
    """
    Core logic to create an Enterprise Edition engine with auto-generated data store.
    
    Args:
        project_id: GCP project ID
        location: Location (global, us, eu)
        engine_name: Display name for the engine (used to generate engine_id)
    
    Returns:
        Dictionary with status and result
    """
    try:
        engine_client = EngineServiceClient()
    except Exception as e:
        raise RuntimeError(f"Failed to create EngineServiceClient. Error: {e}")

    # Generate unique IDs
    # Engine ID: sanitized engine name + short UUID
    engine_id_base = engine_name.lower().replace(" ", "-").replace("_", "-")
    engine_id_base = ''.join(c for c in engine_id_base if c.isalnum() or c == '-')
    engine_id = f"{engine_id_base}-{str(uuid.uuid4())[:8]}"
    
    # Data store ID: UUID-based for uniqueness
    data_store_id = f"ds-{str(uuid.uuid4())}"
    parent = f"projects/{settings.PROJECT_ID}/locations/{settings.LOCATION}/collections/default_collection"
    engine_full_name = f"{parent}/engines/{engine_id}"

    print(f"Generated Engine ID: {engine_id}")
    print(f"Generated Data Store ID: {data_store_id}")

    # Step 1: Create or get data store
    try:
        data_store_result = _create_data_store(settings.PROJECT_ID, settings.LOCATION, data_store_id)
        data_store_status = data_store_result["status"]
        actual_data_store_id = data_store_result["data_store_id"]
    except Exception as e:
        raise RuntimeError(f"Data store creation failed: {e}")

    # Step 2: Create engine linked to the data store
    engine = Engine(
        display_name=engine_name,
        solution_type="SOLUTION_TYPE_SEARCH",
        data_store_ids=[actual_data_store_id],
        search_engine_config=Engine.SearchEngineConfig(
            search_tier="SEARCH_TIER_ENTERPRISE",
            search_add_ons=["SEARCH_ADD_ON_LLM"],
        ),
        industry_vertical="GENERIC",
    )

    request = {
        "parent": parent, 
        "engine": engine, 
        "engine_id": engine_id
    }

    try:
        print(f"Creating engine '{engine_id}' with data store '{actual_data_store_id}'...")
        print("This is a long-running operation and may take 5-10 minutes.")
        
        operation = engine_client.create_engine(request=request)
        response = operation.result(timeout=900)

        bucket_name = f"{engine_id}-{actual_data_store_id}".lower().replace("_", "-")[:63]
        bucket_location = "us" if settings.LOCATION == "global" else settings.LOCATION.lower()
        
        # Call the dedicated create bucket function
        _create_gcs_bucket(
            project_id=settings.PROJECT_ID,
            bucket_name=bucket_name,
            location=bucket_location
        )
        
        db=init_database()

        save_engine_to_db(
            engine_id=engine_id,
            engine_name=engine_name,
            data_store_id=actual_data_store_id,
        )
        
        print(f"Engine '{engine_id}' created successfully!")
        return {
            "status_code": status.HTTP_201_CREATED,
            "result": {
                "engine_id": engine_id,
                "engine_name": response.display_name,
                "data_store_id": actual_data_store_id,
                "solution_type": response.solution_type.name,
                "message": (
                    f"Engine '{engine_id}' created successfully with "
                    f"{data_store_status} data store '{actual_data_store_id}'."
                )
            }
        }
        
    except AlreadyExists:
        # This should be rare since we use UUID in engine_id
        print(f"Engine '{engine_id}' already exists. Fetching existing engine.")
        try:
            existing_engine = engine_client.get_engine(name=engine_full_name)
            save_engine_to_db(
                engine_id=engine_id,
                engine_name=engine_name,
                data_store_id=actual_data_store_id,
            )
            return {
                "status_code": status.HTTP_200_OK,
                "result": {
                    "engine_id": engine_id,
                    "engine_name": existing_engine.display_name,
                    "data_store_id": actual_data_store_id,
                    "solution_type": existing_engine.solution_type.name,
                    "message": f"Engine '{engine_id}' already exists."
                }
            }
        except NotFound:
            raise RuntimeError(
                f"Engine '{engine_id}' was not found immediately after "
                "an 'AlreadyExists' error."
            )
    except Exception as e:
        print(f"An unexpected error occurred during engine creation: {e}")
        raise


async def get_engines_details(engine_id: str):
    """
    Retrieve details of an existing engine from both database and GCP.
    
    - **engine_id**: The ID of the engine to retrieve
    """
    try:
        # First, check database
        db_engine = get_engine_from_db(engine_id)
        
        if not db_engine:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Engine '{engine_id}' not found in database."
            )
        
        # Then, get details from GCP
        client = EngineServiceClient()
        parent = f"projects/{settings.PROJECT_ID}/locations/{settings.LOCATION}/collections/default_collection"
        engine_name = f"{parent}/engines/{engine_id}"
        
        try:
            engine = client.get_engine(name=engine_name)
            
            return {
                "database_info": db_engine,
                "gcp_info": {
                    "engine_id": engine_id,
                    "engine_name": engine.display_name,
                    "data_store_ids": list(engine.data_store_ids),
                    "solution_type": engine.solution_type.name,
                    "create_time": engine.create_time.isoformat() if engine.create_time else None
                }
            }
        except NotFound:
            # Engine in database but not in GCP (shouldn't happen normally)
            return {
                "database_info": db_engine,
                "gcp_info": None,
                "warning": "Engine found in database but not in GCP"
            }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve engine. Error: {str(e)}"
        )
    
def _delete_engine_logic(
    engine_id: str,
    delete_data_store: bool = True,
    delete_gcs_files: bool = True
) -> dict:
    """
    Core logic to delete an Enterprise Edition engine and its data store.
    
    Args:
        engine_id: The engine ID to delete
        delete_data_store: Whether to also delete the associated data store
        delete_gcs_files: Whether to also delete the GCS files
    
    Returns:
        Dictionary with status and result
    """
    try:
        engine_client = EngineServiceClient()
        data_store_client = DataStoreServiceClient()
    except Exception as e:
        raise RuntimeError(f"Failed to create service clients. Error: {e}")

    parent = f"projects/{settings.PROJECT_ID}/locations/{settings.LOCATION}/collections/default_collection"
    engine_full_name = f"{parent}/engines/{engine_id}"

    print(f"\n{'='*80}")
    print(f"DELETING ENGINE: {engine_id}")
    print(f"Delete Data Store: {delete_data_store}")
    print(f"Delete GCS Files: {delete_gcs_files}")
    print(f"{'='*80}\n")

    engine_deleted = False
    data_store_deleted = False
    gcs_files_deleted = False
    data_store_id = None
    warnings = []

    # Step 1: Check if engine exists in database and get data store ID
    try:
        db_engine = get_engine_from_db(engine_id)
        
        if not db_engine:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Engine '{engine_id}' not found in database."
            )
        
        data_store_id = db_engine['data_store_id']
        engine_name = db_engine['engine_name']
        
        print(f"Found engine in database:")
        print(f"  - Engine ID: {engine_id}")
        print(f"  - Engine Name: {engine_name}")
        print(f"  - Data Store ID: {data_store_id}")
        
    except HTTPException:
        raise
    except Exception as e:
        raise RuntimeError(f"Database query failed: {e}")

    # Step 2: Delete engine from Google Cloud
    try:
        print(f"\nDeleting engine '{engine_id}' from Google Cloud...")
        engine_client.delete_engine(name=engine_full_name)
        engine_deleted = True
        print(f"✓ Engine '{engine_id}' deleted successfully from Google Cloud!")
        
    except NotFound:
        warning_msg = f"Engine '{engine_id}' not found in Google Cloud (may have been deleted manually)"
        print(f"⚠ {warning_msg}")
        warnings.append(warning_msg)
        engine_deleted = True
    except Exception as e:
        raise RuntimeError(f"Failed to delete engine from Google Cloud: {e}")

    # Step 3: Delete GCS files if requested (BEFORE deleting documents from table)
    if delete_gcs_files:
        try:
            # Check if other engines are using this data store
            other_engine_ids = get_other_engines_using_datastore(data_store_id, engine_id)
            
            if other_engine_ids:
                warning_msg = (
                    f"Data store '{data_store_id}' is used by other engines: {other_engine_ids}. "
                    "Skipping GCS file deletion to prevent data loss."
                )
                print(f"⚠ {warning_msg}")
                warnings.append(warning_msg)
            else:
                # Delete GCS files using document table data
                success, warning = _delete_gcs_bucket_and_files(
                    project_id=settings.PROJECT_ID,
                    location=settings.LOCATION,
                    data_store_id=data_store_id,
                    engine_id=engine_id
                )
                gcs_files_deleted = success
                if warning:
                    warnings.append(warning)
                        
        except Exception as e:
            warning_msg = f"Error during GCS file deletion: {str(e)}"
            print(f"⚠ {warning_msg}")
            warnings.append(warning_msg)

    # Step 4: Delete data store if requested
    if delete_data_store and data_store_id:
        try:
            data_store_name = f"{parent}/dataStores/{data_store_id}"
            print(f"\nDeleting data store '{data_store_id}' from Google Cloud...")
            
            # Check again if other engines are using this data store
            other_engine_ids = get_other_engines_using_datastore(data_store_id, engine_id)
            
            if other_engine_ids:
                warning_msg = (
                    f"Data store '{data_store_id}' is used by other engines: {other_engine_ids}. "
                    "Skipping data store deletion to prevent data loss."
                )
                print(f"⚠ {warning_msg}")
                warnings.append(warning_msg)
            else:
                data_store_client.delete_data_store(name=data_store_name)
                data_store_deleted = True
                print(f"✓ Data store '{data_store_id}' deleted successfully from Google Cloud!")
                
        except NotFound:
            warning_msg = f"Data store '{data_store_id}' not found in Google Cloud"
            print(f"⚠ {warning_msg}")
            warnings.append(warning_msg)
            data_store_deleted = True
        except Exception as e:
            warning_msg = f"Failed to delete data store '{data_store_id}': {str(e)}"
            print(f"⚠ {warning_msg}")
            warnings.append(warning_msg)
    elif delete_data_store and not data_store_id:
        warning_msg = "No data store ID found for this engine"
        print(f"⚠ {warning_msg}")
        warnings.append(warning_msg)

    # Step 5: Remove engine from database
    try:
        print(f"\nRemoving engine '{engine_id}' from database...")
        delete_engine_from_db(engine_id)
        print(f"✓ Engine '{engine_id}' removed from database.")
        
    except Exception as e:
        raise RuntimeError(f"Failed to remove engine from database: {e}")

    # Step 6: Clean up documents table (AFTER GCS files are deleted)
    try:
        print(f"\nCleaning up documents for engine '{engine_id}'...")
        deleted_count = delete_documents_by_engine(engine_id)
        print(f"✓ Removed {deleted_count} document records from database.")
    except Exception as e:
        warning_msg = f"Failed to clean up document records: {str(e)}"
        print(f"⚠ {warning_msg}")
        warnings.append(warning_msg)

    # Construct response message
    deletion_parts = []
    if engine_deleted:
        deletion_parts.append("engine")
    if data_store_deleted:
        deletion_parts.append("data store")
    if gcs_files_deleted:
        deletion_parts.append("GCS files")
    
    if deletion_parts:
        message = f"Successfully deleted: {', '.join(deletion_parts)} for engine '{engine_id}'."
    else:
        message = f"Engine '{engine_id}' deletion completed with warnings."

    if warnings:
        message += f" Warnings: {'; '.join(warnings)}"

    print(f"\n{'='*80}")
    print("DELETION COMPLETE")
    print(f"{'='*80}\n")

    return {
        "status_code": status.HTTP_200_OK,
        "result": {
            "status": "success",
            "engine_id": engine_id,
            "data_store_id": data_store_id,
            "engine_deleted": engine_deleted,
            "data_store_deleted": data_store_deleted,
            "gcs_files_deleted": gcs_files_deleted,
            "delete_data_store_requested": delete_data_store,
            "delete_gcs_files_requested": delete_gcs_files,
            "warnings": warnings if warnings else None,
            "message": message
        }
    }