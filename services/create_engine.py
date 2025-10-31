import uuid
from fastapi import status,HTTPException
from google.api_core.exceptions import AlreadyExists,NotFound
from services.datastore_service import _create_data_store
from services.database import get_engine_from_db,init_database,save_engine_to_db
from google.cloud.discoveryengine_v1 import (
    Engine, 
    EngineServiceClient)
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