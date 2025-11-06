import time
from google.cloud.discoveryengine_v1 import (
    DocumentServiceClient,
    GcsSource,
    ImportDocumentsRequest,DeleteDocumentRequest
)
from fastapi import UploadFile, HTTPException, status
from typing import Dict,Any
from utils.settings import settings
from schemas.document import IngestResponse
from google.cloud import storage
from services.gcs_service import _get_gcs_bucket, _upload_file_to_gcs
from services.database import save_document_to_db,get_documents_by_engine_id,get_total_document_count,delete_document_from_db,update_task_in_db

import hashlib


def _calculate_document_id_from_gcs_uri(gcs_uri: str) -> str:
    """
    Calculate the document ID that Vertex AI will generate for a GCS URI.
    This matches the algorithm used when data_schema="content".
    
    Args:
        gcs_uri: The GCS URI (e.g., gs://bucket/path/file.pdf)
    
    Returns:
        The document ID as a 32-character hex string
    """
    # Calculate SHA256 hash of the URI
    hash_bytes = hashlib.sha256(gcs_uri.encode('utf-8')).digest()
    
    # Take first 128 bits (16 bytes) and encode as hex
    document_id = hash_bytes[:16].hex()
    
    return document_id

def _ingest_document_from_gcs(
    project_id: str,
    location: str,
    data_store_id: str,
    gcs_uri: str,
    max_retries: int = 3,
    initial_delay: int = 5
) -> dict:
    """
    Ingest a document from GCS into the data store with retry logic.
    
    Args:
        project_id: GCP project ID
        location: Location of the data store
        data_store_id: Data store ID
        gcs_uri: GCS URI of the document
        max_wait_time: Maximum time to wait for import (seconds)
        max_retries: Maximum number of retries
        initial_delay: Initial delay before first import attempt
    
    Returns:
        Dictionary with ingestion results
    """
    client = DocumentServiceClient()
    
    parent_path = client.branch_path(
        project=project_id,
        location=location,
        data_store=data_store_id,
        branch="default_branch",
    )
    
    for attempt in range(max_retries):
        try:
            # Add delay before import to ensure file is ready
            wait_time = initial_delay if attempt == 0 else initial_delay * (attempt + 1)
            print(f"Waiting {wait_time}s before import to ensure file is ready...")
            time.sleep(wait_time)
            
            print(f"Starting document import from {gcs_uri}... (Attempt {attempt + 1}/{max_retries})")
            
            gcs_source = GcsSource(
                input_uris=[gcs_uri],
                data_schema="content"
            )
            
            request = ImportDocumentsRequest(
                parent=parent_path,
                gcs_source=gcs_source,
                reconciliation_mode=ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
            )
            
            operation = client.import_documents(request=request)
            
            print(f"Waiting for import to complete ...")
            response = operation.result()
            
            # Get metadata
            metadata = operation.metadata
            success_count = getattr(metadata, 'success_count', 0)
            failure_count = getattr(metadata, 'failure_count', 0)
            
            print(f"Import complete: {success_count} success, {failure_count} failed")
            
            # Check for failures
            if failure_count > 0:
                error_samples = getattr(metadata, 'error_samples', [])
                error_messages = [str(err) for err in error_samples[:3]]
                raise RuntimeError(f"Import had {failure_count} failures. Errors: {error_messages}")
            
            if success_count == 0:
                raise RuntimeError("Import completed but no documents were successfully imported")
            
            # Wait for indexing
            indexing_wait = 30
            print(f"Waiting {indexing_wait} seconds for document indexing...")
            time.sleep(indexing_wait)
            
            return {
                "success_count": success_count,
                "failure_count": failure_count,
                "operation_name": operation.operation.name if hasattr(operation, 'operation') else None
            }
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # Check if it's a retryable error
            is_retryable = any(keyword in error_msg for keyword in [
                "not found", "404", "unavailable", "deadline", "timeout", 
                "503", "500", "does not exist", "no such object"
            ])
            
            if is_retryable and attempt < max_retries - 1:
                retry_wait = initial_delay * (attempt + 2)  # Exponential backoff
                print(f"⚠ Import attempt {attempt + 1} failed: {e}")
                print(f"   Retrying in {retry_wait}s...")
                time.sleep(retry_wait)
            else:
                raise RuntimeError(f"Document ingestion failed after {attempt + 1} attempts: {e}")
    
    raise RuntimeError(f"Failed to import document after {max_retries} attempts")


def ingestion(task_id: str,file: UploadFile, engine_id: str, data_store_id: str):
    """
    Complete document ingestion workflow with improved error handling.
    """
    print(f"\n{'='*80}")
    print(f"DOCUMENT INGESTION STARTED")
    print(f"Engine: {engine_id}")
    print(f"Data Store: {data_store_id}")
    print(f"File: {file.filename}")
    print(f"{'='*80}\n")
    
    # Step 1: Get or create GCS bucket
    try:
        
        bucket_name_to_find = f"{engine_id}-{data_store_id}".lower().replace("_", "-")[:63]
        bucket_name = _get_gcs_bucket(
            project_id=settings.PROJECT_ID,
            bucket_name=bucket_name_to_find
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to access GCS bucket: {str(e)}"
        )
    
    # Step 2: Read and upload file to GCS
    try:
        file.file.seek(0)  # Reset file pointer
        file_content = file.file.read()
        file_size = len(file_content)
        
        if file_size == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File is empty"
            )
        
        print(f"File size: {file_size:,} bytes")
        
        gcs_uri = _upload_file_to_gcs(
            project_id=settings.PROJECT_ID,
            bucket_name=bucket_name,
            file_content=file_content,
            filename=file.filename,
            max_retries=1,
            retry_delay=2
        )
        document_id = _calculate_document_id_from_gcs_uri(gcs_uri=gcs_uri)
        update_task_in_db(task_id,document_id, status="processing")
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload file to GCS: {str(e)}"
        )
    
    # Step 3: Ingest document into data store with retry logic
    try:
        
        ingest_result = _ingest_document_from_gcs(
            project_id=settings.PROJECT_ID,
            location=settings.LOCATION,
            data_store_id=data_store_id,
            gcs_uri=gcs_uri,
            max_retries=1,
            initial_delay=10
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to ingest document: {str(e)}"
        )
    
    # Step 4: Save to database
    try:
        
        save_document_to_db(
            document_id=document_id,
            engine_id=engine_id,
            data_store_id=data_store_id,
            filename=file.filename,
            gcs_uri=gcs_uri,
            file_size=file_size,
            content_type=file.content_type or "application/octet-stream"
        )
        print(f"✓ Document saved to database (ID: {document_id})")
        success_message = f"Successfully ingested document. GCS URI: {gcs_uri}"
        update_task_in_db(task_id,document_id,status="completed", result=success_message)
        
        
    except Exception as e:
        print(f"⚠️ Failed to save document to database: {e}")
        error_message = f"An error occurred: {str(e)}"
        update_task_in_db(task_id, document_id,status="failed", error=error_message)
        # Don't fail the entire operation if DB save fails
        document_id = None
    
    # Build response
    response = IngestResponse(
        success_count=ingest_result["success_count"],
        failure_count=ingest_result["failure_count"],
        bucket_name=bucket_name,
        gcs_uri=gcs_uri,
        operation_name=ingest_result.get("operation_name"),
        document_id=document_id,
        message=(
            f"Document '{file.filename}' ingested successfully. "
            f"Success: {ingest_result['success_count']}, "
            f"Failed: {ingest_result['failure_count']}"
        )
    )
    
    print(f"\n{'='*80}")
    print(f"✓ INGESTION COMPLETE!")
    print(f"{'='*80}\n")
    
    return response

def get_documents_by_engine(
    engine_id: str,
    limit: int = 100,
    offset: int = 0,
    sort_order: str = "desc"
) -> Dict[str, Any]:
    """
    Service function to get documents by engine ID.
    
    Returns:
        Dictionary with documents and metadata
    """
    # Get documents from database
    documents = get_documents_by_engine_id(
        engine_id=engine_id,
        limit=limit,
        offset=offset,
        sort_order=sort_order
    )
    
    # Get total count
    total_count = get_total_document_count(engine_id)
    
    # Get data_store_id from first document if available
    data_store_id = documents[0]["data_store_id"] if documents else None
    
    return {
        "engine_id": engine_id,
        "data_store_id": data_store_id,
        "total_count": total_count,
        "returned_count": len(documents),
        "documents": documents
    }



def delete_document_logic(
    document_id: str,
    engine_id: str,
    data_store_id: str, # We need this to build the resource name
    gcs_uri: str,
    filename: str,
    # delete_from_datastore: bool = True,
    # delete_from_gcs: bool = True
) -> Dict[str, Any]:
    """
    Deletes a document from the Vertex AI Search Data Store, GCS, and the local database.

    Args:
        document_id: The UUID of the document.
        engine_id: The engine ID for scoping.
        data_store_id: The Vertex AI Search data store ID.
        gcs_uri: The GCS path to the document file.
        filename: The original name of the file.
        delete_from_datastore: If True, deletes from the search index.
        delete_from_gcs: If True, deletes the file from the GCS bucket.

    Returns:
        A dictionary summarizing the deletion status.
    """
    datastore_deleted = False
    gcs_deleted = False

    # 1. Delete from Vertex AI Search Data Store
    
    try:
        client = DocumentServiceClient()
        
        # Construct the full resource name of the document
        document_name = client.document_path(
            project=settings.PROJECT_ID,
            location=settings.LOCATION,  # e.g., "global" or "us"
            data_store=data_store_id,
            branch="default_branch", # Or '0'
            document=document_id,
        )

        request = DeleteDocumentRequest(name=document_name)
        client.delete_document(request=request)
        
        datastore_deleted = True
        print(f"Document '{document_id}' deleted from data store '{data_store_id}'.")
        
    except Exception as e:
        # This could be a google.api_core.exceptions.NotFound error if it's already gone
        print(f"Failed to delete document from Vertex AI Search Data Store: {str(e)}")
        # You might want to re-raise or handle this more gracefully
        # For now, we'll log it and continue to delete from GCS/DB

    # 2. Delete from GCS
    if  gcs_uri:
        try:
            if gcs_uri.startswith("gs://"):
                storage_client = storage.Client()
                # The .from_string() method is robust for parsing gs:// URIs
                blob = storage.Blob.from_string(gcs_uri, client=storage_client)
                blob.delete()
                
                gcs_deleted = True
                print(f"Deleted from GCS: {gcs_uri}")
        except Exception as e:
            print(f"Failed to delete from GCS: {str(e)}")

    # 3. Delete from local database
    # This should always be last
    db_deleted = delete_document_from_db(document_id, engine_id)
    
    return {
        "document_id": document_id,
        "filename": filename,
        "database_deleted": db_deleted,
        "gcs_deleted": gcs_deleted,
        "datastore_deleted": datastore_deleted,
        "message": "Deletion process completed."
    }

