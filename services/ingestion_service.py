import time
from google.cloud.discoveryengine_v1 import (
    DocumentServiceClient,
    GcsSource,
    ImportDocumentsRequest
)
from fastapi import UploadFile, HTTPException, status
from utils.settings import settings
from schemas.document import IngestResponse
from services.gcs_service import _get_or_create_bucket, _upload_file_to_gcs


def _ingest_document_from_gcs(
    project_id: str,
    location: str,
    data_store_id: str,
    gcs_uri: str
) -> dict:
    """
    Ingest a document from GCS into the data store.
    
    Args:
        project_id: GCP project ID
        location: Location of the data store
        data_store_id: Data store ID
        gcs_uri: GCS URI of the document
    
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
    
    gcs_source = GcsSource(
        input_uris=[gcs_uri],
        data_schema="content"
    )
    
    request = ImportDocumentsRequest(
        parent=parent_path,
        gcs_source=gcs_source,
        reconciliation_mode=ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
    )
    
    try:
        print(f"Starting document import from {gcs_uri}...")
        operation = client.import_documents(request=request)
        
        print("Waiting for import to complete (up to 5 minutes)...")
        response = operation.result(timeout=300)
        
        metadata = operation.metadata
        
        print(f"Import complete: {metadata.success_count} success, {metadata.failure_count} failed")
        
        # Wait for indexing
        print("Waiting 30 seconds for document indexing...")
        time.sleep(30)
        
        return {
            "success_count": metadata.success_count,
            "failure_count": metadata.failure_count,
            "operation_name": operation.operation.name if metadata.failure_count > 0 else None
        }
        
    except Exception as e:
        raise RuntimeError(f"Document ingestion failed: {e}")
    

    
async def ingestion(file: UploadFile, engine_id: str, data_store_id: str):
        
        # Step 1: Get or create GCS bucket
    try:
        bucket_name = _get_or_create_bucket(
            engine_id=engine_id,
            data_store_id=data_store_id,
            project_id=settings.PROJECT_ID,
            location=settings.LOCATION
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create/access GCS bucket: {str(e)}"
        )
    
    # Step 2: Read and upload file to GCS
    try:
        file_content = await file.read()
        
        if len(file_content) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File is empty"
            )
        
        gcs_uri = _upload_file_to_gcs(
            project_id=settings.PROJECT_ID,
            bucket_name=bucket_name,
            file_content=file_content,
            filename=file.filename
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload file to GCS: {str(e)}"
            )
        
        # Step 3: Ingest document into data store
    try:
        ingest_result = _ingest_document_from_gcs(
            project_id=settings.PROJECT_ID,
            location=settings.LOCATION,
            data_store_id=data_store_id,
            gcs_uri=gcs_uri
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to ingest document: {str(e)}"
        )
    
    # Build response
    response = IngestResponse(
        success_count=ingest_result["success_count"],
        failure_count=ingest_result["failure_count"],
        bucket_name=bucket_name,
        gcs_uri=gcs_uri,
        operation_name=ingest_result.get("operation_name"),
        message=(
            f"Document '{file.filename}' ingested successfully. "
            f"Success: {ingest_result['success_count']}, "
            f"Failed: {ingest_result['failure_count']}"
        )
    )
    
    print(f"\n✅ Ingestion complete!")
    return response