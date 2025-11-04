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
from services.gcs_service import _get_or_create_bucket, _upload_file_to_gcs
from services.database import save_document_to_db,get_documents_by_engine_id,get_total_document_count,delete_document_from_db,get_document_by_id

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
        file_size=len(file_content)
        
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
    try:
        document_id=_calculate_document_id_from_gcs_uri( gcs_uri=gcs_uri)
        save_document_to_db(
            document_id=document_id,
            engine_id=engine_id,
            data_store_id=data_store_id,
            filename=file.filename,
            gcs_uri=gcs_uri,
            file_size=file_size,
            content_type=file.content_type or "application/octet-stream"
        )
    except Exception as e:
        print(f"⚠️  Failed to save document to database: {e}")
    
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
    
    print(f"\n Ingestion complete!")
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

