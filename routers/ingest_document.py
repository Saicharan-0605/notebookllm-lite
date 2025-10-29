import os
from typing import Optional
from fastapi import status,APIRouter
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from services.ingestion_service import ingestion
from utils.settings import settings

from schemas.document import IngestResponse

router=APIRouter()

@router.post(
    "/ingest-document",
    response_model=IngestResponse,
    summary="Ingest Document into Data Store",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "An error occurred during document ingestion.",
        }
    }
)
async def ingest_document_endpoint(
    data_store_id: str = Form(..., description="Data store ID"),
    engine_id: str = Form(..., description="Engine ID"),
    file: UploadFile = File(..., description="Document file to upload (PDF, DOCX, TXT, etc.)")
):
    """
    Upload and ingest a document into a Vertex AI Search data store.
    
    This endpoint:
    1. Creates or reuses a GCS bucket for the data store
    2. Uploads the file to GCS
    3. Ingests the document into the data store
    4. Waits for indexing to complete
    
    **Form Data:**
    - **data_store_id**: The data store ID (returned from /create-engine)
    - **engine_id**: The engine ID (returned from /create-engine)
    - **file**: The document file to upload
    
    **Supported File Types:**
    - PDF (.pdf)
    - Microsoft Word (.docx, .doc)
    - Text (.txt)
    - HTML (.html, .htm)
    - Markdown (.md)
    
    **Response:**
    - **success_count**: Number of documents successfully ingested
    - **failure_count**: Number of documents that failed
    - **bucket_name**: GCS bucket where file was uploaded
    - **gcs_uri**: Full GCS URI of the uploaded file
    - **operation_name**: Operation name (if there were failures)
    - **message**: Status message
    
    **Note:** Document ingestion takes 1-3 minutes. The document will be searchable
    30 seconds after the operation completes.
    """
    try:
        if not file.filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No file provided"
            )
        
        # Check file extension
        allowed_extensions = {'.pdf', '.docx', '.doc', '.txt', '.html', '.htm', '.md'}
        file_ext = os.path.splitext(file.filename)[1].lower()
        
        if file_ext not in allowed_extensions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported file type: {file_ext}. Allowed: {', '.join(allowed_extensions)}"
            )
        
       
        return await ingestion(file=file, engine_id=engine_id, data_store_id=data_store_id)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error during document ingestion: {str(e)}"
        )