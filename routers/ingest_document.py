import os
import uuid
from typing import Optional
import asyncio
from fastapi import status,APIRouter
from fastapi import FastAPI, HTTPException, UploadFile, File, Form,BackgroundTasks
from services.ingestion_service import ingestion,get_documents_by_engine
from services.database import create_task_in_db
from utils.settings import settings

from schemas.document import IngestResponse,DocumentListResponse,DocumentResponse,TaskCreateResponse

router=APIRouter()

@router.post(
    "/ingest-document",
    
    response_model=TaskCreateResponse,
    summary="Accept Document for Background Ingestion",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_400_BAD_REQUEST: {
            "description": "Bad request, e.g., no file provided or unsupported file type."
        }
    }
)
async def ingest_document_endpoint(
    background_tasks: BackgroundTasks,
    data_store_id: str = Form(..., description="Data store ID"),
    engine_id: str = Form(..., description="Engine ID"),
    file: UploadFile = File(..., description="Document file to upload")
):
    """
    Accepts a document and begins the ingestion process in the background.
    
    This endpoint returns an immediate response, and the document processing
    (GCS upload, Vertex AI ingestion) happens asynchronously.
    """
    task_id = str(uuid.uuid4())
    create_task_in_db(task_id=task_id, filename=file.filename)
    if not file.filename:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No file provided")
    
    allowed_extensions = {'.pdf', '.docx', '.doc', '.txt', '.html', '.htm', '.md'}
    file_ext = os.path.splitext(file.filename)[1].lower()
    
    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported file type: {file_ext}. Allowed: {', '.join(allowed_extensions)}"
        )
    
    # Schedule the long-running task
    await asyncio.to_thread(
        ingestion,
        task_id=task_id, 
        file=file, 
        engine_id=engine_id, 
        data_store_id=data_store_id
    )
    
    
    return TaskCreateResponse(
        message="Document ingestion has been accepted and is processing in the background.",
        task_id=task_id,
        filename=file.filename
    )

from schemas.document import TaskStatusResponse
from services.database import get_task_from_db

@router.get(
    "/tasks/{task_id}",
    response_model=TaskStatusResponse,
    summary="Get Background Task Status",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task not found"}
    }
)
async def get_task_status_endpoint(task_id: str):
    """
    Poll this endpoint to check the status of a background ingestion task.
    """
    task = get_task_from_db(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return task

@router.get(
    "/documents/{engine_id}",
    response_model=DocumentListResponse,
    summary="List Documents by Engine ID",
    status_code=status.HTTP_200_OK,
    # You can remove the 404 response from the docs if you make this change
    responses={
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "An error occurred while fetching documents.",
        }
    }
)
async def list_documents_endpoint(
    engine_id: str,
    limit: Optional[int] ,
    offset: Optional[int] ,
    sort_order: Optional[str] 
):
    
    try:
        
        valid_sort_orders = ["asc", "desc"]
        if sort_order and sort_order.lower() not in valid_sort_orders:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid sort_order. Must be 'asc' or 'desc'"
            )
        
        
        result = get_documents_by_engine(
            engine_id=engine_id,
            limit=limit,
            offset=offset,
            sort_order=sort_order
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching documents: {str(e)}"
        )
    


@router.get(
    "/documents/{engine_id}/{document_id}",
    response_model=Optional[DocumentResponse],
    summary="Get Document by ID",
    status_code=status.HTTP_200_OK,
)
async def get_document_endpoint(
    engine_id: str,
    document_id: str
):
    """
    Get details of a specific document by its ID.
    Returns null if document not found.
    
    **Path Parameters:**
    - **engine_id**: The engine ID
    - **document_id**: The document ID (row ID from database)
    """
    try:
        from services.ingestion_service import get_document_by_id
        
        document = get_document_by_id(document_id, engine_id)
        
        # Returns None if not found, which will be null in JSON
        return document
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching document: {str(e)}"
        )


@router.delete(
    "/documents/{engine_id}/{document_id}",
    summary="Delete Document",
    status_code=status.HTTP_200_OK,
)

async def delete_document_endpoint(
    engine_id: str,
    document_id: str,
    
):
    """
    Delete a document from the database, GCS bucket, and Vertex AI Search index.
    
    This performs a multi-step deletion:
    1. Removes the document from the search index (if requested).
    2. Deletes the source file from the GCS bucket (if requested).
    3. Removes the document's metadata record from the local database.
    """
    try:
        
        from services.ingestion_service import get_document_by_id,delete_document_logic
        doc = get_document_by_id(document_id=document_id, engine_id=engine_id)
        if not doc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Document {document_id} not found for engine {engine_id}"
            )
        
        # Call the service layer to perform the deletions
        result = delete_document_logic(
            document_id=document_id,
            engine_id=engine_id,
            data_store_id=doc["data_store_id"],
            gcs_uri=doc["gcs_uri"],
            filename=doc["filename"]
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred during document deletion: {str(e)}"
        )