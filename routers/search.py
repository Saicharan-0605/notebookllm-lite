from fastapi import APIRouter, HTTPException
from schemas.document import IngestRequest, QueryRequest, QueryResponse, IngestResponse
from services import search_service

router = APIRouter()

@router.post("/ingest", response_model=IngestResponse, summary="Ingest a document from GCS")
async def ingest_document(request: IngestRequest):
    """
    Ingests a document from the specified Google Cloud Storage URI into the search engine.
    """
    try:
        response = search_service.ingest_documents_service(request.gcs_document_uri)
        if response.failure_count > 0:
            raise HTTPException(status_code=500, detail=f"Document ingestion failed for {response.failure_count} documents.")
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/query", response_model=QueryResponse, summary="Query the search engine")
async def query_documents(request: QueryRequest):
    """
    Queries the search engine with a given question and returns an AI-generated summary with citations.
    """
    try:
        response = search_service.query_documents_service(request.question)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))