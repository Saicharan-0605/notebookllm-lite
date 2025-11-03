from fastapi import APIRouter, HTTPException
from schemas.document import IngestRequest, QueryRequest, QueryResponse, IngestResponse
from services import search_service

router = APIRouter()


@router.post("/query", response_model=QueryResponse, summary="Query the search engine")
async def query_documents(request: QueryRequest):
    """
    Queries the search engine with a given question and returns an AI-generated summary with citations.
    """
    try:
        response = search_service.query_documents_service(request.question,request.ENGINE_ID)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))