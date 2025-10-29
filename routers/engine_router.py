import os
from typing import Optional
from fastapi import FastAPI, HTTPException, status,UploadFile, File, Form,APIRouter
from pydantic import BaseModel, Field
from google.api_core.exceptions import  NotFound
from google.cloud.discoveryengine_v1 import ( 
    EngineServiceClient  
)

from services.create_engine import _create_enterprise_engine_logic,get_engine_details
from schemas.document import EngineCreationRequest,EngineResponse

router = APIRouter()

@router.post(
    "/create-engine",
    response_model=EngineResponse,
    summary="Create a Discovery Engine with Auto-Generated Data Store",
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_200_OK: {
            "description": "Engine already existed and was retrieved.",
            "model": EngineResponse,
        },
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "An error occurred during engine or data store creation.",
        }
    }
)
async def create_engine_endpoint(req: EngineCreationRequest):
    """
    Create a new Enterprise Edition engine for AI-powered search.

    This endpoint automatically:
    1. Generates a unique data store ID (UUID-based)
    2. Creates the data store (or reuses if it exists)
    3. Creates an engine linked to the data store
    4. Returns the engine and data store information

    The engine will be empty initially. You can ingest documents later using
    the returned `data_store_id`.

    **Request Body:**
    - **engine_name**: Display name for your engine (e.g., "Compliance Search")

    **Response:**
    - **engine_id**: Generated unique engine identifier
    - **engine_name**: Display name of the engine
    - **data_store_id**: Generated data store ID (use this for document ingestion)
    - **solution_type**: Type of solution (SEARCH)
    - **message**: Status message

    **Note:** This operation takes 5-10 minutes to complete.
    """
    try:
        result_data = _create_enterprise_engine_logic(
            engine_name=req.engine_name
        )
        
        if result_data["status_code"] == status.HTTP_200_OK:
            return EngineResponse(**result_data["result"])

        return result_data["result"]

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create or retrieve engine. Error: {str(e)}"
        )







@router.get(
    "/engines/{engine_id}",
    summary="Get Engine Details",
    status_code=status.HTTP_200_OK
)
async def get_engine_details(engine_id: str):
    """
    Retrieve details of an existing engine.
    
    - **engine_id**: The ID of the engine to retrieve
    """
    try:
        return get_engine_details(engine_id)
        
    except NotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Engine '{engine_id}' not found."
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve engine. Error: {str(e)}"
        )
