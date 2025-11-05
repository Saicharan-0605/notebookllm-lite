from typing import List
from fastapi import HTTPException, status, APIRouter

from google.api_core.exceptions import  NotFound


from services.create_engine import _create_enterprise_engine_logic,get_engines_details,_delete_engine_logic
from schemas.document import EngineCreationRequest,EngineResponse,EngineInfo
from services.database import get_all_engines_from_db

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
        return await get_engines_details(engine_id)
        
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


@router.get(
    "/engines",
    response_model=List[EngineInfo],
    summary="List All Engines",
    status_code=status.HTTP_200_OK
)


async def list_engines():
    """
    Retrieve all engines from the database.
    
    Returns a list of all engines that have been created through this API,
    including their engine ID, name, data store ID, and creation time.
    """
    try:
        engines = get_all_engines_from_db()
        return [EngineInfo(**engine) for engine in engines]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve engines from database: {str(e)}"
        )



@router.delete(
    "/delete-engine/{engine_id}",
    response_model=dict,
    summary="Delete a Discovery Engine, Data Store, and GCS Files",
    status_code=status.HTTP_200_OK,
)
async def delete_engine_endpoint(
    engine_id: str,
    delete_data_store: bool = True,
    delete_gcs_files: bool = True
):
    """
    Delete an existing Enterprise Edition engine and all associated resources.

    **Deletion Order:**
    1. Engine from Google Cloud
    2. GCS files (retrieved from documents table)
    3. Data Store from Google Cloud
    4. Engine record from database
    5. Document records from database

    **Query Parameters:**
    - **delete_data_store**: If true, delete the data store (default: true)
    - **delete_gcs_files**: If true, delete all GCS files (default: true)
    """
    try:
        result_data = _delete_engine_logic(
            engine_id=engine_id,
            delete_data_store=delete_data_store,
            delete_gcs_files=delete_gcs_files
        )
        return result_data["result"]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete engine. Error: {str(e)}"
        )