
from fastapi import status,APIRouter,HTTPException
from schemas.document import MindMapResponse,MindMapRequest
from utils.settings import settings
from services.mindmap import generate_mind_map
router = APIRouter()


    
@router.post("/generate-mindmap", response_model=MindMapResponse, summary="Generate Overview Mind Map with Mermaid Diagram", status_code=status.HTTP_200_OK)
async def generate_mindmap_endpoint(req: MindMapRequest):
        try:
            mind_map = generate_mind_map(
                project_id=settings.PROJECT_ID,
                location=settings.LOCATION,
                engine_id=req.engine_id
            )
            return mind_map
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to generate mind map: {str(e)}")