from fastapi import FastAPI
from routers import search as search_router
from routers import ingest_document as ingest_router
from routers import engine_router as engine_router


app = FastAPI(
    title="NotebookLM-like API with Vertex AI Search",
    description="An API for ingesting and querying documents using Google Cloud's Vertex AI Search.",
    version="1.0.0",
)

@app.on_event("startup")
async def startup_event():
    """
    On startup, create the Enterprise Edition engine if it doesn't exist.
    """
app.include_router(search_router.router, prefix="/api/v1", tags=["Search"])
app.include_router(ingest_router.router, prefix="/api/v1", tags=["Ingest"])
app.include_router(engine_router.router, prefix="/api/v1", tags=["Engine"])

@app.get("/", tags=["Root"])
async def read_root():
    """
    Root endpoint for the API.
    """
    return {"message": "Welcome to the NotebookLM-like API!"}