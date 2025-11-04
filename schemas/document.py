# schemas/documents.py

from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class IngestRequest(BaseModel):
    """Schema for the document ingestion request."""
    gcs_document_uri: str

class QueryRequest(BaseModel):
    """Schema for the document query request."""
    question: str
    ENGINE_ID: str

class Citation(BaseModel):
    """Schema for a single citation in the search results."""
    start_index: int
    end_index: int
    source: str

class ExtractiveAnswer(BaseModel):
    """Schema for a single extractive answer from a document."""
    page_number: str
    content: str

class ExtractiveSegment(BaseModel):
    """Schema for a single extractive segment from a document."""
    page_number: str
    content: str

class SearchResult(BaseModel):
    """Schema for a single search result, containing document info and answers."""
    title: str
    uri: str
    extractive_answers: List[ExtractiveAnswer]

class QueryResponse(BaseModel):
    """Schema for the complete query response."""
    summary: str
    results: List[SearchResult]
    citations: List[Citation]

class IngestResponse(BaseModel):
    """Schema for the document ingestion response."""
    success_count: int
    failure_count: int
    operation_name: Optional[str] = None


class EngineCreationRequest(BaseModel):
    """Defines the simplified request body for creating an engine."""
    engine_name: str

class EngineResponse(BaseModel):
    """Defines the successful response structure."""
    engine_id: str
    engine_name: str
    data_store_id: str
    solution_type: str
    message: str


class IngestRequest(BaseModel):
    """Request body for document ingestion."""
    data_store_id: str 
    engine_id: str 


class IngestResponse(BaseModel):
    """Response for document ingestion."""
    success_count: int
    failure_count: int
    bucket_name: str
    gcs_uri: str
    operation_name: Optional[str] = None
    message: str

class EngineInfo(BaseModel):
    """Engine information from database."""
    id: int
    engine_id: str
    engine_name: str
    data_store_id: str


class DocumentResponse(BaseModel):
    """Response model for a single document"""
    document_id: str
    engine_id: str 
    data_store_id: str 
    filename: str 
    gcs_uri: str 
    file_size: int
    content_type: str 
    
    class Config:
        json_schema_extra = {
            "example": {
                "document_id": "ds-uieeeeuo083",
                "engine_id": "my-engine-123",
                "data_store_id": "my-datastore-123",
                "filename": "safety_manual.pdf",
                "gcs_uri": "gs://my-bucket/documents/safety_manual.pdf",
                "file_size": 2048576,
                "content_type": "application/pdf",
                "created_at": "2024-01-15T10:30:00"
            }
        }


class DocumentListResponse(BaseModel):
    """Response model for list of documents"""
    engine_id: str 
    data_store_id: Optional[str] 
    total_count: int 
    returned_count: int 
    documents: List[DocumentResponse] 
    
    class Config:
        json_schema_extra = {
            "example": {
                "engine_id": "my-engine-123",
                "data_store_id": "my-datastore-123",
                "total_count": 25,
                "returned_count": 10,
                "documents": [
                    {
                        "id": 1,
                        "engine_id": "my-engine-123",
                        "data_store_id": "my-datastore-123",
                        "filename": "safety_manual.pdf",
                        "gcs_uri": "gs://my-bucket/documents/safety_manual.pdf",
                        "file_size": 2048576,
                        "content_type": "application/pdf",
                        "created_at": "2024-01-15T10:30:00"
                    }
                ]
            }
        }


class IngestResponse(BaseModel):
    """Your existing IngestResponse schema"""
    success_count: int
    failure_count: int
    bucket_name: str
    gcs_uri: str
    operation_name: Optional[str] = None
    message: str