import time
import os
from google.oauth2 import service_account
from google.cloud.discoveryengine_v1 import (
    EngineServiceClient,
    DocumentServiceClient,
    SearchServiceClient,
    GcsSource,
    ImportDocumentsRequest,
    SearchRequest,
    Engine,
)
from google.cloud.discoveryengine_v1.services.search_service.pagers import SearchPager
from typing import List
from google.api_core.exceptions import AlreadyExists, NotFound
from schemas.document import IngestResponse, QueryResponse, SearchResult, Citation,ExtractiveAnswer,ExtractiveSegment
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================
PROJECT_ID = "gcp-agents-personal"
LOCATION = "global"
DATA_STORE_ID = "agent-space_1761539656965"
ENGINE_ID = "notebooklm-search-engine"

# ============================================================================
# AUTHENTICATION HELPER
# ============================================================================
def get_gcp_credentials():
    """
    Loads Google Cloud credentials from a service account file.

    The path to the service account file should be specified in the
    GOOGLE_APPLICATION_CREDENTIALS environment variable.
    """
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise ValueError(
            "The GOOGLE_APPLICATION_CREDENTIALS environment variable is not set. "
            "Please point it to your service account JSON file."
        )
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(
            f"Service account file not found at path: {credentials_path}"
        )
    
    return service_account.Credentials.from_service_account_file(credentials_path)

# ============================================================================
# SERVICE FUNCTIONS
# ============================================================================

def create_enterprise_engine():
    """
    Create an Enterprise Edition engine using a service account for authentication.
    """
    credentials = get_gcp_credentials()
    client = EngineServiceClient(credentials=credentials)
    
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}/collections/default_collection"

    engine = Engine(
        display_name="NotebookLM Search Engine",
        solution_type="SOLUTION_TYPE_SEARCH",
        data_store_ids=[DATA_STORE_ID],
        search_engine_config=Engine.SearchEngineConfig(
            search_tier="SEARCH_TIER_ENTERPRISE",
            search_add_ons=["SEARCH_ADD_ON_LLM"],
        ),
        industry_vertical="GENERIC",
    )

    request = {
        "parent": parent,
        "engine": engine,
        "engine_id": ENGINE_ID,
    }

    try:
        print("Attempting to create enterprise engine...")
        operation = client.create_engine(request=request)
        response = operation.result(timeout=900)
        print("Engine created successfully.")
        return response
    except AlreadyExists:
        print(f"Engine '{ENGINE_ID}' already exists. Skipping creation.")
        engine_name = f"{parent}/engines/{ENGINE_ID}"
        return client.get_engine(name=engine_name)
    except Exception as e:
        print(f"Error creating engine: {e}")
        raise

def ingest_documents_service(gcs_uri: str) -> IngestResponse:
    """
    Ingest a document from GCS using a service account for authentication.
    """
    credentials = get_gcp_credentials()
    client = DocumentServiceClient(credentials=credentials)
    
    parent_path = client.branch_path(
        project=PROJECT_ID,
        location=LOCATION,
        data_store=DATA_STORE_ID,
        branch="default_branch",
    )

    gcs_source = GcsSource(input_uris=[gcs_uri], data_schema="content")
    request = ImportDocumentsRequest(
        parent=parent_path,
        gcs_source=gcs_source,
        reconciliation_mode=ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
    )

    operation = client.import_documents(request=request)
    response = operation.result(timeout=300)
    metadata = operation.metadata

    time.sleep(30)

    return IngestResponse(
        success_count=metadata.success_count,
        failure_count=metadata.failure_count,
        operation_name=operation.operation.name if metadata.failure_count > 0 else None,
    )




def load_search_response(pages: SearchPager) -> QueryResponse:
    """
    Loads the search results from a SearchPager object into the QueryResponse Pydantic model.

    Args:
        pages: The SearchPager object returned by the Discovery Engine client's search method.

    Returns:
        A QueryResponse object populated with the search results.
    """
    results: List[SearchResult] = []
    citations: List[Citation] = []
    summary_text = ""

    first_response = next(iter(pages.pages), None)

    if first_response and first_response.summary:
        summary_text = first_response.summary.summary_text
        
        # Check if the detailed metadata for citations is present
        if first_response.summary.summary_with_metadata:
            summary_meta = first_response.summary.summary_with_metadata
            
            source_documents = [ref.document for ref in summary_meta.references]

            if summary_meta.citation_metadata:
                for api_citation in summary_meta.citation_metadata.citations:
                    source_id = source_documents[0] if source_documents else ""
                    
                    citations.append(
                        Citation(
                            start_index=api_citation.start_index,
                            end_index=api_citation.end_index,
                            source=source_id,
                        )
                    )

    # Iterate through all search results across all pages
    for result in pages:
        # The 'document' attribute contains the core information
        doc_info = result.document
        # 'derived_struct_data' holds fields like title, link, snippets, etc.
        doc_data = doc_info.derived_struct_data

        extractive_answers = []
        # Check for extractive answers and process them
        if "extractive_answers" in doc_data:
            for answer in doc_data["extractive_answers"]:
                extractive_answers.append(
                    ExtractiveAnswer(
                        page_number=answer.get("pageNumber", ""),
                        content=answer.get("content", ""),
                    )
                )

        extractive_segments = []
        # Check for extractive segments and process them
        if "extractive_segments" in doc_data:
            for segment in doc_data["extractive_segments"]:
                extractive_segments.append(
                    ExtractiveSegment(
                        page_number=segment.get("pageNumber", ""),
                        content=segment.get("content", ""),
                    )
                )

        results.append(
            SearchResult(
                # Use the document name/ID as a fallback for title if not present
                title=doc_data.get("title", doc_info.name),
                uri=doc_data.get("link", ""),
                extractive_answers=extractive_answers,
            )
        )

    return QueryResponse(
        summary=summary_text,
        results=results,
        citations=citations,
    )


def query_documents_service(question: str) -> bool:
    """
    Query documents using a service account for authentication.
    """
    print("Querying documents...")
    credentials = get_gcp_credentials()
    client = SearchServiceClient(credentials=credentials)
    
    serving_config = (
        f"projects/{PROJECT_ID}/locations/{LOCATION}/"
        f"collections/default_collection/engines/{ENGINE_ID}/"
        f"servingConfigs/default_config"
    )

    content_search_spec = SearchRequest.ContentSearchSpec(
        summary_spec=SearchRequest.ContentSearchSpec.SummarySpec(
            summary_result_count=5,
            include_citations=True,
            # Other specs...
        ),
        extractive_content_spec=SearchRequest.ContentSearchSpec.ExtractiveContentSpec(
            max_extractive_answer_count=3,
        ),
    )

    request = SearchRequest(
        serving_config=serving_config,
        query=question,
        page_size=10,
        content_search_spec=content_search_spec,
        query_expansion_spec=SearchRequest.QueryExpansionSpec(
            condition=SearchRequest.QueryExpansionSpec.Condition.AUTO
        ),
    )

    #get the SearchPager from client
    pages = client.search(request)


    
    return load_search_response(pages)
