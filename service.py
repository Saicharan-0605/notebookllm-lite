from google.cloud.discoveryengine_v1beta import (
    DocumentServiceClient,
    ConversationalSearchServiceClient,
    ImportDocumentsRequest,
    GcsSource,
    Session,
    TextInput,
    ConverseConversationRequest,
    CreateSessionRequest,

)
from google.cloud import discoveryengine_v1 as discoveryengine
from google.api_core import exceptions
from dotenv import load_dotenv
import time

load_dotenv()

# --- Configuration Variables ---
PROJECT_ID = "gcp-agents-personal"
LOCATION = "global"
DATA_STORE_ID = "risk-analyst-data-store_1760851888697"
GCS_INPUT_URI = "gs://risk_analyst_bucket/System+and+Organization+Controls+(SOC)+2+Report(mini).pdf"

# Resource paths construction
DATA_STORE_PARENT = f"projects/{PROJECT_ID}/locations/{LOCATION}/dataStores/{DATA_STORE_ID}"
DOCUMENT_BRANCH = f"{DATA_STORE_PARENT}/branches/default_branch"

# --- STEP 1: INGEST THE DOCUMENT ---

def ingest_document_from_gcs():
    """Triggers a bulk document import and waits for it to complete."""
    print("Starting document ingestion...")
    
    try:
        document_client = DocumentServiceClient()
        gcs_source = GcsSource(input_uris=[GCS_INPUT_URI])
        discovery_client = discoveryengine.DocumentServiceClient()
        parent=discovery_client.branch_path(
            project=PROJECT_ID,
            location="global", # Ingestion is typically 'global' location
            data_store=DATA_STORE_ID,
            branch="default_branch",
        )
        request = ImportDocumentsRequest(
            parent=parent,
            gcs_source=gcs_source,
            reconciliation_mode=ImportDocumentsRequest.ReconciliationMode.FULL
        )
        
        operation = document_client.import_documents(request=request)
        
        print(f"Ingestion triggered. Waiting for operation to complete: {operation.operation.name}")
        response = operation.result() # This will block until the operation is done
        print("Document ingestion completed successfully!")
        
    except exceptions.GoogleAPICallError as e:
        print(f"An error occurred during ingestion: {e}")


# --- STEP 2: ASK A QUERY ---

def create_conversational_session() -> str:
    """Creates a new conversational session."""
    cs_client = ConversationalSearchServiceClient()

    # --- THE FIX IS HERE ---
    # Use the proper request object instead of a dictionary
    create_session_request = CreateSessionRequest(
        parent=DATA_STORE_PARENT,
        session=Session(), # Pass an empty Session object
    )
    
    session_response = cs_client.create_session(request=create_session_request)
    session_name = session_response.name
    print(f"Session created: {session_name}")
    return session_name

def send_query(session_name: str, user_query: str) -> str:
    """Sends a query within an existing session."""
    print(f"\n> Querying: {user_query}")
    
    cs_client = ConversationalSearchServiceClient()
    
    converse_request = ConverseConversationRequest(
        # name=session_name,
        query=TextInput(input=user_query),
    )
    
    converse_response = cs_client.converse_conversation(request=converse_request)
    
    answer_text = converse_response.reply.text
    print(f"\nAI Response:\n{answer_text}")
    
    # Return the session name from the response to be used in the next turn
    return converse_response.conversation.name
    
# --- Execution Example ---
if __name__ == "__main__":
    try:
        # NOTE: Only run ingestion when you need to add or update documents.
        # It can take several minutes.
        # ingest_document_from_gcs()

        # 1. Create a single session for the entire conversation
        active_session = create_conversational_session()

        # 2. Ask your query (or multiple queries) using that session
        query = (
            "Information protection program: Do the provided information protection artifacts "
            "provide a reasonable assurance that the supplier has an information protection program "
            "complete with ownership, adoption, enforcement and compliance that is based on an "
            "industry wide framework?"
        )
        active_session = send_query(active_session, query)

        # You can now ask a follow-up question using the same 'active_session'
        # follow_up_query = "What framework is it based on?"
        # active_session = send_query(active_session, follow_up_query)

    except exceptions.NotFound as e:
        print(f"Error: A required resource was not found. Please check your PROJECT_ID, LOCATION, and DATA_STORE_ID. Details: {e}")
    except exceptions.InvalidArgument as e:
        print(f"Error: An invalid argument was provided. This can happen if the Data Store is not ready or the resource name is malformed. Details: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")