
from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud.discoveryengine_v1 import(DataStore,DataStoreServiceClient)

def _create_data_store(project_id: str, location: str, data_store_id: str) -> dict:
    """
    Create a new data store with the given ID.
    Returns the created or existing data store.
    """
    try:
        client = DataStoreServiceClient()
    except Exception as e:
        raise RuntimeError(f"Failed to create DataStoreServiceClient. Error: {e}")

    parent = f"projects/{project_id}/locations/{location}/collections/default_collection"
    data_store_name = f"{parent}/dataStores/{data_store_id}"

    # Check if data store already exists
    try:
        existing_store = client.get_data_store(name=data_store_name)
        print(f"Data store '{data_store_id}' already exists. Reusing it.")
        return {
            "status": "existing",
            "data_store_id": data_store_id,
            "data_store": existing_store
        }
    except NotFound:
        # Data store doesn't exist, create it
        pass

    # Create new data store
    data_store = DataStore(
        display_name=f"Auto-generated Data Store ({data_store_id})",
        industry_vertical="GENERIC",  # Use string instead of enum
        content_config="CONTENT_REQUIRED",  # Use string instead of enum
        solution_types=["SOLUTION_TYPE_SEARCH"],  # Use string instead of enum
    )

    request = {
        "parent": parent,
        "data_store": data_store,
        "data_store_id": data_store_id,
        "create_advanced_site_search": False,
    }

    try:
        print(f"Creating new data store: {data_store_id}...")
        operation = client.create_data_store(request=request)
        response = operation.result(timeout=600)
        
        print(f"Data store '{data_store_id}' created successfully!")
        return {
            "status": "created",
            "data_store_id": data_store_id,
            "data_store": response
        }
        
    except AlreadyExists:
        # Race condition: created between check and create
        print(f"Data store '{data_store_id}' was created concurrently. Fetching it.")
        existing_store = client.get_data_store(name=data_store_name)
        return {
            "status": "existing",
            "data_store_id": data_store_id,
            "data_store": existing_store
        }
    except Exception as e:
        raise RuntimeError(f"Failed to create data store '{data_store_id}'. Error: {e}")