from google.cloud import storage
import time
from google.api_core.exceptions import  NotFound
from typing import Any,Dict
from services.database import get_document_by_id,delete_document_from_db

def _get_or_create_bucket(engine_id: str, data_store_id: str,project_id: str, location: str = "us") -> str:
    """
    Get or create a GCS bucket for the data store.
    
    Args:
        project_id: GCP project ID
        data_store_id: Data store ID (used in bucket name)
        location: GCS bucket location
    
    Returns:
        Bucket name
    """
    storage_client = storage.Client(project=project_id)
    
    # Generate bucket name: datastore-id with valid GCS naming
    bucket_name = f"{engine_id}-{data_store_id}".lower()
    bucket_name = bucket_name.replace("_", "-")[:63]  # GCS bucket name limit
    
    try:
        # Check if bucket exists
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' already exists. Reusing it.")
        return bucket_name
        
    except NotFound:
        # Create new bucket
        print(f"Creating new bucket: {bucket_name}")
        bucket = storage_client.bucket(bucket_name)
        bucket.storage_class = "STANDARD"
        
        # Set location based on data store location
        if location == "global":
            bucket_location = "us"  # Default to US for global
        else:
            bucket_location = location.lower()
        
        new_bucket = storage_client.create_bucket(bucket, location=bucket_location)
        print(f"Bucket '{bucket_name}' created successfully in {bucket_location}.")
        return bucket_name
        
    except Exception as e:
        raise RuntimeError(f"Failed to get or create bucket: {e}")
    

def _upload_file_to_gcs(
    project_id: str,
    bucket_name: str, 
    file_content: bytes, 
    filename: str
) -> str:
    """
    Upload a file to GCS bucket.
    
    Args:
        project_id: GCP project ID
        bucket_name: GCS bucket name
        file_content: File content as bytes
        filename: Original filename
    
    Returns:
        GCS URI (gs://bucket/filename)
    """
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    # Generate unique blob name to avoid conflicts
    timestamp = int(time.time())
    blob_name = f"documents/{timestamp}_{filename}"
    
    blob = bucket.blob(blob_name)
    
    try:
        print(f"Uploading {filename} to gs://{bucket_name}/{blob_name}")
        blob.upload_from_string(file_content)
        
        gcs_uri = f"gs://{bucket_name}/{blob_name}"
        print(f"File uploaded successfully: {gcs_uri}")
        return gcs_uri
        
    except Exception as e:
        raise RuntimeError(f"Failed to upload file to GCS: {e}")
    
