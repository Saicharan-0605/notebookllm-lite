from google.cloud import storage
import time
from google.api_core.exceptions import  NotFound
from typing import Any,Dict
from services.database import get_document_by_id,delete_document_from_db,get_document_gcs_uris_by_engine

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


from google.cloud import storage
from typing import Optional, Tuple

def _delete_gcs_bucket_and_files(
    project_id: str,
    location: str,
    data_store_id: str, 
    engine_id: str
) -> Tuple[bool, Optional[str]]:
    """
    Delete GCS bucket and all files associated with a data store.
    Files are retrieved from the documents table.
    
    Args:
        project_id: GCP project ID
        location: GCP location
        data_store_id: The data store ID
        engine_id: The engine ID (used for retrieving documents)
    
    Returns:
        Tuple of (success: bool, warning_message: Optional[str])
    """
    try:
        storage_client = storage.Client(project=project_id)
        
        # Get all GCS URIs from documents table for this engine
        gcs_uris = get_document_gcs_uris_by_engine(engine_id)
        
        if not gcs_uris:
            warning = f"No GCS files found for engine '{engine_id}' in documents table"
            print(f"⚠ {warning}")
            return True, warning
        
        print(f"\nDeleting {len(gcs_uris)} files from GCS for engine '{engine_id}'...")
        
        deleted_files = 0
        failed_files = []
        
        # Delete individual files from GCS
        for gcs_uri in gcs_uris:
            try:
                # Parse GCS URI: gs://bucket-name/path/to/file
                if gcs_uri.startswith('gs://'):
                    uri_parts = gcs_uri[5:].split('/', 1)
                    bucket_name = uri_parts[0]
                    blob_name = uri_parts[1] if len(uri_parts) > 1 else ''
                    
                    bucket = storage_client.bucket(bucket_name)
                    blob = bucket.blob(blob_name)
                    blob.delete()
                    deleted_files += 1
                    print(f"  ✓ Deleted: {gcs_uri}")
                else:
                    print(f"  ⚠ Invalid GCS URI format: {gcs_uri}")
                    failed_files.append(gcs_uri)
                    
            except Exception as e:
                print(f"  ⚠ Failed to delete {gcs_uri}: {e}")
                failed_files.append(gcs_uri)
        
        print(f"✓ Deleted {deleted_files}/{len(gcs_uris)} files from GCS")
        
        if failed_files:
            warning = f"Failed to delete {len(failed_files)} files: {failed_files[:3]}"
            return deleted_files > 0, warning
        
        return True, None
                
    except Exception as e:
        warning = f"Failed to delete GCS files for engine '{engine_id}': {str(e)}"
        print(f"⚠ {warning}")
        return False, warning