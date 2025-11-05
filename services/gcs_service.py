from google.cloud import storage
import time
from google.api_core.exceptions import  NotFound,Conflict
from typing import Optional, Tuple
from services.database import get_document_gcs_uris_by_engine
def _get_or_create_bucket(
    engine_id: str, 
    data_store_id: str,
    project_id: str, 
    location: str = "us",
    max_retries: int = 3,
    retry_delay: int = 2
) -> str:
    """
    Get or create a GCS bucket for the data store with retry logic.
    
    Args:
        engine_id: Engine ID (used in bucket name)
        data_store_id: Data store ID (used in bucket name)
        project_id: GCP project ID
        location: GCS bucket location
        max_retries: Maximum number of retries
        retry_delay: Delay between retries in seconds
    
    Returns:
        Bucket name
    """
    storage_client = storage.Client(project=project_id)
    
    # Generate bucket name: datastore-id with valid GCS naming
    bucket_name = f"{engine_id}-{data_store_id}".lower()
    bucket_name = bucket_name.replace("_", "-")[:63]  # GCS bucket name limit
    
    for attempt in range(max_retries):
        try:
            # Check if bucket exists
            bucket = storage_client.get_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' already exists. Reusing it.")
            return bucket_name
            
        except NotFound:
            # Bucket doesn't exist, try to create it
            try:
                print(f"Creating new bucket: {bucket_name} (Attempt {attempt + 1}/{max_retries})")
                bucket = storage_client.bucket(bucket_name)
                bucket.storage_class = "STANDARD"
                
                # Set location based on data store location
                if location == "global":
                    bucket_location = "us"  # Default to US for global
                else:
                    bucket_location = location.lower()
                
                # Create the bucket
                new_bucket = storage_client.create_bucket(bucket, location=bucket_location)
                print(f"Bucket '{bucket_name}' created successfully in {bucket_location}.")
                
                # CRITICAL: Wait longer for bucket to be fully propagated across all GCS services
                # The Document AI/Vertex AI Search service needs time to recognize the new bucket
                propagation_wait = 15  # Increased from 3 to 15 seconds
                print(f"Waiting {propagation_wait}s for bucket to propagate across all GCS services...")
                time.sleep(propagation_wait)
                
                # Verify bucket is accessible
                verify_bucket = storage_client.get_bucket(bucket_name)
                print(f"✓ Bucket '{bucket_name}' is ready and accessible.")
                return bucket_name
                
            except Conflict:
                # Bucket was created by another process (race condition)
                print(f"Bucket '{bucket_name}' was created by another process. Retrieving it.")
                time.sleep(retry_delay)
                try:
                    bucket = storage_client.get_bucket(bucket_name)
                    return bucket_name
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"⚠ Failed to retrieve bucket: {e}. Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        raise RuntimeError(f"Failed to retrieve bucket after conflict: {e}")
                        
            except Exception as e:
                error_msg = str(e).lower()
                is_retryable = any(keyword in error_msg for keyword in [
                    "unavailable", "deadline", "timeout", "503", "500"
                ])
                
                if is_retryable and attempt < max_retries - 1:
                    print(f"⚠ Bucket creation attempt {attempt + 1} failed: {e}")
                    print(f"   Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    raise RuntimeError(f"Failed to create bucket after {attempt + 1} attempts: {e}")
                    
        except Exception as e:
            # Unexpected error when checking bucket existence
            error_msg = str(e).lower()
            is_retryable = any(keyword in error_msg for keyword in [
                "unavailable", "deadline", "timeout", "503", "500"
            ])
            
            if is_retryable and attempt < max_retries - 1:
                print(f"⚠ Attempt {attempt + 1} to access bucket failed: {e}")
                print(f"   Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                raise RuntimeError(f"Failed to get or create bucket: {e}")
    
    raise RuntimeError(f"Failed to get or create bucket '{bucket_name}' after {max_retries} attempts")


def _upload_file_to_gcs(
    project_id: str,
    bucket_name: str, 
    file_content: bytes, 
    filename: str,
    max_retries: int = 3,
    retry_delay: int = 2
) -> str:
    """
    Upload a file to GCS bucket with retry logic.
    
    Args:
        project_id: GCP project ID
        bucket_name: GCS bucket name
        file_content: File content as bytes
        filename: Original filename
        max_retries: Maximum number of retries
        retry_delay: Delay between retries in seconds
    
    Returns:
        GCS URI (gs://bucket/filename)
    """
    storage_client = storage.Client(project=project_id)
    
    # Generate unique blob name to avoid conflicts
    timestamp = int(time.time())
    blob_name = f"documents/{timestamp}_{filename}"
    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    
    for attempt in range(max_retries):
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            print(f"Uploading {filename} to {gcs_uri} (Attempt {attempt + 1}/{max_retries})")
            blob.upload_from_string(file_content)
            
            # Verify file was uploaded successfully
            if blob.exists():
                print(f"✓ File uploaded successfully: {gcs_uri}")
                
                # Small delay to ensure file is fully available
                time.sleep(1)
                return gcs_uri
            else:
                raise RuntimeError("Upload completed but file not found in bucket")
            
        except NotFound as e:
            if attempt < max_retries - 1:
                print(f"⚠ Bucket not found: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                raise RuntimeError(f"Bucket '{bucket_name}' not found after {max_retries} attempts: {e}")
                
        except Exception as e:
            error_msg = str(e).lower()
            is_retryable = any(keyword in error_msg for keyword in [
                "unavailable", "deadline", "timeout", "503", "500", "connection"
            ])
            
            if is_retryable and attempt < max_retries - 1:
                print(f"⚠ Upload attempt {attempt + 1} failed: {e}")
                print(f"   Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                raise RuntimeError(f"Failed to upload file after {attempt + 1} attempts: {e}")
    
    raise RuntimeError(f"Failed to upload file to GCS after {max_retries} attempts")



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