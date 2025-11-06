from google.cloud import storage
import time
from google.api_core.exceptions import  NotFound,Conflict
from typing import Optional, Tuple
from services.database import get_document_gcs_uris_by_engine

def _create_gcs_bucket(
    project_id: str,
    bucket_name: str,
    location: str,
) -> str:
    """
    Creates a new GCS bucket and waits for it to propagate.
    This function is intended to be called ONCE during engine setup.

    Args:
        project_id: The GCP project ID.
        bucket_name: The globally unique name for the new bucket.
        location: The GCS location for the bucket (e.g., "us-central1").

    Returns:
        The name of the created bucket.
        Raises RuntimeError on persistent failure.
    """
    try:
        print(f"Attempting to create new GCS bucket: '{bucket_name}' in location '{location}'...")
        storage_client = storage.Client(project=project_id)
        
        bucket_to_create = storage_client.bucket(bucket_name)
        bucket_to_create.storage_class = "STANDARD"
        
        storage_client.create_bucket(bucket_to_create, location=location)
        print(f"Bucket '{bucket_name}' creation request sent successfully.")
        
        # CRITICAL: Wait for bucket to be fully propagated across GCS services.
        propagation_wait = 15
        print(f"Waiting {propagation_wait}s for bucket to propagate...")
        time.sleep(propagation_wait)
        
        # Final verification to ensure it's ready
        storage_client.get_bucket(bucket_name)
        print(f"✓ Bucket '{bucket_name}' created and verified.")
        return bucket_name

    except Conflict:
        # This is a rare race condition, but it's safe to assume it's ready.
        print(f"Conflict: Bucket '{bucket_name}' already existed. Assuming it's ready for use.")
        return bucket_name
    except Exception as e:
        # If bucket creation fails for any other reason, it's a critical error.
        raise RuntimeError(f"Failed to create and verify GCS bucket '{bucket_name}'. Error: {e}")
def _get_gcs_bucket(
    project_id: str,
    bucket_name: str,
    max_retries: int = 3,
    retry_delay: int = 2
) -> str:
    """
    Checks for a GCS bucket's existence and accessibility with retries.
    This function is intended to be called during document ingestion.

    Args:
        project_id: The GCP project ID.
        bucket_name: The name of the bucket to find.
        max_retries: Maximum number of retries for transient network errors.
        retry_delay: Delay between retries in seconds.

    Returns:
        The bucket name if it is found and accessible.
        Raises RuntimeError if the bucket is not found or if a persistent error occurs.
    """
    print(f"Attempting to find GCS bucket '{bucket_name}'...")
    storage_client = storage.Client(project=project_id)

    for attempt in range(max_retries):
        try:
            storage_client.get_bucket(bucket_name)
            print(f"✓ Found bucket '{bucket_name}'.")
            return bucket_name
        except NotFound:
            # This is a fatal configuration error. The bucket should already exist.
            raise RuntimeError(f"Configuration Error: Bucket '{bucket_name}' was not found. It should have been created with its engine.")
        except Exception as e:
            # Handle transient network/API errors that are worth retrying
            if "unavailable" in str(e).lower() and attempt < max_retries - 1:
                print(f"⚠ Bucket check failed (transient error): {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                # Re-raise the exception if it's not retryable or retries are exhausted
                raise RuntimeError(f"A persistent error occurred while trying to access bucket '{bucket_name}': {e}")
    
    # This line should not be reached but is a fallback
    raise RuntimeError(f"Failed to find bucket '{bucket_name}' after {max_retries} attempts.")


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