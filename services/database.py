import sqlite3
import uuid
from contextlib import contextmanager
from typing import Optional,List,Dict,Any

DB_PATH = "notebookllm.db"

def init_database():
    """
    Initialize SQLite database and create tables if they don't exist.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create engines table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS engines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            engine_id TEXT UNIQUE NOT NULL,
            engine_name TEXT NOT NULL,
            data_store_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create documents table (optional - for tracking uploaded documents)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            document_id TEXT PRIMARY KEY NOT NULL,
            engine_id TEXT NOT NULL,
            data_store_id TEXT NOT NULL,
            filename TEXT NOT NULL,
            gcs_uri TEXT NOT NULL,
            file_size INTEGER,
            content_type TEXT,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (engine_id) REFERENCES engines(engine_id)
        )
    """)

        # In your init_database() function
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY NOT NULL,
            filename TEXT,
            status TEXT NOT NULL,
            result TEXT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create a trigger to auto-update the 'updated_at' timestamp
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS update_tasks_updated_at
        AFTER UPDATE ON tasks FOR EACH ROW
        BEGIN
            UPDATE tasks SET updated_at = CURRENT_TIMESTAMP WHERE task_id = OLD.task_id;
        END;
    """)
    
    conn.commit()
    conn.close()
    print(" Database initialized successfully")


@contextmanager
def get_db_connection():
    """
    Context manager for database connections.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def save_engine_to_db(
    engine_id: str,
    engine_name: str,
    data_store_id: str
) -> int:
    """
    Save engine information to the database.
    
    Returns:
        Row ID of the inserted record
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO engines (engine_id, engine_name, data_store_id)
                VALUES (?, ?, ?)
            """, (engine_id, engine_name, data_store_id))
            
            row_id = cursor.lastrowid
            print(f" Engine saved to database (ID: {row_id})")
            return row_id
            
        except sqlite3.IntegrityError:
            # Engine already exists in database
            cursor.execute("SELECT id FROM engines WHERE engine_id = ?", (engine_id,))
            row = cursor.fetchone()
            print(f" Engine already exists in database (ID: {row['id']})")
            return row['id']


def get_engine_from_db(engine_id: str) -> Optional[dict]:
    """
    Retrieve engine information from the database.
    
    Returns:
        Dictionary with engine info or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, engine_id, engine_name, data_store_id, created_at
            FROM engines
            WHERE engine_id = ?
        """, (engine_id,))
        
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        return None


def get_all_engines_from_db() -> List[dict]:
    """
    Retrieve all engines from the database.
    
    Returns:
        List of dictionaries with engine info
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, engine_id, engine_name, data_store_id, created_at
            FROM engines
            ORDER BY created_at DESC
        """)
        
        rows = cursor.fetchall()
        return [dict(row) for row in rows]



def create_task_in_db(task_id: str, filename: str) -> None:
    """Creates a new task record in the database."""
    init_database()
    with get_db_connection() as conn:
        conn.cursor().execute(
            "INSERT INTO tasks (task_id, filename, status) VALUES (?, ?, ?)",
            (task_id, filename, "pending")
        )

def update_task_in_db(task_id: str, status: str, result: str = None, error: str = None) -> None:
    """Updates the status and result/error of a task."""
    with get_db_connection() as conn:
        conn.cursor().execute(
            "UPDATE tasks SET status = ?, result = ?, error_message = ? WHERE task_id = ?",
            (status, result, error, task_id)
        )

def get_task_from_db(task_id: str) -> Dict[str, Any] | None:
    """Retrieves a task record from the database."""
    with get_db_connection() as conn:
        row = conn.cursor().execute(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()
        return dict(row) if row else None



def save_document_to_db(
    document_id: str,
    engine_id: str,
    data_store_id: str,
    filename: str,
    gcs_uri: str,
    file_size: int,
    content_type: str
) -> str: 
    """
    Save uploaded document information to the database using a UUID as the primary key.
    
    Returns:
        The UUID (document_id) of the inserted record.
    """
    
    
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO documents (document_id, engine_id, data_store_id, filename, gcs_uri, file_size, content_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (document_id, engine_id, data_store_id, filename, gcs_uri, file_size, content_type))
        
        print(f"Document saved to database (UUID: {document_id})")
        return document_id

def get_documents_by_engine_id(
    engine_id: str,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "uploaded_at",
    sort_order: str = "desc"
) -> List[Dict[str, Any]]:
    """
    Get all documents for a specific engine from the database.
    
    Args:
        engine_id: The engine ID to filter by
        limit: Maximum number of documents to return
        offset: Number of documents to skip
        sort_by: Field to sort by (created_at, filename, file_size)
        sort_order: Sort order (asc or desc)
    
    Returns:
        List of document dictionaries
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Validate sort parameters to prevent SQL injection
        valid_sort_fields = {
            "uploaded_at": "uploaded_at",
            "filename": "filename",
            "file_size": "file_size"
        }
        sort_field = valid_sort_fields.get(sort_by, "uploaded_at")
        sort_direction = "DESC" if sort_order.lower() == "desc" else "ASC"
        
        query = f"""
            SELECT 
                document_id,
                engine_id,
                data_store_id,
                filename,
                gcs_uri,
                file_size,
                content_type,
                uploaded_at
            FROM documents
            WHERE engine_id = ?
            ORDER BY {sort_field} {sort_direction}
            LIMIT ? OFFSET ?
        """
        
        cursor.execute(query, (engine_id, limit, offset))
        
        columns = [desc[0] for desc in cursor.description]
        documents = []
        
        for row in cursor.fetchall():
            doc = dict(zip(columns, row))
            documents.append(doc)
        
        return documents


def get_total_document_count(engine_id: str) -> int:
    """
    Get total count of documents for an engine.
    
    Args:
        engine_id: The engine ID to count documents for
    
    Returns:
        Total number of documents
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM documents
            WHERE engine_id = ?
        """, (engine_id,))
        
        result = cursor.fetchone()
        return result[0] if result else 0


def get_document_by_id(document_id: str, engine_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a specific document by ID and engine_id.
    
    Args:
        document_id: The document ID
        engine_id: The engine ID (for security)
    
    Returns:
        Document dictionary or None if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                document_id,
                engine_id,
                data_store_id,
                filename,
                gcs_uri,
                file_size,
                content_type
            FROM documents
            WHERE document_id = ? AND engine_id = ?
        """, (document_id, engine_id))
        
        row = cursor.fetchone()
        if not row:
            return None
        
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, row))


def delete_document_from_db(document_id: str, engine_id: str) -> bool:
    """
    Delete a document from the database.
    
    Args:
        document_id: The document ID to delete
        engine_id: The engine ID (for security)
    
    Returns:
        True if deleted, False if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM documents
            WHERE document_id = ? AND engine_id = ?
        """, (document_id, engine_id))
        
        deleted = cursor.rowcount > 0
        if deleted:
            print(f"Document {document_id} deleted from database")
        
        return deleted
    
def delete_engine_from_db(engine_id: str) -> bool:
    """
    Delete an engine from the database.
    
    Args:
        engine_id: The engine ID to delete
    
    Returns:
        True if deleted, False if not found
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM engines WHERE engine_id = ?", (engine_id,))
        deleted = cursor.rowcount > 0
        
        if deleted:
            print(f" Engine '{engine_id}' deleted from database")
        else:
            print(f" Engine '{engine_id}' not found in database")
        
        return deleted


def delete_documents_by_engine(engine_id: str) -> int:
    """
    Delete all documents associated with an engine.
    
    Args:
        engine_id: The engine ID
    
    Returns:
        Number of documents deleted
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM documents WHERE engine_id = ?", (engine_id,))
        deleted_count = cursor.rowcount
        
        print(f" Deleted {deleted_count} documents for engine '{engine_id}'")
        return deleted_count


def get_engines_by_datastore(data_store_id: str) -> List[dict]:
    """
    Get all engines that use a specific data store.
    
    Args:
        data_store_id: The data store ID
    
    Returns:
        List of dictionaries with engine info
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, engine_id, engine_name, data_store_id, created_at
            FROM engines
            WHERE data_store_id = ?
        """, (data_store_id,))
        
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
def get_other_engines_using_datastore(data_store_id: str, exclude_engine_id: str) -> List[str]:
    """
    Get all other engines that use a specific data store, excluding a given engine.
    
    Args:
        data_store_id: The data store ID to check
        exclude_engine_id: Engine ID to exclude from results
    
    Returns:
        List of engine IDs using the data store
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT engine_id FROM engines WHERE data_store_id = ? AND engine_id != ?",
            (data_store_id, exclude_engine_id)
        )
        rows = cursor.fetchall()
        return [row['engine_id'] for row in rows]
    
def get_document_gcs_uris_by_engine(engine_id: str) -> List[str]:
    """
    Get all GCS URIs for documents associated with an engine.
    
    Args:
        engine_id: The engine ID
    
    Returns:
        List of GCS URIs
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT gcs_uri FROM documents WHERE engine_id = ?",
            (engine_id,)
        )
        rows = cursor.fetchall()
        return [row['gcs_uri'] for row in rows if row['gcs_uri']]
    

def delete_documents_table():
    """
    Drops the 'documents' table from the database if it exists.
    """
    try:
        # get_db_connection() is expected to return a connection object 
        # that supports the context manager (the 'with' statement).
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Execute the DROP TABLE command
            cursor.execute("DROP TABLE IF EXISTS documents")
            
            print("Successfully dropped table 'documents'.")
            
    except Exception as e:
        # Handle potential connection or execution errors
        print(f"An error occurred while trying to drop the table: {e}")

# if __name__ == "__main__":
#     delete_documents_table()