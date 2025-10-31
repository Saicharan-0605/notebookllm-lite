import sqlite3
from contextlib import contextmanager
from typing import Optional,List

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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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


def save_document_to_db(
    engine_id: str,
    data_store_id: str,
    filename: str,
    gcs_uri: str,
    file_size: int,
    content_type: str
) -> int:
    """
    Save uploaded document information to the database.
    
    Returns:
        Row ID of the inserted record
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO documents (engine_id, data_store_id, filename, gcs_uri, file_size, content_type)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (engine_id, data_store_id, filename, gcs_uri, file_size, content_type))
        
        row_id = cursor.lastrowid
        print(f"Document saved to database (ID: {row_id})")
        return row_id

