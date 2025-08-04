import multiprocessing
import requests
import psycopg2
from psycopg2.extras import execute_values
import time
import sys

from worker import worker_main

# --- Configuration ---
NUM_WORKERS = 15
CHUNK_SIZE = 200
DB_URI = "postgresql://myuser:mypassword@localhost:5432/hacker_news"
STALE_JOB_TIMEOUT_MINUTES = 15

def log(message):
    """Custom log function for the dispatcher to ensure immediate output."""
    print(message, flush=True)

def get_db_connection():
    """Establishes a new database connection."""
    return psycopg2.connect(DB_URI)

def setup_database(reset=False):
    """Ensures tables exist. If reset is True, drops them first."""
    conn = get_db_connection()
    with conn.cursor() as cursor:
        if reset:
            log("⚠️ Resetting database: Dropping existing tables...")
            cursor.execute("DROP TABLE IF EXISTS items, job_chunks;")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id BIGINT PRIMARY KEY, type TEXT, by TEXT, time BIGINT, text TEXT,
            url TEXT, title TEXT, score INTEGER, descendants INTEGER,
            parent BIGINT, kids JSONB, deleted BOOLEAN, dead BOOLEAN
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_chunks (
            id SERIAL PRIMARY KEY, start_id BIGINT NOT NULL, end_id BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', worker_id INTEGER,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_chunks_status ON job_chunks(status);")
    conn.commit()
    conn.close()

def fetch_max_id():
    """Fetches the latest item ID from the Hacker News API."""
    try:
        response = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json", timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        log(f"Error fetching max ID: {e}")
        return None

def populate_job_chunks():
    """Populates the job_chunks table if it's empty."""
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM job_chunks;")
        if cursor.fetchone()[0] > 0:
            log("Job queue is already populated. Skipping.")
            conn.close()
            return

        log("Job queue is empty. Populating now...")
        max_id = fetch_max_id()
        if max_id is None:
            log("Cannot populate jobs without a max_id.")
            conn.close()
            return

        chunks_to_insert = []
        for i in range(1, max_id, CHUNK_SIZE):
            chunks_to_insert.append((i, min(i + CHUNK_SIZE - 1, max_id)))

        log(f"Inserting {len(chunks_to_insert)} job chunks...")
        execute_values(cursor, "INSERT INTO job_chunks (start_id, end_id) VALUES %s;", chunks_to_insert)
        conn.commit()
    conn.close()
    log("Job queue population complete.")

def reset_stale_jobs():
    """Resets jobs that were 'in_progress' for too long."""
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE job_chunks SET status = 'pending', worker_id = NULL
            WHERE status = 'in_progress' AND updated_at < NOW() - INTERVAL '%s minutes';
        """, (STALE_JOB_TIMEOUT_MINUTES,))
        if cursor.rowcount > 0:
            log(f"Reset {cursor.rowcount} stale jobs.")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    should_reset_db = '--reset-db' in sys.argv or "--reset" in sys.argv

    log("--- Dispatcher Starting ---")
    setup_database(reset=should_reset_db)
    
    if not should_reset_db:
        reset_stale_jobs()

    populate_job_chunks()

    worker_ids = list(range(1,NUM_WORKERS+1))
    log(f"Launching {NUM_WORKERS} workers...")

    # THE CRITICAL FIX IS HERE: maxtasksperchild=1
    # This forces each worker process to exit and restart after completing one
    # job, which forces its output buffers to be flushed to the console.
    with multiprocessing.Pool(processes=NUM_WORKERS, maxtasksperchild=1) as pool:
        pool.map(worker_main, worker_ids)

    log("--- All workers have finished. Dispatcher shutting down. ---")
