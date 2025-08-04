import multiprocessing
import requests
import psycopg2
from psycopg2.extras import execute_values
import time
import sys

# Import the worker function
from worker import worker_main

# --- Configuration ---
NUM_WORKERS = 10
CHUNK_SIZE = 1000
DB_URI = "postgresql://myuser:mypassword@localhost:5432/hacker_news"
STALE_JOB_TIMEOUT_MINUTES = 15
# How often (in seconds) the progress percentage is updated
PROGRESS_UPDATE_INTERVAL = 4

def log(message):
    """Custom log function for the dispatcher to ensure immediate output."""
    # The extra newline at the start ensures this log appears on a new line
    # separate from the updating progress percentage.
    print(f"\n{message}", flush=True)

def get_db_connection():
    """Establishes a new database connection."""
    return psycopg2.connect(DB_URI)

def setup_database(reset=False):
    """Ensures tables exist. If reset is True, drops them first."""
    conn = get_db_connection()
    with conn.cursor() as cursor:
        if reset:
            print("⚠️ Resetting database: Dropping existing tables...", flush=True)
            cursor.execute("DROP TABLE IF EXISTS items, job_chunks;")

        # Create tables
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
        print(f"\nError fetching max ID: {e}", flush=True)
        return None

def populate_job_chunks():
    """Populates the job_chunks table if it's empty."""
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM job_chunks;")
        if cursor.fetchone()[0] > 0:
            return

        print("\nJob queue is empty. Populating now...", flush=True)
        max_id = fetch_max_id()
        if max_id is None:
            print("\nCannot populate jobs without a max_id.", flush=True)
            return

        chunks_to_insert = []
        for i in range(1, max_id, CHUNK_SIZE):
            chunks_to_insert.append((i, min(i + CHUNK_SIZE - 1, max_id)))

        print(f"\nInserting {len(chunks_to_insert)} job chunks...", flush=True)
        execute_values(cursor, "INSERT INTO job_chunks (start_id, end_id) VALUES %s;", chunks_to_insert)
        conn.commit()
    conn.close()
    print("\nJob queue population complete.", flush=True)

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
    should_reset_db = '--reset-db' in sys.argv

    print("--- Dispatcher Starting ---", flush=True)
    setup_database(reset=should_reset_db)
    
    if not should_reset_db:
        reset_stale_jobs()

    populate_job_chunks()

    worker_ids = list(range(NUM_WORKERS))
    log(f"Launching {NUM_WORKERS} workers...")

    # The maxtasksperchild argument helps with memory management and logging.
    with multiprocessing.Pool(processes=NUM_WORKERS, maxtasksperchild=1) as pool:
        # Use map_async to run workers in the background without blocking the dispatcher.
        worker_result = pool.map_async(worker_main, worker_ids)

        # --- Monitoring Loop ---
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM job_chunks;")
                total_jobs = cursor.fetchone()[0]

                if total_jobs == 0:
                    log("No jobs found to monitor.")
                else:
                    # Loop until all workers have finished their tasks.
                    while not worker_result.ready():
                        cursor.execute("SELECT COUNT(*) FROM job_chunks WHERE status = 'completed';")
                        completed_jobs = cursor.fetchone()[0]
                        
                        percentage = (completed_jobs / total_jobs) * 100
                        
                        # The '\r' character moves the cursor to the beginning of the line,
                        # and end='' prevents a newline, effectively overwriting the previous output.
                        print(f"\rProgress: {percentage:.2f}% ({completed_jobs}/{total_jobs} chunks complete)", end="", flush=True)
                        
                        time.sleep(PROGRESS_UPDATE_INTERVAL)
        finally:
            conn.close()

    # Final print to ensure it shows 100% and a clean newline at the end.
    print(f"\rProgress: 100.00% ({total_jobs}/{total_jobs} chunks complete)      ")
    log("--- All workers have finished. Dispatcher shutting down. ---")
