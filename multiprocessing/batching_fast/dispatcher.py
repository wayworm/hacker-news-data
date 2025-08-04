import multiprocessing
import requests
from sqlalchemy import create_engine, text

# Import only the necessary functions from the worker file
from worker import worker_main

# --- Configuration ---
NUM_WORKERS = 10
CHUNK_SIZE = 200 # Smaller chunk size for more frequent feedback
DB_URI = "postgresql+psycopg2://myuser:mypassword@localhost:5432/hacker_news"

def setup_database():
    """
    Connects to the database and ensures the 'items' table is created
    with the correct data types before any workers start.
    """
    # This engine is used only for setup and is not shared with workers.
    engine = create_engine(DB_URI)
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS items (
            id BIGINT PRIMARY KEY,
            type TEXT,
            by TEXT,
            time BIGINT,
            text TEXT,
            url TEXT,
            title TEXT,
            score INTEGER,
            descendants INTEGER,
            parent BIGINT,
            kids TEXT,
            deleted BOOLEAN,
            dead BOOLEAN
        );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_parent ON items(parent);"))
    engine.dispose()

def fetch_max_id():
    """Fetches the latest item ID from the Hacker News API."""
    try:
        response = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json", timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching max ID: {e}")
        return None

def get_start_id_from_db():
    """Finds the highest ID already in the database to avoid re-downloading."""
    engine = create_engine(DB_URI)
    start_id = 1
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(id) FROM items;")).scalar_one_or_none()
            if result is not None:
                start_id = int(result) + 1
    except Exception as e:
        # This can happen if the table doesn't exist yet. Default to 1.
        print(f"Could not determine start ID from DB (table might be empty): {e}")
    
    engine.dispose()
    return start_id


if __name__ == "__main__":
    # 1. Setup the database schema from a single process first.
    print("Setting up database schema...")
    setup_database()

    # 2. Determine the total work to be done.
    max_id = fetch_max_id()
    if max_id is None:
        exit("Could not fetch max ID. Exiting.")

    start_id = get_start_id_from_db()
    print(f"Starting from ID {start_id}. Total items to fetch: {max_id - start_id}")

    # 3. Divide the work into chunks.
    work_chunks = []
    for i in range(start_id, max_id, CHUNK_SIZE):
        chunk_start = i
        chunk_end = min(i + CHUNK_SIZE - 1, max_id)
        work_chunks.append((chunk_start, chunk_end))

    # 4. Create and run the worker pool.
    # We add a worker_id to each task for better logging.
    tasks = [(chunk[0], chunk[1], i % NUM_WORKERS) for i, chunk in enumerate(work_chunks)]
    
    print(f"Dispatching {len(tasks)} chunks of work to {NUM_WORKERS} workers...")
    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        pool.starmap(worker_main, tasks)

    print("All work has been dispatched and completed.")

