import argparse
import logging
from multiprocessing import Pool
from functools import partial
import os
import time

import psycopg2
from psycopg2.extras import execute_values
import requests

from Scraper.worker import run_worker
import helper.config as config

# -- Configuration --

# Argument Parsing


def reset_flag_str_to_bool(value):
    """Convert a string to boolean."""
    return str(value).lower() in ('true', '1')


parser = argparse.ArgumentParser(description="Hacker News Scraper Dispatcher")
parser.add_argument('--log', action='store_true', default=False,
                    help="Enable detailed worker logging")
parser.add_argument('--reset-db', action='store', default=False,
                    help="Reset the database? Accepts True, true, or 1")
parser.add_argument('--num-workers', action='store', type=int, default=8,
                    help="Number of worker processes to launch")
parser.add_argument('--chunk-size', action='store', type=int, default=1000,
                    help="Portion of job queue a worker will reserve."
                    "If excessive, lots of data may be lost during"
                    " interruption.")
parser.add_argument('--batch-size', action='store', type=int, default=250,
                    help="Number of API objects to request "
                    "at once, per worker.")

args = parser.parse_args()

# Convert reset-db argument to boolean
reset_db = reset_flag_str_to_bool(args.reset_db)

NUM_WORKERS = args.num_workers
CHUNK_SIZE = args.chunk_size
BATCH_SIZE = args.batch_size
STALE_JOB_TIMEOUT_MINUTES = 3
STALE_JOB_CHECK_INTERVAL = (STALE_JOB_TIMEOUT_MINUTES * 60)


# How often (in seconds) the progress percentage is updated
PROGRESS_UPDATE_INTERVAL = 4

env_vars = config.get_db_config()

user = env_vars["user"]
password = env_vars["password"]
host = env_vars["host"]
port = env_vars["port"]
db = env_vars["db"]


# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(filename='worker.log', level=logging.INFO)

DB_URI = f"postgresql://{user}:{password}@{host}:{port}/{db}"

if args.log:
    os.environ['ENABLE_LOGGING'] = '1'
else:
    os.environ['ENABLE_LOGGING'] = '0'


def log(message):
    """Custom log function for the dispatcher to ensure immediate output."""
    print(f"\n{message}", flush=True)
    logger.info(message)


def get_db_connection(db_uri):
    """Establishes a new database connection."""
    return psycopg2.connect(db_uri)


def setup_database(reset=False):
    """Ensures tables exist. If reset is True, drops them first."""

    conn = get_db_connection(DB_URI)
    with conn.cursor() as cursor:
        if reset:
            log("Resetting database: Dropping existing tables...")
            cursor.execute("DROP TABLE IF EXISTS items, job_chunks;")

        # Create Items Table with Generated Search Vector
        cursor.execute("""
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
            kids JSONB,
            deleted BOOLEAN,
            dead BOOLEAN,
            text_search_vector tsvector
            GENERATED ALWAYS AS ( to_tsvector('english',
                        coalesce(title, '') || ' ' || coalesce(text, ''))
            ) STORED
        );
        """)

        # Create Job Queue Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_chunks (
            id SERIAL PRIMARY KEY,
            start_id BIGINT NOT NULL,
            end_id BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            worker_id INTEGER,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """)

        # Create Indexes
        cursor.execute("""CREATE INDEX IF NOT EXISTS idx_job_chunks_status
                       ON job_chunks(status);""")

        # GIN Index for the Full-Text Search
        log("Ensuring Full-Text Search index exists...")
        cursor.execute("""CREATE INDEX IF NOT EXISTS idx_items_search_vector
                       ON items USING GIN(text_search_vector);""")

        log("Creating index for user searching")
        cursor.execute("""CREATE INDEX idx_items_user_score_type
                        ON items (by, type, score)
                        WHERE score IS NOT NULL AND by IS NOT NULL;
                        """)

        cursor.execute("CREATE INDEX idx_items_by_time ON items (by, time);")

        cursor.execute("CREATE INDEX idx_items_type ON items (type);")

    conn.commit()
    conn.close()
    log("Database setup complete.")


def fetch_max_id():
    """Fetches the latest item ID from the Hacker News API."""
    try:
        response = requests.get(
            "https://hacker-news.firebaseio.com/v0/maxitem.json",
            timeout=10)

        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"\nError fetching max ID: {e}", flush=True)
        return None


def populate_job_chunks():
    conn = get_db_connection(DB_URI)
    with conn.cursor() as cursor:
        cursor.execute("SELECT COALESCE(MAX(end_id), 0) FROM job_chunks;")
        current_max = cursor.fetchone()[0]

        max_id = fetch_max_id()
        if max_id is None:
            log("Cannot populate jobs without a max_id.")
            conn.close()
            return

        if current_max >= max_id:
            log("No new items to add. Job queue is up to date.")
            conn.close()
            return

        chunks_to_insert = []
        for i in range(current_max + 1, max_id + 1, CHUNK_SIZE):
            chunks_to_insert.append((i, min(i + CHUNK_SIZE - 1, max_id)))

        if chunks_to_insert:
            log(f"Inserting {len(chunks_to_insert)} new job chunks...")
            execute_values(cursor, """INSERT INTO job_chunks (start_id, end_id)
            VALUES %s;""", chunks_to_insert)
            conn.commit()
    conn.close()
    log("Job queue population complete.")


def reset_stale_jobs():
    """Resets jobs that were 'in_progress' for too long."""

    conn = get_db_connection(DB_URI)
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE job_chunks SET status = 'pending', worker_id = NULL
            WHERE status = 'in_progress'
            AND updated_at < NOW() - INTERVAL '%s minutes';
        """, (STALE_JOB_TIMEOUT_MINUTES,))
        if cursor.rowcount > 0:
            log(f"Reset {cursor.rowcount} stale jobs.")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    print("Dispatcher Starting.", flush=True)
    setup_database(reset=reset_db)

    reset_stale_jobs()
    last_stale_check = time.time()

    populate_job_chunks()

    worker_ids = list(range(NUM_WORKERS))
    log(f"Launching {NUM_WORKERS} workers...")

    # The maxtasksperchild argument helps with memory management and logging.
    with Pool(processes=NUM_WORKERS, maxtasksperchild=1) as pool:

        # pass in batch_size
        run_worker = partial(run_worker, batch_size=args.batch_size)

        # Use map_async to run workers in the background
        # without blocking the dispatcher.
        worker_result = pool.map_async(run_worker, worker_ids)

        while not worker_result.ready():
            conn = get_db_connection(DB_URI)
            try:
                with conn.cursor() as cursor:

                    duration = time.time() - last_stale_check

                    # Check for stale jobs only
                    # every STALE_JOB_CHECK_INTERVAL seconds
                    if duration >= STALE_JOB_CHECK_INTERVAL:
                        cursor.execute(f"""
                            UPDATE job_chunks
                            SET status = 'pending', worker_id = NULL
                            WHERE status = 'in_progress'
                            AND updated_at < NOW() - INTERVAL
                            '{STALE_JOB_TIMEOUT_MINUTES} minutes';
                        """)
                        if cursor.rowcount > 0:
                            log(f"Reset {cursor.rowcount} stale jobs"
                                f" during execution.")
                        conn.commit()
                        last_stale_check = time.time()

                    # Always check progress
                    cursor.execute("SELECT COUNT(*) FROM job_chunks;")
                    total_jobs = cursor.fetchone()[0]
                    cursor.execute("""SELECT COUNT(*) FROM job_chunks
                                WHERE status = 'completed';""")
                    completed_jobs = cursor.fetchone()[0]
            finally:
                conn.close()

            percentage = (completed_jobs / total_jobs) * 100
            print(f"\rProgress: {percentage:.2f}% "
                  f"({completed_jobs}/{total_jobs} chunks complete)",
                  end="", flush=True)

            time.sleep(PROGRESS_UPDATE_INTERVAL)

    # Final print to ensure it shows 100%
    print(f"\rProgress: 100.00% ({total_jobs}/{total_jobs} chunks complete)")
    log("--- All workers have finished. Dispatcher shutting down. ---")
