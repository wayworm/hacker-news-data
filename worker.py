import time
import json
import requests
import psycopg2
from psycopg2.extras import execute_values
import os
import sys

# --- Configuration ---
DB_URI = "postgresql://myuser:mypassword@localhost:5432/hacker_news"
REQUEST_DELAY = 0.02
BATCH_SIZE = 1000
PROGRESS_UPDATE_INTERVAL = 500

# --- Custom Logging ---
def log(worker_id, message):
    """Custom log function to ensure immediate, unbuffered output from workers."""
    log_message = f"[Worker {worker_id}, PID: {os.getpid()}] {message}\n"
    # Write directly to the standard output stream and flush it immediately.
    sys.stdout.write(log_message)
    sys.stdout.flush()

# --- API Fetching ---
def fetch_item(item_id):
    """Fetches a single item by its ID from the Hacker News API."""
    url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return None

# --- Data Storage (Optimized) ---
def store_batch(conn, items_batch, worker_id):
    """Stores a batch of items using a highly efficient, direct database insert."""
    if not items_batch: return
    columns = ['id', 'type', 'by', 'time', 'text', 'url', 'title', 'score', 'descendants', 'parent', 'kids', 'deleted', 'dead']
    values_to_insert = [tuple(json.dumps(item.get(col)) if col == 'kids' and item.get(col) else item.get(col) for col in columns) for item in items_batch]
    with conn.cursor() as cursor:
        try:
            insert_query = f"INSERT INTO items ({', '.join(columns)}) VALUES %s ON CONFLICT (id) DO NOTHING;"
            execute_values(cursor, insert_query, values_to_insert)
            conn.commit()
        except Exception as e:
            log(worker_id, f"DB Error during batch insert: {e}")
            conn.rollback()

# --- Job Management ---
def claim_job(conn, worker_id):
    """Atomically finds and claims a 'pending' job from the database."""
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE job_chunks SET status = 'in_progress', worker_id = %s, updated_at = NOW()
            WHERE id = (
                SELECT id FROM job_chunks WHERE status = 'pending' ORDER BY start_id
                FOR UPDATE SKIP LOCKED LIMIT 1
            ) RETURNING id, start_id, end_id;
        """, (worker_id,))
        job = cursor.fetchone()
        conn.commit()
        if job:
            return {'id': job[0], 'start_id': job[1], 'end_id': job[2]}
    return None

def complete_job(conn, job_id):
    """Marks a job as 'completed' in the database."""
    with conn.cursor() as cursor:
        cursor.execute("UPDATE job_chunks SET status = 'completed', updated_at = NOW() WHERE id = %s;", (job_id,))
        conn.commit()

# --- Main Worker Logic ---
def worker_main(worker_id):
    """A worker process that continuously claims and executes jobs."""
    jobs_completed = 0
    total_items = 0

    log(worker_id, "Starting up.")
    conn = None
    try:
        conn = psycopg2.connect(DB_URI)
        while True:
            job = claim_job(conn, worker_id)
            if job is None:
                log(worker_id, "No more jobs to claim. Exiting.")
                break

            log(worker_id, f"▶️ Claimed job {job['id']}. Range: {job['start_id']} to {job['end_id']}.")
            items_batch = []
            items_in_job = 0
            
            for item_id in range(job['start_id'], job['end_id'] + 1):
                data = fetch_item(item_id)
                if data:
                    items_batch.append(data)
                    items_in_job += 1
                
                current_progress = item_id - job['start_id']
                if current_progress > 0 and current_progress % PROGRESS_UPDATE_INTERVAL == 0:
                    log(worker_id, f"...Job {job['id']} progress: at item {item_id}")

                if len(items_batch) >= BATCH_SIZE or (item_id == job['end_id'] and items_batch):
                    store_batch(conn, items_batch, worker_id)
                    items_batch = [] 

                time.sleep(REQUEST_DELAY)
            
            complete_job(conn, job['id'])
            jobs_completed += 1
            total_items += items_in_job
            log(worker_id, f"✅ Completed job {job['id']}. Processed {items_in_job} new items in this job.")

    except Exception as e:
        log(worker_id, f"An unhandled error occurred: {e}")
    finally:
        if conn:
            conn.close()
        log(worker_id, f"⏹️ Shutting down. Completed {jobs_completed} jobs and processed {total_items} items.")
