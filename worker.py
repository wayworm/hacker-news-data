import time
import json
import os
import sys
import asyncio
import aiohttp
import asyncpg

# --- Configuration ---
# NOTE: The DSN format for asyncpg is slightly different.
DB_URI = "postgresql://myuser:mypassword@localhost:5432/hacker_news"
# How many items a single worker will try to download at the same time.
# This is the most important dial for performance. Start around 200-300.
CONCURRENT_REQUESTS = 300
# The batch size for DB writes should be large.
BATCH_SIZE = 1000

# --- Custom Logging ---
def log(worker_id, message):
    """Custom log function to ensure immediate, unbuffered output from workers."""

    if "--log" in sys.argv:
        log_message = f"[Worker {worker_id}, PID: {os.getpid()}] {message}\n"
        sys.stdout.write(log_message)
        sys.stdout.flush()
    
    return None

   

# --- Asynchronous API Fetching ---
async def fetch_item_async(session, item_id):
    """Asynchronously fetches a single item, returning the JSON data or None."""
    url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
    try:
        # Use aiohttp's ClientSession to make the request
        async with session.get(url, timeout=10) as response:
            response.raise_for_status()
            return await response.json()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        # Silently ignore failed requests for single items
        return None

# --- Asynchronous Data Storage ---
async def store_batch_async(conn, items_batch, worker_id):
    """Asynchronously stores a batch of items using asyncpg for high performance."""
    if not items_batch: return
    columns = ['id', 'type', 'by', 'time', 'text', 'url', 'title', 'score', 'descendants', 'parent', 'kids', 'deleted', 'dead']
    
    # Prepare data, ensuring kids is a JSON string
    values_to_insert = [
        tuple(json.dumps(item.get(col)) if col == 'kids' and item.get(col) else item.get(col) for col in columns)
        for item in items_batch if item and 'id' in item
    ]
    
    if not values_to_insert: return

    try:
        # asyncpg's executemany is highly optimized for bulk inserts.
        await conn.executemany(
            f"INSERT INTO items ({', '.join(columns)}) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT (id) DO NOTHING",
            values_to_insert
        )
    except Exception as e:
        log(worker_id, f"DB Error during batch insert: {e}")

# --- Asynchronous Job Management ---
async def claim_job_async(conn, worker_id):
    """Atomically finds and claims a 'pending' job from the database."""
    # Use a transaction to ensure atomicity
    async with conn.transaction():
        job = await conn.fetchrow("""
            UPDATE job_chunks SET status = 'in_progress', worker_id = $1, updated_at = NOW()
            WHERE id = (
                SELECT id FROM job_chunks WHERE status = 'pending' ORDER BY start_id
                FOR UPDATE SKIP LOCKED LIMIT 1
            ) RETURNING id, start_id, end_id;
        """, worker_id)
    if job:
        return {'id': job['id'], 'start_id': job['start_id'], 'end_id': job['end_id']}
    return None

async def complete_job_async(conn, job_id):
    """Marks a job as 'completed' in the database."""
    await conn.execute("UPDATE job_chunks SET status = 'completed', updated_at = NOW() WHERE id = $1;", job_id)

# --- Main Asynchronous Worker Logic ---
async def worker_main_async(worker_id):
    """The core async worker function."""
    log(worker_id, "Starting up.")
    conn = await asyncpg.connect(DB_URI)
    
    try:
        # Create a single, reusable aiohttp session for connection pooling
        async with aiohttp.ClientSession() as session:
            while True:
                job_start_time = time.monotonic() # Record time when job is claimed
                job = await claim_job_async(conn, worker_id)
                if job is None:
                    log(worker_id, "No more jobs to claim. Exiting.")
                    break

                log(worker_id, f"▶️ Claimed job {job['id']}. Range: {job['start_id']} to {job['end_id']}.")
                
                # Create a list of all fetch tasks for the current job
                tasks = [fetch_item_async(session, item_id) for item_id in range(job['start_id'], job['end_id'] + 1)]
                
                results = []
                # Process tasks in chunks to avoid overwhelming memory
                for i in range(0, len(tasks), BATCH_SIZE):
                    task_chunk = tasks[i:i+BATCH_SIZE]
                    # asyncio.gather runs all tasks in the chunk concurrently
                    log(worker_id, f"...Job {job['id']} progress: fetching items {i} to {i+len(task_chunk)-1}")
                    chunk_results = await asyncio.gather(*task_chunk)
                    
                    # Filter out None results from failed fetches
                    valid_results = [res for res in chunk_results if res]
                    if valid_results:
                        await store_batch_async(conn, valid_results, worker_id)
                        results.extend(valid_results)

                await complete_job_async(conn, job['id'])
                
                # --- Performance Calculation ---
                job_end_time = time.monotonic()
                duration_seconds = job_end_time - job_start_time
                items_processed = len(results)
                records_per_minute = 0
                if duration_seconds > 0:
                    records_per_minute = (items_processed / duration_seconds) * 60

                log(worker_id, f"✅ Completed job {job['id']}. Processed {items_processed} items. Rate: {records_per_minute:.2f} items/min.")

    except Exception as e:
        log(worker_id, f"An unhandled error occurred: {e}")
    finally:
        await conn.close()
        log(worker_id, f"⏹️ Shutting down.")

def worker_main(worker_id):
    """
    A simple synchronous wrapper to launch the async worker.
    This is what the multiprocessing Pool will call.
    """
    try:
        asyncio.run(worker_main_async(worker_id))
    except KeyboardInterrupt:
        pass
