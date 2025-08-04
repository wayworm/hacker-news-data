import time
import json
import requests
import psycopg2
from psycopg2.extras import execute_values

# --- Configuration ---
# NOTE: You may need to run 'pip install psycopg2-binary' if you haven't already.
DB_URI = "postgresql://myuser:mypassword@localhost:5432/hacker_news"
# We can be more aggressive with the delay now that DB writes are faster.
# WARNING: A very low delay risks getting your IP address rate-limited or banned.
REQUEST_DELAY = 0.02
# A larger batch size means fewer, more efficient database writes.
BATCH_SIZE = 500

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
def store_batch(conn, items_batch):
    """
    Stores a batch of items using a highly efficient, direct database insert.
    This bypasses the overhead of pandas.to_sql.
    """
    if not items_batch:
        return

    # Define the columns in the order they appear in the database table.
    columns = [
        'id', 'type', 'by', 'time', 'text', 'url', 'title', 'score',
        'descendants', 'parent', 'kids', 'deleted', 'dead'
    ]
    
    # Prepare the data as a list of tuples for execute_values.
    # This is much faster than creating a DataFrame.
    values_to_insert = []
    for item in items_batch:
        # For each item, create a tuple with values in the correct column order.
        # Use .get(key) which safely returns None if a key is missing.
        values_to_insert.append(tuple(
            # Special handling for 'kids' list, which needs to be a JSON string.
            json.dumps(item.get(col)) if col == 'kids' and item.get(col) else item.get(col)
            for col in columns
        ))

    # Use a cursor to execute the command
    with conn.cursor() as cursor:
        try:
            # The ON CONFLICT clause prevents errors if we try to insert a duplicate ID.
            # It simply does nothing instead of crashing the worker.
            insert_query = f"""
                INSERT INTO items ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (id) DO NOTHING;
            """
            # execute_values is the fastest way to bulk-insert in psycopg2.
            execute_values(cursor, insert_query, values_to_insert)
            conn.commit() # Commit the transaction
        except Exception as e:
            print(f"Database error during batch insert: {e}")
            conn.rollback() # Roll back the transaction on error

# --- Main Worker Logic ---
def worker_main(start_id, end_id, worker_id):
    """
    A worker process that fetches items and stores them in batches.
    """
    conn = None
    try:
        # Each worker creates its own direct database connection.
        conn = psycopg2.connect(DB_URI)
        items_batch = []
        
        print(f"[Worker {worker_id}] Starting. Range: {start_id} to {end_id}")
        
        for item_id in range(start_id, end_id + 1):
            data = fetch_item(item_id)
            if data:
                items_batch.append(data)

            if len(items_batch) >= BATCH_SIZE or (item_id == end_id and items_batch):
                store_batch(conn, items_batch)
                items_batch = [] # Reset the batch

            time.sleep(REQUEST_DELAY)

    except Exception as e:
        print(f"[Worker {worker_id}] An error occurred: {e}")
    finally:
        if conn:
            conn.close() # Ensure the connection is always closed.
        print(f"[Worker {worker_id}] Finished its range.")

