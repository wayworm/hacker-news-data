import time
import pandas as pd
from sqlalchemy import create_engine
import json
import requests

# --- Configuration ---
DB_URI = "postgresql+psycopg2://myuser:mypassword@localhost:5432/hacker_news"
REQUEST_DELAY = 0.05  # We can slightly decrease the delay with batching
BATCH_SIZE = 100      # How many items to collect before writing to DB

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

# --- Data Storage ---
def store_batch(items_batch, engine):
    """
    Stores a batch of items in the database.
    """
    if not items_batch:
        return

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(items_batch)
    
    # Ensure 'kids' is serialized
    if 'kids' in df.columns:
        df['kids'] = df['kids'].apply(lambda k: json.dumps(k) if k else None)

    try:
        df.to_sql("items", con=engine, if_exists='append', index=False, method='multi')
    except Exception as e:
        print(f"Database error during batch insert: {e}")

# --- Main Worker Logic ---
def worker_main(start_id, end_id, worker_id):
    """
    A worker process that fetches items and stores them in batches.
    """
    engine = create_engine(DB_URI)
    items_batch = []
    
    print(f"[Worker {worker_id}] Starting. Range: {start_id} to {end_id}")
    
    for item_id in range(start_id, end_id + 1):
        data = fetch_item(item_id)
        if data:
            # Add the fetched item to our batch list
            items_batch.append(data)

        # If the batch is full, or if this is the last item in the range
        if len(items_batch) >= BATCH_SIZE or item_id == end_id:
            print(f"[Worker {worker_id}] Storing batch of {len(items_batch)} items...")
            store_batch(items_batch, engine)
            items_batch = [] # Reset the batch

        time.sleep(REQUEST_DELAY)

    # Final check to store any remaining items in the batch
    if items_batch:
        print(f"[Worker {worker_id}] Storing final batch of {len(items_batch)} items...")
        store_batch(items_batch, engine)

    engine.dispose()
    print(f"[Worker {worker_id}] Finished its range.")
