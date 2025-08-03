import requests
import time
import os
import pandas as pd
from sqlalchemy import create_engine, text
import json

# --- Configuration ---
MAX_ITEMS_TO_FETCH = 500  # Set a limit for how many items to fetch in one run
DB_NAME = "hacker_news.db"
LAST_ID_FILE = "last_processed_id.txt"
REQUEST_DELAY = 0.05  # Seconds to wait between API requests

# --- Global State ---
engine = create_engine(f"sqlite:///{DB_NAME}", echo=False)
shutdown_requested = False

# --- Database Setup ---
def setup_database():
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY,
            type TEXT,
            by TEXT,
            time INTEGER,
            text TEXT,
            url TEXT,
            title TEXT,
            score INTEGER,
            descendants INTEGER,
            parent INTEGER,
            kids TEXT, -- Storing list of kids as a JSON string
            deleted BOOLEAN,
            dead BOOLEAN
        );
        """))
        # Add an index on the 'parent' column for faster lookups of children
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_parent ON items(parent);"))

# --- State Management ---
def get_last_processed_id():
    """Reads the last processed item ID from the state file."""
    if not os.path.exists(LAST_ID_FILE):
        return 0
    try:
        with open(LAST_ID_FILE, "r") as f:
            return int(f.readline().strip() or 0)
    except (IOError, ValueError):
        return 0

def save_last_processed_id(item_id):
    """Saves the last processed item ID to the state file."""
    try:
        with open(LAST_ID_FILE, "w") as f:
            f.write(str(item_id))
    except IOError as e:
        print(f"Error saving last processed ID: {e}")

# --- API Fetching ---
def fetch_max_id():
    """Fetches the latest item ID from the Hacker News API."""
    try:
        response = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json", timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching max ID: {e}")
        return None

def fetch_item(item_id):
    """Fetches a single item by its ID from the Hacker News API."""
    url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching item {item_id}: {e}")
        return None

# --- Data Storage ---
def store_item(data):
    """
    Stores any item (story, comment, etc.) in the database.
    It prepares the data to fit the unified 'items' table schema.
    """
    # The 'kids' field is a list, so we serialize it to a JSON string for storage.
    kids_json = json.dumps(data.get("kids")) if data.get("kids") else None

    item_df = pd.DataFrame([{
        "id": data.get("id"),
        "type": data.get("type"),
        "by": data.get("by"),
        "time": data.get("time"),
        "text": data.get("text"),
        "url": data.get("url"),
        "title": data.get("title"),
        "score": data.get("score"),
        "descendants": data.get("descendants"),
        "parent": data.get("parent"),
        "kids": kids_json,
        "deleted": data.get("deleted", False),
        "dead": data.get("dead", False),
    }])

    try:
        # Use 'replace' to handle cases where an item might be updated.
        item_df.to_sql("items", con=engine, if_exists='append', index=False)
    except Exception as e:
        print(f"Database error while storing item {data.get('id')}: {e}")

# --- Main Application Logic ---
def main():
    """Main function to run the data fetching and storing process."""
    setup_database()

    max_id = fetch_max_id()
    if max_id is None:
        print("Could not retrieve max ID. Exiting.")
        return

    start_id = get_last_processed_id()
    if start_id >= max_id:
        print("Database is already up to date.")
        return

    print(f"Starting data fetch from ID {start_id + 1} to {max_id}. Press Ctrl+C to stop.")

    processed_count = 0
    for item_id in range(start_id + 1, max_id + 1):
        if processed_count >= MAX_ITEMS_TO_FETCH:
            print(f"Reached fetch limit of {MAX_ITEMS_TO_FETCH}. Stopping.")
            break

        data = fetch_item(item_id)
        
        # Always save progress, even if the item fetch failed or the item was null.
        # This prevents getting stuck on a deleted or invalid item ID.
        save_last_processed_id(item_id)

        if not data:
            continue  # Skip to the next item

        item_type = data.get("type", "unknown")
        print(f"Processing ID: {item_id}/{max_id} — Type: {item_type}")

        store_item(data)
        processed_count += 1
        time.sleep(REQUEST_DELAY)

    print("Program finished.")

if __name__ == "__main__":
    main()
