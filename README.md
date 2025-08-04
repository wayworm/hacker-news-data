# High-Performance Hacker News Downloader

This project is a high-performance, parallelized data scraper designed to download the entire history of Hacker News items (stories, comments, jobs, etc.) and store them in a local PostgreSQL database. It uses a resilient dispatcher/worker architecture to ensure fast, efficient, and fault-tolerant downloading.

## Features

-   **Massively Parallel:** Uses Python's `multiprocessing` to run multiple worker processes, maximizing CPU and network usage.
-   **Asynchronous Workers:** Each worker uses `asyncio` and `aiohttp` to handle hundreds of concurrent API requests, dramatically increasing download speed.
-   **Resilient Job Queue:** A PostgreSQL-backed job queue ensures that if the script is stopped or crashes, it can resume exactly where it left off with no data loss or duplication.
-   **Efficient Database Storage:** Uses highly optimized, batched database inserts (`asyncpg`) to handle a high volume of writes without overwhelming the database.
-   **Real-time Monitoring:** The dispatcher provides a live, updating progress bar showing the percentage of data chunks completed.
-   **Dockerized Database:** The PostgreSQL database runs in a Docker container for easy setup, portability, and cleanup.

---
## Architecture

The system is built on a dispatcher/worker model:

1.  **`docker-compose.yml`**: Defines and runs the PostgreSQL database service in a Docker container, ensuring a consistent and isolated environment.
2.  **`dispatcher.py`**: The main control script. On its first run, it populates a `job_chunks` table in the database with the entire range of Hacker News item IDs to be downloaded. It then launches a pool of worker processes and monitors the overall progress.
3.  **`worker.py`**: The workhorse. Each worker process connects to the database, atomically claims a "chunk" of work from the `job_chunks` table, and then uses asynchronous requests to download all items in that range concurrently. The results are written to the database in large, efficient batches.

---
## Prerequisites

Before you begin, ensure you have the following installed:

-   **Docker Desktop**: To run the PostgreSQL database container.
-   **Python 3.10+**: For running the scripts.
-   **Git**: For cloning the repository.

---
## Setup and Installation

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/wayworm/hacker-news-data
    cd hacker-news-data
    ```

2.  **Install Python Dependencies**
    It's recommended to use a virtual environment.
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    pip install requests psycopg2-binary aiohttp asyncpg
    ```

3.  **Start the Database Server**
    This command will download the PostgreSQL image and start the database container in the background.
    ```bash
    docker-compose up -d
    ```

---
## How to Run

The entire process is managed by the dispatcher script.

1.  **Start the Download**
    From the project directory, run the dispatcher. It will automatically populate the job queue on the first run and then launch the workers.
    ```bash
    python dispatcher.py
    ```

2.  **Resetting the Database (Optional)**
    If you want to start the download from scratch and delete all existing data, use the `--reset-db` flag.
    ```bash
    python dispatcher.py --reset-db
    ```

---
## Configuration

You can tune the performance by adjusting the constants at the top of the `dispatcher.py` and `worker.py` files.

-   **In `dispatcher.py`:**
    -   `NUM_WORKERS`: The number of worker processes to launch on the main PC. A good starting point is 1.5x the number of your CPU cores.
    -   `CHUNK_SIZE`: The number of item IDs in each job. Larger chunks mean less job management overhead.

-   **In `worker.py`:**
    -   `CONCURRENT_REQUESTS`: The number of API requests a single async worker will make simultaneously. This is the most powerful dial for performance. `300` is a good, aggressive value.
    -   `BATCH_SIZE`: The number of downloaded items to collect in memory before writing them to the database in a single batch. Larger batches are more efficient.

---
## Database Management

You can connect to your database to view the data using any standard SQL client, like DBeaver.

-   **Connection Details:**
    -   **Host**: `localhost`
    -   **Port**: `5432`
    -   **Database**: `hacker_news`
    -   **Username**: `myuser`
    -   **Password**: `mypassword`

-   **Example Query: Get Child Comments**
    To find the direct children of a story (e.g., item ID `363`), you can use the following query, which correctly handles the `kids` JSONB column:
    ```sql
    SELECT *
    FROM public.items
    WHERE id IN (
        SELECT
            value::bigint
        FROM
            public.items,
            jsonb_array_elements_text(kids)
        WHERE
            id = 363
            AND kids IS NOT NULL
    );
    ```

