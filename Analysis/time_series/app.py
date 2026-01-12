from datetime import datetime

from flask import Flask, render_template, request, jsonify
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from pathlib import Path

import helper.config as config

matplotlib.use("Agg")

app = Flask(__name__)

env_vars = config.get_db_config()

# --- CONFIGURE CONNECTION ---
DB_USER = env_vars["user"]
DB_PASSWORD = env_vars["password"]
DB_HOST = env_vars["host"]
DB_PORT = env_vars["port"]
DB_NAME = env_vars["db"]

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    connect_args={"options": "-c statement_timeout=0"},
)


def sanitize_tsquery(s: str) -> str:
    """Convert user-entered keyword into a valid tsquery string."""
    # Remove unsafe characters
    s = s.replace("'", " ")
    # Multi-word keywords become AND expressions
    parts = [p for p in s.strip().split() if p]
    if len(parts) == 1:
        return parts[0]
    return " & ".join(parts)


# --- KEYWORD QUERY CONFIGURATIONS ---
KEYWORD_QUERIES = {
    "go": "golang | (go <-> lang) | (go <2> (programming | language | goroutine | channel | concurrency))",
    "rust": "rust & (programming | language | cargo | rustc | crate) & !corrosion & !metal",
    "c": "((c <-> programming) | (c <-> language) | (c <-> code)) & !vitamin & !temperature",
    "r": "(r <-> (language | programming | statistical | ggplot | dplyr | cran))",
    "scala": "scala & (programming | language | jvm | akka) & !opera",
    "dart": "dart & (flutter | programming | language | google) & !game & !arrow",
    "python": "python",
    "javascript": "javascript | js",
    "typescript": "typescript",
    "kotlin": "kotlin",
    "swift": "swift & (programming | ios | apple | language)",
    "java": "java & programming",
    "ruby": "ruby & (programming | rails | gem)",
    "php": "php",
    "haskell": "haskell",
    "elixir": "elixir & (programming | erlang | phoenix)",
    "clojure": "clojure",
    "julia": "julia & (programming | language | scientific)",
    "react": "react & (javascript | component | jsx | hook)",
    "vue": "vue & (javascript | vuejs | framework)",
    "angular": "angular & (javascript | typescript | framework)",
    "django": "django",
    "flask": "flask & python",
    "rails": "rails & ruby",
    "spring": "spring & java",
    "spark": "spark & (apache | hadoop | data | scala)",
    "beam": "beam & (apache | dataflow | pipeline)",
}

BASE_DIR = Path(__file__).parent.resolve()

cache_dir = BASE_DIR / "cache"
cache_dir.mkdir(exist_ok=True)
image_dir = BASE_DIR / "static" / "images"
image_dir.mkdir(parents=True, exist_ok=True)


def get_baseline(time_bin, refresh=False):
    """Load or create baseline data"""
    baseline_cache = cache_dir / f"baseline_{time_bin}.csv"
    time_bin_map = {"D": "day", "W": "week", "ME": "month"}
    sql_time_bin = time_bin_map[time_bin]

    if not refresh and baseline_cache.exists():
        return pd.read_csv(baseline_cache, index_col=0, parse_dates=True)

    query_baseline = text(
        """
        SELECT
            date_trunc(:time_bin, to_timestamp(time)) AS time_period,
            COUNT(*) AS total_items
        FROM items
        GROUP BY time_period
        ORDER BY time_period ASC
    """
    )

    with engine.connect() as conn:
        df_baseline = pd.read_sql(
            query_baseline,
            conn,
            params={"time_bin": sql_time_bin},
            index_col="time_period",
        )

    df_baseline.to_csv(baseline_cache)
    return df_baseline


def query_keyword(keyword, tsquery, time_bin, refresh=False):
    """Query or load cached data for a keyword"""
    cache_filename = cache_dir / f"{keyword}_{time_bin}_aggregated.csv"
    time_bin_map = {"D": "day", "W": "week", "ME": "month"}
    sql_time_bin = time_bin_map[time_bin]

    if not refresh and cache_filename.exists():
        return pd.read_csv(cache_filename, index_col=0, parse_dates=True)

    query = text(
        """
        SELECT
            date_trunc(:time_bin, to_timestamp(time)) AS time_period,
            COUNT(*) AS post_count
        FROM items
        WHERE text_search_vector @@ to_tsquery('english', :tsquery)
        GROUP BY time_period
        ORDER BY time_period ASC
    """
    )

    with engine.connect() as conn:
        conn.execute(text("SET max_parallel_workers_per_gather = 4"))
        conn.execute(text("SET parallel_setup_cost = 1000"))
        conn.execute(text("SET parallel_tuple_cost = 0.01"))

        df_grouped = pd.read_sql(
            query,
            conn,
            params={"tsquery": tsquery, "time_bin": sql_time_bin},
            index_col="time_period",
        )

    if not df_grouped.empty:
        df_grouped.to_csv(cache_filename)

    return df_grouped


@app.route("/")
def index():
    return render_template(
        "index.html", predefined_keywords=list(KEYWORD_QUERIES.keys())
    )


@app.route("/analyse", methods=["POST"])
def analyse():
    try:
        data = request.json
        keywords_raw = [k.strip()
                        for k in data["keywords"].split(",") if k.strip()]
        time_bin = data["timeBin"]
        rolling = int(data["rolling"])
        refresh = data.get("refresh", False)

        # Map keywords to queries
        keyword_queries = {}
        for kw in keywords_raw:
            kw_lower = kw.lower()
            if kw_lower in KEYWORD_QUERIES:
                keyword_queries[kw] = KEYWORD_QUERIES[kw_lower]
            else:
                keyword_queries[kw] = sanitize_tsquery(kw_lower)

        # Get baseline
        df_baseline = get_baseline(time_bin, refresh)

        results = []
        for keyword, tsquery in keyword_queries.items():
            df_grouped = query_keyword(keyword, tsquery, time_bin, refresh)

            if df_grouped.empty:
                results.append({"keyword": keyword, "status": "no_data"})
                continue

            # Normalize
            df_norm = df_grouped.join(df_baseline, how="left")
            df_norm["normalised"] = df_norm["post_count"] / df_norm["total_items"]
            df_norm["scaled"] = df_norm["normalised"] * 100

            if rolling > 0:
                df_norm["scaled_rolled"] = df_norm["scaled"].rolling(rolling).mean()
                plt.plot(
                    df_norm.index,
                    df_norm["scaled_rolled"],
                    label=f"{keyword} ({rolling}-period avg)",
                )
            else:
                plt.plot(
                    df_norm.index, df_norm["scaled"], label=f"{keyword} (per 100 posts)"
                )

            results.append(
                {"keyword": keyword, "status": "success",
                 "points": len(df_grouped)}
            )

        # plot
        plt.figure(figsize=(12, 6))
        plt.xlabel("Time")
        plt.ylabel("Mentions per 100 comments & posts")
        plt.title(f"Posts about {', '.join(keywords_raw)}")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        # Save plot
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        keywords_raw.sort()
        filename = f"plot_{timestamp}_{', '.join(keywords_raw)}.png"
        filepath = image_dir / filename
        plt.savefig(filepath, dpi=150)
        plt.close()

        return jsonify(
            {"success": True,
             "image": f"/static/images/{filename}", "results": results}
        )

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
