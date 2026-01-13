from datetime import datetime

from flask import Flask, render_template, request, jsonify
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text

import helper.config as config
from helper.paths import get_cache_path, get_image_path

matplotlib.use("Agg")

app = Flask(__name__)

env_vars = config.get_db_config()

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
    # Removing unsafe character
    s = s.replace("'", " ")

    parts = [p for p in s.strip().split() if p]
    if len(parts) == 1:
        return parts[0]
    return " & ".join(parts)


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


def get_baseline(time_bin, refresh=False):
    """Load or create baseline data"""
    baseline_cache = get_cache_path(f"baseline_{time_bin}.csv")
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
    cache_filename = get_cache_path(f"{keyword}_{time_bin}_aggregated.csv")
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


@app.route("/users", methods=["GET", "POST"])
def users():
    if request.method == "GET":
        return render_template("users.html")

    try:
        data = request.json
        item_type = data.get("itemType", "all")
        limit = data.get("limit", 100)
        refresh = data.get("refresh", False)

        cache_filename = get_cache_path(f"top_users_{item_type}_top{limit}.csv")

        if not refresh and cache_filename.exists():
            df = pd.read_csv(cache_filename)
        else:
            if item_type == "all":
                type_filter = "type IN ('story', 'comment')"
            elif item_type in ["story", "comment"]:
                type_filter = f"type = '{item_type}'"
            else:
                return jsonify({"success": False, "error": "Invalid item type"}), 400

            query = text(
                f"""
                SELECT
                    by AS username,
                    COUNT(*) AS total_posts,
                    SUM(score) AS cumulative_score,
                    ROUND(AVG(score)::numeric, 2) AS avg_score,
                    MAX(score) AS top_post_score,
                    MIN(to_timestamp(time)) AS first_post_date,
                    MAX(to_timestamp(time)) AS last_post_date
                FROM items
                WHERE by IS NOT NULL
                    AND by != ''
                    AND {type_filter}
                    AND score IS NOT NULL
                GROUP BY by
                HAVING SUM(score) > 0
                ORDER BY cumulative_score DESC
                LIMIT :limit
            """
            )

            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"limit": limit})

            df.to_csv(cache_filename, index=False)

        images = []
        leaderboard_count = min(20, len(df))
        df_plot = df.head(leaderboard_count).copy()

        plt.figure(figsize=(12, 8))
        colors = plt.cm.viridis(range(len(df_plot)))
        plt.barh(df_plot["username"], df_plot["cumulative_score"], color=colors)
        plt.xlabel("Cumulative Score")
        plt.ylabel("Username")
        plt.title(f"Top {leaderboard_count} Users by Cumulative Score")
        plt.gca().invert_yaxis()
        plt.grid(axis="x", alpha=0.3)
        plt.tight_layout()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename1 = f"leaderboard_{timestamp}.png"
        filepath1 = get_image_path(filename1)
        plt.savefig(filepath1, dpi=150, bbox_inches="tight")
        plt.close()
        images.append(f"/static/images/{filename1}")

        scatter_count = min(limit, len(df))
        df_plot2 = df.head(scatter_count).copy()
        plt.figure(figsize=(12, 8))
        scatter = plt.scatter(
            df_plot2["total_posts"],
            df_plot2["cumulative_score"],
            c=df_plot2["avg_score"],
            cmap="plasma",
            s=100,
            alpha=0.6,
            edgecolors="black",
            linewidth=0.5,
        )

        for idx, row in df_plot2.head(10).iterrows():
            plt.annotate(
                row["username"],
                (row["total_posts"], row["cumulative_score"]),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=8,
                alpha=0.7,
            )

        plt.colorbar(scatter, label="Average Score")
        plt.xlabel("Total Posts")
        plt.ylabel("Cumulative Score")
        plt.title(f"Quality vs Quantity: Top {scatter_count} Users")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        filename2 = f"quality_vs_quantity_{timestamp}.png"
        filepath2 = get_image_path(filename2)
        plt.savefig(filepath2, dpi=150, bbox_inches="tight")
        plt.close()
        images.append(f"/static/images/{filename2}")

        users_list = df.to_dict("records")

        return jsonify({"success": True, "images": images, "users": users_list})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/analyse", methods=["POST"])
def analyse():
    try:
        data = request.json
        keywords_raw = [k.strip() for k in data["keywords"].split(",") if k.strip()]
        time_bin = data["timeBin"]
        rolling = int(data["rolling"])
        refresh = data.get("refresh", False)

        keyword_queries = {}
        for kw in keywords_raw:
            kw_lower = kw.lower()
            if kw_lower in KEYWORD_QUERIES:
                keyword_queries[kw] = KEYWORD_QUERIES[kw_lower]
            else:
                keyword_queries[kw] = sanitize_tsquery(kw_lower)

        df_baseline = get_baseline(time_bin, refresh)

        plt.figure(figsize=(12, 6))

        results = []
        for keyword, tsquery in keyword_queries.items():
            df_grouped = query_keyword(keyword, tsquery, time_bin, refresh)

            if df_grouped.empty:
                results.append({"keyword": keyword, "status": "no_data"})
                continue

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
                {"keyword": keyword, "status": "success", "points": len(df_grouped)}
            )

        keywords_raw.sort()

        plt.xlabel("Time")
        plt.ylabel("Mentions per 100 comments & posts")
        plt.title(f"Posts about {', '.join(keywords_raw)}")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"plot_{timestamp}_{', '.join(keywords_raw)}.png"
        filepath = get_image_path(filename)
        plt.savefig(filepath, dpi=150)
        plt.close()

        return jsonify(
            {"success": True, "image": f"/static/images/{filename}", "results": results}
        )

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)