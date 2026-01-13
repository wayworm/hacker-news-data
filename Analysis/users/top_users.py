import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from datetime import datetime
import argparse
import logging

import helper.config as config
from helper.paths import get_cache_path, get_image_path

# Q3. **Which users have accumulated the most influence on the platform?**

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

logger = logging.getLogger(__name__)
logging.basicConfig(filename='users.log', level=logging.INFO)


def log(message):
    """Custom log function for the dispatcher to ensure immediate output."""
    print(f"\n{message}", flush=True)
    logger.info(message)


def get_top_users(item_type="all", limit=100, refresh=False):
    """
    Get top users by cumulative score

    Args:
        item_type: 'story', 'comment', or 'all'
        limit: number of top users to return
        refresh: force refresh from database
    """
    cache_filename = get_cache_path(f"top_users_{item_type}_top{limit}.csv")

    if not refresh and cache_filename.exists():
        log(f"Loading cached data from {cache_filename}")
        return pd.read_csv(cache_filename)

    log(f"Querying database for top {limit} users ({item_type})...")

    if item_type == "all":
        type_filter = "type IN ('story', 'comment')"
    elif item_type in ['story', 'cooment']:
        type_filter = f"type = '{item_type}'"
    else:
        return RuntimeError

    query = text(
        f"""
        SELECT
            by AS username,
            COUNT(*) AS total_posts,
            SUM(score) AS cumulative_score,
            ROUND(AVG(score)::numeric, 2) AS avg_score,
            MAX(score) AS top_post_score,
            MIN(to_timestamp(time)) AS first_post_date,
            MAX(to_timestamp(time)) AS last_post_date,
            EXTRACT(EPOCH FROM (MAX(to_timestamp(time)) - MIN(to_timestamp(time)))) / 86400 AS days_active
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

    df["posts_per_day"] = df["total_posts"] / df["days_active"].replace(0, 1)
    df["posts_per_day"] = df["posts_per_day"].round(2)

    df.to_csv(cache_filename, index=False)
    log(f"Cached results to {cache_filename}")

    return df


def user_activity(username, time_bin="month", refresh=False):
    """
    Get a specific user's posting activity over time

    Args:
        username: the user to analyze
        time_bin: 'day', 'week', 'month', or 'year'
        refresh: force refresh from database
    """
    cache_filename = cache_dir / f"user_{username}_{time_bin}_activity.csv"

    if not refresh and cache_filename.exists():
        return pd.read_csv(cache_filename, index_col=0, parse_dates=True)

    query = text(
        """
        SELECT
            date_trunc(:time_bin, to_timestamp(time)) AS time_period,
            COUNT(*) AS post_count,
            SUM(score) AS period_score,
            AVG(score) AS avg_score
        FROM items
        WHERE by = :username
            AND score IS NOT NULL
        GROUP BY time_period
        ORDER BY time_period ASC
    """
    )

    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={"username": username, "time_bin": time_bin},
            index_col="time_period",
        )

    if not df.empty:
        df.to_csv(cache_filename)

    return df


def plot_leaderboard(df, top_n=20, metric="cumulative_score"):
    """Create a horizontal bar chart of top users"""
    df_plot = df.head(top_n).copy()

    plt.figure(figsize=(12, 8))
    colors = plt.cm.viridis(range(len(df_plot)))

    plt.barh(df_plot["username"], df_plot[metric], color=colors)
    plt.xlabel(metric.replace("_", " ").title())
    plt.ylabel("Username")
    plt.title(f'Top {top_n} Users by {metric.replace("_", " ").title()}')
    plt.gca().invert_yaxis()
    plt.grid(axis="x", alpha=0.3)
    plt.tight_layout()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"leaderboard_{metric}_top{top_n}_{timestamp}.png"
    filepath = get_image_path(filename)
    plt.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close()
    log(f"Saved leaderboard to {filepath}")

    return filepath


def plot_quality_vs_quantity(df, top_n=100):
    """Scatter plot: total posts vs cumulative score"""
    df_plot = df.head(top_n).copy()

    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(
        df_plot["total_posts"],
        df_plot["cumulative_score"],
        c=df_plot["avg_score"],
        cmap="plasma",
        s=100,
        alpha=0.6,
        edgecolors="black",
        linewidth=0.5,
    )

    # Label top 10 users
    for idx, row in df_plot.head(10).iterrows():
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
    plt.title(f"Quality vs Quantity: Top {top_n} Users")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"quality_vs_quantity_top{top_n}_{timestamp}.png"
    filepath = image_dir / filename
    plt.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close()
    log(f"Saved scatter plot to {filepath}")

    return filepath


def plot_user_timeline(username, time_bin="month"):
    """Plot a user's activity over time"""
    df = user_activity(username, time_bin)

    if df.empty:
        log(f"No data found for user: {username}")
        return None

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    # Plot 1: Post count over time
    ax1.plot(df.index, df["post_count"], linewidth=2, color="steelblue")
    ax1.fill_between(df.index, df["post_count"], alpha=0.3, color="steelblue")
    ax1.set_ylabel("Post Count")
    ax1.set_title(f"Activity Timeline for User: {username}")
    ax1.grid(True, alpha=0.3)

    # Plot 2: Cumulative and average score
    ax2_twin = ax2.twinx()
    ax2.plot(
        df.index, df["period_score"], linewidth=2, color="green", label="Period Score"
    )
    ax2_twin.plot(
        df.index,
        df["avg_score"],
        linewidth=2,
        color="orange",
        label="Avg Score",
        linestyle="--",
    )

    ax2.set_xlabel("Time")
    ax2.set_ylabel("Period Score", color="green")
    ax2_twin.set_ylabel("Average Score", color="orange")
    ax2.tick_params(axis="y", labelcolor="green")
    ax2_twin.tick_params(axis="y", labelcolor="orange")
    ax2.grid(True, alpha=0.3)

    # Combine legends
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax2_twin.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.tight_layout()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"user_timeline_{username}_{time_bin}_{timestamp}.png"
    filepath = image_dir / filename
    plt.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close()
    log(f"Saved user timeline to {filepath}")

    return filepath


def main():
    parser = argparse.ArgumentParser(description="Analyze top Hacker News users")

    parser.add_argument(
        "--type",
        choices=["all", "story", "comment"],
        default="all",
        help="Type of items to analyze",

    )
    parser.add_argument(
        "--limit", type=int, default=100, help="Number of top users to retrieve"
    )

    parser.add_argument(
        "--refresh", action="store_true", help="Force refresh from database"
    )

    parser.add_argument("--user", type=str, help="Analyze specific user timeline")

    parser.add_argument(
        "--time-bin",
        choices=["day", "week", "month", "year"],
        default="month",
        help="Time bin for user timeline",
    )

    args = parser.parse_args()

    # If analyzing specific user
    if args.user:
        plot_user_timeline(args.user, args.time_bin)
        return

    # Get top users
    df = get_top_users(args.type, args.limit, args.refresh)

    # Display summary statistics
    log("\n" + "=" * 60)
    log(f"TOP {min(20, len(df))} USERS BY CUMULATIVE SCORE ({args.type})")
    log("=" * 60)
    log(
        df[
            [
                "username",
                "total_posts",
                "cumulative_score",
                "avg_score",
                "top_post_score",
            ]
        ]
        .head(20)
        .to_string(index=False)
    )
    log("\n")

    log("\nGenerating visualizations...")
    plot_leaderboard(df, top_n=20, metric="cumulative_score")
    plot_leaderboard(df, top_n=20, metric="total_posts")
    plot_quality_vs_quantity(df, top_n=100)
    log("\nAnalysis complete! Check the 'images' directory for plots.")
    log(f"Cached data available in: {get_cache_path('')}")


if __name__ == "__main__":
    main()
