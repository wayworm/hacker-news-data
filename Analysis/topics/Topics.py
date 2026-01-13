import argparse
from datetime import datetime, timedelta
import logging
import pickle

from bertopic import BERTopic
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import HDBSCAN
from umap import UMAP

from helper import config
from helper.paths import get_cache_path, get_image_path

matplotlib.use("Agg")  # Use non-interactive backend

# Logging
logger = logging.getLogger(__name__)
logging.basicConfig(filename='topics.log', level=logging.INFO)

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


model_dir = Path("models")
model_dir.mkdir(exist_ok=True)


def log(message):
    """Custom log function for the dispatcher to ensure immediate output."""
    print(f"\n{message}", flush=True)
    logger.info(message)


def fetch_recent_posts(
    days_back=365,
    min_score=10,
    max_items=50000,
    item_type="story",
    refresh=False,
):
    """
    Fetch recent high-scoring posts from the database

    Args:
        days_back: how many days back to fetch
        min_score: minimum score threshold
        max_items: maximum number of items to retrieve
        item_type: 'story', 'comment', or 'all'
        refresh: force refresh from database
    """
    caching_file = f"recent_posts_{item_type}_days{days_back}_minscore{min_score}_max{max_items}.csv"
    cache_filename = get_cache_path(caching_file)

    if not refresh and cache_filename.exists():
        log(f"Loading cached data from {cache_filename}")
        df = pd.read_csv(cache_filename)
        log(f"Loaded {len(df)} items from cache")
        return df

    log(f"Fetching recent {item_type} items from database...")
    log(
        f"Parameters: days_back={days_back}, min_score={min_score}, max_items={max_items}"
    )

    cutoff_date = datetime.now() - timedelta(days=days_back)
    cutoff_timestamp = int(cutoff_date.timestamp())

    # Build type filter
    if item_type == "all":
        type_filter = "type IN ('story', 'comment')"
    else:
        type_filter = f"type = '{item_type}'"

    query = text(
        f"""
        SELECT
            id,
            type,
            by AS username,
            time,
            score,
            title,
            text,
            url,
            CASE
                WHEN title IS NOT NULL AND title != '' THEN title
                WHEN text IS NOT NULL THEN text
                ELSE ''
            END AS content
        FROM items
        WHERE time >= :cutoff_timestamp
            AND score >= :min_score
            AND {type_filter}
            AND by IS NOT NULL
            AND (
                (type = 'story' AND title IS NOT NULL AND title != '')
                OR (type = 'comment' AND text IS NOT NULL AND text != '')
            )
        ORDER BY score DESC
        LIMIT :max_items
    """
    )

    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={
                "cutoff_timestamp": cutoff_timestamp,
                "min_score": min_score,
                "max_items": max_items,
            },
        )

    log(f"Fetched {len(df)} items from database")

    # Clean content
    df["content"] = df["content"].fillna("")
    df["content"] = df["content"].str.strip()

    # Remove items with very short content
    df = df[df["content"].str.len() >= 20].copy()

    log(f"After filtering short content: {len(df)} items")

    # Save to cache
    df.to_csv(cache_filename, index=False)
    log(f"Cached data to {cache_filename}")

    return df


def train_bertopic_model(documents, min_topic_size=10, nr_topics=None):
    """
    Train a BERTopic model on documents

    Args:
        documents: list of text documents
        min_topic_size: minimum number of documents per topic
        nr_topics: number of topics (None for auto)
    """

    log("\nTraining BERTopic model...")
    log(f"Number of documents: {len(documents)}")
    log(f"Min topic size: {min_topic_size}")

    log("Loading sentence transformer model...")
    embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

    # Initialize UMAP for dimensionality reduction
    umap_model = UMAP(
        n_neighbors=15,
        n_components=5,
        min_dist=0.0,
        metric="cosine",
        random_state=42,
    )

    # Initialize HDBSCAN for clustering
    # Note: sklearn's HDBSCAN has different parameters than the standalone package
    hdbscan_model = HDBSCAN(
        min_cluster_size=min_topic_size,
        metric="euclidean",
        cluster_selection_method="eom",
    )

    # remove stopwords and short words
    vectorizer_model = CountVectorizer(
        stop_words="english", min_df=2, ngram_range=(1, 2)
    )

    # Create BERTopic model
    topic_model = BERTopic(
        embedding_model=embedding_model,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer_model,
        nr_topics=nr_topics,
        top_n_words=10,
        verbose=True,
        calculate_probabilities=False,  # Set to False for speed
    )

    log("Fitting BERTopic model (this may take several minutes)...")
    topics, probs = topic_model.fit_transform(documents)

    log("\nModel training complete!")
    log(
        f"Number of topics found: {len(set(topics)) - 1}"
    )  # -1 for outlier topic

    return topic_model, topics


def save_model(topic_model, model_name):
    """Save the trained BERTopic model"""
    model_path = model_dir / f"{model_name}.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(topic_model, f)
    log(f"Model saved to {model_path}")


def load_model(model_name):
    """Load a trained BERTopic model"""
    model_path = model_dir / f"{model_name}.pkl"
    if not model_path.exists():
        return None
    with open(model_path, "rb") as f:
        topic_model = pickle.load(f)
    log(f"Model loaded from {model_path}")
    return topic_model


def analyze_topics(topic_model, df, topics):
    """Analyze and display topic information"""

    # Add topics to dataframe
    df["topic"] = topics

    # Get topic info
    topic_info = topic_model.get_topic_info()

    log("\n" + "=" * 80)
    log("TOPIC ANALYSIS SUMMARY")
    log("=" * 80)
    log(
        f"\nTotal topics found: {len(topic_info) - 1}"
    )  # -1 for outlier topic
    log(f"Outlier documents (topic -1): {len(df[df['topic'] == -1])}")

    # Display top topics
    log("\n" + "-" * 80)
    log("TOP 20 TOPICS BY SIZE")
    log("-" * 80)

    for idx, row in topic_info.head(
        21
    ).iterrows():  # +1 to account for outlier topic
        if row["Topic"] == -1:
            continue
        topic_words = ", ".join(row["Representation"][:5])
        log(f"\nTopic {row['Topic']}: {row['Name']}")
        log(f"  Size: {row['Count']} documents")
        log(f"  Keywords: {topic_words}")

    # Calculate topic statistics by score
    log("\n" + "-" * 80)
    log("TOPICS BY AVERAGE SCORE (Top 10)")
    log("-" * 80)

    topic_stats = (
        df[df["topic"] != -1]
        .groupby("topic")
        .agg({"score": ["mean", "median", "sum", "count"], "id": "count"})
        .round(2)
    )
    topic_stats.columns = [
        "avg_score",
        "median_score",
        "total_score",
        "count",
        "doc_count",
    ]
    topic_stats = topic_stats.sort_values("avg_score", ascending=False)

    for topic_id, row in topic_stats.head(10).iterrows():
        topic_name = topic_info[topic_info["Topic"] == topic_id]["Name"].iloc[0]
        topic_words = ", ".join(
            topic_info[topic_info["Topic"] == topic_id]["Representation"].iloc[
                0
            ][:5]
        )
        log(f"\nTopic {topic_id}: {topic_name}")
        log(
            f"  Avg Score: {row['avg_score']:.1f} | Median: {row['median_score']:.1f}"
        )
        log(
            f"  Total Score: {row['total_score']:.0f} | Count: {int(row['count'])}"
        )
        log(f"  Keywords: {topic_words}")

    return topic_info, topic_stats


def visualize_topics(topic_model, df, topics, model_name):
    """Create visualizations of topics"""

    log("\nGenerating visualizations...")

    # Get topic info for better labels
    topic_info = topic_model.get_topic_info()
    topic_labels = {}
    for idx, row in topic_info.iterrows():
        if row["Topic"] != -1:
            # Create readable label with top 3 keywords
            keywords = ", ".join(row["Representation"][:3])
            topic_labels[row["Topic"]] = f"{row['Topic']}: {keywords}"

    # 1. Topic distribution bar chart with keyword labels
    fig, ax = plt.subplots(figsize=(16, 10))
    topic_counts = pd.Series(topics).value_counts().head(20)
    topic_counts = topic_counts[
        topic_counts.index != -1
    ]  # Remove outlier topic

    # Create labels and data
    labels = [topic_labels.get(t, str(t)) for t in topic_counts.index]

    colors = plt.cm.viridis(np.linspace(0, 1, len(topic_counts)))
    bars = ax.barh(range(len(topic_counts)), topic_counts.values, color=colors)
    ax.set_yticks(range(len(topic_counts)))
    ax.set_yticklabels(labels, fontsize=10)
    ax.set_xlabel("Number of Documents", fontsize=12)
    ax.set_ylabel("Topic", fontsize=12)
    ax.set_title(
        "Top 20 Topics by Document Count", fontsize=14, fontweight="bold"
    )
    ax.invert_yaxis()

    # Add value labels on bars
    for i, (bar, count) in enumerate(zip(bars, topic_counts.values)):
        ax.text(count + 1, i, str(count), va="center", fontsize=9)

    plt.tight_layout()

    filepath = get_image_path(f"topic_distribution_{model_name}.png")
    plt.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close()
    log(f"Saved topic distribution to {filepath}")

    # 2. Topic by average score with keyword labels
    fig, ax = plt.subplots(figsize=(16, 10))
    topic_avg_score = (
        df[df["topic"] != -1]
        .groupby("topic")["score"]
        .mean()
        .sort_values(ascending=False)
        .head(20)
    )

    labels = [topic_labels.get(t, str(t)) for t in topic_avg_score.index]

    colors = plt.cm.plasma(np.linspace(0, 1, len(topic_avg_score)))
    bars = ax.barh(
        range(len(topic_avg_score)), topic_avg_score.values, color=colors
    )
    ax.set_yticks(range(len(topic_avg_score)))
    ax.set_yticklabels(labels, fontsize=10)
    ax.set_xlabel("Average Score", fontsize=12)
    ax.set_ylabel("Topic", fontsize=12)
    ax.set_title(
        "Top 20 Topics by Average Score", fontsize=14, fontweight="bold"
    )
    ax.invert_yaxis()

    # Add value labels on bars
    for i, (bar, score) in enumerate(zip(bars, topic_avg_score.values)):
        ax.text(score + 10, i, f"{score:.0f}", va="center", fontsize=9)

    plt.tight_layout()

    filepath = get_image_path(f"topic_avg_score_{model_name}.png")
    plt.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close()
    log(f"Saved topic scores to {filepath}")

    # 3. Create detailed topic terms document
    log("\nGenerating detailed topic terms list...")
    topic_terms_path = get_cache_path(f"topic_terms_detailed_{model_name}.txt")
    with open(topic_terms_path, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("DETAILED TOPIC TERMS\n")
        f.write("=" * 80 + "\n\n")

        for idx, row in topic_info.iterrows():
            if row["Topic"] == -1:
                continue

            f.write(f"\nTopic {row['Topic']}: {row['Name']}\n")
            f.write(f"Document Count: {row['Count']}\n")
            f.write(f"Top 20 Terms: {', '.join(row['Representation'][:20])}\n")

            # Get sample documents
            topic_docs = df[df["topic"] == row["Topic"]].nlargest(3, "score")
            f.write(f"\nTop Documents:\n")
            for _, doc in topic_docs.iterrows():
                title = doc.get("title", doc.get("content", ""))[:100]
                f.write(f"  - [{doc['score']}] {title}...\n")
            f.write("-" * 80 + "\n")

    log(f"Saved detailed topic terms to {topic_terms_path}")

    # 4. 2D visualization of topic clusters using UMAP embeddings
    log("\nGenerating 2D topic cluster visualization...")
    try:
        # Get document embeddings - use transform to get existing embeddings
        documents = df["content"].tolist()

        # Get embeddings using the sentence transformer directly
        from sentence_transformers import SentenceTransformer

        embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
        embeddings = embedding_model.encode(documents, show_progress_bar=True)

        # Use UMAP to reduce to 2D for visualization
        from umap import UMAP

        reducer = UMAP(
            n_components=2, random_state=42, min_dist=0.1, n_neighbors=15
        )
        embeddings_2d = reducer.fit_transform(embeddings)

        # Create scatter plot
        fig, ax = plt.subplots(figsize=(16, 12))

        # Plot each topic with different color
        unique_topics = sorted([t for t in df["topic"].unique() if t != -1])
        colors_map = plt.cm.tab20(np.linspace(0, 1, len(unique_topics)))

        for i, topic_id in enumerate(unique_topics):
            mask = df["topic"] == topic_id
            topic_embeddings = embeddings_2d[mask]

            label = topic_labels.get(topic_id, str(topic_id))
            ax.scatter(
                topic_embeddings[:, 0],
                topic_embeddings[:, 1],
                c=[colors_map[i]],
                label=label,
                alpha=0.6,
                s=30,
                edgecolors="black",
                linewidth=0.3,
            )

        # Plot outliers in gray
        if -1 in df["topic"].values:
            mask = df["topic"] == -1
            outlier_embeddings = embeddings_2d[mask]
            ax.scatter(
                outlier_embeddings[:, 0],
                outlier_embeddings[:, 1],
                c="lightgray",
                label="Outliers",
                alpha=0.3,
                s=20,
            )

        ax.set_xlabel("UMAP Dimension 1", fontsize=12)
        ax.set_ylabel("UMAP Dimension 2", fontsize=12)
        ax.set_title(
            "2D Topic Cluster Visualization", fontsize=14, fontweight="bold"
        )
        ax.legend(
            bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=8, ncol=2
        )
        ax.grid(True, alpha=0.3)

        plt.tight_layout()

        filepath = get_image_path(f"topic_clusters_2d_{model_name}.png")
        plt.savefig(filepath, dpi=150, bbox_inches="tight")
        plt.close()
        log(f"Saved 2D cluster visualization to {filepath}")

    except Exception as e:
        log(f"Could not generate cluster visualization: {e}")

    # 5. Save topic-document mapping
    topic_docs = df[["id", "topic", "score", "title", "content"]].copy()
    topic_docs = topic_docs[topic_docs["topic"] != -1]
    topic_docs = topic_docs.sort_values(
        ["topic", "score"], ascending=[True, False]
    )

    csv_path = get_cache_path(f"topic_documents_{model_name}.csv")
    topic_docs.to_csv(csv_path, index=False)
    log(f"Saved topic-document mapping to {csv_path}")


def parser_config():
    parser = argparse.ArgumentParser(
        description="Analyze HN posts with BERTopic"
    )
    parser.add_argument(
        "--days", type=int, default=365, help="Number of days back to fetch"
    )
    parser.add_argument(
        "--min-score", type=int, default=10, help="Minimum score threshold"
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=50000,
        help="Maximum number of items to analyze",
    )
    parser.add_argument(
        "--type",
        choices=["story", "comment", "all"],
        default="story",
        help="Type of items to analyze",
    )
    parser.add_argument(
        "--min-topic-size",
        type=int,
        default=10,
        help="Minimum documents per topic",
    )
    parser.add_argument(
        "--num-topics",
        type=int,
        default=None,
        help="Target number of topics (None for auto)",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Force refresh data from database",
    )
    parser.add_argument(
        "--load-model",
        type=str,
        help="Load existing model instead of training new one",
    )

    return parser


def log_completion_report(model_name):
    log("\n" + "=" * 80)
    log("ANALYSIS COMPLETE!")
    log("=" * 80)
    log("Results saved to:")
    log(f"  - Images: {get_image_path('')}/")
    log(f"  - Data: {get_cache_path('')}/")
    log(f"  - Model: {model_dir}/{model_name}.pkl")


def get_model_identity(args):
    if args.load_model:
        return args.load_model
    return f"bertopic_{args.type}_days{args.days}_minscore{args.min_score}_max{args.max_items}"


def load_or_train_model(args, model_name):
    """Encapsulates the core logic branch."""

    df = fetch_recent_posts(
        args.days, args.min_score, args.max_items, args.type, args.refresh
    )

    if df.empty:
        log("Error: No documents found with the given criteria")
        return None, None, None

    content = df["content"].tolist()

    if args.load_model:
        topic_model = load_model(args.load_model)
        if topic_model is None:
            log(f"Error: Model '{args.load_model}' not found")
            return None, None, None

        log(f"Transforming data with loaded model: {model_name}")
        topics, _ = topic_model.transform(content)

    else:
        topic_model, topics = train_bertopic_model(
            content,
            min_topic_size=args.min_topic_size,
            nr_topics=args.num_topics,
        )
        save_model(topic_model, model_name)

    return topic_model, topics, df


def main():
    args = parser_config().parse_args()
    model_name = get_model_identity(args)

    topic_model, topics, df = load_or_train_model(args, model_name)

    if not topic_model:
        return

    # Downstream Analysis (Decoupled from model creation)
    topic_info, topic_stats = analyze_topics(topic_model, df, topics)
    visualize_topics(topic_model, df, topics, model_name)

    log_completion_report(model_name)


if __name__ == "__main__":
    main()
