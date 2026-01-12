# Analysing Hacker News: Trends, Topics, and Influence

## Introduction

This project aims to explore historical patterns in posts and comments on [Hacker News](news.ycombinator.com) using data mining and machine learning techniques. I set out to answer three key questions:

Q1. **How has the popularity of programming languages changed over time?**
Q2. **What categories of content are most popular on Hacker News?**
Q3. **Which users have accumulated the most influence on the platform?**

To answer these questions, I collected my own copy of the complete history of posts and comments, totally 45 million items, using the [Hacker News API](https://github.com/HackerNews/API) and created a system for querying keywords and their relative popularity, to answer Q1, then I applied machine learning techniques found in [topic modeling](https://en.wikipedia.org/wiki/Topic_model) for Q2, and finally I simply queried user metrics to approach Q3.

---

## Data Collection

I collected all posts (stories) and comments from [news.ycombinator.com](https://news.ycombinator.com) using the [Hacker News API](https://github.com/HackerNews/API).

### Collection Architecture

The collection system used parallel processing to efficiently download millions of items:

- **20 parallel worker processes** requesting data simultaneously
- **PostgreSQL database** for data storage and tracking completion of jobs. 
- **Job management system** to coordinate workers and prevent duplicate downloads
- **Asynchronous operations** to avoid blocking on API calls

The `dispatcher.py` script managed worker instances and assigned jobs (ranges of item IDs to download). A `jobs` table in PostgreSQL tracked which portions of the data had been downloaded, allowing the system to resume after interruptions where it had left off.

### Example API Response

```json
{
  "by" : "dhouston",
  "descendants" : 71,
  "id" : 8863,
  "kids" : [ 9224, 8917, 8884, 8887, 8952, 8869, 8873, 8958, 8940, 8908, 9005, 9671, 9067, 9055, 8865, 8881, 8872, 8955, 10403, 8903, 8928, 9125, 8998, 8901, 8902, 8907, 8894, 8870, 8878, 8980, 8934, 8943, 8876 ],
  "score" : 104,
  "time" : 1175714200,
  "title" : "My YC app: Dropbox - Throw away your USB drive",
  "type" : "story",
  "url" : "http://www.getdropbox.com/u/2/screencast.html"
}
```

### Evolution of the Collection System

The data collection process evolved through several iterations:

1. **Initial prototype**: Simple single-threaded program using an SQLite database. I was familiar with SQLite, and so it was my first choice.

2. **Multiprocessing implementation**: Reduced collection time from ~100 days to ~10 days. This was a simple speed-up, as each data item needed to be fetched independently, it was a perfect use-case for parallelism.

3. **Migration to PostgreSQL**: This allowed for [JSONB](https://www.postgresql.org/docs/current/datatype-json.html) storage which I wanted for the kids column. Additionally, I wanted to explore using PostgreSQL and it's functionality. I made use of the [Dbeaver](https://dbeaver.io/) database management tool throughout this project.

4. **Job-based coordination**: Added the jobs table system for fault tolerance and resumability. By having this system, I meant any "job" could be picked up by a worker instance as long as it was not currently underway, so no portion of the work was tied to any given running instance, so it was easy to stop the system and increase the number of workers if needed, with minimal loss. Writes to the database were also now being done in large chunks, as I'd read this would increase efficiency [add sources]

---

## Data Processing

### Full-Text Search Setup

To enable fast keyword searches across millions of documents, I used PostgreSQL's [full-text search capabilities](https://www.postgresql.org/docs/current/datatype-textsearch.html). The text content was transformed into a `tsvector` column, which:

- **Stems words** to their root forms (e.g., "running" → "run")
- **Normalizes text** for consistent matching
- **Enables fast searches** through a [GIN (Generalized Inverted Index)](https://www.postgresql.org/docs/current/gin.html)

### Database Indexes

Key indexes created for performance:

```sql
-- Full-text search
CREATE INDEX items_text_search_idx ON public.items USING GIN(text_search_vector);

-- Time-series queries
CREATE INDEX idx_items_time ON items (time);
```

### Query Pattern for Time-Series Analysis

Keyword frequency queries followed this pattern:

```sql
SELECT
  date_trunc(:time_bin, to_timestamp(time)) AS time_period,
  COUNT(*) AS post_count
FROM items
WHERE text_search_vector @@ plainto_tsquery('english', :keyword)
GROUP BY time_period
ORDER BY time_period ASC
```

This returns well-ordered time-series data in configurable time bins (day, week, month, or year).


## TimescaleDB efficiencies

TODO: task about this a bit

---

## Analysis 1: Programming Language Popularity Over Time

### Methodology

I built a Flask web application that queries the PostgreSQL database for keyword frequencies over time. The system:

- Uses SQLAlchemy to execute time-series queries. This allowed for database-agnosticism.
- Normalizes frequencies (mentions per 100 total posts + comments) to account for Hacker News's growth
- Applies rolling averages to smooth short-term fluctuations
- Generates matplotlib visualizations comparing multiple keywords

The web interface allows users to enter keywords, select time bins, and configure rolling average windows. Results are cached for faster subsequent queries.

### Results

[FINDINGS TO BE ADDED]

Example visualization:

![A graph comparing the relative number of posts containing the terms "Python", "Java", "Javascript", "Rust", "C++", "AI", "ML" showing the term C++ to be more popular than the others, until a spiking of usage in the term AI from last 2022, where it begins to dominate.](images/posts_python_java_javascript_rust_C++_AI_ML_normalised_rolling_average-10.png)

**Key observations:**

[INSERT FINDINGS ABOUT LANGUAGE TRENDS]
- [Language popularity shifts]
- [AI/ML growth patterns]
- [Notable inflection points]

---

## Analysis 2: Topic Modeling of Popular Posts

### The Challenge: Beyond Simple Keywords

Simple keyword searches proved insufficient for understanding what Hacker News discusses. The challenges included:

- **Polysemy**: Words with multiple meanings (e.g., "Go" as a programming language vs. a verb)
- **False positives**: Context-dependent interpretation required
- **Unknown patterns**: Cannot discover topics without knowing what to search for

After researching approaches to this problem, I discovered [Topic Modeling](https://en.wikipedia.org/wiki/Topic_model) – a family of techniques designed to automatically discover abstract topics in document collections.

### Choosing a Approach

I explored several topic modeling approaches:

- **[Latent Dirichlet Allocation (LDA)](https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)**: A classical probabilistic approach
- **[BERTopic](https://maartengr.github.io/BERTopic/)**: A modern transformer-based approach

I chose BERTopic because it:
- Uses transformer models (BERT) to understand semantic context, not just word co-occurrence
- Automatically discovers topics without predefined categories
- Produces interpretable results with clear keyword representations
- Leverages state-of-the-art natural language processing

### BERTopic Implementation

The BERTopic pipeline consists of four stages:

1. **Sentence embeddings**: Documents are converted to 384-dimensional vectors using the `all-MiniLM-L6-v2` model
2. **Dimensionality reduction**: UMAP reduces embeddings to 5 dimensions for clustering
3. **Clustering**: HDBSCAN groups similar documents into topics
4. **Topic representation**: The most distinctive keywords are extracted for each cluster

### Results

[FINDINGS TO BE ADDED]

**Data analyzed:**
- [Number of documents]
- [Time period covered]
- [Number of topics discovered]

**Major topic categories:**

[INSERT TOPIC FINDINGS]
- [Top topics with descriptions]
- [Topic score analysis]
- [Temporal topic trends]

**Example topics discovered:**

```
Topic 1: AI, coding, model, agents, agent
  - 68 documents, avg score: 765.8
  - Discussion of AI coding assistants and autonomous agents

Topic 3: apple, backdoor, uk, macos, encryption
  - 33 documents, avg score: 827.4
  - Privacy and encryption policy debates

[INSERT ADDITIONAL EXAMPLE TOPICS]
```

**Visualizations:**

[INSERT CLUSTER DIAGRAM]
[INSERT TOPIC DISTRIBUTION CHART]
[INSERT TOPIC SCORE ANALYSIS]

---

## Analysis 3: User Influence on Hacker News

### Methodology

To identify the most influential Hacker News users, I analyzed cumulative karma scores and posting patterns. The analysis tracked:

- **Total posts** (stories + comments)
- **Cumulative score** (total karma earned)
- **Average score per post** (quality metric)
- **Top single post score** (peak influence)
- **Activity timeline** (first/last post dates, posting frequency)

I created separate analyses for:
- Overall rankings (all content)
- Story posters vs. commenters
- Individual user timelines

### Results

[FINDINGS TO BE ADDED]

**Top users:**

[INSERT TOP 10 USERS WITH STATISTICS]

**Patterns observed:**

[INSERT OBSERVATIONS]
- [Quality vs. quantity patterns]
- [Story posters vs. commenters]
- [Veteran vs. new user dynamics]
- [Posting frequency patterns]
- [Temporal activity trends]

---

## Key Findings and Insights

### Programming Language Trends

[INSERT MAJOR FINDINGS]

### Popular Topics on Hacker News

[INSERT TOPIC INSIGHTS]

### User Influence Patterns

[INSERT USER FINDINGS]

### Cross-Analysis Insights

[INSERT CONNECTIONS BETWEEN ANALYSES]

---

## Conclusions

[SUMMARY OF PROJECT OUTCOMES]

[LESSONS LEARNED]

[FUTURE DIRECTIONS]

---

## Technical Details

### Tools and Technologies

- **Database**: PostgreSQL with full-text search and JSONB
- **Backend**: Python with SQLAlchemy, Flask
- **Analysis**: BERTopic, sentence-transformers, scikit-learn
- **Visualization**: Matplotlib, Seaborn
- **Deployment**: [Details if applicable]

### Code Repository

[Link to GitHub repository if public]

---

## References

- [Hacker News API Documentation](https://github.com/HackerNews/API)
- [BERTopic Documentation](https://maartengr.github.io/BERTopic/)
- [PostgreSQL Full-Text Search](https://www.postgresql.org/docs/current/textsearch.html)
- [Topic Modeling (Wikipedia)](https://en.wikipedia.org/wiki/Topic_model)
- [Latent Dirichlet Allocation](https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)