# Hacker News Analysis Suite

A complete data pipeline for collecting, storing, and analysing the entire history of Hacker News. This project combines high-performance data scraping with powerful analytical tools to uncover keyword trends and topic modelling of the data.

## Project Components

This suite consists of two main components:

### 1. **[Data Scraper](Scraper/README.md)** 
High-performance parallel scraper that downloads all Hacker News items into PostgreSQL.
- **What it does:** Downloads stories, comments, and other items from the Hacker News API
- **Key features:** Parallel processing, fault tolerance, resumable downloads
- **Output:** Complete HN database with 45M+ items

### 2. **[Analysis Tools](Analysis/README.md)**
Suite of analytical tools for extracting insights from the data.
- **Temporal Analysis:** Track keyword frequency over time (Flask web app)
- **Topic Modelling:** Discover popular topics using BERTopic (ML-based)
- **User Influence:** Identify top contributors and analyse posting patterns

---

## Quick Start

### Step 1: Scrape the Data

Follow the instructions [here](Scraper/README.md/#how-to-run).

⏱️ Takes ~3-20 hours to download full history. Read the whole [Data Scraper README](Scraper/README.md) for more details.

### Step 2: Run Analysis
```bash
cd ../Analysis

# Temporal keyword analysis (web interface)
cd temporal
python app.py

# Topic modelling
cd ../topics
python bertopic_analysis.py --days 365 --max-items 5000

# User influence ranking
cd ../users
python users.py --limit 100
```

---

## What You Can Discover

- **Programming Language Trends:** Which languages are gaining/losing popularity?
- **Topic Evolution:** What topics dominate HN discussions?
- **Influential Users:** Who are the most impactful contributors?
- **Temporal Patterns:** How do discussions change over time?

---

## 📋 Prerequisites

Before you begin, ensure you have the following installed:

- Docker: To run the PostgreSQL database container.
- Python 3.10+: For running the scripts.
- Git: For cloning the repository.

### System Requirements
**Minimum**:
4 GB RAM
50 GB free disk space
Stable internet connection

**Recommended**:
8+ GB RAM
100+ GB disk space (for full history + indexes)
Multi-core CPU (4+ cores)

---

## 📖 Documentation

- **[Scraper Setup](Scraper/README.md)** - Detailed installation and configuration
- **[Analysis Guide](Analysis/README.md)** - How to use each analysis tool
- **[Database Schema](Schema.md)** - Database structure reference


## Contributing

Contributions welcome! Please open an issue or PR.

---

## Acknowledgments

- Hacker News team for the free, open API!
- BERTopic and sentence-transformers communities!

---

## References

- [BERTopic Documentation](https://maartengr.github.io/BERTopic/)
- [PostgreSQL Full-Text Search](https://www.postgresql.org/docs/current/textsearch.html)
- [Flask Documentation](https://flask.palletsprojects.com/)
- [Matplotlib Gallery](https://matplotlib.org/stable/gallery/index.html)
- [PostgreSQL](https://www.postgresql.org/)
- [DBeaver](https://dbeaver.io/)
- [Docker](https://www.docker.com/)