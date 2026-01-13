FROM python:3.11-slim AS base
WORKDIR /app
RUN apt-get update && apt-get install -y gcc build-essential python3-dev && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./helper ./helper


FROM base AS scraper_image
COPY ./Scraper ./Scraper
ENV PYTHONPATH=/app
CMD ["python", "Scraper/dispatcher.py"]

FROM base AS analysis_image
COPY ./Analysis ./Analysis
ENV PYTHONPATH=/app
ENV PROJECT_ROOT=/app
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--chdir", "Analysis/time_series", "app:app"]