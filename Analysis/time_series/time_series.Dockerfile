FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./helper ./helper
COPY ./Analysis/time_series ./Analysis/time_series
COPY ./Analysis/topics ./Analysis/topics
COPY ./Analysis/users ./Analysis/users

RUN mkdir -p /app/cache 
RUN mkdir -p /app/Analysis/time_series/static/images

ENV PYTHONPATH=/app 
ENV PROJECT_ROOT=/app

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--chdir", "Analysis/time_series", "--timeout", "600", "app:app"]