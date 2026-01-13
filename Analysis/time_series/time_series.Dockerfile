FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./helper ./helper
COPY ./Analysis/time_series ./Analysis/time_series
COPY ./Analysis/topics ./Analysis/topics
COPY ./Analysis/users ./Analysis/users

RUN mkdir -p /app/cache /app/static/images

ENV PYTHONPATH=/app
ENV PROJECT_ROOT=/app

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--chdir", "Analysis/time_series", "--timeout", "600", "app:app"]