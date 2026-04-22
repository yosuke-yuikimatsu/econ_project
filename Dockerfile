FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY scripts /app/scripts
COPY pyproject.toml /app/pyproject.toml
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Run workers as non-root to avoid Celery SecurityWarning.
RUN groupadd -g 1000 appgroup && useradd -m -u 1000 -g appgroup appuser \
    && mkdir -p /app/data/raw_html /app/data/parsed /app/data/logs \
    && chown -R appuser:appgroup /app

USER appuser

ENTRYPOINT ["/app/entrypoint.sh"]
