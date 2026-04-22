from __future__ import annotations

import logging
from datetime import datetime, timezone

from celery.exceptions import SoftTimeLimitExceeded

from app.core.celery_app import celery_app
from app.core.config import settings
from app.core.logging import setup_logging
from app.storage.state import StateStore
from app.utils.fingerprint import content_hash, url_fingerprint
from app.utils.http_client import fetch_sync

setup_logging()
logger = logging.getLogger(__name__)


@celery_app.task(bind=True, name="app.tasks.fetch.fetch_page", max_retries=settings.max_retries, acks_late=True)
def fetch_page(self, url: str, page_type: str, ogrn: str | None = None, meta: dict | None = None):
    store = StateStore()
    url_fp = url_fingerprint(url)
    if not store.should_fetch_url(url_fp):
        return {"status": "skipped_cached", "url": url}

    retries = self.request.retries
    try:
        res = fetch_sync(url)
        if res.status in {429, 500, 502, 503, 504}:
            raise RuntimeError(f"Retriable HTTP status: {res.status}")

        html_dir = settings.raw_html_dir / (ogrn or "global")
        html_dir.mkdir(parents=True, exist_ok=True)
        file_path = html_dir / f"{url_fp}.html"
        file_path.write_bytes(res.body)

        payload = {
            "url_fp": url_fp,
            "url": url,
            "page_type": page_type,
            "ogrn": ogrn,
            "html_path": str(file_path),
            "http_status": res.status,
            "content_hash": content_hash(res.body),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "retries": retries,
            "error": None,
        }
        store.record_fetched_page(payload)
        metric_name = "report_index_pages_fetched" if page_type == "reports_index" else "report_pages_fetched"
        store.metric_incr(metric_name, 1)

        celery_app.send_task(
            "app.tasks.parse.parse_page",
            kwargs={
                "url": url,
                "url_fp": url_fp,
                "html_path": str(file_path),
                "page_type": page_type,
                "ogrn": ogrn,
                "meta": meta or {},
            },
            queue="parse",
        )
        return {"status": "ok", "url": url, "path": str(file_path)}
    except SoftTimeLimitExceeded:
        store.log_error("fetch_timeout", "soft time limit exceeded", key=url)
        raise
    except Exception as exc:
        store.metric_incr("errors", 1)
        store.metric_incr("retries", 1)
        store.log_error("fetch", str(exc), key=url)
        countdown = min(settings.retry_base_sec * (2 ** retries), 300)
        raise self.retry(exc=exc, countdown=countdown)
