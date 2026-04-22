from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.core.celery_app import celery_app
from app.core.config import settings
from app.core.logging import setup_logging
from app.parsers.bank_list_parser import is_active_license, parse_bank_list
from app.storage.state import StateStore
from app.tasks.fetch import fetch_page
from app.utils.http_client import fetch_sync
from app.utils.fingerprint import url_fingerprint

setup_logging()
logger = logging.getLogger(__name__)


@celery_app.task(bind=True, name="app.tasks.bootstrap.start_bootstrap", autoretry_for=(Exception,), retry_backoff=True, max_retries=5)
def start_bootstrap(self):
    store = StateStore()
    list_url = f"{settings.cbr_base_url}{settings.cbr_full_list_path}"
    res = fetch_sync(list_url)
    if res.status >= 400:
        raise RuntimeError(f"Could not fetch bank list: {res.status}")

    html = res.body.decode("utf-8", errors="ignore")
    banks = parse_bank_list(html, settings.cbr_base_url, settings.reports_path)
    store.metric_incr("banks_discovered", len(banks))

    active = []
    for b in banks:
        is_active = is_active_license(b["license_status"])
        store.upsert_bank(b, is_active=is_active)
        if is_active:
            active.append(b)

    if settings.demo_ogrn:
        active = [b for b in active if b["ogrn"] == settings.demo_ogrn]

    store.metric_incr("banks_filtered_active", len(active))

    now = datetime.now(timezone.utc).isoformat()
    logger.info("bootstrap complete", extra={"metric": "banks_filtered_active", "value": len(active), "task_id": self.request.id})

    for bank in active:
        fp = url_fingerprint(bank["reports_page_url"])
        if store.should_fetch_url(fp):
            fetch_page.apply_async(
                kwargs={
                    "url": bank["reports_page_url"],
                    "page_type": "reports_index",
                    "ogrn": bank["ogrn"],
                    "meta": {
                        "bank_name": bank["name"],
                        "reg_number": bank.get("reg_number"),
                        "seeded_at": now,
                    },
                },
                queue="fetch",
            )
