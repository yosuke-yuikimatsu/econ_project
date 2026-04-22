from __future__ import annotations

from pathlib import Path

from app.core.celery_app import celery_app
from app.core.config import settings
from app.parsers.report_page_parser import parse_report_page
from app.parsers.reports_index_parser import parse_reports_index
from app.storage.state import StateStore


@celery_app.task(bind=True, name="app.tasks.parse.parse_page", max_retries=settings.max_retries, acks_late=True)
def parse_page(self, url: str, url_fp: str, html_path: str, page_type: str, ogrn: str | None = None, meta: dict | None = None):
    store = StateStore()
    meta = meta or {}
    text = Path(html_path).read_text("utf-8", errors="ignore")

    if page_type == "reports_index":
        bank_name = meta.get("bank_name") or "unknown_bank"
        reg_number = meta.get("reg_number")
        items = parse_reports_index(text, url, ogrn or "", bank_name, reg_number)
        created = 0
        for item in items:
            inserted = store.upsert_report_index_item(item)
            if inserted:
                created += 1
                celery_app.send_task(
                    "app.tasks.fetch.fetch_page",
                    kwargs={
                        "url": item["report_url"],
                        "page_type": "report_page",
                        "ogrn": item["ogrn"],
                        "meta": item,
                    },
                    queue="fetch",
                )
        store.metric_incr("report_links_discovered", len(items))
        return {"status": "ok", "links_discovered": len(items), "new": created}

    if page_type == "report_page":
        if store.parsed_exists(url_fp):
            return {"status": "skipped_parsed", "url": url}

        ref = store.get_report_ref(url_fp) or {}
        parsed = parse_report_page(text)
        payload = {
            "ogrn": ogrn,
            "bank_name": ref.get("bank_name") or meta.get("bank_name"),
            "reg_number": ref.get("reg_number") or meta.get("reg_number"),
            "form_name": ref.get("form_name") or meta.get("form_name"),
            "form_code": ref.get("form_code") or meta.get("form_code"),
            "report_date_raw": ref.get("report_date_raw") or meta.get("report_date_raw"),
            "report_date_iso": ref.get("report_date_iso") or meta.get("report_date_iso"),
            "report_url": url,
            **parsed,
        }
        store.store_parsed_report(url_fp=url_fp, ogrn=ogrn or "", report_url=url, payload=payload)
        store.metric_incr("report_pages_parsed", 1)
        return {"status": "ok", "url": url, "tables": len(parsed["tables"])}

    return {"status": "ignored", "reason": f"unknown page_type={page_type}"}
