from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

import orjson

from app.core.celery_app import celery_app
from app.core.config import settings
from app.storage.state import StateStore


@celery_app.task(bind=True, name="app.tasks.aggregate.build_final_json")
def build_final_json(self):
    store = StateStore()
    rows = store.export_aggregate_rows()

    by_bank = defaultdict(lambda: {"forms": defaultdict(lambda: {"form_name": None, "form_code": None, "reports": []})})

    for row in rows:
        bank_key = row["ogrn"]
        bank_obj = by_bank[bank_key]
        bank_obj.update(
            {
                "ogrn": row["ogrn"],
                "reg_number": row["reg_number"],
                "name": row["name"],
                "license_status": row["license_status"],
                "source_url": row["source_url"],
                "reports_page_url": row["reports_page_url"],
            }
        )
        payload_raw = row.get("payload_json")
        if not payload_raw:
            continue
        rep = orjson.loads(payload_raw)
        form_key = f"{rep.get('form_name')}::{rep.get('form_code')}"
        form_obj = bank_obj["forms"][form_key]
        form_obj["form_name"] = rep.get("form_name")
        form_obj["form_code"] = rep.get("form_code")
        form_obj["reports"].append(rep)

    banks = []
    reports_total = 0
    for bank in by_bank.values():
        forms = []
        for form in bank["forms"].values():
            seen = set()
            uniq_reports = []
            for r in form["reports"]:
                key = r.get("report_url")
                if key in seen:
                    continue
                seen.add(key)
                uniq_reports.append(r)
            form["reports"] = sorted(uniq_reports, key=lambda x: (x.get("report_date_iso") or "", x.get("report_url") or ""))
            reports_total += len(form["reports"])
            forms.append(form)
        bank["forms"] = sorted(forms, key=lambda x: (x.get("form_code") or "", x.get("form_name") or ""))
        banks.append(bank)

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "cbr.ru",
        "banks_total": len(banks),
        "reports_total": reports_total,
        "banks": sorted(banks, key=lambda x: x["name"]),
        "summary": store.summary(),
    }

    settings.parsed_json_path.parent.mkdir(parents=True, exist_ok=True)
    settings.parsed_json_path.write_bytes(orjson.dumps(out, option=orjson.OPT_INDENT_2))
    return {"status": "ok", "out": str(settings.parsed_json_path), "banks": len(banks), "reports": reports_total}
