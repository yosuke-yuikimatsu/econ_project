from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from dateutil import parser as dt_parser

from app.utils.fingerprint import url_fingerprint


FORM_CODE_RE = re.compile(r"\b(\d{2,4})\b")


def parse_date_iso(raw: str) -> str | None:
    raw = raw.strip()
    if not raw:
        return None
    try:
        dt = dt_parser.parse(raw, dayfirst=True, fuzzy=True)
        return dt.date().isoformat()
    except Exception:
        return None


def parse_reports_index(html: str, page_url: str, ogrn: str, bank_name: str, reg_number: str | None) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    output: list[dict] = []

    current_form_name = None
    current_form_code = None

    for node in soup.select("h1, h2, h3, h4, p, li, tr, a"):
        txt = node.get_text(" ", strip=True)
        if not txt:
            continue

        if node.name in {"h1", "h2", "h3", "h4"}:
            current_form_name = txt
            m = FORM_CODE_RE.search(txt)
            current_form_code = m.group(1) if m else None
            continue

        if node.name == "a" and node.has_attr("href"):
            href = node["href"]
            if not href:
                continue
            full_url = urljoin(page_url, href)
            if "/vfs/" not in full_url and "reports" not in full_url and ".htm" not in full_url:
                continue
            report_date_raw = txt
            if node.parent and node.parent.name in {"td", "li", "p"}:
                parent_text = node.parent.get_text(" ", strip=True)
                if len(parent_text) > len(txt):
                    report_date_raw = parent_text
            output.append(
                {
                    "ogrn": ogrn,
                    "bank_name": bank_name,
                    "reg_number": reg_number,
                    "form_name": current_form_name or "unknown_form",
                    "form_code": current_form_code,
                    "report_date_raw": report_date_raw,
                    "report_date_iso": parse_date_iso(report_date_raw),
                    "report_url": full_url,
                    "report_url_fp": url_fingerprint(full_url),
                }
            )

    dedup = {}
    for it in output:
        dedup[it["report_url_fp"]] = it
    return list(dedup.values())
