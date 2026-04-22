from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup


def _extract_ogrn(text: str) -> str | None:
    m = re.search(r"\b\d{13}\b", text)
    return m.group(0) if m else None


def parse_bank_list(html: str, base_url: str, reports_path: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table tr")
    banks: list[dict] = []

    for tr in rows:
        cells = [c.get_text(" ", strip=True) for c in tr.select("td")]
        if len(cells) < 3:
            continue

        line_text = " | ".join(cells)
        ogrn = _extract_ogrn(line_text)
        if not ogrn:
            continue

        name = cells[0]
        reg_number = cells[1] if len(cells) > 1 else None
        license_status = cells[-1]

        source_link = tr.select_one("a[href]")
        source_url = urljoin(base_url, source_link["href"]) if source_link else ""
        reports_page_url = urljoin(base_url, f"{reports_path}?ogrn={ogrn}")

        banks.append(
            {
                "ogrn": ogrn,
                "reg_number": reg_number,
                "name": name,
                "license_status": license_status,
                "source_url": source_url,
                "reports_page_url": reports_page_url,
            }
        )

    seen = set()
    unique = []
    for bank in banks:
        if bank["ogrn"] in seen:
            continue
        seen.add(bank["ogrn"])
        unique.append(bank)
    return unique


def is_active_license(status: str) -> bool:
    s = status.lower()
    return "действ" in s and "отоз" not in s and "аннулир" not in s
