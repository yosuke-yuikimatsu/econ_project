from __future__ import annotations

from bs4 import BeautifulSoup


def parse_report_page(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    title = soup.title.get_text(" ", strip=True) if soup.title else None

    unit = None
    for cand in soup.find_all(["p", "div", "span"]):
        txt = cand.get_text(" ", strip=True)
        low = txt.lower()
        if "единиц" in low or "тыс." in low or "млн" in low:
            unit = txt
            break

    metadata = {}
    for meta in soup.select("meta[name], meta[property]"):
        key = meta.get("name") or meta.get("property")
        val = meta.get("content")
        if key and val:
            metadata[key] = val

    tables = []
    for idx, table in enumerate(soup.select("table")):
        header_rows = []
        body_rows = []

        trs = table.select("tr")
        for tr in trs:
            row = [td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])]
            if not row:
                continue
            if tr.find("th"):
                header_rows.append(row)
            else:
                body_rows.append(row)

        section = None
        prev = table.find_previous(["h1", "h2", "h3", "h4", "p"])
        if prev:
            section = prev.get_text(" ", strip=True)

        tables.append(
            {
                "table_index": idx,
                "section": section,
                "headers": header_rows,
                "rows": body_rows,
                "raw_html": str(table),
            }
        )

    raw_text = soup.get_text("\n", strip=True)
    raw_html_snippet = str(soup.body)[:20000] if soup.body else html[:20000]

    return {
        "title": title,
        "unit": unit,
        "metadata": metadata,
        "tables": tables,
        "raw_text": raw_text,
        "raw_html_snippet": raw_html_snippet,
    }
