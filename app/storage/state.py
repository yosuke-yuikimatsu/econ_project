from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import orjson

from app.core.config import settings


class StateStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or settings.db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def conn(self):
        con = sqlite3.connect(self.db_path)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        try:
            yield con
            con.commit()
        finally:
            con.close()

    def _init_db(self) -> None:
        with self.conn() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS banks (
                    ogrn TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    reg_number TEXT,
                    license_status TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    reports_page_url TEXT NOT NULL,
                    is_active INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS fetched_pages (
                    url_fp TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    page_type TEXT NOT NULL,
                    ogrn TEXT,
                    html_path TEXT,
                    http_status INTEGER,
                    content_hash TEXT,
                    fetched_at TEXT,
                    error TEXT,
                    retries INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS report_index_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ogrn TEXT NOT NULL,
                    report_url TEXT NOT NULL,
                    report_url_fp TEXT NOT NULL,
                    form_name TEXT,
                    form_code TEXT,
                    report_date_raw TEXT,
                    report_date_iso TEXT,
                    bank_name TEXT,
                    reg_number TEXT,
                    UNIQUE(report_url_fp)
                );
                CREATE TABLE IF NOT EXISTS parsed_reports (
                    report_url_fp TEXT PRIMARY KEY,
                    ogrn TEXT NOT NULL,
                    report_url TEXT NOT NULL,
                    payload_json BLOB NOT NULL,
                    parsed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS metrics (
                    metric TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope TEXT NOT NULL,
                    key TEXT,
                    error TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )

    def upsert_bank(self, bank: dict, is_active: bool) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.conn() as con:
            con.execute(
                """
                INSERT INTO banks (ogrn, name, reg_number, license_status, source_url, reports_page_url, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ogrn) DO UPDATE SET
                  name=excluded.name,
                  reg_number=excluded.reg_number,
                  license_status=excluded.license_status,
                  source_url=excluded.source_url,
                  reports_page_url=excluded.reports_page_url,
                  is_active=excluded.is_active
                """,
                (
                    bank["ogrn"],
                    bank["name"],
                    bank.get("reg_number"),
                    bank["license_status"],
                    bank["source_url"],
                    bank["reports_page_url"],
                    1 if is_active else 0,
                    now,
                ),
            )

    def metric_incr(self, metric: str, amount: int = 1) -> None:
        with self.conn() as con:
            con.execute(
                """
                INSERT INTO metrics(metric, value) VALUES(?, ?)
                ON CONFLICT(metric) DO UPDATE SET value = value + excluded.value
                """,
                (metric, amount),
            )

    def should_fetch_url(self, url_fp: str) -> bool:
        with self.conn() as con:
            row = con.execute("SELECT html_path, error FROM fetched_pages WHERE url_fp = ?", (url_fp,)).fetchone()
            return row is None

    def record_fetched_page(self, payload: dict) -> None:
        with self.conn() as con:
            con.execute(
                """
                INSERT INTO fetched_pages(url_fp, url, page_type, ogrn, html_path, http_status, content_hash, fetched_at, error, retries)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url_fp) DO UPDATE SET
                  html_path=excluded.html_path,
                  http_status=excluded.http_status,
                  content_hash=excluded.content_hash,
                  fetched_at=excluded.fetched_at,
                  error=excluded.error,
                  retries=excluded.retries
                """,
                (
                    payload["url_fp"],
                    payload["url"],
                    payload["page_type"],
                    payload.get("ogrn"),
                    payload.get("html_path"),
                    payload.get("http_status"),
                    payload.get("content_hash"),
                    payload.get("fetched_at"),
                    payload.get("error"),
                    payload.get("retries", 0),
                ),
            )

    def upsert_report_index_item(self, item: dict) -> bool:
        with self.conn() as con:
            try:
                con.execute(
                    """
                    INSERT INTO report_index_items(ogrn, report_url, report_url_fp, form_name, form_code, report_date_raw, report_date_iso, bank_name, reg_number)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item["ogrn"],
                        item["report_url"],
                        item["report_url_fp"],
                        item.get("form_name"),
                        item.get("form_code"),
                        item.get("report_date_raw"),
                        item.get("report_date_iso"),
                        item.get("bank_name"),
                        item.get("reg_number"),
                    ),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def get_report_ref(self, report_url_fp: str) -> dict | None:
        with self.conn() as con:
            row = con.execute(
                """
                SELECT ogrn, report_url, form_name, form_code, report_date_raw, report_date_iso, bank_name, reg_number
                FROM report_index_items WHERE report_url_fp=?
                """,
                (report_url_fp,),
            ).fetchone()
            if not row:
                return None
            keys = ["ogrn", "report_url", "form_name", "form_code", "report_date_raw", "report_date_iso", "bank_name", "reg_number"]
            return dict(zip(keys, row))

    def store_parsed_report(self, report_url_fp: str, ogrn: str, report_url: str, payload: dict) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.conn() as con:
            con.execute(
                """
                INSERT INTO parsed_reports(report_url_fp, ogrn, report_url, payload_json, parsed_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(report_url_fp) DO UPDATE SET
                  payload_json=excluded.payload_json,
                  parsed_at=excluded.parsed_at
                """,
                (report_url_fp, ogrn, report_url, orjson.dumps(payload), now),
            )

    def parsed_exists(self, report_url_fp: str) -> bool:
        with self.conn() as con:
            row = con.execute("SELECT 1 FROM parsed_reports WHERE report_url_fp=?", (report_url_fp,)).fetchone()
            return row is not None

    def log_error(self, scope: str, err: str, key: str | None = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.conn() as con:
            con.execute(
                "INSERT INTO errors(scope, key, error, created_at) VALUES (?, ?, ?, ?)",
                (scope, key, err, now),
            )
            
    def summary(self) -> dict:
        with self.conn() as con:
            banks_total = con.execute("SELECT COUNT(*) FROM banks WHERE is_active=1").fetchone()[0]
            reports_total = con.execute("SELECT COUNT(*) FROM parsed_reports").fetchone()[0]
            metrics = dict(con.execute("SELECT metric, value FROM metrics").fetchall())
            return {
                "banks_total": banks_total,
                "reports_total": reports_total,
                "metrics": metrics,
            }

    def export_aggregate_rows(self) -> list[dict]:
        with self.conn() as con:
            rows = con.execute(
                """
                SELECT b.ogrn, b.reg_number, b.name, b.license_status, b.source_url, b.reports_page_url,
                       p.payload_json
                FROM banks b
                LEFT JOIN parsed_reports p ON p.ogrn = b.ogrn
                WHERE b.is_active = 1
                ORDER BY b.name
                """
            ).fetchall()
        result: list[dict] = []
        for row in rows:
            result.append(
                {
                    "ogrn": row[0],
                    "reg_number": row[1],
                    "name": row[2],
                    "license_status": row[3],
                    "source_url": row[4],
                    "reports_page_url": row[5],
                    "payload_json": row[6],
                }
            )
        return result
