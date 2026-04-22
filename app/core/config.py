from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    redis_url: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    cbr_base_url: str = os.getenv("CBR_BASE_URL", "https://www.cbr.ru")
    cbr_full_list_path: str = os.getenv("CBR_FULL_LIST_PATH", "/banking_sector/credit/FullCoList/")
    reports_path: str = os.getenv("CBR_REPORTS_PATH", "/finorg/foinfo/reports/")

    data_dir: Path = Path(os.getenv("DATA_DIR", "/app/data"))
    db_path: Path = Path(os.getenv("DB_PATH", "/app/data/state.db"))
    raw_html_dir: Path = Path(os.getenv("RAW_HTML_DIR", "/app/data/raw_html"))
    parsed_dir: Path = Path(os.getenv("PARSED_DIR", "/app/data/parsed"))
    parsed_json_path: Path = Path(os.getenv("PARSED_JSON_PATH", "/app/data/parsed/all_banks_reports.json"))

    request_timeout_sec: int = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
    connect_timeout_sec: int = int(os.getenv("CONNECT_TIMEOUT_SEC", "10"))
    fetch_concurrency: int = int(os.getenv("FETCH_CONCURRENCY", "32"))
    per_host_limit: int = int(os.getenv("FETCH_PER_HOST_LIMIT", "8"))
    user_agent: str = os.getenv("USER_AGENT", "cbr-report-harvester/1.0")

    celery_prefetch_multiplier: int = int(os.getenv("CELERY_PREFETCH_MULTIPLIER", "1"))
    celery_task_time_limit: int = int(os.getenv("CELERY_TASK_TIME_LIMIT", "1200"))
    celery_task_soft_time_limit: int = int(os.getenv("CELERY_TASK_SOFT_TIME_LIMIT", "900"))

    bootstrap_batch_size: int = int(os.getenv("BOOTSTRAP_BATCH_SIZE", "100"))
    max_retries: int = int(os.getenv("TASK_MAX_RETRIES", "7"))
    retry_base_sec: int = int(os.getenv("RETRY_BASE_SEC", "2"))

    full_run: bool = os.getenv("FULL_RUN", "1") == "1"
    demo_ogrn: str | None = os.getenv("DEMO_OGRN")


settings = Settings()

for d in [settings.data_dir, settings.raw_html_dir, settings.parsed_dir]:
    d.mkdir(parents=True, exist_ok=True)
