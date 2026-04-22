from __future__ import annotations

from celery import Celery

from app.core.config import settings


celery_app = Celery("cbr_pipeline", broker=settings.redis_url, backend=settings.redis_url)

celery_app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=settings.celery_prefetch_multiplier,
    task_time_limit=settings.celery_task_time_limit,
    task_soft_time_limit=settings.celery_task_soft_time_limit,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_default_queue="bootstrap",
    task_routes={
        "app.tasks.bootstrap.*": {"queue": "bootstrap"},
        "app.tasks.fetch.*": {"queue": "fetch"},
        "app.tasks.parse.*": {"queue": "parse"},
        "app.tasks.aggregate.*": {"queue": "aggregate"},
    },
    task_default_retry_delay=3,
)

celery_app.autodiscover_tasks(["app.tasks"])
