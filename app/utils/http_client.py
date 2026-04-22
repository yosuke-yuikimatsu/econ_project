from __future__ import annotations

import asyncio
from dataclasses import dataclass

import aiohttp

from app.core.config import settings


@dataclass
class FetchResult:
    url: str
    status: int
    body: bytes


async def fetch_url(url: str) -> FetchResult:
    timeout = aiohttp.ClientTimeout(total=settings.request_timeout_sec, connect=settings.connect_timeout_sec)
    connector = aiohttp.TCPConnector(limit=settings.fetch_concurrency, limit_per_host=settings.per_host_limit, ssl=False)
    headers = {"User-Agent": settings.user_agent, "Accept": "text/html,application/xhtml+xml"}
    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        async with session.get(url) as resp:
            body = await resp.read()
            return FetchResult(url=url, status=resp.status, body=body)


def fetch_sync(url: str) -> FetchResult:
    return asyncio.run(fetch_url(url))
