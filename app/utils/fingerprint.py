from __future__ import annotations

import hashlib


def url_fingerprint(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def content_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
