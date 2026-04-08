"""API key authentication dependency."""
from __future__ import annotations

from fastapi import Depends, HTTPException, Query, Security
from fastapi.security import APIKeyHeader

from ingestion.db.engine import get_db
from ingestion.db import crud
from ingestion.storage.file_hash import sha256_string

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(
    header_key: str | None = Security(_api_key_header),
    query_key: str | None = Query(default=None, alias="key"),
    db=Depends(get_db),
) -> crud.ApiKey:
    # Accept the key from either the X-API-Key header (JS fetches)
    # or a ?key= query parameter (browser link/tab navigation)
    api_key = header_key or query_key
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header")
    key_hash = sha256_string(api_key)
    record = crud.get_api_key_by_hash(db, key_hash)
    if record is None:
        raise HTTPException(status_code=401, detail="Invalid or inactive API key")
    return record
