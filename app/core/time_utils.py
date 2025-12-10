# app/core/time_utils.py
from datetime import datetime, timezone

def utc_now() -> datetime:
    """Return current time in UTC as aware datetime."""
    return datetime.now(timezone.utc)

def ensure_utc(dt: datetime) -> datetime:
    """Normalize any datetime to UTC (assume naive as UTC)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
