# app/core/time_utils.py
from datetime import datetime
import pytz

UTC = pytz.UTC
JAKARTA_TZ = pytz.timezone("Asia/Jakarta")

def jkt_utc_now() -> datetime:
    """Return current time in UTC as aware datetime."""
    return datetime.now(UTC)

def ensure_jkt_utc(dt: datetime) -> datetime:
    """Normalize any datetime to UTC (assume naive as UTC)."""
    if dt.tzinfo is None:
        local = JAKARTA_TZ.localize(dt)
        return local.astimezone(UTC)
    return dt.astimezone(UTC)
