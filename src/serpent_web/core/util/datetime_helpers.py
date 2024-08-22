from datetime import datetime, timezone


def utc_now_time_aware() -> datetime:
    return datetime.now(timezone.utc)
