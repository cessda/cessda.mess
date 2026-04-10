"""Cache freshness check — pure functions, no I/O.

A DigitalObject is considered "fresh" if it was enriched (last_checked updated) within
the configured TTL window (`settings.mess_cache_ttl_hours`, default 48 h).

If fresh, `run_enrichment` returns cached data immediately without calling any external
APIs.  If stale, the full enrichment pipeline re-runs and refreshes `last_checked`.
"""

from datetime import UTC, datetime, timedelta


def is_fresh(last_checked: datetime, ttl_hours: int) -> bool:
    """Return True if `last_checked` falls within the TTL window.

    Args:
        last_checked: UTC timestamp of the last successful enrichment.
        ttl_hours:    Maximum age in hours before the cache entry is considered stale.

    Note:
        Handles naive datetimes by assuming UTC, which is defensive against ORM
        drivers that strip timezone info on retrieval.
    """
    now = datetime.now(UTC)
    # Guard against timezone-naive datetimes from the DB driver.
    if last_checked.tzinfo is None:
        last_checked = last_checked.replace(tzinfo=UTC)
    return (now - last_checked) < timedelta(hours=ttl_hours)
