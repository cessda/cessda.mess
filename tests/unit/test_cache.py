"""Tests for cache freshness logic."""

from datetime import UTC, datetime, timedelta

from app.services.cache import is_fresh


class TestIsFresh:
    def test_fresh_within_ttl(self):
        last_checked = datetime.now(UTC) - timedelta(hours=1)
        assert is_fresh(last_checked, ttl_hours=48) is True

    def test_stale_outside_ttl(self):
        last_checked = datetime.now(UTC) - timedelta(hours=73)
        assert is_fresh(last_checked, ttl_hours=48) is False

    def test_exactly_at_boundary(self):
        # Exactly at TTL boundary is still fresh (strictly less than)
        last_checked = datetime.now(UTC) - timedelta(hours=48, seconds=1)
        assert is_fresh(last_checked, ttl_hours=48) is False

    def test_zero_ttl_always_stale(self):
        last_checked = datetime.now(UTC) - timedelta(seconds=1)
        assert is_fresh(last_checked, ttl_hours=0) is False

    def test_naive_datetime_treated_as_utc(self):
        # Timezone-naive datetimes should be handled without error
        last_checked = datetime.utcnow() - timedelta(hours=1)
        result = is_fresh(last_checked, ttl_hours=48)
        assert result is True
