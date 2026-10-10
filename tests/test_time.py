"""Tests for the Hong Kong timezone helper."""

from datetime import date, datetime, timezone

from app.utils.time import hk_today


class TestHkToday:
    def test_returns_date(self):
        assert type(hk_today()) is date

    def test_maps_utc_to_hk_date(self, monkeypatch):
        # 2026-01-01T16:30:00Z is already 2026-01-02 00:30 in Hong Kong.
        frozen = datetime(2026, 1, 1, 16, 30, tzinfo=timezone.utc)

        class FrozenDatetime:
            @staticmethod
            def now(tz=None):
                return frozen.astimezone(tz) if tz is not None else frozen.replace(tzinfo=None)

        monkeypatch.setattr("app.utils.time.datetime", FrozenDatetime)
        assert hk_today() == date(2026, 1, 2)

    def test_same_hk_day_maps_to_same_date(self, monkeypatch):
        # 2026-01-01T10:00:00Z is still 2026-01-01 18:00 in Hong Kong.
        frozen = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)

        class FrozenDatetime:
            @staticmethod
            def now(tz=None):
                return frozen.astimezone(tz) if tz is not None else frozen.replace(tzinfo=None)

        monkeypatch.setattr("app.utils.time.datetime", FrozenDatetime)
        assert hk_today() == date(2026, 1, 1)
