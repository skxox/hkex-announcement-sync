"""Tests for HKEX client cancellation tag detection and chunk windowing."""
from datetime import date

import pytest

from app.scraper.hkex_client import CANCELLATION_PATTERNS, HKEXClient


class TestFetchAllChunks:
    """Regression tests for the monthly chunk loop boundaries."""

    @staticmethod
    def _collect_chunks(monkeypatch, date_from, date_to, lang="E"):
        client = HKEXClient()
        calls = []

        def fake_fetch_chunk(stock_id, chunk_from, chunk_to, lang="E"):
            calls.append((chunk_from, chunk_to, lang))
            return []

        monkeypatch.setattr(client, "_fetch_chunk", fake_fetch_chunk)
        client._fetch_all_chunks("1", date_from, date_to, lang=lang)
        return calls

    def test_same_day_window_fetches_one_chunk(self, monkeypatch):
        # Incremental sync passes date_from == date_to once today's announcements
        # exist; the loop must still fetch the [D, D] chunk instead of skipping it.
        day = date(2026, 10, 9)
        calls = self._collect_chunks(monkeypatch, day, day)
        assert calls == [(day, day, "E")]

    def test_month_span_fetches_contiguous_chunks(self, monkeypatch):
        calls = self._collect_chunks(monkeypatch, date(2026, 5, 10), date(2026, 7, 5))
        assert calls == [
            (date(2026, 5, 10), date(2026, 5, 31), "E"),
            (date(2026, 6, 1), date(2026, 6, 30), "E"),
            (date(2026, 7, 1), date(2026, 7, 5), "E"),
        ]

    def test_year_boundary_chunks(self, monkeypatch):
        calls = self._collect_chunks(monkeypatch, date(2026, 12, 30), date(2027, 1, 2))
        assert calls == [
            (date(2026, 12, 30), date(2026, 12, 31), "E"),
            (date(2027, 1, 1), date(2027, 1, 2), "E"),
        ]

    def test_lang_is_forwarded_to_fetch_chunk(self, monkeypatch):
        day = date(2026, 10, 9)
        calls = self._collect_chunks(monkeypatch, day, day, lang="ZH")
        assert calls == [(day, day, "ZH")]

    def test_inverted_range_raises(self):
        client = HKEXClient()
        with pytest.raises(ValueError, match="after date_to"):
            client._fetch_all_chunks("1", date(2026, 6, 2), date(2026, 6, 1))


class TestCancellationTagDetection:
    def test_active_when_no_tag(self):
        status, clean = HKEXClient._parse_cancellation_tag("Monthly Returns")
        assert status == "active"
        assert clean == "Monthly Returns"

    def test_active_when_empty(self):
        status, clean = HKEXClient._parse_cancellation_tag("")
        assert status == "active"
        assert clean == ""

    def test_cancelled_superseded(self):
        text = "(Cancelled since Headlines Superseded and Replaced) Announcements and Notices - [Final Results]"
        status, clean = HKEXClient._parse_cancellation_tag(text)
        assert status == "cancelled_superseded"
        assert "Cancelled" not in clean
        assert "Final Results" in clean

    def test_cancelled_reissued(self):
        text = "(Cancelled and Reissued) Monthly Returns"
        status, clean = HKEXClient._parse_cancellation_tag(text)
        assert status == "cancelled_reissued"
        assert clean == "Monthly Returns"

    def test_headlines_revised(self):
        text = "(Headlines Revised) Announcements and Notices - [Final Results / Modified Report by Auditors]"
        status, clean = HKEXClient._parse_cancellation_tag(text)
        assert status == "headlines_revised"
        assert "Revised" not in clean
        assert "Final Results" in clean

    def test_three_patterns_defined(self):
        assert len(CANCELLATION_PATTERNS) == 3

    def test_parse_single_record_with_cancelled_tag(self):
        raw = {
            "NEWS_ID": "202406280007",
            "TITLE": "MONTHLY RETURN",
            "STOCK_NAME": "CHINA OCEAN GP",
            "LONG_TEXT": "(Cancelled and Reissued) Monthly Returns",
            "SHORT_TEXT": "",
            "FILE_LINK": "/listedco/listconews/sehk/2024/0628/202406280007.pdf",
            "FILE_TYPE": "PDF",
            "DATE_TIME": "28/06/2024 22:50",
        }
        result = HKEXClient._parse_single_record(raw, "08047")
        assert result["status"] == "cancelled_reissued"
        assert result["filing_type"] == "Monthly Returns"
        assert result["news_id"] == "202406280007"

    def test_parse_single_record_active(self):
        raw = {
            "NEWS_ID": "202404070007",
            "TITLE": "MONTHLY RETURN",
            "STOCK_NAME": "CHINA OCEAN GP",
            "LONG_TEXT": "Monthly Returns",
            "SHORT_TEXT": "",
            "FILE_LINK": "/listedco/listconews/sehk/2024/0407/202404070007.pdf",
            "FILE_TYPE": "PDF",
            "DATE_TIME": "07/04/2024 20:55",
        }
        result = HKEXClient._parse_single_record(raw, "08047")
        assert result["status"] == "active"
        assert result["filing_type"] == "Monthly Returns"

    def test_parse_single_record_empty_raw(self):
        result = HKEXClient._parse_single_record({}, "08047")
        assert result["status"] == "active"
        assert result["news_id"] == ""

    def test_clean_text_strips_html(self):
        assert HKEXClient._clean_text("<b>Hello</b> &amp; World") == "Hello & World"
