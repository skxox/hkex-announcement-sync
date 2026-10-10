"""Tests for Pydantic request/response schemas."""
from datetime import date

from app.schemas.announcement import AnnouncementListParams, AnnouncementResponse
from app.schemas.sync import ReconcileRequest


class TestReconcileRequest:
    def test_defaults(self):
        req = ReconcileRequest()
        assert req.days_back == 30
        assert req.stock_codes is None
        assert req.date_from is None
        assert req.date_to is None

    def test_custom_values(self):
        req = ReconcileRequest(
            stock_codes=["00700"],
            date_from=date(2024, 1, 1),
            date_to=date(2024, 12, 31),
            days_back=60,
        )
        assert req.stock_codes == ["00700"]
        assert req.days_back == 60

    def test_status_default_in_response(self):
        resp = AnnouncementResponse(
            id="550e8400-e29b-41d4-a716-446655440000",
            stock_code="00700",
            news_id="202404300007",
            title="Test",
            stock_name="Test Corp",
            source="auto",
            created_at="2024-05-01T10:00:00",
            updated_at="2024-05-01T10:00:00",
        )
        assert resp.status == "active"


class TestAnnouncementListParams:
    def test_status_filter(self):
        params = AnnouncementListParams(status="cancelled_superseded")
        assert params.status == "cancelled_superseded"

    def test_no_status_filter(self):
        params = AnnouncementListParams()
        assert params.status is None
