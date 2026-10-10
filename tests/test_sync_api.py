"""Tests for manual sync/reconcile concurrency protection and API wiring."""

import asyncio
import uuid
from datetime import date

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.models import Base, SyncLog, SyncStatus
from app.services.scheduler import is_sync_running, set_sync_running


@pytest.fixture
async def api_env(monkeypatch):
    """App client with an in-memory DB and controllable fake inline tasks."""
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr("app.database.async_session_factory", factory)

    sync_release = asyncio.Event()
    reconcile_release = asyncio.Event()
    sync_calls: list[dict] = []
    reconcile_calls: list[dict] = []

    async def fake_sync_inline(log_id, stock_codes, mode, date_from=None, date_to=None):
        sync_calls.append(
            {"log_id": log_id, "stock_codes": stock_codes, "mode": mode,
             "date_from": date_from, "date_to": date_to}
        )
        await sync_release.wait()

    async def fake_reconcile_inline(log_id, stock_codes, date_from, date_to, days_back):
        reconcile_calls.append(
            {"log_id": log_id, "stock_codes": stock_codes, "date_from": date_from,
             "date_to": date_to, "days_back": days_back}
        )
        await reconcile_release.wait()

    monkeypatch.setattr("app.api.sync._run_sync_inline", fake_sync_inline)
    from app.api import sync as sync_api

    real_reconcile_inline = sync_api._run_reconcile_inline
    monkeypatch.setattr("app.api.sync._run_reconcile_inline", fake_reconcile_inline)

    from app.main import app

    set_sync_running(False)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield {
            "client": client,
            "sync_release": sync_release,
            "reconcile_release": reconcile_release,
            "sync_calls": sync_calls,
            "reconcile_calls": reconcile_calls,
            "factory": factory,
            "real_reconcile_inline": real_reconcile_inline,
        }

    sync_release.set()
    reconcile_release.set()
    set_sync_running(False)
    await engine.dispose()


async def _wait_flag_released() -> None:
    for _ in range(100):
        if not is_sync_running():
            break
        await asyncio.sleep(0.01)
    assert is_sync_running() is False


class TestSyncOverlapProtection:
    async def test_second_manual_sync_rejected_while_first_running(self, api_env):
        client, release = api_env["client"], api_env["sync_release"]

        first = await client.post("/api/sync", json={"stock_codes": ["00700"], "mode": "incremental"})
        assert first.status_code == 200
        assert is_sync_running() is True

        second = await client.post("/api/sync", json={"stock_codes": ["00700"], "mode": "incremental"})
        assert second.status_code == 409

        # Flag must be released once the task finishes.
        release.set()
        await _wait_flag_released()

    async def test_reconcile_blocks_sync_and_releases_flag(self, api_env):
        client, release = api_env["client"], api_env["sync_release"]

        first = await client.post("/api/sync", json={"stock_codes": ["00700"]})
        assert first.status_code == 200

        reconcile = await client.post("/api/sync/reconcile", json={})
        assert reconcile.status_code == 409

        release.set()
        await _wait_flag_released()

    async def test_sync_blocked_while_reconcile_running(self, api_env):
        """A running reconcile must claim the flag and reject a concurrent sync.

        Pins the second half of the overlap fix: reconcile used to run without
        claiming the flag, so a sync could start mid-reconcile.
        """
        client, release = api_env["client"], api_env["reconcile_release"]

        reconcile = await client.post("/api/sync/reconcile", json={})
        assert reconcile.status_code == 200
        assert is_sync_running() is True

        sync = await client.post("/api/sync", json={"stock_codes": ["00700"]})
        assert sync.status_code == 409

        release.set()
        await _wait_flag_released()


class TestApiWiring:
    async def test_request_dates_forwarded_to_inline_run(self, api_env):
        """POST /api/sync date_from/date_to must reach the inline run, not be dropped."""
        env = api_env

        resp = await env["client"].post(
            "/api/sync",
            json={
                "stock_codes": ["00700"],
                "mode": "incremental",
                "date_from": "2026-01-01",
                "date_to": "2026-01-31",
            },
        )
        assert resp.status_code == 200

        env["sync_release"].set()
        await _wait_flag_released()

        assert len(env["sync_calls"]) == 1
        call = env["sync_calls"][0]
        assert call["date_from"] == date(2026, 1, 1)
        assert call["date_to"] == date(2026, 1, 31)
        assert call["mode"] == "incremental"

    async def test_inverted_date_range_rejected(self, api_env):
        resp = await api_env["client"].post(
            "/api/sync",
            json={"date_from": "2026-02-01", "date_to": "2026-01-01"},
        )
        assert resp.status_code == 422


class TestReconcileStatusHonesty:
    async def test_partial_errors_mark_synclog_failed(self, api_env, monkeypatch):
        """Reconcile with per-stock errors must record FAILED, not SUCCESS.

        Pins the reconcile half of the status-honesty fix: the inline runner
        used to set SUCCESS unconditionally.
        """
        env = api_env

        class FakeReconcileService:
            def __init__(self, settings):
                pass

            def run(self, stock_codes=None, date_from=None, date_to=None, days_back=30):
                return {
                    "total": 3, "updated": 2, "unchanged": 1, "missing": 0,
                    "failed": 0, "errors": ["Failed to reconcile 00700: boom"],
                }

        monkeypatch.setattr("app.services.reconcile_service.ReconcileService", FakeReconcileService)
        # Run the real inline runner (saved by the fixture) against the fake service.
        monkeypatch.setattr("app.api.sync._run_reconcile_inline", api_env["real_reconcile_inline"])

        resp = await env["client"].post("/api/sync/reconcile", json={})
        assert resp.status_code == 200
        log_id = uuid.UUID(resp.json()["task_id"])

        await _wait_flag_released()

        async with env["factory"]() as db:
            log = await db.get(SyncLog, log_id)
            assert log is not None
            assert log.status == SyncStatus.FAILED
            assert "boom" in (log.error or "")
            assert log.synced == 2  # updated
            assert log.skipped == 1  # unchanged
