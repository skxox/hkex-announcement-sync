"""Tests for manual sync/reconcile concurrency protection."""

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.models import Base
from app.services.scheduler import is_sync_running, set_sync_running


@pytest.fixture
async def api_env(monkeypatch):
    """App client with an in-memory DB and a controllable fake inline sync."""
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr("app.database.async_session_factory", factory)

    release = asyncio.Event()

    async def fake_run_inline(log_id, stock_codes, mode, date_from=None, date_to=None):
        await release.wait()

    monkeypatch.setattr("app.api.sync._run_sync_inline", fake_run_inline)

    from app.main import app

    set_sync_running(False)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client, release

    set_sync_running(False)
    await engine.dispose()


class TestSyncOverlapProtection:
    async def test_second_manual_sync_rejected_while_first_running(self, api_env):
        client, release = api_env

        first = await client.post("/api/sync", json={"stock_codes": ["00700"], "mode": "incremental"})
        assert first.status_code == 200
        assert is_sync_running() is True

        second = await client.post("/api/sync", json={"stock_codes": ["00700"], "mode": "incremental"})
        assert second.status_code == 409

        # Flag must be released once the task finishes.
        release.set()
        for _ in range(50):
            if not is_sync_running():
                break
            await asyncio.sleep(0.01)
        assert is_sync_running() is False

    async def test_reconcile_blocks_sync_and_releases_flag(self, api_env):
        client, release = api_env

        first = await client.post("/api/sync", json={"stock_codes": ["00700"]})
        assert first.status_code == 200

        reconcile = await client.post("/api/sync/reconcile", json={})
        assert reconcile.status_code == 409

        release.set()
        for _ in range(50):
            if not is_sync_running():
                break
            await asyncio.sleep(0.01)
        assert is_sync_running() is False
