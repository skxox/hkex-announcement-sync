"""Regression tests for the scheduled-sync cycle's return value and SSE event.

A dedent accident once moved `return result` and the sync_complete broadcast
inside the `if RECONCILE_ENABLED and status == SUCCESS:` block, so a FAILED
cycle (or RECONCILE_ENABLED=false) returned None and run_scheduler crashed on
`result.get(...)` every cycle, freezing the poll-interval backoff logic.
"""

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.config import Settings
from app.models import Base
from app.services.scheduler import _execute_scheduled_sync, subscribe_sse, unsubscribe_sse


@pytest.fixture
async def db_factory(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr("app.database.async_session_factory", factory)
    yield
    await engine.dispose()


def _install_fake_sync_service(monkeypatch, outcome):
    """Patch SyncService so the scheduled cycle runs without touching HKEX."""
    calls = []

    class FakeSyncService:
        def __init__(self, settings):
            pass

        def run(self, task_state=None):
            calls.append(True)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    monkeypatch.setattr("app.services.sync_service.SyncService", FakeSyncService)
    return calls


class TestScheduledSyncCycle:
    async def test_returns_result_and_broadcasts_when_reconcile_disabled(self, db_factory, monkeypatch):
        """RECONCILE_ENABLED=false must not stop the cycle returning its result."""
        fake_result = {"total": 3, "synced": 3, "skipped": 0, "failed": 0, "errors": []}
        calls = _install_fake_sync_service(monkeypatch, fake_result)

        settings = Settings(RECONCILE_ENABLED=False, NOTIFIER_ENABLED=False)
        queue = subscribe_sse()
        try:
            result = await _execute_scheduled_sync(settings)
        finally:
            unsubscribe_sse(queue)

        assert calls, "the fake sync service must have run"
        assert result == fake_result, "cycle must return the sync result dict, not None"
        event = queue.get_nowait()
        assert event["event"] == "sync_complete"
        assert event["data"]["synced"] == 3

    async def test_failed_sync_still_returns_result(self, db_factory, monkeypatch):
        """A FAILED cycle must still return a dict so run_scheduler can back off."""
        calls = _install_fake_sync_service(monkeypatch, RuntimeError("hkex down"))

        # Reconcile is skipped for a FAILED cycle regardless of this flag.
        settings = Settings(NOTIFIER_ENABLED=False)
        result = await _execute_scheduled_sync(settings)

        assert calls, "the fake sync service must have run"
        assert isinstance(result, dict)
        assert result["failed"] == 1
        assert result["errors"], "failure must be reported in the result"
