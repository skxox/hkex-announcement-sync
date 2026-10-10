"""Regression tests for sync service session handling and date windows."""

import asyncio
from datetime import date

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.config import Settings
from app.models import Announcement, Base
from app.services.sync_service import SyncService


class TestPerStockRollback:
    async def test_failed_stock_does_not_poison_session(self, monkeypatch):
        """A flush failure inside one stock's sync must not leave the session
        in pending-rollback state for the remaining stocks."""
        engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        monkeypatch.setattr("app.database.create_run_engine", lambda settings=None: engine)
        factory = async_sessionmaker(engine, expire_on_commit=False)

        async with factory() as db:
            db.add(Announcement(stock_code="00700", news_id="dup-1"))
            await db.commit()

        service = SyncService(Settings(SYNC_STOCK_CODES="00700,09988"))
        calls: list[str] = []

        async def fake_sync_stock_async(client, downloader, stock_code, task_state, mode="incremental",
                                        date_from=None, date_to=None):
            calls.append(stock_code)
            if stock_code == "00700":
                # Violates the (stock_code, news_id) unique index on flush.
                service._db.add(Announcement(stock_code="00700", news_id="dup-1"))
                await service._db.flush()
                raise RuntimeError("unreachable")
            # Second stock: a plain query must work -- before the rollback fix
            # it raised PendingRollbackError after the first stock's failure.
            result = await service._db.execute(select(func.count()).select_from(Announcement))
            return 0, result.scalar(), 0, []

        monkeypatch.setattr(service, "_sync_stock_async", fake_sync_stock_async)

        # Production invokes run() via asyncio.to_thread from scheduler/API.
        result = await asyncio.to_thread(service.run)

        assert calls == ["00700", "09988"]
        assert result["failed"] == 1
        assert len(result["errors"]) == 1
        # The second stock's query succeeded and counted only the seeded row.
        assert result["skipped"] == 1

        await engine.dispose()


class TestExplicitDateRange:
    async def test_explicit_range_overrides_mode_window(self, monkeypatch):
        """POST /api/sync date range must reach the per-stock pipeline."""
        engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        monkeypatch.setattr("app.database.create_run_engine", lambda settings=None: engine)

        captured: dict = {}

        async def fake_sync_stock_async(client, downloader, stock_code, task_state, mode="incremental",
                                        date_from=None, date_to=None):
            captured["date_from"], captured["date_to"] = date_from, date_to
            return 0, 0, 0, []

        service = SyncService(Settings())
        monkeypatch.setattr(service, "_sync_stock_async", fake_sync_stock_async)

        await asyncio.to_thread(
            service.run,
            mode="incremental",
            date_from=date(2026, 1, 1),
            date_to=date(2026, 1, 31),
        )

        assert captured == {"date_from": date(2026, 1, 1), "date_to": date(2026, 1, 31)}
        await engine.dispose()
