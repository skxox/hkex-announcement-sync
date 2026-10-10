"""Regression tests for sync service session handling."""

import asyncio

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import Settings
from app.models import Announcement, Base
from app.services.sync_service import SyncService


class TestPerStockRollback:
    async def test_failed_stock_does_not_poison_session(self, monkeypatch):
        """A flush failure inside one stock's sync must not leave the shared
        session in pending-rollback state for the remaining stocks."""
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(engine, expire_on_commit=False)

        async with factory() as db:
            db.add(Announcement(stock_code="00700", news_id="dup-1"))
            await db.commit()

            service = SyncService(db, Settings())

            async def failing_stock_async(client, downloader, stock_code, task_state, mode="incremental", date_to=None):
                # Violates the (stock_code, news_id) unique index on flush.
                db.add(Announcement(stock_code="00700", news_id="dup-1"))
                await db.flush()
                raise RuntimeError("boom after flush")

            monkeypatch.setattr(service, "_sync_stock_async", failing_stock_async)

            # Production invokes _sync_stock via asyncio.to_thread from the
            # scheduler/API coroutine; replicate that so the nested loop runs
            # in a worker thread like it does for real.
            with pytest.raises(IntegrityError):
                await asyncio.to_thread(service._sync_stock, None, None, "00700", None)

            # ...but the session must still be usable for the next stock.
            # Before the rollback fix this raised PendingRollbackError.
            result = await db.execute(select(func.count()).select_from(Announcement))
            assert result.scalar() == 1

        await engine.dispose()
