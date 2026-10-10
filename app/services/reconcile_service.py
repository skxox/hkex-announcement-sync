"""
Reconciliation service for detecting HKEX announcement status changes.

港交所公告状态变更检测的对账服务。

Re-fetches HKEX data for a date range and compares cancellation tags
with existing database records, updating status when changes are detected.
"""

import asyncio
import logging
from datetime import date, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.scraper.hkex_client import HKEXClient
from app.services import announcement_service
from app.utils.time import hk_today

logger = logging.getLogger(__name__)


class ReconcileService:
    """
    Re-fetches HKEX data and reconciles cancellation status with existing DB records.

    重新获取港交所数据并与数据库中的现有记录核对取消状态。

    Flow:
    1. For each configured stock code, determine a date range
    2. Fetch announcements from HKEX for that range
    3. Compare status: if HKEX record has a cancellation tag and DB record is active, update
    4. Bulk update changed statuses
    """

    def __init__(self, db: AsyncSession, settings: Settings | None = None):
        self._db = db
        self._settings = settings or Settings()

    def run(
        self,
        stock_codes: list[str] | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        days_back: int = 30,
    ) -> dict[str, Any]:
        """Run reconciliation synchronously."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self._run_async(stock_codes, date_from, date_to, days_back)
            )
        finally:
            loop.close()

    async def _run_async(
        self,
        stock_codes: list[str] | None,
        date_from: date | None,
        date_to: date | None,
        days_back: int,
    ) -> dict[str, Any]:
        codes = stock_codes or self._settings.stock_codes
        date_to = date_to or hk_today()
        date_from = date_from or (date_to - timedelta(days=days_back))

        total_updated = 0
        total_unchanged = 0
        total_checked = 0
        total_failed = 0
        errors: list[str] = []

        with HKEXClient(self._settings) as client:
            for stock_code in codes:
                try:
                    u, uc, checked = await self._reconcile_stock(
                        client, stock_code, date_from, date_to
                    )
                    total_updated += u
                    total_unchanged += uc
                    total_checked += checked
                except Exception as exc:
                    # Roll back so one stock's failure does not poison the shared
                    # session for the remaining stocks (PendingRollbackError).
                    try:
                        await self._db.rollback()
                    except Exception:
                        logger.warning("Rollback after failed reconcile of %s also failed", stock_code, exc_info=True)
                    msg = f"Failed to reconcile {stock_code}: {exc}"
                    logger.exception(msg)
                    errors.append(msg)
                    total_failed += 1

        return {
            "total": total_checked + total_failed,
            "updated": total_updated,
            "unchanged": total_unchanged,
            "failed": total_failed,
            "errors": errors,
        }

    async def _reconcile_stock(
        self,
        client: HKEXClient,
        stock_code: str,
        date_from: date,
        date_to: date,
    ) -> tuple[int, int, int]:
        """
        Reconcile a single stock code.

        Returns (updated_count, unchanged_count, total_checked).
        """
        stock_id = client.get_stock_id(stock_code)
        records = client.search_announcements(
            stock_id, date_from, date_to, stock_code=stock_code
        )
        logger.info("Reconciliation: fetched %d records for %s", len(records), stock_code)

        news_ids = [r["news_id"] for r in records if r["news_id"]]
        existing = await announcement_service.get_announcements_by_news_ids(
            self._db, stock_code, news_ids
        )

        updates = []
        for r in records:
            nid = r["news_id"]
            new_status = r.get("status", "active")
            if nid in existing and existing[nid].status != new_status:
                updates.append({
                    "news_id": nid,
                    "status": new_status,
                    "filing_type_en": r.get("filing_type_en"),
                    "filing_type_zh": r.get("filing_type_zh"),
                    "filing_type_cn": r.get("filing_type_cn"),
                })

        updated = await announcement_service.bulk_update_status(self._db, updates)
        await self._db.commit()

        logger.info(
            "Reconciliation for %s: %d updated of %d checked",
            stock_code, updated, len(records),
        )
        return updated, len(records) - updated, len(records)
