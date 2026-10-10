"""
Sync orchestration service for HKEX announcement pipeline.

港交所公告同步流水线的编排服务。
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.scraper.hkex_client import HKEXClient
from app.scraper.pdf_downloader import PDFDownloader
from app.services import announcement_service
from app.storage.factory import create_storage_backend
from app.utils.time import hk_today

logger = logging.getLogger(__name__)


class SyncService:
    """
    Orchestrates the full sync pipeline: scrape metadata -> dedup -> store -> download PDFs.

    编排完整的同步流水线：抓取元数据 -> 去重 -> 存储 -> 下载 PDF 文件。
    """

    def __init__(self, db: AsyncSession, settings: Settings | None = None):
        """
        Initialize the sync service with a database session and optional settings.

        使用数据库会话和可选配置初始化同步服务。

        Args:
        db: Async SQLAlchemy database session. / 异步 SQLAlchemy 数据库会话。
        settings: Application settings. Uses defaults if None.
        应用配置。为 None 时使用默认值。

        """
        self._db = db
        self._settings = settings or Settings()
        self._storage = create_storage_backend(self._settings)

    def run(self, task_state: dict | None = None, mode: str = "incremental") -> dict[str, Any]:
        """
        Run the full sync pipeline for all configured stock codes.

        对所有已配置的股票代码运行完整同步流水线。

        Args:
        task_state: Optional dict for reporting progress to Celery task.
        用于向 Celery 任务报告进度的可选字典。
        mode: Sync mode - 'incremental' or 'full'. / 同步模式。
        incremental: sync from last sync date; full: sync from HKEX_FULL_HISTORY_START.

        Returns:
        dict[str, Any]: Summary with keys: total, synced, skipped, failed, errors.
        包含 total、synced、skipped、failed、errors 键的摘要字典。

        """
        stock_codes = self._settings.stock_codes
        total_synced = 0
        total_skipped = 0
        total_failed = 0
        errors: list[str] = []

        with HKEXClient(self._settings) as client, PDFDownloader(self._storage, self._settings) as downloader:
            run_date_to = hk_today()
            for stock_code in stock_codes:
                try:
                    s, k, f, e = self._sync_stock(
                        client, downloader, stock_code, task_state, mode=mode, date_to=run_date_to
                    )
                    total_synced += s
                    total_skipped += k
                    total_failed += f
                    errors.extend(e)
                except Exception as exc:
                    msg = f"Failed to sync {stock_code}: {exc}"
                    logger.exception(msg)
                    errors.append(msg)
                    total_failed += 1

        return {
            "total": total_synced + total_skipped + total_failed,
            "synced": total_synced,
            "skipped": total_skipped,
            "failed": total_failed,
            "errors": errors,
        }

    def _sync_stock(
        self,
        client: HKEXClient,
        downloader: PDFDownloader,
        stock_code: str,
        task_state: dict | None,
        mode: str = "incremental",
        date_to: date | None = None,
    ) -> tuple[int, int, int, list[str]]:
        """
        Sync a single stock code by running the async pipeline in a new event loop.

        通过在新事件循环中运行异步流水线来同步单个股票代码。

        Args:
        client: HKEX scraper client instance. / 港交所抓取客户端实例。
        downloader: PDF downloader instance. / PDF 下载器实例。
        stock_code: The stock code to sync. / 要同步的股票代码。
        task_state: Optional progress tracking dict. / 可选的进度跟踪字典。
        mode: Sync mode - 'incremental' or 'full'. / 同步模式。

        Returns:
        tuple[int, int, int, list[str]]: (synced, skipped, failed, errors) counts.
        （已同步、已跳过、已失败、错误列表）计数。

        """
        import asyncio

        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self._sync_stock_async(client, downloader, stock_code, task_state, mode=mode, date_to=date_to)
            )
        except Exception:
            # A failed flush leaves the session in pending-rollback state; without
            # an explicit rollback every subsequent stock fails with
            # PendingRollbackError. Roll back on the same loop that ran the work.
            try:
                loop.run_until_complete(self._db.rollback())
            except Exception:
                logger.warning("Rollback after failed sync of %s also failed", stock_code, exc_info=True)
            raise
        finally:
            loop.close()

    async def _sync_stock_async(
        self,
        client: HKEXClient,
        downloader: PDFDownloader,
        stock_code: str,
        task_state: dict | None,
        mode: str = "incremental",
        date_to: date | None = None,
    ) -> tuple[int, int, int, list[str]]:
        """
        Async implementation of stock sync: resolve ID, fetch metadata, dedup, insert, download PDFs.

        股票同步的异步实现：解析 ID、获取元数据、去重、插入、下载 PDF。

        Steps / 步骤：
        1. Resolve stock ID from stock code. / 从股票代码解析股票 ID。
        2. Determine date range based on mode. / 根据模式确定日期范围。
        3. Fetch trilingual metadata from HKEX. / 从港交所获取三语元数据。
        4. Deduplicate by news_id. / 按 news_id 去重。
        5. Insert new records to database. / 将新记录插入数据库。
        6. Download PDFs for EN and ZH versions. / 下载英文和繁体中文版 PDF。
        7. Update DB with file storage info. / 使用文件存储信息更新数据库。

        Args:
        client: HKEX scraper client instance. / 港交所抓取客户端实例。
        downloader: PDF downloader instance. / PDF 下载器实例。
        stock_code: The stock code to sync. / 要同步的股票代码。
        task_state: Optional progress tracking dict. / 可选的进度跟踪字典。
        mode: Sync mode - 'incremental' syncs from last sync date; 'full' syncs from HKEX_FULL_HISTORY_START.

        Returns:
        tuple[int, int, int, list[str]]: (synced, skipped, failed, errors) counts.
        （已同步、已跳过、已失败、错误列表）计数。

        """
        # 1. Resolve stock ID
        stock_id = client.get_stock_id(stock_code)

        # 2. Determine date range based on mode
        if mode == "full":
            from datetime import datetime as dt

            start_str = self._settings.HKEX_FULL_HISTORY_START
            date_from = dt.strptime(start_str, "%Y-%m-%d").date()
        else:
            last_sync = await announcement_service.get_last_sync_date(self._db, stock_code)
            if last_sync:
                date_from = last_sync.date()
            else:
                date_from = hk_today() - timedelta(days=90)
        # HKEX dates are Hong Kong local time, so "today" must be HK time too,
        # otherwise a UTC server lags a day behind. date_to is computed once by
        # run() so a multi-stock run straddling HK midnight stays consistent.
        date_to = date_to or hk_today()

        # Guard against a water-mark in the future (e.g. manually inserted
        # records) silently producing an empty window forever.
        if date_from > date_to:
            logger.warning("date_from %s is after date_to %s, clamping to date_to", date_from, date_to)
            date_from = date_to

        # 3. Fetch trilingual metadata from HKEX (EN + ZH + SC conversion)
        records = client.search_announcements(stock_id, date_from, date_to, stock_code=stock_code)
        logger.info("Fetched %d trilingual records for %s", len(records), stock_code)

        # 4. Dedup by news_id. Records without a news_id cannot be deduplicated
        # and would violate the (stock_code, news_id) unique index on every
        # re-fetch of the same window, so they are skipped with a warning.
        existing_news_ids = await announcement_service.get_existing_news_ids(
            self._db, stock_code, [r["news_id"] for r in records if r["news_id"]]
        )
        empty_id_count = sum(1 for r in records if not r["news_id"])
        if empty_id_count:
            logger.warning("Skipping %d records for %s with empty news_id", empty_id_count, stock_code)
        new_records = [r for r in records if r["news_id"] and r["news_id"] not in existing_news_ids]
        logger.info("New records after dedup: %d", len(new_records))

        if not new_records:
            return 0, len(records), 0, []

        # 5. Insert metadata to DB
        db_records = []
        for r in new_records:
            db_records.append(
                {
                    "stock_code": r["stock_code"],
                    "news_id": r["news_id"],
                    "title_en": r.get("title_en", ""),
                    "title_zh": r.get("title_zh", ""),
                    "title_cn": r.get("title_cn", ""),
                    "stock_name_en": r.get("stock_name_en", ""),
                    "stock_name_zh": r.get("stock_name_zh", ""),
                    "stock_name_cn": r.get("stock_name_cn", ""),
                    "filing_type_en": r.get("filing_type_en", ""),
                    "filing_type_zh": r.get("filing_type_zh", ""),
                    "filing_type_cn": r.get("filing_type_cn", ""),
                    "short_text_en": r.get("short_text_en", ""),
                    "short_text_zh": r.get("short_text_zh", ""),
                    "short_text_cn": r.get("short_text_cn", ""),
                    "long_text_en": r.get("long_text_en", ""),
                    "long_text_zh": r.get("long_text_zh", ""),
                    "long_text_cn": r.get("long_text_cn", ""),
                    "hkex_url_en": r.get("hkex_url_en", ""),
                    "hkex_url_zh": r.get("hkex_url_zh", ""),
                    "file_type": r.get("file_type", "PDF"),
                    "announcement_date": r.get("announcement_date"),
                    "source": "auto",
                    "status": r.get("status", "active"),
                    "last_synced_at": datetime.now(timezone.utc).replace(tzinfo=None),
                }
            )

        await announcement_service.bulk_insert_announcements(self._db, db_records)
        await self._db.commit()

        # Send new announcements notification
        if self._settings.NOTIFIER_ENABLED and self._settings.NOTIFIER_ON_NEW:
            try:
                from app.notifiers.factory import create_notifier

                notifier = create_notifier(self._settings)
                if notifier:
                    notifier.send_new_announcements(new_records, stock_code)
            except Exception:
                logger.exception("Failed to send new announcement notification")

        # Re-fetch inserted records to get their IDs for PDF download
        inserted_news_ids = [r["news_id"] for r in db_records]
        from sqlalchemy import select

        from app.models import Announcement

        result = await self._db.execute(select(Announcement).where(Announcement.news_id.in_(inserted_news_ids)))
        inserted_announcements = list(result.scalars().all())

        # 6. Download PDFs (EN and ZH versions)
        download_tasks = []
        ann_map = {}
        for ann in inserted_announcements:
            if ann.hkex_url_en:
                key_en = f"{ann.stock_code}/{ann.id}_en.pdf"
                download_tasks.append((ann.hkex_url_en, key_en))
                ann_map[key_en] = (ann, "en")
            if ann.hkex_url_zh:
                key_zh = f"{ann.stock_code}/{ann.id}_zh.pdf"
                download_tasks.append((ann.hkex_url_zh, key_zh))
                ann_map[key_zh] = (ann, "zh")

        if download_tasks:
            download_results = downloader.download_batch(download_tasks)

            # 7. Update DB with file info
            for dr in download_results:
                if dr.success and dr.key in ann_map:
                    ann, lang_suffix = ann_map[dr.key]
                    if lang_suffix == "en":
                        ann.file_path_en = dr.file_path
                        ann.file_size_en = dr.file_size
                        ann.file_hash_en = dr.file_hash
                    else:
                        ann.file_path_zh = dr.file_path
                        ann.file_size_zh = dr.file_size
                        ann.file_hash_zh = dr.file_hash
                    ann.last_synced_at = datetime.now(timezone.utc).replace(tzinfo=None)

            await self._db.commit()

        # Update task state for progress tracking
        synced = len(new_records)
        skipped = len(records) - len(new_records)
        failed_dl = sum(1 for r in download_results if not r.success) if download_tasks else 0

        if task_state is not None:
            task_state["progress"] = {
                "total": len(records),
                "synced": synced,
                "skipped": skipped,
                "failed": failed_dl,
            }

        return synced, skipped, failed_dl, []
