"""
Built-in smart polling scheduler for automatic announcement sync.

内置智能轮询调度器，用于自动同步公告。

Replaces Celery Beat for standalone mode. Runs as an asyncio background task
with adaptive polling frequency and SSE event broadcasting.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from app.config import Settings, get_settings
from app.models import SyncLog, SyncStatus

logger = logging.getLogger(__name__)

# --- Global state ---

_sync_running: bool = False
_current_interval: int = 0
_no_data_count: int = 0
_scheduler_task: asyncio.Task | None = None

# SSE subscribers: list of asyncio.Queue
_sse_subscribers: list[asyncio.Queue] = []


def is_sync_running() -> bool:
    """
    Check if a sync task is currently in progress.

    检查是否有同步任务正在执行。
    """
    return _sync_running


def set_sync_running(value: bool) -> None:
    """
    Set the sync-running flag from outside the scheduler.

    供调度器之外的模块（如 API 内联触发的同步/对账）设置同步运行标志，
    使并发保护对手动触发的任务同样生效。

    Args:
    value: True while the task is running, False when it finishes.
    任务运行期间为 True，结束时为 False。

    """
    global _sync_running
    _sync_running = value


def get_current_interval() -> int:
    """
    Get the current polling interval in seconds.

    获取当前轮询间隔（秒）。
    """
    return _current_interval


def reset_scheduler_interval() -> None:
    """
    Reset polling interval to the base value.

    重置轮询间隔到基础值。Called after manual sync.
    """
    global _current_interval, _no_data_count
    settings = get_settings()
    _current_interval = settings.SCHEDULER_INTERVAL_SECONDS
    _no_data_count = 0


def subscribe_sse() -> asyncio.Queue:
    """
    Subscribe to SSE events. Returns a queue that receives event dicts.

    订阅 SSE 事件。返回一个接收事件字典的队列。
    """
    q: asyncio.Queue = asyncio.Queue()
    _sse_subscribers.append(q)
    return q


def unsubscribe_sse(q: asyncio.Queue) -> None:
    """
    Unsubscribe from SSE events.

    取消订阅 SSE 事件。
    """
    if q in _sse_subscribers:
        _sse_subscribers.remove(q)


def _broadcast_sse(event: str, data: dict[str, Any]) -> None:
    """
    Broadcast an event to all SSE subscribers.

    向所有 SSE 订阅者广播事件。
    """
    for q in _sse_subscribers:
        try:
            q.put_nowait({"event": event, "data": data})
        except asyncio.QueueFull:
            pass


async def run_scheduler() -> None:
    """
    Main scheduler loop. Runs as a background asyncio task.

    调度器主循环。作为后台 asyncio 任务运行。

    Smart polling logic:
    - Base interval from SCHEDULER_INTERVAL_SECONDS (default 15 min)
    - When SCHEDULER_SMART_BACKOFF is enabled:
      - Double interval each time no new data is found
      - Cap at SCHEDULER_MAX_INTERVAL_SECONDS
      - Reset to base interval when new data is found or manual sync fires
    """
    global _sync_running, _current_interval, _no_data_count

    settings = get_settings()
    _current_interval = settings.SCHEDULER_INTERVAL_SECONDS

    logger.info(
        "Scheduler started: base_interval=%ds, smart_backoff=%s, max_interval=%ds",
        settings.SCHEDULER_INTERVAL_SECONDS,
        settings.SCHEDULER_SMART_BACKOFF,
        settings.SCHEDULER_MAX_INTERVAL_SECONDS,
    )

    _broadcast_sse("scheduler_started", {
        "base_interval": settings.SCHEDULER_INTERVAL_SECONDS,
        "smart_backoff": settings.SCHEDULER_SMART_BACKOFF,
    })

    while True:
        try:
            await asyncio.sleep(_current_interval)
        except asyncio.CancelledError:
            logger.info("Scheduler stopped")
            _broadcast_sse("scheduler_stopped", {})
            break

        # Skip if a sync is already running (manual or overlapping)
        if _sync_running:
            logger.debug("Skipping scheduled sync: another sync is running")
            continue

        try:
            result = await _execute_scheduled_sync(settings)

            # Adjust interval based on result
            if result.get("synced", 0) > 0:
                # Found new data → reset to base interval
                _current_interval = settings.SCHEDULER_INTERVAL_SECONDS
                _no_data_count = 0
            elif settings.SCHEDULER_SMART_BACKOFF:
                # No new data → back off
                _no_data_count += 1
                _current_interval = min(
                    _current_interval * 2,
                    settings.SCHEDULER_MAX_INTERVAL_SECONDS,
                )
                logger.info(
                    "No new data (%d consecutive), interval now %ds",
                    _no_data_count,
                    _current_interval,
                )

        except Exception:
            logger.exception("Scheduled sync failed")


async def _execute_scheduled_sync(settings: Settings) -> dict[str, Any]:
    """
    Execute a single scheduled sync cycle.

    执行一次调度同步周期。

    Creates a SyncLog record, runs the sync pipeline, and broadcasts results.
    """
    global _sync_running

    _sync_running = True
    from app.database import async_session_factory

    try:
        # Create SyncLog record
        async with async_session_factory() as db:
            sync_log = SyncLog(
                stock_codes=settings.SYNC_STOCK_CODES,
                mode="incremental",
                status=SyncStatus.RUNNING,
                started_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
            db.add(sync_log)
            await db.commit()
            await db.refresh(sync_log)
            log_id = sync_log.id

        # Run sync. The service creates its own loop/session; the SyncLog is
        # updated on this coroutine's session afterwards.
        from app.services.sync_service import SyncService

        service = SyncService(settings)
        start = datetime.now(timezone.utc).replace(tzinfo=None)

        try:
            result = await asyncio.to_thread(service.run)
            elapsed = (datetime.now(timezone.utc).replace(tzinfo=None) - start).total_seconds()

            status = SyncStatus.SUCCESS if not result.get("errors") else SyncStatus.FAILED
        except Exception as exc:
            elapsed = (datetime.now(timezone.utc).replace(tzinfo=None) - start).total_seconds()
            result = {"total": 0, "synced": 0, "skipped": 0, "failed": 1, "errors": [str(exc)]}
            status = SyncStatus.FAILED

        # Update SyncLog
        async with async_session_factory() as db:
            sync_log = await db.get(SyncLog, log_id)
            if sync_log:
                sync_log.status = status
                sync_log.total = result.get("total", 0)
                sync_log.synced = result.get("synced", 0)
                sync_log.skipped = result.get("skipped", 0)
                sync_log.failed = result.get("failed", 0)
                sync_log.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
                sync_log.duration_seconds = elapsed
                if result.get("errors"):
                    sync_log.error = "; ".join(result["errors"][:3])
                await db.commit()

                # Send notification. The notifier uses a blocking HTTP client,
                # so run it in a thread to keep the event loop responsive.
                if settings.NOTIFIER_ENABLED and settings.NOTIFIER_ON_SYNC:
                    try:
                        from app.notifiers.factory import create_notifier

                        notifier = create_notifier(settings)
                        if notifier:
                            await asyncio.to_thread(notifier.send_sync_result, sync_log)
                    except Exception:
                        logger.exception("Scheduler notification failed")

        logger.info(
            "Scheduled sync completed: status=%s, synced=%d, skipped=%d, %.1fs",
            status.value,
            result.get("synced", 0),
            result.get("skipped", 0),
            elapsed,
        )

        # Run reconciliation after successful sync
        if settings.RECONCILE_ENABLED and status == SyncStatus.SUCCESS:
            try:
                from app.services.reconcile_service import ReconcileService

                reconcile_service = ReconcileService(settings)
                reconcile_result = await asyncio.to_thread(
                    reconcile_service.run,
                    days_back=settings.RECONCILE_DAYS_BACK,
                )
                logger.info(
                    "Post-sync reconciliation: %d updated, %d unchanged",
                    reconcile_result.get("updated", 0),
                    reconcile_result.get("unchanged", 0),
                )
                _broadcast_sse("reconcile_complete", {
                    "updated": reconcile_result.get("updated", 0),
                    "unchanged": reconcile_result.get("unchanged", 0),
                })
            except Exception:
                logger.exception("Post-sync reconciliation failed")

            # Broadcast SSE event
            _broadcast_sse("sync_complete", {
                "task_id": str(log_id),
                "status": status.value,
                "synced": result.get("synced", 0),
                "skipped": result.get("skipped", 0),
                "failed": result.get("failed", 0),
                "duration": round(elapsed, 1),
                "next_interval": _current_interval,
            })

            return result

    finally:
        _sync_running = False
