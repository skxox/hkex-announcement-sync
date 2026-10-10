"""
API endpoints for triggering, monitoring, and reviewing sync operations.

触发、监控和查看同步操作的 API 端点。
"""

import asyncio
import json
import logging
import uuid
from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import get_db
from app.models import Announcement, SyncLog, SyncStatus
from app.schemas.sync import (
    ReconcileRequest,
    SyncLogResponse,
    SyncProgress,
    SyncRequest,
    SyncStatusResponse,
    SyncSummaryResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sync", tags=["sync"])


async def _run_sync_inline(
    sync_log_id: uuid.UUID,
    stock_codes: list[str] | None,
    mode: str,
) -> None:
    """
    Execute sync directly in-process, recording progress to the sync_logs table.

    在进程内直接执行同步，并将进度记录到 sync_logs 表。
    """
    from app.config import Settings
    from app.database import async_session_factory
    from app.services.sync_service import SyncService

    async with async_session_factory() as db:
        sync_log = await db.get(SyncLog, sync_log_id)
        if not sync_log:
            return

        sync_log.status = SyncStatus.RUNNING
        sync_log.started_at = datetime.utcnow()
        await db.commit()

        settings = Settings()
        if stock_codes:
            settings = settings.model_copy(update={"SYNC_STOCK_CODES": ",".join(stock_codes)})

        start = datetime.utcnow()

        try:
            # Constructed inside try so a failure (e.g. bad storage config)
            # marks the SyncLog FAILED instead of leaving it RUNNING forever.
            service = SyncService(settings)
            result = await asyncio.to_thread(service.run, mode=mode)
            elapsed = (datetime.utcnow() - start).total_seconds()

            sync_log.status = SyncStatus.SUCCESS if not result.get("errors") else SyncStatus.FAILED
            sync_log.total = result.get("total", 0)
            sync_log.synced = result.get("synced", 0)
            sync_log.skipped = result.get("skipped", 0)
            sync_log.failed = result.get("failed", 0)
            sync_log.finished_at = datetime.utcnow()
            sync_log.duration_seconds = elapsed
            if result.get("errors"):
                sync_log.error = "; ".join(result["errors"][:3])

        except Exception as exc:
            elapsed = (datetime.utcnow() - start).total_seconds()
            sync_log.status = SyncStatus.FAILED
            sync_log.finished_at = datetime.utcnow()
            sync_log.duration_seconds = elapsed
            sync_log.error = str(exc)[:500]
            logger.exception("Inline sync task %s failed", sync_log_id)

        await db.commit()
        logger.info("Inline sync task %s completed", sync_log_id)

        # Send sync result notification. The notifier uses a blocking HTTP
        # client, so run it in a thread to keep the event loop responsive.
        if settings.NOTIFIER_ENABLED and settings.NOTIFIER_ON_SYNC:
            try:
                from app.notifiers.factory import create_notifier

                notifier = create_notifier(settings)
                if notifier:
                    await asyncio.to_thread(notifier.send_sync_result, sync_log)
            except Exception:
                logger.exception("Failed to send sync result notification")


@router.post("", response_model=dict)
async def trigger_sync(request: SyncRequest):
    """
    Trigger a sync task via Celery or inline execution.

    触发同步任务，通过 Celery 或内联执行。
    """
    settings = get_settings()
    stock_codes_str = ",".join(request.stock_codes) if request.stock_codes else ",".join(settings.stock_codes)

    # Prevent concurrent sync execution and reset scheduler backoff
    from app.services.scheduler import is_sync_running, reset_scheduler_interval

    if is_sync_running():
        raise HTTPException(status_code=409, detail="A sync task is already running")

    reset_scheduler_interval()

    if settings.CELERY_ENABLED:
        from app.tasks.sync_tasks import sync_announcements_task

        task = sync_announcements_task.delay(
            stock_codes=request.stock_codes,
            mode=request.mode.value,
            date_from=request.date_from.isoformat() if request.date_from else None,
            date_to=request.date_to.isoformat() if request.date_to else None,
        )
        return {"task_id": task.id}

    # Inline mode — create a SyncLog record and run in background.
    # Claim the running flag synchronously (before any await) so a concurrent
    # POST in the same tick still gets 409; the spawned task releases it.
    from app.services.scheduler import set_sync_running

    set_sync_running(True)
    try:
        from app.database import async_session_factory

        async with async_session_factory() as db:
            sync_log = SyncLog(
                stock_codes=stock_codes_str,
                mode=request.mode.value,
                status=SyncStatus.PENDING,
            )
            db.add(sync_log)
            await db.commit()
            await db.refresh(sync_log)
            log_id = sync_log.id

        async def _run_and_release() -> None:
            try:
                await _run_sync_inline(
                    log_id,
                    request.stock_codes,
                    request.mode.value,
                )
            finally:
                set_sync_running(False)

        asyncio.create_task(_run_and_release())
    except Exception:
        set_sync_running(False)
        raise

    return {"task_id": str(log_id)}


async def _run_reconcile_inline(
    sync_log_id: uuid.UUID,
    stock_codes: list[str],
    date_from: date | None,
    date_to: date | None,
    days_back: int,
) -> None:
    """
    Execute reconciliation directly in-process, recording progress to the sync_logs table.

    在进程内直接执行对账，并将进度记录到 sync_logs 表。
    """
    from app.config import Settings
    from app.database import async_session_factory
    from app.services.reconcile_service import ReconcileService

    async with async_session_factory() as db:
        sync_log = await db.get(SyncLog, sync_log_id)
        if not sync_log:
            return

        sync_log.status = SyncStatus.RUNNING
        sync_log.started_at = datetime.utcnow()
        await db.commit()

        settings = Settings()
        start = datetime.utcnow()

        try:
            service = ReconcileService(settings)
            result = await asyncio.to_thread(
                service.run,
                stock_codes=stock_codes,
                date_from=date_from,
                date_to=date_to,
                days_back=days_back,
            )
            elapsed = (datetime.utcnow() - start).total_seconds()

            sync_log.status = SyncStatus.SUCCESS
            sync_log.total = result.get("total", 0)
            sync_log.synced = result.get("updated", 0)
            sync_log.skipped = result.get("unchanged", 0)
            sync_log.failed = result.get("failed", 0)
            sync_log.finished_at = datetime.utcnow()
            sync_log.duration_seconds = elapsed

        except Exception as exc:
            sync_log.status = SyncStatus.FAILED
            sync_log.finished_at = datetime.utcnow()
            sync_log.duration_seconds = (datetime.utcnow() - start).total_seconds()
            sync_log.error = str(exc)[:500]
            logger.exception("Inline reconcile task %s failed", sync_log_id)

        await db.commit()
        logger.info("Inline reconcile task %s completed", sync_log_id)


@router.post("/reconcile", response_model=dict)
async def trigger_reconcile(request: ReconcileRequest):
    """
    Trigger a reconciliation to detect announcement status changes.

    触发对账操作，检测公告状态变更。
    """
    from app.services.scheduler import is_sync_running, set_sync_running

    if is_sync_running():
        raise HTTPException(status_code=409, detail="A sync task is already running")

    from app.database import async_session_factory

    settings = get_settings()
    stock_codes = request.stock_codes or settings.stock_codes
    stock_codes_str = ",".join(stock_codes)

    # Claim the running flag synchronously (before any await); the spawned
    # task releases it, so reconcile and sync mutually exclude each other.
    set_sync_running(True)
    try:
        async with async_session_factory() as db:
            sync_log = SyncLog(
                stock_codes=stock_codes_str,
                mode="reconcile",
                status=SyncStatus.PENDING,
            )
            db.add(sync_log)
            await db.commit()
            await db.refresh(sync_log)
            log_id = sync_log.id

        async def _run_and_release() -> None:
            try:
                await _run_reconcile_inline(
                    log_id,
                    stock_codes,
                    request.date_from,
                    request.date_to,
                    request.days_back,
                )
            finally:
                set_sync_running(False)

        asyncio.create_task(_run_and_release())
    except Exception:
        set_sync_running(False)
        raise

    return {"task_id": str(log_id)}


@router.get("/status/{task_id}", response_model=SyncStatusResponse)
async def get_sync_status(task_id: str):
    """
    Query the current status of a sync task by its task ID.

    通过任务 ID 查询同步任务的当前状态。
    """
    settings = get_settings()

    if settings.CELERY_ENABLED:
        from celery.result import AsyncResult

        result = AsyncResult(task_id)

        if result.state == "PENDING":
            return SyncStatusResponse(task_id=task_id, status="pending", progress=SyncProgress())
        elif result.state == "RUNNING":
            meta = result.info or {}
            progress = meta.get("progress", {})
            return SyncStatusResponse(task_id=task_id, status="running", progress=SyncProgress(**progress))
        elif result.state == "SUCCESS":
            info = result.result or {}
            return SyncStatusResponse(
                task_id=task_id,
                status="success",
                progress=SyncProgress(
                    total=info.get("total", 0),
                    synced=info.get("synced", 0),
                    skipped=info.get("skipped", 0),
                    failed=info.get("failed", 0),
                ),
            )
        elif result.state == "FAILURE":
            return SyncStatusResponse(task_id=task_id, status="failed", error=str(result.info))
        else:
            return SyncStatusResponse(task_id=task_id, status=result.state.lower())

    # Inline mode — look up SyncLog in database
    from app.database import async_session_factory

    try:
        log_uuid = uuid.UUID(task_id)
    except ValueError:
        return SyncStatusResponse(task_id=task_id, status="not_found", error="Invalid task ID")

    async with async_session_factory() as db:
        sync_log = await db.get(SyncLog, log_uuid)
        if not sync_log:
            return SyncStatusResponse(task_id=task_id, status="not_found", error="Task not found")

        return SyncStatusResponse(
            task_id=task_id,
            status=sync_log.status.value,
            progress=SyncProgress(
                total=sync_log.total,
                synced=sync_log.synced,
                skipped=sync_log.skipped,
                failed=sync_log.failed,
            ),
            error=sync_log.error,
        )


@router.get("/summary", response_model=SyncSummaryResponse)
async def get_sync_summary(db: AsyncSession = Depends(get_db)):
    """
    Get a summary of the most recent sync and overall stats.

    获取最近一次同步的摘要和整体统计信息。
    """
    last_log = await db.execute(
        select(SyncLog)
        .where(SyncLog.status.in_([SyncStatus.SUCCESS, SyncStatus.FAILED]))
        .order_by(SyncLog.created_at.desc())
        .limit(1)
    )
    last = last_log.scalar_one_or_none()

    total_syncs = await db.execute(select(func.count(SyncLog.id)))
    total_announcements = await db.execute(select(func.count(Announcement.id)))

    return SyncSummaryResponse(
        last_sync_at=last.finished_at if last else None,
        last_sync_status=last.status.value if last else None,
        last_sync_stock_codes=last.stock_codes if last else None,
        last_sync_duration_seconds=last.duration_seconds if last else None,
        last_sync_synced=last.synced if last else 0,
        last_sync_skipped=last.skipped if last else 0,
        last_sync_failed=last.failed if last else 0,
        total_syncs=total_syncs.scalar() or 0,
        total_announcements=total_announcements.scalar() or 0,
    )


@router.get("/logs", response_model=list[SyncLogResponse])
async def list_sync_logs(
    page: int = 1,
    page_size: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """
    List recent sync operation logs.

    列出最近的同步操作日志。
    """
    offset = (page - 1) * page_size
    result = await db.execute(
        select(SyncLog).order_by(SyncLog.created_at.desc()).offset(offset).limit(page_size)
    )
    logs = result.scalars().all()
    return logs


@router.get("/events")
async def sync_events():
    """
    SSE endpoint for real-time sync status updates.

    SSE 端点，用于实时同步状态推送。

    Clients receive events when syncs complete or scheduler state changes.
    """
    from app.services.scheduler import get_current_interval, subscribe_sse, unsubscribe_sse

    queue = subscribe_sse()

    async def event_generator():
        try:
            # Send initial state
            yield f"event: scheduler_state\ndata: {json.dumps({'interval': get_current_interval()})}\n\n"

            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"event: {event['event']}\ndata: {json.dumps(event['data'])}\n\n"
                except asyncio.TimeoutError:
                    yield "event: heartbeat\ndata: {}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            unsubscribe_sse(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.get("/scheduler-status")
async def scheduler_status():
    """
    Get current scheduler state (running, interval, smart backoff).

    获取调度器当前状态（是否运行、当前间隔、智能降频状态）。
    """
    from app.services.scheduler import get_current_interval, is_sync_running

    settings = get_settings()
    return {
        "enabled": settings.SCHEDULER_ENABLED,
        "running": is_sync_running(),
        "base_interval": settings.SCHEDULER_INTERVAL_SECONDS,
        "current_interval": get_current_interval(),
        "smart_backoff": settings.SCHEDULER_SMART_BACKOFF,
        "max_interval": settings.SCHEDULER_MAX_INTERVAL_SECONDS,
    }
