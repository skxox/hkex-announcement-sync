"""
Database service for announcement CRUD operations.

公告 CRUD 操作的数据库服务模块。
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Announcement
from app.schemas.announcement import AnnouncementListParams


async def get_announcements(
    db: AsyncSession,
    params: AnnouncementListParams,
) -> tuple[list[Announcement], int]:
    """
    Get paginated announcements with optional filters for stock code and date range.

    获取带分页的公告列表，支持按股票代码和日期范围过滤。

    Args:
    db: Async database session. / 异步数据库会话。
    params: Query parameters including stock_code, date_from, date_to, page, page_size.
    查询参数，包括 stock_code、date_from、date_to、page、page_size。

    Returns:
    tuple[list[Announcement], int]: A tuple of (announcement items, total count).
    （公告项目列表，总数）的元组。

    """
    query = select(Announcement).where(Announcement.is_visible == True)  # noqa: E712

    if params.stock_code:
        query = query.where(Announcement.stock_code == params.stock_code)
    if params.date_from:
        query = query.where(Announcement.announcement_date >= params.date_from)
    if params.date_to:
        query = query.where(Announcement.announcement_date <= params.date_to)
    if getattr(params, "status", None):
        query = query.where(Announcement.status == params.status)

    count_query = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_query)).scalar() or 0

    query = query.order_by(Announcement.announcement_date.desc())
    offset = (params.page - 1) * params.page_size
    query = query.offset(offset).limit(params.page_size)

    result = await db.execute(query)
    items = list(result.scalars().all())

    return items, total


async def get_announcement_by_id(
    db: AsyncSession,
    announcement_id: uuid.UUID,
) -> Announcement | None:
    """
    Retrieve a single announcement by its UUID.

    通过 UUID 获取单条公告。

    Args:
    db: Async database session. / 异步数据库会话。
    announcement_id: UUID of the announcement. / 公告的 UUID。

    Returns:
    Announcement | None: The announcement object, or None if not found.
    公告对象，未找到时返回 None。

    """
    result = await db.execute(select(Announcement).where(Announcement.id == announcement_id))
    return result.scalar_one_or_none()


async def get_existing_news_ids(
    db: AsyncSession,
    stock_code: str,
    news_ids: list[str],
) -> set[str]:
    """
    Get the set of news_ids that already exist in the database for a given stock code.

    获取指定股票代码下已存在于数据库中的 news_id 集合。

    Args:
    db: Async database session. / 异步数据库会话。
    stock_code: Stock code to filter by. / 用于过滤的股票代码。
    news_ids: List of news_id strings to check. / 要检查的 news_id 字符串列表。

    Returns:
    set[str]: Set of existing news_id values. / 已存在的 news_id 值集合。

    """
    if not news_ids:
        return set()
    result = await db.execute(
        select(Announcement.news_id).where(
            Announcement.stock_code == stock_code,
            Announcement.news_id.in_(news_ids),
        )
    )
    return {row[0] for row in result.all()}


async def get_last_sync_date(
    db: AsyncSession,
    stock_code: str,
) -> datetime | None:
    """
    Get the most recent announcement_date for a given stock code.

    获取指定股票代码的最新公告日期。

    Args:
    db: Async database session. / 异步数据库会话。
    stock_code: Stock code to query. / 要查询的股票代码。

    Returns:
    datetime | None: The latest announcement datetime, or None if no records exist.
    最新公告日期时间，无记录时返回 None。

    """
    result = await db.execute(
        select(func.max(Announcement.announcement_date)).where(Announcement.stock_code == stock_code)
    )
    return result.scalar()


async def bulk_insert_announcements(
    db: AsyncSession,
    records: list[dict],
) -> int:
    """
    Bulk insert announcement records into the database.

    批量插入公告记录到数据库。

    Args:
    db: Async database session. / 异步数据库会话。
    records: List of announcement field dicts to insert.
    要插入的公告字段字典列表。

    Returns:
    int: Number of inserted rows. / 插入的行数。

    """
    if not records:
        return 0
    objects = [Announcement(**r) for r in records]
    db.add_all(objects)
    await db.flush()
    return len(objects)


async def update_announcement_file(
    db: AsyncSession,
    announcement_id: uuid.UUID,
    file_path: str,
    file_size: int,
    file_hash: str,
    lang: str = "en",
) -> None:
    """
    Update the file storage metadata for an announcement.

    更新公告的文件存储元数据。

    Args:
    db: Async database session. / 异步数据库会话。
    announcement_id: UUID of the announcement to update. / 要更新的公告 UUID。
    file_path: Storage path of the downloaded file. / 下载文件的存储路径。
    file_size: File size in bytes. / 文件大小（字节）。
    file_hash: SHA-256 hash of the file. / 文件的 SHA-256 哈希值。
    lang: Language of the file ('en' or 'zh'). / 文件语言。

    """
    announcement = await get_announcement_by_id(db, announcement_id)
    if announcement:
        if lang == "en":
            announcement.file_path_en = file_path
            announcement.file_size_en = file_size
            announcement.file_hash_en = file_hash
        else:
            announcement.file_path_zh = file_path
            announcement.file_size_zh = file_size
            announcement.file_hash_zh = file_hash
        announcement.last_synced_at = datetime.now(timezone.utc).replace(tzinfo=None)
        await db.flush()


async def get_announcements_by_news_ids(
    db: AsyncSession,
    stock_code: str,
    news_ids: list[str],
) -> dict[str, Announcement]:
    """
    Get announcements keyed by news_id for reconciliation.

    获取按 news_id 索引的公告，用于对账。

    Args:
    db: Async database session. / 异步数据库会话。
    stock_code: Stock code to filter by. / 用于过滤的股票代码。
    news_ids: List of news_id strings to look up. / 要查找的 news_id 列表。

    Returns:
    dict[str, Announcement]: Mapping of news_id -> Announcement.

    """
    if not news_ids:
        return {}
    result = await db.execute(
        select(Announcement).where(
            Announcement.stock_code == stock_code,
            Announcement.news_id.in_(news_ids),
        )
    )
    return {ann.news_id: ann for ann in result.scalars().all()}


async def bulk_update_status(
    db: AsyncSession,
    updates: list[dict],
    stock_code: str,
) -> int:
    """
    Bulk update status and filing_type for announcements identified by news_id.

    按 news_id 批量更新公告状态和 filing_type。

    Uniqueness is (stock_code, news_id) -- joint filings can carry the same
    news_id under two stocks -- so the lookup must be scoped to one stock.

    唯一性为 (stock_code, news_id)——联合公告可能在两只股票下使用相同
    news_id——因此查询必须限定在单一股票内。

    Args:
    db: Async database session. / 异步数据库会话。
    updates: List of dicts with keys: news_id, status, and optionally filing_type_en/zh/cn.
    stock_code: Stock code all updates belong to. / 这些更新所属的股票代码。

    Returns:
    int: Number of records actually updated.

    """
    if not updates:
        return 0
    count = 0
    for upd in updates:
        result = await db.execute(
            select(Announcement).where(
                Announcement.news_id == upd["news_id"],
                Announcement.stock_code == stock_code,
            )
        )
        ann = result.scalar_one_or_none()
        if ann and ann.status != upd["status"]:
            ann.status = upd["status"]
            if "filing_type_en" in upd:
                ann.filing_type_en = upd["filing_type_en"]
            if "filing_type_zh" in upd:
                ann.filing_type_zh = upd["filing_type_zh"]
            if "filing_type_cn" in upd:
                ann.filing_type_cn = upd["filing_type_cn"]
            count += 1
    await db.flush()
    return count
