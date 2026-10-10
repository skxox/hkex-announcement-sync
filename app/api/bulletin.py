"""
API endpoints for HKEX bulletin queries with HKEX-compatible format.

港交所公告查询 API 端点，兼容港交所响应格式。
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.announcements import _get_lang_fields
from app.database import get_db
from app.models import Announcement
from app.schemas.announcement import (
    BulletinData,
    BulletinListResponse,
    DataStatus,
    LanguageEnum,
    SortOrderEnum,
)

router = APIRouter(prefix="/bulletin", tags=["bulletin"])


@router.get("", response_model=BulletinListResponse)
async def list_bulletins(
    symbol: str = Query(..., description="Stock code(s), comma-separated"),
    pageindex: int = Query(1, ge=1),
    pagesize: int = Query(10, ge=1, le=20),
    language: LanguageEnum = Query(LanguageEnum.en),
    sortorder: SortOrderEnum = Query(SortOrderEnum.desc),
    db: AsyncSession = Depends(get_db),
):
    """
    Query HKEX bulletins with language support and HKEX-compatible response format.

    查询港交所公告，支持多语言和港交所兼容的响应格式。

    Args:
    symbol: Stock code(s), comma-separated (e.g. '00700,09988').
    股票代码，逗号分隔（如 '00700,09988'）。
    pageindex: Page number (1-based). / 页码（从 1 开始）。
    pagesize: Items per page (max 20). / 每页条数（最大 20）。
    language: Response language (en/zh/cn). / 响应语言。
    sortorder: Sort direction (desc/asc). / 排序方向。
    db: Async database session (injected). / 异步数据库会话（自动注入）。

    Returns:
    BulletinListResponse: HKEX-compatible bulletin list with status metadata.
    港交所兼容的公告列表及状态元数据。

    """
    symbols = [s.strip() for s in symbol.split(",") if s.strip()]

    query = select(Announcement).where(
        Announcement.is_visible == True,  # noqa: E712
        Announcement.stock_code.in_(symbols),
    )

    if sortorder == SortOrderEnum.desc:
        query = query.order_by(Announcement.announcement_date.desc())
    else:
        query = query.order_by(Announcement.announcement_date.asc())

    count_query = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_query)).scalar() or 0

    offset = (pageindex - 1) * pagesize
    query = query.offset(offset).limit(pagesize)

    result = await db.execute(query)
    announcements = list(result.scalars().all())

    bulletin_data = []
    for ann in announcements:
        lang_fields = _get_lang_fields(ann, language)

        bulletin_dt = ""
        if ann.announcement_date:
            bulletin_dt = ann.announcement_date.strftime("%Y-%m-%dT%H:%M:%S")

        file_size_str = ""
        lang_file_size = lang_fields.get("file_size")
        if lang_file_size:
            if lang_file_size >= 1024 * 1024:
                file_size_str = f"{lang_file_size / 1024 / 1024:.0f}MB"
            else:
                file_size_str = f"{lang_file_size / 1024:.0f}KB"

        bulletin_data.append(
            BulletinData(
                symbol=ann.stock_code,
                stock_name=lang_fields["stock_name"],
                title=lang_fields["title"],
                short_text=lang_fields["short_text"],
                long_text=lang_fields["long_text"],
                file_size=file_size_str or None,
                file_type=ann.file_type,
                file_link=lang_fields["hkex_url"] or None,
                bulletin_date_time=bulletin_dt or None,
                unique_id=ann.news_id,
            )
        )

    return BulletinListResponse(
        data_status=DataStatus(
            status_code=100,
            status_description="正常返回",
            response_date_time=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            data_total_count=total,
        ),
        data=bulletin_data,
    )
