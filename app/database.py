"""
Database engine, session factory, and dependency injection helpers.

数据库引擎、会话工厂和依赖注入辅助模块。
"""

from collections.abc import AsyncGenerator
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings, get_settings


def _create_engine(settings: Settings | None = None, poolclass=None) -> AsyncEngine:
    settings = settings or get_settings()

    kwargs: dict = {"echo": False}
    if poolclass is not None:
        kwargs["poolclass"] = poolclass
    elif not settings.is_sqlite:
        kwargs.update(pool_size=10, max_overflow=20, pool_pre_ping=True)

    # SQLite has limited support for concurrent connections
    if settings.is_sqlite:
        # Ensure parent directory exists for SQLite file
        db_path = settings.DATABASE_URL.split("///")[-1]
        parent = Path(db_path).parent
        if parent and str(parent) != ".":
            parent.mkdir(parents=True, exist_ok=True)

    return create_async_engine(settings.DATABASE_URL, **kwargs)


def create_run_engine(settings: Settings | None = None) -> AsyncEngine:
    """
    Create a dedicated engine for a one-off sync/reconcile run.

    为一次性的同步/对账运行创建专用引擎。

    Sync and reconcile runs execute on their own event loop (see
    SyncService.run / ReconcileService.run), and SQLAlchemy's connection pool
    has no loop affinity: a pooled asyncmy/asyncpg connection created on
    another loop (e.g. the API's) raises RuntimeError when checked out there.
    NullPool hands the run fresh connections every time; the caller disposes
    the engine when the run finishes.
    """
    from sqlalchemy.pool import NullPool

    return _create_engine(settings, poolclass=NullPool)


engine = _create_engine()
async_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Yield an async database session for FastAPI dependency injection.

    为 FastAPI 依赖注入提供异步数据库会话。
    """
    async with async_session_factory() as session:
        yield session


async def init_db():
    """
    Create all tables. Used for SQLite which doesn't use Alembic migrations in dev.
    """
    from app.models import Base

    settings = get_settings()
    if settings.is_sqlite:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
