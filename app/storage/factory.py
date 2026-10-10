"""
Factory function for creating storage backend instances.

创建存储后端实例的工厂函数模块。
"""

from app.config import Settings
from app.config import StorageBackend as StorageBackendEnum
from app.storage.base import StorageBackend
from app.storage.local import LocalStorage


def create_storage_backend(settings: Settings | None = None) -> StorageBackend:
    """
    Create a storage backend instance based on application settings.

    根据应用配置创建存储后端实例。
    """
    settings = settings or Settings()
    if settings.STORAGE_BACKEND == StorageBackendEnum.S3:
        # Import lazily so the default install (no "s3" extra) can still boot;
        # boto3 is only required when S3 storage is actually selected.
        try:
            from app.storage.s3 import S3Storage
        except ImportError as exc:
            raise RuntimeError(
                "STORAGE_BACKEND='s3' requires the 's3' extra. "
                "Install it with: uv sync --extra s3 (or pip install 'hkex-announcement-sync[s3]')"
            ) from exc

        return S3Storage(
            endpoint=settings.S3_ENDPOINT,
            bucket=settings.S3_BUCKET,
            access_key=settings.S3_ACCESS_KEY,
            secret_key=settings.S3_SECRET_KEY,
            region=settings.S3_REGION,
        )
    return LocalStorage(base_path=settings.STORAGE_LOCAL_PATH)
