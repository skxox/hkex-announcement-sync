"""
Shared HTTP client factory with anti-crawling features.

共享 HTTP 客户端工厂，集成反反爬功能。

Provides rate limiting, User-Agent rotation, browser-like headers,
and proxy support for all HTTP requests to HKEX.
"""

import random
import threading
import time

import httpx

from app.config import Settings

USER_AGENTS: list[str] = [
    # Chrome (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # Chrome (macOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    " (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Firefox (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    # Firefox (macOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
    # Edge (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    " (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
]


class RateLimiter:
    """
    Thread-safe rate limiter with random jitter.

    线程安全的请求限速器，带随机抖动。

    Ensures a minimum interval between requests and adds random jitter
    to make request patterns less predictable.
    """

    def __init__(self, requests_per_second: float = 2.0, jitter: float = 0.5):
        self._min_interval = 1.0 / requests_per_second
        self._jitter = jitter
        self._last_request = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        """
        Block until enough time has elapsed since the last request.

        阻塞直到距上次请求已过足够时间。
        """
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request
            delay = max(0, self._min_interval - elapsed)
            jitter = random.uniform(0, self._jitter)
            total_delay = delay + jitter
        if total_delay > 0:
            time.sleep(total_delay)
        with self._lock:
            self._last_request = time.monotonic()


def random_ua() -> str:
    """Return a randomly chosen browser User-Agent string."""
    return random.choice(USER_AGENTS)


def browser_headers() -> dict[str, str]:
    """
    Return a realistic set of browser request headers.

    返回一组真实的浏览器请求头。
    """
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,zh-HK;q=0.8,zh;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }


def create_client(settings: Settings) -> httpx.Client:
    """
    Create an httpx.Client with anti-crawling features configured.

    创建一个集成了反反爬功能的 httpx.Client。

    Applies a random User-Agent, full browser headers, proxy support,
    and connection settings from the application configuration.
    """
    headers = browser_headers()
    headers["User-Agent"] = random_ua()

    proxy = settings.HTTP_PROXY or None

    return httpx.Client(
        timeout=settings.HTTP_TIMEOUT,
        headers=headers,
        follow_redirects=True,
        proxy=proxy,
    )
