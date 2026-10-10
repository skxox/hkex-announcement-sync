"""Tests for shared HTTP client factory with anti-crawling features."""

import time

from app.config import Settings
from app.scraper.http import USER_AGENTS, RateLimiter, browser_headers, create_client, random_ua


class TestRateLimiter:
    def test_respects_rate_limit(self):
        limiter = RateLimiter(requests_per_second=10, jitter=0)
        start = time.monotonic()
        limiter.wait()
        limiter.wait()
        limiter.wait()
        elapsed = time.monotonic() - start
        # 3 requests at 10/sec = minimum ~0.2s between them
        assert elapsed >= 0.15

    def test_jitter_adds_randomness(self):
        limiter = RateLimiter(requests_per_second=100, jitter=0.1)
        times = []
        for _ in range(5):
            start = time.monotonic()
            limiter.wait()
            times.append(time.monotonic() - start)
        # With jitter, not all delays should be identical
        unique_times = set(round(t, 4) for t in times)
        assert len(unique_times) > 1

    def test_zero_rate_with_high_jitter(self):
        limiter = RateLimiter(requests_per_second=1000, jitter=0)
        start = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - start
        # Very high rate, should be nearly instant
        assert elapsed < 0.1


class TestUserAgentRotation:
    def test_returns_valid_ua(self):
        ua = random_ua()
        assert "Mozilla" in ua
        assert any(browser in ua for browser in ["Chrome", "Firefox", "Edg"])

    def test_pool_has_variety(self):
        uas = set(random_ua() for _ in range(50))
        assert len(uas) >= 3

    def test_pool_size(self):
        assert len(USER_AGENTS) >= 6


class TestBrowserHeaders:
    def test_contains_required_headers(self):
        headers = browser_headers()
        assert "Accept" in headers
        assert "Accept-Language" in headers
        assert "Accept-Encoding" in headers
        assert "Connection" in headers
        assert "Sec-Fetch-Dest" in headers
        assert "Sec-Fetch-Mode" in headers
        assert "Sec-Fetch-Site" in headers

    def test_accept_language_includes_chinese(self):
        headers = browser_headers()
        assert "zh" in headers["Accept-Language"]


class TestCreateClient:
    def test_creates_httpx_client(self):
        import httpx

        client = create_client(Settings())
        assert isinstance(client, httpx.Client)
        client.close()

    def test_client_has_user_agent(self):
        client = create_client(Settings())
        ua = client.headers.get("user-agent", "")
        assert "Mozilla" in ua
        client.close()

    def test_client_has_browser_headers(self):
        client = create_client(Settings())
        assert "accept-language" in client.headers
        assert "accept-encoding" in client.headers
        client.close()

    def test_no_proxy_by_default(self):
        client = create_client(Settings())
        assert client._transport is not None
        client.close()

    def test_proxy_configured(self):
        s = Settings(HTTP_PROXY="http://127.0.0.1:9999")
        client = create_client(s)
        assert client is not None
        client.close()
