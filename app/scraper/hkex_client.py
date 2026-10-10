"""
HKEX disclosure platform scraper client.

港交所披露平台数据抓取客户端。
"""

import html
import json
import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.config import Settings

logger = logging.getLogger(__name__)

HKEX_BASE_URL = "https://www1.hkexnews.hk"
HKEX_SEARCH_PAGE = "https://www1.hkexnews.hk/search/titlesearch.xhtml"
HKEX_API_ENDPOINT = "https://www1.hkexnews.hk/search/titleSearchServlet.do"

CANCELLATION_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\(Cancelled since Headlines Superseded and Replaced\)"), "cancelled_superseded"),
    (re.compile(r"\(Cancelled and Reissued\)"), "cancelled_reissued"),
    (re.compile(r"\(Headlines Revised\)"), "headlines_revised"),
]


class HKEXClient:
    """
    Client for fetching announcement data from the HKEX disclosure platform.

    从港交所披露平台获取公告数据的客户端。

    Fetches announcements in three languages / 支持三种语言的公告获取：
    - EN (English): lang=E
    - ZH (Traditional Chinese / 繁体中文): lang=ZH
    - SC (Simplified Chinese / 简体中文): converted from ZH via opencc

    HKEX search requires a three-step session-based approach /
    港交所搜索需要三步基于会话的请求流程：
    1. GET search page with params to get ViewState and form action URL
    获取搜索页面参数以提取 ViewState 和表单 action URL
    2. POST the JSF form with ViewState + date range to initialize session
    提交 JSF 表单（含 ViewState 和日期范围）以初始化会话
    3. GET the JSON API with pagination to fetch records
    通过分页请求 JSON API 获取记录
    """

    def __init__(self, settings: Settings | None = None):
        """
        Initialize the HKEX client with optional settings.

        使用可选的配置初始化港交所客户端。

        Args:
        settings: Application settings instance. Uses default Settings if None.
        应用配置实例。为 None 时使用默认配置。

        """
        self._settings = settings or Settings()
        self._session: httpx.Client | None = None
        self._rate_limiter: Any = None

    def close(self):
        """
        Close the underlying HTTP session and release resources.

        关闭底层 HTTP 会话并释放资源。
        """
        if self._session:
            self._session.close()

    def __enter__(self):
        """
        Enter context manager, returning self.

        进入上下文管理器，返回自身。
        """
        return self

    def __exit__(self, *args):
        """
        Exit context manager, closing the HTTP session.

        退出上下文管理器，关闭 HTTP 会话。
        """
        self.close()

    def _get_session(self) -> httpx.Client:
        """
        Get or create the shared HTTP client session (lazy initialization).

        获取或创建共享的 HTTP 客户端会话（延迟初始化）。

        Returns:
        httpx.Client: The configured HTTP client session.
        已配置的 HTTP 客户端会话。

        """
        if self._session is None:
            from app.scraper.http import create_client

            self._session = create_client(self._settings)
        return self._session

    def _get_rate_limiter(self):
        """Get or create the rate limiter (lazy initialization)."""
        if self._rate_limiter is None:
            from app.scraper.http import RateLimiter

            self._rate_limiter = RateLimiter(
                requests_per_second=self._settings.HTTP_REQUESTS_PER_SECOND,
                jitter=self._settings.HTTP_DELAY_JITTER,
            )
        return self._rate_limiter

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    def get_stock_id(self, stock_code: str) -> str:
        """
        Resolve a stock code (e.g. '00700') to its HKEX internal stock ID.

        将股票代码（如 '00700'）解析为港交所内部股票 ID。

        Args:
        stock_code: HKEX stock code string (e.g. '00700').
        港交所股票代码字符串。

        Returns:
        str: The internal stock ID used by HKEX APIs.
        港交所 API 使用的内部股票 ID。

        Raises:
        ValueError: If the stock code cannot be resolved.
        当股票代码无法解析时抛出。

        """
        params = {
            "callback": "callback",
            "lang": "EN",
            "type": "A",
            "name": stock_code,
            "market": "SEHK",
            "_": str(int(time.time() * 1000)),
        }
        resp = self._get_session().get(self._settings.HKEX_PREFIX_URL, params=params)
        resp.raise_for_status()

        match = re.search(r"callback\((.*)\)", resp.text, re.DOTALL)
        if not match:
            raise ValueError(f"Failed to parse stock ID response for {stock_code}")

        data = json.loads(match.group(1))
        stocks = data.get("stockInfo", [])
        if not stocks:
            raise ValueError(f"Stock code {stock_code} not found in HKEX")

        stock_id = str(stocks[0]["stockId"])
        logger.info("Resolved stock_code=%s -> stock_id=%s", stock_code, stock_id)
        return stock_id

    def search_announcements(
        self,
        stock_id: str,
        date_from: date,
        date_to: date,
        stock_code: str = "",
    ) -> list[dict[str, Any]]:
        """
        Fetch announcements in EN and ZH, then generate SC fields, merging by DATE_TIME.

        获取英文和繁体中文公告，然后生成简体中文字段，按日期时间合并。

        Args:
        stock_id: HKEX internal stock ID. / 港交所内部股票 ID。
        date_from: Start date of the search range. / 搜索范围的起始日期。
        date_to: End date of the search range. / 搜索范围的结束日期。
        stock_code: Stock code for tagging records. / 用于标记记录的股票代码。

        Returns:
        list[dict[str, Any]]: Merged trilingual announcement records.
        合并后的三语公告记录列表。

        """
        # Fetch EN records
        logger.info("Fetching EN announcements for stock_id=%s", stock_id)
        en_records = self._fetch_all_chunks(stock_id, date_from, date_to, lang="E")
        logger.info("Fetched %d EN records", len(en_records))

        # Fetch ZH records
        logger.info("Fetching ZH announcements for stock_id=%s", stock_id)
        zh_records = self._fetch_all_chunks(stock_id, date_from, date_to, lang="ZH")
        logger.info("Fetched %d ZH records", len(zh_records))

        # Build ZH lookup by DATE_TIME (EN and ZH use different NEWS_IDs,
        # but the same announcement shares the same DATE_TIME)
        zh_by_datetime: dict[str, dict] = {}
        for r in zh_records:
            dt = r.get("DATE_TIME", "").strip()
            if dt:
                zh_by_datetime[dt] = r

        # Merge EN + ZH into unified records
        merged = []
        for en_raw in en_records:
            news_id = en_raw.get("NEWS_ID", "")
            en_dt = en_raw.get("DATE_TIME", "").strip()
            zh_raw = zh_by_datetime.get(en_dt, {})

            en_parsed = self._parse_single_record(en_raw, stock_code)
            zh_parsed = self._parse_single_record(zh_raw, stock_code) if zh_raw else {}

            record = {
                "stock_code": en_parsed["stock_code"],
                "news_id": news_id,
                "title_en": en_parsed.get("title", ""),
                "title_zh": zh_parsed.get("title", ""),
                "stock_name_en": en_parsed.get("stock_name", ""),
                "stock_name_zh": zh_parsed.get("stock_name", ""),
                "filing_type_en": en_parsed.get("filing_type", ""),
                "filing_type_zh": zh_parsed.get("filing_type", ""),
                "short_text_en": en_parsed.get("short_text", ""),
                "short_text_zh": zh_parsed.get("short_text", ""),
                "long_text_en": en_parsed.get("long_text", ""),
                "long_text_zh": zh_parsed.get("long_text", ""),
                "hkex_url_en": en_parsed.get("hkex_url", ""),
                "hkex_url_zh": zh_parsed.get("hkex_url", ""),
                "file_type": en_parsed.get("file_type", "PDF"),
                "announcement_date": en_parsed.get("announcement_date"),
                "status": en_parsed.get("status", "active"),
            }
            merged.append(record)

        # Generate SC fields using opencc
        self._fill_sc_fields(merged)

        logger.info("Merged %d trilingual records", len(merged))
        return merged

    def _fetch_all_chunks(
        self,
        stock_id: str,
        date_from: date,
        date_to: date,
        lang: str = "E",
    ) -> list[dict[str, Any]]:
        """
        Fetch all announcements for a stock within a date range, chunked by month.

        按月分块获取指定股票在日期范围内的所有公告。

        Args:
        stock_id: HKEX internal stock ID. / 港交所内部股票 ID。
        date_from: Start date of the range. / 范围起始日期。
        date_to: End date of the range. / 范围结束日期。
        lang: Language code ('E' for English, 'ZH' for Traditional Chinese).
        语言代码（'E' 为英文，'ZH' 为繁体中文）。

        Returns:
        list[dict[str, Any]]: All raw announcement records from HKEX.
        来自港交所的所有原始公告记录。

        """
        all_records: list[dict[str, Any]] = []

        if date_from > date_to:
            raise ValueError(f"date_from {date_from} is after date_to {date_to}")

        chunk_start = date_from
        # Inclusive comparison: incremental sync passes date_from == date_to
        # once today's announcements exist in the DB, and a [D, D] window is valid.
        while chunk_start <= date_to:
            if chunk_start.month == 12:
                next_month = date(chunk_start.year + 1, 1, 1)
            else:
                next_month = date(chunk_start.year, chunk_start.month + 1, 1)
            chunk_end = min(next_month - timedelta(days=1), date_to)

            logger.info(
                "Fetching lang=%s: stock_id=%s, %s to %s",
                lang,
                stock_id,
                chunk_start,
                chunk_end,
            )
            records = self._fetch_chunk(stock_id, chunk_start, chunk_end, lang=lang)
            all_records.extend(records)

            chunk_start = next_month

        return all_records

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    def _fetch_chunk(
        self,
        stock_id: str,
        date_from: date,
        date_to: date,
        lang: str = "E",
    ) -> list[dict[str, Any]]:
        """
        Fetch one month chunk of announcements using the JSF session-based approach.

        使用基于 JSF 会话的方式获取一个月内的公告数据。

        Performs a three-step process / 执行三步流程：
        1. GET search page to extract ViewState and form action.
        获取搜索页面以提取 ViewState 和表单 action。
        2. POST JSF form to set the date range on the server session.
        提交 JSF 表单以在服务器会话上设置日期范围。
        3. GET JSON API endpoint with pagination to retrieve records.
        通过分页请求 JSON API 端点获取记录。

        Args:
        stock_id: HKEX internal stock ID. / 港交所内部股票 ID。
        date_from: Chunk start date. / 分块起始日期。
        date_to: Chunk end date. / 分块结束日期。
        lang: Language code ('E' or 'ZH'). / 语言代码。

        Returns:
        list[dict[str, Any]]: Raw announcement records for the chunk.
        该分块的原始公告记录。

        """
        session = self._get_session()
        from_str = date_from.strftime("%Y%m%d")
        to_str = date_to.strftime("%Y%m%d")

        # Step 1: GET search page to extract ViewState and form action
        page_resp = session.get(
            HKEX_SEARCH_PAGE,
            params={
                "sortDir": "0",
                "sortByRecordDate": "on",
                "searchType": "0",
                "category": "0",
                "t1code": "-2",
                "t2Gcode": "-2",
                "t2code": "-2",
                "documentType": "-1",
                "rowRange": "0",
                "lang": "EN",
            },
            timeout=30,
        )
        page_resp.raise_for_status()

        html = page_resp.text
        vs_match = re.search(r'javax\.faces\.ViewState.*?value="([^"]+)"', html)
        view_state = vs_match.group(1) if vs_match else ""
        fa_match = re.search(r'<form[^>]*action="([^"]+)"', html)
        form_action = fa_match.group(1) if fa_match else ""

        submit_url = f"{HKEX_BASE_URL}{form_action}" if form_action.startswith("/") else form_action

        # Step 2: POST JSF form to set date range on server session
        if submit_url and view_state:
            session.post(
                submit_url,
                data={
                    "j_idt10": "j_idt10",
                    "j_idt10:loadMoreRange": "100",
                    "javax.faces.ViewState": view_state,
                    "from": from_str,
                    "to": to_str,
                },
                timeout=30,
            )

        # Step 3: GET JSON API with pagination
        all_records: list[dict[str, Any]] = []
        fetched = 0

        while True:
            api_resp = session.get(
                HKEX_API_ENDPOINT,
                params={
                    "sortDir": "0",
                    "sortByOptions": "DateTime",
                    "category": "0",
                    "market": "SEHK",
                    "stockId": stock_id,
                    "documentType": "-1",
                    "fromDate": from_str,
                    "toDate": to_str,
                    "title": "",
                    "searchType": "0",
                    "t1code": "-2",
                    "t2Gcode": "-2",
                    "t2code": "-2",
                    "rowRange": str(fetched + 5000),
                    "lang": lang,
                },
                headers={
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Referer": HKEX_SEARCH_PAGE,
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=120,
            )
            api_resp.raise_for_status()

            data = api_resp.json()
            result_raw = data.get("result", "null")
            if not result_raw or result_raw == "null":
                break

            if isinstance(result_raw, str):
                records = json.loads(result_raw)
            else:
                records = result_raw

            if not records:
                break

            has_next = data.get("hasNextRow", False)
            new_records = records[fetched:] if fetched < len(records) else []
            all_records.extend(new_records)
            fetched = len(records)

            if not has_next:
                break

            self._get_rate_limiter().wait()

        return all_records

    @staticmethod
    def _clean_text(text: str) -> str:
        """
        Decode HTML entities and strip HTML tags from HKEX API text fields.

        解码 HTML 实体并清除港交所 API 文本字段中的 HTML 标签。

        Args:
        text: Raw text from HKEX API. / 港交所 API 原始文本。

        Returns:
        str: Cleaned plain text. / 清洗后的纯文本。

        """
        if not text:
            return ""
        text = html.unescape(text)
        text = re.sub(r"<[^>]+>", " ", text)
        return " ".join(text.split())

    @staticmethod
    def _parse_cancellation_tag(long_text: str) -> tuple[str, str]:
        """
        Extract cancellation status tag from LONG_TEXT, returning (status, clean_filing_type).

        从 LONG_TEXT 中提取取消状态标记，返回 (status, clean_filing_type)。

        HKEX prefixes cancelled announcements with tags like:
        "(Cancelled since Headlines Superseded and Replaced)", "(Cancelled and Reissued)", etc.

        Returns:
        tuple[str, str]: (status_string, filing_type_with_tag_removed)
        Status is one of: "active", "cancelled_superseded", "cancelled_reissued", "headlines_revised".

        """
        if not long_text:
            return "active", ""
        for pattern, status in CANCELLATION_PATTERNS:
            if pattern.search(long_text):
                return status, pattern.sub("", long_text).strip()
        return "active", long_text

    @staticmethod
    def _parse_single_record(raw: dict[str, Any], stock_code: str) -> dict[str, Any]:
        """
        Parse a single raw HKEX API record into a normalized dictionary.

        将单条港交所 API 原始记录解析为标准化字典。

        Args:
        raw: Raw record dict from HKEX API response.
        来自港交所 API 响应的原始记录字典。
        stock_code: Stock code to tag the record with.
        用于标记记录的股票代码。

        Returns:
        dict[str, Any]: Normalized record with keys: stock_code, stock_name,
        title, announcement_date, filing_type, short_text, long_text,
        hkex_url, file_link, file_type, news_id.
        标准化后的记录字典。

        """
        if not raw:
            return {
                "stock_code": stock_code,
                "stock_name": "",
                "title": "",
                "announcement_date": None,
                "filing_type": "",
                "short_text": "",
                "long_text": "",
                "hkex_url": "",
                "file_link": "",
                "file_type": "PDF",
                "news_id": "",
                "status": "active",
            }

        file_link = raw.get("FILE_LINK", "")
        title = HKEXClient._clean_text(raw.get("TITLE", ""))
        stock_name = HKEXClient._clean_text(raw.get("STOCK_NAME", ""))
        long_text_raw = HKEXClient._clean_text(raw.get("LONG_TEXT", ""))
        status, filing_type = HKEXClient._parse_cancellation_tag(long_text_raw)
        if not filing_type:
            filing_type = HKEXClient._clean_text(raw.get("SHORT_TEXT", ""))
        news_id = raw.get("NEWS_ID", "")
        short_text = HKEXClient._clean_text(raw.get("SHORT_TEXT", ""))
        long_text = long_text_raw
        file_type = raw.get("FILE_TYPE", "PDF")

        date_str = raw.get("DATE_TIME", "")
        announcement_date = None
        if date_str:
            for fmt in ("%d/%m/%Y %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%d"):
                try:
                    announcement_date = datetime.strptime(date_str.strip(), fmt)
                    break
                except ValueError:
                    continue

        hkex_url = f"https://www1.hkexnews.hk{file_link}" if file_link else ""

        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "title": title,
            "announcement_date": announcement_date,
            "filing_type": filing_type,
            "short_text": short_text,
            "long_text": long_text,
            "hkex_url": hkex_url,
            "file_link": file_link,
            "file_type": file_type,
            "news_id": news_id,
            "status": status,
        }

    @staticmethod
    def _fill_sc_fields(records: list[dict[str, Any]]) -> None:
        """
        Fill Simplified Chinese (SC/CN) fields by converting from Traditional Chinese using opencc.

        使用 opencc 将繁体中文字段转换为简体中文字段。

        Modifies records in-place, adding keys: title_cn, stock_name_cn,
        filing_type_cn, short_text_cn, long_text_cn.

        就地修改记录，添加以下键：title_cn、stock_name_cn、
        filing_type_cn、short_text_cn、long_text_cn。

        Args:
        records: List of merged announcement dicts with ZH fields.
        包含繁体中文字段的合并公告字典列表。

        Note:
        If opencc is not installed, SC fields will be set to empty strings.
        如未安装 opencc，简体中文字段将被设为空字符串。

        """
        try:
            import opencc

            converter = opencc.OpenCC("t2s")
        except Exception:
            logger.warning("opencc not available, SC fields will be empty")
            for r in records:
                r["title_cn"] = ""
                r["stock_name_cn"] = ""
                r["filing_type_cn"] = ""
                r["short_text_cn"] = ""
                r["long_text_cn"] = ""
            return

        for r in records:
            r["title_cn"] = converter.convert(r.get("title_zh", ""))
            r["stock_name_cn"] = converter.convert(r.get("stock_name_zh", ""))
            r["filing_type_cn"] = converter.convert(r.get("filing_type_zh", "") or "")
            r["short_text_cn"] = converter.convert(r.get("short_text_zh", "") or "")
            r["long_text_cn"] = converter.convert(r.get("long_text_zh", "") or "")
