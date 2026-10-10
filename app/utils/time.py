"""
Hong Kong timezone helpers for HKEX business dates.

港时区辅助函数，用于港交所业务日期。

HKEX announcement DATE_TIME values are Hong Kong local time, so "today"
must be derived in Asia/Hong_Kong rather than the server's local timezone,
otherwise servers running UTC go stale by up to a day.

港交所公告的日期时间为香港本地时间，因此"今天"必须按 Asia/Hong_Kong
时区计算，否则使用 UTC 的服务器会滞后最多一天。
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

HK_TIMEZONE = ZoneInfo("Asia/Hong_Kong")


def hk_today() -> date:
    """
    Return today's date in Hong Kong local time.

    返回香港本地时间的今天日期。

    Returns:
    date: Current calendar date in Asia/Hong_Kong timezone.
    亚洲/香港时区的当前日历日期。

    """
    return datetime.now(HK_TIMEZONE).date()
