"""保留原始时间索引，仅为排序、校验和范围选择解析时间。"""

import json
import re
from calendar import monthrange
from datetime import date, datetime

import pandas as pd


def published_time(value):
    """解析排序用时间，原索引仍原样写回；不转换时区。"""
    if isinstance(value, str):
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?", value):
            raise ValueError("date 必须含完整时分秒：{}".format(value))
    elif not isinstance(value, (datetime, pd.Timestamp)):
        raise ValueError("date 必须为完整时间文本或时间戳对象")
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("date 不能缺失")
    return stamp


def history_window_start(value, years):
    """从披露时刻回推指定日历年，闰日取平年二月末，保留时区和纳秒。"""
    stamp = published_time(value)
    year = stamp.year - years
    day = min(stamp.day, monthrange(year, stamp.month)[1])
    return stamp.replace(year=year, day=day)


def table_date_bounds(document):
    """解析结果表元数据的日期范围；空表返回两个 None，不转换原时区。"""
    try:
        if isinstance(document, str):
            document = json.loads(document)
        meta = document["meta"]
        values = [meta["start_time"], meta["end_time"]]
        if values == [None, None]:
            return None, None
        if any(not isinstance(value, str) or not value for value in values):
            raise ValueError("时间须为 ISO 字符串，空表两端须同时为 null")
        for value in values:
            date.fromisoformat(value[:10])
        # 沿用 pandas 的纳秒解析能力，避免 Python 3.8 拒绝九位小数。
        start, end = [pd.Timestamp(value) for value in values]
        if pd.isna(start) or pd.isna(end):
            raise ValueError("时间不能缺失")
        if start > end:
            raise ValueError("起点晚于终点")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("结果表元数据须提供合法的 meta.start_time/end_time：{}".format(exc)) from exc
    return start.date(), end.date()


def in_scope(value, settings):
    if settings.start_time is None:
        return True
    day = published_time(value).date()
    return date.fromisoformat(settings.start_time) <= day <= date.fromisoformat(settings.end_time)


def select_scope(frame, settings):
    return frame.loc[[in_scope(key[0], settings) for key in frame.index]].copy(deep=True)
