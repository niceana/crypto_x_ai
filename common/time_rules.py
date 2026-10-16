"""保留原始时间索引，仅为排序、校验和范围选择解析时间。"""

import re
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


def in_scope(value, settings):
    if settings.start_time is None:
        return True
    day = published_time(value).date()
    return date.fromisoformat(settings.start_time) <= day <= date.fromisoformat(settings.end_time)


def select_scope(frame, settings):
    return frame.loc[[in_scope(key[0], settings) for key in frame.index]].copy(deep=True)
