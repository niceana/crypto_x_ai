"""保留原始时间索引，仅为排序、校验和范围选择解析时间。"""

import json
import re
from bisect import bisect_left, bisect_right
from calendar import monthrange
from datetime import date, datetime, timedelta

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


def history_window_start(value, months):
    """从披露时刻回推日历月，目标月缺少对应日期时取月末，保留时区和纳秒。"""
    stamp = published_time(value)
    year, month = divmod(stamp.year * 12 + stamp.month - 1 - months, 12)
    month += 1
    day = min(stamp.day, monthrange(year, month)[1])
    return stamp.replace(year=year, month=month, day=day)


def history_read_bounds(current_times, scope_start, months):
    """计算已读任务范围之前需补读的首尾日期，无需补读时返回 None。

    current_times 由各轮按自己的业务规则选定，可以是一次性迭代器。取这些
    时刻回推日历月后的最早日期，下界包含当天，上界为 scope_start 的前一天。
    此函数不读取数据；下界当天的精确时刻仍由后续内存窗口判断。
    """
    begin = min((history_window_start(stamp, months).date() for stamp in current_times), default=None)
    if begin is None:
        return None
    end = date.fromisoformat(scope_start) - timedelta(days=1)
    return (begin, end) if begin <= end else None


def is_history_needed(history_time, current_times, months):
    """判断历史是否落在任一待处理窗口中，输入时刻须已升序排列。

    current_times 是本币或本组的 pd.Timestamp 序列，允许重复，不在本函数中
    排序或修改。先二分选出第一个严格晚于历史的时点；月末截断可能使随后
    几天的下界时分秒更早，因此逐日检查这些边界，不逐条扫描同日任务。
    下界日期严格晚于历史日期后即可停止；组与状态由调用方筛选。
    """
    stamp = published_time(history_time)
    position = bisect_right(current_times, stamp)
    while position < len(current_times):
        current = current_times[position]
        begin = history_window_start(current, months)
        if stamp >= begin:
            return True
        if begin.date() > stamp.date():
            return False
        position = bisect_left(current_times, current.normalize() + pd.DateOffset(days=1))
    return False


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
