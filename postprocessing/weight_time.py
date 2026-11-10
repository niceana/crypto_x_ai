"""权重小时窗口：目标整点 T 对应 [T-75分钟, T-15分钟)。"""

import pandas as pd


def event_target_hours(published_at):
    """保留 Series/DatetimeIndex 及原索引；45 分钟边界归入下一窗口。"""
    shifted = published_at + pd.Timedelta(minutes=15)
    floored = shifted.dt.floor("h") if isinstance(shifted, pd.Series) else shifted.floor("h")
    return floored + pd.Timedelta(hours=1)


def weight_output_bounds(start_time, end_time):
    """结束日最后 15 分钟落到次日 01:00，时间轴保留该整点。"""
    return pd.Timestamp(start_time), pd.Timestamp(end_time) + pd.Timedelta(days=1, hours=1)


def event_window_bounds(first_hour, last_hour):
    """返回连续目标整点（含两端）需要的完整事件范围，右端不包含。"""
    return first_hour - pd.Timedelta(minutes=75), last_hour - pd.Timedelta(minutes=15)
