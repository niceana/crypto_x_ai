"""爬虫输入转为旧三轮需要的原帖发布时间索引；不修改宿主快照。"""

from datetime import datetime

import pandas as pd

from . import _compat as core


def utc_dates(values, label):
    """原帖无时区时间按 UTC 解释；带时区值先转 UTC，再移除标签供原表使用。"""
    stamps = []
    for value in values:
        if not isinstance(value, (str, datetime, pd.Timestamp)):
            raise ValueError("{} 必须为有效的时间文本或 datetime".format(label))
        try:
            stamp = pd.Timestamp(value)
            if pd.isna(stamp):
                raise ValueError("时间不能缺失")
            stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
        except (ValueError, TypeError) as exc:
            raise ValueError("{} 包含无效时间：{}".format(label, value)) from exc
        stamps.append(stamp.tz_localize(None))
    return pd.DatetimeIndex(stamps)


def frame(frame, id_name, now_utc):
    if not isinstance(frame, pd.DataFrame):
        raise ValueError("输入表必须为 DataFrame")
    frame = frame.copy(deep=True)
    core.validation.validate_index(frame, id_name)
    dates = utc_dates(frame.index.get_level_values("date"), "内部 UTC 发布时间 date")
    if len(dates) and (dates > now_utc).any():
        raise ValueError("输入包含晚于本次系统时间的原帖发布时间")
    frame.index = pd.MultiIndex.from_arrays([dates, frame.index.get_level_values(id_name)], names=["date", id_name])
    return frame.sort_index()


def posts(new_posts, now_utc):
    if not isinstance(new_posts, pd.DataFrame) or not new_posts.columns.is_unique:
        raise ValueError("new_posts 必须为列名唯一的 DataFrame")
    if new_posts.shape == (0, 0):
        new_posts = pd.DataFrame(index=pd.DatetimeIndex([], name="date"),
                                 columns=["id", "created_at", "text", "user_name"])
    if isinstance(new_posts.index, pd.MultiIndex):
        core.validation.validate_index(new_posts, "id")
        ids = new_posts.index.get_level_values("id")
    else:
        if new_posts.index.name != "date":
            raise ValueError("new_posts 的爬虫时间索引必须名为 date")
        if "id" not in new_posts.columns:
            raise ValueError("new_posts 必须包含普通 id 字符串列")
        ids = new_posts["id"]
    utc_dates(new_posts.index.get_level_values("date"), "爬虫时间 date")  # 只校验，允许同批重复。
    if "created_at" not in new_posts.columns:
        raise ValueError("new_posts 必须包含原帖 created_at 发布时间列，不能用爬虫 date 代替")
    published = utc_dates(new_posts["created_at"], "原帖 created_at")
    core.validation.validate_post_strings(new_posts, ["text", "user_name"])
    source = new_posts[["text", "user_name"]].copy(deep=True)
    source.index = pd.MultiIndex.from_arrays([published, ids], names=["date", "id"])
    source = frame(source, "id", now_utc)
    tables = []
    core.post_tasks.initialize_tasks(source, tables.append)
    tasks = tables[0]
    tasks = core.call_meta.with_meta_columns(tasks, core.call_meta.POST_META_COLUMNS)
    return tasks
