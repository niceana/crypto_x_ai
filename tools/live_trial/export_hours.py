"""一次读取历史回放数据；复用 export_xdata.py 顶部已填写的表路径。"""

import hashlib
import json
from pathlib import Path
import sys
import uuid

import pandas as pd

import export_xdata as settings
from export_xdata import BEIJING, indexed_utc, read_assets, read_days, require_complete, timestamp, validate_event_columns
from _replay import bounds, schedule, select, write_json

ROOT = settings.ROOT

# ==================== 运行前修改这里 ====================
FIRST_TIME = settings.TEST_TIME  # 第一个对账时刻（UTC+8），例如 2021-03-20 00:47:00
HOURS = 240                     # 240 个小时；完整回放会调用 240×4=960 批
OUTPUT_DIR = ROOT / "data_240h"
# 表路径、资产名单、METHOD 和 WEIGHT_PARAM_ID 沿用 export_xdata.py 顶部配置。
# ======================================================


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def export_hours(cli, *, first_time, hours, output_dir, configuration=settings):
    calls = schedule(first_time, hours)
    for name in ("POSTS_PATH", "EVENTS_PATH", "OLD_WEIGHT_PATH", "ASSETS_PATH"):
        value = getattr(configuration, name)
        if not isinstance(value, str) or not value.strip() or "填写" in value:
            raise ValueError("请先填写 export_xdata.py 顶部的 {}".format(name))
    first_begin, _ = bounds(calls[0])
    _, final_end = bounds(calls[-1])
    history_begin = calls[0] - pd.Timedelta(hours=8, days=100)
    instruments, coins = read_assets(cli, configuration.ASSETS_PATH)
    events = indexed_utc(read_days(cli, configuration.EVENTS_PATH, history_begin, final_end), "record_id")
    validate_event_columns(events)
    events = select(events, history_begin, final_end)
    events = events.loc[events["asset"].isin(coins)].copy(deep=True)
    seed = select(events, history_begin, first_begin)
    reference = select(events, first_begin, final_end)
    require_complete(seed, "初始历史 B", vectors=True)
    require_complete(reference, "历史对账事件")
    del events
    posts = indexed_utc(read_days(cli, configuration.POSTS_PATH, first_begin, final_end,
                                  columns=["text", "user_name"]), "id")
    posts = select(posts, first_begin, final_end)
    if not {"text", "user_name"}.issubset(posts.columns):
        raise ValueError("帖子表缺少 text/user_name")
    for name in ("text", "user_name"):
        if not posts[name].map(lambda value: isinstance(value, str)).all():
            raise ValueError("帖子 {} 必须为字符串".format(name))
    if any(":" in value for value in posts.index.get_level_values("id")):
        raise ValueError("帖子 id 不能含冒号")
    if seed["source_post_id"].isin(posts.index.get_level_values("id")).any():
        raise ValueError("初始历史 B 与回放原帖存在相同 source_post_id，请核对索引")
    references = calls[calls.minute == 47].floor("h") + pd.Timedelta(hours=1)
    old = read_days(cli, configuration.OLD_WEIGHT_PATH, references[0], references[-1] + pd.Timedelta(nanoseconds=1))
    if isinstance(old.index, pd.MultiIndex):
        raise ValueError("旧 weight 应为单层 date 索引的宽表")
    old.index = pd.DatetimeIndex([timestamp(value, BEIJING).tz_convert(BEIJING).tz_localize(None) for value in old.index], name="date")
    if not old.index.is_unique:
        raise ValueError("旧 weight 日期索引不能重复")
    absent_times = references[~references.isin(old.index)]
    if len(absent_times):
        raise ValueError("旧 weight 缺少 {} 个对账整点，前几项：{}".format(len(absent_times), list(absent_times[:5])))
    old = old.loc[references].copy(deep=True)
    expected = [value if "." in value else value + ".USDT.UFUTURE.BINANCE" for value in instruments]
    missing = [value for value in expected if value not in old.columns]
    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    frames = dict(history_seed=seed, posts=posts, reference_events=reference, old_weight=old)
    for name, frame in frames.items():
        frame.to_pickle(output / (name + ".pkl"))
    (output / "assets.json").write_text(json.dumps(instruments, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest = dict(version=1, dataset_id=uuid.uuid4().hex, first_time=str(calls[3]), hours=hours,
                    call_count=len(calls), first_call=str(calls[0]), last_call=str(calls[-1]),
                    posts_utc=[str(first_begin), str(final_end)], history_utc=[str(history_begin), str(first_begin)],
                    aggregation_method=configuration.METHOD, weight_param_id=configuration.WEIGHT_PARAM_ID,
                    old_weight_missing_assets=missing, rows={name: len(value) for name, value in frames.items()},
                    source_paths={name: getattr(configuration, name) for name in ("POSTS_PATH", "EVENTS_PATH", "OLD_WEIGHT_PATH", "ASSETS_PATH")},
                    files={name: digest(output / name) for name in [item + ".pkl" for item in frames] + ["assets.json"]},
                    history_policy="只用初始 B；后续历史仅由本次新结果更新，reference_events 只用于对账")
    write_json(output / "manifest.json", manifest)
    print("导出完成：{}".format(output), flush=True)
    print("{} 小时，{} 次 15 分钟调用，{} 次 47 分对账；原帖 {} 条。".format(hours, len(calls), hours, len(posts)), flush=True)
    print("空批次仍会运行并输出权重；旧 weight 缺少的资产列在对账时按 0 处理。", flush=True)
    return manifest


def main(cli=None):
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    return export_hours(cli, first_time=FIRST_TIME, hours=HOURS, output_dir=OUTPUT_DIR)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, TypeError, KeyError) as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        raise SystemExit(2)
