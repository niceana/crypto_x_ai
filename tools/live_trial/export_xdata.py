"""只读 xdata，导出历史试跑数据；不更新数据库、不请求 AI。"""

import argparse
import json
from datetime import timedelta, timezone
from numbers import Integral
from pathlib import Path
import shlex
import sys

import pandas as pd
from _trial import ROOT, SCRIPT_DIR

# ==================== 运行前只需修改这里 ====================
TEST_TIME = "2021-03-20 16:47:00"  # 替换为旧程序已处理完成的时刻，UTC+8，建议 xx:47
POSTS_PATH = "填写旧原帖表或第一轮任务表路径"  # 旧 (date,id) 索引，date 为 UTC 发布时间
EVENTS_PATH = "填写旧第二三轮结果表路径"
OLD_WEIGHT_PATH = "填写旧weight具体表路径"  # 某一评分组合的实际表，不能只填基础目录
ASSETS_PATH = "crypto_1m/research_prod/meta/alphas/Cpt380_BA_Daily_Update_All.json"
METHOD = "sum"  # 与旧 weight 一致："sum" 或 "max_abs"
MODE = "all"  # "all"：同时准备两种测试；"weight"：只准备 weight 对账
WEIGHT_PARAM_ID = None  # 评分配置有多个组合时填写对应 param_id，否则保留 None
OUTPUT_DIR = ROOT / "data"  # 导出目录；也可改为服务器上的绝对路径
# ==========================================================

BEIJING = timezone(timedelta(hours=8))
EVENT_COLUMNS = ["event_id", "event", "asset", "source_post_id", "event_relation", "new_information",
                 "round2_ai_response", "round3_ai_response", "round2_status", "round2_config_hash",
                 "round3_status", "round3_config_hash", "error_message", "embedding"]
OPTIONAL_EVENT_COLUMNS = ["round2_call_meta", "round3_call_meta"]


def validate_event_columns(events):
    if not set(EVENT_COLUMNS).issubset(events.columns) or set(events.columns) - set(EVENT_COLUMNS + OPTIONAL_EVENT_COLUMNS):
        raise ValueError("事件表须保留原 14 个基础列，可另含两轮 call_meta；缺少={}，多出={}".format(
            sorted(set(EVENT_COLUMNS) - set(events.columns)),
            sorted(set(events.columns) - set(EVENT_COLUMNS + OPTIONAL_EVENT_COLUMNS))))


def timestamp(value, naive_timezone):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("时间不能缺失")
    return stamp.tz_localize(naive_timezone) if stamp.tzinfo is None else stamp


def utc_dates(values):
    # 旧原帖/事件表的无时区 date 是 UTC 发布时间。
    return pd.DatetimeIndex([timestamp(value, "UTC").tz_convert("UTC").tz_localize(None) for value in values])


def read_days(cli, table_path, begin, end, *, columns=None):
    """xdata 按日期读取，随后在内存中进行精确的半开时间筛选。"""
    options = dict(start_time=begin.date().isoformat(),
                   end_time=(end - pd.Timedelta(nanoseconds=1)).date().isoformat())
    if columns is not None:
        options["columns"] = columns
    print("cli.read({}, {})".format(repr(table_path), options), flush=True)
    frame = cli.read(table_path, **options)
    if not isinstance(frame, pd.DataFrame) or not frame.columns.is_unique:
        raise ValueError("{} 必须返回列名唯一的 DataFrame".format(table_path))
    return frame.copy(deep=True)


def indexed_utc(frame, id_name):
    if not isinstance(frame.index, pd.MultiIndex) or list(frame.index.names) != ["date", id_name]:
        raise ValueError("源表须为 (date, {}) 索引；date 为旧程序的 UTC 发布时间".format(id_name))
    result = frame.copy(deep=True)
    ids = frame.index.get_level_values(id_name)
    if any(isinstance(value, bool) or not isinstance(value, (str, Integral))
           or not str(value).strip() for value in ids):
        raise ValueError("{} 必须为非空字符串或整数，不接受浮点 ID".format(id_name))
    ids = [str(value) for value in ids]
    if len(ids) != len(set(ids)):
        raise ValueError("{} 转为字符串后仍须全表唯一".format(id_name))
    result.index = pd.MultiIndex.from_arrays([utc_dates(frame.index.get_level_values("date")), ids],
                                             names=["date", id_name])
    return result.sort_index()


def interval(frame, begin, end):
    dates = frame.index.get_level_values("date")
    return frame.loc[(dates >= begin) & (dates < end)].copy(deep=True)


def require_complete(frame, label, *, vectors=False):
    for name in ("round2_status", "round3_status"):
        bad = ~frame[name].eq("1")
        if bad.any():
            raise ValueError("{} 中 {} 有 {} 条非字符串 '1'；请选已处理完成的数据，不自动删行".format(label, name, int(bad.sum())))
    if vectors and frame["embedding"].map(lambda value: not isinstance(value, str) or not value.strip()).any():
        raise ValueError("历史 B 中有缺失向量；请先在旧系统核对，不生成或覆盖历史向量")


def read_assets(cli, object_path):
    document = cli.read_object(object_path)
    if isinstance(document, bytes):
        document = document.decode("utf-8")
    if isinstance(document, str):
        document = json.loads(document)
    values = document.get("InstrumentList") if isinstance(document, dict) else document
    if not isinstance(values, list) or not values:
        raise ValueError("资产对象应为非空数组或含 InstrumentList 的 JSON")
    values = [value.strip().upper() if isinstance(value, str) else "" for value in values]
    coins = [value.split(".", 1)[0] for value in values]
    if any(not coin for coin in coins) or len(coins) != len(set(coins)):
        raise ValueError("资产名单含空值或重复币名，请指定与旧 weight 对账范围一致的名单")
    return values, set(coins)


def export_data(cli, args):
    local = timestamp(args.test_time, BEIJING).tz_convert(BEIJING).tz_localize(None)
    cutoff = local.floor("15min")
    if cutoff.minute != 45:
        raise ValueError("新旧完整小时对账请选择 xx:45~xx:59 的 TEST_TIME，例如 16:47")
    cutoff_utc = cutoff - pd.Timedelta(hours=8)
    batch_begin = cutoff_utc - pd.Timedelta(minutes=15)
    history_begin = local - pd.Timedelta(hours=8, days=100)
    window_begin = cutoff.floor("h") - pd.Timedelta(minutes=15, hours=8)
    old_time = cutoff.floor("h") + pd.Timedelta(hours=1)
    instruments, coins = read_assets(cli, args.assets_path)

    begin = history_begin if args.mode == "all" else window_begin
    events = indexed_utc(read_days(cli, args.events_path, begin, cutoff_utc), "record_id")
    validate_event_columns(events)
    events = interval(events, begin, cutoff_utc)
    events = events.loc[events["asset"].isin(coins)].copy(deep=True)
    completed = interval(events, window_begin, cutoff_utc)
    if completed.empty:
        raise ValueError("weight 窗口内没有目标资产事件，请换一个有数据且已完成的时段")
    require_complete(completed, "weight 窗口")
    tables = {"events_completed": completed}

    if args.mode == "all":
        if not args.posts_path:
            raise ValueError("请填写顶部 POSTS_PATH：旧原帖表或旧第一轮任务表")
        # 只读原帖内容，不复用原第一轮结果；这里的旧 date 明确为发布时间。
        source = indexed_utc(read_days(cli, args.posts_path, batch_begin, cutoff_utc,
                                       columns=["text", "user_name"]), "id")
        source = interval(source, batch_begin, cutoff_utc)
        if source.empty:
            raise ValueError("本批 15 分钟没有帖子，请换一个有帖的时段")
        if not {"text", "user_name"}.issubset(source.columns):
            raise ValueError("帖子表缺少 text/user_name")
        for name in ("text", "user_name"):
            if not source[name].map(lambda value: isinstance(value, str)).all():
                raise ValueError("原帖 {} 必须为字符串".format(name))
        ids = source.index.get_level_values("id")
        if any(":" in value for value in ids):
            raise ValueError("原帖 id 不能含冒号")
        posts = pd.DataFrame({"id": list(ids),
                              "created_at": [value.tz_localize("UTC").isoformat() for value in source.index.get_level_values("date")],
                              "text": source["text"].to_numpy(), "user_name": source["user_name"].to_numpy()},
                             index=pd.DatetimeIndex([cutoff] * len(source), name="date"))
        history = interval(events, history_begin, batch_begin)
        if history["source_post_id"].isin(ids).any():
            raise ValueError("历史 B 中存在本批 source_post_id 的更早记录，请核对源表时间与 ID")
        require_complete(history, "历史 B", vectors=True)
        tables.update(posts=posts, history_before=history)

    old = read_days(cli, args.old_weight_path, old_time, old_time + pd.Timedelta(nanoseconds=1))
    if isinstance(old.index, pd.MultiIndex):
        raise ValueError("旧 weight 须为单层 date 索引的资产宽表，请指定某一参数组合的实际表路径")
    old.index = pd.DatetimeIndex([timestamp(value, BEIJING).tz_convert(BEIJING).tz_localize(None) for value in old.index], name="date")
    if not old.index.is_unique or old_time not in old.index:
        raise ValueError("旧 weight 索引重复或缺少 {}；旧表 date 应是写库后的 UTC+8 时间".format(old_time))
    old = old.loc[[old_time]].copy(deep=True)
    expected_columns = [value if "." in value else value + ".USDT.UFUTURE.BINANCE" for value in instruments]
    missing = [name for name in expected_columns if name not in old.columns]
    if missing:
        print("旧 weight 按资产交集生成，缺少 {} 列；保留完整资产名单，对账时这些列按 0 比较：{}".format(len(missing), missing), flush=True)
    # 原旧表按原列保存，缺列信息另行记录；不缩减正式函数接收的资产名单。
    tables["old_weight"] = old
    output = Path(args.output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    for name, frame in tables.items():
        frame.to_pickle(output / (name + ".pkl"))
    (output / "assets.json").write_text(json.dumps(instruments, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest = dict(mode=args.mode, test_time=str(local), aggregation_method=args.method, pandas_version=pd.__version__,
                    interval_rule="begin <= time < end", batch_utc=[str(batch_begin), str(cutoff_utc)],
                    history_utc=[str(history_begin), str(batch_begin)], weight_window_utc=[str(window_begin), str(cutoff_utc)],
                    old_weight_time_utc8=str(old_time), old_weight_missing_assets=missing,
                    old_weight_missing_policy="comparison_only_zero_for_absent_columns", source_paths=dict(posts=args.posts_path, events=args.events_path,
                    old_weight=args.old_weight_path, assets=args.assets_path), rows={name: len(frame) for name, frame in tables.items()},
                    crawl_date_note="历史源表没有爬虫时间；posts.date 统一模拟为本批结束刻度（UTC+8），created_at 保留 UTC 发布时间",
                    embedding_note="历史 embedding 字符串原样保留；版本/维度仍由正式入口严格检查")
    (output / "export_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print("已导出到 {}：{}".format(output, manifest["rows"]), flush=True)
    print("模拟调用时刻（UTC+8）: {}; 旧 weight 对应: {}".format(local, old_time), flush=True)
    quote = shlex.quote
    shared = " --assets {} --method {} --test-time {}".format(quote(str(output / "assets.json")), args.method, quote(str(local)))
    if args.weight_param_id:
        shared += " --weight-param-id " + quote(args.weight_param_id)
    if args.mode == "all":
        print("{} {} --posts {} --history {}{}".format(quote(sys.executable), quote(str(SCRIPT_DIR / "run_full.py")),
            quote(str(output / "posts.pkl")), quote(str(output / "history_before.pkl")), shared))
    print("{} {} --events {} --old-weight {}{}".format(quote(sys.executable), quote(str(SCRIPT_DIR / "check_weight.py")),
        quote(str(output / "events_completed.pkl")), quote(str(output / "old_weight.pkl")), shared))
    return manifest


def main(argv=None, cli=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-time", default=TEST_TIME, help="默认使用顶部 TEST_TIME（UTC+8）")
    parser.add_argument("--mode", choices=("all", "weight"), default=MODE)
    parser.add_argument("--posts-path", default=POSTS_PATH, help="默认使用顶部 POSTS_PATH")
    parser.add_argument("--events-path", default=EVENTS_PATH, help="默认使用顶部 EVENTS_PATH")
    parser.add_argument("--old-weight-path", default=OLD_WEIGHT_PATH, help="默认使用顶部 OLD_WEIGHT_PATH")
    parser.add_argument("--assets-path", default=ASSETS_PATH, help="默认使用顶部 ASSETS_PATH")
    parser.add_argument("--method", choices=("sum", "max_abs"), default=METHOD)
    parser.add_argument("--weight-param-id", default=WEIGHT_PARAM_ID)
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    args = parser.parse_args(argv)
    required_paths = {"EVENTS_PATH": args.events_path, "OLD_WEIGHT_PATH": args.old_weight_path,
                      "ASSETS_PATH": args.assets_path}
    if args.mode == "all":
        required_paths["POSTS_PATH"] = args.posts_path
    for name, value in required_paths.items():
        if not isinstance(value, str) or not value.strip() or "填写" in value:
            raise ValueError("请先在脚本顶部填写实际的 {}，再运行 python export_xdata.py".format(name))
    if cli is None:
        try:
            from xdata.client import get_client
        except ImportError as exc:
            raise ValueError("请用已接入 xdata 的旧程序 Python 环境运行导出脚本；再用试跑环境读取导出的 PKL") from exc
        cli = get_client()
    return export_data(cli, args)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, TypeError, KeyError) as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        raise SystemExit(2)
