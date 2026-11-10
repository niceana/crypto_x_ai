"""先备份并预览 JSONL 补录计划，再通过 apply 续写原始帖子表。"""

import argparse
import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from numbers import Integral
from pathlib import Path

import pandas as pd

from common.validation import validate_post_strings
from entrypoints.runtime import DEFAULT_LOGGING


# ==================== 修改这里后运行 ====================
# python -X utf8 -m entrypoints.supplement_posts preview
# python -X utf8 -m entrypoints.supplement_posts apply "reports/supplement_posts/<预览目录>"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCE_PATH = ""                       # 现有原始帖子表，不是第一轮任务表
JSON_FILES = ["_ALL_new.jsonl"]        # 相对项目根目录；支持多个文件，也支持绝对路径
JSON_FIELD_MAP = {                    # 目标字段: JSON 每条记录中的字段；支持 user.screen_name 等路径
    "date": "created_at",             # 发布时间，不使用采集时间
    "id": "id",
    "user_id": "user.id",
    "user_name": "user.screen_name",  # 当前爬虫 JSONL 的实际账号字段
    "text": "text",
    "created_at": "created_at",       # 普通列保留 JSON 原始发布时间字符串
    "retweet_count": "retweet_count",
    "like_count": "favorite_count",   # JSONL 的点赞数字段名为 favorite_count
}
# date/id 为双层索引，普通列为 user_id/user_name/text/created_at/retweet_count/like_count。
# 最后提交时 date 保持 datetime，id 及全部普通列统一为文本。
REPORT_DIR = PROJECT_ROOT / "reports" / "supplement_posts"
# JSONL 已筛选完毕：不再按时间、账号或正文过滤；date 使用无时区 datetime，不换算时分秒。
# 默认仅预览并备份；只有 apply 命令会提交续表。
# ========================================================

logger = logging.getLogger(__name__)


def _field(value, path):
    for key in path.split("."):
        value = value[key]
    return value


def _post_id(value, label="帖子 ID"):
    if isinstance(value, str) and value.strip():
        return value
    if isinstance(value, Integral) and not isinstance(value, bool):
        return str(value)
    raise ValueError("{} 必须为非空字符串或整数，不能是浮点数、布尔值或空值".format(label))


def _dates(values, label, *, strip_timezone=False):
    stamps = []
    stripped = 0
    for position, value in enumerate(values, 1):
        try:
            if not isinstance(value, (str, datetime, pd.Timestamp)):
                raise ValueError("须为时间字符串或 datetime，不接受数字时间戳")
            stamp = pd.Timestamp(value)
            if pd.isna(stamp):
                raise ValueError("时间不能为空")
            if strip_timezone and stamp.tzinfo is not None:
                stamp = stamp.tz_localize(None)
                stripped += 1
            stamps.append(stamp)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("{} 第 {} 行时间不合法：{}".format(label, position, exc)) from exc
    if stripped:
        logger.info("%s：%d 条时间已去掉时区标记，保留原时分秒，不进行时区换算", label, stripped)
    try:
        return pd.DatetimeIndex(stamps)
    except (TypeError, ValueError) as exc:
        raise ValueError("{} 内部的日期时区不一致，请先统一".format(label)) from exc


def _json_records(path):
    """JSONL 每行一条帖子，跳过空白行并保留真实行号。"""
    with path.open(encoding="utf-8-sig") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                # 标准库保留大整数精度，避免帖子 ID 被转换为浮点数。
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError("{} 第 {} 行 JSON 语法错误：{}".format(path, line_number, exc.msg)) from exc
            yield line_number, record


def _json_path(filename):
    path = Path(filename).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def load_json_posts(files, *, field_map=None):
    """逐行读取并校验全部 JSONL，组合为 date/id 索引的 df；不依赖 xdata。"""
    field_map = JSON_FIELD_MAP if field_map is None else field_map
    if not isinstance(files, (list, tuple)) or not files:
        raise ValueError("请在 JSON_FILES 中填写至少一个本地 JSONL 文件路径")
    if (not isinstance(field_map, dict) or not {"date", "id", "text", "user_name"}.issubset(field_map)
            or any(not isinstance(key, str) or not key or not isinstance(value, str) or not value
                   for key, value in field_map.items())):
        raise ValueError("JSON_FIELD_MAP 须提供 date/id/text/user_name 及各自的 JSON 字段路径")
    rows = []
    for filename in files:
        if not isinstance(filename, (str, Path)) or not str(filename).strip():
            raise ValueError("JSON_FILES 中的文件路径不能为空")
        path = _json_path(filename)
        file_rows = 0
        for position, record in _json_records(path):
            location = "{} 第 {} 行".format(path, position)
            if not isinstance(record, dict):
                raise ValueError("{} 必须为 JSON 对象".format(location))
            try:
                row = {name: _field(record, source) for name, source in field_map.items()}
                row["id"] = _post_id(row["id"])
                if "user_id" in row:
                    row["user_id"] = _post_id(row["user_id"], "user_id")
                if not isinstance(row["text"], str) or not isinstance(row["user_name"], str) or not row["user_name"].strip():
                    raise ValueError("text 须为字符串，user_name 须为非空字符串")
                rows.append(row)
                file_rows += 1
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("{} 字段不合法：{}".format(location, exc)) from exc
        logger.info("读取 JSONL：%s，帖子=%d", path, file_rows)
    frame = pd.DataFrame(rows, columns=list(field_map))
    frame["date"] = _dates(frame["date"], "JSON 发布时间", strip_timezone=True)
    return frame.set_index(["date", "id"])


def _prepare_existing(frame):
    if (not isinstance(frame, pd.DataFrame) or not frame.columns.is_unique
            or not isinstance(frame.index, pd.MultiIndex) or list(frame.index.names) != ["date", "id"]):
        raise ValueError("原始帖子表必须为列名唯一、date/id 双层索引的 DataFrame")
    validate_post_strings(frame, ["text", "user_name"])
    dates = _dates(frame.index.get_level_values("date"), "原表 date")
    if dates.tz is not None:
        raise ValueError("原表 date 应为无时区时间，请核对读取结果；脚本不进行时区换算")
    result = frame.copy()
    result.index = pd.MultiIndex.from_arrays([
        dates,
        [_post_id(value) for value in frame.index.get_level_values("id")],
    ], names=["date", "id"])
    if not result.index.is_unique or not result.index.get_level_values("id").is_unique:
        raise ValueError("原表的 (date, id) 或帖子 ID 重复，请核对原表")
    return result


def _write_frame(frame, columns):
    result = frame.copy()
    for name in ("date", "id"):
        if name in columns:
            result[name] = result.index.get_level_values(name)
    result = result.loc[:, columns]
    # date 索引为 datetime，id 为字符串；其余字段在提交前统一转为文本。
    return result.astype({name: str for name in result.columns if name != "date"})


def _prepare_import(existing, incoming):
    json_rows = len(incoming)
    expected_columns = [name for name in existing.columns if name not in ("date", "id")]
    if set(incoming.columns) != set(expected_columns):
        raise ValueError("JSON_FIELD_MAP 与原表普通列不一致：缺少={}，多出={}；请补齐字段映射".format(
            sorted(set(expected_columns) - set(incoming.columns)), sorted(set(incoming.columns) - set(expected_columns))))
    # 相同索引内容一致才合并；内容冲突或同 ID 不同 date 须先核对。
    duplicated = incoming.loc[incoming.index.duplicated(keep=False)]
    for key, group in duplicated.groupby(level=["date", "id"], sort=False):
        if any(not group.iloc[0].equals(group.iloc[position]) for position in range(1, len(group))):
            raise ValueError("JSON 中相同 (date, id) 的内容冲突：{}".format(key))
    incoming = incoming.loc[~incoming.index.duplicated(keep="first")].sort_index()
    if not incoming.index.get_level_values("id").is_unique:
        raise ValueError("JSON 中同一帖子 ID 对应不同 date，请核对发布时间")
    batch = incoming.loc[~incoming.index.isin(existing.index)].copy()
    conflicting_ids = batch.index.get_level_values("id").intersection(existing.index.get_level_values("id"))
    incoming = _write_frame(incoming, existing.columns)
    batch = _write_frame(batch, existing.columns)
    overlap_new = incoming.loc[incoming.index.isin(existing.index)]
    overlap_old = _write_frame(existing.loc[overlap_new.index], existing.columns)
    changed = (overlap_old != overlap_new).any(axis=1)
    original_only = existing.loc[~existing.index.isin(incoming.index)]
    conflict_old = existing.loc[existing.index.get_level_values("id").isin(conflicting_ids)]
    conflict_new = batch.loc[batch.index.get_level_values("id").isin(conflicting_ids)]
    summary = {
        "original_rows": len(existing), "json_rows": json_rows,
        "duplicate_rows_removed": json_rows - len(incoming), "existing_rows_skipped": len(incoming) - len(batch),
        "overlap_rows": len(overlap_new), "overlap_changed_rows": int(changed.sum()),
        "original_only_rows": len(original_only), "id_date_conflicts": len(conflicting_ids),
        "new_rows": len(batch), "written_rows": 0, "can_apply": len(conflicting_ids) == 0,
    }
    return incoming, batch, overlap_old, overlap_new, original_only, conflict_old, conflict_new, summary


def _review_frame(frame, source=None):
    result = frame.drop(columns=[name for name in ("date", "id") if name in frame]).reset_index()
    if source is not None:
        result.insert(0, "source", source)
    return result


def _save_csv(frames, path):
    review = pd.concat(frames, ignore_index=True)
    review["_account"] = review["user_name"].map(lambda value: value.strip().lstrip("@").casefold())
    sort_columns = ["date", "_account", "id"] + (["source"] if "source" in review else [])
    review = review.sort_values(sort_columns, kind="mergesort").drop(columns="_account")
    review["date"] = review["date"].map(str)
    review.to_csv(path, index=False, encoding="utf-8-sig")


def _sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _now():
    return datetime.now(timezone(timedelta(hours=8)))


def _get_client(cli):
    if cli is None:
        from xdata.client import get_client
        return get_client()
    return cli


def preview_json_posts(cli, source_path, files, *, report_dir=None, field_map=None):
    """先读取并备份完整原表，再生成只读对比及固定的续表 df，不写数据库。"""
    if not isinstance(source_path, str) or not source_path.strip():
        raise ValueError("请填写现有原始帖子表 SOURCE_PATH")
    source_path = source_path.strip()
    cli = _get_client(cli)
    raw_original = cli.read(source_path, start_time="", end_time="")
    if not isinstance(raw_original, pd.DataFrame):
        raise ValueError("原始帖子表读取结果必须为 DataFrame")
    output = (Path(report_dir if report_dir is not None else REPORT_DIR).expanduser()
              / _now().strftime("%Y%m%d_%H%M%S_%f")).resolve()
    output.mkdir(parents=True, exist_ok=False)
    backup_path = output / "original.pkl"
    raw_original.to_pickle(backup_path)
    pd.testing.assert_frame_equal(raw_original, pd.read_pickle(backup_path), check_exact=True)
    logger.info("原表完整备份已保存并读回核验：%s，行数=%d", backup_path, len(raw_original))
    existing = _prepare_existing(raw_original)
    incoming = load_json_posts(files, field_map=field_map)
    incoming, batch, overlap_old, overlap_new, original_only, conflict_old, conflict_new, summary = _prepare_import(existing, incoming)
    batch.to_pickle(output / "new_posts.pkl")
    _save_csv([_review_frame(incoming)], output / "incoming_posts.csv")
    _save_csv([_review_frame(batch)], output / "new_posts.csv")
    _save_csv([_review_frame(overlap_old, "original"), _review_frame(overlap_new, "incoming")],
              output / "overlap_comparison.csv")
    _save_csv([_review_frame(original_only)], output / "original_only.csv")
    _save_csv([_review_frame(conflict_old, "original"), _review_frame(conflict_new, "incoming")],
              output / "id_date_conflicts.csv")
    summary["plan_dir"] = str(output)
    plan = {
        "version": 1, "source_path": source_path, "created_at": _now().isoformat(),
        "json_files": [str(_json_path(path).resolve()) for path in files],
        "field_map": JSON_FIELD_MAP if field_map is None else field_map,
        "date_rule": "created_at_wall_time_without_timezone",
        "sha256": {name: _sha256(output / name) for name in ("original.pkl", "new_posts.pkl")},
        "summary": summary,
    }
    (output / "plan.json").write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("预览完成，未写数据库：原表=%d，候选去重后=%d，重叠=%d，重叠内容不同=%d，待补录=%d，ID日期冲突=%d",
                len(existing), len(incoming), summary["overlap_rows"], summary["overlap_changed_rows"],
                len(batch), summary["id_date_conflicts"])
    logger.info("查看 %s 和 %s；备份及计划目录：%s", output / "new_posts.csv", output / "overlap_comparison.csv", output)
    if not summary["can_apply"]:
        logger.warning("同一 ID 在原表和待补录数据中日期不同，apply 将停止；请查看 id_date_conflicts.csv，修正后重新 preview")
    logger.info('确认后执行：python -X utf8 -m entrypoints.supplement_posts apply "%s"', output)
    return summary


def apply_plan(cli, plan_dir):
    """仅执行指定预览中保存的 df，复核原表与完整备份一致后才续表。"""
    output = Path(plan_dir).expanduser().resolve()
    if (output / "apply_result.json").exists():
        raise ValueError("该预览已执行 apply；请查看 apply_result.json，不要重复执行")
    plan = json.loads((output / "plan.json").read_text(encoding="utf-8"))
    if plan.get("version") != 1 or not isinstance(plan.get("source_path"), str) or not plan["source_path"].strip():
        raise ValueError("补录计划格式不合法，请重新 preview")
    for name in ("original.pkl", "new_posts.pkl"):
        if _sha256(output / name) != plan.get("sha256", {}).get(name):
            raise ValueError("{} 与预览时不一致，请重新 preview".format(name))
    if not plan["summary"]["can_apply"]:
        raise ValueError("预览存在同一 ID 不同 date 的冲突，请先检查 id_date_conflicts.csv 并重新 preview")
    original = pd.read_pickle(output / "original.pkl")
    batch = pd.read_pickle(output / "new_posts.pkl")
    existing = _prepare_existing(original)
    checked_batch = _prepare_existing(batch)
    if list(batch.columns) != list(original.columns) or len(batch) != plan["summary"]["new_rows"]:
        raise ValueError("续表 df 结构或行数与预览不一致，请重新 preview")
    if not isinstance(batch.index.get_level_values("date"), pd.DatetimeIndex) or any(
            not isinstance(value, str) for value in batch.index.get_level_values("id")) or any(
            not isinstance(value, str) for name in batch.columns if name != "date" for value in batch[name]):
        raise ValueError("续表 df 必须为 datetime date，其他字段为文本，请重新 preview")
    if checked_batch.index.isin(existing.index).any() or checked_batch.index.get_level_values("id").isin(
            existing.index.get_level_values("id")).any():
        raise ValueError("续表 df 与原表备份存在重叠 ID 或索引，请重新 preview")
    cli = _get_client(cli)
    current = cli.read(plan["source_path"], start_time="", end_time="")
    _prepare_existing(current)
    try:
        pd.testing.assert_frame_equal(original.sort_index(), current.sort_index(), check_exact=True)
    except (AssertionError, TypeError, ValueError) as exc:
        raise ValueError("原表在 preview 后已变化，未写入；请重新 preview、备份并查看差异") from exc
    logger.info("apply 复核通过：原表与本地备份一致，续表无重叠，待补录=%d", len(batch))
    if not batch.empty:
        cli.update_by_index(plan["source_path"], batch.copy(deep=True))
    result = {"source_path": plan["source_path"], "plan_dir": str(output),
              "written_rows": len(batch), "original_rows": len(original),
              "expected_total_rows": len(original) + len(batch), "applied_at": _now().isoformat()}
    (output / "apply_result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("补录完成：%s", result)
    return result


def main(cli=None, argv=None):
    logging.basicConfig(**DEFAULT_LOGGING)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("preview", help="读取并备份原表，导出重叠和待补录记录，不写数据库")
    apply_parser = commands.add_parser("apply", help="执行已经查看过的指定预览计划")
    apply_parser.add_argument("plan_dir", help="preview 输出的带时间戳目录")
    args = parser.parse_args(argv)
    if args.command == "apply":
        return apply_plan(cli, args.plan_dir)
    return preview_json_posts(cli, SOURCE_PATH, JSON_FILES, report_dir=REPORT_DIR,
                              field_map=JSON_FIELD_MAP)


if __name__ == "__main__":
    main()
