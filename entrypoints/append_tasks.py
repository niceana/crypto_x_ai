"""追加第一轮任务并刷新待处理正文，保留已成功任务及已有状态和 AI 结果。"""

import argparse
import hashlib
import json
import logging
import tempfile
from datetime import date, datetime, timedelta, timezone
from numbers import Integral
from pathlib import Path

import pandas as pd

from common.config import load_job_config
from common.process_lock import stage_guard
from common.validation import TASK_COLUMNS, validate_post_index, validate_post_strings, validate_post_task_structure
from entrypoints.runtime import DEFAULT_LOGGING, execute


logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXCLUDED_CRAWLER_USERNAMES = frozenset({"wublockchain"})
EXCLUDED_POST_PREFIXES = ("RT @", "JUST IN ")


def _timestamped_directory(root):
    root = Path(root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    return Path(tempfile.mkdtemp(prefix=stamp + "_", dir=str(root)))


def _paths(settings):
    source_path, task_path = settings["source_path"], settings["task_path"]
    if any(not isinstance(path, str) or not path.strip() for path in (source_path, task_path)) or source_path == task_path:
        raise ValueError("请填写不同的 source_path 和已有 task_path")
    return source_path, task_path


def _publication_bounds(settings):
    begin, end = settings.get("start_time"), settings.get("end_time")
    if begin is None and end is None:
        return None
    if not isinstance(begin, str) or not isinstance(end, str):
        raise ValueError("起止日期必须同时设置为 YYYY-MM-DD 字符串，或同时留空")
    if date.fromisoformat(begin).isoformat() != begin or date.fromisoformat(end).isoformat() != end:
        raise ValueError("起止日期必须为 YYYY-MM-DD 字符串")
    if begin > end:
        raise ValueError("起点不能晚于终点")
    return pd.Timestamp(begin), pd.Timestamp(end) + pd.Timedelta(days=1)


def _read_existing_tasks(cli, task_path):
    existing = cli.read(task_path, start_time="", end_time="")
    validate_post_task_structure(existing)
    validate_post_strings(existing, TASK_COLUMNS)
    return existing


def _crawler_posts(raw, *, report_dir, target_accounts):
    """采集 date 只用于识别表结构；任务 date 来自 created_at，统一为无时区 UTC。"""
    required = ["id", "created_at", "text", "username"]
    if (not isinstance(raw, pd.DataFrame) or not raw.columns.is_unique
            or isinstance(raw.index, pd.MultiIndex) or raw.index.name != "date"
            or not set(required).issubset(raw.columns)):
        raise ValueError("爬虫表须为单层 date 采集时间索引，并含 id/created_at/text/username 普通列")
    normalized_accounts = raw["username"].map(lambda value: value.strip().casefold() if isinstance(value, str) else "")
    excluded_non_target = ~normalized_accounts.isin(target_accounts)
    excluded_account = normalized_accounts.isin(EXCLUDED_CRAWLER_USERNAMES) & ~excluded_non_target
    excluded_prefix = raw["text"].map(
        lambda value: isinstance(value, str) and value.startswith(EXCLUDED_POST_PREFIXES)
    ).astype(bool) & ~excluded_non_target & ~excluded_account
    excluded = excluded_non_target | excluded_account | excluded_prefix
    source_rows = [position for position, blocked in enumerate(excluded, 1) if not blocked]
    excluded_rows = int(excluded_account.sum())
    excluded_prefix_rows = int(excluded_prefix.sum())
    excluded_non_target_rows = int(excluded_non_target.sum())
    raw = raw.loc[~excluded].copy(deep=True)
    logger.info("爬虫原帖过滤：目标账号数=%d，非目标账号剔除记录=%d，排除账号=%s，账号剔除记录=%d，"
                "正文前缀剔除记录=%d，保留记录=%d（采集行数，尚未按帖子 ID 去重或筛选发布时间；"
                "按非目标账号、排除账号、正文前缀依次统计，不重复计数）",
                len(target_accounts), excluded_non_target_rows, ", ".join(sorted(EXCLUDED_CRAWLER_USERNAMES)),
                excluded_rows, excluded_prefix_rows, len(raw))
    validate_post_strings(raw, ["text", "username"])
    ids, dates = [], []
    for post_id, value in zip(raw["id"], raw["created_at"]):
        if isinstance(post_id, str) and post_id.strip():
            pass
        elif isinstance(post_id, Integral) and not isinstance(post_id, bool):
            post_id = str(post_id)
        else:
            raise ValueError("爬虫表 id 须为非空字符串或整数，不能为浮点数、布尔值或空值")
        try:
            if not isinstance(value, (str, datetime, pd.Timestamp)):
                raise ValueError("发布时间须为时间文本或 datetime")
            stamp = pd.Timestamp(value)
            if pd.isna(stamp):
                raise ValueError("发布时间不能缺失")
            stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
            dates.append(stamp.tz_localize(None))
        except (ValueError, TypeError, OverflowError) as exc:
            raise ValueError("爬虫表 created_at 无效：id={}".format(post_id)) from exc
        ids.append(post_id)
    selected = raw[["text", "username"]].rename(columns={"username": "user_name"}).copy(deep=True)
    selected.index = pd.MultiIndex.from_arrays([pd.DatetimeIndex(dates), ids], names=["date", "id"])
    # 同一帖允许被重复采集；忽略采集时间和点赞等变化，仅比较固定任务输入。
    unique = selected.reset_index().drop_duplicates()
    conflicts = unique.loc[unique["id"].duplicated(keep=False)]
    if not conflicts.empty:
        columns = [("date", "created_at"), ("text", "text"), ("user_name", "username")]
        counts = conflicts.groupby("id", sort=True)[[name for name, _ in columns]].nunique()
        fields = {post_id: ", ".join(label for name, label in columns if row[name] > 1)
                  for post_id, row in counts.iterrows()}
        # 保留冲突 ID 的每次采集，包括相同版本的重复记录，方便回查源表。
        report = raw[required].reset_index().rename(columns={"date": "capture_date"})
        report["id"] = ids
        report["published_at_utc"] = pd.DatetimeIndex(dates)
        report["source_row"] = source_rows
        report = report.loc[report["id"].isin(fields)].copy()
        report["conflict_fields"] = report["id"].map(fields)
        for name in ("text", "username"):
            report[name + "_repr"] = report[name].map(lambda value: json.dumps(value, ensure_ascii=False))
        report = report.sort_values(["id", "source_row"])
        path = report_dir / "crawler_conflicts.csv"
        report.to_csv(path, index=False, encoding="utf-8-sig")
        samples = "; ".join("{} [{}]".format(post_id, names) for post_id, names in list(fields.items())[:10])
        raise ValueError("爬虫表同一 ID 的发布时间、正文或账号不一致，尚未追加任务："
                         "冲突 ID={}，采集记录={}；前10个 ID 及差异字段={}；完整明细={}".format(
                             len(fields), len(report), samples, path))
    selected = unique.set_index(["date", "id"]).sort_index()
    validate_post_index(selected)
    return selected, excluded_rows, excluded_prefix_rows, excluded_non_target_rows


def append_tasks_from_source(cli, settings, *, apply=False, report_dir=None):
    """按 UTC 发布时间范围筛选爬虫表，追加任务并刷新 status=0 的正文。"""
    source_path, task_path = _paths(settings)
    bounds = _publication_bounds(settings)
    existing = _read_existing_tasks(cli, task_path)
    target_accounts = {name.strip().casefold() for name in existing["user_name"].unique() if name.strip()}
    logger.info("第一轮任务表目标账号：全表 user_name 去重并忽略大小写和首尾空格，目标账号数=%d", len(target_accounts))
    # 数据库 date 是采集时间，不能用发布时间范围限制 cli.read，否则会漏掉迟到采集。
    raw = cli.read(source_path, start_time="", end_time="", columns=["id", "created_at", "text", "username"])
    output = _timestamped_directory(report_dir if report_dir is not None else PROJECT_ROOT / "reports" / "append_tasks")
    selected, excluded_rows, excluded_prefix_rows, excluded_non_target_rows = _crawler_posts(
        raw, report_dir=output, target_accounts=target_accounts)
    unique_posts = len(selected)
    duplicate_rows = len(raw) - excluded_non_target_rows - excluded_rows - excluded_prefix_rows - len(selected)
    if bounds is not None:
        dates = selected.index.get_level_values("date")
        selected = selected.loc[(dates >= bounds[0]) & (dates < bounds[1])]
    logger.info("爬虫表任务来源：采集记录=%d，非目标账号剔除=%d，账号过滤剔除=%d，正文前缀剔除=%d，"
                "重复采集合并=%d，过滤后唯一帖子=%d，发布时间范围内帖子=%d；date 来自 created_at（UTC）",
                len(raw), excluded_non_target_rows, excluded_rows, excluded_prefix_rows, duplicate_rows, unique_posts, len(selected))
    summary = _append_selected(cli, task_path, selected, output, apply=apply, existing=existing)
    summary["report_dir"] = str(output)
    summary["excluded_rows"] = excluded_rows
    summary["excluded_prefix_rows"] = excluded_prefix_rows
    summary["excluded_non_target_rows"] = excluded_non_target_rows
    summary["target_account_count"] = len(target_accounts)
    return summary


def append_tasks_from_plan(cli, settings, plan_dir, *, apply=False):
    """使用补录时保存的批次，追加缺失任务并按状态刷新正文。"""
    source_path, task_path = _paths(settings)
    output = Path(plan_dir).expanduser().resolve()
    plan = json.loads((output / "plan.json").read_text(encoding="utf-8"))
    applied = json.loads((output / "apply_result.json").read_text(encoding="utf-8"))
    if plan.get("version") != 1 or plan.get("source_path") != source_path or applied.get("source_path") != source_path:
        raise ValueError("本次补录目录的原表路径与 post_extraction_config.yaml 的 source_path 不一致")
    snapshot_path = output / "new_posts.pkl"
    if hashlib.sha256(snapshot_path.read_bytes()).hexdigest() != plan.get("sha256", {}).get("new_posts.pkl"):
        raise ValueError("new_posts.pkl 与补录时不一致，请使用当时的原始预览目录")
    batch = pd.read_pickle(snapshot_path)
    validate_post_index(batch)
    validate_post_strings(batch, ["text", "user_name"])
    if len(batch) != applied.get("written_rows") or len(batch) != plan["summary"]["new_rows"]:
        raise ValueError("本地批次行数与原表补录成功记录不一致")
    if batch.empty:
        return _append_selected(cli, task_path, batch, output, apply=apply)
    dates = batch.index.get_level_values("date")
    begin, end = pd.Timestamp(dates.min()).date().isoformat(), pd.Timestamp(dates.max()).date().isoformat()
    # 从原表核实这批帖子已落库，任务输入取当前原表，并与补录批次逐条核对。
    source = cli.read(source_path, start_time=begin, end_time=end, columns=["text", "user_name"])
    validate_post_index(source)
    validate_post_strings(source, ["text", "user_name"])
    missing = batch.index[~batch.index.isin(source.index)]
    if len(missing):
        raise ValueError("原表缺少本批 {} 条帖子，尚未追加任务，请先核对补录结果".format(len(missing)))
    selected = source.loc[batch.index, ["text", "user_name"]]
    if not selected.equals(batch[["text", "user_name"]]):
        raise ValueError("原表正文或账号与补录批次不一致，尚未追加任务")
    return _append_selected(cli, task_path, selected, output, apply=apply)


def _task_input_report(existing, incoming, keys, differences):
    """逐条保存任务表与本批输入的完整值及空白显示，供冲突和正文更新清单复用。"""
    report = pd.DataFrame(index=keys)
    report["conflict_fields"] = [", ".join(name for name in ("text", "user_name") if row[name])
                                 for _, row in differences.loc[keys].iterrows()]
    for label, frame in (("existing", existing), ("incoming", incoming)):
        for name in ("text", "user_name"):
            column = "{}_{}".format(label, name)
            report[column] = frame.loc[keys, name].to_numpy()
            report[column + "_repr"] = report[column].map(lambda value: json.dumps(value, ensure_ascii=False))
    report["existing_status"] = existing.loc[keys, "status"].to_numpy()
    return report


def _append_selected(cli, task_path, selected, output, *, apply, existing=None):
    """两种来源共用状态分流、完整备份、待处理正文刷新和任务追加。"""
    summary = {"batch_rows": len(selected), "already_in_tasks": 0, "new_tasks": 0,
               "written_rows": 0, "text_updates": 0, "written_text_updates": 0,
               "skipped_completed_texts": 0, "start_time": "", "end_time": ""}
    if selected.empty:
        logger.info("本批没有新增原始帖子，无需追加第一轮任务")
        return summary
    dates = selected.index.get_level_values("date")
    begin, end = pd.Timestamp(dates.min()).date().isoformat(), pd.Timestamp(dates.max()).date().isoformat()
    summary.update(start_time=begin, end_time=end)
    if existing is None:
        existing = _read_existing_tasks(cli, task_path)
    new_source = selected.loc[~selected.index.isin(existing.index)].sort_index()
    if new_source.index.get_level_values("id").isin(existing.index.get_level_values("id")).any():
        raise ValueError("第一轮任务表已存在同 ID 不同 date 的记录，尚未追加任务")
    shared = selected.index[selected.index.isin(existing.index)]
    columns = ["text", "user_name"]
    differences = existing.loc[shared, columns].ne(selected.loc[shared, columns])
    statuses = existing.loc[shared, "status"]
    text_only = differences["text"] & ~differences["user_name"]
    update_keys = shared[(text_only & statuses.eq("0")).to_numpy(dtype=bool)]
    skipped_completed = int((text_only & statuses.eq("1")).sum())
    blocked = differences["user_name"] | (differences["text"] & ~statuses.isin(["0", "1"]))
    conflicts = shared[blocked.to_numpy(dtype=bool)]
    if len(conflicts):
        report = _task_input_report(existing, selected, conflicts, differences)
        path = output / "round1_task_conflicts.csv"
        report.reset_index().to_csv(path, index=False, encoding="utf-8-sig")
        samples = "; ".join("{} [{}]".format(key[1], names)
                            for key, names in report["conflict_fields"].iloc[:10].items())
        raise ValueError("第一轮已有任务的正文或账号与本批不一致，尚未追加任务："
                         "冲突帖子={}；前10个 ID 及差异字段={}；完整明细={}".format(
                             len(report), samples, path))
    text_patch = existing.loc[update_keys, ["text"]].copy(deep=True)
    text_patch["text"] = selected.loc[update_keys, "text"].to_numpy()
    text_patch = text_patch.sort_index()
    updates_path = output / "round1_text_updates.csv"
    _task_input_report(existing, selected, update_keys, differences).reset_index().to_csv(
        updates_path, index=False, encoding="utf-8-sig")
    tasks = pd.DataFrame("", index=new_source.index, columns=TASK_COLUMNS, dtype=object)
    tasks["text"] = new_source["text"].to_numpy()
    tasks["user_name"] = new_source["user_name"].to_numpy()
    tasks["status"] = "0"
    tasks = tasks.loc[:, existing.columns]
    summary.update(already_in_tasks=len(shared), new_tasks=len(tasks), text_updates=len(text_patch),
                   skipped_completed_texts=skipped_completed)
    tasks.to_csv(output / "round1_tasks_to_append.csv", encoding="utf-8-sig")
    logger.info("第一轮增量任务预览：本批=%d，已在任务表=%d，待追加=%d，待更新正文=%d，已成功正文差异跳过=%d；"
                "日期=%s 至 %s；追加清单=%s；正文更新清单=%s",
                len(selected), len(shared), len(tasks), len(text_patch), skipped_completed, begin, end,
                output / "round1_tasks_to_append.csv", updates_path)
    if apply and (not tasks.empty or not text_patch.empty):
        backup = _timestamped_directory(output / "round1_backups")
        existing.to_pickle(backup / "tasks.pkl")
        pd.testing.assert_frame_equal(existing, pd.read_pickle(backup / "tasks.pkl"), check_exact=True)
        logger.info("追加前完整第一轮任务表已备份：%s", backup / "tasks.pkl")
        if not text_patch.empty:
            cli.update_by_column(task_path, text_patch.copy(deep=True))
            summary["written_text_updates"] = len(text_patch)
            logger.info("第一轮待处理任务正文更新完成：更新=%d，仅提交 text 列，状态和已有结果保留", len(text_patch))
        if not tasks.empty:
            cli.update_by_index(task_path, tasks.copy(deep=True))
            summary["written_rows"] = len(tasks)
            logger.info("第一轮任务追加完成：新增=%d，原任务状态和结果保留", len(tasks))
    elif not apply:
        logger.info("仅预览，未写数据库；查看清单后，以相同配置和来源执行 append_tasks apply")
    else:
        logger.info("本批无需追加或更新，已成功任务的正文和结果保留")
    logger.info('三轮处理日期可配置为 start_time: "%s" / end_time: "%s"', begin, end)
    return summary


def main(cli=None, argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["preview", "apply"])
    parser.add_argument("plan_dir", nargs="?", help="可选：上次 supplement_posts apply 的补录目录；省略时直接读取爬虫表")
    args = parser.parse_args(argv)
    settings = load_job_config(PROJECT_ROOT / "configs" / "post_extraction_config.yaml", ("source_path", "task_path"))
    logging.basicConfig(**settings.get("logging", DEFAULT_LOGGING))
    with stage_guard("round1", settings["task_path"]):
        if cli is None:
            from xdata.client import get_client
            cli = get_client()
        if args.plan_dir is None:
            return append_tasks_from_source(cli, settings, apply=args.action == "apply")
        return append_tasks_from_plan(cli, settings, args.plan_dir, apply=args.action == "apply")


if __name__ == "__main__":
    execute(main)
