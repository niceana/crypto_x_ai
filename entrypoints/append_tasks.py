"""把已补录批次追加为第一轮任务；保留已有任务的状态和 AI 结果。"""

import argparse
import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from common.config import load_job_config
from common.process_lock import stage_guard
from common.validation import TASK_COLUMNS, validate_post_index, validate_post_strings, validate_post_task_structure
from entrypoints.runtime import DEFAULT_LOGGING, execute


logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def append_tasks_from_plan(cli, settings, plan_dir, *, apply=False):
    """使用补录时保存的批次，只给已有第一轮任务表追加缺失索引。"""
    source_path, task_path = settings["source_path"], settings["task_path"]
    if any(not isinstance(path, str) or not path.strip() for path in (source_path, task_path)) or source_path == task_path:
        raise ValueError("请填写不同的 source_path 和已有 task_path")
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
    summary = {"batch_rows": len(batch), "already_in_tasks": 0, "new_tasks": 0,
               "written_rows": 0, "start_time": "", "end_time": ""}
    if batch.empty:
        logger.info("本批没有新增原始帖子，无需追加第一轮任务")
        return summary
    dates = batch.index.get_level_values("date")
    begin, end = pd.Timestamp(dates.min()).date().isoformat(), pd.Timestamp(dates.max()).date().isoformat()
    summary.update(start_time=begin, end_time=end)
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
    existing = cli.read(task_path, start_time="", end_time="")
    validate_post_task_structure(existing)
    validate_post_strings(existing, TASK_COLUMNS)
    new_source = selected.loc[~selected.index.isin(existing.index)].sort_index()
    if new_source.index.get_level_values("id").isin(existing.index.get_level_values("id")).any():
        raise ValueError("第一轮任务表已存在同 ID 不同 date 的记录，尚未追加任务")
    shared = selected.index[selected.index.isin(existing.index)]
    if len(shared) and not existing.loc[shared, ["text", "user_name"]].equals(selected.loc[shared]):
        raise ValueError("第一轮已有任务的正文或账号与本批不一致，尚未追加任务")
    tasks = pd.DataFrame("", index=new_source.index, columns=TASK_COLUMNS, dtype=object)
    tasks["text"] = new_source["text"].to_numpy()
    tasks["user_name"] = new_source["user_name"].to_numpy()
    tasks["status"] = "0"
    tasks = tasks.loc[:, existing.columns]
    summary.update(already_in_tasks=len(shared), new_tasks=len(tasks))
    tasks.to_csv(output / "round1_tasks_to_append.csv", encoding="utf-8-sig")
    logger.info("第一轮增量任务预览：本批=%d，已在任务表=%d，待追加=%d；日期=%s 至 %s；清单=%s",
                len(batch), len(shared), len(tasks), begin, end, output / "round1_tasks_to_append.csv")
    if apply and not tasks.empty:
        stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
        backup = output / "round1_backups" / stamp
        backup.mkdir(parents=True, exist_ok=False)
        existing.to_pickle(backup / "tasks.pkl")
        pd.testing.assert_frame_equal(existing, pd.read_pickle(backup / "tasks.pkl"), check_exact=True)
        logger.info("追加前完整第一轮任务表已备份：%s", backup / "tasks.pkl")
        cli.update_by_index(task_path, tasks.copy(deep=True))
        summary["written_rows"] = len(tasks)
        logger.info("第一轮任务追加完成：新增=%d，原任务状态和结果保留", len(tasks))
    elif not apply:
        logger.info("仅预览，未写数据库；确认清单后执行 append_tasks apply，使用同一个补录目录")
    else:
        logger.info("本批任务均已存在，无需追加")
    logger.info('三轮处理日期可配置为 start_time: "%s" / end_time: "%s"', begin, end)
    return summary


def main(cli=None, argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["preview", "apply"])
    parser.add_argument("plan_dir", help="上次 supplement_posts apply 使用的同一个补录目录")
    args = parser.parse_args(argv)
    settings = load_job_config(PROJECT_ROOT / "configs" / "post_extraction_config.yaml", ("source_path", "task_path"))
    logging.basicConfig(**settings.get("logging", DEFAULT_LOGGING))
    with stage_guard("round1", settings["task_path"]):
        if cli is None:
            from xdata.client import get_client
            cli = get_client()
        return append_tasks_from_plan(cli, settings, args.plan_dir, apply=args.action == "apply")


if __name__ == "__main__":
    execute(main)
