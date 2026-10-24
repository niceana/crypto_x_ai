"""运行round1事件提取，并自动将本次范围内成功帖子导入事件去重分析表。"""

import logging
from dataclasses import replace
from pathlib import Path

import pandas as pd
from xdata.client import get_client

from common.config import load_event_config, load_job_config
from entrypoints.runtime import DEFAULT_LOGGING
from initialization.event_records import import_event_records_from_posts
from round1.runner import run_extraction


def main():
    """同一 cli 先运行round1事件提取，再自动补导成功帖子；返回round1事件提取统计。

    round1事件提取普通失败不阻止其他成功帖子导入；round1事件提取异常或中断直接传播。
    导入失败保留round1事件提取已保存结果，再运行同一范围时跳过成功帖 AI 并补导。
    """
    project_root = Path(__file__).resolve().parent.parent
    settings = load_job_config(project_root / "configs" / "post_extraction_config.yaml", ("task_path", "ai_config_path"))
    if "logging" in settings and not isinstance(settings["logging"], dict):
        raise ValueError("任务配置的 logging 必须为字典")

    ai_config_path = Path(settings["ai_config_path"])
    if not ai_config_path.is_absolute():
        ai_config_path = project_root / "configs" / ai_config_path
    ai_config_path = ai_config_path.resolve()
    if not ai_config_path.is_file():
        raise FileNotFoundError("AI 配置文件不存在")
    settings["ai_config_path"] = str(ai_config_path)

    # 后续任务沿用round1事件提取的实际来源和范围，不要求两份配置手工同步日期。
    event_settings = replace(
        load_event_config(project_root / "configs" / "event_processing_config.yaml"),
        first_round_task_path=settings["task_path"],
        start_time=settings.get("start_time"), end_time=settings.get("end_time"),
    )
    if event_settings.result_path == settings["task_path"]:
        raise ValueError("第一轮表与结果表路径必须不同")

    logging.basicConfig(**settings.get("logging", DEFAULT_LOGGING))
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # 客户端始终在当前调用线程使用，整轮处理复用同一个对象。
    cli = get_client()
    new_successes, previous_successes = [], []

    def collect_successful_posts(new_posts, previous_posts):
        if not new_posts.empty:
            new_successes.append(new_posts)
        if not previous_posts.empty:
            previous_successes.append(previous_posts)

    summary = run_extraction(
        cli, settings["task_path"], settings["ai_config_path"],
        start_time=settings.get("start_time"), end_time=settings.get("end_time"),
        on_successful_posts=collect_successful_posts,
        user_name_to_tickers=event_settings.user_name_to_tickers,
    )
    if summary.already_succeeded + summary.new_succeeded == 0:
        logging.getLogger(__name__).info("round1事件提取本次范围没有成功帖子，无需自动导入事件去重分析表")
        return summary
    logger = logging.getLogger(__name__)
    logger.info("round1事件提取已结束，开始将本次范围内已成功保存的帖子自动导入事件去重分析表")
    try:
        imported = import_event_records_from_posts(
            cli, event_settings,
            new_posts=pd.concat(new_successes).sort_index() if new_successes else None,
            previous_posts=pd.concat(previous_successes).sort_index() if previous_successes else None,
        )
    except Exception:
        logger.error("事件去重分析表自动导入未完成，round1帖子事件提取表已保存结果保留；重新运行同一范围的 python -m entrypoints.run_round1 可继续补导")
        raise
    logger.info("事件去重分析表自动导入完成：新增=%d", imported)
    return summary


def execute_round1():
    """处理第一轮入口的异常日志和退出码。"""
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception:
        logging.getLogger(__name__).exception("round1事件提取未完成")
        raise SystemExit(1) from None


if __name__ == "__main__":
    execute_round1()
