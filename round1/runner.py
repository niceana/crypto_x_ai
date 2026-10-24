"""调用线程按自然月协调round1事件提取任务，共用网络线程执行请求。"""

import logging
import time
from dataclasses import dataclass
from datetime import date

from common.config import load_ai_config
from common.event_context import build_extraction_input, prepare_account_tickers
from common.network import RequestJob, NetworkWorker as _NetworkWorker, _create_client, parse_retry_after as _retry_after
from common.execution import check_month_complete, month_ranges as _month_ranges, run_segments
from common.validation import validate_extraction_output
from common.store import PostTaskStore

logger = logging.getLogger(__name__)


@dataclass
class ExtractionSummary:
    """累加各分段标量统计，不持有全年 round1帖子事件提取表或响应。"""

    total: int = 0
    already_succeeded: int = 0
    new_succeeded: int = 0
    new_failed: int = 0
    pending: int = 0
    processing: int = 0
    http_attempts_started: int = 0
    elapsed_seconds: float = 0

    def add_month(self, store):
        counts = store.summarize("round1", already=store.already_completed)
        check_month_complete(counts["processing"])
        for target, source in (("total", "total"), ("already_succeeded", "already_completed"),
                               ("new_succeeded", "new_completed"), ("new_failed", "failed"),
                               ("pending", "pending"), ("processing", "processing")):
            setattr(self, target, getattr(self, target) + counts[source])
        return counts["new_completed"], counts["failed"]


def run_extraction(cli, task_path, config_path, start_time=None, end_time=None, *,
               on_successful_posts=None, user_name_to_tickers=None) -> ExtractionSummary:
    """同步处理指定日期范围；当前月结果全部确认后才读取下一月。

    cli 直接读取 task_path 并按原索引更新指定列，始终在调用入口线程执行。
    start_time/end_time 同时设置时按自然月处理左闭右闭的日期范围；均为空时读全表。
    正常返回跨月份 ExtractionSummary；全局停止抛 BatchRunError，保存失败抛
    BatchPersistenceError，用户中断继续传播 KeyboardInterrupt。
    on_successful_posts(new_posts, previous_posts) 可在分段保存结束后接收成功帖副本；
    previous_posts 是本段载入时已经成功的帖子，两表仅含原索引和后续处理所需字段。
    user_name_to_tickers 仅作为 AI 判断事件归属的账号背景，不在导入时兜底。
    """
    started = time.monotonic()
    logger.info("round1事件提取开始启动")
    try:
        if start_time is None and end_time is None:
            start, end = None, None
        else:
            if start_time is None or end_time is None:
                raise ValueError("起止日期必须同时设置或同时不设置")
            start = date.fromisoformat(start_time)
            end = date.fromisoformat(end_time)
            if start > end:
                raise ValueError("start_time 不能晚于 end_time")
        config = load_ai_config(config_path)
        account_tickers = prepare_account_tickers(user_name_to_tickers)
    except Exception:
        logger.exception("round1事件提取启动校验失败；尚未读取或更新round1帖子事件提取表")
        raise
    logger.info("round1事件提取启动校验完成：起点=%s，终点=%s，并发上限=%d，结果批次=%d，单帖尝试上限=%d",
                start, end, config.run.concurrency, config.run.batch_size, config.run.max_attempts)
    summary = ExtractionSummary()
    digest = None

    def load_segment(segment_start, segment_end):
        store = PostTaskStore(config, cli, task_path, segment_start, segment_end)
        store.digest = digest
        previous = store.df.index[store.df["status"].eq("1")] if on_successful_posts is not None else None
        store.recover()

        def claim_jobs(limit):
            nonlocal digest
            keys = store.select(limit)
            jobs = store.claim(keys)
            digest = store.digest
            return keys, (RequestJob(key, build_extraction_input(store.df.loc[key], account_tickers),
                                     validate_extraction_output) for key, _ in jobs)

        def summarize_month():
            counts = summary.add_month(store)
            if on_successful_posts is not None:
                succeeded = store.df["status"].eq("1")
                if succeeded.any():
                    was_successful = store.df.index.isin(previous)
                    columns = ["text", "user_name", "status", "ai_response_content"]
                    # 月末检查已经通过，交出的新成功结果均已落库；不携带失败或无关 AI 字段。
                    on_successful_posts(
                        store.df.loc[succeeded & ~was_successful, columns].copy(deep=True),
                        store.df.loc[succeeded & was_successful, columns].copy(deep=True),
                    )
            return counts

        return store, store.candidates, claim_jobs, store.save_results, summarize_month

    def create_network():
        return _NetworkWorker(config, client_factory=_create_client,
                              retry_after=_retry_after, log=logger)

    run_segments(_month_ranges(start, end), load_segment, summary, create_network,
                 log=logger)
    summary.elapsed_seconds = time.monotonic() - started
    scope = "本次读取范围" if start is None else "本次日期范围"
    logger.info("round1事件提取：%s内的任务全部处理完了：成功 %d，失败 %d；HTTP起发=%d，耗时=%.3f秒",
                scope, summary.new_succeeded, summary.new_failed,
                summary.http_attempts_started, summary.elapsed_seconds)
    return summary
