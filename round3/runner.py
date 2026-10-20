"""round3事件影响分析独立分析和并发调度；按正式提示词校验五字段与枚举。"""

import logging
from collections import deque
from datetime import date

from common.config import load_ai_config, require_prompts
from common.execution import month_ranges, run_segments
from common.event_context import build_impact_input
from common.network import NetworkWorker, RequestJob
from common.validation import validate_impact_output
from common.store import (
    EventRecordStore, StageSummary,
    validate_stage_config_hash, summarize,
)

logger = logging.getLogger(__name__)


def _prepare_impact(cli, settings, run, ranges):
    """先检查整个范围的第二轮结果，再按月恢复第三轮，不读取历史或原帖。

    首遍仅保留跨月唯一性检查所需的记录键和汇总；后月不合法时，前月也不
    恢复状态。第二轮已负责归组，本轮只检查各条记录自身的状态和基本关系。
    """
    summary = StageSummary("round3")
    needs_ai = False
    record_keys = {}
    for start, end in ranges:
        store = EventRecordStore(cli, settings, run, start, end)
        keys = store.keys_in_scope()
        store.require_relation(keys)
        for key in keys:
            previous = record_keys.setdefault(key[1], key)
            if previous != key:
                raise ValueError("record_id 跨分段重复，必须全表唯一")
        needs_ai = needs_ai or bool((store.df["round3_status"].isin(["0", "9", "-1"])
                                    & store.df["event_relation"].isin(["new", "update"])).any())
        # 遗留的 9 尚未恢复，此处不执行月末状态检查。
        summary.total += len(keys)
        summary.already_completed += store.count_completed("round3", keys)
        store = None

    if summary.total == summary.already_completed:
        return summary, needs_ai

    # 整个范围通过后才恢复；保留配置冲突前可能已经完成恢复的既有顺序。
    summary = StageSummary("round3")
    for start, end in ranges:
        store = EventRecordStore(cli, settings, run, start, end)
        keys = store.keys_in_scope()
        store.recover("round3", keys)
        already = store.count_completed("round3", keys)
        summary.add_month(summarize(store, keys, "round3", already))
        store = None
    return summary, needs_ai


def _impact_result(message):
    return message.key, message.response, message.http_attempts_started


def run_impact(cli, settings, ai_config=None, network_factory=NetworkWorker, validator=validate_impact_output):
    """检查本范围第二轮结果后，分月分析本条事件输入。"""
    if not settings.start_time or not settings.end_time:
        raise ValueError("第三轮必须配置 start_time/end_time，明确本次处理的事件日期范围")
    start, end = date.fromisoformat(settings.start_time), date.fromisoformat(settings.end_time)
    if start > end:
        raise ValueError("start_time 不能晚于 end_time")
    ranges = tuple(month_ranges(start, end))
    logger.info("round3事件影响分析启动准备：起点=%s，终点=%s，分段数=%d；先检查整个范围的第二轮状态，再恢复第三轮状态",
                settings.start_time, settings.end_time, len(ranges))
    summary, needs_ai = _prepare_impact(cli, settings, ai_config.run if ai_config else None, ranges)
    logger.info("round3事件影响分析启动准备完成：任务数=%d，已有完成=%d，待处理数=%d，失败数=%d，处理中数=%d",
                summary.total, summary.already_completed, summary.pending, summary.failed, summary.processing)
    if not needs_ai:
        logger.info("round3事件影响分析本范围无需调用 AI，返回当前任务统计")
        return summary
    config = ai_config or load_ai_config(settings.round3_ai_config_path)
    require_prompts(config)
    digest = validate_stage_config_hash(config, cli, settings.result_path, "round3_config_hash", "round3")

    def load_segment(start, end):
        store = EventRecordStore(cli, settings, config.run if config else None, start, end)
        keys = store.keys_in_scope()
        store.require_relation(keys)
        if config is not None:
            store.config = config
        already = store.count_completed("round3", keys)
        candidates = deque(key for key in store.pending_keys("round3", keys)
                           if store.df.loc[key, "event_relation"] in ("new", "update"))

        def claim_jobs(limit):
            selected = store.select_pending(candidates, limit, "round3")
            store.claim(selected, "round3", digest)
            return selected, (RequestJob(key, build_impact_input(store, key), validator)
                              for key in selected)

        return (store, candidates, claim_jobs, store.save_impact_batch,
                lambda: summary.add_month(summarize(store, keys, "round3", already)))

    summary = StageSummary("round3")
    run_segments(ranges, load_segment, summary,
                 lambda: network_factory(config), _impact_result, logger)
    logger.info("round3事件影响分析处理结束：%s", summary)
    return summary
