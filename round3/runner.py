"""round3事件影响分析独立分析和并发调度；按正式提示词校验五字段与枚举。"""

import logging
from collections import deque
from datetime import date, timedelta
from itertools import groupby

from common.config import load_ai_config, require_prompts
from common.execution import month_ranges, run_segments
from common.event_context import build_impact_input
from common.network import NetworkWorker, RequestJob
from common.validation import parse_strict_json, validate_impact_output
from common.store import (
    EventRecordStore, StageSummary,
    read_event_history, validate_stage_config_hash, summarize,
)
from common.time_rules import published_time, table_date_bounds

logger = logging.getLogger(__name__)

_REPEAT_RESPONSE_COLUMNS = ["asset", "event_id", "round2_status", "round3_status",
                            "round3_ai_response", "round3_config_hash"]


def _ordered_impact_keys(frame):
    return sorted(frame.index, key=lambda key: (published_time(key[0]), key[1]))


def _remember_impact_response(latest, groups, key, row):
    """校验并记住目标组的有效响应；调用方按披露时间推进，排除同刻和未来。"""
    group = row["asset"], row["event_id"]
    if group not in groups or row["round2_status"] != "1" or row["round3_status"] != "1":
        return
    raw, digest = row["round3_ai_response"], row["round3_config_hash"]
    try:
        if not isinstance(raw, str) or not raw or not isinstance(digest, str):
            raise ValueError("响应或配置摘要不是合法字符串")
        body = parse_strict_json(raw)
        if not isinstance(body, dict) or not isinstance(body.get("ai_response_content"), str):
            raise ValueError("响应缺少 ai_response_content 字符串")
        validate_impact_output(body["ai_response_content"])
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        logger.warning("repeat 继承跳过无效第三轮响应：record_id=%s，原因=%s", key[1], exc)
        return
    latest[group] = published_time(key[0]), raw, digest


def _load_repeat_sources(cli, settings, groups):
    """从近到远按月查找范围前的有效来源，每组只保留最近一条响应。"""
    latest = {}
    begin, _ = table_date_bounds(cli.read_meta(settings.result_path))
    end = date.fromisoformat(settings.start_time) - timedelta(days=1)
    if begin is not None and begin <= end:
        # 继承没有三个月期限；所有目标组找到有效来源后即停止读取。
        for start, finish in reversed(tuple(month_ranges(begin, end))):
            frame = read_event_history(cli, settings.result_path, start, finish,
                                       columns=_REPEAT_RESPONSE_COLUMNS, label="repeat 第三轮响应历史读取")
            for key in reversed(_ordered_impact_keys(frame)):
                row = frame.loc[key]
                if (row["asset"], row["event_id"]) not in latest:
                    _remember_impact_response(latest, groups, key, row)
            if groups.issubset(latest):
                break
    return latest


def _complete_repeat_impacts(store, groups, latest):
    """本月 AI 结果全部保存后，按完整披露时间复制 repeat，再推进组历史。"""
    for stamp, keys in groupby(_ordered_impact_keys(store.df), key=lambda key: published_time(key[0])):
        keys = list(keys)
        items = []
        for key in keys:
            row = store.df.loc[key]
            if row["event_relation"] != "repeat" or row["round3_status"] != "0":
                continue
            source = latest.get((row["asset"], row["event_id"]))
            response, digest = (source[1], source[2]) if source is not None and source[0] < stamp else (None, "")
            items.append((key, response, digest))
            if len(items) >= store.config.run.batch_size:
                store.save_repeat_impact_batch(items)
                items.clear()
        if items:
            store.save_repeat_impact_batch(items)
        # 同刻的所有复制结束后才推进，避免同刻响应互相继承；同分取最大 record_id。
        for key in keys:
            _remember_impact_response(latest, groups, key, store.df.loc[key])


def _prepare_impact(cli, settings, run, ranges):
    """先检查整个范围的第二轮结果，再按月恢复第三轮，不读取原帖。

    首遍仅保留跨月唯一性检查所需的记录键和汇总；后月不合法时，前月也不
    恢复状态。第二轮已负责归组，本轮只检查各条记录自身的状态和基本关系。
    """
    summary = StageSummary("round3")
    needs_ai = False
    repeat_groups = set()
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
        for key in keys:
            row = store.df.loc[key]
            if row["event_relation"] == "repeat" and row["round3_status"] in ("0", "9", "-1"):
                repeat_groups.add((row["asset"], row["event_id"]))
        # 遗留的 9 尚未恢复，此处不执行月末状态检查。
        summary.total += len(keys)
        summary.already_completed += store.count_completed("round3", keys)
        store = None

    if summary.total == summary.already_completed:
        return summary, needs_ai, repeat_groups

    # 整个范围通过后才恢复；保留配置冲突前可能已经完成恢复的既有顺序。
    summary = StageSummary("round3")
    for start, end in ranges:
        store = EventRecordStore(cli, settings, run, start, end)
        keys = store.keys_in_scope()
        store.recover("round3", keys)
        already = store.count_completed("round3", keys)
        summary.add_month(summarize(store, keys, "round3", already))
        store = None
    return summary, needs_ai, repeat_groups


def _impact_result(message):
    return message.key, message.response, message.http_attempts_started


def run_impact(cli, settings, ai_config=None, network_factory=NetworkWorker, validator=validate_impact_output):
    """检查本范围第二轮结果后，分月分析 new/update，并为 repeat 复制此前响应。"""
    if not settings.start_time or not settings.end_time:
        raise ValueError("第三轮必须配置 start_time/end_time，明确本次处理的事件日期范围")
    start, end = date.fromisoformat(settings.start_time), date.fromisoformat(settings.end_time)
    if start > end:
        raise ValueError("start_time 不能晚于 end_time")
    ranges = tuple(month_ranges(start, end))
    logger.info("round3事件影响分析启动准备：起点=%s，终点=%s，分段数=%d；先检查整个范围的第二轮状态，再恢复第三轮状态",
                settings.start_time, settings.end_time, len(ranges))
    summary, needs_ai, repeat_groups = _prepare_impact(cli, settings, ai_config.run if ai_config else None, ranges)
    logger.info("round3事件影响分析启动准备完成：任务数=%d，已有完成=%d，待处理数=%d，失败数=%d，处理中数=%d",
                summary.total, summary.already_completed, summary.pending, summary.failed, summary.processing)
    if not needs_ai and not repeat_groups:
        logger.info("round3事件影响分析本范围无需调用 AI，返回当前任务统计")
        return summary
    config, digest = None, None
    if needs_ai:
        config = ai_config or load_ai_config(settings.round3_ai_config_path)
        require_prompts(config)
        digest = validate_stage_config_hash(config, cli, settings.result_path, "round3_config_hash", "round3")
    repeat_sources = _load_repeat_sources(cli, settings, repeat_groups) if repeat_groups else {}
    run = config.run if config is not None else (ai_config.run if ai_config else None)

    def load_segment(start, end):
        store = EventRecordStore(cli, settings, run, start, end)
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

        def finish_month():
            if repeat_groups:
                _complete_repeat_impacts(store, repeat_groups, repeat_sources)
            return summary.add_month(summarize(store, keys, "round3", already))

        return (store, candidates, claim_jobs, store.save_impact_batch,
                finish_month)

    summary = StageSummary("round3")
    run_segments(ranges, load_segment, summary,
                 lambda: network_factory(config), _impact_result, logger)
    logger.info("round3事件影响分析处理结束：%s", summary)
    return summary
