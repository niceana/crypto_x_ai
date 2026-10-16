"""round3事件影响分析独立分析和并发调度；评分口径由待配置提示词与校验接口提供。"""

import logging
from bisect import bisect_right
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, timedelta

from common.config import load_ai_config, require_prompts
from common.execution import month_ranges, run_segments
from common.event_context import EventHistory, build_impact_input, load_source_texts
from common.network import NetworkWorker, RequestJob
from common.validation import validate_impact_output, validate_index
from common.store import (
    EventRecordStore, StageSummary, is_stored_relation_valid,
    validate_stage_config_hash, summarize,
)
from common.time_rules import history_window_start, published_time

logger = logging.getLogger(__name__)

# 第三轮按用户确认固定回看五个日历年，不受第二轮可配置的期限影响。
HISTORY_LOOKBACK_YEARS = 5
HISTORY_COLUMNS = ["asset", "event_id", "event_relation", "event", "new_information", "round2_status"]


@dataclass
class _ImpactContext:
    """跨分段保留所需组的五年事实、起点和统计，不保留原帖或完整任务行。"""

    history: EventHistory
    roots: dict
    needs_ai: bool
    summary: StageSummary


def _prepare_impact(cli, settings, run, ranges):
    """先检查任务范围，再按未完成 update 的五年窗口补读历史，最后恢复状态。

    首遍暂存范围内的必要事实，确定所需组后筛除无关记录；补读只取六列，
    不查询表元数据或窗口外起点。任何月份或所需历史不合法时都不先恢复状态。
    """
    context = _ImpactContext(EventHistory(history_lookback_years=HISTORY_LOOKBACK_YEARS,
                                          truncate_group_history=True), {}, False, StageSummary("round3"))
    scope_records, pending_updates = [], []
    record_keys = {}
    for start, end in ranges:
        store = EventRecordStore(cli, settings, run, start, end)
        keys = store.keys_in_scope()
        # 先检查第二轮状态和基本关系。组起点仅在待分析任务确实需要时补读。
        store.require_relation(keys, {}, HISTORY_LOOKBACK_YEARS)
        retryable = set(store.df.index[store.df["round3_status"].isin(["0", "9", "-1"])])
        for key, row in store.df.iterrows():
            _remember_record_key(record_keys, key)
            facts = {name: row[name] for name in HISTORY_COLUMNS}
            scope_records.append((key, facts))
            if key in retryable and row["event_relation"] in ("new", "update"):
                context.needs_ai = True
                if row["event_relation"] == "update":
                    pending_updates.append((key, facts))
        already = store.count_completed("round3", keys)
        # 此处仍可能有待恢复的 9，不能调用带月末状态检查的 add_month。
        context.summary.total += len(keys)
        context.summary.already_completed += already
        # 不保留 DataFrame 行视图，下一月开始前释放本月完整任务。
        store = None

    if context.summary.total == context.summary.already_completed:
        return context

    times = defaultdict(list)
    for key, row in pending_updates:
        times[(row["asset"], row["event_id"])].append(published_time(key[0]))
    times = {group: sorted(set(stamps)) for group, stamps in times.items()}
    selected_records = []
    root_ids = {event_id for _, event_id in times}
    for key, row in scope_records:
        _select_impact_history(context, key, row, times, root_ids, selected_records)
    del scope_records

    if pending_updates:
        begin = min(history_window_start(key[0], HISTORY_LOOKBACK_YEARS).date()
                    for key, _ in pending_updates)
        history_end = date.fromisoformat(settings.start_time) - timedelta(days=1)
        # 当前范围的事实已在首遍暂存；只补读该范围之前缺少的日期。
        if begin <= history_end:
            for start, end in month_ranges(begin, history_end):
                frame = cli.read(settings.result_path, start_time=start, end_time=end, columns=HISTORY_COLUMNS)
                validate_index(frame, "record_id")
                if not set(HISTORY_COLUMNS).issubset(frame.columns):
                    raise ValueError("第三轮历史读取缺少必要列")
                first_day, last_day = date.fromisoformat(start), date.fromisoformat(end)
                for key, row in frame.iterrows():
                    if not first_day <= published_time(key[0]).date() <= last_day:
                        raise ValueError("第三轮历史读取返回了分段日期以外的行")
                    _remember_record_key(record_keys, key)
                    _select_impact_history(context, key, row, times, root_ids, selected_records)
                frame = None

    # 一次建立有序索引，避免先载入当前事实、再插入多年旧事实产生大量列表移动。
    context.history.add_records(selected_records)
    for key, row in pending_updates:
        stamp = published_time(key[0])
        begin = history_window_start(stamp, HISTORY_LOOKBACK_YEARS)
        if not is_stored_relation_valid(key, row, None, context.roots,
                                        origin_start=begin):
            raise ValueError("第二轮未完成或结果不合法：{}".format(key[1]))
        if not any(begin <= time < stamp for time, _ in
                   context.history.groups.get((row["asset"], row["event_id"]), ())):
            raise ValueError("第三轮所需5年窗口内没有可用事件组历史：{} / {}".format(row["asset"], key[1]))
        context.history.build_group_history(row["asset"], row["event_id"], key[0])

    # 保留原顺序：整个范围和历史通过后恢复；配置冲突发生时恢复可能已完成。
    context.summary = StageSummary("round3")
    for start, end in ranges:
        store = EventRecordStore(cli, settings, run, start, end)
        keys = store.keys_in_scope()
        store.recover("round3", keys)
        already = store.count_completed("round3", keys)
        context.summary.add_month(summarize(store, keys, "round3", already))
        store = None
    return context


def _remember_record_key(record_keys, key):
    """只检查本次实际读取的数据，防止同一 ID 跨日期出现而串接历史。"""
    previous = record_keys.setdefault(key[1], key)
    if previous != key:
        raise ValueError("record_id 跨分段重复，必须全表唯一")


def _select_impact_history(context, key, row, times, root_ids, selected_records):
    """保留已读到的目标起点，并只收集至少落入一个待分析窗口的同组事实。"""
    if key[1] in root_ids:
        context.roots[key[1]] = key, {name: row[name] for name in (
            "round2_status", "event_relation", "event_id", "asset")}
    stamps = times.get((row["asset"], row["event_id"]))
    if not stamps or row["round2_status"] != "1":
        return
    stamp = published_time(key[0])
    position = bisect_right(stamps, stamp)
    if position < len(stamps) and stamp >= history_window_start(stamps[position], HISTORY_LOOKBACK_YEARS):
        if row["event_relation"] == "new" and row["event_id"] != key[1]:
            raise ValueError("第三轮窗口内组起点未引用自身：{}".format(key[1]))
        selected_records.append((key, {name: row[name] for name in HISTORY_COLUMNS}))


def _impact_result(message):
    return message.key, message.response, message.http_attempts_started


def run_impact(cli, settings, ai_config=None, network_factory=NetworkWorker, validator=validate_impact_output):
    """按配置范围检查任务和五年历史后，分段处理，只为待分析任务读原文。"""
    if not settings.start_time or not settings.end_time:
        raise ValueError("第三轮必须配置 start_time/end_time，明确本次处理的事件日期范围")
    start, end = date.fromisoformat(settings.start_time), date.fromisoformat(settings.end_time)
    if start > end:
        raise ValueError("start_time 不能晚于 end_time")
    ranges = tuple(month_ranges(start, end))
    context = _prepare_impact(cli, settings, ai_config.run if ai_config else None, ranges)
    if not context.needs_ai:
        return context.summary
    config = ai_config or load_ai_config(settings.round3_ai_config_path)
    require_prompts(config)
    digest = validate_stage_config_hash(config, cli, settings.result_path, "round3_config_hash", "round3")

    def load_segment(start, end):
        store = EventRecordStore(cli, settings, config.run if config else None, start, end)
        keys = store.keys_in_scope()
        store.require_relation(keys, context.roots, HISTORY_LOOKBACK_YEARS)
        if config is not None:
            store.config = config
        already = store.count_completed("round3", keys)
        candidates = deque(key for key in store.pending_keys("round3", keys)
                           if store.df.loc[key, "event_relation"] in ("new", "update"))
        source_texts = load_source_texts(store, candidates)

        def claim_jobs(limit):
            selected = store.select_pending(candidates, limit, "round3")
            store.claim(selected, "round3", digest)
            return selected, (RequestJob(key, build_impact_input(store, key, source_texts, context.history), validator)
                              for key in selected)

        return (store, candidates, claim_jobs, store.save_impact_batch,
                lambda: summary.add_month(summarize(store, keys, "round3", already)))

    summary = StageSummary("round3")
    run_segments(ranges, load_segment, summary,
                 lambda: network_factory(config), _impact_result, logger)
    logger.info("round3事件影响分析处理结束：%s", summary)
    return summary
