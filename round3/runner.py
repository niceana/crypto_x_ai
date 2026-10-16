"""round3事件影响分析独立分析和并发调度；评分口径由待配置提示词与校验接口提供。"""

import json
import logging
from collections import deque
from dataclasses import dataclass
from datetime import date

import pandas as pd

from common.config import load_ai_config, require_prompts
from common.execution import month_ranges, run_segments
from common.event_context import EventHistory, build_impact_input, load_source_texts
from common.network import NetworkWorker, RequestJob
from common.validation import validate_impact_output
from common.store import EventRecordStore, StageSummary, validate_stage_config_hash, summarize

logger = logging.getLogger(__name__)


@dataclass
class _ImpactContext:
    """跨分段只保留必要事件历史、起点和任务标识，不保留原帖及完整任务行。"""

    history: EventHistory
    roots: dict
    needs_ai: bool


def _prepare_impact(cli, settings, run, ranges):
    """合并全表与处理范围的只读月检查，全部通过后再恢复round3事件影响分析。

    同月结果只读一次；读取边界覆盖全表元数据段与处理段，不读取原帖。
    检查与恢复分开，保证后月round2事件去重归组不合法时，前月不会先恢复或发请求。
    """
    table_ranges = _result_table_ranges(cli, settings.result_path) if settings.start_time else ((None, None),)
    table_months = {start[:7] if start else None: (start, end) for start, end in table_ranges}
    scope_months = {start[:7] if start else None: (start, end) for start, end in ranges}
    context = _ImpactContext(EventHistory(), {}, False)
    seen_records = set()
    for month in sorted(table_months.keys() | scope_months.keys()):
        table_range, scope_range = table_months.get(month), scope_months.get(month)
        start, end = table_range or scope_range
        if scope_range and start is not None:
            start, end = min(start, scope_range[0]), max(end, scope_range[1])
        store = EventRecordStore(cli, settings, run, start, end)
        if not table_months and not store.df.empty:
            raise ValueError("元数据时间范围为空，但结果表分段包含记录")
        if table_range:
            ids = set(store.df.index.get_level_values("record_id"))
            if seen_records.intersection(ids):
                raise ValueError("record_id 跨分段重复，必须全表唯一")
            seen_records.update(ids)
            # 同月全部起点先进入索引，再检查关联；合法性仍要求严格早于当前行。
            _collect_impact_context(context, store)
        if scope_range:
            keys = store.keys_in_scope()
            store.require_relation(keys, context.roots)
        # 只留下必要历史和标识；下一月读取前释放事件去重分析表的完整月数据。
        store = None
    del seen_records

    # 保留原顺序：整个范围round2事件去重归组通过后恢复；配置冲突发生时恢复可能已完成。
    for start, end in ranges:
        store = EventRecordStore(cli, settings, run, start, end)
        keys = store.keys_in_scope()
        store.recover("round3", keys)
        context.needs_ai |= any(store.df.loc[key, "event_relation"] in ("new", "update")
                                for key in store.pending_keys("round3", keys))
        store = None
    return context


def _result_table_ranges(cli, path):
    """按用户提供的元数据接口确定整表扫描边界，不猜测数据起点或截止日。"""
    document = cli.read_meta(path)
    try:
        if isinstance(document, str):
            document = json.loads(document)
        meta = document["meta"]
        values = [meta["start_time"], meta["end_time"]]
        if values == [None, None]:
            return ()
        if any(not isinstance(value, str) or not value for value in values):
            raise ValueError("时间须为 ISO 字符串，空表两端须同时为 null")
        # 只提取原时间日历日期；带时区的 ISO 时间也不转换时区。
        # pandas 沿用项目已有的纳秒解析能力，避免 Python 3.8 拒绝九位小数。
        for value in values:
            date.fromisoformat(value[:10])
        start, end = [pd.Timestamp(value) for value in values]
        if pd.isna(start) or pd.isna(end):
            raise ValueError("时间不能缺失")
        if start > end:
            raise ValueError("起点晚于终点")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("结果表元数据须提供合法的 meta.start_time/end_time：{}".format(exc)) from exc
    return tuple(month_ranges(start.date(), end.date()))


def _collect_impact_context(context, store):
    """跨段只累计历史和起点必要字段；摘要另用全表按列接口核对。"""
    names = ["round2_status", "event_relation", "event_id", "asset"]
    for key, row in store.df.iterrows():
        if row["event_relation"] == "new":
            context.roots[key[1]] = key, row[names].to_dict()
    context.history.add_frame(store.df)


def _impact_result(message):
    return message.key, message.response, message.http_attempts_started


def run_impact(cli, settings, ai_config=None, network_factory=NetworkWorker, validator=validate_impact_output):
    """全范围已有任务通过检查后，按状态分段处理，只为待分析任务读原文。"""
    start = date.fromisoformat(settings.start_time) if settings.start_time else None
    end = date.fromisoformat(settings.end_time) if settings.end_time else None
    ranges = tuple(month_ranges(start, end))
    context = _prepare_impact(cli, settings, ai_config.run if ai_config else None, ranges)
    config = ai_config
    digest = None
    if context.needs_ai:
        config = config or load_ai_config(settings.round3_ai_config_path)
        require_prompts(config)
        digest = validate_stage_config_hash(config, cli, settings.result_path, "round3_config_hash", "round3")

    def load_segment(start, end):
        store = EventRecordStore(cli, settings, config.run if config else None, start, end)
        keys = store.keys_in_scope()
        store.require_relation(keys, context.roots)
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
