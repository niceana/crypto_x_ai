"""round2事件去重归组业务与调度：同代币按时间推进，相同时间使用同一份历史。"""

import logging
import queue
from collections import defaultdict, deque
from datetime import date, timedelta

from common.config import load_ai_config, load_embedding_config, require_prompts
from common.event_context import (
    EventHistory,
    build_relation_input,
    build_current_record_input,
)
from common.network import BatchRunError, NetworkWorker, RequestJob
from common.validation import validate_relation_output
from common.persistence import BatchPersistenceError
from common.store import EventRecordStore, read_event_history, validate_relation_history, summarize
from common.time_rules import history_read_bounds, is_history_needed, published_time, table_date_bounds
from round2.retrieval import validate_retrieval_selection, load_retriever
from round2.embedding import EmbeddingRetriever

logger = logging.getLogger(__name__)


def _load_relation_history(store, pending_keys):
    """按日期补入必要前史，范围之后仅读取补录检查所需的两列。"""
    settings = store.settings
    # 第二轮继续以原有的最早待处理时刻作为补读基准。
    earliest = min(published_time(key[0]) for key in pending_keys)
    bounds = history_read_bounds((earliest,), settings.start_time, settings.history_lookback_years)
    if bounds is not None:
        begin, history_end = bounds
        store.load_relation_history(begin.isoformat(), history_end.isoformat())
    _, table_end = table_date_bounds(store.cli.read_meta(settings.result_path))
    if table_end is None:
        raise ValueError("元数据时间范围为空，但结果表包含待处理记录")
    scope_end = date.fromisoformat(settings.end_time)
    if scope_end >= table_end:
        return None
    later_begin = scope_end + timedelta(days=1)
    return read_event_history(store.cli, settings.result_path, later_begin.isoformat(), table_end.isoformat(),
                              columns=["asset", "round2_status"], label="补录检查读取")


def run_relation(cli, settings, ai_config=None, retriever=None, network_factory=NetworkWorker):
    """读取已有任务，恢复状态，按币协调检索、请求与即时保存。

    调用线程独占 cli；同币下一时点必须等当前时间全部结果保存确认。相同
    时间共享此前历史，普通失败只暂停该币，输入和候选不会额外落库。
    """
    if not settings.start_time or not settings.end_time:
        raise ValueError("第二轮必须配置 start_time/end_time，明确本次处理的事件日期范围")
    if date.fromisoformat(settings.start_time) > date.fromisoformat(settings.end_time):
        raise ValueError("start_time 不能晚于 end_time")
    store = EventRecordStore(cli, settings, ai_config.run if ai_config else None,
                             settings.start_time, settings.end_time)
    keys = store.keys_in_scope()
    store.recover("round2", keys)
    if store.count_completed("round2", keys) == len(keys):
        return summarize(store, keys, "round2", len(keys))
    pending_keys = store.pending_keys("round2", keys)
    if not pending_keys:
        return summarize(store, keys, "round2", store.count_completed("round2", keys))
    later_records = _load_relation_history(store, pending_keys)
    validate_relation_history(store, keys, settings.history_lookback_years, later_records)
    del later_records
    embedding = None
    if retriever is not None:
        retrieve = retriever
    elif settings.retriever or not settings.embedding_config_path:
        retrieve = load_retriever(settings.retriever)
    else:
        embedding = EmbeddingRetriever(load_embedding_config(settings.embedding_config_path))
        retrieve = embedding
    config = ai_config or load_ai_config(settings.round2_ai_config_path)
    require_prompts(config)
    store.config = config
    digest = store.check_config("round2")
    already = store.count_completed("round2", keys)
    history = EventHistory(store.df, settings.history_lookback_years, truncate_group_history=True)
    grouped = defaultdict(lambda: defaultdict(list))
    for key in pending_keys:
        grouped[store.df.loc[key, "asset"]][published_time(key[0])].append(key)
    if embedding is not None:
        embedding_keys = set(pending_keys)
        times = {asset: sorted(batches) for asset, batches in grouped.items()}
        for key, row in store.df.iterrows():
            if row["round2_status"] != "1" or row["asset"] not in times:
                continue
            if is_history_needed(key[0], times[row["asset"]], settings.history_lookback_years):
                embedding_keys.add(key)
        # 编码固定事实可以提前批处理；实际候选仍由 history 严格按披露时点筛选。
        # 向量按批写入事件去重分析表后才开始认领，避免同步请求阻塞关联结果的保存。
        ordered = sorted(embedding_keys, key=lambda key: (published_time(key[0]), key[1]))
        embedding.prepare(((key, store.df.at[key, "event"], store.df.at[key, "embedding"])
                           for key in ordered), store.save_embeddings)
    waves = {asset: deque(deque(sorted(batch, key=lambda key: key[1])) for _, batch in sorted(times.items()))
             for asset, times in sorted(grouped.items())}
    logger.info("round2事件去重归组准备运行：范围记录=%d，已有完成=%d，待处理资产=%d", len(keys), already, len(waves))
    active, confirmed, blocked = {}, defaultdict(list), set()
    network = network_factory(config)
    current_result = []

    def consume(message):
        asset = active.get(message.key)
        if asset is None:
            raise BatchRunError("收到未提交或重复的第二轮结果")
        if message.error is not None:
            raise BatchRunError("第二轮网络作业发生未预期异常") from message.error
        if message.response is None:
            active.pop(message.key)
            return
        # 消息已经出队；保存成功前保留这一条，以便失败或中断后交给调用方。
        current_result[:] = [message]
        store.save_relation(message.key, message.value, message.response, message.http_attempts_started)
        if message.response.success:
            confirmed[asset].append(message.key)
        else:
            blocked.add(asset)
            logger.warning("round2事件去重归组最终失败，暂停同代币后续关联：asset=%s，record_id=%s", asset, message.key[1])
        current_result.clear()
        active.pop(message.key)

    def finish():
        if current_result:
            consume(current_result[0])
        while not network.completions.empty():
            consume(network.completions.get_nowait())
        while network.unpublished:
            consume(network.unpublished.pop(0))

    def retain_unsaved(error):
        error.unsaved_results = current_result
        error.completed_results = network.completions
        error.unpublished_results = network.unpublished

    def advance():
        # 相同时间允许共同并发，全部落库后才加入历史，避免用同刻答复互相匹配。
        for asset in list(waves):
            if asset in blocked:
                continue
            if waves[asset] and not waves[asset][0] and asset not in active.values():
                for key in confirmed.pop(asset, []):
                    history.add(key, store.df.loc[key])
                waves[asset].popleft()

    try:
        try:
            network.start()
            while True:
                network.check()
                advance()
                launched = False
                for asset, batches in waves.items():
                    if len(active) >= config.run.concurrency:
                        break
                    if asset in blocked or not batches or not batches[0]:
                        continue
                    selected = store.select_pending(batches[0], 1, "round2")
                    if not selected:
                        continue
                    key = selected[0]
                    store.claim([key], "round2", digest)
                    row = store.df.loc[key]
                    current = build_current_record_input(key, row)
                    searchable = history.get_searchable_records(asset, key[0])
                    selection = validate_retrieval_selection(retrieve(current, searchable), {record["event_id"] for record in searchable})
                    if selection.auto_new:
                        store.save_relation(key, {"event_relation": "new", "matched_event_id": None, "new_information": None})
                        confirmed[asset].append(key)
                        logger.info("round2事件去重归组自动判新：record_id=%s，decision_source=auto_new，规则及原因=%s", key[1], selection.reason)
                    else:
                        candidates = [history.build_group_history(asset, event_id, key[0]) for event_id in selection.event_ids]
                        text, candidate_event_ids = build_relation_input(current, candidates)
                        active[key] = asset
                        # 每个作业独立捕获映射，所有重试复用它，避免并发请求的 E1 串组。
                        network.submit(RequestJob(key, text, lambda content, ids=candidate_event_ids:
                                                  validate_relation_output(content, ids)))
                    launched = True
                advance()
                if not active and not any(batches and asset not in blocked for asset, batches in waves.items()):
                    break
                if launched and len(active) < config.run.concurrency:
                    continue
                try:
                    consume(network.completions.get(timeout=0.25))
                except queue.Empty:
                    pass
        finally:
            network.shutdown()
        if network.failure is not None:
            raise BatchRunError("第二轮网络运行或关闭失败") from network.failure
    except BaseException as error:
        try:
            # 保存预算耗尽即停止，不能在收尾时继续保存其他答复或重开预算。
            if not isinstance(error, BatchPersistenceError):
                finish()
        except BaseException as persistence_error:
            retain_unsaved(persistence_error)
            raise persistence_error from error
        retain_unsaved(error)
        raise
    summary = summarize(store, keys, "round2", already, network.http_attempts_started, blocked)
    logger.info("round2事件去重归组处理结束：%s", summary)
    return summary
