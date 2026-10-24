"""round2事件去重归组业务与调度：同代币按时间推进，相同时间使用同一份历史。"""

import json
import logging
import queue
import time
from collections import defaultdict, deque
from datetime import date, timedelta

from common.config import load_ai_config, load_embedding_config, require_prompts
from common.event_context import (
    EventHistory,
    build_current_record_input,
)
from common.network import BatchRunError, NetworkWorker
from common.persistence import BatchPersistenceError
from common.store import EventRecordStore, read_event_history, validate_relation_history, summarize
from common.time_rules import history_read_bounds, is_history_needed, published_time, table_date_bounds
from round2.retrieval import load_retriever
from round2.embedding import EmbeddingRetriever
from round2.preparation import AutoNewResult, RelationPreparer

logger = logging.getLogger(__name__)


def _load_relation_history(store, pending_keys):
    """按日期补入必要前史，范围之后仅读取补录检查所需的两列。"""
    settings = store.settings
    bounds = history_read_bounds((key[0] for key in pending_keys), settings.start_time,
                                 settings.history_lookback_months)
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
    logger.info("round2事件去重归组启动准备：起点=%s，终点=%s", settings.start_time, settings.end_time)
    target_assets = None
    if settings.round2_asset_list_path:
        universe = json.loads(cli.read_object(settings.round2_asset_list_path).decode("utf-8"))["InstrumentList"]
        target_assets = {instrument.split(".", 1)[0].strip().upper() for instrument in universe}
    logger.info("round2事件去重归组开始读取本范围任务")
    store = EventRecordStore(cli, settings, ai_config.run if ai_config else None,
                             settings.start_time, settings.end_time)
    keys = store.keys_in_scope()
    if target_assets is not None:
        total = len(keys)
        keys = [key for key in keys if store.df.at[key, "asset"] in target_assets]
        logger.info("round2事件去重归组资产筛选完成：范围记录=%d，目标记录=%d，跳过记录=%d，命中资产=%d",
                    total, len(keys), total - len(keys), len({store.df.at[key, "asset"] for key in keys}))
    store.recover("round2", keys)
    if store.count_completed("round2", keys) == len(keys):
        logger.info("round2事件去重归组无需处理：范围记录=%d，已有完成=%d", len(keys), len(keys))
        return summarize(store, keys, "round2", len(keys))
    pending_keys = store.pending_keys("round2", keys)
    if not pending_keys:
        return summarize(store, keys, "round2", store.count_completed("round2", keys))
    logger.info("round2事件去重归组开始读取和校验历史：待处理记录=%d", len(pending_keys))
    later_records = _load_relation_history(store, pending_keys)
    validate_relation_history(store, keys, settings.history_lookback_months, later_records)
    del later_records
    logger.info("round2事件去重归组历史校验完成，开始准备检索与 AI 配置")
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
    grouped = defaultdict(lambda: defaultdict(list))
    for key in pending_keys:
        grouped[store.df.loc[key, "asset"]][published_time(key[0])].append(key)
    history = EventHistory(store.df.loc[store.df["asset"].isin(grouped)],
                           settings.history_lookback_months, truncate_group_history=True)
    if embedding is not None:
        embedding_keys = set(pending_keys)
        times = {asset: sorted(batches) for asset, batches in grouped.items()}
        for key, row in store.df.iterrows():
            if row["round2_status"] != "1" or row["asset"] not in times:
                continue
            if is_history_needed(key[0], times[row["asset"]], settings.history_lookback_months):
                embedding_keys.add(key)
        # 编码固定事实可以提前批处理；实际候选仍由 history 严格按披露时点筛选。
        # 向量按批写入事件去重分析表后才开始认领，避免同步请求阻塞关联结果的保存。
        ordered = sorted(embedding_keys, key=lambda key: (published_time(key[0]), key[1]))
        logger.info("round2事件去重归组开始准备向量：当前及历史记录=%d", len(ordered))
        embedding.prepare(((key, store.df.at[key, "event"], store.df.at[key, "embedding"])
                           for key in ordered), store.save_embeddings)
        logger.info("round2事件去重归组向量准备及保存完成")
    waves = {asset: deque(deque(sorted(batch, key=lambda key: key[1])) for _, batch in sorted(times.items()))
             for asset, times in sorted(grouped.items())}
    logger.info("round2事件去重归组准备运行：范围记录=%d，已有完成=%d，待处理记录=%d，待处理资产=%d，HTTP并发上限=%d，备用AI请求上限=%d；独立补货，已有结果即时合批写库",
                len(keys), already, len(pending_keys), len(waves), config.run.concurrency,
                config.run.ready_request_buffer_size)
    active, confirmed, blocked = {}, defaultdict(list), set()
    incomplete = defaultdict(int)
    ready_assets = deque(waves)
    network = network_factory(config)
    preparer = RelationPreparer(history, retrieve, config, network)
    network.bind_ready_queue(preparer.ready_requests, preparer.wake)
    current_results = []
    succeeded = failed = auto_new = 0
    save_calls = save_records = claim_calls = claim_records = 0
    save_seconds = claim_seconds = 0.0
    started_at = last_progress_at = time.monotonic()
    next_progress = config.run.progress_every

    def report_progress(force=False, phase="处理中"):
        nonlocal last_progress_at, next_progress
        now = time.monotonic()
        processed = succeeded + failed
        if not force and processed < next_progress and now - last_progress_at < 10:
            return
        elapsed = now - started_at
        # 只读内存计数和队列快照，不为日志额外查库或扫描整表。
        with network.completions.mutex:
            waiting_results = list(network.completions.queue)
        waiting_results.extend(current_results)
        waiting_results.extend(network.unpublished)
        waiting_save = len(waiting_results)
        waiting_auto = sum(isinstance(message, AutoNewResult) for message in waiting_results)
        waiting_ai = sum(not isinstance(message, AutoNewResult) and message.response is not None
                         and message.http_attempts_started > 0 for message in waiting_results)
        waiting_assets = {active[message.key] for message in waiting_results if message.key in active}
        waiting_frontiers = sum(asset not in blocked and len(waves[asset]) > 1 for asset in waiting_assets)
        unfinished_assets = sum(bool(batches) and asset not in blocked for asset, batches in waves.items())
        claimable_assets = sum(asset not in blocked and bool(waves[asset]) and bool(waves[asset][0])
                               for asset in set(ready_assets))
        http_in_flight, http_peak, http_average = network.http_snapshot()
        preparation_state, pending_preparation = preparer.snapshot()
        logger.info(
            "round2事件去重归组处理进度：阶段=%s，已处理=%d/%d（%.1f%%），已有完成=%d，"
            "本次成功=%d，失败=%d，自动判新=%d，剩余未处理=%d，"
            "HTTP请求中=%d/%d，准备状态=%s，待准备=%d，就绪请求=%d/%d，已认领未落库=%d，待写库结果=%d，"
            "HTTP累计尝试=%d，暂停币种=%d，耗时=%.1f秒，结果确认速度=%.2f条/秒",
            phase, already + processed, len(keys), (already + processed) * 100.0 / len(keys), already,
            succeeded, failed, auto_new, len(keys) - already - processed,
            http_in_flight, config.run.concurrency, preparation_state, pending_preparation,
            preparer.ready_requests.qsize(), config.run.ready_request_buffer_size,
            len(active), waiting_save, network.http_attempts_started, len(blocked),
            elapsed, processed / elapsed if elapsed > 0 else 0.0)
        logger.info(
            "round2事件去重归组瓶颈统计：HTTP平均并发=%.2f/%d，HTTP峰值=%d，"
            "待写库AI=%d，自动判新=%d，其他=%d，有后续数据且前序待写库币种=%d，"
            "未结束币种=%d，可认领币种=%d，结果保存平均每批耗时=%.3f秒，"
            "结果保存平均批量=%.1f条，结果保存折合每条耗时=%.3f秒，"
            "认领写库平均每批耗时=%.3f秒，认领平均批量=%.1f条，认领写库折合每条耗时=%.3f秒",
            http_average, config.run.concurrency, http_peak, waiting_ai, waiting_auto,
            waiting_save - waiting_ai - waiting_auto, waiting_frontiers, unfinished_assets, claimable_assets,
            save_seconds / save_calls if save_calls else 0.0,
            save_records / save_calls if save_calls else 0.0,
            save_seconds / save_records if save_records else 0.0,
            claim_seconds / claim_calls if claim_calls else 0.0,
            claim_records / claim_calls if claim_calls else 0.0,
            claim_seconds / claim_records if claim_records else 0.0)
        last_progress_at = now
        next_progress = (processed // config.run.progress_every + 1) * config.run.progress_every

    def collect_results(include_unpublished=False):
        # 固定本次快照大小，准备线程持续产出也不会拖延开始写库。
        # 出队后立即留在当前批中，失败或中断时保留整批原消息。
        for _ in range(network.completions.qsize()):
            try:
                current_results.append(network.completions.get_nowait())
            except queue.Empty:
                break
        if include_unpublished:
            current_results.extend(network.unpublished)
            network.unpublished.clear()
        return bool(current_results)

    def consume():
        nonlocal succeeded, failed, auto_new, save_calls, save_records, save_seconds
        items, stopped, seen = [], [], set()
        for message in current_results:
            if message.key not in active or message.key in seen:
                raise BatchRunError("收到未提交或重复的第二轮结果")
            seen.add(message.key)
            automatic = isinstance(message, AutoNewResult)
            if not automatic and message.error is not None:
                raise BatchRunError("第二轮网络作业发生未预期异常") from message.error
            if not automatic and message.response is None:
                stopped.append(message)
            elif automatic:
                items.append((message.key, {"event_relation": "new", "matched_event_id": None,
                                            "new_information": None}, None, 0))
            else:
                items.append((message.key, message.value, message.response, message.http_attempts_started))
        for message in stopped:
            incomplete[active.pop(message.key)] -= 1
        stopped_keys = {message.key for message in stopped}
        current_results[:] = [message for message in current_results if message.key not in stopped_keys]
        if not items:
            return
        save_started = time.monotonic()
        try:
            store.save_relation_batch(items)
        finally:
            save_seconds += time.monotonic() - save_started
            save_calls += 1
            save_records += len(items)
        saved = current_results[:]
        for message in saved:
            asset = active.pop(message.key)
            incomplete[asset] -= 1
            automatic = isinstance(message, AutoNewResult)
            if automatic:
                auto_new += 1
            if automatic or message.response.success:
                confirmed[asset].append(message.key)
                succeeded += 1
            else:
                failed += 1
                blocked.add(asset)
        current_results.clear()
        for message in saved:
            if isinstance(message, AutoNewResult):
                logger.info("round2事件去重归组自动判新：record_id=%s，decision_source=auto_new，规则及原因=%s", message.key[1], message.reason)
            elif not message.response.success:
                logger.warning("round2事件去重归组最终失败，暂停同代币后续关联：asset=%s，record_id=%s",
                               store.df.at[message.key, "asset"], message.key[1])

    def finish():
        if collect_results(include_unpublished=True):
            consume()

    def retain_unsaved(error):
        error.unsaved_results = current_results
        error.completed_results = network.completions
        error.unpublished_results = network.unpublished

    def advance():
        # 相同时间允许共同并发，全部落库后才加入历史，避免用同刻答复互相匹配。
        for asset in list(waves):
            if asset in blocked:
                continue
            if waves[asset] and not waves[asset][0] and not incomplete[asset]:
                saved = confirmed.pop(asset, [])
                records = [(key, {name: store.df.at[key, name] for name in (
                    "asset", "event", "event_id", "event_relation", "new_information", "round2_status")})
                           for key in saved]
                preparer.add_history(records)
                waves[asset].popleft()
                if waves[asset]:
                    ready_assets.append(asset)

    def admit():
        nonlocal claim_calls, claim_records, claim_seconds
        # 只限制一次认领调用的大小，不限制准备、请求和结果的总量。
        selected = []
        while ready_assets and len(selected) < 200:
            asset = ready_assets.popleft()
            batches = waves[asset]
            if asset in blocked or not batches or not batches[0]:
                continue
            selected.extend(store.select_pending(batches[0], 1, "round2"))
            if batches[0]:
                ready_assets.append(asset)
        if not selected:
            return False
        claim_started = time.monotonic()
        try:
            store.claim(selected, "round2", digest)
        finally:
            claim_seconds += time.monotonic() - claim_started
            claim_calls += 1
            claim_records += len(selected)
        records = [(key, build_current_record_input(key, store.df.loc[key])) for key in selected]
        for key, current in records:
            active[key] = current["asset"]
            incomplete[current["asset"]] += 1
        preparer.submit_batch(records)
        return True

    try:
        try:
            network.start()
            preparer.start()
            report_progress(force=True)
            while True:
                preparer.check()
                try:
                    network.check()
                except BatchRunError:
                    # 准备线程可能刚刚触发网络停止，保留原始检索/构造异常。
                    preparer.check()
                    raise
                # 已有一条就写一条，已有多条就一次合并；不等待未来结果凑批。
                consumed = collect_results()
                if consumed:
                    consume()
                advance()
                # 一批确认后，共同认领已解锁任务，立即交给准备线程补充候选。
                admitted = admit() if ready_assets else False
                report_progress()
                if admitted:
                    continue
                if not active and not any(batches and asset not in blocked for asset, batches in waves.items()):
                    break
                if consumed:
                    continue
                try:
                    current_results.append(network.completions.get(timeout=0.25))
                except queue.Empty:
                    pass
        finally:
            preparer.request_stop()
            try:
                network.shutdown()
            finally:
                preparer.shutdown()
        preparer.check()
        if network.failure is not None:
            raise BatchRunError("第二轮网络运行或关闭失败") from network.failure
    except BaseException as error:
        try:
            # 保存预算耗尽即停止，不能在收尾时继续保存其他答复或重开预算。
            if not isinstance(error, BatchPersistenceError):
                finish()
        except BaseException as persistence_error:
            report_progress(force=True, phase="异常停止")
            retain_unsaved(persistence_error)
            raise persistence_error from error
        report_progress(force=True, phase="异常停止")
        retain_unsaved(error)
        raise
    report_progress(force=True, phase="处理结束")
    summary = summarize(store, keys, "round2", already, network.http_attempts_started, blocked)
    logger.info("round2事件去重归组处理结束：%s", summary)
    return summary
