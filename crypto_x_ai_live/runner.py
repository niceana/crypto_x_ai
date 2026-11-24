"""异步实盘入口：成功提交三表，失败整批丢弃；原执行器保留准备/网络/协调分工，不保存失败检查点。"""

import asyncio
import logging
import math
from dataclasses import replace

import pandas as pd

from . import _compat as core
from .config import LiveConfig, load_live_config
from .inputs import frame as _frame, posts as _posts, utc_dates
from .memory import EVENTS, POSTS, MemoryTables, empty_events

logger = logging.getLogger(__name__)


def _system_now():
    return pd.Timestamp.now(tz="UTC")


def _batch_time():
    now = utc_dates([_system_now()], "系统当前时刻")[0]
    return now, now + pd.Timedelta(hours=8)


def _require_complete(frame, column, label):
    incomplete = int((~frame[column].eq("1").fillna(False)).sum())
    if incomplete:
        raise ValueError("{}尚有 {} 条未成功，停止后续阶段".format(label, incomplete))


async def _pipeline(new_posts, history_results, assets, config, method, control, client_factory, scope):
    control.check()
    columns = core.weights.normalize_live_assets(assets)
    target = set(columns)
    now_utc, stamp = _batch_time()
    core.weight_parameters.validate_aggregation_method(method)
    config = load_live_config() if config is None else config
    if not isinstance(config, LiveConfig):
        raise ValueError("config 必须为 LiveConfig")
    # 内存写入失败直接结束，不进入旧持久化模块的同步重试等待。
    config = replace(config, **{name: replace(getattr(config, name),
        run=replace(getattr(config, name).run, storage_max_attempts=1))
        for name in ("round1_ai", "round2_ai", "round3_ai")})
    tasks = _posts(new_posts, now_utc)
    control.check()
    if history_results is None or (isinstance(history_results, pd.DataFrame) and history_results.shape == (0, 0)):
        history_results = empty_events()
    history = _frame(history_results, "record_id", now_utc)
    core.validation.validate_event_table_structure(history)
    history = history.loc[history.index.get_level_values("date") >= now_utc - pd.Timedelta(days=100)]
    memory = MemoryTables(tasks, history, control=control)
    control.check()
    scope[0] = "round1"
    def network_factory(ai):
        return core.network.NetworkWorker(ai, client_factory=client_factory)

    core.extraction.run_extraction(memory, POSTS, "", ai_config=config.round1_ai,
        user_name_to_tickers=config.user_name_to_tickers, network_factory=network_factory, control=control)
    _require_complete(memory.tables[POSTS], "status", "本批第一轮")
    control.check()
    incoming = core.event_records.expand_first_round(memory.tables[POSTS])
    current_keys = memory.merge_events(incoming)
    scope[0] = "history"
    context = memory.tables[EVENTS]
    previous = context.loc[~context.index.isin(current_keys) & context["asset"].isin(target)]
    _require_complete(previous, "round2_status", "历史第二轮")
    _require_complete(previous, "round3_status", "历史第三轮")
    control.check()
    scope[0] = "vectors"
    vector_frame = context.loc[context["asset"].isin(target)]
    if not vector_frame.empty:
        engine = core.vectors.EmbeddingEncoder(config.embedding,
            client_factory=client_factory, control=control)
        protected = (~vector_frame.index.isin(current_keys) | vector_frame["round2_status"].ne("0")
                     | vector_frame["round3_status"].ne("0") | vector_frame["event_id"].ne("")
                     | vector_frame["event_relation"].ne(""))
        for position, (key, row) in enumerate(vector_frame.loc[protected].iterrows()):
            if position % 64 == 0:
                control.check()
            try:
                engine.load_prepared(((key, row["event"], row["embedding"]),))
            except Exception as exc:
                raise ValueError("历史或已处理事件的向量缺失、损坏或版本/维度不匹配：{}".format(key)) from exc
        pending = vector_frame.loc[~protected]

        def save_vectors(values):
            if any(key not in pending.index for key in values):
                raise ValueError("不允许覆盖历史或已处理记录的向量")
            index = pd.MultiIndex.from_tuples(list(values), names=["date", "record_id"])
            patch = pd.DataFrame({"embedding": list(values.values())}, index=index, dtype=object).sort_index()
            memory.update_by_column(EVENTS, patch)

        await engine.prepare(((key, row["event"], row["embedding"])
                                    for key, row in pending.iterrows()), save_vectors)
    current = memory.tables[EVENTS].loc[current_keys]
    selected = current.index[current["asset"].isin(target)]
    if len(selected):
        dates = selected.get_level_values("date")
        settings = core.config.EventConfig(POSTS, EVENTS, "memory://round2", "memory://round3",
            start_time=dates.min().date().isoformat(), end_time=dates.max().date().isoformat(),
            embedding_config_path="memory://embedding", history_lookback_months=3,
            round3_skip_incomplete_round2=False)
        scope[0] = "round2"
        core.relation.run_relation(memory, settings, ai_config=config.round2_ai,
            embedding_config=config.embedding, target_assets=target, network_factory=network_factory, control=control)
        _require_complete(memory.tables[EVENTS].loc[selected], "round2_status", "本批第二轮")
        scope[0] = "round3"
        core.impact.run_impact(memory, settings, ai_config=config.round3_ai, target_assets=target,
            repeat_lookback_months=3, network_factory=network_factory, control=control)
        _require_complete(memory.tables[EVENTS].loc[selected], "round3_status", "本批第三轮")
    scope[0] = "weight"
    control.check()
    weight = core.weights.build_live_weight(memory.tables[EVENTS], as_of=stamp, assets=list(columns.values()),
                                           aggregation_method=method, parameters=config.weight_parameters)
    control.check()
    result = {"status": "success", "round1_results": memory.tables[POSTS].copy(deep=True),
              "event_results": memory.event_changes(), "weight": weight}
    # 表复制可能耗时；最终检查截止时间后才发布成功并填写输出行时间。
    control.check()
    _, written_at = _batch_time()
    weight.index = pd.DatetimeIndex([written_at], name="date")
    logger.info("实盘异步批次成功：原帖=%d，事件增量=%d", len(tasks), len(result["event_results"]))
    return result


async def run_live_batch(new_posts, history_results, *, assets, config=None,
                         aggregation_method="max_abs", timeout_seconds=840, client_factory=None):
    """await 一轮处理；成功返回 status+三表，失败/超时只返回 status。

    timeout_seconds 是整轮计算预算，默认 14 分钟，取消收尾可能额外耗时。
    宿主需给资源清理和 Redis 写回预留时间；同进程取消不能强制中断底层卡死。
    不接受 resume_state；失败后下一轮合并原帖从头重算。client_factory(config)
    是异步 HTTP 客户端注入入口。宿主主动取消时，收尾后传播 CancelledError。
    """
    scope = ["inputs"]
    control = None
    try:
        if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                or not math.isfinite(timeout_seconds) or timeout_seconds <= 0):
            raise ValueError("timeout_seconds 必须为有限的正数")
        control = core.control.ExecutionControl(timeout_seconds)
        logger.info("实盘批次开始：原执行器并发分工，计算预算=%.3f秒", timeout_seconds)
        return await core.execution.run_controlled(lambda: asyncio.run(_pipeline(new_posts, history_results, assets,
            config, aggregation_method, control, client_factory, scope)), control)
    except core.control.ExecutionStopped:
        status = "timeout" if control is not None and control.timed_out else "failed"
        logger.warning("实盘批次停止，所有执行线程已退出：阶段=%s，状态=%s", scope[0], status)
        return {"status": status}
    except asyncio.CancelledError:
        logger.info("实盘批次被宿主取消，执行线程及连接已退出：阶段=%s", scope[0])
        raise
    except Exception:
        logger.exception("实盘批次失败，丢弃本轮结果：阶段=%s", scope[0])
        return {"status": "failed"}
