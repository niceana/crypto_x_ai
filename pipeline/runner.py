"""四个独立进程按月交接；业务结果以数据库为准，清单只记录运行进度。"""

import hashlib
import json
import logging
import multiprocessing
import os
import queue
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from common.config import ai_config_hash
from common.execution import month_ranges
from common.network import NetworkWorker
from common.store import validate_stage_config_hash
from initialization.event_records import expand_first_round, import_event_records_from_posts
from round1.runner import run_extraction
from vectorization.encoder import EmbeddingEncoder
from vectorization.storage import StoredVectors
from round2.runner import run_relation
from vectorization.preparation import prepare_vectors, read_target_assets
from round3.runner import run_impact


STAGES = ("round1", "embedding", "round2", "round3")
logger = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    pass


@dataclass(frozen=True)
class Month:
    start_time: str
    end_time: str
    empty: bool = False

    @property
    def key(self):
        return self.start_time[:7]


@dataclass(frozen=True)
class PipelineConfig:
    post_settings: dict
    event_settings: object
    ai_configs: tuple
    embedding_config: object
    target_assets: object = None


def _get_client():
    from xdata.client import get_client
    return get_client()


def monthly_ranges(config):
    start, end = (config.post_settings.get(name) for name in ("start_time", "end_time"))
    if not isinstance(start, str) or not isinstance(end, str):
        raise ValueError("月流水线必须在帖子配置填写 start_time/end_time")
    begin, finish = date.fromisoformat(start), date.fromisoformat(end)
    if begin.isoformat() != start or finish.isoformat() != end or begin > finish:
        raise ValueError("月流水线日期必须为合法、顺序正确的 YYYY-MM-DD")
    if config.event_settings.retriever or not config.event_settings.embedding_config_path:
        raise ValueError("月流水线要求配置默认 Embedding 检索器")
    if config.post_settings["task_path"] == config.event_settings.result_path:
        raise ValueError("第一轮表与结果表路径必须不同")
    return tuple(Month(left, right) for left, right in month_ranges(begin, finish))


def _require_complete(summary, stage):
    if stage == "round1":
        completed = summary.already_succeeded + summary.new_succeeded
        unfinished = summary.new_failed + summary.pending + summary.processing
    else:
        completed = summary.already_completed + summary.new_completed
        unfinished = summary.failed + summary.pending + summary.processing + summary.skipped_round2
    if unfinished or completed != summary.total:
        raise PipelineError("{} 本月未完全成功，禁止交接：{}".format(stage, summary))


def run_month(stage, month, config, cli, network_factory=NetworkWorker, stop_event=None):
    """执行一个月份；第一轮普通失败也阻止导入、向量准备和第二轮。"""
    settings = replace(config.event_settings, start_time=month.start_time, end_time=month.end_time,
                       first_round_task_path=config.post_settings["task_path"],
                       round3_skip_incomplete_round2=False)
    if stage != "round1" and month.empty:
        return month, {"empty": True}
    if stage == "round1":
        new, previous = [], []

        def collect(new_posts, previous_posts):
            if not new_posts.empty:
                new.append(new_posts)
            if not previous_posts.empty:
                previous.append(previous_posts)

        summary = run_extraction(
            cli, settings.first_round_task_path, config.post_settings["ai_config_path"],
            month.start_time, month.end_time, on_successful_posts=collect,
            user_name_to_tickers=settings.user_name_to_tickers, ai_config=config.ai_configs[0],
            network_factory=network_factory)
        _require_complete(summary, stage)
        new_posts = pd.concat(new).sort_index() if new else None
        previous_posts = pd.concat(previous).sort_index() if previous else None
        sources = [frame for frame in (new_posts, previous_posts) if frame is not None]
        event_count = len(expand_first_round(pd.concat(sources))) if sources else 0
        imported = import_event_records_from_posts(cli, settings, new_posts, previous_posts)
        return replace(month, empty=event_count == 0), dict(asdict(summary), event_count=event_count, imported=imported)
    if stage == "embedding":
        encoder = EmbeddingEncoder(config.embedding_config, stop_event=stop_event)
        summary = prepare_vectors(cli, settings, encoder=encoder, target_assets=config.target_assets,
                                  run_config=config.ai_configs[1].run)
    elif stage == "round2":
        summary = run_relation(cli, settings, ai_config=config.ai_configs[1], network_factory=network_factory,
                               embedding_config=config.embedding_config,
                               target_assets=config.target_assets)
        _require_complete(summary, stage)
    elif stage == "round3":
        summary = run_impact(cli, settings, ai_config=config.ai_configs[2], network_factory=network_factory,
                             target_assets=config.target_assets)
        _require_complete(summary, stage)
    else:
        raise ValueError("未知流水线阶段")
    return month, asdict(summary)


def _put(channel, value, stop):
    while not stop.is_set():
        try:
            channel.put(value, timeout=0.2)
            return True
        except queue.Full:
            pass
    return False


def _worker(stage, config, months, incoming, outgoing, events, stop,
            client_factory=_get_client, operation=run_month):
    """每个工作进程独占客户端，网络停止时沿用各轮的结果保存及收尾逻辑。"""
    logging.basicConfig(**(config.event_settings.logging or {"level": "INFO",
                        "format": "%(asctime)s | %(processName)s | %(name)s | %(message)s"}))
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    networks, mutex, finished = [], threading.Lock(), threading.Event()

    def network_factory(ai_config):
        network = NetworkWorker(ai_config)
        with mutex:
            networks.append(network)
        if stop.is_set():
            network.request_stop("月流水线停止")
        return network

    def watch_stop():
        while not finished.wait(0.1):
            if stop.is_set():
                with mutex:
                    for network in networks:
                        network.request_stop("其他阶段失败或用户中断，停止月流水线")
                return

    watcher = threading.Thread(target=watch_stop, name="pipeline-stop", daemon=True)
    watcher.start()
    month = None
    checked = False
    try:
        cli = client_factory()
        iterator = iter(months)
        while not stop.is_set():
            if incoming is None:
                month = next(iterator, None)
            else:
                try:
                    month = incoming.get(timeout=0.2)
                except queue.Empty:
                    continue
            if month is None:
                if outgoing is not None:
                    _put(outgoing, None, stop)
                events.put({"kind": "done", "stage": stage})
                return
            events.put({"kind": "running", "stage": stage, "month": month.key})
            if operation is run_month and not checked and (stage == "round1" or not month.empty):
                if stage in ("round1", "round2", "round3"):
                    index = {"round1": 0, "round2": 1, "round3": 2}[stage]
                    path = config.post_settings["task_path"] if stage == "round1" else config.event_settings.result_path
                    column = "config_hash" if stage == "round1" else stage + "_config_hash"
                    validate_stage_config_hash(config.ai_configs[index], cli, path, column, stage)
                checked = True
            result, summary = operation(stage, month, config, cli, network_factory, stop)
            events.put({"kind": "completed", "stage": stage, "month": month.key, "summary": summary})
            if outgoing is not None and not _put(outgoing, result, stop):
                return
    except BaseException as error:
        logger.exception("月流水线工作进程停止：stage=%s，month=%s", stage, month)
        events.put({"kind": "failed", "stage": stage, "month": month.key if month else None,
                    "error": "{}: {}".format(type(error).__name__, str(error))})
        stop.set()
    finally:
        finished.set()
        watcher.join()


def _identity(config, months):
    embedding = StoredVectors(config.embedding_config)
    identity = {"version": 1, "task_path": config.post_settings["task_path"],
                "result_path": config.event_settings.result_path,
                "months": [[month.start_time, month.end_time] for month in months],
                "ai_hashes": [ai_config_hash(ai) for ai in config.ai_configs],
                "ai_endpoint_hashes": [hashlib.sha256(ai.api.url.encode("utf-8")).hexdigest()
                                       for ai in config.ai_configs],
                "embedding_hash": embedding.config_hash,
                "similarity_threshold": config.embedding_config.similarity_threshold,
                "history_lookback_months": config.event_settings.history_lookback_months,
                "account_tickers": config.event_settings.user_name_to_tickers,
                "target_assets": sorted(config.target_assets) if config.target_assets is not None else None}
    encoded = json.dumps(identity, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return identity, hashlib.sha256(encoded).hexdigest()


def _timestamp():
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


def _write_manifest(path, document):
    document["updated_at"] = _timestamp()
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


@contextmanager
def _run_lock(path):
    """OS 文件锁在进程退出后自动释放，崩溃不会留下需要手工清理的锁。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise PipelineError("已有月流水线正在运行") from error
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def run_pipeline(config, report_dir, *, resume=False, client_factory=_get_client,
                 worker_target=_worker, shutdown_seconds=30, lock_path=None):
    """启动四个常驻进程；续跑重新按月核验数据库，清单不用于跳过业务校验。"""
    months = monthly_ranges(config)
    report_dir = Path(report_dir).resolve()
    lock = Path(lock_path) if lock_path else report_dir.parent / ".monthly-pipeline.lock"
    with _run_lock(lock):
        if config.target_assets is None and config.event_settings.round2_asset_list_path:
            config = replace(config, target_assets=read_target_assets(client_factory(), config.event_settings))
        identity, digest = _identity(config, months)
        path = report_dir / "manifest.json"
        attempt = 1
        if resume:
            previous = json.loads(path.read_text(encoding="utf-8"))
            if previous.get("identity_hash") != digest:
                raise PipelineError("续跑配置、日期或资产范围与原流水线不一致")
            attempt = previous.get("attempt", 1) + 1
        else:
            report_dir.mkdir(parents=True, exist_ok=False)
        document = {"status": "running", "attempt": attempt, "identity": identity, "identity_hash": digest,
                    "started_at": _timestamp(), "months": {
                        month.key: {"start_time": month.start_time, "end_time": month.end_time,
                                    "stages": {stage: {"status": "pending"} for stage in STAGES}}
                        for month in months}}
        _write_manifest(path, document)
        context = multiprocessing.get_context("spawn")
        stop, events = context.Event(), context.Queue()
        channels = [context.Queue(maxsize=2) for _ in range(3)]
        processes = [context.Process(
            target=worker_target, name="pipeline-" + stage,
            args=(stage, config, months, channels[index - 1] if index else None,
                  channels[index] if index < 3 else None, events, stop, client_factory))
            for index, stage in enumerate(STAGES)]
        started, done, failure = [], set(), None
        try:
            for process in processes:
                process.start()
                started.append(process)
            while len(done) < len(STAGES):
                try:
                    event = events.get(timeout=0.2)
                except queue.Empty:
                    crashed = [p for p, stage in zip(started, STAGES)
                               if p.exitcode is not None and stage not in done]
                    if crashed:
                        raise PipelineError("工作进程异常退出：{}".format([p.name for p in crashed]))
                    continue
                stage, kind = event["stage"], event["kind"]
                if kind == "done":
                    done.add(stage)
                    continue
                month = event.get("month")
                if month is not None:
                    document["months"][month]["stages"][stage] = dict(event, status=kind)
                _write_manifest(path, document)
                logger.info("月流水线：月份=%s，阶段=%s，状态=%s", month, stage, kind)
                if kind == "failed":
                    raise PipelineError(event["error"])
            if any(data["status"] != "completed" for item in document["months"].values()
                   for data in item["stages"].values()):
                raise PipelineError("工作进程已退出，但仍有未完成月份")
            document["status"] = "completed"
        except BaseException as error:
            failure = error
            stop.set()
            document["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "failed"
            document["error"] = "{}: {}".format(type(error).__name__, str(error))
        finally:
            deadline = time.monotonic() + shutdown_seconds
            for process in started:
                process.join(timeout=max(0, deadline - time.monotonic()))
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                    if failure is None:
                        failure = PipelineError("工作进程未正常退出：{}".format(process.name))
                        document["status"] = "failed"
                elif process.exitcode != 0 and failure is None:
                    failure = PipelineError("工作进程退出码异常：{} ({})".format(process.name, process.exitcode))
                    document["status"] = "failed"
            for item in document["months"].values():
                for data in item["stages"].values():
                    if data["status"] == "running":
                        data["status"] = "interrupted"
            if failure is not None and "error" not in document:
                document["error"] = "{}: {}".format(type(failure).__name__, str(failure))
            _write_manifest(path, document)
            for channel in channels + [events]:
                channel.cancel_join_thread()
                channel.close()
        if failure is not None:
            raise failure
        return document
