"""调用线程按自然月协调任务，独立网络线程处理异步请求与内存重试。"""

import asyncio
import logging
import math
import queue
import random
import threading
import time
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import httpx

from .config import TaskRenderError, load_config, render_payload
from .response import ParsedResponse, parse_response, request_failure
from .tasks import BatchPersistenceError, TaskKey, TaskStore

logger = logging.getLogger(__name__)


class BatchRunError(RuntimeError):
    """整个 worker 停止或发生批次异常，不代表单帖普通失败。"""


@dataclass
class RunSummary:
    """累加各分段标量统计，不持有全年任务表或响应。"""

    total: int = 0
    already_succeeded: int = 0
    new_succeeded: int = 0
    new_failed: int = 0
    pending: int = 0
    processing: int = 0
    http_attempts_started: int = 0
    elapsed_seconds: float = 0

    def add_month(self, store):
        self.total += len(store.df)
        self.already_succeeded += store.already_succeeded
        self.new_succeeded += store.new_succeeded
        self.new_failed += store.new_failed
        self.pending += int(store.df["status"].eq("0").sum())
        self.processing += int(store.df["status"].eq("9").sum())


@dataclass(frozen=True)
class RequestJob:
    """认领确认后的不可变输入快照；原键只用于内部关联。"""

    key: TaskKey
    text: str


@dataclass(frozen=True)
class CompletionMessage:
    """每个作业仅发布一次；停止消息不伪造 AI 最终结果。"""

    key: TaskKey
    response: Optional[ParsedResponse] = None
    http_attempts_started: int = 0
    stopped: bool = False
    error: Optional[BaseException] = None


# 只列出已确认可重试的具体类型，避免基类捕获扩大重试范围。
_RETRY_NETWORK = (httpx.ConnectError, httpx.ReadError, httpx.WriteError,
                  httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout)
_STOP_REQUEST = (httpx.InvalidURL, httpx.UnsupportedProtocol, httpx.LocalProtocolError,
                 httpx.RemoteProtocolError, httpx.ProxyError, httpx.PoolTimeout)


def _exception_reason(exc):
    """保留失败结果的具体异常说明，无消息时沿用异常类名。"""
    return str(exc).strip() or type(exc).__name__


def _month_ranges(start, end):
    """按日历月生成包含起止日的日期字符串分段。"""
    if start is None:
        yield None, None
        return
    cursor = start
    while cursor <= end:
        month_end = date(cursor.year, cursor.month, monthrange(cursor.year, cursor.month)[1])
        segment_end = min(month_end, end)
        yield cursor.isoformat(), segment_end.isoformat()
        if segment_end == end:
            return
        cursor = segment_end + timedelta(days=1)


def _retry_after(value):
    """将秒数或 HTTP 日期换算为非负等待；非法值不影响本地退避。"""
    if not value:
        return None
    try:
        if value.strip().isdigit():
            seconds = float(value.strip())
        else:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            seconds = max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())
        return seconds if math.isfinite(seconds) and seconds >= 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _backoff(run, count):
    """计算指数随机退避，提前截到上限以避免乘方溢出。"""
    upper = run.backoff_base_seconds
    for _ in range(count - 1):
        if upper >= run.backoff_max_seconds / 2:
            return random.uniform(0, run.backoff_max_seconds)
        upper *= 2
    return random.uniform(0, min(upper, run.backoff_max_seconds))


def _create_client(config):
    """建立全运行复用的客户端，由作业层统一控制请求重试次数。"""
    # HTTPX 默认传输重试为 0；trust_env 同时影响环境代理发现。
    return httpx.AsyncClient(
        limits=httpx.Limits(max_connections=config.run.concurrency,
                           max_keepalive_connections=config.run.concurrency),
        timeout=httpx.Timeout(config.run.timeout_seconds),
        follow_redirects=False, trust_env=config.api.trust_env,
    )


class _RequestGate:
    """全部起发共用限速与服务端暂停，等待期间可响应全局停止。"""

    def __init__(self, rps, stopped):
        self.interval = 1.0 / rps if rps else 0.0
        self.next_start = 0.0
        self.pause_until = 0.0
        self.stopped = stopped
        self.lock = asyncio.Lock()

    def pause(self, seconds):
        self.pause_until = max(self.pause_until, time.monotonic() + seconds)
        logger.warning("服务端要求全局暂停：本次等待=%.3f秒；后续请求遵守最晚暂停截止", seconds)

    async def wait_until(self, deadline):
        while True:
            if self.stopped.is_set():
                raise asyncio.CancelledError()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 0.25))

    async def acquire(self):
        # 限速锁只保护起发时间，不覆盖实际 HTTP 请求；空闲不积累额度。
        async with self.lock:
            while True:
                await self.wait_until(max(self.next_start, self.pause_until))
                now = time.monotonic()
                if now >= max(self.next_start, self.pause_until):
                    self.next_start = now + self.interval
                    return


class _NetworkWorker:
    """网络线程独占事件循环和客户端，通过有界队列向调用线程报告。"""

    def __init__(self, config):
        self.config = config
        self.completions = queue.Queue(maxsize=config.run.concurrency)
        self.stopped = threading.Event()
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self._thread_main, name="ai-batch-network")
        self.loop = None
        self.shutdown_event = None
        self.client = None
        self.gate = None
        self.tasks = {}
        self.failure = None
        self.stop_reason = ""
        self.http_attempts_started = 0
        self.unpublished = []

    def request_stop(self, reason="worker 停止"):
        """立即阻止新起发和重试，正常关闭也使用同一停止信号。"""
        if not self.stopped.is_set():
            self.stop_reason = reason
        self.stopped.set()

    def start(self):
        """等待客户端就绪，同时检查线程是否提前退出。"""
        self.thread.start()
        while not self.ready.wait(0.1):
            if not self.thread.is_alive():
                raise BatchRunError("网络线程在就绪前退出") from self.failure
        self.check()
        logger.info("网络线程已就绪：并发上限=%d", self.config.run.concurrency)

    def check(self):
        if self.failure is not None:
            raise BatchRunError("网络线程或作业发生未预期异常") from self.failure
        if self.stopped.is_set():
            raise BatchRunError(self.stop_reason or "worker 已停止")
        if not self.thread.is_alive():
            raise BatchRunError("网络线程意外退出")

    def submit(self, job):
        # 调用线程不直接操作 asyncio.Task；实际创建发生在网络循环。
        self.loop.call_soon_threadsafe(self._launch, job)

    def _launch(self, job):
        if self.stopped.is_set():
            self._publish(CompletionMessage(job.key, stopped=True))
            return
        task = self.loop.create_task(self._execute(job))
        self.tasks[task] = job
        task.add_done_callback(self._completed)

    def _publish(self, message):
        """发布一次完成消息；队列异常满时保留结果并停止，不能丢弃。"""
        try:
            self.completions.put_nowait(message)
        except queue.Full:
            self.unpublished.append(message)
            self.failure = RuntimeError("完成队列容量不变量被破坏，结果已保留")
            logger.error("完成队列容量不变量被破坏：任务=%r，未发布结果已保留", message.key)
            self.request_stop()

    def _completed(self, task):
        # 协程开始前就被取消也必须发布停止消息，以释放 submitted 名额。
        job = self.tasks.pop(task)
        if task.cancelled():
            message = CompletionMessage(job.key, stopped=True)
        else:
            try:
                message = task.result()
            except BaseException as exc:
                logger.error("网络作业发生未预期异常：任务=%r，异常类型=%s", job.key, type(exc).__name__)
                self.failure = exc
                self.request_stop()
                message = CompletionMessage(job.key, error=exc)
        self._publish(message)

    async def _execute(self, job):
        """一个作业内完成全部尝试；中间失败不发布消息或写入终态。"""
        count = 0
        try:
            try:
                payload = render_payload(self.config, job.text)
            except TaskRenderError as exc:
                logger.warning("单帖请求构造失败：任务=%r，异常类型=%s；不发送 HTTP", job.key, type(exc).__name__)
                return CompletionMessage(job.key, request_failure(_exception_reason(exc)))
            while True:
                await self.gate.acquire()
                if self.stopped.is_set():
                    raise asyncio.CancelledError()
                # 网络故障与 5xx 共用实际起发计数，不按错误类型拆分预算。
                count += 1
                self.http_attempts_started += 1
                logger.debug("开始 HTTP 请求：任务=%r，尝试=%d/%d", job.key, count, self.config.run.max_attempts)
                retry = False
                try:
                    response = await self.client.post(self.config.api.url, headers=self.config.api.headers, json=payload)
                except _RETRY_NETWORK as exc:
                    parsed = request_failure(_exception_reason(exc))
                    retry = count < self.config.run.max_attempts
                    logger.warning("网络传输故障：任务=%r，尝试=%d/%d，异常类型=%s，允许继续尝试=%s",
                                   job.key, count, self.config.run.max_attempts, type(exc).__name__, "是" if retry else "否")
                except _STOP_REQUEST as exc:
                    parsed = request_failure(_exception_reason(exc))
                    logger.error("请求异常触发整个 worker 停止：任务=%r，异常类型=%s；需要人工排查",
                                 job.key, type(exc).__name__)
                    self.request_stop(parsed.error_message)
                except httpx.RequestError as exc:
                    logger.warning("单帖请求异常：任务=%r，异常类型=%s；不自动重试", job.key, type(exc).__name__)
                    parsed = request_failure(_exception_reason(exc))
                else:
                    status = response.status_code
                    is_server_failure = 500 <= status <= 599
                    must_stop = (not 200 <= status < 300 and not is_server_failure) or (
                        is_server_failure and count >= self.config.run.max_attempts
                    )
                    if must_stop:
                        self.request_stop("HTTP {}：需要人工排查后重启".format(status))
                        logger.error("HTTP 失败触发整个 worker 停止：任务=%r，HTTP状态=%d，尝试=%d/%d；需要人工排查",
                                     job.key, status, count, self.config.run.max_attempts)
                    parsed = parse_response(status, response.text)
                    retry = is_server_failure and count < self.config.run.max_attempts
                    if retry and not self.stopped.is_set():
                        logger.warning("接口返回 5xx，准备重试：任务=%r，HTTP状态=%d，尝试=%d/%d",
                                       job.key, status, count, self.config.run.max_attempts)
                        waiting = _retry_after(response.headers.get("Retry-After"))
                        if waiting is not None:
                            self.gate.pause(waiting)
                    # 退避可能较长，等待前释放完整响应，仅保留必要解析字段。
                    del response
                if parsed.success or not retry:
                    if parsed.success:
                        logger.debug("单帖请求成功：任务=%r，实际尝试=%d", job.key, count)
                    else:
                        logger.warning("单帖请求最终失败：任务=%r，HTTP状态=%s，实际尝试=%d",
                                       job.key, parsed.http_status, count)
                    return CompletionMessage(job.key, parsed, count)
                if self.stopped.is_set():
                    raise asyncio.CancelledError()
                delay = _backoff(self.config.run, count)
                logger.warning("请求重试退避：任务=%r，下次尝试=%d/%d，本地等待=%.3f秒",
                               job.key, count + 1, self.config.run.max_attempts, delay)
                await self.gate.wait_until(time.monotonic() + delay)
        except asyncio.CancelledError:
            logger.debug("作业因整个 worker 停止而结束：任务=%r，已起发=%d；无最终响应时保留状态 9", job.key, count)
            return CompletionMessage(job.key, http_attempts_started=count, stopped=True)
        except Exception as exc:
            self.failure = exc
            self.request_stop()
            logger.error("网络作业发生批次异常：任务=%r，异常类型=%s", job.key, type(exc).__name__)
            return CompletionMessage(job.key, http_attempts_started=count, error=exc)

    async def _serve(self):
        """创建共享客户端，并在停止时等待实际请求 Task 全部收尾。"""
        self.shutdown_event = asyncio.Event()
        async with _create_client(self.config) as client:
            self.client = client
            self.gate = _RequestGate(self.config.run.requests_per_second, self.stopped)
            self.ready.set()
            try:
                await self.shutdown_event.wait()
            finally:
                # 本地取消只用于资源清理，不能保证服务端撤销已收到的请求。
                tasks = list(self.tasks)
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

    def _thread_main(self):
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._serve())
        except BaseException as exc:
            logger.error("网络线程运行或关闭失败：异常类型=%s", type(exc).__name__)
            self.failure = exc
            self.request_stop()
        finally:
            self.ready.set()
            if self.loop is not None:
                try:
                    self.loop.run_until_complete(self.loop.shutdown_asyncgens())
                except BaseException as exc:
                    logger.error("异步生成器关闭失败：异常类型=%s", type(exc).__name__)
                    self.failure = exc
                    self.request_stop()
                finally:
                    self.loop.close()
            logger.debug("网络线程清理结束")

    def shutdown(self):
        """停止真实请求并等待线程结束，避免上次作业遗留到下次启动。"""
        self.request_stop()
        if self.loop is not None and self.loop.is_running() and self.shutdown_event is not None:
            try:
                self.loop.call_soon_threadsafe(self.shutdown_event.set)
            except RuntimeError:
                pass  # 网络线程可能已先行关闭事件循环。
        if self.thread.ident is not None:
            while self.thread.is_alive():
                self.thread.join(0.25)


class _MonthCoordinator:
    """调用线程协调当前月的认领、完成消息和有容量上限的结果缓冲。"""

    def __init__(self, store, network, confirmed_before=0):
        self.store = store
        self.network = network
        # submitted 在收取消息后释放；active_keys 要等终态保存确认才释放。
        self.submitted = set()
        self.buffer = []
        self.first_buffer_at = None
        self.confirmed_before = confirmed_before
        self.next_progress = (confirmed_before // store.config.run.progress_every + 1) * store.config.run.progress_every

    def flush(self):
        """确认整批最终更新后才清空缓冲，保存失败时保留原结果。"""
        if not self.buffer:
            return
        logger.debug("开始提交最终结果缓冲：结果数=%d", len(self.buffer))
        self.store.save_results(self.buffer)
        self.buffer.clear()
        self.first_buffer_at = None
        confirmed = self.confirmed_before + self.store.new_succeeded + self.store.new_failed
        if confirmed >= self.next_progress:
            logger.info("已确认最终结果 %d 条", confirmed)
            step = self.store.config.run.progress_every
            self.next_progress = (confirmed // step + 1) * step

    def flush_due(self):
        run = self.store.config.run
        return bool(self.buffer) and (
            len(self.buffer) >= run.batch_size
            or len(self.buffer) >= run.result_buffer_size
            or time.monotonic() - self.first_buffer_at >= run.flush_interval_seconds
            or (not self.submitted and not self.store.candidates)
        )

    def consume(self, message):
        """按原任务键关联响应；停止消息只释放网络名额，不产生终态。"""
        if message.key not in self.submitted:
            raise BatchRunError("收到未提交或重复的作业完成消息")
        self.submitted.remove(message.key)
        if message.response is not None:
            if len(self.buffer) >= self.store.config.run.result_buffer_size:
                raise BatchRunError("结果缓冲容量不变量被破坏")
            if not self.buffer:
                self.first_buffer_at = time.monotonic()
            self.buffer.append((message.key, message.response))

    def run(self):
        """每轮先保存、逐条收取并检查容量，最后才补充新作业。"""
        run = self.store.config.run
        while True:
            self.network.check()
            if self.flush_due():
                self.flush()
            while True:
                # 缓冲满时先确认写回，不能先取完整队列再检查阈值。
                if len(self.buffer) >= run.result_buffer_size:
                    self.flush()
                try:
                    message = self.network.completions.get_nowait()
                except queue.Empty:
                    break
                self.consume(message)
                if self.flush_due():
                    self.flush()
                self.network.check()
            self.network.check()
            if not self.submitted and not self.store.candidates:
                self.flush()
                if self.store.active_keys or self.store.pending_patch is not None:
                    raise BatchRunError("月份切换前存在未确认任务")
                return
            self.network.check()
            keys = self.store.select(run.concurrency - len(self.submitted))
            if keys:
                self.network.check()
                jobs = self.store.claim(keys)
                for key, text in jobs:
                    # 同步认领期间可能收到停止；确认 9 后仍要再次检查。
                    self.network.check()
                    self.submitted.add(key)
                    self.network.submit(RequestJob(key, text))
                continue
            timeout = 0.25
            if self.buffer:
                timeout = min(timeout, max(0.0, run.flush_interval_seconds - (time.monotonic() - self.first_buffer_at)))
            try:
                message = self.network.completions.get(timeout=timeout)
            except queue.Empty:
                continue
            self.consume(message)
            if self.flush_due():
                self.flush()

    def finish(self):
        """网络线程停止后按同一容量规则收取并保存已发布的最终结果。"""
        while not self.network.completions.empty():
            if len(self.buffer) >= self.store.config.run.result_buffer_size:
                self.flush()
            self.consume(self.network.completions.get_nowait())
            if self.flush_due():
                self.flush()
        for message in self.network.unpublished:
            if len(self.buffer) >= self.store.config.run.result_buffer_size:
                self.flush()
            self.consume(message)
            if self.flush_due():
                self.flush()
        self.network.unpublished.clear()
        self.flush()


def _retain_unsaved(error, store, coordinator, network):
    """仅在异常对象中保留进程内未确认数据，不伪造持久化成功。"""
    if store is not None and store.pending_patch is not None:
        error.pending_patch = store.pending_patch.copy(deep=True)
    if coordinator is not None:
        error.unsaved_results = coordinator.buffer
    if network is not None:
        error.completed_results = network.completions
        error.unpublished_results = network.unpublished


def run_worker(cli, task_path, config_path, start_time=None, end_time=None) -> RunSummary:
    """同步处理指定日期范围；当前月结果全部确认后才读取下一月。

    cli 直接读取 task_path 并按原索引更新指定列，始终在调用入口线程执行。
    start_time/end_time 同时设置时按自然月处理左闭右闭的日期范围；均为空时读全表。
    正常返回跨月份 RunSummary；全局停止抛 BatchRunError，保存未确认抛
    BatchPersistenceError，用户中断继续传播 KeyboardInterrupt。
    """
    started = time.monotonic()
    logger.info("开始启动批量处理程序")
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
        config = load_config(config_path)
    except Exception:
        logger.exception("worker 启动校验失败；尚未读取或更新任务")
        raise
    logger.info("worker 启动校验完成：起点=%s，终点=%s，并发上限=%d，结果批次=%d，单帖尝试上限=%d",
                start, end, config.run.concurrency, config.run.batch_size, config.run.max_attempts)
    summary = RunSummary()
    network = None
    store = None
    coordinator = None
    try:
        for segment_start, segment_end in _month_ranges(start, end):
            # 切换月份前检查全局停止，未进入月份不得读取或恢复。
            if network is not None:
                network.check()
            logger.info("开始处理当前分段：起点=%s，终点=%s", segment_start, segment_end)
            store = TaskStore(config, cli, task_path, segment_start, segment_end)
            store.recover()
            if store.candidates:
                if network is None:
                    network = _NetworkWorker(config)
                    network.start()
                coordinator = _MonthCoordinator(store, network, summary.new_succeeded + summary.new_failed)
                coordinator.run()
            else:
                logger.info("当前分段无待处理任务：%s → %s", segment_start, segment_end)
            summary.add_month(store)
            if store.new_succeeded or store.new_failed:
                logger.info("当前分段 %s → %s：成功 %d，失败 %d",
                            segment_start, segment_end, store.new_succeeded, store.new_failed)
            # 只保留汇总计数，释放当前月的表、候选键和结果引用。
            coordinator = None
            store = None
        if network is not None:
            network.shutdown()
            if network.failure is not None:
                raise BatchRunError("网络资源关闭失败") from network.failure
            summary.http_attempts_started = network.http_attempts_started
    except BaseException as error:
        if isinstance(error, KeyboardInterrupt):
            logger.warning("收到用户中断，开始停止请求并保存已完成结果")
        else:
            logger.exception("worker 异常停止，开始收尾")
        if network is not None:
            network.shutdown()
        try:
            if coordinator is not None and not isinstance(error, BatchPersistenceError) and store.pending_patch is None:
                coordinator.finish()
        except BaseException as persistence_error:
            _retain_unsaved(persistence_error, store, coordinator, network)
            logger.exception("异常收尾时结果保存未确认")
            logger.info("worker 收尾结束")
            raise persistence_error from error
        _retain_unsaved(error, store, coordinator, network)
        logger.info("worker 收尾结束")
        raise
    summary.elapsed_seconds = time.monotonic() - started
    scope = "本次读取范围" if start is None else "本次日期范围"
    logger.info("%s内的任务全部处理完了：成功 %d，失败 %d；HTTP起发=%d，耗时=%.3f秒",
                scope, summary.new_succeeded, summary.new_failed,
                summary.http_attempts_started, summary.elapsed_seconds)
    return summary
