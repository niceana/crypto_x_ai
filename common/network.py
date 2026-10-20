"""共用网络执行：输入仅在内存保存，数据库操作由调用线程负责。"""

import asyncio
import logging
import math
import queue
import random
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Optional, Tuple

import httpx

from common.ai_protocol import (
    TaskRenderError,
    render_payload,
    ParsedResponse,
    parse_response,
    request_failure,
)
from common.validation import OutputValidationError

logger = logging.getLogger(__name__)
TaskKey = Tuple[Any, str]


class BatchRunError(RuntimeError):
    """整个请求执行器停止；普通单条失败由完成消息返回。"""


@dataclass(frozen=True)
class RequestJob:
    """认领确认后的不可变输入快照；原键只用于内部关联。"""

    key: TaskKey
    text: str
    validate_output: Optional[Callable[[str], Any]] = None


@dataclass(frozen=True)
class CompletionMessage:
    """每个作业仅发布一次；停止消息不伪造 AI 最终结果。"""

    key: TaskKey
    response: Optional[ParsedResponse] = None
    http_attempts_started: int = 0
    stopped: bool = False
    error: Optional[BaseException] = None
    value: Any = None


# 只列出已确认可重试的具体类型，避免基类捕获扩大重试范围。
RETRYABLE_NETWORK_ERRORS = (httpx.ConnectError, httpx.ReadError, httpx.WriteError,
                            httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout)
_STOP_REQUEST = (httpx.InvalidURL, httpx.UnsupportedProtocol, httpx.LocalProtocolError,
                 httpx.RemoteProtocolError, httpx.ProxyError, httpx.PoolTimeout)


def _exception_reason(exc):
    """保留失败结果的具体异常说明，无消息时沿用异常类名。"""
    return str(exc).strip() or type(exc).__name__


def parse_retry_after(value):
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


def retry_backoff(run, count):
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

    def __init__(self, rps, stopped, log):
        self.logger = log
        self.interval = 1.0 / rps if rps else 0.0
        self.next_start = 0.0
        self.pause_until = 0.0
        self.stopped = stopped
        self.lock = asyncio.Lock()

    def pause(self, seconds):
        self.pause_until = max(self.pause_until, time.monotonic() + seconds)
        self.logger.warning("服务端要求全局暂停：本次等待=%.3f秒；后续请求遵守最晚暂停截止", seconds)

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


class NetworkWorker:
    """网络线程独占事件循环和客户端，通过有界队列向调用线程报告。"""

    def __init__(self, config, client_factory=None, retry_after=None, log=None):
        # round1事件提取传入本轮日志对象，使共用请求日志仍归属本轮。
        self.logger = log or logger
        self.config = config
        self._client_factory = client_factory or _create_client
        self._retry_after = retry_after or parse_retry_after
        self.completions = queue.Queue(maxsize=config.run.concurrency)
        self._submission_lock = threading.Lock()
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
        """阻止新起发并通知网络线程收尾；不等待线程退出。"""
        # 与提交共用短锁，确保已接受的回调排在关闭通知之前。
        with self._submission_lock:
            if self.stopped.is_set():
                return
            self.stop_reason = reason
            self.stopped.set()
            if self.loop is not None and self.loop.is_running() and self.shutdown_event is not None:
                try:
                    self.loop.call_soon_threadsafe(self.shutdown_event.set)
                except RuntimeError:
                    pass  # 网络线程可能已在异常收尾中关闭事件循环。

    def start(self):
        """等待客户端就绪，同时检查线程是否提前退出。"""
        self.thread.start()
        while not self.ready.wait(0.1):
            if not self.thread.is_alive():
                raise BatchRunError("网络线程在就绪前退出") from self.failure
        self.check()
        self.logger.info("网络线程已就绪：并发上限=%d", self.config.run.concurrency)

    def check(self):
        if self.failure is not None:
            raise BatchRunError("网络线程或作业发生未预期异常") from self.failure
        if self.stopped.is_set():
            raise BatchRunError(self.stop_reason or "worker 已停止")
        if not self.thread.is_alive():
            raise BatchRunError("网络线程意外退出")

    def submit(self, job):
        # 调用线程不直接操作 asyncio.Task；实际创建发生在网络循环。
        with self._submission_lock:
            if not self.stopped.is_set():
                self.loop.call_soon_threadsafe(self._launch, job)
                return
        # 停止后循环可能已经关闭，直接回报；发布失败时可能再次申请停止，不能持锁。
        self._publish(CompletionMessage(job.key, stopped=True))

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
            self.logger.error("完成队列容量不变量被破坏：任务=%r，未发布结果已保留", message.key)
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
                self.logger.error("网络作业发生未预期异常：任务=%r，异常类型=%s", job.key, type(exc).__name__)
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
                self.logger.warning("单任务请求构造失败：任务=%r，异常类型=%s；不发送 HTTP", job.key, type(exc).__name__)
                return CompletionMessage(job.key, request_failure(_exception_reason(exc)))
            while True:
                await self.gate.acquire()
                if self.stopped.is_set():
                    raise asyncio.CancelledError()
                # 网络故障与 5xx 共用实际起发计数，不按错误类型拆分预算。
                count += 1
                self.http_attempts_started += 1
                self.logger.debug("开始 HTTP 请求：任务=%r，尝试=%d/%d", job.key, count, self.config.run.max_attempts)
                retry = False
                value = None
                try:
                    response = await self.client.post(self.config.api.url, headers=self.config.api.headers, json=payload)
                except RETRYABLE_NETWORK_ERRORS as exc:
                    parsed = request_failure(_exception_reason(exc))
                    retry = count < self.config.run.max_attempts
                    self.logger.warning("网络传输故障：任务=%r，尝试=%d/%d，异常类型=%s，允许继续尝试=%s",
                                   job.key, count, self.config.run.max_attempts, type(exc).__name__, "是" if retry else "否")
                except _STOP_REQUEST as exc:
                    parsed = request_failure(_exception_reason(exc))
                    self.logger.error("请求异常触发整个 worker 停止：任务=%r，异常类型=%s；需要人工排查",
                                 job.key, type(exc).__name__)
                    self.request_stop(parsed.error_message)
                except httpx.RequestError as exc:
                    self.logger.warning("单任务请求异常：任务=%r，异常类型=%s；不自动重试", job.key, type(exc).__name__)
                    parsed = request_failure(_exception_reason(exc))
                else:
                    status = response.status_code
                    is_server_failure = 500 <= status <= 599
                    must_stop = (not 200 <= status < 300 and not is_server_failure) or (
                        is_server_failure and count >= self.config.run.max_attempts
                    )
                    if must_stop:
                        self.request_stop("HTTP {}：需要人工排查后重启".format(status))
                        self.logger.error("HTTP 失败触发整个 worker 停止：任务=%r，HTTP状态=%d，尝试=%d/%d；需要人工排查",
                                     job.key, status, count, self.config.run.max_attempts)
                    parsed = parse_response(status, response.text)
                    retry = is_server_failure and count < self.config.run.max_attempts
                    # 三轮都提供各自的正文校验；无效输出与网络重试共用本任务预算。
                    if job.validate_output is not None and 200 <= status < 300:
                        if parsed.success:
                            try:
                                value = job.validate_output(parsed.ai_response_content)
                            except OutputValidationError as exc:
                                parsed = replace(parsed, success=False,
                                                 error_message=str(exc)[:512])
                        if not parsed.success:
                            retry = count < self.config.run.max_attempts
                    if retry and not self.stopped.is_set():
                        if is_server_failure:
                            self.logger.warning("接口返回 5xx，准备重试：任务=%r，HTTP状态=%d，尝试=%d/%d",
                                           job.key, status, count, self.config.run.max_attempts)
                            waiting = self._retry_after(response.headers.get("Retry-After"))
                            if waiting is not None:
                                self.gate.pause(waiting)
                        else:
                            self.logger.warning("业务输出无效，准备重试：任务=%r，尝试=%d/%d，原因=%s",
                                           job.key, count, self.config.run.max_attempts, parsed.error_message)
                    # 退避可能较长，等待前释放完整响应，仅保留必要解析字段。
                    del response
                if parsed.success or not retry:
                    if parsed.success:
                        self.logger.debug("单任务请求成功：任务=%r，实际尝试=%d", job.key, count)
                    else:
                        # 2xx 下的响应与正文校验失败提供具体原因；HTTP 或传输错误
                        # 仍沿用原日志口径，外部接口和网络异常说明保存在任务表。
                        detail = ("，原因=" + parsed.error_message
                                  if parsed.http_status is not None and 200 <= parsed.http_status < 300 else "")
                        self.logger.warning("单任务请求最终失败：任务=%r，HTTP状态=%s，实际尝试=%d%s",
                                       job.key, parsed.http_status, count, detail)
                    return CompletionMessage(job.key, parsed, count, value=value)
                if self.stopped.is_set():
                    raise asyncio.CancelledError()
                delay = retry_backoff(self.config.run, count)
                self.logger.warning("请求重试退避：任务=%r，下次尝试=%d/%d，本地等待=%.3f秒",
                               job.key, count + 1, self.config.run.max_attempts, delay)
                await self.gate.wait_until(time.monotonic() + delay)
        except asyncio.CancelledError:
            self.logger.debug("作业因整个 worker 停止而结束：任务=%r，已起发=%d；无最终响应时保留状态 9", job.key, count)
            return CompletionMessage(job.key, http_attempts_started=count, stopped=True)
        except Exception as exc:
            self.failure = exc
            self.request_stop()
            self.logger.error("网络作业发生批次异常：任务=%r，异常类型=%s", job.key, type(exc).__name__)
            return CompletionMessage(job.key, http_attempts_started=count, error=exc)

    async def _serve(self):
        """创建共享客户端，收到停止通知后自行取消任务并关闭资源。"""
        self.shutdown_event = asyncio.Event()
        async with self._client_factory(self.config) as client:
            self.client = client
            self.gate = _RequestGate(self.config.run.requests_per_second, self.stopped, self.logger)
            self.ready.set()
            try:
                # 初始化前收到停止时，通知可能还无法投递，直接进入收尾。
                if not self.stopped.is_set():
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
            self.logger.error("网络线程运行或关闭失败：异常类型=%s", type(exc).__name__)
            self.failure = exc
            self.request_stop()
        finally:
            self.ready.set()
            if self.loop is not None:
                try:
                    self.loop.run_until_complete(self.loop.shutdown_asyncgens())
                except BaseException as exc:
                    self.logger.error("异步生成器关闭失败：异常类型=%s", type(exc).__name__)
                    self.failure = exc
                    self.request_stop()
                finally:
                    self.loop.close()
            self.logger.debug("网络线程清理结束")

    def shutdown(self):
        """调用线程发出停止通知并等待退出，网络线程自行完成资源清理。"""
        self.request_stop()
        if self.thread.ident is not None:
            while self.thread.is_alive():
                self.thread.join(0.25)


