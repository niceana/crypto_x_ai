"""三轮共用的请求准备线程：就绪队列取走一条即补货，不访问数据库。"""

import json
import logging
import queue
import threading
from collections import deque
from dataclasses import replace

from common.ai_protocol import TaskRenderError, render_payload, request_failure
from common.network import CompletionMessage


class RequestPreparer:
    """接收已认领的固定请求输入，持续补足有界就绪队列。

    数据库与工作表仅由调用线程操作。子类可扩展候选选择和业务准备，
    共用唤醒、请求编码、构造失败交付和停止收尾。
    """

    def __init__(self, config, network, *, log=None, label="AI", control=None):
        self.config = config
        self.network = network
        self.logger = log or logging.getLogger(__name__)
        self.label = label
        self.control = control
        self.ready_requests = queue.Queue(maxsize=config.run.ready_request_buffer_size)
        self.condition = threading.Condition()
        self.candidates = deque()
        self.queued = 0
        self.preparing = False
        self.stopped = threading.Event()
        self.failure = None
        self.thread = threading.Thread(target=self._run, name="ai-request-prepare")

    def start(self):
        self.thread.start()

    def check(self):
        if self.control is not None:
            self.control.check()
        if self.failure is not None:
            raise self.failure

    def snapshot(self):
        with self.condition:
            if self.stopped.is_set():
                state = "已停止"
            elif self.preparing:
                state = "准备中"
            elif self.ready_requests.full():
                state = "就绪队列已满"
            else:
                state = "等待可推进任务"
            return state, self.queued + int(self.preparing)

    def wake(self):
        with self.condition:
            self.condition.notify()

    def submit_batch(self, jobs):
        with self.condition:
            self.candidates.extend(jobs)
            self.queued += len(jobs)
            self.condition.notify()

    def _take_work(self):
        if self.candidates and not self.ready_requests.full():
            self.queued -= 1
            return "prepare", self.candidates.popleft()

    def _next_work(self):
        with self.condition:
            while not self.stopped.is_set() and not self.network.stopped.is_set():
                work = self._take_work()
                if work is not None:
                    self.preparing = work[0] == "prepare"
                    return work
                self.condition.wait()

    def _process_work(self, kind, value):
        if self.control is not None:
            self.control.check()
        self._prepare_request(value)

    def _run(self):
        try:
            while True:
                work = self._next_work()
                if work is None:
                    return
                try:
                    self._process_work(*work)
                finally:
                    with self.condition:
                        self.preparing = False
        except BaseException as error:
            self.failure = error
            self.request_stop()
            self.network.request_stop(self.label + "数据准备异常，停止请求")

    def _prepare_request(self, job):
        if self.control is not None:
            self.control.check()
        try:
            if job.body is None:
                payload = render_payload(self.config, job.text)
                body = json.dumps(payload, ensure_ascii=False, allow_nan=False,
                                  separators=(",", ":")).encode("utf-8")
                job = replace(job, body=body)
        except TaskRenderError as error:
            self.logger.warning("%s单任务请求构造失败：任务=%r，原因=%s；不发送 HTTP",
                                self.label, job.key, error)
            if not self.stopped.is_set() and not self.network.stopped.is_set():
                self.network.completions.put_nowait(CompletionMessage(job.key, request_failure(str(error))))
            return
        if not self.stopped.is_set() and not self.network.stopped.is_set():
            if self.control is not None:
                self.control.check()
            # 单一生产者只在未满时开始准备，消费者只取走，因此此处不等待容量。
            self.ready_requests.put_nowait(job)
            self.network.notify_ready()

    def request_stop(self):
        with self.condition:
            self.stopped.set()
            self.condition.notify()

    def shutdown(self):
        self.request_stop()
        if self.thread.ident is not None:
            while self.thread.is_alive():
                self.thread.join(0.25)
