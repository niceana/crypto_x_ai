"""第二轮独立补货：就绪队列腾出位置即继续准备，不等待结果写库。"""

import json
import logging
import queue
import threading
from collections import defaultdict, deque
from dataclasses import dataclass

from common.ai_protocol import TaskRenderError, render_payload, request_failure
from common.event_context import build_relation_input
from common.network import CompletionMessage, RequestJob
from common.validation import validate_relation_output
from round2.retrieval import validate_retrieval_selection

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AutoNewResult:
    key: tuple
    reason: str


class RelationPreparer:
    """单一线程拥有历史和待准备候选，持续补足有界的可发送 AI 请求。

    只接收调用线程认领后的固定输入和已写库历史副本；不读取工作表或数据库。
    自动判新及构造失败直接进入共用结果队列，不占就绪队列容量。
    """

    def __init__(self, history, retrieve, config, network):
        self.history = history
        self.retrieve = retrieve
        self.config = config
        self.network = network
        self.ready_requests = queue.Queue(maxsize=config.run.ready_request_buffer_size)
        self.condition = threading.Condition()
        self.candidates = defaultdict(deque)
        self.assets = deque()
        self.history_updates = deque()
        self.queued = 0
        self.preparing = False
        self.stopped = threading.Event()
        self.failure = None
        self.thread = threading.Thread(target=self._run, name="round2-prepare")

    def start(self):
        self.thread.start()

    def check(self):
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

    def submit_batch(self, records):
        with self.condition:
            for key, current in records:
                asset = current["asset"]
                if not self.candidates[asset]:
                    self.assets.append(asset)
                self.candidates[asset].append((key, current))
                self.queued += 1
            self.condition.notify()

    def add_history(self, records):
        with self.condition:
            # 更新优先于取候选；同币下一时点的输入由调用线程在此调用后登记。
            self.history_updates.append(records)
            self.condition.notify()

    def _next_work(self):
        with self.condition:
            while not self.stopped.is_set() and not self.network.stopped.is_set():
                if self.history_updates:
                    return "history", self.history_updates.popleft()
                if self.assets and not self.ready_requests.full():
                    asset = self.assets.popleft()
                    record = self.candidates[asset].popleft()
                    if self.candidates[asset]:
                        self.assets.append(asset)
                    else:
                        del self.candidates[asset]
                    self.queued -= 1
                    self.preparing = True
                    return "prepare", record
                self.condition.wait()
            return None

    def _run(self):
        try:
            while True:
                work = self._next_work()
                if work is None:
                    return
                kind, value = work
                if kind == "history":
                    self.history.add_records(value)
                else:
                    try:
                        self._prepare(*value)
                    finally:
                        with self.condition:
                            self.preparing = False
        except BaseException as error:
            self.failure = error
            self.request_stop()
            self.network.request_stop("第二轮数据准备异常，停止请求")

    def _prepare(self, key, current):
        asset = current["asset"]
        searchable = self.history.get_searchable_records(asset, key[0])
        selection = validate_retrieval_selection(
            self.retrieve(current, searchable), {record["event_id"] for record in searchable})
        if selection.auto_new:
            if not self.stopped.is_set():
                self.network.completions.put_nowait(AutoNewResult(key, selection.reason))
            return
        candidates = [self.history.build_group_history(asset, event_id, key[0])
                      for event_id in selection.event_ids]
        text, candidate_event_ids = build_relation_input(current, candidates)
        try:
            payload = render_payload(self.config, text)
            body = json.dumps(payload, ensure_ascii=False, allow_nan=False,
                              separators=(",", ":")).encode("utf-8")
        except TaskRenderError as error:
            logger.warning("round2事件去重归组单任务请求构造失败：record_id=%s，原因=%s；不发送 HTTP",
                           key[1], error)
            if not self.stopped.is_set():
                self.network.completions.put_nowait(CompletionMessage(key, request_failure(str(error))))
            return
        if not self.stopped.is_set() and not self.network.stopped.is_set():
            # 单一生产者只在未满时开始准备，消费者只取走，因此此处不等待容量。
            self.ready_requests.put_nowait(RequestJob(
                key, text, lambda content, ids=candidate_event_ids: validate_relation_output(content, ids),
                body=body))
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
