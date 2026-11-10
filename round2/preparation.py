"""第二轮独立补货：就绪队列腾出位置即继续准备，不等待结果写库。"""

import logging
from collections import defaultdict, deque
from dataclasses import dataclass

from common.ai_protocol import request_failure
from common.event_context import build_relation_input
from common.network import CompletionMessage, RequestJob
from common.preparation import RequestPreparer
from common.validation import validate_relation_output
from round2.retrieval import validate_retrieval_selection

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AutoNewResult:
    key: tuple
    reason: str


class RelationPreparer(RequestPreparer):
    """单一线程拥有历史和待准备候选，持续补足有界的可发送 AI 请求。

    只接收调用线程认领后的固定输入和已写库历史副本；不读取工作表或数据库。
    自动判新及构造失败直接进入共用结果队列，不占就绪队列容量。
    """

    def __init__(self, history, retrieve, config, network, *, max_candidate_groups=10,
                 max_input_characters=None):
        super().__init__(config, network, log=logger, label="第二轮")
        self.max_candidate_groups = max_candidate_groups
        self.max_input_characters = max_input_characters
        self.history = history
        self.retrieve = retrieve
        self.candidates = defaultdict(deque)
        self.assets = deque()
        self.history_updates = deque()

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

    def _take_work(self):
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
            return "prepare", record

    def _process_work(self, kind, value):
        if kind == "history":
            self.history.add_records(value)
        else:
            self._prepare(*value)

    def _prepare(self, key, current):
        asset = current["asset"]
        searchable = self.history.get_searchable_records(asset, key[0])
        selection = validate_retrieval_selection(
            self.retrieve(current, searchable), {record["event_id"] for record in searchable},
            max_groups=self.max_candidate_groups)
        if selection.auto_new:
            if not self.stopped.is_set():
                self.network.completions.put_nowait(AutoNewResult(key, selection.reason))
            return
        candidates = [self.history.build_group_history(asset, event_id, key[0])
                      for event_id in selection.event_ids]
        text, candidate_event_ids = build_relation_input(current, candidates)
        self.logger.debug("第二轮请求准备：record_id=%s，候选组=%d，输入字符数=%d",
                          key[1], len(candidates), len(text))
        if self.max_input_characters is not None and len(text) > self.max_input_characters:
            self.network.completions.put_nowait(CompletionMessage(key, request_failure(
                "第二轮输入字符数 {} 超过配置上限 {}，候选组 {}；未截断输入，未发送 AI 请求".format(
                    len(text), self.max_input_characters, len(candidates)))))
            return
        self._prepare_request(RequestJob(
            key, text, lambda content, ids=candidate_event_ids: validate_relation_output(content, ids)))
