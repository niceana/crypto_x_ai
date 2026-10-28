"""只读已准备的事件向量，缓存矩阵并按原规则筛选候选；不调用向量服务。"""

from collections import defaultdict
import numpy as np
from round2.retrieval import RetrievalSelection
from vectorization.storage import EmbeddingError, StoredVectors

class _CandidateMatrix:
    """按披露缓存 float64 行；连续时间窗口直接使用切片，不缓存分组或检索结果。"""

    def __init__(self):
        self.positions = {}
        self.sources = {}
        self.data = None
        self.size = 0

    def select(self, records, vectors):
        occurrences, rows = defaultdict(int), []
        for record in records:
            text = record["event"]
            # 正式记录使用 record_id；也兼容只提供 event/event_id 的自定义调用方。
            identity = (record.get("record_id"), text)
            occurrence = occurrences[identity]
            occurrences[identity] += 1
            key = identity + (occurrence,)
            vector = vectors[text]
            position = self.positions.get(key)
            if position is None:
                if self.data is None or self.size == len(self.data):
                    capacity = max(32, self.size * 2)
                    expanded = np.empty((capacity, len(vector)), dtype=np.float64)
                    if self.data is not None:
                        expanded[:self.size] = self.data[:self.size]
                    self.data = expanded
                position = self.size
                self.size += 1
                self.positions[key] = position
            if self.sources.get(key) is not vector:
                self.data[position] = vector
                self.sources[key] = vector
            rows.append(position)
        begin = rows[0]
        if all(position == begin + offset for offset, position in enumerate(rows)):
            return self.data[begin:begin + len(rows)]
        # 非顺序回查仍按调用方原顺序计算，不能改变并列分数的候选语义。
        return self.data[rows]


class EmbeddingRetriever(StoredVectors):
    """只加载已保存向量并检索，不生成、不保存、不修复向量。"""

    def __init__(self, config):
        super().__init__(config)
        self._matrices = {}

    def __call__(self, current, searchable):
        if not searchable:
            return RetrievalSelection((), True, "embedding-event-v2：同币配置的历史窗口无已完成披露，前史检查已通过")
        text = current["event"]
        if text not in self.vectors or any(record["event"] not in self.vectors for record in searchable):
            raise EmbeddingError("评分前必须准备并保存当前及历史 event 的向量")
        cache = self._matrices.setdefault(current.get("asset", ""), _CandidateMatrix())
        matrix = cache.select(searchable, self.vectors)
        scores = np.clip(matrix @ self.vectors[text], -1, 1)
        groups = {}
        for record, score in zip(searchable, scores):
            event_id = record["event_id"]
            groups[event_id] = max(groups.get(event_id, -float("inf")), float(score))
        qualifying = [event_id for event_id in groups if groups[event_id] >= self.config.similarity_threshold]
        if not qualifying:
            reason = "embedding-event-v2：所有组低于阈值，max_similarity={:.8f}，similarity_threshold={}".format(
                max(groups.values()), self.config.similarity_threshold)
            return RetrievalSelection((), True, reason)
        ids = sorted(qualifying, key=lambda event_id: (-groups[event_id], event_id))[:10]
        return RetrievalSelection(tuple(ids))
