"""生成并复用事件去重分析表中的向量，按事件组最高相似度和阈值选择候选。"""

import hashlib
import json
import logging
import time
from collections import defaultdict

import httpx
import numpy as np

from round2.retrieval import RetrievalSelection
from common.network import RETRYABLE_NETWORK_ERRORS, parse_retry_after, retry_backoff
from common.validation import parse_strict_json

logger = logging.getLogger(__name__)


class EmbeddingError(RuntimeError):
    """向量生成或保存内容无效；停止本轮，不转成低分或自动判新。"""


def _create_client(config):
    """批量准备期间复用一个客户端，重试次数只由本模块计算。"""
    return httpx.Client(timeout=config.timeout_seconds, trust_env=config.trust_env,
                        follow_redirects=False)


def _validate_unit_vector(raw):
    """校验服务返回的单位向量，保留分量原值供保存和内积计算。"""
    if not isinstance(raw, list) or not raw or any(type(value) not in (int, float) for value in raw):
        raise EmbeddingError("Embedding 向量必须为非空数值数组")
    try:
        vector = np.asarray(raw, dtype=np.float64)
    except (ValueError, OverflowError) as exc:
        raise EmbeddingError("Embedding 向量包含无效数值") from exc
    if not np.isfinite(vector).all():
        raise EmbeddingError("Embedding 向量包含非有限数值")
    if not np.any(vector):
        raise EmbeddingError("Embedding 不能使用零向量")
    # 服务负责归一化；这里只检查约定，不修正异常输出或改变数值精度。
    with np.errstate(over="ignore", invalid="ignore"):
        norm = np.linalg.norm(vector)
    if not np.isfinite(norm) or abs(norm - 1) > 1e-5:
        raise EmbeddingError("Embedding 向量未归一化，请核查模型输出或保存内容")
    return vector


class EmbeddingRetriever:
    """只编码 event，以组内最高余弦相似度和配置阈值筛选候选。

    prepare 接收原键、event 和表内 embedding，复用有效旧向量，按批调用
    save_embeddings 保存新 JSON。runner 在认领前准备并保存，随后仅比较
    内存向量。本模块不读写业务客户端，不建立本地持久缓存。
    """

    def __init__(self, config, client_factory=None):
        self.config = config
        self.client_factory = client_factory or _create_client
        self.vectors = {}
        self.serialized = {}
        self.dimension = None
        identity = {"base_url": config.base_url, "model": config.model,
                    "model_version": config.model_version, "encoding_format": "float",
                    "representation": "event-only-service-unit-f64-v3"}
        # event 导入后固定；此摘要只判断各记录的向量配置是否与本轮相同。
        encoded = json.dumps(identity, sort_keys=True, ensure_ascii=False).encode("utf-8")
        self.config_hash = hashlib.sha256(encoded).hexdigest()

    def _check_dimension(self, dimension):
        if self.dimension is not None and dimension != self.dimension:
            raise EmbeddingError("Embedding 向量维度与本轮已有向量不一致，请核查模型版本")

    def _parse_response(self, response, size):
        """先校验完整批次再允许保存；index 必须与本批输入一一对应。"""
        try:
            body = parse_strict_json(response.text)
        except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
            raise EmbeddingError("Embedding 响应不是有效 JSON") from exc
        data = body.get("data") if isinstance(body, dict) and "error" not in body else None
        if not isinstance(data, list) or len(data) != size:
            raise EmbeddingError("Embedding 返回数量与本批输入不一致")
        result = [None] * size
        for item in data:
            index = item.get("index") if isinstance(item, dict) else None
            if type(index) is not int or not 0 <= index < size or result[index] is not None:
                raise EmbeddingError("Embedding index 缺失、重复或超出本批范围")
            vector = _validate_unit_vector(item.get("embedding"))
            self._check_dimension(len(vector))
            result[index] = vector
        if len({len(vector) for vector in result}) != 1:
            raise EmbeddingError("Embedding 同一批向量维度不一致")
        return result

    def _request(self, client, texts):
        """传输、429、5xx 和无效输出共用本批预算，复用 AI 网络的退避规则。"""
        for attempt in range(1, self.config.max_attempts + 1):
            retry_after = 0
            try:
                response = client.post(
                    self.config.base_url + "/embeddings",
                    headers={"Authorization": "Bearer " + self.config.api_key},
                    json={"model": self.config.model, "input": texts, "encoding_format": "float"},
                )
            except RETRYABLE_NETWORK_ERRORS as exc:
                if attempt == self.config.max_attempts:
                    raise EmbeddingError("Embedding 网络请求预算耗尽") from exc
            except httpx.RequestError as exc:
                raise EmbeddingError("Embedding 请求异常，需排查后重启") from exc
            else:
                if 200 <= response.status_code < 300:
                    try:
                        return self._parse_response(response, len(texts))
                    except EmbeddingError as exc:
                        if attempt == self.config.max_attempts:
                            raise
                        logger.warning("Embedding 输出无效，使用本批剩余预算重试：原因=%s", exc)
                elif ((response.status_code != 429 and not 500 <= response.status_code <= 599)
                      or attempt == self.config.max_attempts):
                    raise EmbeddingError("Embedding HTTP {}，停止本轮".format(response.status_code))
                else:
                    retry_after = parse_retry_after(response.headers.get("Retry-After")) or 0
                    logger.warning("Embedding HTTP %d，使用本批剩余预算重试：尝试=%d/%d",
                                   response.status_code, attempt, self.config.max_attempts)
            # 批次串行准备，无需聊天并发的全局暂停门；两种等待从本次失败同时计时。
            delay = max(retry_backoff(self.config, attempt), retry_after)
            logger.warning("round2事件去重归组 Embedding 批次准备重试：尝试=%d/%d，等待=%.3f秒",
                           attempt, self.config.max_attempts, delay)
            time.sleep(delay)

    def _load_saved_vector(self, text, value):
        """空值或标识不匹配时重新生成；匹配内容损坏时明确停止。"""
        if not isinstance(value, str):
            raise EmbeddingError("结果表 embedding 必须为空字符串或 JSON 对象字符串")
        if value == "":
            return False
        try:
            body = parse_strict_json(value)
        except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
            raise EmbeddingError("结果表 embedding 不是合法 JSON") from exc
        if (not isinstance(body, dict) or set(body) != {"model", "model_version", "config_hash", "vector"}
                or any(not isinstance(body[name], str) or not body[name] for name in ("model", "model_version", "config_hash"))):
            raise EmbeddingError("结果表 embedding 的对象结构不合法")
        if (body["model"] != self.config.model or body["model_version"] != self.config.model_version
                or body["config_hash"] != self.config_hash):
            return False
        vector = _validate_unit_vector(body["vector"])
        self._check_dimension(len(vector))
        self.dimension = len(vector)
        if text not in self.vectors:
            self.vectors[text] = vector
            self.serialized[text] = value
        return True

    def prepare(self, records, save_embeddings):
        """records 为 (原键, event, embedding)；成功批次先按列保存再用于评分。

        相同 event 在本轮只生成一次，向量仍写入每条对应披露。保存回调负责
        存储重试；它失败时立即传播，不回到接口请求循环，不伪造完成状态。
        """
        pending = defaultdict(list)
        for key, text, value in records:
            if not isinstance(text, str) or not text.strip():
                raise EmbeddingError("Embedding 输入必须为非空 event 字符串")
            if not self._load_saved_vector(text, value):
                pending[text].append(key)
        reusable = {key: self.serialized[text] for text, keys in pending.items()
                    if text in self.vectors for key in keys}
        if reusable:
            save_embeddings(reusable)
        missing = [text for text in pending if text not in self.vectors]
        logger.info("round2事件去重归组 Embedding 准备：已复用文本=%d，需请求文本=%d", len(self.vectors), len(missing))
        if not missing:
            return
        with self.client_factory(self.config) as client:
            for offset in range(0, len(missing), self.config.batch_size):
                batch = missing[offset:offset + self.config.batch_size]
                vectors = self._request(client, batch)
                serialized = {
                    text: json.dumps({"model": self.config.model, "model_version": self.config.model_version,
                                      "config_hash": self.config_hash, "vector": vector.tolist()},
                                     separators=(",", ":"), ensure_ascii=False, allow_nan=False)
                    for text, vector in zip(batch, vectors)
                }
                # 一批全部有效后提交；回调失败不重新请求 Embedding。
                save_embeddings({key: serialized[text] for text in batch for key in pending[text]})
                self.dimension = len(vectors[0])
                self.vectors.update(zip(batch, vectors))
                self.serialized.update(serialized)
                logger.info("round2事件去重归组 Embedding 新文本保存进度：%d/%d", offset + len(batch), len(missing))

    def __call__(self, current, searchable):
        if not searchable:
            return RetrievalSelection((), True, "embedding-event-v2：同币配置的历史窗口无已完成披露，前史检查已通过")
        text = current["event"]
        if text not in self.vectors or any(record["event"] not in self.vectors for record in searchable):
            raise EmbeddingError("评分前必须准备并保存当前及历史 event 的向量")
        matrix = np.stack([self.vectors[record["event"]] for record in searchable])
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
