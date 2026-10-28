"""独立向量计算：沿用已有 HTTP 重试和批量生成规则。"""

import json
import logging
import time
from collections import defaultdict
import httpx
from common.network import RETRYABLE_NETWORK_ERRORS, parse_retry_after, retry_backoff
from common.validation import parse_strict_json
from vectorization.storage import EmbeddingError, StoredVectors, _validate_unit_vector

logger = logging.getLogger(__name__)

def _create_client(config):
    """批量准备期间复用一个客户端，重试次数只由本模块计算。"""
    return httpx.Client(timeout=config.timeout_seconds, trust_env=config.trust_env,
                        follow_redirects=False)


class EmbeddingEncoder(StoredVectors):
    """只计算、复用及保存向量，不做候选检索或事件归组。"""

    def __init__(self, config, client_factory=None, stop_event=None):
        super().__init__(config)
        self.client_factory = client_factory or _create_client
        self.stop_event = stop_event

    def _check_running(self):
        if self.stop_event is not None and self.stop_event.is_set():
            raise EmbeddingError("月流水线已停止，取消向量准备")

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
            self._check_running()
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
            logger.warning("独立向量计算 批次准备重试：尝试=%d/%d，等待=%.3f秒",
                           attempt, self.config.max_attempts, delay)
            if self.stop_event is None:
                time.sleep(delay)
            elif self.stop_event.wait(delay):
                self._check_running()

    def prepare(self, records, save_embeddings):
        """records 为 (原键, event, embedding)；成功批次先按列保存再用于评分。

        相同 event 在本轮只生成一次，向量仍写入每条对应披露。保存回调负责
        存储重试；它失败时立即传播，不回到接口请求循环，不伪造完成状态。
        """
        self._check_running()
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
        logger.info("独立向量计算 准备：已复用文本=%d，需请求文本=%d", len(self.vectors), len(missing))
        if not missing:
            return
        with self.client_factory(self.config) as client:
            for offset in range(0, len(missing), self.config.batch_size):
                self._check_running()
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
                logger.info("独立向量计算 新文本保存进度：%d/%d", offset + len(batch), len(missing))

