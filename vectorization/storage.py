"""向量格式、版本标识和只读载入；不访问向量服务。"""

import hashlib
import json
import numpy as np
from common.validation import parse_strict_json

class EmbeddingError(RuntimeError):
    """向量生成或保存内容无效；停止本轮，不转成低分或自动判新。"""


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


class StoredVectors:
    """共用不可变事件向量格式，保持已有表内 config_hash 和 float64 数值。"""

    def __init__(self, config):
        self.config = config
        self.vectors = {}
        self.serialized = {}
        self.dimension = None
        identity = {"base_url": config.base_url, "model": config.model,
                    "model_version": config.model_version, "encoding_format": "float",
                    "representation": "event-only-service-unit-f64-v3"}
        encoded = json.dumps(identity, sort_keys=True, ensure_ascii=False).encode("utf-8")
        self.config_hash = hashlib.sha256(encoded).hexdigest()

    def _check_dimension(self, dimension):
        if self.dimension is not None and dimension != self.dimension:
            raise EmbeddingError("Embedding 向量维度与本轮已有向量不一致，请核查模型版本")

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

    def load_prepared(self, records):
        """仅载入已持久化且版本匹配的向量；第二轮禁止回退到 HTTP 计算。"""
        for key, text, value in records:
            if not isinstance(text, str) or not text.strip():
                raise EmbeddingError("Embedding 输入必须为非空 event 字符串")
            if not self._load_saved_vector(text, value):
                raise EmbeddingError("事件 {} 缺少同版本的已保存向量，请先运行向量准备".format(key))

