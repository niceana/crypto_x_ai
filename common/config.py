"""读取各类配置、检查启动要求并计算配置摘要；不构造业务输入或请求。"""

import copy
import hashlib
import json
import logging
import math
import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlsplit

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class ConfigError(ValueError):
    """启动配置不符合约定；不在错误说明中暴露配置值。"""


@dataclass(frozen=True)
class Prompts:
    """尚未填入单帖正文的提示词模板。"""

    system: str
    user: str


@dataclass(frozen=True)
class ApiConfig:
    """本轮使用的接口配置；认证头和 payload 不写入日志。"""

    url: str
    headers: Dict[str, str]
    payload: Dict[str, Any]
    trust_env: bool = False


@dataclass(frozen=True)
class RunConfig:
    """并发、保存、限速和内存重试的运行参数。"""

    concurrency: int = 100
    batch_size: int = 200
    result_buffer_size: int = 200
    flush_interval_seconds: float = 1
    timeout_seconds: float = 120
    requests_per_second: float = 0
    max_attempts: int = 3
    backoff_base_seconds: float = 1
    backoff_max_seconds: float = 30
    storage_max_attempts: int = 3
    storage_retry_seconds: float = 1
    progress_every: int = 100


@dataclass(frozen=True)
class AIConfig:
    """本轮固定使用的配置对象，月间不重新读取 YAML。"""

    prompts: Prompts
    api: ApiConfig
    run: RunConfig


class _UniqueLoader(yaml.SafeLoader):
    """在安全 YAML 加载基础上拒绝重复键，避免配置被静默覆盖。"""


def _unique_mapping(loader, node):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if not isinstance(key, str):
            raise ConfigError("配置字典键必须为字符串")
        if key in result:
            raise ConfigError("配置存在重复键")
        result[key] = loader.construct_object(value_node, deep=True)
    return result


_UniqueLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _unique_mapping
)
TEMPLATE_VARIABLE = re.compile(r"\{\{(.*?)\}\}", re.DOTALL)
_ENVIRONMENT = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_INTEGER_FIELDS = {
    "concurrency", "batch_size", "result_buffer_size", "max_attempts",
    "storage_max_attempts", "progress_every",
}


def _mapping(value, field, keys):
    if not isinstance(value, dict):
        raise ConfigError("{} 必须为对象".format(field))
    if any(key not in keys for key in value):
        raise ConfigError("{} 包含未知配置键".format(field))
    return value


def _json_value(value, field):
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float and math.isfinite(value):
        return
    if isinstance(value, list):
        for item in value:
            _json_value(item, field)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for item in value.values():
            _json_value(item, field)
        return
    raise ConfigError("{} 必须只包含标准 JSON 值".format(field))


def _validate_templates(value, allowed, field):
    if isinstance(value, str):
        for match in TEMPLATE_VARIABLE.finditer(value):
            if match.group(1).strip() not in allowed:
                raise ConfigError("{} 包含不支持的模板变量".format(field))
        residue = TEMPLATE_VARIABLE.sub("", value)
        if "{{" in residue or "}}" in residue:
            raise ConfigError("{} 模板括号不完整".format(field))
    elif isinstance(value, dict):
        for item in value.values():
            _validate_templates(item, allowed, field)
    elif isinstance(value, list):
        for item in value:
            _validate_templates(item, allowed, field)


def _expand_environment(value, field):
    if not isinstance(value, str):
        raise ConfigError("{} 必须为字符串".format(field))

    def replace(match):
        name = match.group(1)
        expanded = os.environ.get(name)
        if not expanded:
            raise ConfigError("{} 的环境变量 {} 未设置或为空".format(field, name))
        return expanded

    return _ENVIRONMENT.sub(replace, value)


def load_ai_config(path) -> AIConfig:
    """加载 UTF-8 YAML 和同目录可选 .env，返回独立的固定配置快照。

    配置或模板不合法时抛 ConfigError；此阶段不计算任务配置摘要，
    不访问任务表，也不将配置值写入日志。
    """
    logger.debug("开始加载并校验配置")
    config_path = Path(path)
    try:
        with config_path.open(encoding="utf-8") as handle:
            document = yaml.load(handle, Loader=_UniqueLoader)
    except (yaml.YAMLError, UnicodeError) as exc:
        raise ConfigError("配置无法作为安全 UTF-8 YAML 解析") from exc
    document = _mapping(document, "配置", {"prompts", "api", "run"})
    # 进程环境优先；仅补充同目录 .env 中尚未设置的变量。
    load_dotenv(dotenv_path=config_path.parent / ".env", override=False)
    prompts = _mapping(document.get("prompts"), "prompts", {"system", "user"})
    if any(not isinstance(prompts.get(name), str) for name in ("system", "user")):
        raise ConfigError("prompts.system/user 必须为字符串")
    api = _mapping(document.get("api"), "api", {"url", "headers", "payload", "trust_env"})
    url = _expand_environment(api.get("url"), "api.url")
    try:
        parts = urlsplit(url)
        _ = parts.port
        valid_url = (parts.scheme in ("http", "https") and bool(parts.hostname)
                     and parts.username is None and parts.password is None
                     and not any(character.isspace() for character in parts.netloc))
    except ValueError:
        valid_url = False
    if not valid_url:
        raise ConfigError("api.url 必须为不含用户名密码的绝对 HTTP/HTTPS 地址")
    headers = api.get("headers", {})
    if not isinstance(headers, dict) or any(not isinstance(k, str) for k in headers):
        raise ConfigError("api.headers 必须为字符串字典")
    headers = {key: _expand_environment(value, "api.headers") for key, value in headers.items()}
    trust_env = api.get("trust_env", False)
    if type(trust_env) is not bool:
        raise ConfigError("api.trust_env 必须为布尔值")
    payload = api.get("payload")
    if not isinstance(payload, dict):
        raise ConfigError("api.payload 必须为 JSON 对象")
    _json_value(payload, "api.payload")
    if "stream" in payload and payload["stream"] is not False:
        raise ConfigError("api.payload.stream 只允许布尔 false")
    defaults = RunConfig().__dict__
    run = _mapping(document.get("run", {}), "run", set(defaults))
    values = dict(defaults, **run)
    for name, value in values.items():
        if name in _INTEGER_FIELDS:
            valid = type(value) is int and value > 0
        else:
            valid = (type(value) in (int, float) and math.isfinite(value)
                     and (value >= 0 if name == "requests_per_second" else value > 0))
        if not valid:
            raise ConfigError("run.{} 的数值或类型无效".format(name))
    if values["result_buffer_size"] < values["batch_size"]:
        raise ConfigError("result_buffer_size 不能小于 batch_size")
    if values["backoff_max_seconds"] < values["backoff_base_seconds"]:
        raise ConfigError("backoff_max_seconds 不能小于 backoff_base_seconds")
    # 先校验模板变量，确保未知字段在任何任务状态更新前报错。
    for prompt in prompts.values():
        _validate_templates(prompt, {"task.text"}, "prompts")
    _validate_templates(payload, {"task.text", "prompts.system", "prompts.user"}, "api.payload")
    try:
        json.dumps({"prompts": prompts, "payload": payload}, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, TypeError, UnicodeError) as exc:
        raise ConfigError("提示词或 payload 无法编码为 UTF-8 JSON") from exc
    logger.debug("配置校验完成：并发上限=%d，结果批次=%d，单任务尝试上限=%d",
                 values["concurrency"], values["batch_size"], values["max_attempts"])
    return AIConfig(Prompts(**prompts), ApiConfig(url, headers, copy.deepcopy(payload), trust_env), RunConfig(**values))


def ai_config_hash(config: AIConfig) -> str:
    """认领时计算处理版本，只包含原始提示词和 AI 调用参数。"""
    # 使用渲染前模板；不同帖子共享同一摘要，运行参数和凭证不参与。
    identity = {
        "prompts": {"system": config.prompts.system, "user": config.prompts.user},
        "api.payload": config.api.payload,
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def require_prompts(config):
    if not config.prompts.system.strip() or not config.prompts.user.strip():
        raise ValueError("提示词尚未配置，请先填写该轮 AI 配置")


@dataclass(frozen=True)
class EventConfig:
    first_round_task_path: str
    result_path: str
    round2_ai_config_path: str
    round3_ai_config_path: str
    start_time: str = None
    end_time: str = None
    user_name_to_tickers: dict = None
    retriever: str = ""
    initialize_mode: str = "create"
    logging: dict = None
    embedding_config_path: str = ""
    history_lookback_months: int = 3
    weight_path: str = ""
    weight_grid: dict = None


def load_event_config(path):
    """安全加载并拒绝重复键，按配置目录解析 AI 路径，数据系统路径原样保留。"""
    path = Path(path).absolute()
    document = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueLoader)
    if not isinstance(document, dict):
        raise ValueError("扩展配置必须为字典")
    allowed = set(EventConfig.__dataclass_fields__)
    if set(document) - allowed:
        raise ValueError("扩展配置包含未知字段：{}".format(sorted(set(document) - allowed)))
    for name in ("first_round_task_path", "result_path", "round2_ai_config_path", "round3_ai_config_path"):
        if not isinstance(document.get(name), str) or not document[name].strip():
            raise ValueError("{} 必须为非空字符串".format(name))
    if document["first_round_task_path"] == document["result_path"]:
        raise ValueError("第一轮表与结果表路径必须不同")
    if not isinstance(document.get("weight_path", ""), str):
        raise ValueError("weight_path 必须为字符串，可留空供权重入口运行前填写")
    if "weight_grid" in document and not isinstance(document["weight_grid"], dict):
        raise ValueError("weight_grid 必须为字典")
    start, end = document.get("start_time"), document.get("end_time")
    if (start is None) != (end is None):
        raise ValueError("起止日期必须同时设置")
    if start is not None:
        if not isinstance(start, str) or not isinstance(end, str):
            raise ValueError("起止日期必须为 YYYY-MM-DD 字符串")
        if date.fromisoformat(start).isoformat() != start or date.fromisoformat(end).isoformat() != end:
            raise ValueError("起止日期必须为 YYYY-MM-DD 字符串")
        if start > end:
            raise ValueError("起点不能晚于终点")
    mapping = document.get("user_name_to_tickers", {})
    if not isinstance(mapping, dict):
        raise ValueError("user_name_to_tickers 必须为字典")
    for user, tickers in mapping.items():
        if not isinstance(user, str) or not user.strip() or not isinstance(tickers, list) or not tickers:
            raise ValueError("账号映射必须为非空用户名到非空代币列表")
        if any(not isinstance(ticker, str) or not ticker.strip() for ticker in tickers):
            raise ValueError("映射代币必须为非空字符串")
    if not isinstance(document.get("retriever", ""), str):
        raise ValueError("retriever 必须为字符串")
    if not isinstance(document.get("embedding_config_path", ""), str):
        raise ValueError("embedding_config_path 必须为字符串")
    months = document.get("history_lookback_months", 3)
    if type(months) is not int or months <= 0:
        raise ValueError("history_lookback_months 必须为正整数日历月数")
    if document.get("initialize_mode", "create") not in ("auto", "create", "resume"):
        raise ValueError("initialize_mode 只能为 auto、create 或 resume")
    if not isinstance(document.get("logging", {}), dict):
        raise ValueError("logging 必须为字典")
    values = dict(document, user_name_to_tickers=mapping, logging=document.get("logging", {}))
    for name in ("round2_ai_config_path", "round3_ai_config_path"):
        values[name] = str((path.parent / document[name]).absolute())
    if document.get("embedding_config_path"):
        values["embedding_config_path"] = str((path.parent / document["embedding_config_path"]).absolute())
    return EventConfig(**values)


@dataclass(frozen=True)
class EmbeddingConfig:
    """纯向量检索的固定配置；model_version 由部署者在更换权重时更新。"""

    base_url: str
    api_key: str = field(repr=False)
    model: str = "bge-m3"
    model_version: str = "bge-m3-v1"
    batch_size: int = 8
    timeout_seconds: float = 120
    max_attempts: int = 3
    backoff_base_seconds: float = 1
    backoff_max_seconds: float = 30
    trust_env: bool = False
    similarity_threshold: float = 0.50


def load_embedding_config(path):
    """加载向量接口及筛选阈值；.env 以本配置目录为准。"""
    path = Path(path).absolute()
    try:
        document = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueLoader)
    except (yaml.YAMLError, UnicodeError) as exc:
        raise ConfigError("Embedding 配置无法作为 UTF-8 YAML 解析") from exc
    document = _mapping(document, "Embedding 配置", set(EmbeddingConfig.__dataclass_fields__))
    load_dotenv(dotenv_path=path.parent / ".env", override=False)
    values = {name: document.get(name, definition.default)
              for name, definition in EmbeddingConfig.__dataclass_fields__.items()}
    for name in ("base_url", "api_key", "model", "model_version"):
        if not isinstance(values[name], str) or not values[name].strip():
            raise ConfigError("Embedding {} 必须为非空字符串".format(name))
    values["api_key"] = _expand_environment(values["api_key"], "Embedding api_key")
    if any(character in values["api_key"] for character in "\r\n"):
        raise ConfigError("Embedding api_key 不能含换行")
    try:
        parts = urlsplit(values["base_url"])
        _ = parts.port
        valid_url = (parts.scheme in ("http", "https") and bool(parts.hostname)
                     and parts.username is None and parts.password is None
                     and not parts.query and not parts.fragment
                     and not any(character.isspace() for character in values["base_url"]))
    except ValueError:
        valid_url = False
    if not valid_url:
        raise ConfigError("Embedding base_url 必须为不含认证、查询或片段的绝对 HTTP/HTTPS 地址")
    values["base_url"] = values["base_url"].rstrip("/")
    for name in ("batch_size", "max_attempts", "timeout_seconds", "backoff_base_seconds", "backoff_max_seconds"):
        value = values[name]
        valid = (type(value) is int and value > 0) if name in ("batch_size", "max_attempts") else (
            type(value) in (int, float) and math.isfinite(value) and value > 0)
        if not valid:
            raise ConfigError("Embedding {} 的数值或类型无效".format(name))
    if values["backoff_max_seconds"] < values["backoff_base_seconds"]:
        raise ConfigError("Embedding backoff_max_seconds 不能小于 backoff_base_seconds")
    if type(values["trust_env"]) is not bool:
        raise ConfigError("Embedding trust_env 必须为布尔值")
    threshold = values["similarity_threshold"]
    if type(threshold) not in (int, float) or not math.isfinite(threshold) or not -1 <= threshold <= 1:
        raise ConfigError("Embedding similarity_threshold 必须为 [-1, 1] 内的有限数值")
    return EmbeddingConfig(**values)


def load_job_config(path, required_fields):
    """安全加载并拒绝重复键，检查入口必填字段；其他约束由入口继续检查。"""
    settings = yaml.load(Path(path).read_text(encoding="utf-8"), Loader=_UniqueLoader)
    if not isinstance(settings, dict):
        raise ValueError("任务配置必须为字典")
    for field in required_fields:
        value = settings.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError("任务配置的 {} 必须为非空字符串".format(field))
    return settings
