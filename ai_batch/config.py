"""加载固定配置快照，仅根据提示词和单帖 text 构造请求。"""

import copy
import hashlib
import json
import logging
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlsplit

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class ConfigError(ValueError):
    """启动配置不符合约定；不在错误说明中暴露配置值。"""


class TaskRenderError(ValueError):
    """单帖请求无法构造为合法的 UTF-8 JSON。"""


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
class Config:
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
_VARIABLE = re.compile(r"\{\{(.*?)\}\}", re.DOTALL)
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
        for match in _VARIABLE.finditer(value):
            if match.group(1).strip() not in allowed:
                raise ConfigError("{} 包含不支持的模板变量".format(field))
        residue = _VARIABLE.sub("", value)
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


def load_config(path) -> Config:
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
    logger.debug("配置校验完成：并发上限=%d，结果批次=%d，单帖尝试上限=%d",
                 values["concurrency"], values["batch_size"], values["max_attempts"])
    return Config(Prompts(**prompts), ApiConfig(url, headers, copy.deepcopy(payload), trust_env), RunConfig(**values))


def _render(value, variables):
    """递归替换配置中的字符串值；插入的文本不再进行第二次解析。"""
    if isinstance(value, str):
        return _VARIABLE.sub(lambda match: variables[match.group(1).strip()], value)
    if isinstance(value, list):
        return [_render(item, variables) for item in value]
    if isinstance(value, dict):
        return {key: _render(item, variables) for key, item in value.items()}
    return value


def render_payload(config: Config, text: str) -> Dict[str, Any]:
    """根据单帖 text 返回独立请求对象，不修改固定配置快照。

    任务键和 user_name 不参与请求构造。模板只替换一次；结果无法编码为
    标准 UTF-8 JSON 时抛 TaskRenderError，由运行模块作为该帖最终失败处理。
    """
    if not isinstance(text, str):
        raise TaskRenderError("text 必须为字符串")
    try:
        variables = {"task.text": text}
        system = _render(config.prompts.system, variables)
        user = _render(config.prompts.user, variables)
        # 先渲染提示词，再将已渲染文本原样插入 payload。
        variables.update({"prompts.system": system, "prompts.user": user})
        payload = _render(config.api.payload, variables)
        json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        return payload
    except (ValueError, TypeError, UnicodeError, KeyError) as exc:
        raise TaskRenderError("请求无法按模板构造为 UTF-8 JSON") from exc


def config_hash(config: Config) -> str:
    """认领时计算处理版本，只包含原始提示词和 AI 调用参数。"""
    # 使用渲染前模板；不同帖子共享同一摘要，运行参数和凭证不参与。
    identity = {
        "prompts": {"system": config.prompts.system, "user": config.prompts.user},
        "api.payload": config.api.payload,
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
