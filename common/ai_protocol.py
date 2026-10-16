"""三轮大模型请求体构造与接口响应解析；网络请求及重试由网络模块负责。"""

import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from common.config import AIConfig, TEMPLATE_VARIABLE

logger = logging.getLogger(__name__)
_BEIJING_TIMEZONE = timezone(timedelta(hours=8))
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class TaskRenderError(ValueError):
    """任务输入无法构造为合法的 UTF-8 JSON。"""


def _render(value, variables):
    """递归替换配置中的字符串值；插入的文本不再进行第二次解析。"""
    if isinstance(value, str):
        return TEMPLATE_VARIABLE.sub(lambda match: variables[match.group(1).strip()], value)
    if isinstance(value, list):
        return [_render(item, variables) for item in value]
    if isinstance(value, dict):
        return {key: _render(item, variables) for key, item in value.items()}
    return value


def render_payload(config: AIConfig, text: str) -> Dict[str, Any]:
    """根据任务输入文本 text 返回独立请求对象，不修改固定配置快照。

    任务键和 user_name 不参与请求构造。模板只替换一次；结果无法编码为
    标准 UTF-8 JSON 时抛 TaskRenderError，由运行模块作为该任务最终失败处理。
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


@dataclass(frozen=True)
class ParsedResponse:
    """单次最终解析结果；生成时间为北京时间文本，不包含响应原文或重试计划。"""

    success: bool
    http_status: Optional[int]
    ai_response_content: str = ""
    reasoning_content: str = ""
    usage: Any = None
    response_created: str = ""
    error_message: str = ""


def _reason(value):
    """保证失败原因可用 UTF-8 保存，并限制为最多 512 个字符。"""
    value = value.encode("utf-8", errors="replace").decode("utf-8")
    return value if len(value) <= 512 else value[:511] + "…"


def request_failure(error_message: str) -> ParsedResponse:
    """封装未取得 HTTP 响应的失败，结果字段保持为空。"""
    return ParsedResponse(False, None, error_message=_reason(error_message))


def _reject_constant(value):
    raise ValueError("响应包含非标准 JSON 数值")


def _format_response_created(value) -> str:
    """将数值 Unix 秒转为北京时间文本；缺失留空，格式或范围错误抛 ValueError。"""
    if value is None:
        return ""
    if type(value) not in (int, float):
        raise ValueError("created 必须为 Unix 秒时间戳数值")
    try:
        # 按秒取整，再从固定 UTC 起点计算，避免本机时区和系统时间戳范围影响。
        created = (_UNIX_EPOCH + timedelta(seconds=math.floor(value))).astimezone(_BEIJING_TIMEZONE)
    except (OverflowError, ValueError) as exc:
        raise ValueError("created 超出可转换的日期范围") from exc
    return created.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")


def _error_message(body, non_success):
    """按接口消息优先级提取说明，不猜测厂商错误码的含义。"""
    error = body.get("error")
    if isinstance(error, dict):
        message = error.get("message")
        if isinstance(message, str) and message.strip():
            return message
    if isinstance(error, str) and error.strip():
        return error
    message = body.get("message")
    if non_success and isinstance(message, str) and message.strip():
        return message
    return ""


def parse_response(http_status: int, raw_response: str) -> ParsedResponse:
    """解析本次完整响应文本，返回保留实际 HTTP 状态的 ParsedResponse。

    完整有效的 AI 回答才判成功；预期解析、结构或编码失败作为失败结果返回。
    原始响应只用于本次解析，不进入结果或日志；重试和全局停止由运行模块决定。
    """
    content = ""
    reasoning = ""
    usage = None
    created = ""
    reason = ""
    non_success = not 200 <= http_status < 300
    try:
        body = json.loads(raw_response, parse_constant=_reject_constant)
        # 额外拒绝 1e999 等解析后成为非有限数值的 JSON。
        json.dumps(body, allow_nan=False)
    except (ValueError, UnicodeError, RecursionError) as exc:
        logger.debug("响应 JSON 解析失败：HTTP状态=%d，异常类型=%s",
                     http_status, type(exc).__name__)
        message = "HTTP {}".format(http_status) if non_success else str(exc)
        return ParsedResponse(False, http_status, error_message=_reason(message))
    if not isinstance(body, dict):
        logger.debug("响应结构无效：HTTP状态=%d，JSON根值不是对象", http_status)
        message = "HTTP {}".format(http_status) if non_success else "响应 JSON 根值不是对象"
        return ParsedResponse(False, http_status, error_message=_reason(message))
    usage, raw_created = body.get("usage"), body.get("created")
    error = body.get("error")
    explicit_error = error is not None and error != ""
    # HTTP 失败与明确 error 优先，不能被 body 中的正常 choices 掩盖。
    if non_success:
        message = _error_message(body, True)
        reason = "HTTP {}{}".format(http_status, "：" + message if message else "")
    elif explicit_error:
        reason = _error_message(body, False) or "接口返回错误，但未提供失败说明"
    else:
        choices = body.get("choices")
        if not isinstance(choices, list) or not choices or any(not isinstance(item, dict) for item in choices):
            reason = "choices 必须为非空对象列表"
        else:
            # 按编号选择而非列表位置；bool 不作为合法的整数编号。
            selected = [item for item in choices if type(item.get("index")) is int and item["index"] == 0]
            if len(selected) != 1:
                reason = "choices 必须包含唯一的整数 index=0"
            else:
                choice = selected[0]
                message = choice.get("message")
                if not isinstance(message, dict):
                    reason = "message 必须为对象"
                else:
                    raw_content = message.get("content")
                    raw_reasoning = message.get("reasoning_content")
                    content = raw_content if isinstance(raw_content, str) else ""
                    reasoning = raw_reasoning if isinstance(raw_reasoning, str) else ""
                    provider = message.get("provider_specific_fields")
                    if message.get("role") != "assistant":
                        reason = "message.role 必须为 assistant"
                    elif raw_content is not None and not isinstance(raw_content, str):
                        reason = "message.content 必须为字符串或 null"
                    elif raw_reasoning is not None and not isinstance(raw_reasoning, str):
                        reason = "message.reasoning_content 必须为字符串或 null"
                    elif provider is not None and not isinstance(provider, dict):
                        reason = "provider_specific_fields 必须为对象或 null"
                    else:
                        refusal = (provider or {}).get("refusal")
                        finish = choice.get("finish_reason")
                        if (refusal is not None and refusal != "") or finish == "content_filter":
                            reason = refusal if isinstance(refusal, str) and refusal.strip() else "AI 拒绝生成内容"
                        elif finish == "length":
                            reason = "回答被截断"
                        elif finish != "stop":
                            reason = "finish_reason 不是 stop"
                        elif not content.strip():
                            reason = "AI 正文为空"
    # 失败也可保留安全提取的最终字段，但不可编码的值必须清空。
    for name, value in (("ai_response_content", content), ("reasoning_content", reasoning),
                        ("usage", usage)):
        try:
            if name == "usage":
                json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
            else:
                value.encode("utf-8")
        except (ValueError, TypeError, UnicodeError) as exc:
            logger.debug("响应字段编码失败：HTTP状态=%d，字段=%s，异常类型=%s",
                         http_status, name, type(exc).__name__)
            if name == "ai_response_content":
                content = ""
            elif name == "reasoning_content":
                reasoning = ""
            else:
                usage = None
            if not reason:
                reason = "{} 无法编码：{}".format(name, str(exc))
    try:
        created = _format_response_created(raw_created)
    except ValueError as exc:
        logger.debug("响应生成时间转换失败：HTTP状态=%d，异常类型=%s",
                     http_status, type(exc).__name__)
        # 时间不合法时整条 AI 结果无效，保存空值以覆盖同一任务的旧结果。
        content = reasoning = ""
        usage = None
        if not reason:
            reason = "response_created 无法转换：{}".format(str(exc))
    logger.debug("响应解析完成：HTTP状态=%d，结果=%s", http_status, "失败" if reason else "成功")
    return ParsedResponse(not bool(reason), http_status, content, reasoning, usage, created, _reason(reason))
