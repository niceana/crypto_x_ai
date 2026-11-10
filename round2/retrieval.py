"""自定义检索接入与候选选择校验；内置纯向量评分见 embedding。"""

import importlib
from dataclasses import dataclass


class RetrievalNotConfigured(RuntimeError):
    """既未配置内置向量检索，也未提供自定义评分器。"""


@dataclass(frozen=True)
class RetrievalSelection:
    """评分实现返回按相关性排序的组 ID；自动判新须由已验证规则明确授权。"""

    event_ids: tuple = ()
    auto_new: bool = False
    reason: str = ""


def load_retriever(reference):
    if not reference:
        raise RetrievalNotConfigured("检索评分尚未配置；请设置 embedding_config_path、retriever=模块:函数，或传入检索函数")
    module, separator, name = reference.partition(":")
    if not separator or not module or not name:
        raise ValueError("retriever 必须为 模块:函数")
    function = getattr(importlib.import_module(module), name)
    if not callable(function):
        raise ValueError("retriever 必须指向可调用函数")
    return function


def select_all_groups(current, searchable):
    """提交窗口内全部不同事件组；空历史也交给 AI，不按分数自动判新。"""
    return RetrievalSelection(tuple(sorted({record["event_id"] for record in searchable})))


def validate_retrieval_selection(selection, available_groups, max_groups=10):
    """只检查检索接口结果，检索实现自身错误不作为 AI 输出重试。"""
    if not isinstance(selection, RetrievalSelection):
        raise ValueError("检索函数须返回 RetrievalSelection")
    ids = selection.event_ids
    if max_groups is not None and (type(max_groups) is not int or max_groups <= 0):
        raise ValueError("候选组数量上限须为正整数或 None")
    if (not isinstance(ids, tuple) or len(set(ids)) != len(ids)
            or (max_groups is not None and len(ids) > max_groups)):
        if max_groups == 10:
            raise ValueError("检索结果须为最多 10 个不同事件组的 ID 元组")
        raise ValueError("检索结果须为数量不超过配置上限的不同事件组 ID 元组")
    if type(selection.auto_new) is not bool or not isinstance(selection.reason, str):
        raise ValueError("检索自动判新标记或原因不合法")
    if any(not isinstance(value, str) or value not in available_groups for value in ids):
        raise ValueError("检索结果包含不存在、跨资产或超出历史窗口的事件组")
    if selection.auto_new and (ids or not selection.reason.strip()):
        raise ValueError("自动判新须无匹配候选并提供放行原因")
    return selection
