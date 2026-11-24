"""媒体配置命名适配；三轮继续使用原有配置对象和处理函数。"""

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from common.config import ConfigError, EventConfig, event_config_from_mapping, load_job_config


MEDIA_OBJECTS = frozenset(("INDUSTRY", "MACRO"))


@dataclass(frozen=True)
class MediaEventConfig(EventConfig):
    # 仅限制业务 JSON 的字符数，不等同模型 token 上限；None 交由接口判断。
    round2_max_input_characters: int = None


def _check_keys(document, allowed):
    extra = set(document) - set(allowed)
    if extra:
        raise ConfigError("媒体配置包含未知字段：{}".format(sorted(extra)))


def load_media_job_config(path, *, require_ai=True):
    """将公开的 round1_result_path 转为现有第一轮入口的内部 task_path。"""
    path = Path(path).absolute()
    required = ("round1_result_path",) + (("ai_config_path",) if require_ai else ())
    document = load_job_config(path, required)
    _check_keys(document, ("source_path", "json_files", "round1_result_path", "ai_config_path",
                           "start_time", "end_time", "logging"))
    source_path = document.get("source_path", "")
    files = document.get("json_files", [])
    if not isinstance(source_path, str):
        raise ConfigError("source_path 必须为字符串；使用本地 JSON 时留空")
    if not isinstance(files, list) or any(not isinstance(value, str) or not value.strip() for value in files):
        raise ConfigError("json_files 必须为文件路径字符串组成的列表")
    if bool(source_path.strip()) == bool(files):
        raise ConfigError("source_path 与 json_files 必须且只能填写一项")
    if source_path == document["round1_result_path"]:
        raise ConfigError("媒体源帖子表与第一轮结果表路径必须不同")
    start, end = document.get("start_time"), document.get("end_time")
    if (start is None) != (end is None):
        raise ConfigError("起止日期必须同时设置")
    if start is not None:
        if not isinstance(start, str) or not isinstance(end, str):
            raise ConfigError("起止日期必须为 YYYY-MM-DD 字符串")
        if date.fromisoformat(start).isoformat() != start or date.fromisoformat(end).isoformat() != end:
            raise ConfigError("起止日期必须为 YYYY-MM-DD 字符串")
        if start > end:
            raise ConfigError("起点不能晚于终点")
    if not isinstance(document.get("logging", {}), dict):
        raise ConfigError("logging 必须为字典")
    values = dict(document)
    values["source_path"] = source_path
    if files:
        values["json_files"] = [str((path.parent / Path(value).expanduser()).resolve()) for value in files]
    values["task_path"] = values.pop("round1_result_path")
    if require_ai:
        values["ai_config_path"] = str((path.parent / document["ai_config_path"]).absolute())
    return values


def load_media_event_config(path):
    """第二、三轮同表；不允许媒体配置引入账号映射或向量筛选。"""
    path = Path(path).absolute()
    document = load_job_config(path, ("round1_result_path", "round2_result_path", "round3_result_path",
                                      "round2_ai_config_path", "round3_ai_config_path"))
    _check_keys(document, ("round1_result_path", "round2_result_path", "round3_result_path",
                           "round2_ai_config_path", "round3_ai_config_path", "start_time", "end_time",
                           "history_lookback_months", "round2_asset_list_path", "round3_skip_incomplete_round2",
                           "round2_max_input_characters", "logging"))
    if document["round2_result_path"] != document["round3_result_path"]:
        raise ConfigError("媒体第二、三轮必须使用同一张结果表")
    limit = document.get("round2_max_input_characters")
    if limit is not None and (type(limit) is not int or limit <= 0):
        raise ConfigError("round2_max_input_characters 必须为正整数或 null")
    if not isinstance(document.get("round2_asset_list_path", ""), str):
        raise ConfigError("round2_asset_list_path 必须为字符串")
    values = {key: value for key, value in document.items() if key not in (
        "round1_result_path", "round2_result_path", "round3_result_path", "round2_max_input_characters")}
    values.update(first_round_task_path=document["round1_result_path"],
                  result_path=document["round2_result_path"], user_name_to_tickers={},
                  embedding_config_path="", retriever="round2.retrieval:select_all_groups")
    values.setdefault("history_lookback_months", 1)
    base = event_config_from_mapping(values, path)
    return MediaEventConfig(**base.__dict__, round2_max_input_characters=limit)


def load_media_target_assets(cli, settings):
    """可选交易名单外始终保留行业、宏观对象；未设名单时处理所有对象。"""
    if not settings.round2_asset_list_path:
        return None
    document = json.loads(cli.read_object(settings.round2_asset_list_path).decode("utf-8"))
    instruments = document["InstrumentList"]
    if not isinstance(instruments, list) or any(not isinstance(value, str) or not value.strip()
                                              for value in instruments):
        raise ValueError("InstrumentList 必须为非空字符串组成的列表")
    return {value.split(".", 1)[0].strip().upper() for value in instruments} | MEDIA_OBJECTS
