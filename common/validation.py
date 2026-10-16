"""两张表的字段和结构约定，以及三轮各自的输出校验；保留不同校验口径。"""

import json
from datetime import date, timedelta
from numbers import Integral

import pandas as pd

from common.persistence import TaskValidationError
from common.time_rules import published_time


POST_RESULT_COLUMNS = ["status", "ai_response_content", "reasoning_content", "usage_json",
                       "response_created", "error_message"]


TASK_COLUMNS = ["config_hash", "text", "user_name"] + POST_RESULT_COLUMNS


RESULT_COLUMNS = ["event_id", "event", "asset", "source_post_id", "event_relation",
                  "new_information", "round2_ai_response", "round3_ai_response", "round2_status", "round2_config_hash",
                  "round3_status", "round3_config_hash", "error_message", "embedding"]


HASH_COLUMNS = ["round2_config_hash", "round3_config_hash"]


IMPACT_FIELDS = ["direction", "materiality", "certainty", "persistence", "materiality_reason"]


INPUT_COLUMNS = ["event", "asset", "source_post_id"]


RELATION_COLUMNS = ["event_id", "event_relation", "new_information"]


def validate_index(frame, id_name, *, error_type=ValueError):
    """检查 date 与指定 ID 组成的双层索引，以及索引、ID 和列名唯一性。

    error_type 由调用方指定，保留各轮已有的异常类型。
    日期格式、ID 内容和普通列的业务规则由调用方继续检查。
    """
    if not isinstance(frame, pd.DataFrame):
        raise error_type("输入必须为 DataFrame")
    if not isinstance(frame.index, pd.MultiIndex) or list(frame.index.names) != ["date", id_name]:
        raise error_type("索引必须为名称和顺序固定的 date/{} MultiIndex".format(id_name))
    if not frame.columns.is_unique or not frame.index.is_unique:
        raise error_type("索引和列名必须唯一")
    if not frame.index.get_level_values(id_name).is_unique:
        raise error_type("同一 {} 不允许对应多行或不同 date".format(id_name))


def validate_post_index(df):
    """共用结构校验后，检查round1事件提取的日期、ID 内容及同名列一致性。"""
    validate_index(df, "id", error_type=TaskValidationError)
    dates = df.index.get_level_values("date")
    ids = df.index.get_level_values("id")
    if dates.isna().any() or any(not isinstance(value, str) or not value.strip() for value in ids):
        raise TaskValidationError("date 不能缺失，id 必须为非空白字符串")
    for name, values in (("date", dates), ("id", ids)):
        if name in df.columns and not all(a == b for a, b in zip(df[name], values)):
            raise TaskValidationError("索引字段与同名列不一致")


def validate_post_strings(df, columns):
    if any(name not in df.columns for name in columns):
        raise TaskValidationError("任务缺少必要列")
    for name in columns:
        if any(not isinstance(value, str) for value in df[name]):
            raise TaskValidationError("{} 的所有单元格必须为字符串".format(name))


def validate_task_range(df, start_time, end_time):
    """校验包含起止日全天的范围，保留原日期类型和时区。"""
    if start_time is None:
        return
    dates = df.index.get_level_values("date")
    try:
        # 下一日日界线只用于校验，cli 的截止日期仍原样传递。
        end_next_day = (date.fromisoformat(end_time) + timedelta(days=1)).isoformat()
        if not ((dates >= start_time) & (dates < end_next_day)).all():
            raise TaskValidationError("cli.read 返回了当前日期范围外的任务")
    except TypeError as exc:
        raise TaskValidationError("读取结果与日期范围的类型或时区不兼容") from exc


def validate_post_task_structure(df):
    """运行端只检查表结构；数据库普通列原值保留。"""
    validate_post_index(df)
    if set(df.columns) != set(TASK_COLUMNS):
        raise TaskValidationError("任务表必须恰好包含约定的九个普通列")


def validate_event_source_index(source):
    if not isinstance(source, pd.DataFrame) or not isinstance(source.index, pd.MultiIndex):
        raise ValueError("第一轮结果必须为 DataFrame 和 date/id 索引")
    if list(source.index.names) != ["date", "id"] or not source.index.is_unique or not source.columns.is_unique:
        raise ValueError("第一轮索引须为唯一的 date/id，列名不能重复")
    normalized = set()
    for raw_date, raw_id in source.index:
        published_time(raw_date)
        if isinstance(raw_id, bool) or not isinstance(raw_id, (str, Integral)) or not str(raw_id).strip() or ":" in str(raw_id):
            raise ValueError("原帖 ID 必须为非空字符串或整数，不能含冒号")
        post_id = str(raw_id)
        if post_id in normalized:
            raise ValueError("原帖 ID 重复：{}".format(post_id))
        normalized.add(post_id)


def validate_event_table_structure(frame):
    """仅检查事件去重分析表结构和原索引；不检查数据库普通列内容。"""
    validate_index(frame, "record_id")
    if set(frame.columns) != set(RESULT_COLUMNS):
        raise ValueError("结果表须恰好包含约定的 {} 个普通列；缺少={}，多出={}".format(
            len(RESULT_COLUMNS), sorted(set(RESULT_COLUMNS) - set(frame.columns)), sorted(set(frame.columns) - set(RESULT_COLUMNS))))
    ids = frame.index.get_level_values("record_id")
    if any(not isinstance(value, str) or not value for value in ids):
        raise ValueError("record_id 必须为全表唯一的非空字符串")
    for raw_date in frame.index.get_level_values("date"):
        published_time(raw_date)


class OutputValidationError(ValueError):
    """已识别的业务输出错误，可在同一请求预算内重试。"""


def parse_strict_json(text):
    """拒绝重复字段、非标准数值和非法文本，避免同一回答有不同解释。"""
    def reject(value):
        raise ValueError("包含非标准 JSON 数值")

    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("JSON 字段重复：{}".format(key))
            result[key] = value
        return result

    value = json.loads(text, parse_constant=reject, object_pairs_hook=object_pairs)
    json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    return value


def validate_extraction_output(content):
    """解析并验证 events/event/tickers，返回对象但不改写原回答。

    空事件列表和空代币列表均合法；账号映射、代币规范化及事实判断不在这里处理。
    可预期的格式错误统一转为 OutputValidationError，供网络层计入同一请求预算。
    """
    try:
        body = parse_strict_json(content)
        if not isinstance(body, dict) or set(body) != {"events"} or not isinstance(body["events"], list):
            raise ValueError("第一轮 JSON 必须只包含 events 数组")
        for event in body["events"]:
            if not isinstance(event, dict) or set(event) != {"event", "tickers"}:
                raise ValueError("事件须包含 event 和 tickers")
            if not isinstance(event["event"], str) or not event["event"].strip() or not isinstance(event["tickers"], list):
                raise ValueError("事件正文须非空，tickers 须为数组")
            if any(not isinstance(ticker, str) or not ticker.strip() for ticker in event["tickers"]):
                raise ValueError("tickers 中的代币须为非空字符串")
        return body
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise OutputValidationError(str(exc)) from exc


def validate_relation_output(content, candidate_event_ids):
    """校验 AI 的请求内短编号，再返回带真实 matched_event_id 的内部结果。

    candidate_event_ids 是本任务首次提交时固定的短编号到真实组 ID 映射，
    重试不重新编号，不接受 AI 返回真实长 ID，也不改写待保存的原始正文。
    """
    try:
        value = parse_strict_json(content)
        if not isinstance(value, dict) or set(value) != {"event_relation", "matched_candidate_id", "new_information"}:
            raise ValueError("第二轮须恰好返回三个关联字段")
        relation, matched, info = value["event_relation"], value["matched_candidate_id"], value["new_information"]
        if relation not in ("new", "update", "repeat", "uncertain"):
            raise ValueError("event_relation 枚举不合法")
        if relation in ("new", "uncertain"):
            if matched is not None or info is not None:
                raise ValueError("new/uncertain 的匹配 ID 和新增信息须为 null")
        else:
            if not isinstance(matched, str) or matched not in candidate_event_ids:
                raise ValueError("matched_candidate_id 不属于本次合法候选")
            if relation == "update" and (not isinstance(info, str) or not info.strip()):
                raise ValueError("update 须有非空新增信息")
            if relation == "repeat" and info is not None:
                raise ValueError("repeat 的新增信息须为 null")
        return {"event_relation": relation,
                "matched_event_id": candidate_event_ids[matched] if matched is not None else None,
                "new_information": info}
    except (ValueError, TypeError, UnicodeError) as exc:
        raise OutputValidationError(str(exc)) from exc


def validate_impact_output(content):
    """先校验五字段结构和文本类型；具体评分枚举可通过调用接口替换校验。"""
    try:
        value = parse_strict_json(content) if isinstance(content, str) else content
        if not isinstance(value, dict) or set(value) != set(IMPACT_FIELDS):
            raise ValueError("第三轮须恰好返回五个经济影响字段")
        if any(not isinstance(value[name], str) or not value[name].strip() for name in IMPACT_FIELDS):
            raise ValueError("第三轮经济影响字段须为非空字符串")
        json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
        return dict(value)
    except (ValueError, TypeError, UnicodeError) as exc:
        raise OutputValidationError(str(exc)) from exc
