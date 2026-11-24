"""从本地 JSON/JSONL 读取媒体原帖，校验并生成第一轮初始化输入。"""

import json
import logging
from numbers import Integral
from pathlib import Path

import pandas as pd

from common.validation import validate_post_index, validate_post_strings


logger = logging.getLogger(__name__)


def _json_lines(stream, path):
    for number, line in enumerate(stream, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError("{} 第 {} 行 JSON 语法错误：{}".format(path, number, exc.msg)) from exc
        yield "{} 第 {} 行".format(path, number), record


def _records(path):
    with path.open(encoding="utf-8-sig") as stream:
        if path.suffix.lower() in (".jsonl", ".ndjson"):
            yield from _json_lines(stream, path)
            return
        try:
            document = json.load(stream)
        except json.JSONDecodeError as exc:
            if exc.msg != "Extra data":
                raise ValueError("{} 第 {} 行 JSON 语法错误：{}".format(path, exc.lineno, exc.msg)) from exc
            # 兼容扩展名为 .json、实际每行一条帖子的文件。
            stream.seek(0)
            yield from _json_lines(stream, path)
            return
    records = document if isinstance(document, list) else [document]
    for number, record in enumerate(records, 1):
        yield "{} 第 {} 条".format(path, number), record


def _post(record):
    if not isinstance(record, dict):
        raise ValueError("每条帖子必须为 JSON 对象")
    post_id = record["id"]
    if isinstance(post_id, Integral) and not isinstance(post_id, bool):
        post_id = str(post_id)
    if not isinstance(post_id, str) or not post_id.strip():
        raise ValueError("id 须为非空字符串或整数，不能为浮点数、布尔值或空值")
    created_at = record["created_at"]
    if not isinstance(created_at, str) or not created_at.strip():
        raise ValueError("created_at 须为非空发布时间字符串")
    stamp = pd.Timestamp(created_at)
    if pd.isna(stamp):
        raise ValueError("created_at 不能为空时间")
    stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
    text, user_name = record["text"], record["user"]["screen_name"]
    if not isinstance(text, str) or not isinstance(user_name, str) or not user_name.strip():
        raise ValueError("text 须为字符串，user.screen_name 须为非空字符串")
    return {"date": stamp.tz_localize(None), "id": post_id, "text": text, "user_name": user_name}


def load_media_json_posts(files):
    """过滤转发和快讯前缀后导入 JSON；相同任务输入合并，同 ID 冲突则停止。"""
    if (not isinstance(files, (list, tuple)) or not files
            or any(not isinstance(value, (str, Path)) or not str(value).strip() for value in files)):
        raise ValueError("json_files 须为至少包含一个本地 JSON/JSONL 文件路径的列表")
    posts, locations = {}, {}
    total = excluded = 0
    for filename in files:
        path = Path(filename).expanduser()
        file_rows = 0
        for location, record in _records(path):
            try:
                row = _post(record)
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                raise ValueError("{} 字段不合法：{}".format(location, exc)) from exc
            file_rows += 1
            if row["text"].startswith(("RT @", "JUST IN")):
                excluded += 1
                continue
            post_id = row["id"]
            if post_id in posts:
                fields = [name for name in ("date", "text", "user_name") if posts[post_id][name] != row[name]]
                if fields:
                    raise ValueError("JSON 同一 ID 的发布时间、正文或账号冲突：id={}，字段={}；{}；首次位置={}".format(
                        post_id, ", ".join(fields), location, locations[post_id]))
            else:
                posts[post_id], locations[post_id] = row, location
        total += file_rows
        logger.info("读取媒体 JSON：%s，记录=%d", path, file_rows)
    if not posts:
        raise ValueError("JSON 文件中没有可导入的帖子（为空或已全部过滤），未初始化任务表")
    source = pd.DataFrame(list(posts.values())).set_index(["date", "id"]).sort_index()
    validate_post_index(source)
    validate_post_strings(source, ["text", "user_name"])
    logger.info("媒体 JSON 校验完成：原始记录=%d，正文前缀剔除=%d，重复记录合并=%d，待初始化任务=%d；date 为 UTC 发布时间",
                total, excluded, total - excluded - len(source), len(source))
    return source
