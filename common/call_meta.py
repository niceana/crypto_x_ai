"""本地调用统计；与 AI 正文、用量及服务端 created 分开保存。"""

import json
from datetime import datetime, timedelta, timezone

POST_META_COLUMNS = ["round1_call_meta"]
EVENT_META_COLUMNS = ["round2_call_meta", "round3_call_meta"]


def local_time():
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="milliseconds")


class CallRecorder:
    """仅在一次作业内累积；请求数在 HTTP 实际起发前递增。"""

    def __init__(self):
        self.started_at = None
        self.attempts = 0
        self.reasons = []

    def start_attempt(self):
        if self.started_at is None:
            self.started_at = local_time()
        self.attempts += 1

    def failure(self, reason, http_status=None, will_retry=False):
        self.reasons.append({"attempt": self.attempts, "time": local_time(),
                             "http_status": http_status, "will_retry": bool(will_retry),
                             "reason": str(reason).split("\nuser=", 1)[0]})

    def finish(self):
        return {"started_at": self.started_at, "finished_at": local_time(),
                "attempts": self.attempts, "retry_count": max(self.attempts - 1, 0),
                "retry_reasons": [dict(item) for item in self.reasons]}


def no_call(source_record_id=None):
    result = {"started_at": None, "finished_at": local_time(), "attempts": 0,
              "retry_count": 0, "retry_reasons": []}
    if source_record_id is not None:
        result["source_record_id"] = source_record_id
    return result


def merge_call_meta(previous, current):
    """覆盖本次统计，累计请求次数并续写原因；不向 AI 请求插入这些字段。"""
    if current is None:
        return previous
    old = {} if previous == "" else json.loads(previous)
    if not isinstance(old, dict):
        raise ValueError("call_meta 必须为 JSON 对象")
    previous_attempts = old.get("attempts_total", old.get("attempts", 0))
    if type(previous_attempts) is not int or previous_attempts < 0:
        raise ValueError("call_meta 的累计请求次数必须为非负整数")
    old_reasons = old.get("retry_reasons", [])
    if not isinstance(old_reasons, list):
        raise ValueError("call_meta 的 retry_reasons 必须为数组")
    result = dict(current)
    result["attempts_total"] = previous_attempts + current["attempts"]
    result["retry_count_total"] = max(result["attempts_total"] - 1, 0)
    additions = [dict(item, attempt=previous_attempts + item["attempt"])
                 for item in current["retry_reasons"]]
    result["retry_reasons"] = old_reasons + additions
    return json.dumps(result, ensure_ascii=False, allow_nan=False)


def with_meta_columns(frame, columns):
    """旧快照的未知统计留空；不修改调用方的 DataFrame。"""
    result = frame.copy(deep=True)
    for name in columns:
        if name not in result:
            result[name] = ""
        else:
            result[name] = result[name].where(result[name].notna(), "")
    return result
