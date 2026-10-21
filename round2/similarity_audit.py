"""只读复算已完成事件的向量候选，导出供人工观察的 CSV，不调用模型或保存业务结果。"""

import csv
import json
import logging
import math
from bisect import bisect_left
from collections import defaultdict
from contextlib import ExitStack
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from common.event_context import EventHistory
from common.store import read_event_history
from common.time_rules import history_read_bounds, history_window_start, published_time
from common.validation import parse_strict_json, validate_index
from round2.embedding import EmbeddingError, _validate_unit_vector

logger = logging.getLogger(__name__)

READ_COLUMNS = ["asset", "event", "event_id", "event_relation", "new_information",
                "round2_status", "embedding"]
EVENT_COLUMNS = [
    "event", "max_similarity_event", "max_similarity", "asset", "date", "record_id",
    "max_similarity_record_id", "max_similarity_event_id", "max_similarity_date",
    "stored_relation", "stored_event_id", "new_information",
    "history_records", "history_groups", "threshold", "recomputed_decision", "qualifying_groups", "groups_sent_to_ai",
    "stored_match_rank", "stored_match_similarity", "manual_has_historical_match", "manual_note",
]
CANDIDATE_COLUMNS = [
    "current_date", "current_record_id", "asset", "current_event", "stored_relation", "stored_event_id",
    "candidate_rank", "candidate_event_id", "similarity", "best_record_id", "best_record_date",
    "best_record_event", "passes_threshold", "would_send_to_ai", "is_stored_match",
    "manual_same_matter", "manual_note",
]
THRESHOLD_COLUMNS = [
    "threshold", "events", "no_history", "below_threshold_auto_new", "send_to_ai_events",
    "candidate_groups_sent", "stored_update_repeat_events", "stored_matches_excluded",
    "stored_matches_missing_from_history",
]


def _threshold(value):
    if type(value) not in (int, float) or not math.isfinite(value) or not -1 <= value <= 1:
        raise ValueError("相似度阈值必须为 [-1, 1] 内的有限数字")
    return float(value)


def _read_batch(cli, settings):
    if not settings.start_time or not settings.end_time:
        raise ValueError("请用 start_time/end_time 指定刚跑完这一批的日期范围")
    if date.fromisoformat(settings.start_time) > date.fromisoformat(settings.end_time):
        raise ValueError("start_time 不能晚于 end_time")
    if settings.retriever:
        raise ValueError("本工具仅复算内置纯向量检索，不能重放自定义 retriever")
    current = read_event_history(cli, settings.result_path, settings.start_time, settings.end_time,
                                 columns=READ_COLUMNS, label="观察批次读取")
    unfinished = current.loc[current["round2_status"] != "1"]
    if not unfinished.empty:
        raise ValueError("本批第二轮尚未全部完成，不能作为已完成批次复算：{}".format(unfinished.index[0][1]))
    keys = sorted(current.index, key=lambda key: (published_time(key[0]), key[1]))
    bounds = history_read_bounds((key[0] for key in keys), settings.start_time,
                                 settings.history_lookback_months)
    if bounds is None:
        return current, keys
    begin, end = bounds
    history = read_event_history(cli, settings.result_path, begin.isoformat(), end.isoformat(),
                                 columns=READ_COLUMNS, label="观察前史读取")
    combined = pd.concat([history, current])
    validate_index(combined, "record_id")
    return combined, keys


class _SavedVectors:
    """只解析实际参与比较的表内向量；不从接口补向量，也不重新归一化。"""

    def __init__(self):
        self.by_record = {}
        self.by_json = {}
        self.identity = None
        self.dimension = None

    def get(self, record_id, raw):
        if record_id in self.by_record:
            return self.by_record[record_id]
        if not isinstance(raw, str) or not raw:
            raise ValueError("记录缺少已保存的 embedding：{}；本工具不会调用模型补齐".format(record_id))
        if raw not in self.by_json:
            try:
                body = parse_strict_json(raw)
                if (not isinstance(body, dict)
                        or set(body) != {"model", "model_version", "config_hash", "vector"}
                        or any(not isinstance(body[name], str) or not body[name]
                               for name in ("model", "model_version", "config_hash"))):
                    raise ValueError("embedding 对象结构不合法")
                identity = tuple(body[name] for name in ("model", "model_version", "config_hash"))
                vector = _validate_unit_vector(body["vector"])
                if self.identity is not None and identity != self.identity:
                    raise ValueError("参与复算的向量混用了模型、版本或配置摘要")
                if self.dimension is not None and len(vector) != self.dimension:
                    raise ValueError("参与复算的向量维度不一致")
            except (ValueError, TypeError, RecursionError, EmbeddingError) as exc:
                raise ValueError("无法复算记录 {}：{}".format(record_id, exc)) from exc
            self.identity, self.dimension = identity, len(vector)
            self.by_json[raw] = vector
        self.by_record[record_id] = self.by_json[raw]
        return self.by_record[record_id]


def _rank_groups(current, searchable, vectors, embeddings):
    """沿用正式检索的 float64 内积、截断和组内最高分；并保留最高分来源。"""
    vector = vectors.get(current["record_id"], embeddings[current["record_id"]])
    if not searchable:
        return []
    matrix = np.stack([vectors.get(row["record_id"], embeddings[row["record_id"]]) for row in searchable])
    scores = np.clip(matrix @ vector, -1, 1)
    groups = {}
    for row, score in zip(searchable, scores):
        event_id = row["event_id"]
        if event_id not in groups or score > groups[event_id]["similarity"]:
            groups[event_id] = {"event_id": event_id, "similarity": float(score), "record": row}
    return sorted(groups.values(), key=lambda group: (-group["similarity"], group["event_id"]))


def _writer(stack, path, columns):
    handle = stack.enter_context(path.open("w", encoding="utf-8-sig", newline=""))
    writer = csv.DictWriter(handle, fieldnames=columns)
    writer.writeheader()
    return writer


def export_similarity_audit(cli, settings, output_dir, *, threshold=0.5, top_k=10,
                            thresholds=(0.4, 0.45, 0.5, 0.55, 0.6)):
    """读取指定批次和必要前史，导出所有当前记录及每条记录阈值筛选前的 Top K 组。

    cli 只须提供 read；不使用 Store，不恢复、认领或保存状态。thresholds 只改变
    候选筛选统计，始终固定使用表内已有分组，不模拟改阈值后的整轮 AI 决策。
    """
    threshold = _threshold(threshold)
    sweep = sorted({_threshold(value) for value in thresholds} | {threshold})
    if type(top_k) is not int or top_k <= 0:
        raise ValueError("top_k 必须为正整数")
    if type(settings.history_lookback_months) is not int or settings.history_lookback_months <= 0:
        raise ValueError("history_lookback_months 必须为正整数")
    output = Path(output_dir).absolute()
    if output.exists():
        raise ValueError("输出目录已存在，请指定新目录，避免覆盖观察结果：{}".format(output))
    frame, keys = _read_batch(cli, settings)
    assets = set(frame.loc[keys, "asset"]) if keys else set()
    frame = frame.loc[frame["asset"].isin(assets)]
    unfinished = defaultdict(list)
    for key, row in frame.iterrows():
        if not isinstance(key[1], str) or not key[1]:
            raise ValueError("record_id 必须为非空字符串")
        if row["round2_status"] != "1":
            unfinished[row["asset"]].append((published_time(key[0]), key[1]))
        elif (not isinstance(row["event"], str) or not row["event"].strip()
              or not isinstance(row["event_id"], str) or not row["event_id"].strip()
              or row["event_relation"] not in ("new", "update", "repeat")):
            raise ValueError("已完成记录缺少合法的事件或关联信息：{}".format(key[1]))
    for entries in unfinished.values():
        entries.sort()
    # 不把最终表内的未来记录带入当前时点；EventHistory 每次查询都排除同刻和未来。
    history = EventHistory(frame, settings.history_lookback_months)
    embeddings = dict(zip(frame.index.get_level_values("record_id"), frame["embedding"]))
    vectors = _SavedVectors()
    totals = {value: dict.fromkeys(THRESHOLD_COLUMNS[1:], 0) for value in sweep}
    output.mkdir(parents=True)
    pair_count = 0
    with ExitStack() as stack:
        event_writer = _writer(stack, output / "events.csv", EVENT_COLUMNS)
        pair_writer = _writer(stack, output / "candidates.csv", CANDIDATE_COLUMNS)
        for position, key in enumerate(keys, 1):
            row = frame.loc[key]
            current = {"date": str(key[0]), "record_id": key[1], "asset": row["asset"], "event": row["event"]}
            bad = unfinished[row["asset"]]
            begin = history_window_start(key[0], settings.history_lookback_months)
            left, right = bisect_left(bad, (begin,)), bisect_left(bad, (published_time(key[0]),))
            if left < right:
                raise ValueError("所需同币前史尚未完成，无法重建本批候选：{} / {}".format(row["asset"], bad[left][1]))
            searchable = history.get_searchable_records(row["asset"], key[0])
            groups = _rank_groups(current, searchable, vectors, embeddings)
            matched = next(((rank, group["similarity"]) for rank, group in enumerate(groups, 1)
                            if group["event_id"] == row["event_id"]), None)
            is_match = row["event_relation"] in ("update", "repeat")
            for cutoff, counts in totals.items():
                eligible = sum(group["similarity"] >= cutoff for group in groups)
                counts["events"] += 1
                counts["no_history"] += int(not groups)
                counts["below_threshold_auto_new"] += int(bool(groups) and eligible == 0)
                counts["send_to_ai_events"] += int(eligible > 0)
                counts["candidate_groups_sent"] += min(10, eligible)
                counts["stored_update_repeat_events"] += int(is_match)
                counts["stored_matches_missing_from_history"] += int(is_match and matched is None)
                counts["stored_matches_excluded"] += int(is_match and (
                    matched is None or matched[0] > 10 or matched[1] < cutoff))
            eligible = sum(group["similarity"] >= threshold for group in groups)
            event = dict(current, stored_relation=row["event_relation"], stored_event_id=row["event_id"],
                         new_information=row["new_information"], history_records=len(searchable),
                         history_groups=len(groups), threshold=threshold, qualifying_groups=eligible,
                         groups_sent_to_ai=min(10, eligible),
                         recomputed_decision=("no_history_auto_new" if not groups else
                                              "send_to_ai" if eligible else "below_threshold_auto_new"))
            if groups:
                best = groups[0]
                event.update(max_similarity=best["similarity"], max_similarity_event_id=best["event_id"],
                             max_similarity_record_id=best["record"]["record_id"],
                             max_similarity_date=best["record"]["date"], max_similarity_event=best["record"]["event"])
            if is_match and matched:
                event.update(stored_match_rank=matched[0], stored_match_similarity=matched[1])
            event_writer.writerow(event)
            for rank, group in enumerate(groups[:top_k], 1):
                best = group["record"]
                pair_writer.writerow({
                    "current_date": current["date"], "current_record_id": key[1], "asset": row["asset"],
                    "current_event": row["event"], "stored_relation": row["event_relation"],
                    "stored_event_id": row["event_id"], "candidate_rank": rank,
                    "candidate_event_id": group["event_id"], "similarity": group["similarity"],
                    "best_record_id": best["record_id"], "best_record_date": best["date"],
                    "best_record_event": best["event"], "passes_threshold": group["similarity"] >= threshold,
                    "would_send_to_ai": rank <= 10 and group["similarity"] >= threshold,
                    "is_stored_match": is_match and group["event_id"] == row["event_id"],
                })
                pair_count += 1
            if position % 100 == 0 or position == len(keys):
                logger.info("相似度只读复算进度：%d/%d", position, len(keys))
        threshold_writer = _writer(stack, output / "thresholds.csv", THRESHOLD_COLUMNS)
        for cutoff, counts in totals.items():
            threshold_writer.writerow(dict(threshold=cutoff, **counts))
    manifest = {
        "completed": True, "result_path": settings.result_path,
        "start_time": settings.start_time, "end_time": settings.end_time,
        "history_lookback_months": settings.history_lookback_months,
        "threshold": threshold, "thresholds": sweep, "top_k": top_k,
        "events": len(keys), "candidate_rows": pair_count,
        "embedding_identity": (dict(zip(("model", "model_version", "config_hash"), vectors.identity))
                               if vectors.identity else None),
        "threshold_counts": [dict(threshold=cutoff, **counts) for cutoff, counts in totals.items()],
        "notes": [
            "只读复算，未调用模型、未写数据库；缺失或不兼容的向量会报错，不自动补齐。",
            "使用当前表内历史和分组，排除同刻及未来；历史补录、向量、分组或窗口变更会影响还原。",
            "按日期范围观察全部已完成记录；没有运行 ID，不能单独识别本次新完成的记录。",
            "Top K 在阈值筛选前导出，包含低于阈值的组；同分来源记录取最早日期和 record_id。",
            "thresholds.csv 固定既有分组，只估算筛选量；AI 既有判断不是人工真值，不代表准确率或召回率。",
            "send_to_ai_events 是待调用的事件数量，不包含重试，也不是实际发生的 HTTP 请求数。",
        ],
    }
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
