"""维护事件历史索引，读取任务所需原帖正文，组织round2事件去重归组与round3事件影响分析业务输入。"""

import json
from collections import defaultdict
from datetime import date

import pandas as pd

from common.validation import validate_event_source_index
from common.time_rules import published_time


class EventHistory:
    """保留round2事件去重归组完成记录的内存索引，每次查询再应用当前发布时间边界。

    records 用于窗口召回，groups 用于回查组的完整前史；加入索引前要求保存
    已确认，所有返回记录都是副本，评分函数不能修改内部历史。
    """
    def __init__(self, frame=None):
        self.records = defaultdict(list)
        self.groups = defaultdict(list)
        self.known = set()
        if frame is not None:
            self.add_frame(frame)

    def add_frame(self, frame):
        """逐段加入已完成关联的必要字段，不保留 DataFrame 或行视图。"""
        for key, row in frame.iterrows():
            if row["round2_status"] == "1":
                self.add(key, row)

    def add(self, key, row):
        """只在持久保存确认后加入；重复加入同一行不改变历史。"""
        if key[1] in self.known:
            return
        if row["round2_status"] != "1":
            raise ValueError("未完成关联的记录不能进入历史")
        record = {"record_id": key[1], "date": str(key[0]), "event": row["event"],
                  "asset": row["asset"], "event_id": row["event_id"],
                  "event_relation": row["event_relation"],
                  "new_information": row["new_information"] or None}
        stamp = published_time(key[0])
        self.records[row["asset"]].append((stamp, record))
        self.groups[(row["asset"], row["event_id"])].append((stamp, record))
        self.known.add(key[1])

    def get_searchable_records(self, asset, current_time):
        """返回同资产前 180 天的记录副本，保留中途更新的事实。"""
        stamp = published_time(current_time)
        begin = stamp - pd.Timedelta(days=180)
        values = [(time, record) for time, record in self.records[asset] if begin <= time < stamp]
        values.sort(key=lambda item: (item[0], item[1]["record_id"]))
        return tuple(dict(record) for _, record in values)

    def build_group_history(self, asset, event_id, current_time):
        """回查窗口外起点；候选只包含当前时点前已披露的组内事实。"""
        stamp = published_time(current_time)
        values = sorted(((time, record) for time, record in self.groups[(asset, event_id)] if time < stamp),
                        key=lambda item: (item[0], item[1]["record_id"]))
        if not values:
            raise ValueError("事件组不存在或不在当前时点以前：{}".format(event_id))
        starts = [record for _, record in values if record["event_relation"] == "new"]
        if len(starts) != 1:
            raise ValueError("事件组必须有唯一建档起点：{}".format(event_id))
        return {"event_id": event_id, "asset": asset, "first_disclosure": dict(starts[0]),
                "timeline": [dict(record) for _, record in values if record["event_relation"] == "update"],
                "repeat_count": sum(record["event_relation"] == "repeat" for _, record in values),
                "disclosure_count": len(values), "history_trimmed": False}


def build_current_record_input(key, row):
    """组织固定事件事实和定位字段；第二轮无需回读原帖。"""
    return {"record_id": key[1], "date": str(key[0]), "asset": row["asset"],
            "event": row["event"], "source_post_id": row["source_post_id"]}


def load_source_texts(store, keys):
    """为第三轮待分析任务查找原文，不展开第一轮结果或核对任务覆盖。

    只返回任务实际使用的正文；已完成、repeat 或空分段无需读原帖。
    按导入时保留的日期和原帖 ID 定位；缺少对应原帖或 text 列时在认领前
    停止，正文单元格原样交给业务输入构造，不在读取阶段检查内容。
    """
    if not keys:
        return {}
    start = store.start_time or store.settings.start_time
    end = store.end_time or store.settings.end_time
    if start is None:
        source = store.cli.read(store.settings.first_round_task_path)
    else:
        source = store.cli.read(store.settings.first_round_task_path, start_time=start, end_time=end)
    validate_event_source_index(source)
    if start is not None:
        begin, finish = date.fromisoformat(start), date.fromisoformat(end)
        if any(not begin <= published_time(key[0]).date() <= finish for key in source.index):
            raise ValueError("原帖读取返回了分段日期以外的行")
    if "text" not in source.columns:
        raise ValueError("原帖缺少 text 字段")
    positions = {(raw_date, str(raw_id)): i for i, (raw_date, raw_id) in enumerate(source.index)}
    texts = {}
    for key in keys:
        post_id = store.df.loc[key, "source_post_id"]
        source_key = (key[0], post_id)
        if source_key not in positions:
            raise ValueError("任务对应原帖不存在：{}，source_post_id={}".format(key[1], post_id))
        if post_id in texts:
            continue
        text = source.iloc[positions[source_key]]["text"]
        texts[post_id] = text
    return texts


def build_relation_input(current, candidates):
    """将已经选定的当前记录和候选历史序列化，不重新检索或改变候选。"""
    return json.dumps({"current": current, "candidates": candidates}, ensure_ascii=False, allow_nan=False)


def build_impact_input(store, key, source_texts, history):
    """认领确认后逐条组织业务输入，不预存整月正文和历史输入副本。"""
    row = store.df.loc[key]
    current = build_current_record_input(key, row)
    current.update(source_text=source_texts[row["source_post_id"]],
                   event_relation=row["event_relation"], new_information=row["new_information"] or None)
    previous = (history.build_group_history(row["asset"], row["event_id"], key[0])
                if row["event_relation"] == "update" else None)
    return json.dumps({"current": current, "previous_history": previous},
                      ensure_ascii=False, allow_nan=False)
