"""维护事件历史索引，读取任务所需原帖正文，组织round2事件去重归组与round3事件影响分析业务输入。"""

import json
from bisect import bisect_left
from collections import defaultdict
from datetime import date

from common.validation import validate_event_source_index
from common.time_rules import history_window_start, published_time


class EventHistory:
    """保留round2事件去重归组完成记录的内存索引，每次查询再应用当前发布时间边界。

    records 按币保存有序的 (时间, record_id, 记录)，用于二分定位候选窗口；
    groups 保存已载入的组事实，启用 truncate_group_history 时只输出窗口内事实
    并标记截断。加入索引前要求保存已确认，返回记录均为副本，评分函数不能改内部历史。
    """
    def __init__(self, frame=None, history_lookback_years=5, truncate_group_history=False):
        self.history_lookback_years = history_lookback_years
        self.truncate_group_history = truncate_group_history
        self.records = defaultdict(list)
        self.groups = defaultdict(list)
        self.known = set()
        if frame is not None:
            self.add_frame(frame)

    def add_frame(self, frame):
        """批量登记必要字段，再按币排序，避免乱序载入时逐条移动有序列表。"""
        self.add_records(frame.iterrows())

    def add_records(self, records):
        """批量接收原键与必要字段，供分段读取后筛选的组历史一次建立有序索引。"""
        assets = set()
        for key, row in records:
            if row["round2_status"] == "1":
                entry = self._register_record(key, row)
                if entry is not None:
                    asset = row["asset"]
                    self.records[asset].append(entry)
                    assets.add(asset)
        for asset in assets:
            self.records[asset].sort(key=lambda entry: entry[:2])

    def add(self, key, row):
        """保存确认后加入有序列表；通常尾部追加，同刻乱序完成时二分插入。"""
        entry = self._register_record(key, row)
        if entry is None:
            return
        records = self.records[row["asset"]]
        if not records or records[-1][:2] < entry[:2]:
            records.append(entry)
        else:
            records.insert(bisect_left(records, entry[:2]), entry)

    def _register_record(self, key, row):
        """登记已完成事实及组历史，返回候选索引条目；重复记录返回 None。

        批量载入与运行中加入共用此步骤，记录字典由两种索引共同引用，
        不保留 DataFrame 行视图；候选列表各自采用批量排序或有序插入。
        """
        if key[1] in self.known:
            return None
        if row["round2_status"] != "1":
            raise ValueError("未完成关联的记录不能进入历史")
        record = {"record_id": key[1], "date": str(key[0]), "event": row["event"],
                  "asset": row["asset"], "event_id": row["event_id"],
                  "event_relation": row["event_relation"],
                  "new_information": row["new_information"] or None}
        stamp = published_time(key[0])
        self.groups[(row["asset"], row["event_id"])].append((stamp, record))
        self.known.add(key[1])
        return stamp, key[1], record

    def get_searchable_records(self, asset, current_time):
        """按配置日历年数二分定位同币历史窗口，只复制左闭右开区间内的事实。"""
        stamp = published_time(current_time)
        begin = history_window_start(stamp, self.history_lookback_years)
        records = self.records[asset]
        # 仅用时间前缀查边界：包括下界全部同刻记录，排除上界全部同刻记录。
        left = bisect_left(records, (begin,))
        right = bisect_left(records, (stamp,))
        return tuple(dict(records[position][2]) for position in range(left, right))

    def build_group_history(self, asset, event_id, current_time):
        """按当前披露时刻组织组事实；启用截断时缺起点仍保留窗口内历史。"""
        stamp = published_time(current_time)
        begin = history_window_start(stamp, self.history_lookback_years) if self.truncate_group_history else None
        values = sorted(((time, record) for time, record in self.groups[(asset, event_id)]
                         if time < stamp and (begin is None or time >= begin)),
                        key=lambda item: (item[0], item[1]["record_id"]))
        if not values:
            raise ValueError("事件组不存在或不在当前时点以前：{}".format(event_id))
        starts = [record for _, record in values if record["event_relation"] == "new"]
        if len(starts) > 1 or (not starts and not self.truncate_group_history):
            raise ValueError("事件组必须有唯一建档起点：{}".format(event_id))
        result = {"event_id": event_id, "asset": asset,
                  "first_disclosure": dict(starts[0]) if starts else None,
                  "timeline": [dict(record) for _, record in values if record["event_relation"] == "update"],
                  "repeat_count": sum(record["event_relation"] == "repeat" for _, record in values),
                  "disclosure_count": len(values), "history_trimmed": not starts}
        if not starts:
            # 仅剩 repeat 时也提供可比较事实，且不把它伪装成原始建档起点。
            result["first_available_disclosure"] = dict(values[0][1])
        return result


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
