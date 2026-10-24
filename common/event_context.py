"""组织三轮业务输入、第一轮账号背景与第二轮事件历史索引。"""

import json
from bisect import bisect_left
from collections import defaultdict

from common.time_rules import history_window_start, published_time


def prepare_account_tickers(mapping):
    """固定本轮账号背景；用户名忽略大小写，币种去空白、转大写并去重。"""
    if mapping is None:
        return {}
    if not isinstance(mapping, dict):
        raise ValueError("user_name_to_tickers 必须为字典")
    result = {}
    for user_name, tickers in mapping.items():
        if not isinstance(user_name, str) or not user_name.strip() or not isinstance(tickers, list) or not tickers:
            raise ValueError("账号背景必须为非空用户名到非空代币列表")
        account = user_name.casefold()
        if account in result:
            raise ValueError("账号背景包含忽略大小写后的重复用户名：{}".format(user_name))
        if any(not isinstance(ticker, str) or not ticker.strip() for ticker in tickers):
            raise ValueError("账号背景代币必须为非空字符串")
        result[account] = list(dict.fromkeys(ticker.strip().upper() for ticker in tickers))
    return result


def build_extraction_input(row, account_tickers):
    """第一轮依次发送账号、可选关联币种及正文，将账号背景放在一起。"""
    current = {"user_name": row["user_name"]}
    tickers = account_tickers.get(row["user_name"].casefold())
    if tickers:
        current["account_tickers"] = tickers
    current["text"] = row["text"]
    return json.dumps(current, ensure_ascii=False, allow_nan=False)


class EventHistory:
    """保留round2事件去重归组完成记录的内存索引，每次查询再应用当前发布时间边界。

    records 按币保存有序的 (时间, record_id, 记录)，用于二分定位候选窗口；
    groups 保存已载入的组事实，启用 truncate_group_history 时只输出窗口内事实
    并标记截断。加入索引前要求保存已确认，返回记录均为副本，评分函数不能改内部历史。
    """
    def __init__(self, frame=None, history_lookback_months=3, truncate_group_history=False):
        self.history_lookback_months = history_lookback_months
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
        """按配置日历月数二分定位同币历史窗口，只复制左闭右开区间内的事实。"""
        stamp = published_time(current_time)
        begin = history_window_start(stamp, self.history_lookback_months)
        records = self.records[asset]
        # 仅用时间前缀查边界：包括下界全部同刻记录，排除上界全部同刻记录。
        left = bisect_left(records, (begin,))
        right = bisect_left(records, (stamp,))
        return tuple(dict(records[position][2]) for position in range(left, right))

    def build_group_history(self, asset, event_id, current_time):
        """按当前披露时刻组织组事实；启用截断时缺起点仍保留窗口内历史。"""
        stamp = published_time(current_time)
        begin = history_window_start(stamp, self.history_lookback_months) if self.truncate_group_history else None
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
    """为第二轮内部检索组织固定事件事实和定位字段。"""
    return {"record_id": key[1], "date": str(key[0]), "asset": row["asset"],
            "event": row["event"], "source_post_id": row["source_post_id"]}


def build_relation_input(current, candidates):
    """将原始 date 索引以 published_at 发送，返回第二轮输入及候选组 ID 映射。

    候选已经完成同币、时间窗口筛选并排好顺序。缺少起点时保留最早可用
    披露；它若同时也是首条 update，按披露 ID 排除重复，不按文本合并事实。
    每组依候选顺序编号 E1、E2 等；调用方将文本与映射固定在同一请求中，
    重试复用，不修改内部记录。第三轮不使用组历史。
    """
    groups, candidate_event_ids = [], {}
    for index, candidate in enumerate(candidates, start=1):
        candidate_id = "E{}".format(index)
        candidate_event_ids[candidate_id] = candidate["event_id"]
        first = candidate["first_disclosure"] or candidate["first_available_disclosure"]
        records = [first] + candidate["timeline"]
        seen, events = set(), []
        for record in records:
            if record["record_id"] not in seen:
                seen.add(record["record_id"])
                events.append({"published_at": record["date"], "event": record["event"]})
        groups.append({"candidate_id": candidate_id, "history": events})
    text = json.dumps({"current": {"published_at": current["date"], "event": current["event"]}, "candidates": groups},
                      ensure_ascii=False, allow_nan=False)
    return text, candidate_event_ids


def build_impact_input(store, key):
    """认领后组织代币和事件，仅在新增信息有内容时附带该字段。"""
    row = store.df.loc[key]
    current = {"asset": row["asset"], "event": row["event"]}
    information = row["new_information"]
    if information and information.strip():
        current["new_information"] = information
    return json.dumps(current, ensure_ascii=False, allow_nan=False)
