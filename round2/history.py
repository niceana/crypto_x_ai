"""第二轮前史及事件组引用校验；只使用已读取的数据，不读取或修改数据库。"""

from common.time_rules import history_window_start, published_time


def _stored_relation_error(key, row, frame, record_positions, origin_start=None):
    """用位置索引检查组引用，返回具体失败原因；合法时返回 None。

    传入 origin_start 时允许未读到或早于下界的组起点，其引用无法核实，
    必须在业务输入中标记历史截断。
    """
    relation, event_id = row["event_relation"], row["event_id"]
    if relation == "new":
        return None if event_id == key[1] else "new 的 event_id 必须等于自身 record_id"
    if relation not in ("update", "repeat"):
        return "event_relation 必须为 new、update 或 repeat"
    if origin_start is not None and (not isinstance(event_id, str) or not event_id.strip()):
        return "update/repeat 的 event_id 必须为非空字符串"
    position = record_positions.get(event_id)
    if position is None:
        return None if origin_start is not None else "未找到事件组起点"
    root_key, origin = frame.index[position], frame.iloc[position]
    if origin_start is not None and published_time(root_key[0]) < origin_start:
        # 不回查或核实窗口外起点，保留已保存组 ID 并在输入中标记截断。
        return None
    problem = None
    if origin["round2_status"] != "1":
        problem = "组起点第二轮状态未成功"
    elif origin["event_relation"] != "new":
        problem = "引用记录不是 new 组起点，可能存在链式 event_id；请核查分组引用数据"
    elif origin["event_id"] != root_key[1]:
        problem = "组起点的 event_id 不等于自身 record_id"
    elif origin["asset"] != row["asset"]:
        problem = "组起点与当前前史资产不同"
    elif published_time(root_key[0]) >= published_time(key[0]):
        problem = "组起点时间未严格早于当前前史"
    if problem is not None:
        return ("{}；引用记录={} / {}，asset={!r}，round2_status={!r}，"
                "event_relation={!r}，event_id={!r}").format(
                    problem, root_key[0], root_key[1], origin["asset"], origin["round2_status"],
                    origin["event_relation"], origin["event_id"])
    return None


def validate_relation_history(store, keys, history_lookback_months=3, later_records=None):
    """用内存位置索引检查指定月数内的同币前史，保留更晚完成冲突检查。

    ID 字典和按币分组只保存原行位置；不复制完整表，不增加数据库查询。
    窗口内较早记录须全部完成；窗口外组起点不再核实，输入另行标记截断。
    later_records 仅含处理范围之后的资产与状态，用于保留补录冲突检查。
    """
    earliest, begins = {}, {}
    for key in keys:
        if store.df.at[key, "round2_status"] != "1":
            asset = store.df.at[key, "asset"]
            stamp = published_time(key[0])
            begin = history_window_start(stamp, history_lookback_months)
            if asset not in begins or begin < begins[asset]:
                begins[asset] = begin
            if asset not in earliest or stamp < earliest[asset][0]:
                earliest[asset] = stamp, key
    if not earliest:
        return
    record_positions = {}
    rows_by_asset = {asset: [] for asset in earliest}
    for frame in (store.df, later_records):
        if frame is None:
            continue
        for position, (key, asset) in enumerate(zip(frame.index, frame["asset"])):
            if frame is store.df:
                record_positions[key[1]] = position
            if asset in rows_by_asset and published_time(key[0]) >= begins[asset]:
                rows_by_asset[asset].append((frame, position))
    for asset, (stamp, pending_key) in earliest.items():
        # 较早任务只检查历史窗口；更晚已完成仍识别为补录或状态缺口。
        for frame, position in rows_by_asset[asset]:
            old_key, row = frame.index[position], frame.iloc[position]
            old_stamp = published_time(old_key[0])
            if old_stamp > stamp and row["round2_status"] == "1":
                raise ValueError("疑似历史补录或历史状态缺口，请核查：asset={}，待处理={} / {}，更晚已完成={} / {}".format(
                    asset, pending_key[0], pending_key[1], old_key[0], old_key[1]))
            if old_stamp < stamp:
                if row["round2_status"] != "1":
                    label, problem = "所需前史未完成，请手动补跑", "round2_status 未成功"
                else:
                    label = "所需前史关联结果不合法，请核查"
                    problem = _stored_relation_error(
                        old_key, row, store.df, record_positions=record_positions, origin_start=begins[asset])
                if problem is None:
                    continue
                context = ("asset={}，前史={} / {}，round2_status={!r}，event_relation={!r}，"
                           "event_id={!r}，待处理={} / {}").format(
                               asset, old_key[0], old_key[1], row["round2_status"],
                               row["event_relation"], row["event_id"], pending_key[0], pending_key[1])
                raise ValueError("{}：{}；原因={}".format(label, context, problem))
