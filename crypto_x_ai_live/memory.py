"""仅在本次函数内存在的表适配器；兼容原 Store 的读写协议，不连接外部存储。"""

import pandas as pd

from . import _compat as core

POSTS = "live_posts"
EVENTS = "live_events"


def empty_events():
    index = pd.MultiIndex.from_arrays([pd.DatetimeIndex([]), []], names=["date", "record_id"])
    return pd.DataFrame(index=index, columns=core.validation.RESULT_COLUMNS + core.call_meta.EVENT_META_COLUMNS,
                        dtype=object)


class MemoryTables:
    def __init__(self, posts, events, control=None):
        self.control = control
        self.tables = {POSTS: posts.copy(deep=True),
                       EVENTS: core.call_meta.with_meta_columns(events, core.call_meta.EVENT_META_COLUMNS)}
        self.changed_events = set()

    def read(self, path, start_time=None, end_time=None, columns=None):
        if self.control is not None:
            self.control.check()
        frame = self.tables[path]
        if start_time not in (None, ""):
            begin, end = pd.Timestamp(start_time).date(), pd.Timestamp(end_time).date()
            frame = frame.loc[[begin <= stamp.date() <= end for stamp in frame.index.get_level_values("date")]]
        return frame.copy(deep=True) if columns is None else frame.loc[:, columns].copy(deep=True)

    def read_meta(self, path):
        if self.control is not None:
            self.control.check()
        dates = self.tables[path].index.get_level_values("date")
        return {"meta": {"start_time": dates.min().isoformat() if len(dates) else None,
                         "end_time": dates.max().isoformat() if len(dates) else None}}

    def update_by_column(self, path, patch):
        if self.control is not None:
            self.control.check()
        frame = self.tables[path]
        if not patch.index.is_unique or not patch.index.is_monotonic_increasing:
            raise ValueError("内存更新的索引必须唯一递增")
        if not patch.index.isin(frame.index).all() or not set(patch.columns).issubset(frame.columns):
            raise ValueError("内存更新引用不存在的行或列")
        changed = ~frame.loc[patch.index, patch.columns].eq(patch).all(axis=1)
        frame.loc[patch.index, patch.columns] = patch.to_numpy()
        if path == EVENTS:
            self.changed_events.update(patch.index[changed])

    def merge_events(self, incoming):
        incoming = core.call_meta.with_meta_columns(incoming, core.call_meta.EVENT_META_COLUMNS)
        old = self.tables[EVENTS]
        self._check_events(incoming)
        additions = incoming.loc[~incoming.index.isin(old.index)]
        additions = core.event_records._only_events_after_latest(additions, old)
        self.tables[EVENTS] = pd.concat([old, additions]).sort_index()
        self.changed_events.update(additions.index)
        return additions.index

    def _check_events(self, incoming):
        core.validation.validate_event_table_structure(incoming)
        old = self.tables[EVENTS]
        positions = {key[1]: key for key in old.index}
        for key in incoming.index:
            if key[1] in positions and positions[key[1]] != key:
                raise ValueError("同一 record_id 对应不同 date：{}".format(key[1]))
        common = incoming.index.intersection(old.index)
        columns = core.validation.INPUT_COLUMNS
        if len(common) and not old.loc[common, columns].equals(incoming.loc[common, columns]):
            raise ValueError("已有事件的固定输入与本次第一轮展开结果冲突")

    def event_changes(self):
        frame = self.tables[EVENTS]
        return frame.loc[frame.index.isin(self.changed_events)].copy(deep=True).sort_index()
