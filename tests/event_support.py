"""验证研究流程的历史边界、任务状态、重试与续跑；不访问真实服务。"""

import json
import threading
from datetime import date

import httpx
import pandas as pd

from common.network import NetworkWorker
from common.config import EventConfig
from round2.retrieval import RetrievalSelection
from round2.runner import run_relation
from initialization.event_records import expand_first_round
from tests.support import Fixture, success


def source_posts(posts):
    keys, rows = [], []
    for index, (when, event, tickers, user) in enumerate(posts):
        keys.append((pd.Timestamp(when), str(index + 100)))
        events = [{"event": event, "tickers": tickers}] if event is not None else []
        rows.append({"status": "1", "ai_response_content": json.dumps({"events": events}),
                     "text": event or "ordinary post", "user_name": user})
    return pd.DataFrame(rows, index=pd.MultiIndex.from_tuples(keys, names=["date", "id"]))


class DataNotFound(Exception):
    """模拟用户确认的 xdata 缺表标识。"""


class Tables:
    def __init__(self, source, result=None):
        self.tables = {"first": source.copy(deep=True)}
        if result is not None:
            self.tables["result"] = result.copy(deep=True)
        self.updates, self.writes, self.registers, self.threads = [], [], [], []
        self.appends = []
        self.update_hook = None
        self.write_hook = None
        self.append_hook = None
        self.reads = []
        self.column_reads = []
        self.read_hook = None
        self.meta_reads = []
        self.metadata = {}

    def read_meta(self, path):
        self.threads.append(threading.get_ident())
        self.meta_reads.append(path)
        if path in self.metadata:
            return self.metadata[path]
        if path not in self.tables:
            raise DataNotFound("missing table: {}".format(path))
        stamps = [pd.Timestamp(when) for when in self.tables[path].index.get_level_values(0)]
        return {"meta": {"start_time": min(stamps).isoformat() if stamps else None,
                         "end_time": max(stamps).isoformat() if stamps else None}}

    def read(self, path, start_time=None, end_time=None, columns=None):
        self.threads.append(threading.get_ident())
        if columns is None:
            self.reads.append((path, start_time, end_time))
            if self.read_hook:
                self.read_hook(path, start_time, end_time)
        else:
            self.column_reads.append((path, start_time, end_time, list(columns)))
        if path not in self.tables:
            raise DataNotFound("missing table: {}".format(path))
        frame = self.tables[path]
        if start_time not in (None, ""):
            start, end = date.fromisoformat(start_time), date.fromisoformat(end_time)
            frame = frame.loc[[start <= pd.Timestamp(when).date() <= end
                               for when in frame.index.get_level_values(0)]]
        if columns is not None:
            frame = frame[columns]
        return frame.copy(deep=True)

    def register(self, path, kind, description):
        if path in self.tables:
            raise ValueError("目标已注册")
        self.registers.append(path)
        self.tables[path] = pd.DataFrame()

    def write(self, path, frame):
        self.assert_sorted(frame)
        self.writes.append(frame.copy(deep=True))
        self.threads.append(threading.get_ident())
        if self.write_hook:
            self.write_hook(path, frame)
        else:
            self.tables[path] = frame.copy(deep=True)

    def update_by_column(self, path, frame):
        self.assert_sorted(frame)
        self.updates.append(frame.copy(deep=True))
        self.threads.append(threading.get_ident())
        if self.update_hook:
            self.update_hook(path, frame)
        else:
            self.apply(path, frame)

    def update_by_index(self, path, frame):
        self.assert_sorted(frame)
        if frame.empty or frame.index.isin(self.tables[path].index).any():
            raise ValueError("追加只能提交非空的新索引，不能包含旧索引")
        self.appends.append(frame.copy(deep=True))
        self.threads.append(threading.get_ident())
        if self.append_hook:
            self.append_hook(path, frame)
        else:
            self.append_rows(path, frame)

    def append_rows(self, path, frame):
        self.tables[path] = pd.concat([self.tables[path], frame]).sort_index()

    def apply(self, path, frame):
        self.tables[path].loc[frame.index, frame.columns] = frame.to_numpy()

    @staticmethod
    def assert_sorted(frame):
        if not frame.index.is_unique or not frame.index.is_monotonic_increasing:
            raise ValueError("索引必须唯一递增")


def choose(current, searchable):
    return RetrievalSelection(tuple(dict.fromkeys(row["event_id"] for row in searchable))[:10])


def new_result():
    return {"event_relation": "new", "matched_candidate_id": None, "new_information": None}


def impact_result():
    return {"direction": "利好", "materiality": "中", "execution_stage": "已完成",
            "persistence": "长期", "materiality_reason": "测试中的机制说明"}


class EventFixture(Fixture):
    def settings(self, **kwargs):
        kwargs.setdefault("user_name_to_tickers", {})
        return EventConfig("first", "result", str(self.path), str(self.path),
                           **kwargs)

    def relation_settings(self, backend, **kwargs):
        """按已有事件确定显式处理日期，供第二、三轮测试复用。"""
        days = [pd.Timestamp(key[0]).date().isoformat() for key in backend.tables["result"].index]
        kwargs.setdefault("start_time", min(days) if days else "2021-01-01")
        kwargs.setdefault("end_time", max(days) if days else "2021-01-01")
        return self.settings(**kwargs)

    def backend(self, posts):
        source = source_posts(posts)
        result = expand_first_round(source, {})
        return Tables(source, result)

    def factory(self, handler):
        def factory(config):
            def create(_):
                return httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
            return NetworkWorker(config, client_factory=create)
        return factory

    def relate(self, backend, handler=None, retriever=choose, **kwargs):
        return run_relation(backend, self.relation_settings(backend), ai_config=self.config(), retriever=retriever,
                            network_factory=self.factory(handler or (lambda req: success(json.dumps(new_result())))), **kwargs)
