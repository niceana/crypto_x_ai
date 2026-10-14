import json
import tempfile
import threading
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
import pandas as pd
import yaml

from ai_batch import run_worker
from ai_batch.config import load_config
from ai_batch.tasks import TASK_COLUMNS


def tasks(dates):
    dates = list(dates)
    index = pd.MultiIndex.from_arrays([pd.DatetimeIndex(dates), [str(i) for i in range(len(dates))]], names=["date", "id"])
    frame = pd.DataFrame("", index=index, columns=TASK_COLUMNS, dtype=object)
    frame["status"] = "0"
    frame["text"] = ["post {}".format(i) for i in range(len(dates))]
    frame["user_name"] = ["private-user-{}".format(i) for i in range(len(dates))]
    return frame


def success(content="ok", **extra):
    body = {"choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": content}}]}
    body.update(extra)
    return httpx.Response(200, json=body)


class FakeCli:
    def __init__(self, frame):
        self.frame = frame.copy(deep=True)
        self.reads = []
        self.read_paths = []
        self.updates = []
        self.update_paths = []
        self.threads = []
        self.update_hook = None
        self.read_hook = None

    def read(self, path, start_time=None, end_time=None):
        if (start_time is None) != (end_time is None):
            raise ValueError("cli.read 日期参数必须同时设置")
        if start_time is not None:
            for value in (start_time, end_time):
                if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
                    raise ValueError("cli.read 日期参数必须为 YYYY-MM-DD 字符串")
        self.threads.append(threading.get_ident())
        self.reads.append((start_time, end_time))
        self.read_paths.append(path)
        if self.read_hook:
            self.read_hook(start_time, end_time)
        if start_time is None:
            return self.frame.copy(deep=True)
        dates = self.frame.index.get_level_values("date")
        end_next_day = (date.fromisoformat(end_time) + timedelta(days=1)).isoformat()
        return self.frame[(dates >= start_time) & (dates < end_next_day)].copy(deep=True)

    def apply(self, patch_frame):
        self.frame.loc[patch_frame.index, patch_frame.columns] = patch_frame.to_numpy()

    def update_by_column(self, path, patch_frame):
        # 按用户确认的 xdata 要求拒绝无序或重复索引，避免模拟客户端掩盖错误。
        if not patch_frame.index.is_unique or not patch_frame.index.is_monotonic_increasing:
            raise ValueError("更新行必须按 (date, id) 严格递增，不能包含重复键")
        self.threads.append(threading.get_ident())
        self.updates.append(patch_frame.copy(deep=True))
        self.update_paths.append(path)
        if self.update_hook:
            self.update_hook(patch_frame)
        else:
            self.apply(patch_frame)


class Fixture:
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix=".test-", dir=str(Path.cwd()))
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "config.yaml"
        self.task_path = "test-task-table"
        self.document = {
            "prompts": {"system": "analyse", "user": "{{task.text}}"},
            "api": {"url": "https://mock.invalid/ai", "headers": {}, "payload": {
                "model": "test", "messages": [{"role": "system", "content": "{{prompts.system}}"},
                                               {"role": "user", "content": "{{prompts.user}}"}], "stream": False}},
            "run": {"concurrency": 3, "batch_size": 200, "result_buffer_size": 200,
                    "flush_interval_seconds": 0.01, "backoff_base_seconds": 0.001,
                    "backoff_max_seconds": 0.001, "storage_retry_seconds": 0.001,
                    "progress_every": 100000},
        }
        self.write_config()

    def write_config(self):
        self.path.write_text(yaml.safe_dump(self.document, allow_unicode=True), encoding="utf-8")

    def config(self):
        self.write_config()
        return load_config(self.path)

    def worker(self, cli, handler, start_time=None, end_time=None):
        self.write_config()
        clients = []

        def create(config):
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
            clients.append(client)
            return client

        # 捕获模块日志，继续核对异常路径不会误报“全部处理完成”。
        try:
            with patch("ai_batch.runner._create_client", side_effect=create), self.assertLogs("ai_batch", level="DEBUG") as captured:
                return run_worker(cli, self.task_path, self.path, start_time, end_time)
        finally:
            self.last_output = "\n".join(captured.output)
            self.log_records = captured.records
            self.clients = clients
            self.assertTrue(all(client.is_closed for client in clients))
            self.assertFalse(any(thread.name == "ai-batch-network" for thread in threading.enumerate()))


def request_text(request):
    return json.loads(request.content)["messages"][1]["content"]
