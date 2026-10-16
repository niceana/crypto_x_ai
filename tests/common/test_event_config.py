"""验证研究流程的历史边界、任务状态、重试与续跑；不访问真实服务。"""

import json
import unittest


from common.config import load_event_config
from tests.event_support import EventFixture


class EventConfigTests(EventFixture, unittest.TestCase):
    def test_event_config_resolves_ai_paths_and_rejects_partial_range(self):
        document = {"first_round_task_path": "first", "result_path": "result",
                    "round2_ai_config_path": "round2.yaml", "round3_ai_config_path": "round3.yaml",
                    "start_time": "2018-01-01", "end_time": "2018-01-01",
                    "initialize_mode": "resume", "user_name_to_tickers": {"a": ["ETH"]}}
        path = self.path.parent / "event_config.yaml"
        path.write_text(json.dumps(document), encoding="utf-8")
        settings = load_event_config(path)
        self.assertEqual(settings.result_path, "result")
        self.assertEqual(settings.round2_ai_config_path, str(path.parent / "round2.yaml"))
        self.assertEqual(settings.user_name_to_tickers, {"a": ["ETH"]})
        self.assertEqual(settings.initialize_mode, "resume")
        document.pop("end_time")
        path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "同时设置"):
            load_event_config(path)
