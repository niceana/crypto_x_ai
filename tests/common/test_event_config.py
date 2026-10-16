"""验证研究流程的历史边界、任务状态、重试与续跑；不访问真实服务。"""

import json
import unittest


from common.config import ConfigError, load_event_config
from tests.event_support import EventFixture


class EventConfigTests(EventFixture, unittest.TestCase):
    def test_event_config_rejects_duplicate_root_account_and_logging_keys(self):
        base = ("first_round_task_path: first\nresult_path: result\n"
                "round2_ai_config_path: round2.yaml\nround3_ai_config_path: round3.yaml\n")
        cases = (
            "result_path: other-result\n",
            "result_path: result\n",
            "user_name_to_tickers:\n  BitcoinMagazine: [BTC]\n  BitcoinMagazine: [ETH]\n",
            "user_name_to_tickers:\n  BitcoinMagazine: [BTC]\n  BitcoinMagazine: [BTC]\n",
            "logging:\n  level: INFO\n  level: DEBUG\n",
        )
        path = self.path.parent / "event_config.yaml"
        for suffix in cases:
            with self.subTest(suffix=suffix):
                path.write_text(base + suffix, encoding="utf-8")
                with self.assertRaisesRegex(ConfigError, "重复键"):
                    load_event_config(path)

    def test_equal_key_names_in_separate_mappings_and_multi_asset_account_are_valid(self):
        document = {"first_round_task_path": "first", "result_path": "result",
                    "round2_ai_config_path": "round2.yaml", "round3_ai_config_path": "round3.yaml",
                    "user_name_to_tickers": {"level": ["BTC", "ETH"]}, "logging": {"level": "INFO"}}
        path = self.path.parent / "event_config.yaml"
        path.write_text(json.dumps(document), encoding="utf-8")
        settings = load_event_config(path)
        self.assertEqual(settings.user_name_to_tickers, {"level": ["BTC", "ETH"]})
        self.assertEqual(settings.logging, {"level": "INFO"})

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
        self.assertEqual(settings.history_lookback_years, 5)
        document.pop("end_time")
        path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "同时设置"):
            load_event_config(path)

    def test_history_lookback_years_requires_positive_integer(self):
        document = {"first_round_task_path": "first", "result_path": "result",
                    "round2_ai_config_path": "round2.yaml", "round3_ai_config_path": "round3.yaml"}
        path = self.path.parent / "event_config.yaml"
        for value in (0, -1, True, "5", 5.5, None):
            with self.subTest(value=value):
                path.write_text(json.dumps(dict(document, history_lookback_years=value)), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "history_lookback_years"):
                    load_event_config(path)
        path.write_text(json.dumps(dict(document, history_lookback_years=2)), encoding="utf-8")
        self.assertEqual(load_event_config(path).history_lookback_years, 2)
