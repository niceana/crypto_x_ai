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
            "weight_grid:\n  coefficients:\n    B: [1]\n    B: [2]\n",
            "weight_grid:\n  mappings:\n    direction:\n      利好: [1]\n      利好: [2]\n",
        )
        path = self.path.parent / "event_processing_config.yaml"
        for suffix in cases:
            with self.subTest(suffix=suffix):
                path.write_text(base + suffix, encoding="utf-8")
                with self.assertRaisesRegex(ConfigError, "重复键"):
                    load_event_config(path)

    def test_equal_key_names_in_separate_mappings_and_multi_asset_account_are_valid(self):
        document = {"first_round_task_path": "first", "result_path": "result",
                    "round2_ai_config_path": "round2.yaml", "round3_ai_config_path": "round3.yaml",
                    "user_name_to_tickers": {"level": ["BTC", "ETH"]}, "logging": {"level": "INFO"}}
        path = self.path.parent / "event_processing_config.yaml"
        path.write_text(json.dumps(document), encoding="utf-8")
        settings = load_event_config(path)
        self.assertEqual(settings.user_name_to_tickers, {"level": ["BTC", "ETH"]})
        self.assertEqual(settings.logging, {"level": "INFO"})

    def test_event_config_resolves_ai_paths_and_rejects_partial_range(self):
        document = {"first_round_task_path": "first", "result_path": "result",
                    "round2_ai_config_path": "round2.yaml", "round3_ai_config_path": "round3.yaml",
                    "start_time": "2018-01-01", "end_time": "2018-01-01",
                    "initialize_mode": "resume", "user_name_to_tickers": {"a": ["ETH"]}}
        path = self.path.parent / "event_processing_config.yaml"
        path.write_text(json.dumps(document), encoding="utf-8")
        settings = load_event_config(path)
        self.assertEqual(settings.result_path, "result")
        self.assertEqual(settings.round2_ai_config_path, str(path.parent / "round2.yaml"))
        self.assertEqual(settings.user_name_to_tickers, {"a": ["ETH"]})
        self.assertEqual(settings.initialize_mode, "resume")
        self.assertEqual(settings.history_lookback_months, 3)
        document.pop("end_time")
        path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "同时设置"):
            load_event_config(path)

    def test_history_lookback_months_requires_positive_integer(self):
        document = {"first_round_task_path": "first", "result_path": "result",
                    "round2_ai_config_path": "round2.yaml", "round3_ai_config_path": "round3.yaml"}
        path = self.path.parent / "event_processing_config.yaml"
        for value in (0, -1, True, "3", 0.25, 3.5, None):
            with self.subTest(value=value):
                path.write_text(json.dumps(dict(document, history_lookback_months=value)), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "history_lookback_months"):
                    load_event_config(path)
        for months in (3, 15):
            path.write_text(json.dumps(dict(document, history_lookback_months=months)), encoding="utf-8")
            self.assertEqual(load_event_config(path).history_lookback_months, months)

    def test_old_year_parameter_is_rejected_instead_of_silently_ignored(self):
        document = {"first_round_task_path": "first", "result_path": "result",
                    "round2_ai_config_path": "round2.yaml", "round3_ai_config_path": "round3.yaml",
                    "history_lookback_years": 5}
        path = self.path.parent / "event_processing_config.yaml"
        path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "history_lookback_years"):
            load_event_config(path)

    def test_optional_weight_path_accepts_empty_or_actual_path(self):
        document = {"first_round_task_path": "first", "result_path": "result",
                    "round2_ai_config_path": "round2.yaml", "round3_ai_config_path": "round3.yaml"}
        path = self.path.parent / "event_processing_config.yaml"
        path.write_text(json.dumps(document), encoding="utf-8")
        self.assertEqual(load_event_config(path).weight_path, "")
        for value in ("", "weights"):
            with self.subTest(value=value):
                path.write_text(json.dumps(dict(document, weight_path=value)), encoding="utf-8")
                self.assertEqual(load_event_config(path).weight_path, value)
        path.write_text(json.dumps(dict(document, weight_path=None)), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "weight_path"):
            load_event_config(path)

    def test_weight_grid_is_optional_and_preserves_candidate_arrays(self):
        document = {"first_round_task_path": "first", "result_path": "result",
                    "round2_ai_config_path": "round2.yaml", "round3_ai_config_path": "round3.yaml"}
        path = self.path.parent / "event_processing_config.yaml"
        path.write_text(json.dumps(document), encoding="utf-8")
        self.assertIsNone(load_event_config(path).weight_grid)
        grid = {"coefficients": {"B": [1, 2]}, "mappings": {"materiality": {"高": [3, 5]}}}
        path.write_text(json.dumps(dict(document, weight_grid=grid)), encoding="utf-8")
        self.assertEqual(load_event_config(path).weight_grid, grid)
        for value in (None, [], "grid"):
            with self.subTest(value=value):
                path.write_text(json.dumps(dict(document, weight_grid=value)), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "weight_grid"):
                    load_event_config(path)
