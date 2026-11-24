"""媒体配置与原配置适配，不连接 SDK 或 AI。"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from common.config import ConfigError, load_ai_config
from common.media_config import load_media_event_config, load_media_job_config, load_media_target_assets
from common.ai_protocol import render_payload


class MediaConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix=".test-media-config-", dir=str(Path.cwd()))
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "media.yaml"

    def write(self, document):
        self.path.write_text(yaml.safe_dump(document, allow_unicode=True), encoding="utf-8")

    def event_document(self):
        return dict(round1_result_path="media-first", round2_result_path="media-events",
                    round3_result_path="media-events", round2_ai_config_path="r2.yaml",
                    round3_ai_config_path="r3.yaml", start_time="2021-01-01", end_time="2021-01-31")

    def test_job_public_names_map_to_existing_interface_and_init_needs_no_ai(self):
        self.write(dict(source_path="raw", round1_result_path="media-first"))
        settings = load_media_job_config(self.path, require_ai=False)
        self.assertEqual(settings, {"source_path": "raw", "task_path": "media-first"})
        with self.assertRaisesRegex(ValueError, "ai_config_path"):
            load_media_job_config(self.path)
        self.write(dict(source_path="raw", round1_result_path="media-first", ai_config_path="r1.yaml"))
        self.assertEqual(load_media_job_config(self.path)["ai_config_path"], str(self.root / "r1.yaml"))

    def test_media_settings_keep_one_month_shared_table_and_no_mapping_or_embedding(self):
        self.write(self.event_document())
        settings = load_media_event_config(self.path)
        self.assertEqual((settings.first_round_task_path, settings.result_path), ("media-first", "media-events"))
        self.assertEqual(settings.history_lookback_months, 1)
        self.assertEqual(settings.user_name_to_tickers, {})
        self.assertEqual(settings.embedding_config_path, "")
        self.assertEqual(settings.round2_ai_config_path, str(self.root / "r2.yaml"))
        self.assertIsNone(settings.round2_max_input_characters)

    def test_json_source_resolves_paths_from_config_directory_and_needs_no_ai_for_init(self):
        document = dict(json_files=["posts.jsonl", str(self.root / "other.json")], round1_result_path="media-first")
        self.write(document)
        settings = load_media_job_config(self.path, require_ai=False)
        self.assertEqual(settings["source_path"], "")
        self.assertEqual(settings["task_path"], "media-first")
        self.assertEqual(settings["json_files"], [str(self.root / "posts.jsonl"), str(self.root / "other.json")])
        self.write(dict(document, source_path="", ai_config_path="r1.yaml"))
        self.assertEqual(load_media_job_config(self.path)["json_files"], settings["json_files"])

    def test_job_requires_exactly_one_source_and_rejects_invalid_file_lists(self):
        for change in ({}, {"source_path": ""}, {"json_files": []},
                       {"source_path": "raw", "json_files": ["posts.jsonl"]},
                       {"source_path": None}, {"json_files": "posts.jsonl"},
                       {"json_files": [""]}, {"json_files": [None]}, {"json_files": None}):
            with self.subTest(change=change):
                self.write(dict(round1_result_path="first", **change))
                with self.assertRaises(ConfigError):
                    load_media_job_config(self.path, require_ai=False)
        self.write(dict(source_path="raw", json_files=[], round1_result_path="first"))
        self.assertEqual(load_media_job_config(self.path, require_ai=False)["source_path"], "raw")

    def test_invalid_paths_options_dates_and_limits_are_rejected(self):
        cases = [({"round3_result_path": "different"}, "同一张"),
                 ({"round1_result_path": "media-events"}, "路径必须不同"),
                 ({"end_time": None}, "同时设置"),
                 ({"history_lookback_months": 0}, "正整数"),
                 ({"embedding_config_path": "unused.yaml"}, "未知字段"),
                 ({"user_name_to_tickers": {"media": ["BTC"]}}, "未知字段"),
                 ({"round2_max_input_characters": True}, "正整数"),
                 ({"round2_max_input_characters": 0}, "正整数"),
                 ({"round2_asset_list_path": []}, "字符串")]
        for change, error in cases:
            with self.subTest(change=change):
                self.write(dict(self.event_document(), **change))
                with self.assertRaisesRegex(ValueError, error):
                    load_media_event_config(self.path)
        self.write(dict(source_path="same", round1_result_path="same", ai_config_path="r1.yaml"))
        with self.assertRaisesRegex(ValueError, "路径必须不同"):
            load_media_job_config(self.path)
        self.write(dict(source_path="raw", round1_result_path="first", task_path="old", ai_config_path="r1.yaml"))
        with self.assertRaisesRegex(ValueError, "未知字段"):
            load_media_job_config(self.path)

    def test_duplicate_keys_are_not_silently_accepted(self):
        self.path.write_text("round1_result_path: first\nround1_result_path: other\n", encoding="utf-8")
        with self.assertRaisesRegex(ConfigError, "重复键"):
            load_media_event_config(self.path)

    def test_optional_universe_keeps_industry_and_macro(self):
        self.write(dict(self.event_document(), round2_asset_list_path="universe"))
        cli = Mock()
        cli.read_object.return_value = json.dumps({"InstrumentList": ["btc.USDT.FUTURE", "ETH.x"]}).encode()
        self.assertEqual(load_media_target_assets(cli, load_media_event_config(self.path)),
                         {"BTC", "ETH", "INDUSTRY", "MACRO"})
        self.write(self.event_document())
        cli.reset_mock()
        self.assertIsNone(load_media_target_assets(cli, load_media_event_config(self.path)))
        cli.read_object.assert_not_called()

    def test_three_shipped_prompts_load_and_render_with_independent_keys(self):
        configs = Path(__file__).resolve().parents[2] / "configs"
        systems = []
        for stage in (1, 2, 3):
            document = yaml.safe_load((configs / "media_round{}_ai_config.yaml".format(stage)).read_text(encoding="utf-8"))
            self.assertEqual(document["api"]["url"], "")
            document["api"]["url"] = "https://mock.invalid/ai"
            document["api"]["payload"]["model"] = "offline"
            self.write(document)
            variable = "MEDIA_ROUND{}_AI_API_KEY".format(stage)
            with patch.dict(os.environ, {variable: "offline-only"}):
                config = load_ai_config(self.path)
                payload = render_payload(config, '{"event":"测试"}')
            self.assertEqual(payload["messages"][1]["content"], '{"event":"测试"}')
            systems.append(config.prompts.system)
        self.assertEqual(len(set(systems)), 3)
