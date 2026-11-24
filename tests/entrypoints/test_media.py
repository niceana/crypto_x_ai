"""媒体真实入口与模拟 SDK/HTTP 的三轮离线集成，不访问外部服务。"""

import importlib
import json
import os
import sys
import threading
import types
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import Mock, patch

import httpx
import pandas as pd
import yaml

from entrypoints import initialize_media_tasks, run_media_round1, run_media_round2, run_media_round3, import_media_events
from common.validation import TASK_COLUMNS
from tests.event_support import EventFixture, Tables, impact_result, new_result, source_posts
from tests.support import success


class MediaEntryTests(EventFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.root = self.path.parent
        self.configs = self.root / "configs"
        self.configs.mkdir()
        env = patch.dict(os.environ, {"CRYPTO_X_AI_LOCK_DIR": str(self.root / "locks")})
        env.start()
        self.addCleanup(env.stop)
        self.job = dict(source_path="source", round1_result_path="first", ai_config_path="r1.yaml",
                        start_time="2021-01-01", end_time="2021-01-31", logging={"level": "CRITICAL"})
        self.events = dict(round1_result_path="first", round2_result_path="result", round3_result_path="result",
                           round2_ai_config_path="r2.yaml", round3_ai_config_path="r3.yaml",
                           start_time="2021-01-01", end_time="2021-01-31", history_lookback_months=1,
                           round2_asset_list_path="universe", round3_skip_incomplete_round2=True,
                           logging={"level": "CRITICAL"})
        self.write_media_configs()
        for stage in (1, 2, 3):
            document = dict(self.document)
            document["api"] = dict(self.document["api"], payload=dict(self.document["api"]["payload"],
                                                                    model="media-r{}".format(stage)))
            (self.configs / "r{}.yaml".format(stage)).write_text(yaml.safe_dump(document), encoding="utf-8")
        for module in (initialize_media_tasks, run_media_round1, run_media_round2, run_media_round3, import_media_events):
            location = patch.object(module, "__file__", str(self.root / "entrypoints" / "module.py"))
            location.start()
            self.addCleanup(location.stop)

    def write_media_configs(self):
        for name, document in (("media_post_extraction_config.yaml", self.job),
                               ("media_event_processing_config.yaml", self.events)):
            (self.configs / name).write_text(yaml.safe_dump(document), encoding="utf-8")

    def sdk(self, backend):
        factory = Mock(return_value=backend)
        client = types.ModuleType("xdata.client")
        client.get_client = factory
        package = types.ModuleType("xdata")
        package.__path__ = []
        package.client = client
        return patch.dict(sys.modules, {"xdata": package, "xdata.client": client}), factory

    def test_two_tables_all_three_rounds_scopes_uncertain_repeat_and_resume(self):
        raw = pd.DataFrame({"text": ["a", "b", "c", "d", "opinion"], "user_name": ["reporter"] * 5},
                           index=pd.MultiIndex.from_tuples(
                               [(pd.Timestamp("2021-01-{:02d}".format(i)), "p{}".format(i)) for i in range(1, 6)],
                               names=["date", "id"]))
        backend = Tables(raw)
        del backend.tables["first"]
        backend.tables["source"] = raw.copy(deep=True)
        backend.read_object = Mock(return_value=b'{"InstrumentList":["BTC.USDT.UFUTURE.BINANCE"]}')
        calls = Counter()
        extraction_inputs = []
        impact_objects = []

        def handler(request):
            body = json.loads(request.content)
            stage = body["model"]
            calls[stage] += 1
            data = json.loads(body["messages"][1]["content"])
            if stage == "media-r1":
                extraction_inputs.append(data)
                events = {
                    "a": [{"event": "事故初步", "tickers": ["btc", "BTC", "industry"]},
                          {"event": "宏观政策报道", "tickers": ["macro"]},
                          {"event": "名单外事件", "tickers": ["DOGE"]}],
                    "b": [{"event": "事故确认", "tickers": ["BTC", "INDUSTRY"]}],
                    "c": [{"event": "复述确认", "tickers": ["BTC", "INDUSTRY"]}],
                    "d": [{"event": "不确定事项", "tickers": ["BTC"]}],
                    "opinion": [],
                }[data["text"]]
                value = {"events": events}
            elif stage == "media-r2":
                event = data["current"]["event"]
                if event in ("事故确认", "复述确认"):
                    value = {"event_relation": "update" if event == "事故确认" else "repeat",
                             "matched_candidate_id": data["candidates"][0]["candidate_id"],
                             "new_information": "由初步报道推进为确认，关键事实已明确。" if event == "事故确认" else None}
                elif event == "不确定事项":
                    value = dict(new_result(), event_relation="uncertain")
                else:
                    value = new_result()
            else:
                self.assertEqual(stage, "media-r3")
                impact_objects.append(data["asset"])
                if data["event"] == "事故确认":
                    self.assertIn("由初步报道", data["new_information"])
                value = impact_result()
            return success(json.dumps(value, ensure_ascii=False))

        sdk, factory = self.sdk(backend)
        was_loaded = "entrypoints.run_round1" in sys.modules
        with sdk:
            shared = importlib.import_module("entrypoints.run_round1")
            if not was_loaded:
                self.addCleanup(sys.modules.pop, "entrypoints.run_round1", None)
            create = lambda config: httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
            with patch.object(shared, "get_client", factory), \
                    patch("common.network._create_client", side_effect=create), \
                    patch("round1.runner._create_client", side_effect=create), \
                    patch("round2.runner.EmbeddingRetriever", side_effect=AssertionError("不能调用向量")):
                initialize_media_tasks.main()
                run_media_round1.main()
                run_media_round2.main()
                run_media_round3.main()
                self.assertEqual(backend.registers, ["first", "result"])
                self.assertEqual(len(backend.writes), 2)
                self.assertEqual(calls, {"media-r1": 5, "media-r2": 8, "media-r3": 6})
                frame = backend.tables["result"]
                self.assertEqual(len(frame), 9)
                self.assertEqual(set(frame["asset"]), {"BTC", "INDUSTRY", "MACRO", "DOGE"})
                self.assertTrue(frame.loc[frame["asset"].eq("DOGE"), "round2_status"].eq("0").all())
                self.assertEqual(set(impact_objects), {"BTC", "INDUSTRY", "MACRO"})
                self.assertTrue(all(set(value) == {"user_name", "text"} for value in extraction_inputs))
                row = frame.loc[frame["event"].eq("不确定事项")].iloc[0]
                self.assertEqual(row["event_relation"], "new")
                self.assertEqual(json.loads(json.loads(row["round2_ai_response"])["ai_response_content"])["event_relation"],
                                 "uncertain")
                for asset in ("BTC", "INDUSTRY"):
                    block = frame.loc[frame["asset"].eq(asset)].set_index("event")
                    self.assertEqual(block.at["复述确认", "round3_ai_response"], block.at["事故确认", "round3_ai_response"])
                before = frame.copy(deep=True)
                run_media_round1.main()
                run_media_round2.main()
                run_media_round3.main()
                import_media_events.main()
                pd.testing.assert_frame_equal(backend.tables["result"], before)
                pd.testing.assert_frame_equal(backend.tables["source"], raw)
                self.assertEqual(calls, {"media-r1": 5, "media-r2": 8, "media-r3": 6})
                self.assertEqual(set(backend.threads), {threading.get_ident()})

                # 自动导入按索引补缺；独立导入会严格发现同索引输入被改动。
                first = backend.tables["first"]
                content = json.loads(first.iloc[0]["ai_response_content"])
                content["events"][0]["event"] = "改变后的第一轮输入"
                first.loc[first.index[0], "ai_response_content"] = json.dumps(content)
                run_media_round1.main()
                with self.assertRaisesRegex(ValueError, "固定输入不一致"):
                    import_media_events.main()
                pd.testing.assert_frame_equal(backend.tables["result"], before)

    def test_independent_import_only_appends_missing_and_keeps_old_analysis(self):
        source = source_posts([("2021-01-01", "old", ["BTC"], "media"),
                               ("2021-01-02", "new", ["MACRO"], "media")])
        from initialization.event_records import expand_first_round
        old = expand_first_round(source.iloc[:1])
        old.loc[:, "round3_ai_response"] = "已有分析应保留"
        backend = Tables(source, old)
        sdk, _ = self.sdk(backend)
        with sdk:
            result = import_media_events.main()
            self.assertEqual(result.written, 1)
            self.assertEqual(len(backend.appends), 1)
            self.assertEqual(list(backend.appends[0]["event"]), ["new"])
            pd.testing.assert_frame_equal(backend.tables["result"].loc[old.index], old)
            self.assertEqual(import_media_events.main().written, 0)
        self.assertEqual(backend.registers, [])
        self.assertEqual(backend.writes, [])

    def test_invalid_config_stops_before_sdk_client_creation(self):
        self.events["round3_result_path"] = "different-table"
        self.write_media_configs()
        sdk, factory = self.sdk(object())
        with sdk:
            with self.assertRaisesRegex(ValueError, "同一张"):
                run_media_round2.main()
        factory.assert_not_called()

    def test_json_initialization_registers_tasks_without_reading_database_source_or_ai(self):
        post = {"id": "2023554387919122594", "created_at": "Tue Feb 17 00:25:36 +0000 2026",
                "text": "Ethereum news", "user": {"screen_name": "WuBlockchain"},
                "fetched_at": "2026-09-30T12:00:26.403852+00:00"}
        (self.root / "media.jsonl").write_text(json.dumps(post), encoding="utf-8")
        self.job.pop("ai_config_path")
        self.job.update(source_path="", json_files=["../media.jsonl"])
        self.write_media_configs()
        backend = Mock(spec=["read", "register", "write"])
        sdk, factory = self.sdk(backend)
        with sdk:
            result = initialize_media_tasks.main()
        self.assertEqual((result.total, result.written), (1, 1))
        factory.assert_called_once_with()
        backend.read.assert_not_called()
        backend.register.assert_called_once_with("first", "timeseries", "")
        backend.write.assert_called_once()
        self.assertEqual([call[0] for call in backend.mock_calls], ["register", "write"])
        target, tasks = backend.write.call_args.args
        self.assertEqual(target, "first")
        self.assertEqual(list(tasks.columns), TASK_COLUMNS)
        # 配置处理范围仍为 2021 年；初始化包含 JSON 中 2026 年的完整帖子。
        self.assertEqual(tasks.index[0], (pd.Timestamp("2026-02-17 00:25:36"), post["id"]))
        self.assertEqual(tasks.iloc[0]["text"], post["text"])
        self.assertEqual(tasks.iloc[0]["user_name"], "WuBlockchain")
        self.assertEqual(tasks.iloc[0]["status"], "0")
        for column in set(TASK_COLUMNS) - {"text", "user_name", "status"}:
            self.assertTrue(tasks[column].eq("").all(), column)

    def test_invalid_json_initialization_fails_before_sdk_client_creation(self):
        self.job.update(source_path="", json_files=["../media.jsonl"])
        self.write_media_configs()
        sdk, factory = self.sdk(Mock())
        for content in ("{bad", "[]", '{"id":"missing"}'):
            with self.subTest(content=content):
                (self.root / "media.jsonl").write_text(content, encoding="utf-8")
                with sdk, self.assertRaises(ValueError):
                    initialize_media_tasks.main()
                factory.assert_not_called()

    def test_first_round_rejects_inconsistent_paths_before_client_creation(self):
        sdk, factory = self.sdk(object())
        for change, error in (({"round1_result_path": "wrong-first"}, "必须一致"),
                              ({"round2_result_path": "source", "round3_result_path": "source"}, "路径必须不同")):
            with self.subTest(change=change):
                self.events = dict(self.events, **change)
                if "round2_result_path" in change:
                    self.events["round1_result_path"] = "first"
                self.write_media_configs()
                with sdk:
                    with self.assertRaisesRegex(ValueError, error):
                        run_media_round1.main()
                factory.assert_not_called()
