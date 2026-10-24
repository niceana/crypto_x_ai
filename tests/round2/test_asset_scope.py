"""验证目标币种的事件筛选与续跑；不访问真实服务。"""

import json
import queue
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpx
import pandas as pd

from common.ai_protocol import parse_response
from common.config import EmbeddingConfig
from common.network import CompletionMessage
from round2.runner import run_relation
from tests.event_support import EventFixture, choose, new_result
from tests.support import success


ASSET_LIST_PATH = "crypto_1m/research_prod/meta/alphas/Cpt380_BA_Daily_Update_All.json"


class AssetScopeTests(EventFixture, unittest.TestCase):
    def asset_list(self, backend, document):
        return patch.object(backend, "read_object", return_value=json.dumps(document).encode("utf-8"), create=True)

    def memory_network(self, requests):
        """用完成队列替代网络，保留请求正文校验与实际结果保存。"""
        def factory(config):
            network = SimpleNamespace(completions=queue.Queue(maxsize=config.run.concurrency),
                                      unpublished=[], failure=None, http_attempts_started=0,
                                      start=Mock(), check=Mock(), shutdown=Mock())

            def submit(job):
                requests.append(json.loads(job.text))
                content = json.dumps(new_result())
                response = parse_response(200, success(content).text)
                network.http_attempts_started += 1
                network.completions.put_nowait(CompletionMessage(
                    job.key, response, 1, value=job.validate_output(content)))

            network.submit = submit
            return self.add_memory_ready_queue(network)

        return factory

    def test_only_target_tasks_are_recovered_requested_and_counted_and_restart_is_idle(self):
        backend = self.backend([("2021-01-01", "target-one", ["ETH"], "a"),
                                ("2021-01-01", "skip-processing", ["ZZZ"], "a"),
                                ("2021-01-02", "target-two", ["ETH"], "a"),
                                ("2021-01-02", "skip-failed", ["ZZZ"], "a")])
        frame = backend.tables["result"]
        frame.loc[frame["asset"] == "ZZZ", "round2_status"] = ["9", "-1"]
        frame.loc[frame["asset"] == "ETH", "round2_status"] = ["9", "0"]
        skipped = frame.loc[frame["asset"] == "ZZZ"].copy(deep=True)
        settings = self.relation_settings(backend, round2_asset_list_path=ASSET_LIST_PATH)
        requests = []

        document = {"InstrumentList": ["eth.exchange", "ETH.other", "BTC.exchange"]}
        with self.asset_list(backend, document) as read:
            with self.assertLogs("round2.runner", level="INFO") as captured:
                summary = run_relation(backend, settings, ai_config=self.config(), retriever=choose,
                                       network_factory=self.memory_network(requests))
            read.assert_called_once_with(ASSET_LIST_PATH)
        self.assertEqual((summary.total, summary.new_completed, summary.http_attempts_started), (2, 2, 2))
        self.assertEqual([body["current"]["event"] for body in requests], ["target-one", "target-two"])
        self.assertTrue(any("目标记录=2，跳过记录=2，命中资产=1" in line for line in captured.output))
        pd.testing.assert_frame_equal(backend.tables["result"].loc[skipped.index], skipped)
        self.assertTrue(all(set(batch.index).issubset(frame.index[frame["asset"] == "ETH"])
                            for batch in backend.updates))

        updates = len(backend.updates)
        with self.asset_list(backend, document) as read:
            resumed = run_relation(backend, settings,
                                   network_factory=lambda config: self.fail("目标已完成不应启动网络"))
            read.assert_called_once_with(ASSET_LIST_PATH)
        self.assertEqual((resumed.total, resumed.already_completed, resumed.new_completed), (2, 2, 0))
        self.assertEqual(len(backend.updates), updates)
        pd.testing.assert_frame_equal(backend.tables["result"].loc[skipped.index], skipped)

    def test_no_target_tasks_returns_without_history_embeddings_or_network(self):
        backend = self.backend([("2021-01-01", "unwanted", ["ZZZ"], "a")])
        backend.tables["result"]["round2_status"] = "-1"
        before = backend.tables["result"].copy(deep=True)
        with self.asset_list(backend, {"InstrumentList": ["ETH.exchange"]}):
            with patch("round2.runner.EmbeddingRetriever", side_effect=AssertionError("不应准备向量")):
                summary = run_relation(backend, self.relation_settings(backend, round2_asset_list_path=ASSET_LIST_PATH),
                                       network_factory=lambda config: self.fail("无目标不应启动网络"))
        self.assertEqual((summary.total, summary.new_completed, summary.pending, summary.failed), (0, 0, 0, 0))
        self.assertFalse(backend.meta_reads or backend.column_reads or backend.updates)
        pd.testing.assert_frame_equal(backend.tables["result"], before)

    def test_only_target_history_and_current_events_receive_embeddings(self):
        backend = self.backend([("2020-12-01", "target-history", ["ETH"], "a"),
                                ("2020-12-01", "unwanted-history-gap", ["ZZZ"], "a"),
                                ("2021-01-01", "target-current", ["ETH"], "a"),
                                ("2021-01-01", "unwanted-current", ["ZZZ"], "a"),
                                ("2021-02-01", "unwanted-later-completed", ["ZZZ"], "a")])
        frame = backend.tables["result"]
        history = frame.index[frame["event"] == "target-history"][0]
        frame.loc[history, ["event_id", "event_relation", "round2_status"]] = [history[1], "new", "1"]
        frame.loc[frame["event"] == "unwanted-later-completed", "round2_status"] = "1"
        skipped = frame.loc[frame["asset"] == "ZZZ"].copy(deep=True)
        settings = self.settings(start_time="2021-01-01", end_time="2021-01-01",
                                 round2_asset_list_path=ASSET_LIST_PATH, embedding_config_path="embedding.yaml")
        embedding_texts, requests = [], []

        def embedding_handler(request):
            texts = json.loads(request.content)["input"]
            embedding_texts.extend(texts)
            return httpx.Response(200, json={"data": [
                {"index": index, "embedding": [1.0, 0.0]} for index in range(len(texts))]})

        embedding_client = httpx.Client(transport=httpx.MockTransport(embedding_handler), trust_env=False)
        self.addCleanup(embedding_client.close)
        with self.asset_list(backend, {"InstrumentList": ["ETH.exchange"]}):
            with patch("round2.runner.load_embedding_config", return_value=EmbeddingConfig("https://mock.invalid/v1", "test-key")):
                with patch("round2.embedding._create_client", return_value=embedding_client):
                    summary = run_relation(backend, settings, ai_config=self.config(), network_factory=self.memory_network(requests))
        self.assertEqual((summary.total, summary.new_completed), (1, 1))
        self.assertEqual(embedding_texts, ["target-history", "target-current"])
        self.assertEqual(requests[0]["candidates"], [{"candidate_id": "E1", "history": [
            {"published_at": str(history[0]), "event": "target-history"}]}])
        self.assertTrue(embedding_client.is_closed)
        pd.testing.assert_frame_equal(backend.tables["result"].loc[skipped.index], skipped)

    def test_selected_asset_still_requires_its_own_earlier_history(self):
        backend = self.backend([("2020-12-01", "target-history-gap", ["ETH"], "a"),
                                ("2021-01-01", "target-current", ["ETH"], "a")])
        settings = self.settings(start_time="2021-01-01", end_time="2021-01-01", round2_asset_list_path=ASSET_LIST_PATH)
        with self.asset_list(backend, {"InstrumentList": ["ETH.exchange"]}):
            with self.assertRaisesRegex(ValueError, "所需前史未完成"):
                run_relation(backend, settings, network_factory=lambda config: self.fail("前史未完成不应启动网络"))
        self.assertFalse(backend.updates)

    def test_empty_path_keeps_all_assets_without_reading_an_object(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a"),
                                ("2021-01-01", "two", ["ZZZ"], "a")])
        requests = []
        summary = run_relation(backend, self.relation_settings(backend), ai_config=self.config(), retriever=choose,
                               network_factory=self.memory_network(requests))
        self.assertEqual((summary.total, summary.new_completed), (2, 2))
        self.assertEqual({body["current"]["event"] for body in requests}, {"one", "two"})
