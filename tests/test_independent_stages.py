"""验证独立阶段交接、向量复用与矩阵缓存，不连接真实服务。"""

import json
import queue
import threading
import unittest
from dataclasses import replace
from datetime import date
from unittest.mock import patch

import httpx
import numpy as np
import pandas as pd

from common.ai_protocol import parse_response
from common.config import EmbeddingConfig
from common.execution import month_ranges
from common.network import CompletionMessage
from initialization.event_records import import_event_records_from_posts
from round1.runner import run_extraction
from round2.embedding import EmbeddingRetriever
from round2.runner import run_relation
from round3.runner import run_impact
from tests.event_support import EventFixture, Tables, source_posts, impact_result
from tests.support import success, tasks
from vectorization.encoder import EmbeddingEncoder
from vectorization.preparation import prepare_vectors
from vectorization.storage import EmbeddingError


class ImmediateNetwork:
    """执行真实业务校验的内存网络替身，离线覆盖完整三轮及 repeat 继承。"""

    def __init__(self, captured):
        self.captured = captured
        self.completions = queue.Queue()
        self.failure = None
        self.stopped = threading.Event()
        self.http_attempts_started = 0

    def start(self):
        pass

    def shutdown(self):
        pass

    def check(self):
        pass

    def request_stop(self, reason=""):
        self.stopped.set()

    def http_snapshot(self):
        return 0, 0, 0.0

    def bind_ready_queue(self, ready, wake):
        self.ready, self.wake = ready, wake

    def notify_ready(self):
        while not self.ready.empty():
            job = self.ready.get_nowait()
            self.wake()
            self.submit(job)

    def submit(self, job):
        value = json.loads(job.text)
        self.captured.append(value)
        if "text" in value:
            answer = {"events": [{"event": value["text"], "tickers": ["ETH"]}]}
        elif "current" in value:
            candidate = value["candidates"][0]["candidate_id"]
            repeat = value["current"]["event"] == "repeat"
            answer = {"event_relation": "repeat" if repeat else "update", "matched_candidate_id": candidate,
                      "new_information": None if repeat else "新变化"}
        else:
            answer = impact_result()
        content = json.dumps(answer, ensure_ascii=False)
        response = parse_response(200, success(content).text)
        result = job.validate_output(content) if job.validate_output else None
        self.http_attempts_started += 1
        self.completions.put_nowait(CompletionMessage(job.key, response, 1, value=result))


class IndependentStageTests(EventFixture, unittest.TestCase):
    def embedding_config(self):
        return EmbeddingConfig(base_url="https://mock.invalid/v1", api_key="not-real")

    def vector_engine(self, calls):
        def handle(request):
            texts = json.loads(request.content)["input"]
            calls.extend(texts)
            return httpx.Response(200, json={"data": [
                {"index": index, "embedding": [1.0, 0.0]} for index, _ in enumerate(texts)]})
        return EmbeddingEncoder(self.embedding_config(), client_factory=lambda config:
                                httpx.Client(transport=httpx.MockTransport(handle)))

    def test_standalone_vector_and_relation_entrypoints_require_complete_first_round(self):
        from entrypoints import prepare_vectors as vector_entry
        from entrypoints import run_round2 as relation_entry
        source = source_posts([("2021-01-01", "one", ["ETH"], "a"),
                               ("2021-01-02", "two", ["ETH"], "a")])
        backend = Tables(source)
        settings = self.settings(start_time="2021-01-01", end_time="2021-01-31")
        for module, method in ((vector_entry, "prepare_vectors"), (relation_entry, "run_relation")):
            with patch.object(module, method, return_value="done") as operation:
                backend.tables["first"].loc[source.index[-1], "status"] = "-1"
                with self.assertRaisesRegex(ValueError, "禁止进入"):
                    module.run_stage(backend, settings)
                operation.assert_not_called()
                backend.tables["first"].loc[:, "status"] = "1"
                self.assertEqual(module.run_stage(backend, settings), "done")
                operation.assert_called_once_with(backend, settings)


    def test_vectors_include_pending_preceding_month_but_only_write_embedding(self):
        backend = self.backend([("2020-10-01", "outside", ["ETH"], "a"),
                                ("2021-01-01", "previous-pending", ["ETH"], "a"),
                                ("2021-02-01", "current", ["ETH"], "a"),
                                ("2021-02-01", "other", ["BTC"], "b")])
        before = backend.tables["result"].drop(columns="embedding").copy(deep=True)
        calls = []
        settings = self.settings(start_time="2021-02-01", end_time="2021-02-28",
                                 embedding_config_path=str(self.path))
        summary = prepare_vectors(backend, settings, encoder=self.vector_engine(calls), target_assets={"ETH"})
        self.assertEqual((summary.current_records, summary.prepared_records, summary.saved_records), (1, 2, 2))
        self.assertEqual(calls, ["previous-pending", "current"])
        pd.testing.assert_frame_equal(backend.tables["result"].drop(columns="embedding"), before)
        self.assertTrue(all(list(batch.columns) == ["embedding"] for batch in backend.updates))
        prepare_vectors(backend, settings, encoder=self.vector_engine(calls), target_assets={"ETH"})
        self.assertEqual(calls, ["previous-pending", "current"])


    def test_prepared_relation_missing_vectors_stops_before_claim_or_http(self):
        backend = self.backend([("2021-01-01", "missing", ["ETH"], "a")])
        with self.assertRaisesRegex(EmbeddingError, "先运行向量准备"):
            run_relation(backend, self.settings(start_time="2021-01-01", end_time="2021-01-31",
                         embedding_config_path=str(self.path)), ai_config=self.config(),
                         embedding_config=self.embedding_config(),
                         network_factory=lambda config: self.fail("不能启动 AI 请求"))
        self.assertFalse(backend.updates)


    def test_prepared_vectors_reject_version_mismatch_without_http(self):
        saved = {}
        engine = self.vector_engine([])
        engine.prepare([("key", "event", "")], saved.update)
        engine.config_hash = "different-version"
        engine.client_factory = lambda config: self.fail("不能重新请求向量")
        with self.assertRaisesRegex(EmbeddingError, "先运行向量准备"):
            engine.load_prepared([("key", "event", saved["key"])])


    def test_matrix_cache_matches_original_scores_groups_threshold_and_sort(self):
        engine = EmbeddingRetriever(self.embedding_config())
        rng = np.random.RandomState(29)
        raw = rng.normal(size=(70, 16))
        raw /= np.linalg.norm(raw, axis=1, keepdims=True)
        engine.vectors = {str(index): vector for index, vector in enumerate(raw)}
        records = [{"record_id": str(index), "event": str(index), "event_id": "group-{:02d}".format(index % 17)}
                   for index in range(1, 70)]
        for selected in (records[:30], records[4:35], records[10:60], records[2:12],
                         list(reversed(records[20:45])), records[:30]):
            matrix = np.stack([engine.vectors[record["event"]] for record in selected])
            scores = np.clip(matrix @ engine.vectors["0"], -1, 1)
            for threshold in (0, float(scores[0]), float(scores.max()) + 1e-10):
                engine.config = replace(engine.config, similarity_threshold=threshold)
                groups = {}
                for record, score in zip(selected, scores):
                    groups[record["event_id"]] = max(groups.get(record["event_id"], -float("inf")), float(score))
                expected = tuple(sorted((key for key in groups if groups[key] >= threshold),
                                        key=lambda key: (-groups[key], key))[:10])
                with patch("round2.embedding.np.stack", side_effect=AssertionError("不能重复拼装矩阵")):
                    actual = engine({"asset": "ETH", "event": "0"}, selected)
                self.assertEqual(actual.event_ids, expected)
                self.assertEqual(actual.auto_new, not bool(expected))
        cache = engine._matrices["ETH"]
        self.assertTrue(np.shares_memory(cache.select(records[5:10], engine.vectors), cache.data))


    def test_three_month_business_flow_matches_serial_candidates_and_repeat_results(self):
        source = tasks(["2021-01-31 23:59:59", "2021-02-01 00:00:00", "2021-03-01 00:00:00"])
        source["text"] = ["first", "update", "repeat"]
        backend = Tables(source)
        settings = self.settings(start_time="2021-01-01", end_time="2021-03-31",
                                 embedding_config_path=str(self.path))
        ai, embedding = self.config(), self.embedding_config()
        captured = []
        factory = lambda config: ImmediateNetwork(captured)
        months = tuple(month_ranges(date(2021, 1, 1), date(2021, 3, 31)))
        for start, end in months:
            new_posts, previous_posts = [], []

            def collect(new, previous):
                if not new.empty:
                    new_posts.append(new)
                if not previous.empty:
                    previous_posts.append(previous)

            run_extraction(backend, "first", str(self.path), start, end,
                           ai_config=ai, network_factory=factory, on_successful_posts=collect)
            current = replace(settings, start_time=start, end_time=end)
            import_event_records_from_posts(
                backend, current,
                new_posts=pd.concat(new_posts).sort_index() if new_posts else None,
                previous_posts=pd.concat(previous_posts).sort_index() if previous_posts else None)
        serial = Tables(source, backend.tables["result"])
        calls = []
        for start, end in months:
            current = replace(settings, start_time=start, end_time=end)
            prepare_vectors(backend, current, encoder=self.vector_engine(calls))
        serial.tables["result"] = backend.tables["result"].copy(deep=True)
        for start, end in months:
            current = replace(settings, start_time=start, end_time=end)
            run_relation(backend, current, ai_config=ai, network_factory=factory, embedding_config=embedding)
            run_impact(backend, current, ai_config=ai, network_factory=factory)
        monthly_requests = [value for value in captured if "current" in value]
        captured.clear()
        run_relation(serial, settings, ai_config=ai, network_factory=factory, embedding_config=embedding)
        run_impact(serial, settings, ai_config=ai, network_factory=factory)
        self.assertEqual(monthly_requests, [value for value in captured if "current" in value])
        columns = ["event_id", "event_relation", "new_information", "round2_status", "round3_status"]
        pd.testing.assert_frame_equal(backend.tables["result"][columns], serial.tables["result"][columns])
        self.assertEqual(list(backend.tables["result"]["event_relation"]), ["new", "update", "repeat"])
        self.assertEqual(set(backend.tables["result"]["round3_status"]), {"1"})
        for column in ("round2_ai_response", "round3_ai_response"):
            content = lambda frame: [json.loads(raw)["ai_response_content"] if raw else ""
                                     for raw in frame[column]]
            self.assertEqual(content(backend.tables["result"]), content(serial.tables["result"]))


if __name__ == "__main__":
    unittest.main()
