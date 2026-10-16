"""验证表内向量保存与复用、阈值及历史边界；不访问真实服务。"""

import json
import os
import unittest
from dataclasses import replace
from unittest.mock import patch

import httpx
import numpy as np
import pandas as pd

from common.config import (
    ConfigError,
    EmbeddingConfig,
    ai_config_hash,
    load_embedding_config,
    load_event_config,
)
from common.event_context import EventHistory
from common.persistence import BatchPersistenceError
from common.store import EventRecordStore
from common.validation import RESULT_COLUMNS
from round2.embedding import EmbeddingError, EmbeddingRetriever
from round2.runner import run_relation
from round3.runner import run_impact
from tests.event_support import EventFixture, impact_result, new_result
from tests.support import request_text, success


class EmbeddingTests(EventFixture, unittest.TestCase):
    def embedding_config(self, **changes):
        config = EmbeddingConfig("https://embedding.invalid/v1", "secret",
                                 backoff_base_seconds=0.001, backoff_max_seconds=0.002)
        return replace(config, **changes)

    def retriever(self, handler, config=None):
        self.clients = []
        def create(settings):
            client = httpx.Client(transport=httpx.MockTransport(handler), trust_env=False)
            self.clients.append(client)
            return client
        return EmbeddingRetriever(config or self.embedding_config(), create)

    def prepare(self, retriever, texts, saved=None):
        saved = {} if saved is None else saved
        retriever.prepare(((text, text, saved.get(text, "")) for text in dict.fromkeys(texts)), saved.update)
        return saved

    def vector_handler(self, vectors, calls):
        def handle(request):
            body = json.loads(request.content)
            self.assertEqual(str(request.url), "https://embedding.invalid/v1/embeddings")
            self.assertEqual(request.headers["Authorization"], "Bearer secret")
            self.assertEqual((body["model"], body["encoding_format"]), ("bge-m3", "float"))
            calls.append(body["input"])
            return httpx.Response(200, json={"data": [
                {"index": index, "embedding": vectors[text]}
                for index, text in reversed(list(enumerate(body["input"])))
            ]})
        return handle

    def test_initialization_has_empty_embedding_column_and_rejects_old_schema(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        frame = backend.tables["result"]
        self.assertEqual(len(RESULT_COLUMNS), 14)
        self.assertEqual(list(frame["embedding"]), [""])
        backend.tables["result"] = frame.drop(columns="embedding")
        with self.assertRaisesRegex(ValueError, "embedding"):
            EventRecordStore(backend, self.settings())
        self.assertFalse(backend.updates)

    def test_batch_eight_tail_reordering_and_table_restart_reuse(self):
        vectors = {"text-{}".format(index): [index + 1, 1] for index in range(10)}
        vectors["text-0"] = np.random.RandomState(53).normal(size=2).tolist()
        vectors = {text: (np.asarray(raw) / np.linalg.norm(raw)).tolist() for text, raw in vectors.items()}
        # 模拟服务单位向量的小量舍入误差，检查保存和复用时均不重新缩放分量。
        vectors["text-0"] = [value * (1 + 1e-6) for value in vectors["text-0"]]
        calls = []
        first = self.retriever(self.vector_handler(vectors, calls))
        before = set(self.path.parent.iterdir())
        saved = self.prepare(first, list(vectors) * 2)
        self.assertEqual(set(self.path.parent.iterdir()), before)
        self.assertEqual([len(batch) for batch in calls], [8, 2])
        for text, raw in vectors.items():
            body = json.loads(saved[text])
            self.assertEqual(set(body), {"model", "model_version", "config_hash", "vector"})
            self.assertEqual((body["model"], body["model_version"]), ("bge-m3", "bge-m3-v1"))
            self.assertEqual(len(body["config_hash"]), 64)
            self.assertEqual(body["vector"], raw)
            np.testing.assert_array_equal(first.vectors[text], raw)
        self.assertTrue(all(client.is_closed for client in self.clients))
        self.assertEqual(len({json.loads(value)["config_hash"] for value in saved.values()}), 1)
        second = self.retriever(lambda req: self.fail("表内向量命中不应请求接口"))
        with patch("builtins.open", side_effect=AssertionError("不能使用本地缓存")):
            self.prepare(second, reversed(list(vectors)), saved)
        self.assertFalse(self.clients)
        for text in vectors:
            np.testing.assert_array_equal(second.vectors[text], first.vectors[text])

    def test_same_text_multiple_rows_reuses_existing_vector_and_saves_only_missing_rows(self):
        initial = self.retriever(self.vector_handler({"same": [1, 0]}, []))
        saved = self.prepare(initial, ["same"])
        updated = {}
        second = self.retriever(lambda req: self.fail("相同文本应复用已有向量"))
        second.prepare([("new-asset", "same", ""), ("old-asset", "same", saved["same"])], updated.update)
        self.assertEqual(updated, {"new-asset": saved["same"]})
        self.assertFalse(self.clients)
        calls, updated = [], {}
        third = self.retriever(self.vector_handler({"fresh": [0, 1]}, calls))
        third.prepare([("ETH", "fresh", ""), ("BTC", "fresh", "")], updated.update)
        self.assertEqual(calls, [["fresh"]])
        self.assertEqual(updated["ETH"], updated["BTC"])

    def test_identity_change_regenerates_but_threshold_change_reuses_vectors(self):
        calls = []
        handler = self.vector_handler({"one": [1, 0]}, calls)
        baseline = self.prepare(self.retriever(handler), ["one"])
        self.prepare(self.retriever(handler, self.embedding_config(similarity_threshold=0.9)), ["one"], dict(baseline))
        self.assertEqual(calls, [["one"]])
        for changes in ({"model_version": "v2"}, {"model": "other"}, {"base_url": "https://other.invalid/v1"}):
            seen = []
            def changed(request):
                seen.append(request)
                return httpx.Response(200, json={"data": [{"index": 0, "embedding": [1, 0]}]})
            self.prepare(self.retriever(changed, self.embedding_config(**changes)), ["one"], dict(baseline))
            self.assertEqual(len(seen), 1)
        other = self.prepare(self.retriever(self.vector_handler({"two": [0, 1]}, [])), ["two"])
        self.assertEqual(json.loads(other["two"])["config_hash"], json.loads(baseline["one"])["config_hash"])

    def test_invalid_response_exhausts_shared_budget_without_saving_partial_vectors(self):
        cases = [None, [], {"error": "bad"}, {"data": []}]
        cases += [{"data": [{"index": index, "embedding": [1]}]} for index in (None, False, -1, 1)]
        cases += [{"data": [{"index": 0, "embedding": vector}]} for vector in
                  ([], [0, 0], [2, 0], [1e308], [1e-300], [True, 1], ["1"], [[1]],
                   [float("nan")], [float("inf")])]
        for body in cases:
            with self.subTest(body=body):
                calls, saved = [], {}
                def handle(request):
                    calls.append(request)
                    return httpx.Response(200, text=json.dumps(body))
                retriever = self.retriever(handle)
                with self.assertRaises(EmbeddingError):
                    self.prepare(retriever, ["one"], saved)
                self.assertEqual(len(calls), 3)
                self.assertFalse(saved or retriever.vectors)
                self.assertTrue(all(client.is_closed for client in self.clients))

    def test_duplicate_index_and_mixed_dimensions_reject_entire_batch(self):
        for indexes, vectors in (([0, 0], [[1, 0], [0, 1]]), ([0, 1], [[1], [0, 1]])):
            with self.subTest(indexes=indexes, vectors=vectors):
                handler = lambda req: httpx.Response(200, json={"data": [
                    {"index": index, "embedding": vector} for index, vector in zip(indexes, vectors)]})
                retriever, saved = self.retriever(handler), {}
                with self.assertRaises(EmbeddingError):
                    self.prepare(retriever, ["a", "b"], saved)
                self.assertFalse(saved or retriever.vectors)

    def test_failed_later_batch_preserves_saved_rows_and_restart_generates_only_missing(self):
        config = self.embedding_config(batch_size=1)
        saved = {}
        def mixed(request):
            text = json.loads(request.content)["input"][0]
            return httpx.Response(200, json={"data": [{"index": 0, "embedding": [1, 0] if text == "a" else [1]}]})
        with self.assertRaisesRegex(EmbeddingError, "维度"):
            self.prepare(self.retriever(mixed, config), ["a", "b"], saved)
        self.assertEqual(set(saved), {"a"})
        calls = []
        self.prepare(self.retriever(self.vector_handler({"b": [0, 1]}, calls), config), ["a", "b"], saved)
        self.assertEqual(calls, [["b"]])
        self.assertEqual(set(saved), {"a", "b"})

    def test_retry_budget_shared_by_network_and_5xx_and_4xx_stops_immediately(self):
        calls = []
        def handle(request):
            calls.append(request)
            if len(calls) == 1:
                raise httpx.ReadTimeout("timeout", request=request)
            return httpx.Response(503)
        retriever = self.retriever(handle)
        with patch("round2.embedding.time.sleep") as sleep, patch("common.network.random.uniform", side_effect=lambda low, high: high):
            with self.assertRaisesRegex(EmbeddingError, "503"):
                self.prepare(retriever, ["one"])
        self.assertEqual(len(calls), 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.001, 0.002])
        for status in (401, 429, 400, 302):
            calls.clear()
            def stop(request):
                calls.append(request)
                return httpx.Response(status)
            with self.assertRaisesRegex(EmbeddingError, str(status)):
                self.prepare(self.retriever(stop), ["one"])
            self.assertEqual(len(calls), 1)

    def test_invalid_output_network_and_5xx_share_one_batch_budget(self):
        calls, saved = [], {}
        def handle(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(200, text="not JSON")
            if len(calls) == 2:
                raise httpx.ReadTimeout("timeout", request=request)
            return httpx.Response(503)
        with patch("round2.embedding.time.sleep") as sleep:
            with self.assertRaisesRegex(EmbeddingError, "503"):
                self.prepare(self.retriever(handle), ["one"], saved)
        self.assertEqual((len(calls), sleep.call_count), (3, 2))
        self.assertFalse(saved)

    def test_5xx_retry_after_and_invalid_output_can_recover_before_budget_exhaustion(self):
        calls, saved = [], {}
        def handle(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(503, headers={"Retry-After": "5"})
            if len(calls) == 2:
                return httpx.Response(200, json={"data": [{"index": 0, "embedding": [0, 0]}]})
            return httpx.Response(200, json={"data": [{"index": 0, "embedding": [0.6, 0.8]}]})
        with patch("round2.embedding.time.sleep") as sleep:
            self.prepare(self.retriever(handle), ["one"], saved)
        self.assertEqual((len(calls), sleep.call_count), (3, 2))
        self.assertEqual(sleep.call_args_list[0].args[0], 5)
        self.assertLessEqual(sleep.call_args_list[1].args[0], 0.002)
        np.testing.assert_allclose(json.loads(saved["one"])["vector"], [0.6, 0.8], rtol=1e-6)

    def test_1024_dimension_unit_dot_matches_cosine_of_raw_vectors(self):
        raw = np.random.RandomState(71).normal(size=(2, 1024))
        expected = np.dot(raw[0], raw[1]) / (np.linalg.norm(raw[0]) * np.linalg.norm(raw[1]))
        # 模拟模型在服务端归一化，程序只接收单位向量并直接计算内积。
        unit = raw / np.linalg.norm(raw, axis=1, keepdims=True)
        retriever = self.retriever(self.vector_handler({"query": unit[0].tolist(), "old": unit[1].tolist()}, []))
        self.prepare(retriever, ["query", "old"])
        actual = np.dot(retriever.vectors["query"], retriever.vectors["old"])
        self.assertEqual(retriever.dimension, 1024)
        self.assertAlmostEqual(float(actual), float(expected), places=7)
        records = [{"event": "old", "event_id": "old-group"}]
        retriever.config = replace(retriever.config, similarity_threshold=float(actual) - 1e-5)
        self.assertEqual(retriever({"event": "query"}, records).event_ids, ("old-group",))
        retriever.config = replace(retriever.config, similarity_threshold=float(actual) + 1e-5)
        self.assertTrue(retriever({"event": "query"}, records).auto_new)

    def test_group_maximum_top_ten_and_ties_do_not_reward_repeated_disclosures(self):
        vectors = {"query": [1, 0], "strong": [1, 0], "weak": [0.1, np.sqrt(0.99)]}
        vectors.update({"other-{}".format(index): [0.5, np.sqrt(0.75)] for index in range(11)})
        retriever = self.retriever(self.vector_handler(vectors, []), self.embedding_config(similarity_threshold=0))
        self.prepare(retriever, vectors)
        records = [{"event": "weak", "event_id": "frequent", "new_information": "query"}] * 30
        records += [{"event": "strong", "event_id": "best"}]
        records += [{"event": "other-{}".format(index), "event_id": "group-{:02d}".format(index)} for index in range(11)]
        selection = retriever({"event": "query", "source_text": "unused"}, records)
        self.assertEqual(selection.event_ids, ("best",) + tuple("group-{:02d}".format(index) for index in range(9)))
        self.assertFalse(selection.auto_new)

    def test_threshold_filters_groups_includes_equal_boundary_and_all_low_auto_new(self):
        vectors = {"query": [1, 0], "below": [0.49, np.sqrt(1 - 0.49 ** 2)],
                   "equal": [0.5, np.sqrt(0.75)], "high": [0.8, 0.6]}
        retriever = self.retriever(self.vector_handler(vectors, []))
        self.prepare(retriever, vectors)
        records = [{"event": "below", "event_id": "progress"}, {"event": "equal", "event_id": "progress"},
                   {"event": "below", "event_id": "low"}, {"event": "high", "event_id": "high"}]
        selection = retriever({"event": "query"}, records)
        self.assertEqual(selection.event_ids, ("high", "progress"))
        self.assertFalse(selection.auto_new)
        low = retriever({"event": "query"}, [records[2]])
        self.assertTrue(low.auto_new)
        self.assertEqual(low.event_ids, ())
        self.assertIn("similarity_threshold=0.5", low.reason)
        self.assertIn("max_similarity=", low.reason)

    def test_empty_history_auto_new_and_unprepared_nonempty_history_stops(self):
        retriever = self.retriever(lambda req: self.fail("评分期间不应请求接口"))
        selection = retriever({"event": "query"}, ())
        self.assertTrue(selection.auto_new)
        self.assertEqual(selection.event_ids, ())
        self.assertTrue(selection.reason)
        with self.assertRaisesRegex(EmbeddingError, "准备"):
            retriever({"event": "query"}, [{"event": "history", "event_id": "old"}])
        self.assertFalse(self.clients)

    def test_time_window_includes_lower_boundary_excludes_peers_future_and_other_assets(self):
        current = pd.Timestamp("2021-01-02 12:00:00")
        begin = pd.Timestamp("2016-01-02 12:00:00")
        backend = self.backend([(str(begin - pd.Timedelta(seconds=1)), "outside", ["ETH"], "a"),
                                (str(begin), "boundary", ["ETH"], "a"), (str(current), "peer", ["ETH"], "a"),
                                (str(current + pd.Timedelta(seconds=1)), "future", ["ETH"], "a"),
                                (str(current - pd.Timedelta(seconds=1)), "other", ["BTC"], "a")])
        frame = backend.tables["result"]
        frame["round2_status"], frame["event_relation"] = "1", "new"
        frame["event_id"] = frame.index.get_level_values("record_id")
        records = EventHistory(frame).get_searchable_records("ETH", current)
        calls = []
        retriever = self.retriever(self.vector_handler({"current": [1, 0], "boundary": [1, 0]}, calls))
        self.prepare(retriever, ["current"] + [row["event"] for row in records])
        self.assertEqual(retriever({"event": "current"}, records).event_ids, ("101:0:ETH",))
        self.assertEqual(calls, [["current", "boundary"]])

    def test_corrupt_saved_vector_stops_without_request_or_auto_repair(self):
        valid = self.prepare(self.retriever(self.vector_handler({"a": [1, 0]}, [])), ["a"])["a"]
        body = json.loads(valid)
        cases = [None, pd.NA, "bad", "[]", "{}"]
        cases += [json.dumps(dict(body, vector=value)) for value in ([0, 0], [2, 0], [True], [], [float("inf")])]
        for value in cases:
            with self.subTest(value=value):
                retriever = self.retriever(lambda req: self.fail("损坏向量须停止"))
                with self.assertRaises(EmbeddingError):
                    retriever.prepare([("a", "a", value)], lambda values: self.fail("不得自动修复损坏内容"))

    def test_configuration_env_priority_threshold_and_removed_cache_parameter(self):
        document = {"base_url": "https://embedding.invalid/v1/", "api_key": "$" + "{TEST_EMBEDDING_KEY}"}
        path = self.path.parent / "embedding.yaml"
        (path.parent / ".env").write_text("TEST_EMBEDDING_KEY=file-secret\n", encoding="utf-8")
        path.write_text(json.dumps(document), encoding="utf-8")
        with patch.dict(os.environ, {"TEST_EMBEDDING_KEY": "process-secret"}):
            config = load_embedding_config(path)
            self.assertEqual(config.api_key, "process-secret")
            self.assertNotIn("process-secret", repr(config))
            self.assertEqual(config.base_url, "https://embedding.invalid/v1")
            self.assertEqual(config.similarity_threshold, 0.5)
            for changes in ({"base_url": ""}, {"base_url": "https://a:b@host/v1"}, {"base_url": "https://host/v1?token=secret"},
                            {"batch_size": False}, {"max_attempts": 0}, {"timeout_seconds": -1},
                            {"backoff_max_seconds": 0.1}, {"trust_env": "false"}, {"model_version": ""},
                            {"cache_path": "obsolete"}, {"similarity_threshold": True},
                            {"similarity_threshold": float("nan")}, {"similarity_threshold": -1.01},
                            {"similarity_threshold": 1.01}):
                with self.subTest(changes=changes):
                    path.write_text(json.dumps(dict(document, **changes)), encoding="utf-8")
                    with self.assertRaises(ConfigError):
                        load_embedding_config(path)
        event_path = path.parent / "event.yaml"
        event_path.write_text(json.dumps({"first_round_task_path": "first", "result_path": "result",
                                         "round2_ai_config_path": "two.yaml", "round3_ai_config_path": "three.yaml",
                                         "embedding_config_path": "embedding.yaml"}), encoding="utf-8")
        self.assertEqual(load_event_config(event_path).embedding_config_path, str(path))

    def test_default_runner_saves_vectors_before_claim_then_filters_candidates(self):
        backend = self.backend([("2021-01-01 12:00:00", "plan", ["ETH", "LEND"], "a"),
                                ("2021-01-01 12:00:00", "peer", ["ETH"], "a"),
                                ("2021-01-02 12:00:00", "execute", ["ETH"], "a")])
        calls, seen, clients = [], [], []
        def handle(request):
            self.assertFalse(backend.updates)
            return self.vector_handler({"plan": [1, 0], "peer": [0, 1], "execute": [1, 0]}, calls)(request)
        def create(config):
            client = httpx.Client(transport=httpx.MockTransport(handle))
            clients.append(client)
            return client
        def relation(request):
            body = json.loads(request_text(request))
            seen.append(body)
            self.assertTrue(all(client.is_closed for client in clients))
            self.assertTrue(backend.tables["result"]["embedding"].ne("").all())
            return success(json.dumps({"event_relation": "update", "matched_candidate_id": body["candidates"][0]["candidate_id"],
                                       "new_information": "开始执行"}))
        settings = self.relation_settings(backend, embedding_config_path="unused.yaml")
        with patch("round2.runner.load_embedding_config", return_value=self.embedding_config()), patch("round2.embedding._create_client", create):
            summary = run_relation(backend, settings, ai_config=self.config(), network_factory=self.factory(relation))
        self.assertEqual(calls, [["plan", "peer", "execute"]])
        self.assertEqual((summary.http_attempts_started, summary.new_completed), (1, 4))
        self.assertEqual(len(seen), 1)
        self.assertEqual([item["history"] for item in seen[0]["candidates"]], [["plan"]])
        self.assertNotIn('"embedding"', json.dumps(seen))
        self.assertEqual(list(backend.updates[0].columns), ["embedding"])
        with patch("round2.runner.load_embedding_config", side_effect=AssertionError("完成后不加载 Embedding")):
            again = run_relation(backend, settings)
        self.assertEqual(again.already_completed, 4)

    def test_embedding_failure_before_claiming_closes_client(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        clients = []
        def create(config):
            client = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(401)))
            clients.append(client)
            return client
        with patch("round2.runner.load_embedding_config", return_value=self.embedding_config()), patch("round2.embedding._create_client", create):
            with self.assertRaisesRegex(EmbeddingError, "401"):
                run_relation(backend, self.relation_settings(backend, embedding_config_path="unused"), ai_config=self.config())
        self.assertFalse(backend.updates)
        self.assertTrue(backend.tables["result"]["round2_status"].eq("0").all())
        self.assertTrue(all(client.is_closed for client in clients))

    def test_runner_encodes_years_old_update_and_does_not_read_old_origin(self):
        backend = self.backend([("2010-01-01", "origin", ["ETH"], "a"),
                                ("2018-01-01", "progress", ["ETH"], "a"),
                                ("2021-01-02", "current", ["ETH"], "a")])
        frame = backend.tables["result"]
        root, update, current = frame.index
        frame.loc[root, ["event_id", "event_relation", "round2_status"]] = [root[1], "new", "1"]
        frame.loc[update, ["event_id", "event_relation", "new_information", "round2_status"]] = [root[1], "update", "执行", "1"]
        frame.loc[[root, update], "round2_config_hash"] = ai_config_hash(self.config())
        settings = self.settings(start_time="2021-01-02", end_time="2021-01-02", embedding_config_path="unused")
        calls, seen = [], []
        handler = self.vector_handler({"current": [1, 0], "progress": [1, 0]}, calls)
        def relation(request):
            seen.append(json.loads(request_text(request)))
            return success(json.dumps({"event_relation": "repeat", "matched_candidate_id": "E1", "new_information": None}))
        with patch("round2.runner.load_embedding_config", return_value=self.embedding_config()), \
                patch("round2.embedding._create_client", side_effect=lambda config: httpx.Client(transport=httpx.MockTransport(handler))):
            summary = run_relation(backend, settings, ai_config=self.config(), network_factory=self.factory(relation))
        self.assertEqual(calls, [["progress", "current"]])
        candidate = seen[0]["candidates"][0]
        self.assertEqual(candidate, {"candidate_id": "E1", "history": ["progress"]})
        self.assertEqual(summary.new_completed, 1)
        self.assertEqual(frame.loc[current, "round3_status"], "1")
        self.assertEqual(frame.loc[root, "embedding"], "")

    def test_runner_prepares_vectors_at_exact_leap_year_boundary(self):
        current = pd.Timestamp("2024-02-29 12:00:00.123456789")
        begin = pd.Timestamp("2019-02-28 12:00:00.123456789")
        backend = self.backend([(str(begin - pd.Timedelta(nanoseconds=1)), "outside", ["ETH"], "a"),
                                (str(begin), "boundary", ["ETH"], "a"),
                                (str(current), "current", ["ETH"], "a")])
        frame = backend.tables["result"]
        for key in frame.index[:2]:
            frame.loc[key, ["event_id", "event_relation", "round2_status"]] = [key[1], "new", "1"]
        calls, seen = [], []
        handler = self.vector_handler({"boundary": [1, 0], "current": [1, 0]}, calls)
        settings = self.settings(start_time="2024-02-29", end_time="2024-02-29", embedding_config_path="unused")
        with patch("round2.runner.load_embedding_config", return_value=self.embedding_config()), \
                patch("round2.embedding._create_client", side_effect=lambda config: httpx.Client(transport=httpx.MockTransport(handler))):
            result = run_relation(backend, settings, ai_config=self.config(), network_factory=self.factory(
                lambda request: seen.append(json.loads(request_text(request))) or success(json.dumps(new_result()))))
        self.assertEqual(calls, [["boundary", "current"]])
        self.assertEqual(result.new_completed, 1)
        self.assertEqual(seen[0]["candidates"][0]["history"], ["boundary"])
        self.assertEqual(frame.iloc[0]["embedding"], "")

    def test_vector_write_retries_do_not_repeat_embedding_request(self):
        for fail_all in (False, True):
            with self.subTest(fail_all=fail_all):
                backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
                calls, attempts = [], []
                def update(path, frame):
                    if list(frame.columns) == ["embedding"]:
                        attempts.append(frame.copy(deep=True))
                        if fail_all or len(attempts) == 1:
                            raise RuntimeError("not written")
                    backend.apply(path, frame)
                backend.update_hook = update
                handler = self.vector_handler({"one": [1, 0]}, calls)
                with patch("round2.runner.load_embedding_config", return_value=self.embedding_config()), \
                        patch("round2.embedding._create_client", side_effect=lambda config: httpx.Client(transport=httpx.MockTransport(handler))):
                    operation = lambda: run_relation(backend, self.relation_settings(backend, embedding_config_path="unused"), ai_config=self.config(),
                                                     network_factory=self.factory(lambda req: self.fail("无历史应跳过第二轮")))
                    if fail_all:
                        with self.assertRaises(BatchPersistenceError):
                            operation()
                    else:
                        self.assertEqual(operation().new_completed, 1)
                self.assertEqual(calls, [["one"]])
                self.assertEqual(len(attempts), 3 if fail_all else 2)
                self.assertEqual(backend.tables["result"].iloc[0]["round2_status"], "0" if fail_all else "1")
                self.assertEqual(backend.tables["result"].iloc[0]["embedding"] == "", fail_all)

    def test_embedding_store_only_updates_new_column_and_preserves_all_other_fields(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        before = backend.tables["result"].copy(deep=True)
        store = EventRecordStore(backend, self.settings())
        store.config = self.config()
        key = before.index[0]
        store.save_embeddings({key: '{"test":"vector"}'})
        self.assertEqual(list(backend.updates[-1].columns), ["embedding"])
        pd.testing.assert_frame_equal(backend.tables["result"].drop(columns="embedding"), before.drop(columns="embedding"))
        pd.testing.assert_frame_equal(store.df, backend.tables["result"])
        with self.assertRaisesRegex(ValueError, "当前阶段"):
            store.update_stage_columns([key], {"embedding": ""}, "round3")

    def test_all_low_scores_create_new_events_and_still_run_third_round(self):
        backend = self.backend([("2021-01-01", "first", ["ETH"], "a"), ("2021-01-02", "second", ["ETH"], "a")])
        calls, seen = [], []
        handler = self.vector_handler({"first": [1, 0], "second": [0, 1]}, calls)
        settings = self.relation_settings(backend, embedding_config_path="unused")
        with patch("round2.runner.load_embedding_config", return_value=self.embedding_config()), \
                patch("round2.embedding._create_client", side_effect=lambda config: httpx.Client(transport=httpx.MockTransport(handler))):
            summary = run_relation(backend, settings, ai_config=self.config(),
                                   network_factory=self.factory(lambda req: self.fail("低分应跳过第二轮")))
        self.assertEqual(summary.http_attempts_started, 0)
        self.assertTrue(backend.tables["result"]["event_relation"].eq("new").all())
        self.assertTrue(backend.tables["result"]["round3_status"].eq("0").all())
        vectors = backend.tables["result"]["embedding"].copy()
        def impact(request):
            seen.append(json.loads(request_text(request)))
            return success(json.dumps(impact_result()))
        result = run_impact(backend, settings, ai_config=self.config(), network_factory=self.factory(impact))
        self.assertEqual((result.new_completed, result.http_attempts_started), (2, 2))
        self.assertNotIn('"embedding"', json.dumps(seen))
        pd.testing.assert_series_equal(vectors, backend.tables["result"]["embedding"])
