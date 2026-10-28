"""验证按月屏障、独立向量准备及 Windows spawn 调度，不连接真实服务。"""

import json
import os
import queue
import threading
import time
import unittest
from dataclasses import replace
from unittest.mock import patch

import httpx
import numpy as np
import pandas as pd

from common.config import EmbeddingConfig
from common.ai_protocol import parse_response
from common.network import CompletionMessage
from pipeline.runner import Month, PipelineConfig, PipelineError, STAGES, _run_lock, _worker, monthly_ranges, run_month, run_pipeline
from common.store import StageSummary
from round1.runner import ExtractionSummary
from round2.embedding import EmbeddingRetriever
from vectorization.storage import EmbeddingError
from vectorization.encoder import EmbeddingEncoder
from round2.runner import run_relation
from vectorization.preparation import prepare_vectors
from round3.runner import run_impact
from tests.event_support import EventFixture, Tables, source_posts, impact_result
from tests.support import success, tasks


class ImmediateNetwork:
    """执行真实业务校验的内存网络替身，离线覆盖完整三轮及 repeat 继承。"""

    def __init__(self, captured):
        self.captured = captured
        self.completions = queue.Queue()
        self.unpublished = []
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


def _null_client():
    return None


def _toy_month(stage, month, config, cli, network_factory, stop):
    begin = time.monotonic()
    if config.post_settings.get("fail_stage") == stage and month.key == "2021-02":
        raise ValueError("模拟分片失败")
    time.sleep(0.15 if stage == "round2" else 0.03)
    return month, {"begin": begin, "end": time.monotonic(), "pid": os.getpid()}


def _toy_worker(stage, config, months, incoming, outgoing, events, stop, client_factory):
    _worker(stage, config, months, incoming, outgoing, events, stop, client_factory, _toy_month)


def _early_exit_worker(stage, config, months, incoming, outgoing, events, stop, client_factory):
    if stage != "round1":
        _toy_worker(stage, config, months, incoming, outgoing, events, stop, client_factory)


class MonthlyPipelineTests(EventFixture, unittest.TestCase):
    def pipeline_config(self, **post_values):
        post = {"task_path": "first", "ai_config_path": str(self.path),
                "start_time": "2021-01-01", "end_time": "2021-03-31"}
        post.update(post_values)
        return PipelineConfig(post, self.settings(embedding_config_path=str(self.path), logging={"level": "CRITICAL"}),
                              (self.config(),) * 3,
                              EmbeddingConfig(base_url="https://mock.invalid/v1", api_key="not-real"))

    def vector_engine(self, calls):
        def handle(request):
            texts = json.loads(request.content)["input"]
            calls.extend(texts)
            return httpx.Response(200, json={"data": [
                {"index": index, "embedding": [1.0, 0.0]} for index, _ in enumerate(texts)]})
        return EmbeddingEncoder(self.pipeline_config().embedding_config, client_factory=lambda config:
                                  httpx.Client(transport=httpx.MockTransport(handle)))

    def test_month_ranges_preserve_leap_year_and_partial_boundary(self):
        config = self.pipeline_config(start_time="2020-01-20", end_time="2020-03-05")
        self.assertEqual([(month.start_time, month.end_time) for month in monthly_ranges(config)],
                         [("2020-01-20", "2020-01-31"), ("2020-02-01", "2020-02-29"),
                          ("2020-03-01", "2020-03-05")])
        for values in ({"start_time": None}, {"end_time": "2020-01-01"}, {"start_time": "2021-1-1"}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                monthly_ranges(self.pipeline_config(**values))

    def test_round1_failure_blocks_import_even_if_other_posts_succeeded(self):
        summary = ExtractionSummary(total=2, new_succeeded=1, new_failed=1)
        with patch("pipeline.runner.run_extraction", return_value=summary), \
                patch("pipeline.runner.import_event_records_from_posts") as importer:
            with self.assertRaisesRegex(PipelineError, "禁止交接"):
                run_month("round1", Month("2021-01-01", "2021-01-31"), self.pipeline_config(), None)
        importer.assert_not_called()

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

    def test_round1_complete_then_import_and_idempotent_resume(self):
        source = source_posts([("2021-01-01", "fact", ["ETH"], "a")])
        backend = Tables(source)

        def extract(cli, path, config_path, start, end, **kwargs):
            self.assertNotIn("result", backend.tables)
            kwargs["on_successful_posts"](source, source.iloc[:0])
            return ExtractionSummary(total=1, new_succeeded=1)

        month = Month("2021-01-01", "2021-01-31")
        with patch("pipeline.runner.run_extraction", side_effect=extract):
            result, summary = run_month("round1", month, self.pipeline_config(), backend)
        self.assertFalse(result.empty)
        self.assertEqual((summary["event_count"], summary["imported"]), (1, 1))

        def resume(cli, path, config_path, start, end, **kwargs):
            kwargs["on_successful_posts"](source.iloc[:0], source)
            return ExtractionSummary(total=1, already_succeeded=1)

        with patch("pipeline.runner.run_extraction", side_effect=resume):
            _, summary = run_month("round1", month, self.pipeline_config(), backend)
        self.assertEqual(summary["imported"], 0)
        self.assertEqual(len(backend.tables["result"]), 1)

    def test_empty_month_can_flow_without_creating_an_event_table(self):
        month = Month("2021-01-01", "2021-01-31")
        with patch("pipeline.runner.run_extraction", return_value=ExtractionSummary()):
            empty, _ = run_month("round1", month, self.pipeline_config(), None)
        self.assertTrue(empty.empty)
        for stage in STAGES[1:]:
            self.assertEqual(run_month(stage, empty, self.pipeline_config(), None), (empty, {"empty": True}))

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
                         embedding_config=self.pipeline_config().embedding_config,
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

    def test_incomplete_round2_summary_is_not_released_to_round3(self):
        with patch("pipeline.runner.run_relation", return_value=StageSummary("round2", total=1, failed=1)):
            with self.assertRaisesRegex(PipelineError, "禁止交接"):
                run_month("round2", Month("2021-01-01", "2021-01-31"), self.pipeline_config(), None)

    def test_run_lock_blocks_duplicate_and_releases_on_exit(self):
        path = self.path.parent / "pipeline.lock"
        with _run_lock(path):
            with self.assertRaisesRegex(PipelineError, "已有月流水线"):
                with _run_lock(path):
                    self.fail("不能重复获取锁")
        with _run_lock(path):
            pass

    def test_matrix_cache_matches_original_scores_groups_threshold_and_sort(self):
        engine = EmbeddingRetriever(self.pipeline_config().embedding_config)
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
        backend, config = Tables(source), self.pipeline_config()
        captured = []
        factory = lambda ai: ImmediateNetwork(captured)
        months = monthly_ranges(config)
        for month in months:
            run_month("round1", month, config, backend, factory)
        serial = Tables(source, backend.tables["result"])
        calls = []
        for month in months:
            settings = replace(config.event_settings, start_time=month.start_time, end_time=month.end_time)
            prepare_vectors(backend, settings, encoder=self.vector_engine(calls))
        serial.tables["result"] = backend.tables["result"].copy(deep=True)
        for month in months:
            run_month("round2", month, config, backend, factory)
            run_month("round3", month, config, backend, factory)
        monthly_requests = [value for value in captured if "current" in value]
        captured.clear()
        settings = replace(config.event_settings, start_time="2021-01-01", end_time="2021-03-31")
        run_relation(serial, settings, ai_config=config.ai_configs[1], network_factory=factory,
                     embedding_config=config.embedding_config)
        run_impact(serial, settings, ai_config=config.ai_configs[2], network_factory=factory)
        self.assertEqual(monthly_requests, [value for value in captured if "current" in value])
        columns = ["event_id", "event_relation", "new_information", "round2_status", "round3_status"]
        pd.testing.assert_frame_equal(backend.tables["result"][columns], serial.tables["result"][columns])
        self.assertEqual(list(backend.tables["result"]["event_relation"]), ["new", "update", "repeat"])
        self.assertEqual(set(backend.tables["result"]["round3_status"]), {"1"})
        for column in ("round2_ai_response", "round3_ai_response"):
            content = lambda frame: [json.loads(raw)["ai_response_content"] if raw else ""
                                     for raw in frame[column]]
            self.assertEqual(content(backend.tables["result"]), content(serial.tables["result"]))

    def test_spawn_pipeline_handoffs_are_ordered_and_stages_overlap(self):
        report = self.path.parent / "pipeline"
        result = run_pipeline(self.pipeline_config(), report, client_factory=_null_client, worker_target=_toy_worker)
        self.assertEqual(result["status"], "completed")
        stages = result["months"]
        for key in stages:
            for left, right in zip(STAGES, STAGES[1:]):
                self.assertLessEqual(stages[key]["stages"][left]["summary"]["end"],
                                     stages[key]["stages"][right]["summary"]["begin"])
        self.assertLess(stages["2021-02"]["stages"]["round1"]["summary"]["begin"],
                        stages["2021-01"]["stages"]["round2"]["summary"]["end"])
        self.assertEqual(len({stages["2021-01"]["stages"][stage]["summary"]["pid"] for stage in STAGES}), 4)

    def test_failure_blocks_month_and_resume_checks_identity(self):
        report = self.path.parent / "pipeline"
        config = self.pipeline_config(fail_stage="round1")
        with self.assertRaisesRegex(PipelineError, "模拟分片失败"):
            run_pipeline(config, report, client_factory=_null_client, worker_target=_toy_worker)
        result = json.loads((report / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["months"]["2021-02"]["stages"]["round2"]["status"], "pending")
        with self.assertRaisesRegex(PipelineError, "不一致"):
            run_pipeline(self.pipeline_config(end_time="2021-02-28"), report, resume=True,
                         client_factory=_null_client, worker_target=_toy_worker)
        result = run_pipeline(self.pipeline_config(), report, resume=True,
                              client_factory=_null_client, worker_target=_toy_worker)
        self.assertEqual((result["status"], result["attempt"]), ("completed", 2))

    def test_worker_exit_without_done_is_detected_even_with_zero_exit_code(self):
        with self.assertRaisesRegex(PipelineError, "异常退出"):
            run_pipeline(self.pipeline_config(), self.path.parent / "pipeline", client_factory=_null_client,
                         worker_target=_early_exit_worker)


if __name__ == "__main__":
    unittest.main()
