"""验证研究流程的历史边界、任务状态、重试与续跑；不访问真实服务。"""

import asyncio
import json
import threading
import unittest
import weakref
from unittest.mock import patch

import httpx
import pandas as pd

from common.config import ai_config_hash
from common.network import BatchRunError
from common.persistence import BatchPersistenceError
from round2.runner import run_relation
from round3.runner import run_impact
from common.validation import HASH_COLUMNS, validate_impact_output
from tests.support import request_text, success
from tests.event_support import EventFixture, new_result, impact_result


class ImpactRunnerTests(EventFixture, unittest.TestCase):
    def test_impact_missing_later_source_preserves_saved_earlier_month(self):
        backend = self.backend([("2021-01-01", "jan", ["ETH"], "a"), ("2021-02-01", "feb", ["ETH"], "a")])
        self.relate(backend)
        backend.tables["first"] = backend.tables["first"].iloc[:1].copy(deep=True)
        seen = []
        def handler(request):
            seen.append(json.loads(request_text(request))["current"]["event"])
            return success(json.dumps(impact_result()))
        with self.assertRaisesRegex(ValueError, "原帖不存在"):
            run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-02-01"),
                       ai_config=self.config(), network_factory=self.factory(handler))
        self.assertEqual(seen, ["jan"])
        frame = backend.tables["result"]
        self.assertEqual(list(frame["round3_status"]), ["1", "0"])
        self.assertEqual(json.loads(json.loads(frame.iloc[0]["round3_ai_response"])["ai_response_content"])["materiality_reason"], impact_result()["materiality_reason"])
        self.assertTrue(frame["round2_status"].eq("1").all())
        self.assertFalse(any(thread.name == "ai-batch-network" for thread in threading.enumerate()))

    def test_impact_requires_all_previous_records_complete(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        with self.assertRaisesRegex(ValueError, "第二轮未完成"):
            run_impact(backend, self.settings())
        self.assertFalse(backend.updates)

    def test_impact_failure_isolated_error_preserved_by_round2_and_retry_only_round3(self):
        backend = self.backend([("2021-01-01", "bad", ["ETH"], "a"),
                                ("2021-01-02", "good", ["ETH"], "a")])
        self.relate(backend)
        seen = []
        def handler(request):
            event = json.loads(request_text(request))["current"]["event"]
            seen.append(event)
            return success("{}" if event == "bad" else json.dumps(impact_result()))
        result = run_impact(backend, self.settings(), ai_config=self.config(), network_factory=self.factory(handler))
        self.assertEqual(result.failed, 1)
        self.assertEqual(result.new_completed, 1)
        error = backend.tables["result"].iloc[0]["error_message"]
        self.assertTrue(error)
        run_relation(backend, self.settings())
        self.assertEqual(backend.tables["result"].iloc[0]["error_message"], error)
        count = []
        again = run_impact(backend, self.settings(), ai_config=self.config(), network_factory=self.factory(
            lambda req: count.append(request_text(req)) or success(json.dumps(impact_result()))))
        self.assertEqual(again.new_completed, 1)
        self.assertEqual(len(count), 1)
        self.assertTrue(backend.tables["result"]["error_message"].eq("").all())
        self.assertTrue(backend.tables["result"]["round2_status"].eq("1").all())

    def test_repeat_stage_complete_without_third_prompt(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "repeat", ["ETH"], "a")])
        def handler(request):
            body = json.loads(request_text(request))
            if not body["candidates"]:
                return success(json.dumps(new_result()))
            return success(json.dumps({"event_relation": "repeat", "matched_event_id": body["candidates"][0]["event_id"], "new_information": None}))
        self.relate(backend, handler)
        source_before = backend.tables["first"].copy(deep=True)
        result = run_impact(backend, self.settings(), ai_config=self.config(), network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
        self.assertEqual(result.http_attempts_started, 1)
        self.assertEqual(backend.tables["result"].iloc[1]["round3_ai_response"], "")
        self.assertTrue(backend.tables["result"].iloc[0]["round3_config_hash"])
        self.assertEqual(backend.tables["result"].iloc[1]["round3_config_hash"], "")
        repeat_scope = self.settings(start_time="2021-01-02", end_time="2021-01-02")
        backend.reads.clear()
        self.assertEqual(run_impact(backend, repeat_scope).already_completed, 1)
        self.assertFalse(any(path == "first" for path, _, _ in backend.reads))
        pd.testing.assert_frame_equal(backend.tables["first"], source_before)

    def test_impact_validates_each_response_once_and_retries_invalid_output(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.relate(backend)
        calls = []
        def handler(request):
            calls.append(request)
            return success("{}" if len(calls) == 1 else json.dumps(impact_result()))
        with patch("common.validation.validate_impact_output", wraps=validate_impact_output) as validate:
            result = run_impact(backend, self.settings(), ai_config=self.config(), validator=validate,
                                network_factory=self.factory(handler))
        self.assertEqual(len(calls), 2)
        self.assertEqual(validate.call_count, 2)
        self.assertEqual(result.http_attempts_started, 2)
        self.assertEqual(result.new_completed, 1)

    def test_interrupted_third_claim_preserves_second_and_resumes(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.relate(backend)
        def interrupted(path, frame):
            raise KeyboardInterrupt()
        backend.update_hook = interrupted
        with self.assertRaises(KeyboardInterrupt):
            run_impact(backend, self.settings(), ai_config=self.config(), network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
        self.assertEqual(backend.tables["result"].iloc[0]["round2_status"], "1")
        self.assertEqual(backend.tables["result"].iloc[0]["round3_status"], "0")
        backend.update_hook = None
        result = run_impact(backend, self.settings(), ai_config=self.config(), network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
        self.assertEqual(result.new_completed, 1)
        self.assertEqual(result.http_attempts_started, 1)

    def test_changed_third_model_stops_and_run_parameter_changes_can_resume(self):
        backend = self.backend([("2021-01-01", "good", ["ETH"], "a"), ("2021-01-02", "bad", ["ETH"], "a")])
        self.relate(backend)
        old_hash = ai_config_hash(self.config())
        def first_handler(request):
            event = json.loads(request_text(request))["current"]["event"]
            return success("{}" if event == "bad" else json.dumps(impact_result()))
        run_impact(backend, self.settings(), ai_config=self.config(), network_factory=self.factory(first_handler))
        error = backend.tables["result"].iloc[1]["error_message"]
        self.assertTrue(backend.tables["result"]["round3_config_hash"].eq(old_hash).all())
        self.document["api"]["payload"]["model"] = "different-model"
        calls = []
        with self.assertRaisesRegex(ValueError, "配置摘要.*冲突"):
            run_impact(backend, self.settings(), ai_config=self.config(),
                       network_factory=self.factory(lambda req: calls.append(req) or success(json.dumps(impact_result()))))
        self.assertFalse(calls)
        self.assertEqual(backend.tables["result"].iloc[1]["round3_status"], "0")
        self.assertEqual(backend.tables["result"].iloc[1]["error_message"], error)
        self.document["api"]["payload"]["model"] = "test"
        self.document["run"].update(concurrency=1, max_attempts=1)
        self.assertEqual(ai_config_hash(self.config()), old_hash)
        result = run_impact(backend, self.settings(), ai_config=self.config(),
                            network_factory=self.factory(lambda req: calls.append(req) or success(json.dumps(impact_result()))))
        self.assertEqual(result.new_completed, 1)
        self.assertEqual(len(calls), 1)
        self.assertTrue(backend.tables["result"][HASH_COLUMNS].eq(old_hash).all().all())

    def test_impact_months_reuse_network_and_save_before_loading_next_month(self):
        backend = self.backend([("2021-01-31 23:59:59", "jan-a", ["ETH"], "a"),
                                ("2021-01-31 23:59:59", "jan-b", ["ETH"], "a"),
                                ("2021-02-01 00:00:00", "feb-a", ["ETH"], "a"),
                                ("2021-02-28 23:59:59", "feb-b", ["ETH"], "a")])
        self.relate(backend)
        backend.reads.clear()
        self.document["run"].update(concurrency=2, batch_size=2, result_buffer_size=2)
        config = self.config()
        settings = self.settings(start_time="2021-01-31", end_time="2021-02-28")
        factories, busy, maximum, ready, arrivals, snapshots = [], [0], [0], {}, {}, []
        import round3.runner as impact
        run_segments = impact.run_segments

        def execute(ranges, load_segment, *args):
            def load(start, end):
                if snapshots:
                    self.assertIsNone(snapshots[-1](), "加载下一月时仍持有上一月任务表")
                segment = load_segment(start, end)
                store = segment[0]
                snapshots.append(weakref.ref(store.df))
                self.assertLessEqual(len(store.df), 2)
                return segment
            return run_segments(ranges, load, *args)

        async def handler(request):
            current = json.loads(request_text(request))["current"]
            month = current["event"].split("-")[0]
            busy[0] += 1
            maximum[0] = max(maximum[0], busy[0])
            arrivals[month] = arrivals.get(month, 0) + 1
            event = ready.setdefault(month, asyncio.Event())
            if arrivals[month] == 2:
                event.set()
            await asyncio.wait_for(event.wait(), 5)
            busy[0] -= 1
            return success(json.dumps(dict(impact_result(), materiality_reason=current["event"])))

        create = self.factory(handler)
        def factory(config):
            factories.append(config)
            return create(config)

        def before_read(path, start, end):
            if factories and start == "2021-02-01":
                self.assertTrue(backend.tables["result"].iloc[:2]["round3_status"].eq("1").all())
        backend.read_hook = before_read
        with patch.object(impact, "run_segments", side_effect=execute):
            summary = run_impact(backend, settings, ai_config=config, network_factory=factory)
        self.assertEqual(len(factories), 1)
        self.assertEqual(backend.meta_reads, ["result"])
        self.assertTrue(all(start is not None for _, start, _ in backend.reads))
        self.assertEqual(maximum[0], 2)
        self.assertEqual((summary.total, summary.new_completed, summary.http_attempts_started), (4, 4, 4))
        self.assertEqual([json.loads(json.loads(value)["ai_response_content"])["materiality_reason"] for value in backend.tables["result"]["round3_ai_response"]], ["jan-a", "jan-b", "feb-a", "feb-b"])
        self.assertTrue(all(ref() is None for ref in snapshots))

        # 两个月：合并只读检查 2 次、恢复 2 次、执行 2 次；不为月末检查再读表。
        self.assertEqual(len([read for read in backend.reads if read[0] == "result"]), 6)
        self.assertEqual([read for read in backend.reads if read[0] == "first"],
                         [("first", "2021-01-31", "2021-01-31"), ("first", "2021-02-01", "2021-02-28")])

    def test_impact_later_month_not_ready_stops_before_any_recovery_or_request(self):
        backend = self.backend([("2021-01-01", "jan", ["ETH"], "a"),
                                ("2021-02-01", "feb", ["ETH"], "a")])
        self.relate(backend)
        frame = backend.tables["result"]
        frame.loc[frame.index[0], "round3_status"] = "9"
        frame.loc[frame.index[1], "round2_status"] = "0"
        baseline, count = frame.copy(deep=True), len(backend.updates)
        with self.assertRaisesRegex(ValueError, "第二轮未完成"):
            run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-02-01"),
                       ai_config=self.config(), network_factory=lambda config: self.fail("不应开始请求"))
        self.assertEqual(len(backend.updates), count)
        pd.testing.assert_frame_equal(frame, baseline)

    def test_impact_old_origin_history_preserved_and_future_excluded(self):
        backend = self.backend([("2020-01-01", "origin", ["ETH"], "a"),
                                ("2021-02-01", "progress", ["ETH"], "a"),
                                ("2021-03-01", "current", ["ETH"], "a"),
                                ("2021-04-01", "future", ["ETH"], "a")])
        frame = backend.tables["result"]
        root = frame.index[0]
        frame.loc[:, "round2_status"] = "1"
        frame.loc[:, "round2_config_hash"] = ai_config_hash(self.config())
        frame.loc[:, "event_id"] = root[1]
        frame.loc[:, "event_relation"] = "update"
        frame.loc[:, "new_information"] = frame["event"]
        frame.loc[root, ["event_relation", "new_information"]] = ["new", ""]
        seen = []
        run_impact(backend, self.settings(start_time="2021-03-01", end_time="2021-03-31"),
                   ai_config=self.config(), network_factory=self.factory(
                       lambda req: seen.append(json.loads(request_text(req))) or success(json.dumps(impact_result()))))
        history = seen[0]["previous_history"]
        self.assertEqual(history["first_disclosure"]["event"], "origin")
        self.assertEqual([item["event"] for item in history["timeline"]], ["progress"])
        self.assertEqual(history["disclosure_count"], 2)
        self.assertEqual(list(frame["round3_status"]), ["0", "0", "1", "0"])

    def test_impact_stop_does_not_execute_next_month(self):
        backend = self.backend([("2021-01-01", "jan", ["ETH"], "a"),
                                ("2021-02-01", "feb", ["ETH"], "a")])
        self.relate(backend)
        actual_reads, calls = [], []
        def factory(config):
            backend.read_hook = lambda *args: actual_reads.append(args)
            return self.factory(lambda req: calls.append(request_text(req)) or httpx.Response(429))(config)
        with self.assertRaises(BatchRunError):
            run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-02-01"),
                       ai_config=self.config(), network_factory=factory)
        self.assertEqual(len(calls), 1)
        self.assertFalse(any(start == "2021-02-01" for _, start, _ in actual_reads))
        self.assertEqual(list(backend.tables["result"]["round3_status"]), ["-1", "0"])

    def test_impact_storage_failure_does_not_read_or_advance_month_and_retains_buffer(self):
        backend = self.backend([("2021-01-01", "jan", ["ETH"], "a"),
                                ("2021-02-01", "feb", ["ETH"], "a")])
        self.relate(backend)
        actual_reads, calls = [], []
        def update(path, frame):
            if "round3_ai_response" in frame.columns:
                raise RuntimeError("offline")
            backend.apply(path, frame)
        backend.update_hook = update
        def factory(config):
            backend.read_hook = lambda *args: actual_reads.append(args)
            return self.factory(lambda req: calls.append(request_text(req)) or success(json.dumps(impact_result())))(config)
        with self.assertRaises(BatchPersistenceError) as caught:
            run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-02-01"),
                       ai_config=self.config(), network_factory=factory)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(caught.exception.unsaved_results), 1)
        self.assertEqual(actual_reads, [])
        self.assertEqual(list(backend.tables["result"]["round3_status"]), ["9", "0"])

    def test_impact_claim_without_final_response_cannot_advance_month(self):
        backend = self.backend([("2021-01-01", "jan", ["ETH"], "a"),
                                ("2021-02-01", "feb", ["ETH"], "a")])
        self.relate(backend)
        actual_reads, calls = [], []

        async def handler(request):
            calls.append(request_text(request))
            raise asyncio.CancelledError()

        def factory(config):
            backend.read_hook = lambda *args: actual_reads.append(args)
            return self.factory(handler)(config)

        with self.assertRaisesRegex(BatchRunError, "月份切换前存在未确认任务"):
            run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-02-01"),
                       ai_config=self.config(), network_factory=factory)
        self.assertEqual(len(calls), 1)
        self.assertFalse(any(start == "2021-02-01" for _, start, _ in actual_reads))
        self.assertEqual(list(backend.tables["result"]["round3_status"]), ["9", "0"])
        self.assertTrue(backend.tables["result"]["round3_ai_response"].eq("").all())

    def test_impact_saved_failure_allows_next_month(self):
        backend = self.backend([("2021-01-01", "jan", ["ETH"], "a"),
                                ("2021-02-01", "feb", ["ETH"], "a")])
        self.relate(backend)
        calls = []

        def handler(request):
            event = json.loads(request_text(request))["current"]["event"]
            calls.append(event)
            return success("{}" if event == "jan" else json.dumps(impact_result()))

        config = self.config()
        summary = run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-02-01"),
                             ai_config=config, network_factory=self.factory(handler))
        self.assertEqual(calls, ["jan"] * config.run.max_attempts + ["feb"])
        self.assertEqual((summary.failed, summary.new_completed, summary.processing), (1, 1, 0))
        self.assertEqual(list(backend.tables["result"]["round3_status"]), ["-1", "1"])

    def test_impact_partial_month_collects_roots_before_checking_and_scopes_month_end(self):
        backend = self.backend([("2021-01-05", "origin", ["ETH"], "a"),
                                ("2021-01-15", "current", ["ETH"], "a"),
                                ("2021-01-25", "future", ["ETH"], "a")])
        frame = backend.tables["result"]
        root, current, future = frame.index
        frame.loc[:, "round2_status"] = "1"
        frame.loc[:, "round2_config_hash"] = ai_config_hash(self.config())
        frame.loc[:, "event_id"] = root[1]
        frame.loc[:, "event_relation"] = "update"
        frame.loc[:, "new_information"] = frame["event"]
        frame.loc[root, ["event_relation", "new_information"]] = ["new", ""]
        frame.loc[[root, future], "round3_status"] = "9"
        # 起点排在当前记录之后，且范围外的 9 不应被恢复或阻止本范围结束。
        backend.tables["result"] = frame.iloc[[2, 1, 0]].copy(deep=True)
        seen = []
        summary = run_impact(backend, self.settings(start_time="2021-01-15", end_time="2021-01-15"),
                             ai_config=self.config(), network_factory=self.factory(
                                 lambda req: seen.append(json.loads(request_text(req)))
                                 or success(json.dumps(impact_result()))))
        self.assertEqual((summary.total, summary.new_completed), (1, 1))
        self.assertEqual(seen[0]["previous_history"]["first_disclosure"]["event"], "origin")
        self.assertEqual(seen[0]["previous_history"]["disclosure_count"], 1)
        self.assertEqual(backend.tables["result"].loc[current, "round3_status"], "1")
        self.assertTrue(backend.tables["result"].loc[[root, future], "round3_status"].eq("9").all())
        self.assertEqual([read for read in backend.reads if read[0] == "result"],
                         [("result", "2021-01-05", "2021-01-25"),
                          ("result", "2021-01-15", "2021-01-15"),
                          ("result", "2021-01-15", "2021-01-15")])

    def test_impact_scope_without_imported_tasks_returns_empty_without_reading_source(self):
        for missing, retained in (("2021-01-01", "2021-02-01"), ("2021-02-01", "2021-01-01")):
            with self.subTest(missing=missing):
                backend = self.backend([(retained, "retained", ["ETH"], "a"),
                                        (missing, "missing", ["ETH"], "a")])
                self.relate(backend)
                backend.tables["result"] = backend.tables["result"].loc[
                    backend.tables["result"]["event"].eq("retained")].copy(deep=True)
                before = backend.tables["result"].copy(deep=True)
                count = len(backend.updates)
                backend.reads.clear()
                summary = run_impact(backend, self.settings(start_time=missing, end_time=missing),
                                     network_factory=lambda config: self.fail("不应发请求"))
                self.assertEqual((summary.total, summary.new_completed, summary.http_attempts_started), (0, 0, 0))
                self.assertFalse(any(path == "first" for path, _, _ in backend.reads))
                self.assertEqual(len(backend.updates), count)
                pd.testing.assert_frame_equal(backend.tables["result"], before)

    def test_impact_empty_meta_rejects_existing_records_without_recovery(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.relate(backend)
        backend.tables["result"].loc[:, "round3_status"] = "9"
        backend.metadata["result"] = {"meta": {"start_time": None, "end_time": None}}
        before = backend.tables["result"].copy(deep=True)
        count = len(backend.updates)
        with self.assertRaisesRegex(ValueError, "元数据时间范围为空"):
            run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-01-31"),
                       ai_config=self.config(), network_factory=lambda config: self.fail("不应发请求"))
        self.assertEqual(len(backend.updates), count)
        pd.testing.assert_frame_equal(backend.tables["result"], before)

    def test_impact_meta_json_bounds_and_release_during_startup(self):
        backend = self.backend([("2021-01-31 20:00:00", "origin", ["ETH"], "a"),
                                ("2021-03-05 12:00:00", "current", ["ETH"], "a")])
        self.relate(backend)
        backend.reads.clear()
        backend.metadata["result"] = json.dumps({"meta": {"start_time": "2021-01-31T20:00:00",
                                                          "end_time": "2021-03-05T12:00:00"}})
        snapshots = []
        read = backend.read
        def checked_read(*args, **kwargs):
            if args[0] == "result":
                self.assertTrue(all(ref() is None for ref in snapshots), "新读取仍保留上一分段完整表")
            frame = read(*args, **kwargs)
            snapshots.append(weakref.ref(frame))
            return frame
        backend.read = checked_read
        summary = run_impact(backend, self.settings(start_time="2021-03-05", end_time="2021-03-05"),
                             ai_config=self.config(), network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
        self.assertEqual(summary.new_completed, 1)
        result_reads = [read for read in backend.reads if read[0] == "result"]
        self.assertEqual(result_reads[:3], [("result", "2021-01-31", "2021-01-31"),
                                          ("result", "2021-02-01", "2021-02-28"),
                                          ("result", "2021-03-01", "2021-03-05")])
        self.assertEqual(len(result_reads), 5)
        self.assertEqual(len(backend.reads), 6)
        self.assertTrue(all(start is not None for _, start, _ in backend.reads))
        self.assertTrue(all(ref() is None for ref in snapshots))

    def test_impact_meta_failure_never_falls_back_to_full_read_or_updates(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.relate(backend)
        baseline = backend.tables["result"].copy(deep=True)
        backend.reads.clear()
        count = len(backend.updates)
        documents = [{}, {"meta": {"start_time": "2021-01-01T00:00:00"}},
                     {"meta": {"start_time": None, "end_time": "2021-01-01T00:00:00"}},
                     {"meta": {"start_time": "invalid", "end_time": "2021-01-01T00:00:00"}},
                     {"meta": {"start_time": "2021-02-01T00:00:00", "end_time": "2021-01-01T00:00:00"}},
                     "not json"]
        for document in documents:
            with self.subTest(document=document):
                backend.metadata["result"] = document
                with self.assertRaisesRegex(ValueError, "元数据"):
                    run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-01-01"),
                               ai_config=self.config(), network_factory=lambda config: self.fail("不能发请求"))
        self.assertFalse(backend.reads)
        self.assertEqual(len(backend.updates), count)
        pd.testing.assert_frame_equal(backend.tables["result"], baseline)

    def test_impact_meta_scan_detects_config_conflict_after_processing_end(self):
        backend = self.backend([("2021-01-01", "pending", ["ETH"], "a"),
                                ("2021-03-01", "completed later", ["ETH"], "a")])
        self.relate(backend)
        frame = backend.tables["result"]
        later = frame.index[1]
        frame.loc[later, "round3_ai_response"] = json.dumps({"ai_response_content": json.dumps(impact_result())})
        frame.loc[later, ["round3_status", "round3_config_hash"]] = ["1", "a" * 64]
        baseline = frame.copy(deep=True)
        backend.reads.clear()
        with self.assertRaisesRegex(ValueError, "配置摘要.*冲突"):
            run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-01-31"),
                       ai_config=self.config(), network_factory=lambda config: self.fail("不能发请求"))
        self.assertIn(("result", "2021-03-01", "2021-03-01"), backend.reads)
        self.assertTrue(all(start is not None for _, start, _ in backend.reads))
        pd.testing.assert_frame_equal(frame, baseline)

    def test_impact_meta_scan_rejects_duplicate_record_id_across_months(self):
        backend = self.backend([("2021-01-01", "first", ["ETH"], "a"),
                                ("2021-02-01", "second", ["ETH"], "a")])
        self.relate(backend)
        frame = backend.tables["result"]
        frame.index = pd.MultiIndex.from_tuples([(key[0], frame.index[0][1]) for key in frame.index],
                                                names=frame.index.names)
        count = len(backend.updates)
        with self.assertRaisesRegex(ValueError, "record_id 跨分段重复"):
            run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-02-01"),
                       ai_config=self.config(), network_factory=lambda config: self.fail("不能发请求"))
        self.assertEqual(len(backend.updates), count)

    def test_impact_meta_empty_table_with_no_events(self):
        backend = self.backend([("2021-01-01", None, [], "a")])
        summary = run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-01-31"),
                             network_factory=lambda config: self.fail("不能发请求"))
        self.assertEqual((summary.total, summary.new_completed, summary.http_attempts_started), (0, 0, 0))
        self.assertEqual(backend.meta_reads, ["result"])
        self.assertTrue(all(start is not None for _, start, _ in backend.reads))

    def test_impact_meta_calendar_preserves_timezone_and_precise_index(self):
        backend = self.backend([("2021-01-31T23:59:59.123456789-02:00", "one", ["ETH"], "a")])
        self.relate(backend)
        index = backend.tables["result"].index.copy()
        backend.reads.clear()
        summary = run_impact(backend, self.settings(start_time="2021-01-31", end_time="2021-01-31"),
                             ai_config=self.config(), network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
        self.assertEqual(summary.new_completed, 1)
        self.assertEqual(next(read for read in backend.reads if read[0] == "result"),
                         ("result", "2021-01-31", "2021-01-31"))
        self.assertTrue(backend.tables["result"].index.equals(index))
        self.assertEqual(backend.tables["result"].index[0][0].nanosecond, 789)
