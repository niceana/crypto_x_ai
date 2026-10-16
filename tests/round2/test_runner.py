"""验证研究流程的历史边界、任务状态、重试与续跑；不访问真实服务。"""

import asyncio
import json
import queue
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pandas as pd

from common.config import ai_config_hash
from common.network import BatchRunError, CompletionMessage, NetworkWorker
from common.persistence import BatchPersistenceError
from common.ai_protocol import parse_response
from initialization.event_records import import_event_records_from_table
from round2.retrieval import RetrievalNotConfigured, RetrievalSelection
from round2.runner import run_relation
from round3.runner import run_impact
from common.store import EventRecordStore
from tests.support import request_text, success
from tests.event_support import EventFixture, source_posts, choose, new_result, impact_result


class RelationRunnerTests(EventFixture, unittest.TestCase):
    def test_scoped_relation_never_reads_source_and_preserves_history(self):
        backend = self.backend([("2021-01-01", "origin", ["ETH"], "a"),
                                ("2021-01-02", "update", ["ETH"], "a")])
        old_scope = self.settings(start_time="2021-01-01", end_time="2021-01-01")
        run_relation(backend, old_scope, ai_config=self.config(), retriever=choose,
                     network_factory=self.factory(lambda req: success(json.dumps(new_result()))))
        del backend.tables["first"]
        backend.reads.clear()
        seen = []
        def handler(request):
            body = json.loads(request_text(request))
            seen.append(body)
            return success(json.dumps({"event_relation": "update", "matched_event_id": body["candidates"][0]["event_id"],
                                       "new_information": "新进展"}))
        scope = self.settings(start_time="2021-01-02", end_time="2021-01-02")
        summary = run_relation(backend, scope, ai_config=self.config(), retriever=choose,
                               network_factory=self.factory(handler))
        self.assertEqual(summary.new_completed, 1)
        self.assertEqual(seen[0]["candidates"][0]["first_disclosure"]["event"], "origin")
        self.assertEqual(backend.reads, [("result", None, None)])
        self.assertEqual(seen[0]["current"]["event"], "update")
        self.assertNotIn("source_text", seen[0]["current"])

    def test_unconfigured_retrieval_and_prompts_do_not_claim(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        with self.assertRaises(RetrievalNotConfigured):
            run_relation(backend, self.settings())
        self.assertFalse(backend.updates)
        self.document["prompts"]["system"] = ""
        with self.assertRaisesRegex(ValueError, "提示词尚未配置"):
            self.relate(backend)
        self.assertFalse(backend.updates)

    def test_sequence_history_progress_and_wrong_id_retry(self):
        backend = self.backend([("2021-01-01", "plan", ["ETH"], "a"),
                                ("2021-01-02", "approve", ["ETH"], "a"),
                                ("2021-01-03", "execute", ["ETH"], "a"),
                                ("2021-01-04", "repeat", ["ETH"], "a")])
        seen, attempts = [], {}
        def handler(request):
            body = json.loads(request_text(request))
            event = body["current"]["event"]
            attempts[event] = attempts.get(event, 0) + 1
            seen.append(body)
            if event == "plan":
                return success(json.dumps(new_result()))
            matched = body["candidates"][0]["event_id"]
            if event == "approve" and attempts[event] == 1:
                matched = "not-provided"
            value = {"event_relation": "repeat" if event == "repeat" else "update",
                     "matched_event_id": matched, "new_information": None if event == "repeat" else event}
            return success(json.dumps(value))
        summary = self.relate(backend, handler)
        self.assertEqual(summary.http_attempts_started, 5)
        self.assertEqual(summary.new_completed, 4)
        execute = next(body for body in seen if body["current"]["event"] == "execute")
        self.assertEqual(execute["candidates"][0]["timeline"][0]["new_information"], "approve")
        self.assertEqual(backend.tables["result"].iloc[-1]["round3_status"], "1")
        self.assertTrue(all(value == threading.get_ident() for value in backend.threads))
        previous = len(seen)
        again = run_relation(backend, self.settings())
        self.assertEqual(again.already_completed, 4)
        self.assertEqual(len(seen), previous)

    def test_same_time_snapshots_exclude_peers_and_future(self):
        backend = self.backend([("2021-01-01 12:00:00", "a", ["ETH"], "a"),
                                ("2021-01-01 12:00:00", "b", ["ETH"], "a"),
                                ("2021-01-02 12:00:00", "c", ["ETH"], "a")])
        seen = []
        def retrieve(current, searchable):
            seen.append((current["event"], tuple(row["event"] for row in searchable)))
            return choose(current, searchable)
        self.relate(backend, retriever=retrieve)
        self.assertEqual(seen[:2], [("a", ()), ("b", ())])
        self.assertEqual(set(seen[2][1]), {"a", "b"})

    def test_assets_overlap_but_different_times_same_asset_do_not(self):
        backend = self.backend([("2021-01-01", "one", ["ETH", "LEND"], "a"),
                                ("2021-01-02", "two", ["ETH", "LEND"], "a")])
        busy, maximum, arrivals, ready_by_time = set(), [0], {}, {}
        async def handler(request):
            body = json.loads(request_text(request))
            asset = body["current"]["asset"]
            self.assertNotIn(asset, busy)
            busy.add(asset)
            maximum[0] = max(maximum[0], len(busy))
            # 等两个资产都实际起发，避免主线程认领稍慢时短延时测试误判串行。
            stamp = body["current"]["date"]
            arrivals.setdefault(stamp, set()).add(asset)
            ready = ready_by_time.setdefault(stamp, asyncio.Event())
            if len(arrivals[stamp]) == 2:
                ready.set()
            await asyncio.wait_for(ready.wait(), timeout=5)
            busy.remove(asset)
            return success(json.dumps(new_result()))
        self.relate(backend, handler)
        self.assertEqual(maximum[0], 2)

    def test_relation_failure_blocks_only_that_asset_and_resumes(self):
        backend = self.backend([("2021-01-01", "bad", ["ETH"], "a"),
                                ("2021-01-02", "later", ["ETH"], "a"),
                                ("2021-01-01", "good", ["LEND"], "b")])
        events = []
        def handler(request):
            event = json.loads(request_text(request))["current"]["event"]
            events.append(event)
            return success("{}" if event == "bad" else json.dumps(new_result()))
        result = self.relate(backend, handler)
        self.assertEqual(result.blocked_assets, ("ETH",))
        self.assertEqual(events.count("bad"), 3)
        self.assertNotIn("later", events)
        frame = backend.tables["result"]
        self.assertEqual(frame.loc[(pd.Timestamp("2021-01-02"), "101:0:ETH"), "round2_status"], "0")
        result = self.relate(backend)
        self.assertEqual(result.new_completed, 2)
        self.assertTrue(backend.tables["result"]["error_message"].eq("").all())

    def test_mixed_network_and_output_errors_share_three_attempts(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        count = [0]
        def handler(request):
            count[0] += 1
            if count[0] == 1:
                raise httpx.ReadTimeout("test timeout", request=request)
            return success("{}")
        result = self.relate(backend, handler)
        self.assertEqual(count[0], 3)
        self.assertEqual(result.failed, 1)

    def test_uncertain_becomes_new_and_auto_new_skips_ai(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.relate(backend, lambda req: success(json.dumps(dict(new_result(), event_relation="uncertain"))))
        row = backend.tables["result"].iloc[0]
        self.assertEqual(row["event_relation"], "new")
        self.assertEqual(row["round3_status"], "0")
        saved = json.loads(row["round2_ai_response"])
        self.assertEqual(json.loads(saved["ai_response_content"])["event_relation"], "uncertain")
        self.assertEqual(saved["attempts"], 1)
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        result = self.relate(backend, retriever=lambda current, rows: RetrievalSelection((), True, "测试放行"))
        self.assertEqual(result.http_attempts_started, 0)
        self.assertEqual(backend.tables["result"].iloc[0]["round2_ai_response"], "")
        self.assertEqual(backend.tables["result"].iloc[0]["round2_config_hash"], ai_config_hash(self.config()))

    def test_missing_prior_history_stops_without_expanding_scope(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a"),
                                ("2021-01-02", "two", ["ETH"], "a")])
        settings = self.settings(start_time="2021-01-02", end_time="2021-01-02")
        with self.assertRaisesRegex(ValueError, "手动补跑"):
            run_relation(backend, settings, ai_config=self.config(), retriever=choose)
        self.assertFalse(backend.updates)

    def test_relation_memory_sync_failure_retains_answer_without_rewriting(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        original = EventRecordStore._apply_saved_patch
        error = RuntimeError("relation sync failed")
        def confirm(store, patch_frame, stage):
            if "event_relation" in patch_frame.columns:
                raise error
            original(store, patch_frame, stage)
        original_shutdown = NetworkWorker.shutdown
        with patch.object(EventRecordStore, "_apply_saved_patch", confirm), \
                patch.object(NetworkWorker, "shutdown", autospec=True, side_effect=original_shutdown) as shutdown:
            with self.assertRaises(BatchPersistenceError) as caught:
                self.relate(backend)
        self.assertEqual(shutdown.call_count, 1)
        self.assertIs(caught.exception.__cause__, error)
        self.assertEqual(len([frame for frame in backend.updates if "event_relation" in frame.columns]), 1)
        self.assertEqual(backend.tables["result"].iloc[0]["round2_status"], "1")
        self.assertEqual(len(caught.exception.unsaved_results), 1)
        self.assertEqual(caught.exception.unsaved_results[0].value, new_result())

    def test_stop_http_preserves_claims_and_cleans_network_thread(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        with self.assertRaises(BatchRunError):
            self.relate(backend, lambda request: httpx.Response(429, json={"error": "limit"}))
        self.assertEqual(backend.tables["result"].iloc[0]["round2_status"], "-1")
        self.assertFalse(any(thread.name == "ai-batch-network" for thread in threading.enumerate()))

    def test_relation_storage_failure_retains_current_and_other_answers_without_more_writes(self):
        for remaining_location in ("queue", "unpublished"):
            with self.subTest(remaining_location=remaining_location):
                backend = self.backend([("2021-01-01", "one", ["ETH"], "a"),
                                        ("2021-01-01", "two", ["ETH"], "a"),
                                        ("2021-01-02", "later", ["ETH"], "a")])
                network = SimpleNamespace(completions=queue.Queue(), unpublished=[], failure=None,
                                          http_attempts_started=0, start=lambda: None, check=lambda: None)
                def submit(job):
                    network.http_attempts_started += 1
                    message = CompletionMessage(job.key, parse_response(200, success(json.dumps(new_result())).text),
                                                1, value=new_result())
                    if remaining_location == "unpublished" and network.http_attempts_started == 2:
                        network.unpublished.append(message)
                    else:
                        network.completions.put_nowait(message)
                def shutdown():
                    network.failure = RuntimeError("close failed too")
                network.submit, network.shutdown = submit, shutdown
                error = RuntimeError("result not written")
                final_calls = []
                def update(path, frame):
                    if "event_relation" in frame.columns:
                        final_calls.append(frame.copy(deep=True))
                        raise error
                    backend.apply(path, frame)
                backend.update_hook = update
                with self.assertRaises(BatchPersistenceError) as caught:
                    run_relation(backend, self.settings(), ai_config=self.config(), retriever=choose,
                                 network_factory=lambda config: network)
                self.assertIs(caught.exception.__cause__, error)
                self.assertEqual(caught.exception.stage, "round2")
                self.assertEqual(len(final_calls), 3)
                self.assertEqual(network.http_attempts_started, 2)
                self.assertEqual(list(backend.tables["result"]["round2_status"]), ["9", "9", "0"])
                current = caught.exception.unsaved_results
                self.assertEqual(len(current), 1)
                self.assertEqual(current[0].value, new_result())
                self.assertEqual(current[0].response.ai_response_content, json.dumps(new_result()))
                self.assertEqual(caught.exception.completed_results.qsize()
                                 + len(caught.exception.unpublished_results), 1)
                self.assertEqual(len(backend.reads), 1)
                for frame in final_calls[1:]:
                    pd.testing.assert_frame_equal(frame, final_calls[0])

    def test_relation_stop_finishes_answers_and_preserves_failure_during_unpublished_save(self):
        for fail_save in (False, True):
            with self.subTest(fail_save=fail_save):
                backend = self.backend([("2021-01-01", "one", ["ETH"], "a"),
                                        ("2021-01-01", "two", ["ETH"], "a")])
                network = SimpleNamespace(completions=queue.Queue(), unpublished=[], failure=None,
                                          http_attempts_started=0, start=lambda: None, shutdown=lambda: None)
                stop = BatchRunError("network stopped")
                def submit(job):
                    network.http_attempts_started += 1
                    message = CompletionMessage(job.key, parse_response(200, success(json.dumps(new_result())).text),
                                                1, value=new_result())
                    if network.http_attempts_started == 1:
                        network.completions.put_nowait(message)
                    else:
                        network.unpublished.append(message)
                def check():
                    if network.http_attempts_started == 2:
                        raise stop
                network.submit, network.check = submit, check
                final_calls = []
                def update(path, frame):
                    if "event_relation" in frame.columns:
                        final_calls.append(frame.copy(deep=True))
                        if fail_save and frame.index[0][1].startswith("101:"):
                            raise RuntimeError("second answer not written")
                    backend.apply(path, frame)
                backend.update_hook = update
                with self.assertRaises(BatchPersistenceError if fail_save else BatchRunError) as caught:
                    run_relation(backend, self.settings(), ai_config=self.config(), retriever=choose,
                                 network_factory=lambda config: network)
                self.assertEqual(len(final_calls), 4 if fail_save else 2)
                self.assertEqual(list(backend.tables["result"]["round2_status"]), ["1", "9" if fail_save else "1"])
                self.assertEqual(len(caught.exception.unsaved_results), 1 if fail_save else 0)
                self.assertEqual(caught.exception.completed_results.qsize(), 0)
                self.assertEqual(caught.exception.unpublished_results, [])
                if fail_save:
                    self.assertIs(caught.exception.__cause__, stop)
                    self.assertIn("second answer not written", str(caught.exception.__context__))
                else:
                    self.assertIs(caught.exception, stop)

    def test_relation_interrupt_during_result_save_preserves_answer_for_finish(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        final_calls, requests = [], []
        def update(path, frame):
            if "event_relation" in frame.columns:
                final_calls.append(frame.copy(deep=True))
                if len(final_calls) == 1:
                    raise KeyboardInterrupt()
            backend.apply(path, frame)
        backend.update_hook = update
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.relate(backend, lambda req: requests.append(req) or success(json.dumps(new_result())))
        self.assertEqual(len(requests), 1)
        self.assertEqual(len(final_calls), 2)
        pd.testing.assert_frame_equal(final_calls[0], final_calls[1])
        self.assertEqual(backend.tables["result"].iloc[0]["round2_status"], "1")
        self.assertEqual(caught.exception.unsaved_results, [])
        self.assertFalse(any(thread.name == "ai-batch-network" for thread in threading.enumerate()))

    def test_interrupted_second_claim_resumes_from_saved_state(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        def interrupted(current, rows):
            raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.relate(backend, retriever=interrupted)
        self.assertEqual(backend.tables["result"].iloc[0]["round2_status"], "9")
        self.assertFalse(any(thread.name == "ai-batch-network" for thread in threading.enumerate()))
        self.assertEqual(self.relate(backend).new_completed, 1)

    def test_unordered_completed_relations_are_validated_without_rewriting(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
        root, update = backend.tables["result"].index
        backend.tables["result"].loc[root, ["event_id", "event_relation", "round2_status"]] = [root[1], "new", "1"]
        backend.tables["result"].loc[update, ["event_id", "event_relation", "new_information", "round2_status"]] = [root[1], "update", "批准", "1"]
        backend.tables["result"].loc[:, "round2_config_hash"] = ai_config_hash(self.config())
        backend.tables["result"] = backend.tables["result"].iloc[::-1].copy(deep=True)
        result = run_relation(backend, self.settings())
        self.assertEqual(result.already_completed, 2)
        self.assertEqual(result.http_attempts_started, 0)
        self.assertFalse(backend.updates)

    def test_window_retrieval_uses_recent_update_and_retains_older_origin(self):
        backend = self.backend([("2020-01-01", "origin", ["ETH"], "a"),
                                ("2021-01-01", "progress", ["ETH"], "a"),
                                ("2021-01-02", "current", ["ETH"], "a")])
        frame = backend.tables["result"]
        root, update, current = frame.index
        frame.loc[root, ["event_id", "event_relation", "round2_status"]] = [root[1], "new", "1"]
        frame.loc[update, ["event_id", "event_relation", "new_information", "round2_status"]] = [root[1], "update", "执行", "1"]
        frame.loc[[root, update], "round2_config_hash"] = ai_config_hash(self.config())
        settings = self.settings(start_time="2021-01-02", end_time="2021-01-02")
        seen = []
        result = run_relation(backend, settings, ai_config=self.config(), retriever=choose,
                              network_factory=self.factory(lambda req: seen.append(json.loads(request_text(req))) or success(json.dumps(new_result()))))
        candidate = seen[0]["candidates"][0]
        self.assertEqual(candidate["first_disclosure"]["record_id"], root[1])
        self.assertEqual(candidate["timeline"][0]["new_information"], "执行")
        self.assertEqual(candidate["disclosure_count"], 2)
        self.assertEqual(result.new_completed, 1)

    def test_repeat_history_is_not_validated_or_repaired_on_recovery(self):
        for stage in ("round2", "round3"):
            for invalid in ("status", "hash", "result"):
                with self.subTest(stage=stage, invalid=invalid):
                    backend = self.backend([("2021-01-01", "one", ["ETH"], "a"),
                                            ("2021-01-02", "repeat", ["ETH"], "a")])
                    frame = backend.tables["result"]
                    root, repeat = frame.index
                    frame.loc[root, ["event_id", "event_relation", "round2_status"]] = [root[1], "new", "1"]
                    frame.loc[repeat, ["event_id", "event_relation", "round2_status", "round3_status"]] = [root[1], "repeat", "1", "1"]
                    frame.loc[:, "round2_config_hash"] = ai_config_hash(self.config())
                    if invalid == "status":
                        frame.loc[repeat, "round3_status"] = "0"
                    elif invalid == "hash":
                        frame.loc[repeat, "round3_config_hash"] = ai_config_hash(self.config())
                    else:
                        frame.loc[repeat, "round3_ai_response"] = "unexpected result"
                    before = frame.copy(deep=True)
                    store = EventRecordStore(backend, self.settings())
                    store.recover(stage, [repeat])
                    self.assertFalse(backend.updates)
                    pd.testing.assert_frame_equal(backend.tables["result"], before)

    def test_changed_second_prompt_conflicts_with_history_outside_current_range(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
        earlier = self.settings(start_time="2021-01-01", end_time="2021-01-01")
        run_relation(backend, earlier, ai_config=self.config(), retriever=choose,
                     network_factory=self.factory(lambda req: success(json.dumps(new_result()))))
        baseline, writes = backend.tables["result"].copy(deep=True), len(backend.updates)
        self.document["prompts"]["system"] = "changed second prompt"
        current = self.settings(start_time="2021-01-02", end_time="2021-01-02")
        calls = []
        with self.assertRaisesRegex(ValueError, "配置摘要.*冲突"):
            run_relation(backend, current, ai_config=self.config(), retriever=choose,
                         network_factory=self.factory(lambda req: calls.append(req) or success(json.dumps(new_result()))))
        self.assertFalse(calls)
        self.assertEqual(len(backend.updates), writes)
        pd.testing.assert_frame_equal(backend.tables["result"], baseline)

    def test_failed_claim_retries_hash_and_status_without_repeating_ai(self):
        for stage in ("round2", "round3"):
            with self.subTest(stage=stage):
                backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
                if stage == "round3":
                    self.relate(backend)
                claims = []
                def hook(path, frame):
                    if stage + "_config_hash" in frame.columns:
                        claims.append(frame.copy(deep=True))
                        if len(claims) == 1:
                            raise RuntimeError("认领整批未写入")
                    backend.apply(path, frame)
                backend.update_hook = hook
                if stage == "round2":
                    result = self.relate(backend)
                else:
                    result = run_impact(backend, self.settings(), ai_config=self.config(),
                                        network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
                self.assertEqual(len(claims), 2)
                self.assertTrue(all(set(frame.columns) == {stage + "_status", stage + "_config_hash"} for frame in claims))
                self.assertEqual(result.http_attempts_started, 1)
                self.assertEqual(backend.tables["result"].iloc[0][stage + "_config_hash"], ai_config_hash(self.config()))

    def test_backfill_is_identified_but_same_time_frontier_and_tail_append_continue(self):
        for inserted_day, blocked in (("2021-01-15", True), ("2021-01-10", True),
                                      ("2021-01-20", False), ("2021-01-21", False)):
            with self.subTest(day=inserted_day):
                backend = self.backend([("2021-01-10", "old", ["ETH"], "a"), ("2021-01-20", "later", ["ETH"], "a")])
                self.relate(backend)
                addition = source_posts([(inserted_day, "inserted", ["ETH"], "a")])
                addition.index = pd.MultiIndex.from_tuples([(pd.Timestamp(inserted_day), "102")], names=["date", "id"])
                backend.tables["first"] = pd.concat([backend.tables["first"], addition])
                scope = self.settings(start_time=inserted_day, end_time=inserted_day)
                before_import = backend.tables["result"].copy(deep=True)
                previous_updates = len(backend.updates)
                summary = run_relation(backend, scope)
                self.assertEqual(summary.new_completed, 0)
                self.assertEqual(len(backend.updates), previous_updates)
                pd.testing.assert_frame_equal(backend.tables["result"], before_import)
                import_event_records_from_table(backend, self.settings(start_time=inserted_day, end_time=inserted_day, initialize_mode="resume"))
                baseline = backend.tables["result"].copy(deep=True)
                if blocked:
                    with self.assertRaisesRegex(ValueError, "疑似历史补录") as caught:
                        run_relation(backend, scope, ai_config=self.config(), retriever=choose)
                    self.assertIn("102:0:ETH", str(caught.exception))
                    self.assertIn("101:0:ETH", str(caught.exception))
                    pd.testing.assert_frame_equal(backend.tables["result"], baseline)
                else:
                    result = run_relation(backend, scope, ai_config=self.config(), retriever=choose,
                                          network_factory=self.factory(lambda req: success(json.dumps(new_result()))))
                    self.assertEqual(result.new_completed, 1)

    def test_history_gap_detection_does_not_compare_different_assets(self):
        backend = self.backend([("2021-01-10", "eth", ["ETH"], "a"), ("2021-01-20", "lend", ["LEND"], "b")])
        later = self.settings(start_time="2021-01-20", end_time="2021-01-20")
        run_relation(backend, later, ai_config=self.config(), retriever=choose,
                     network_factory=self.factory(lambda req: success(json.dumps(new_result()))))
        self.assertEqual(self.relate(backend).new_completed, 1)
