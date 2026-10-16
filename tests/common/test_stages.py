"""验证研究流程的历史边界、任务状态、重试与续跑；不访问真实服务。"""

import json
import unittest
from unittest.mock import patch

import pandas as pd

from common.config import ai_config_hash
from common.persistence import BatchPersistenceError
from round2.runner import run_relation
from round3.runner import run_impact
from common.validation import HASH_COLUMNS
from initialization.event_records import expand_first_round
from common.store import EventRecordStore
from tests.support import request_text, success
from tests.event_support import EventFixture, source_posts, Tables, choose, new_result, impact_result


class EventStageTests(EventFixture, unittest.TestCase):
    def test_stages_use_imported_tasks_without_rechecking_first_results_or_mapping(self):
        for source_mode in ("invalid_results", "text_only"):
            with self.subTest(source_mode=source_mode):
                source = source_posts([("2021-01-02", "已导入事件", [], "a"),
                                       ("2021-01-01", "未导入的旧帖", [], "unmapped")])
                result = expand_first_round(source.iloc[:1], {"a": ["ETH"]})
                backend = Tables(source, result)
                source = backend.tables["first"]
                source.loc[source.index[0], "text"] = "请求仍携带这段原帖正文"
                source.loc[source.index[1], "text"] = None
                source.loc[:, "status"] = "-1"
                source.loc[:, "ai_response_content"] = "invalid JSON"
                if source_mode == "text_only":
                    backend.tables["first"] = source[["text"]].copy(deep=True)
                baseline = backend.tables["first"].copy(deep=True)
                settings = self.relation_settings(backend, user_name_to_tickers={"a": ["BTC"]})
                seen = []
                def relation(request):
                    seen.append(json.loads(request_text(request))["current"])
                    return success(json.dumps(new_result()))
                def impact(request):
                    seen.append(json.loads(request_text(request))["current"])
                    return success(json.dumps(impact_result()))
                with patch("initialization.event_records.expand_first_round", side_effect=AssertionError("不应重新展开")):
                    second = run_relation(backend, settings, ai_config=self.config(), retriever=choose,
                                          network_factory=self.factory(relation))
                    third = run_impact(backend, settings, ai_config=self.config(), network_factory=self.factory(impact))
                self.assertEqual((second.new_completed, third.new_completed), (1, 1))
                self.assertEqual(len(backend.tables["result"]), 1)
                self.assertFalse(backend.appends or backend.registers or backend.writes)
                self.assertEqual(len(seen), 2)
                for current in seen:
                    self.assertEqual((current["event"], current["asset"]), ("已导入事件", "ETH"))
                self.assertNotIn("source_text", seen[0])
                self.assertEqual(seen[1]["source_text"], "请求仍携带这段原帖正文")
                pd.testing.assert_frame_equal(backend.tables["first"], baseline)

    def test_completed_stages_do_not_require_source_table_or_ai_configuration(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.relate(backend)
        run_impact(backend, self.relation_settings(backend), ai_config=self.config(),
                   network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
        del backend.tables["first"]
        backend.reads.clear()
        count = len(backend.updates)
        for operation in (run_relation, run_impact):
            settings = self.relation_settings(backend)
            summary = operation(backend, settings, network_factory=lambda config: self.fail("不应发请求"))
            self.assertEqual((summary.already_completed, summary.http_attempts_started), (1, 0))
        self.assertFalse(any(path == "first" for path, _, _ in backend.reads))
        self.assertEqual(len(backend.updates), count)

    def test_third_round_source_text_errors_stop_before_claiming_tasks(self):
        for problem in ("missing_post", "wrong_date", "missing_column"):
            with self.subTest(problem=problem):
                backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
                self.relate(backend)
                source = backend.tables["first"]
                if problem == "missing_post":
                    backend.tables["first"] = source.iloc[:0].copy(deep=True)
                elif problem == "wrong_date":
                    source.index = pd.MultiIndex.from_tuples([(pd.Timestamp("2021-01-02"), "100")],
                                                            names=["date", "id"])
                else:
                    backend.tables["first"] = source.drop(columns="text")
                before = backend.tables["result"].copy(deep=True)
                count = len(backend.updates)
                with self.assertRaisesRegex(ValueError, "原帖"):
                    run_impact(backend, self.relation_settings(backend), ai_config=self.config(),
                               network_factory=lambda config: self.fail("不应发请求"))
                self.assertEqual(len(backend.updates), count)
                pd.testing.assert_frame_equal(backend.tables["result"], before)

    def test_stages_find_integer_source_ids_and_preserve_original_dates(self):
        for stamp in ("2021-01-01 12:12:12.123456789", pd.Timestamp("2021-01-01T12:12:12.123456789+08:00")):
            with self.subTest(stamp=stamp):
                source = source_posts([("2021-01-01", "one", ["ETH"], "a")])
                source.index = pd.MultiIndex.from_tuples([(stamp, 123)], names=["date", "id"])
                result = expand_first_round(source, {})
                backend = Tables(source, result)
                self.assertEqual(self.relate(backend).new_completed, 1)
                summary = run_impact(backend, self.relation_settings(backend), ai_config=self.config(),
                                     network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
                self.assertEqual(summary.new_completed, 1)
                self.assertTrue(backend.tables["result"].index.equals(result.index))

    def test_unfinished_results_are_not_promoted_to_success_during_recovery(self):
        for stage in ("round2", "round3"):
            for status in ("9", "-1"):
                with self.subTest(stage=stage, status=status):
                    backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
                    key = backend.tables["result"].index[0]
                    frame = backend.tables["result"]
                    frame.loc[key, ["event_id", "event_relation", "round2_status"]] = [key[1], "new", "1"]
                    frame.loc[key, "round3_ai_response"] = json.dumps({"ai_response_content": json.dumps(impact_result())})
                    frame.loc[key, HASH_COLUMNS] = ai_config_hash(self.config())
                    frame.loc[key, [stage + "_status", "error_message"]] = [status, "preserved error"]
                    store = EventRecordStore(backend, self.settings())
                    store.recover(stage, [key])
                    self.assertEqual(backend.tables["result"].loc[key, stage + "_status"], "0")
                    self.assertEqual(backend.tables["result"].loc[key, "error_message"], "preserved error")
                    self.assertEqual([list(patch.columns) for patch in backend.updates], [[stage + "_status"]])

    def test_storage_whole_batch_retry_does_not_repeat_ai(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
        self.relate(backend)
        count, writes = [], []
        def hook(path, frame):
            if "round3_ai_response" in frame.columns:
                writes.append(len(frame))
                if len(writes) == 1:
                    raise RuntimeError("batch not written")
            backend.apply(path, frame)
        backend.update_hook = hook
        result = run_impact(backend, self.relation_settings(backend), ai_config=self.config(), network_factory=self.factory(
            lambda req: count.append(request_text(req)) or success(json.dumps(impact_result()))))
        self.assertEqual(len(count), 2)
        self.assertEqual(writes, [2, 2])
        self.assertEqual(result.new_completed, 2)

    def test_stage_configuration_is_checked_once_for_multiple_claims(self):
        for stage in ("round2", "round3"):
            with self.subTest(stage=stage):
                backend = self.backend([("2021-01-01", "one", ["ETH"], "a"),
                                        ("2021-01-02", "two", ["ETH"], "a"),
                                        ("2021-01-03", "three", ["ETH"], "a")])
                if stage == "round3":
                    self.relate(backend)
                self.hash_reads.clear()
                backend.column_reads.clear()
                with patch("common.store.ai_config_hash", wraps=ai_config_hash) as check:
                    if stage == "round2":
                        result = self.relate(backend)
                    else:
                        result = run_impact(backend, self.relation_settings(backend), ai_config=self.config(),
                                            network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
                self.assertEqual(result.new_completed, 3)
                self.assertEqual(check.call_count, 1)
                self.assertEqual(self.hash_reads, [("result", stage + "_config_hash")])
                self.assertEqual(backend.column_reads, [("result", "", "", [stage + "_config_hash"])])

    def test_storage_failure_retains_unsaved_results_for_recovery(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.relate(backend)
        def hook(path, frame):
            if "round3_ai_response" in frame.columns:
                raise RuntimeError("offline")
            backend.apply(path, frame)
        backend.update_hook = hook
        with self.assertRaises(BatchPersistenceError) as caught:
            run_impact(backend, self.relation_settings(backend), ai_config=self.config(), network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
        self.assertEqual(json.loads(caught.exception.unsaved_results[0][1].ai_response_content), impact_result())
        self.assertEqual(backend.tables["result"].iloc[0]["round3_status"], "9")

    def test_phase_hashes_are_saved_with_claim_and_remain_independent(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.assertTrue(backend.tables["result"][HASH_COLUMNS].eq("").all().all())
        expected_second = ai_config_hash(self.config())
        self.relate(backend)
        second_claim = next(frame for frame in backend.updates if "round2_config_hash" in frame.columns)
        self.assertEqual(set(second_claim.columns), {"round2_status", "round2_config_hash"})
        self.assertEqual(second_claim.iloc[0]["round2_config_hash"], expected_second)
        self.document["prompts"]["system"] = "third phase only"
        third_config = self.config()
        self.assertNotEqual(ai_config_hash(third_config), expected_second)
        run_impact(backend, self.relation_settings(backend), ai_config=third_config,
                   network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
        third_position = next(index for index, frame in enumerate(backend.updates) if "round3_config_hash" in frame.columns)
        third_claim = backend.updates[third_position]
        self.assertEqual(set(third_claim.columns), {"round3_status", "round3_config_hash"})
        row = backend.tables["result"].iloc[0]
        self.assertEqual(row["round2_config_hash"], expected_second)
        self.assertEqual(row["round3_config_hash"], ai_config_hash(third_config))
        self.assertTrue(all(set(frame.columns) <= {"round3_status", "round3_config_hash"} | {"round3_ai_response"} | {"error_message"}
                            for frame in backend.updates[third_position + 1:]))

    def test_completed_history_cells_are_not_validated_or_backfilled(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.relate(backend)
        frame = backend.tables["result"]
        key = frame.index[0]
        frame.loc[key, HASH_COLUMNS] = ["BAD", None]
        frame.loc[key, "round3_status"] = "1"
        frame.loc[key, "round3_ai_response"] = None
        before = frame.copy(deep=True)
        self.hash_reads.clear()
        self.assertEqual(run_relation(backend, self.relation_settings(backend)).already_completed, 1)
        self.assertEqual(run_impact(backend, self.relation_settings(backend)).already_completed, 1)
        self.assertFalse(self.hash_reads)
        pd.testing.assert_frame_equal(backend.tables["result"], before)

    def test_recovery_changes_only_current_status_and_preserves_other_stage(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        key = backend.tables["result"].index[0]
        frame = backend.tables["result"]
        frame.loc[key, ["round2_status", "round3_status", "error_message"]] = ["-1", "-1", "latest failure"]
        store = EventRecordStore(backend, self.settings())
        store.recover("round2", [key])
        self.assertEqual(frame.loc[key, "round2_status"], "0")
        self.assertEqual(frame.loc[key, "round3_status"], "-1")
        self.assertEqual(frame.loc[key, "error_message"], "latest failure")
        self.assertEqual([list(patch.columns) for patch in backend.updates], [["round2_status"]])
