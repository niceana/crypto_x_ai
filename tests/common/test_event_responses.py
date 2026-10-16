"""验证两轮回答合列、最后一次用量、落库时间及存储重试；不访问真实服务。"""

import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import httpx
import pandas as pd

from common.store import EventRecordStore
from common.validation import IMPACT_FIELDS, RESULT_COLUMNS
from round2.retrieval import RetrievalSelection
from round3.runner import run_impact
from tests.event_support import EventFixture, impact_result, new_result
from tests.support import success


class EventResponseTests(EventFixture, unittest.TestCase):
    def prepare_stage(self, stage, count=1):
        backend = self.backend([("2021-01-01", "event {}".format(i), ["ETH"], "a")
                                for i in range(count)])
        if stage == "round3":
            self.relate(backend)
        return backend

    def run_stage(self, stage, backend, handler):
        if stage == "round2":
            return self.relate(backend, handler)
        return run_impact(backend, self.relation_settings(backend), ai_config=self.config(),
                          network_factory=self.factory(handler))

    def test_initialization_uses_14_columns_and_empty_response_cells(self):
        frame = self.prepare_stage("round2").tables["result"]
        self.assertEqual(len(frame.columns), 14)
        self.assertEqual(list(frame.columns), RESULT_COLUMNS)
        self.assertFalse(set(IMPACT_FIELDS).intersection(frame.columns))
        self.assertTrue(frame[["round2_ai_response", "round3_ai_response"]].eq("").all().all())
        self.assertTrue(all(isinstance(value, str) for value in frame.iloc[0]))

    def test_both_stages_preserve_raw_content_final_usage_and_distinct_times(self):
        for stage in ("round2", "round3"):
            with self.subTest(stage=stage):
                backend = self.prepare_stage(stage)
                expected = new_result() if stage == "round2" else impact_result()
                raw = " \n" + json.dumps(expected, ensure_ascii=False) + "\n "
                calls = []
                usage = {"prompt_tokens": 8, "completion_tokens": 5, "total_tokens": 13,
                         "completion_tokens_details": {"reasoning_tokens": 2}}

                def handler(request):
                    calls.append(request)
                    response = success("{}" if len(calls) == 1 else raw,
                                       usage={"total_tokens": 100} if len(calls) == 1 else usage,
                                       created=1791947560)
                    body = json.loads(response.text)
                    body["choices"][0]["message"]["reasoning_content"] = "不应持久保存的推理"
                    return httpx.Response(200, json=body)

                with patch("common.store.datetime") as clock:
                    clock.now.return_value = datetime(2026, 10, 16, 18, 20, 30,
                                                     tzinfo=timezone(timedelta(hours=8)))
                    summary = self.run_stage(stage, backend, handler)
                saved = json.loads(backend.tables["result"].iloc[0][stage + "_ai_response"])
                self.assertEqual(set(saved), {"ai_response_content", "usage", "response_created", "attempts", "created"})
                self.assertEqual(saved["ai_response_content"], raw)
                self.assertEqual(saved["usage"], usage)
                self.assertEqual(saved["attempts"], 2)
                self.assertEqual(saved["response_created"], "2026-10-14 11:12:40")
                self.assertEqual(saved["created"], "2026-10-16 18:20:30")
                clock.now.assert_called_once_with(timezone(timedelta(hours=8)))
                self.assertNotIn("不应持久保存的推理", json.dumps(saved, ensure_ascii=False))
                self.assertEqual((summary.new_completed, summary.http_attempts_started), (1, 2))

    def test_missing_final_usage_does_not_reuse_previous_response_usage(self):
        for stage in ("round2", "round3"):
            with self.subTest(stage=stage):
                backend = self.prepare_stage(stage)
                calls = []

                def handler(request):
                    calls.append(request)
                    if len(calls) == 1:
                        return success("{}", usage={"total_tokens": 100}, created=1791947560)
                    return success(json.dumps(new_result() if stage == "round2" else impact_result()))

                self.run_stage(stage, backend, handler)
                saved = json.loads(backend.tables["result"].iloc[0][stage + "_ai_response"])
                self.assertIsNone(saved["usage"])
                self.assertEqual(saved["response_created"], "")
                self.assertEqual(saved["attempts"], 2)

    def test_final_network_failure_overwrites_old_response_without_reusing_earlier_usage(self):
        self.document["run"]["max_attempts"] = 2
        for stage in ("round2", "round3"):
            with self.subTest(stage=stage):
                backend = self.prepare_stage(stage)
                backend.tables["result"].loc[:, stage + "_ai_response"] = "旧回答"
                calls = []

                def handler(request):
                    calls.append(request)
                    if len(calls) == 1:
                        return success("{}", usage={"total_tokens": 100}, created=1791947560)
                    raise httpx.ReadTimeout("读取超时")

                summary = self.run_stage(stage, backend, handler)
                row = backend.tables["result"].iloc[0]
                saved = json.loads(row[stage + "_ai_response"])
                self.assertEqual((summary.failed, row[stage + "_status"]), (1, "-1"))
                self.assertEqual(saved["ai_response_content"], "")
                self.assertEqual(saved["response_created"], "")
                self.assertIsNone(saved["usage"])
                self.assertEqual(saved["attempts"], 2)
                self.assertTrue(saved["created"])
                self.assertIn("读取超时", row["error_message"])

    def test_final_invalid_output_is_saved_with_failure_and_last_usage(self):
        for stage in ("round2", "round3"):
            with self.subTest(stage=stage):
                backend = self.prepare_stage(stage)
                calls = []

                def handler(request):
                    calls.append(request)
                    return success("错误正文 {}".format(len(calls)), usage={"total_tokens": len(calls)})

                summary = self.run_stage(stage, backend, handler)
                row = backend.tables["result"].iloc[0]
                saved = json.loads(row[stage + "_ai_response"])
                self.assertEqual((summary.failed, row[stage + "_status"]), (1, "-1"))
                self.assertEqual(saved["ai_response_content"], "错误正文 3")
                self.assertEqual(saved["usage"], {"total_tokens": 3})
                self.assertEqual(saved["attempts"], 3)
                if stage == "round2":
                    self.assertTrue(row[["event_id", "event_relation", "new_information"]].eq("").all())

    def test_storage_retry_refreshes_created_only_and_syncs_actual_submitted_values(self):
        for stage in ("round2", "round3"):
            with self.subTest(stage=stage):
                backend = self.prepare_stage(stage)
                column = stage + "_ai_response"
                writes, synchronized, calls = [], [], []
                original_sync = EventRecordStore._apply_saved_patch

                def update(path, frame):
                    if column in frame.columns:
                        writes.append(json.loads(frame.iloc[0][column]))
                        if len(writes) == 1:
                            frame.loc[:, column] = "客户端改变入参后失败"
                            raise RuntimeError("本批未写入")
                    backend.apply(path, frame)
                    if column in frame.columns:
                        frame.loc[:, column] = "客户端改变成功提交的入参"

                def sync(store, frame, operation):
                    original_sync(store, frame, operation)
                    if column in frame.columns:
                        synchronized.append(store.df.loc[frame.index[0], column])
                        self.assertEqual(synchronized[-1], backend.tables["result"].loc[frame.index[0], column])

                backend.update_hook = update
                with patch("common.store.datetime") as clock, patch.object(EventRecordStore, "_apply_saved_patch", new=sync):
                    clock.now.side_effect = [datetime(2026, 10, 16, 18, 20, 30), datetime(2026, 10, 16, 18, 20, 35)]
                    summary = self.run_stage(stage, backend, lambda request: calls.append(request) or success(
                        json.dumps(new_result() if stage == "round2" else impact_result()), usage={"total_tokens": 7}))
                self.assertEqual(len(calls), 1)
                self.assertEqual(summary.http_attempts_started, 1)
                self.assertEqual([value["created"] for value in writes], ["2026-10-16 18:20:30", "2026-10-16 18:20:35"])
                self.assertEqual({key: value for key, value in writes[0].items() if key != "created"},
                                 {key: value for key, value in writes[1].items() if key != "created"})
                self.assertEqual(len(synchronized), 1)
                self.assertEqual(json.loads(synchronized[0]), writes[1])

    def test_third_batch_uses_one_created_and_preserves_second_round(self):
        backend = self.prepare_stage("round3", 2)
        before = backend.tables["result"].copy(deep=True)
        self.document["run"].update(batch_size=2, result_buffer_size=2, flush_interval_seconds=60)
        with patch("common.store.datetime") as clock:
            clock.now.return_value = datetime(2026, 10, 16, 18, 20, 30)
            self.run_stage("round3", backend, lambda request: success(json.dumps(impact_result())))
        frame = backend.tables["result"]
        saved = [json.loads(value) for value in frame["round3_ai_response"]]
        self.assertEqual([value["created"] for value in saved], ["2026-10-16 18:20:30"] * 2)
        self.assertEqual(clock.now.call_count, 1)
        pd.testing.assert_frame_equal(frame.drop(columns=["round3_status", "round3_config_hash", "round3_ai_response"]),
                                      before.drop(columns=["round3_status", "round3_config_hash", "round3_ai_response"]))

    def test_auto_new_has_empty_responses_and_recovery_claim_do_not_change_answers(self):
        backend = self.prepare_stage("round2")
        summary = self.relate(backend, retriever=lambda current, searchable: RetrievalSelection((), True, "无历史"),
                              handler=lambda request: self.fail("自动判新不应调用 AI"))
        row = backend.tables["result"].iloc[0]
        self.assertEqual(summary.http_attempts_started, 0)
        self.assertEqual((row["round2_ai_response"], row["round3_ai_response"]), ("", ""))
        self.run_stage("round3", backend, lambda request: success(json.dumps(impact_result())))
        frame = backend.tables["result"]
        before = frame[["round2_ai_response", "round3_ai_response"]].copy(deep=True)
        for stage in ("round2", "round3"):
            frame.loc[:, stage + "_status"] = "-1"
            store = EventRecordStore(backend, self.settings())
            keys = list(frame.index)
            store.recover(stage, keys)
            store.claim(keys, stage, "a" * 64)
        pd.testing.assert_frame_equal(frame[["round2_ai_response", "round3_ai_response"]], before)
