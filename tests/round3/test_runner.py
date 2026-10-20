"""验证第三轮事件输入、读取范围、任务状态、重试与续跑；不访问真实服务。"""

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
    def test_startup_logs_recovery_and_pending_counts_before_ai(self):
        backend = self.backend([("2021-01-01", "event {}".format(i), ["ETH"], "a") for i in range(4)])
        self.relate(backend)
        frame = backend.tables["result"]
        frame.loc[:, "round3_status"] = ["-1", "9", "0", "1"]
        frame.loc[:, "error_message"] = "preserved error"
        seen = []
        with self.assertLogs(level="INFO") as logs:
            summary = run_impact(backend, self.relation_settings(backend), ai_config=self.config(),
                                 network_factory=self.factory(lambda request: seen.append(request)
                                                              or success(json.dumps(impact_result()))))
        output = [record.getMessage() for record in logs.records]
        startup = next(i for i, line in enumerate(output) if "启动准备：" in line)
        restored = next(i for i, line in enumerate(output) if "状态恢复完成" in line)
        ready = next(i for i, line in enumerate(output) if "启动准备完成" in line)
        network = next(i for i, line in enumerate(output) if "网络线程已就绪" in line)
        self.assertLess(startup, restored)
        self.assertLess(restored, ready)
        self.assertLess(ready, network)
        self.assertIn("历史失败=1，恢复数=2，待处理数=3", output[restored])
        self.assertIn("任务数=4，已有完成=1，待处理数=3，失败数=0，处理中数=0", output[ready])
        self.assertEqual((summary.new_completed, len(seen)), (3, 3))

    def test_completed_and_empty_scopes_log_zero_pending_without_ai(self):
        for completed in (False, True):
            with self.subTest(completed=completed):
                backend = self.backend([("2021-01-01", "event", ["ETH"], "a")])
                if completed:
                    self.relate(backend)
                    backend.tables["result"].loc[:, "round3_status"] = "1"
                    settings = self.relation_settings(backend)
                else:
                    settings = self.settings(start_time="2021-02-01", end_time="2021-02-01")
                with self.assertLogs("round3.runner", level="INFO") as logs:
                    summary = run_impact(backend, settings, network_factory=lambda config: self.fail("不应调用 AI"))
                count = int(completed)
                output = "\n".join(logs.output)
                self.assertIn("任务数={}，已有完成={}，待处理数=0".format(count, count), output)
                self.assertIn("本范围无需调用 AI", output)
                self.assertEqual(summary.http_attempts_started, 0)

    def test_impact_requires_explicit_valid_scope_before_any_read(self):
        backend = self.backend([("2025-01-01", "current", ["ETH"], "a")])
        for values in ({}, {"start_time": "2025-01-01"},
                       {"start_time": "2025-01-02", "end_time": "2025-01-01"}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                run_impact(backend, self.settings(**values))
        self.assertFalse(backend.reads or backend.column_reads or backend.meta_reads or backend.updates)

    def test_impact_update_only_reads_scope_and_sends_three_fields(self):
        backend = self.backend([
            ("2010-01-01", "old-origin", ["ETH"], "a"),
            ("2020-10-16", "outside", ["ETH"], "a"),
            ("2025-10-16 12:00:00", "current", ["ETH"], "a"),
            ("2025-10-16 12:00:00", "same-time", ["ETH"], "a"),
            ("2026-01-01", "future", ["ETH"], "a"),
        ])
        frame = backend.tables["result"]
        root, outside, current, same_time, future = frame.index
        frame.loc[:, "event_id"] = root[1]
        frame.loc[:, "event_relation"] = "repeat"
        frame.loc[:, "round2_status"] = "1"
        frame.loc[:, "round3_status"] = "1"
        frame.loc[root, ["event_relation", "round2_status"]] = ["new", "0"]
        frame.loc[current, ["event_relation", "round3_status", "new_information"]] = ["update", "9", "新进展"]
        before = frame.drop(index=current).copy(deep=True)
        del backend.tables["first"]
        read = backend.read
        def scoped_read(path, start_time=None, end_time=None, columns=None):
            self.assertEqual(path, "result", "第三轮不应读取原帖")
            if columns is None:
                self.assertEqual((start_time, end_time), ("2025-10-16", "2025-10-16"), "不应补读历史或扩大日期范围")
            else:
                self.assertEqual((start_time, end_time, columns), ("", "", ["round3_config_hash"]))
            return read(path, start_time, end_time, columns)
        seen = []
        with patch.object(backend, "read", side_effect=scoped_read):
            summary = run_impact(backend, self.settings(start_time="2025-10-16", end_time="2025-10-16"),
                                 ai_config=self.config(), network_factory=self.factory(
                                     lambda req: seen.append(json.loads(request_text(req)))
                                     or success(json.dumps(impact_result()))))
        self.assertEqual((summary.total, summary.already_completed, summary.new_completed), (2, 1, 1))
        self.assertEqual(seen, [{"asset": "ETH", "event": "current", "new_information": "新进展"}])
        self.assertEqual(backend.column_reads, [("result", "", "", ["round3_config_hash"])])
        self.assertFalse(backend.meta_reads)
        pd.testing.assert_frame_equal(frame.drop(index=current), before)

    def test_impact_asset_and_new_information_follow_each_record(self):
        backend = self.backend([("2025-01-01", "多币事件", ["BTC", "ETH"], "a"),
                                ("2025-01-02", "本次进展含旧事实", ["ETH"], "a")])
        frame = backend.tables["result"]
        frame.loc[:, "round2_status"] = "1"
        frame.loc[:, "event_relation"] = "new"
        frame.loc[:, "event_id"] = frame.index.get_level_values("record_id")
        current = frame.index[-1]
        info = '本次新增 "5000万美元"；附带条件保持原样。'
        frame.loc[current, ["event_relation", "event_id", "new_information"]] = ["update", "missing-group", info]
        del backend.tables["first"]
        seen = []
        summary = run_impact(backend, self.settings(start_time="2025-01-01", end_time="2025-01-02", history_lookback_years=1),
                             ai_config=self.config(), network_factory=self.factory(
                                 lambda req: seen.append(json.loads(request_text(req))) or success(json.dumps(impact_result()))))
        self.assertEqual(summary.new_completed, 3)
        self.assertCountEqual(seen, [{"asset": "BTC", "event": "多币事件"},
                                     {"asset": "ETH", "event": "多币事件"},
                                     {"asset": "ETH", "event": "本次进展含旧事实", "new_information": info}])
        self.assertTrue(all(path == "result" for path, _, _ in backend.reads))
        self.assertEqual(backend.column_reads, [("result", "", "", ["round3_config_hash"])])

    def test_impact_omits_empty_new_information_and_preserves_nonempty_text(self):
        for information in ("", None, " \t\n", "  本次新增事实  "):
            with self.subTest(information=information):
                backend = self.backend([("2025-01-01", "current", ["ETH"], "a")])
                frame = backend.tables["result"]
                key = frame.index[0]
                frame.loc[key, ["round2_status", "event_relation", "event_id"]] = ["1", "new", key[1]]
                expected = {"asset": "ETH", "event": "current"}
                if information == "  本次新增事实  ":
                    frame.loc[key, ["event_relation", "event_id"]] = ["update", "existing-group"]
                    expected["new_information"] = information
                frame.at[key, "new_information"] = information
                seen = []
                summary = run_impact(backend, self.settings(start_time="2025-01-01", end_time="2025-01-01"),
                                     ai_config=self.config(), network_factory=self.factory(
                                         lambda req: seen.append(json.loads(request_text(req)))
                                         or success(json.dumps(impact_result()))))
                self.assertEqual(summary.new_completed, 1)
                self.assertEqual(seen, [expected])
                self.assertEqual(frame.at[key, "new_information"], information)

    def test_impact_update_without_history_or_source_can_resume(self):
        for status in ("0", "9", "-1"):
            with self.subTest(status=status):
                backend = self.backend([("2025-01-01", "current", ["ETH"], "a")])
                frame = backend.tables["result"]
                current = frame.index[0]
                frame.loc[current, ["round2_status", "event_relation", "event_id", "new_information", "round3_status"]] = ["1", "update", "missing-group", "本次新增", status]
                second_before = frame[["event_id", "event_relation", "new_information", "round2_status", "round2_ai_response", "round2_config_hash"]].copy(deep=True)
                del backend.tables["first"]
                summary = run_impact(backend, self.settings(start_time="2025-01-01", end_time="2025-01-01"),
                                     ai_config=self.config(), network_factory=self.factory(
                                         lambda req: success(json.dumps(impact_result()))))
                self.assertEqual((summary.new_completed, summary.http_attempts_started), (1, 1))
                self.assertEqual(frame.loc[current, "round3_status"], "1")
                pd.testing.assert_frame_equal(frame[second_before.columns], second_before)
                self.assertTrue(all(path == "result" for path, _, _ in backend.reads))

    def test_completed_update_only_reads_scope_without_rechecking_old_origin(self):
        backend = self.backend([("2010-01-01", "old-origin", ["ETH"], "a"),
                                ("2025-01-01", "done", ["ETH"], "a")])
        frame = backend.tables["result"]
        root, current = frame.index
        frame.loc[current, ["round2_status", "round3_status", "event_relation", "event_id"]] = ["1", "1", "update", root[1]]
        before = frame.copy(deep=True)
        summary = run_impact(backend, self.settings(start_time="2025-01-01", end_time="2025-01-01"))
        self.assertEqual((summary.total, summary.already_completed), (1, 1))
        self.assertEqual(backend.reads, [("result", "2025-01-01", "2025-01-01")])
        self.assertFalse(backend.column_reads or backend.meta_reads or backend.updates)
        pd.testing.assert_frame_equal(frame, before)

    def test_impact_invalid_local_relations_stop_before_recovery_or_ai(self):
        cases = [("0", "new", "self"), ("-1", "new", "self"),
                 ("1", "", "group"), ("1", "uncertain", "group"),
                 ("1", "new", "other"), ("1", "update", ""),
                 ("1", "update", " "), ("1", "update", None),
                 ("1", "update", "self"), ("1", "repeat", "self"), ("1", "repeat", "")]
        for status, relation, group in cases:
            with self.subTest(status=status, relation=relation, group=group):
                backend = self.backend([("2025-01-01", "current", ["ETH"], "a")])
                frame = backend.tables["result"]
                current = frame.index[0]
                group = current[1] if group == "self" else group
                frame.loc[current, ["round2_status", "event_relation", "event_id", "round3_status"]] = [status, relation, group, "9"]
                before = frame.copy(deep=True)
                with self.assertRaisesRegex(ValueError, "第二轮未完成或结果不合法"):
                    run_impact(backend, self.settings(start_time="2025-01-01", end_time="2025-01-01"),
                               ai_config=self.config(), network_factory=lambda config: self.fail("不应发请求"))
                self.assertFalse(backend.updates or backend.column_reads or backend.meta_reads)
                pd.testing.assert_frame_equal(frame, before)

    def test_impact_invalid_task_projection_stops_before_recovery(self):
        for problem in ("missing-column", "wrong-range"):
            with self.subTest(problem=problem):
                backend = self.backend([("2025-01-01", "current", ["ETH"], "a")])
                frame = backend.tables["result"]
                current = frame.index[0]
                frame.loc[current, ["round2_status", "event_relation", "event_id", "round3_status"]] = ["1", "new", current[1], "9"]
                before = frame.copy(deep=True)
                read = backend.read
                def faulty_read(path, start_time=None, end_time=None, columns=None):
                    result = read(path, start_time, end_time, columns)
                    if problem == "missing-column":
                        return result.drop(columns="event")
                    result.index = pd.MultiIndex.from_tuples([(pd.Timestamp("2025-01-02"), current[1])], names=result.index.names)
                    return result
                with patch.object(backend, "read", side_effect=faulty_read), self.assertRaises(ValueError):
                    run_impact(backend, self.settings(start_time="2025-01-01", end_time="2025-01-01"),
                               ai_config=self.config(), network_factory=lambda config: self.fail("不应发请求"))
                self.assertFalse(backend.updates or backend.meta_reads)
                pd.testing.assert_frame_equal(frame, before)

    def test_impact_missing_source_does_not_block_later_month(self):
        backend = self.backend([("2021-01-01", "jan", ["ETH"], "a"), ("2021-02-01", "feb", ["ETH"], "a")])
        self.relate(backend)
        backend.tables["first"] = backend.tables["first"].iloc[:1].copy(deep=True)
        seen = []
        def handler(request):
            seen.append(json.loads(request_text(request))["event"])
            return success(json.dumps(impact_result()))
        summary = run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-02-01"),
                             ai_config=self.config(), network_factory=self.factory(handler))
        self.assertEqual(seen, ["jan", "feb"])
        self.assertEqual(summary.new_completed, 2)
        self.assertTrue(backend.tables["result"]["round3_status"].eq("1").all())
        self.assertTrue(backend.tables["result"]["round2_status"].eq("1").all())
        self.assertFalse(any(path == "first" for path, _, _ in backend.reads))
        self.assertFalse(any(thread.name == "ai-batch-network" for thread in threading.enumerate()))

    def test_impact_requires_all_scope_relations_complete(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        with self.assertRaisesRegex(ValueError, "第二轮未完成"):
            run_impact(backend, self.relation_settings(backend))
        self.assertFalse(backend.updates)

    def test_impact_failure_isolated_error_preserved_by_round2_and_retry_only_round3(self):
        backend = self.backend([("2021-01-01", "bad", ["ETH"], "a"),
                                ("2021-01-02", "good", ["ETH"], "a")])
        self.relate(backend)
        seen = []
        def handler(request):
            event = json.loads(request_text(request))["event"]
            seen.append(event)
            return success("{}" if event == "bad" else json.dumps(impact_result()))
        result = run_impact(backend, self.relation_settings(backend), ai_config=self.config(), network_factory=self.factory(handler))
        self.assertEqual(result.failed, 1)
        self.assertEqual(result.new_completed, 1)
        error = backend.tables["result"].iloc[0]["error_message"]
        self.assertTrue(error)
        run_relation(backend, self.relation_settings(backend))
        self.assertEqual(backend.tables["result"].iloc[0]["error_message"], error)
        count = []
        again = run_impact(backend, self.relation_settings(backend), ai_config=self.config(), network_factory=self.factory(
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
            return success(json.dumps({"event_relation": "repeat", "matched_candidate_id": body["candidates"][0]["candidate_id"], "new_information": None}))
        self.relate(backend, handler)
        source_before = backend.tables["first"].copy(deep=True)
        result = run_impact(backend, self.relation_settings(backend), ai_config=self.config(), network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
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
        for invalid in ({}, dict(impact_result(), direction="看涨")):
            with self.subTest(invalid=invalid):
                backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
                self.relate(backend)
                calls = []
                def handler(request):
                    calls.append(request)
                    return success(json.dumps(invalid if len(calls) == 1 else impact_result()))
                with patch("common.validation.validate_impact_output", wraps=validate_impact_output) as validate, \
                        self.assertLogs("common.network", level="WARNING") as logs:
                    result = run_impact(backend, self.relation_settings(backend), ai_config=self.config(), validator=validate,
                                        network_factory=self.factory(handler))
                self.assertEqual(len(calls), 2)
                self.assertEqual(validate.call_count, 2)
                self.assertEqual(request_text(calls[0]), request_text(calls[1]))
                self.assertEqual(result.http_attempts_started, 2)
                self.assertEqual(result.new_completed, 1)
                retry = next(line for line in logs.output if "业务输出无效，准备重试" in line)
                self.assertIn("原因=", retry)
                self.assertIn("direction" if invalid else "五个经济影响字段", retry)

    def test_invalid_impact_enum_exhausts_budget_and_preserves_final_response(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.relate(backend)
        self.document["run"]["max_attempts"] = 2
        raw = json.dumps(dict(impact_result(), execution_stage="已发生"), ensure_ascii=False)
        calls = []
        def handler(request):
            calls.append(request)
            return success(raw, usage={"total_tokens": len(calls)})
        with self.assertLogs("common.network", level="WARNING") as logs:
            result = run_impact(backend, self.relation_settings(backend), ai_config=self.config(),
                                network_factory=self.factory(handler))
        self.assertEqual((result.failed, result.new_completed, result.http_attempts_started), (1, 0, 2))
        row = backend.tables["result"].iloc[0]
        self.assertEqual((row["round2_status"], row["round3_status"]), ("1", "-1"))
        self.assertIn("execution_stage", row["error_message"])
        self.assertIn("已完成", row["error_message"])
        saved = json.loads(row["round3_ai_response"])
        self.assertEqual(saved["ai_response_content"], raw)
        self.assertEqual(saved["attempts"], 2)
        self.assertEqual(saved["usage"], {"total_tokens": 2})
        final = next(line for line in logs.output if "单任务请求最终失败" in line)
        self.assertIn("原因=第三轮 execution_stage 枚举不合法", final)
        self.assertNotIn("测试中的机制说明", "\n".join(logs.output))

    def test_interrupted_third_claim_preserves_second_and_resumes(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.relate(backend)
        def interrupted(path, frame):
            raise KeyboardInterrupt()
        backend.update_hook = interrupted
        with self.assertRaises(KeyboardInterrupt):
            run_impact(backend, self.relation_settings(backend), ai_config=self.config(), network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
        self.assertEqual(backend.tables["result"].iloc[0]["round2_status"], "1")
        self.assertEqual(backend.tables["result"].iloc[0]["round3_status"], "0")
        backend.update_hook = None
        result = run_impact(backend, self.relation_settings(backend), ai_config=self.config(), network_factory=self.factory(lambda req: success(json.dumps(impact_result()))))
        self.assertEqual(result.new_completed, 1)
        self.assertEqual(result.http_attempts_started, 1)

    def test_changed_third_model_stops_and_run_parameter_changes_can_resume(self):
        backend = self.backend([("2021-01-01", "good", ["ETH"], "a"), ("2021-01-02", "bad", ["ETH"], "a")])
        self.relate(backend)
        old_hash = ai_config_hash(self.config())
        def first_handler(request):
            event = json.loads(request_text(request))["event"]
            return success("{}" if event == "bad" else json.dumps(impact_result()))
        run_impact(backend, self.relation_settings(backend), ai_config=self.config(), network_factory=self.factory(first_handler))
        error = backend.tables["result"].iloc[1]["error_message"]
        self.assertTrue(backend.tables["result"]["round3_config_hash"].eq(old_hash).all())
        self.document["api"]["payload"]["model"] = "different-model"
        calls = []
        with self.assertRaisesRegex(ValueError, "配置摘要.*冲突"):
            run_impact(backend, self.relation_settings(backend), ai_config=self.config(),
                       network_factory=self.factory(lambda req: calls.append(req) or success(json.dumps(impact_result()))))
        self.assertFalse(calls)
        self.assertEqual(backend.tables["result"].iloc[1]["round3_status"], "0")
        self.assertEqual(backend.tables["result"].iloc[1]["error_message"], error)
        self.document["api"]["payload"]["model"] = "test"
        self.document["run"].update(concurrency=1, max_attempts=1)
        self.assertEqual(ai_config_hash(self.config()), old_hash)
        result = run_impact(backend, self.relation_settings(backend), ai_config=self.config(),
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
        backend.meta_reads.clear()
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
            current = json.loads(request_text(request))
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
        self.assertEqual(backend.meta_reads, [])
        self.assertTrue(all(start is not None for _, start, _ in backend.reads))
        self.assertEqual(maximum[0], 2)
        self.assertEqual((summary.total, summary.new_completed, summary.http_attempts_started), (4, 4, 4))
        self.assertEqual([json.loads(json.loads(value)["ai_response_content"])["materiality_reason"] for value in backend.tables["result"]["round3_ai_response"]], ["jan-a", "jan-b", "feb-a", "feb-b"])
        self.assertTrue(all(ref() is None for ref in snapshots))

        # 两个月：范围检查 2 次、恢复 2 次、执行 2 次；不为月末检查再读表。
        self.assertEqual(len([read for read in backend.reads if read[0] == "result"]), 6)
        self.assertFalse(any(path == "first" for path, _, _ in backend.reads))

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

    def test_impact_only_changes_current_scope_and_preserves_other_records(self):
        backend = self.backend([("2020-01-01", "origin", ["ETH"], "a"),
                                ("2021-02-01", "progress", ["ETH"], "a"),
                                ("2021-03-01", "current", ["ETH"], "a"),
                                ("2021-04-01", "future", ["ETH"], "a")])
        frame = backend.tables["result"]
        root, progress, current, future = frame.index
        frame.loc[:, "round2_status"] = "1"
        frame.loc[:, "round2_config_hash"] = ai_config_hash(self.config())
        frame.loc[:, "event_id"] = root[1]
        frame.loc[:, "event_relation"] = "update"
        frame.loc[:, "new_information"] = frame["event"]
        frame.loc[root, ["event_relation", "new_information"]] = ["new", ""]
        outside_before = frame.drop(index=current).copy(deep=True)
        seen = []
        run_impact(backend, self.settings(start_time="2021-03-01", end_time="2021-03-31"),
                   ai_config=self.config(), network_factory=self.factory(
                       lambda req: seen.append(json.loads(request_text(req))) or success(json.dumps(impact_result()))))
        self.assertEqual(seen, [{"asset": "ETH", "event": "current", "new_information": "current"}])
        self.assertEqual(list(frame["round3_status"]), ["0", "0", "1", "0"])
        pd.testing.assert_frame_equal(frame.drop(index=current), outside_before)
        self.assertTrue(all(path == "result" and start == "2021-03-01" and end == "2021-03-31"
                            for path, start, end in backend.reads))

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
            event = json.loads(request_text(request))["event"]
            calls.append(event)
            return success("{}" if event == "jan" else json.dumps(impact_result()))

        config = self.config()
        summary = run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-02-01"),
                             ai_config=config, network_factory=self.factory(handler))
        self.assertEqual(calls, ["jan"] * config.run.max_attempts + ["feb"])
        self.assertEqual((summary.failed, summary.new_completed, summary.processing), (1, 1, 0))
        self.assertEqual(list(backend.tables["result"]["round3_status"]), ["-1", "1"])

    def test_impact_partial_month_excludes_outside_processing_states(self):
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
        # 任务表乱序，范围外的 9 不应被恢复或阻止本范围结束。
        backend.tables["result"] = frame.iloc[[2, 1, 0]].copy(deep=True)
        seen = []
        summary = run_impact(backend, self.settings(start_time="2021-01-15", end_time="2021-01-15"),
                             ai_config=self.config(), network_factory=self.factory(
                                 lambda req: seen.append(json.loads(request_text(req)))
                                 or success(json.dumps(impact_result()))))
        self.assertEqual((summary.total, summary.new_completed), (1, 1))
        self.assertEqual(seen, [{"asset": "ETH", "event": "current", "new_information": "current"}])
        self.assertEqual(backend.tables["result"].loc[current, "round3_status"], "1")
        self.assertTrue(backend.tables["result"].loc[[root, future], "round3_status"].eq("9").all())
        self.assertEqual([read for read in backend.reads if read[0] == "result"],
                         [("result", "2021-01-15", "2021-01-15")] * 3)
        self.assertEqual(backend.column_reads, [("result", "", "", ["round3_config_hash"])])

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

    def test_impact_new_only_never_reads_meta_or_history(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a")])
        self.relate(backend)
        backend.tables["result"].loc[:, "round3_status"] = "9"
        backend.metadata["result"] = {"meta": {"start_time": None, "end_time": None}}
        backend.meta_reads.clear()
        backend.column_reads.clear()
        with patch.object(backend, "read_meta", side_effect=AssertionError("不应读取元数据")):
            summary = run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-01-31"),
                                 ai_config=self.config(), network_factory=self.factory(
                                     lambda req: success(json.dumps(impact_result()))))
        self.assertEqual(summary.new_completed, 1)
        self.assertFalse(backend.meta_reads)
        self.assertEqual(backend.column_reads, [("result", "", "", ["round3_config_hash"])])

    def test_impact_scope_reads_release_complete_frames_during_startup(self):
        backend = self.backend([("2021-01-31 20:00:00", "origin", ["ETH"], "a"),
                                ("2021-03-05 12:00:00", "current", ["ETH"], "a")])
        self.relate(backend)
        backend.reads.clear()
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
        self.assertEqual(result_reads, [("result", "2021-03-05", "2021-03-05")] * 3)
        self.assertEqual(len(backend.reads), 3)
        self.assertTrue(all(start is not None for _, start, _ in backend.reads))
        self.assertTrue(all(ref() is None for ref in snapshots))

    def test_impact_task_read_failure_never_falls_back_or_recovers(self):
        backend = self.backend([("2025-01-01", "current", ["ETH"], "a")])
        frame = backend.tables["result"]
        current = frame.index[0]
        frame.loc[current, ["round2_status", "event_relation", "event_id", "round3_status"]] = ["1", "new", current[1], "9"]
        baseline = frame.copy(deep=True)
        read = backend.read
        def fail_tasks(path, start_time=None, end_time=None, columns=None):
            read(path, start_time, end_time, columns)
            raise RuntimeError("任务读取失败")
        with patch.object(backend, "read", side_effect=fail_tasks), self.assertRaisesRegex(RuntimeError, "任务读取失败"):
            run_impact(backend, self.settings(start_time="2025-01-01", end_time="2025-01-01"),
                       ai_config=self.config(), network_factory=lambda config: self.fail("不能发请求"))
        self.assertEqual(backend.reads, [("result", "2025-01-01", "2025-01-01")])
        self.assertFalse(backend.updates or backend.meta_reads or backend.column_reads)
        pd.testing.assert_frame_equal(frame, baseline)

    def test_impact_single_hash_column_detects_config_conflict_after_processing_end(self):
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
        self.assertNotIn(("result", "2021-03-01", "2021-03-01"), backend.reads)
        self.assertIn(("result", "", "", ["round3_config_hash"]), backend.column_reads)
        self.assertTrue(all(start is not None for _, start, _ in backend.reads))
        pd.testing.assert_frame_equal(frame, baseline)

    def test_impact_scope_rejects_duplicate_record_id_across_months(self):
        backend = self.backend([("2021-01-01", "first", ["ETH"], "a"),
                                ("2021-02-01", "second", ["ETH"], "a")])
        self.relate(backend)
        frame = backend.tables["result"]
        frame.index = pd.MultiIndex.from_tuples([(key[0], frame.index[0][1]) for key in frame.index],
                                                names=frame.index.names)
        frame.loc[:, "event_id"] = frame.index[0][1]
        count = len(backend.updates)
        with self.assertRaisesRegex(ValueError, "record_id 跨分段重复"):
            run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-02-01"),
                       ai_config=self.config(), network_factory=lambda config: self.fail("不能发请求"))
        self.assertEqual(len(backend.updates), count)

    def test_impact_empty_scope_reads_only_requested_dates(self):
        backend = self.backend([("2021-01-01", None, [], "a")])
        summary = run_impact(backend, self.settings(start_time="2021-01-01", end_time="2021-01-31"),
                             network_factory=lambda config: self.fail("不能发请求"))
        self.assertEqual((summary.total, summary.new_completed, summary.http_attempts_started), (0, 0, 0))
        self.assertEqual(backend.meta_reads, [])
        self.assertEqual(backend.reads, [("result", "2021-01-01", "2021-01-31")])
        self.assertTrue(all(start is not None for _, start, _ in backend.reads))

    def test_impact_calendar_preserves_timezone_and_precise_index(self):
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
