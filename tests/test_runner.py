import asyncio
import contextlib
import io
import json
import queue
import threading
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pandas as pd

from ai_batch import BatchPersistenceError, BatchRunError, run_worker
from ai_batch.config import config_hash
from ai_batch.response import parse_response
from ai_batch.runner import (CompletionMessage, _MonthCoordinator, _month_ranges,
                             _NetworkWorker, _retry_after)
from ai_batch.tasks import TaskStore, TaskValidationError
from tests.support import FakeCli, Fixture, request_text, success, tasks


class RunnerTests(Fixture, unittest.TestCase):
    def test_legacy_username_schema_is_rejected_before_ai_requests(self):
        frame = tasks(["2021-01-01"])
        for invalid in (
            frame.rename(columns={"user_name": "username"}),
            frame.assign(username="extra user"),
        ):
            with self.subTest(columns=list(invalid.columns)):
                cli = FakeCli(invalid)
                requests = []

                def handler(request):
                    requests.append(request)
                    return success()

                with self.assertRaisesRegex(TaskValidationError, "任务表必须恰好包含约定的九个普通列"):
                    self.worker(cli, handler)
                self.assertFalse(requests)
                self.assertFalse(cli.updates)
                self.assertIn("TaskValidationError: 任务表必须恰好包含约定的九个普通列", self.last_output)
                self.assertIn("Traceback", self.last_output)
                self.assertIn("INFO:ai_batch.runner:worker 收尾结束", self.last_output)

    def test_index_field_and_range_validation_logs_specific_reasons(self):
        frame = tasks(["2021-01-01"])
        cases = [
            (frame.rename_axis(["posted_at", "id"]), None, None,
             "索引必须为名称和顺序固定的 date/id MultiIndex"),
            (frame.assign(status=0), None, None, "status 的所有单元格必须为字符串"),
            (frame, "2021-01-02", "2021-02-01", "cli.read 返回了当前日期范围外的任务"),
        ]
        for invalid, start, end, reason in cases:
            with self.subTest(reason=reason):
                cli = FakeCli(invalid)
                # 模拟真实客户端返回非法索引、类型或范围，不能由假客户端先过滤掉。
                with patch.object(cli, "read", return_value=invalid), self.assertRaises(TaskValidationError):
                    self.worker(cli, lambda request: success(), start, end)
                self.assertIn("TaskValidationError: " + reason, self.last_output)
                self.assertFalse(cli.updates)
                self.assertFalse(self.clients)

    def test_old_results_do_not_block_pending_requests_and_successes_are_skipped(self):
        frame = tasks(pd.date_range("2021-01-01", periods=4))
        frame["status"] = ["0", "9", "-1", "1"]
        for column in ("ai_response_content", "reasoning_content", "usage_json",
                       "response_created", "error_message"):
            frame.loc[frame.index[:3], column] = [None, 7, pd.NA]
        frame.loc[frame.index[:3], "usage_json"] = ["invalid JSON", float("nan"), pd.NA]
        frame.at[frame.index[3], "config_hash"] = config_hash(self.config())
        frame.at[frame.index[3], "ai_response_content"] = "saved answer"
        original_success = frame.iloc[3].copy()
        cli = FakeCli(frame)
        requested_texts = []

        def handler(request):
            requested_texts.append(request_text(request))
            return success("new answer", usage={"total_tokens": 3}, created=100)

        summary = self.worker(cli, handler)
        self.assertEqual(summary.already_succeeded, 1)
        self.assertEqual(summary.new_succeeded, 3)
        self.assertEqual(summary.http_attempts_started, 3)
        self.assertCountEqual(requested_texts, ["post 0", "post 1", "post 2"])
        self.assertTrue(all(isinstance(value, str) for value in cli.frame.to_numpy().flat))
        pd.testing.assert_series_equal(cli.frame.iloc[3], original_success)
        pd.testing.assert_frame_equal(cli.frame[["text", "user_name"]], frame[["text", "user_name"]])

    def test_startup_failure_logs_reason_before_any_task_access(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        with self.assertRaises(ValueError):
            self.worker(cli, lambda request: success(), "2021-02-01", "2021-01-01")
        self.assertIn("ValueError: start_time 不能晚于 end_time", self.last_output)
        self.assertIn("尚未读取或更新任务", self.last_output)
        self.assertFalse(cli.reads)
        self.assertFalse(cli.updates)
        self.assertFalse(self.clients)

    def test_next_month_validation_failure_keeps_previous_results(self):
        cli = FakeCli(tasks(["2021-01-01", "2021-02-01"]))

        def before_read(start, end):
            if start[5:7] == "02":
                raise TaskValidationError("第二月任务校验失败")

        cli.read_hook = before_read
        with self.assertRaises(TaskValidationError):
            self.worker(cli, lambda request: success(), "2021-01-01", "2021-03-01")
        self.assertEqual(list(cli.frame["status"]), ["1", "0"])
        self.assertEqual(len(cli.reads), 2)
        self.assertIn("开始处理当前分段：起点=2021-02-01，终点=2021-02-28", self.last_output)
        self.assertIn("TaskValidationError: 第二月任务校验失败", self.last_output)
        self.assertNotIn("全部处理完了", self.last_output)

    def test_read_failure_logs_original_cause_using_standard_logging(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        error = RuntimeError("读取失败")
        error.__cause__ = ConnectionError("数据库连接超时")

        def fail_read(start, end):
            raise error

        cli.read_hook = fail_read
        with self.assertRaises(RuntimeError) as caught:
            self.worker(cli, lambda request: success())
        self.assertIs(caught.exception, error)
        self.assertIn("ConnectionError: 数据库连接超时", self.last_output)
        self.assertIn("RuntimeError: 读取失败", self.last_output)
        self.assertIn("Traceback", self.last_output)
        stop = next(record for record in self.log_records if "worker 异常停止" in record.getMessage())
        self.assertIs(stop.exc_info[1], error)
        self.assertFalse(cli.updates)
        self.assertFalse(self.clients)

    def test_config_conflict_logs_reason_before_claim(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        cli.frame["config_hash"] = "a" * 64
        self.assertNotEqual(config_hash(self.config()), "a" * 64)
        with self.assertRaises(TaskValidationError):
            self.worker(cli, lambda request: success())
        self.assertIn("TaskValidationError: 本段已有配置摘要与本轮提示词或 AI 参数冲突", self.last_output)
        self.assertFalse(cli.updates)

    def test_reset_and_claim_failures_keep_pending_updates_without_requests(self):
        for status, stage in (("9", "reset"), ("0", "claim")):
            with self.subTest(stage=stage):
                cli = FakeCli(tasks(["2021-01-01"]))
                cli.frame["status"] = status

                def fail_update(frame):
                    raise RuntimeError("存储不可用")

                cli.update_hook = fail_update
                calls = []
                with self.assertRaises(BatchPersistenceError) as caught:
                    self.worker(cli, lambda request: calls.append(request) or success())
                self.assertIn("RuntimeError: 存储不可用", self.last_output)
                self.assertEqual(caught.exception.stage, stage)
                self.assertEqual(cli.frame.iloc[0]["status"], status)
                self.assertEqual(len(caught.exception.pending_patch), 1)
                self.assertFalse(calls)

    def test_readback_validation_failure_logs_original_storage_and_validation_reasons(self):
        cli = FakeCli(tasks(["2021-01-01"]))

        def update(frame):
            cli.frame["user_name"] = "changed user"
            raise RuntimeError("写入中断")

        cli.update_hook = update
        with self.assertRaises(BatchPersistenceError) as caught:
            self.worker(cli, lambda request: success())
        self.assertIsInstance(caught.exception.__cause__, TaskValidationError)
        self.assertIn("TaskValidationError: 读回时帖子输入或未提交摘要发生变化", self.last_output)
        self.assertIn("RuntimeError: 写入中断", self.last_output)
        self.assertEqual(caught.exception.stage, "claim")

    def test_concurrency_and_calling_thread_storage(self):
        cli = FakeCli(tasks(pd.date_range("2021-01-01", periods=15)))
        main_thread = threading.get_ident()
        active = 0
        peak = 0
        network_threads = set()

        async def handler(request):
            nonlocal active, peak
            network_threads.add(threading.get_ident())
            payload = json.loads(request.content)
            self.assertEqual(set(payload), {"model", "messages", "stream"})
            self.assertNotIn("private-user", request.content.decode())
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1
            return success(request_text(request))

        summary = self.worker(cli, handler)
        self.assertEqual(summary.new_succeeded, 15)
        self.assertEqual(summary.http_attempts_started, 15)
        self.assertEqual(peak, 3)
        self.assertEqual(set(cli.threads), {main_thread})
        self.assertEqual(len(network_threads), 1)
        self.assertNotIn(main_thread, network_threads)
        self.assertEqual(len(self.clients), 1)
        self.assertEqual(summary.total, summary.already_succeeded + summary.new_succeeded + summary.new_failed + summary.pending + summary.processing)

    def test_reverse_concurrent_completions_save_sorted_batches_with_aligned_answers(self):
        self.document["run"].update({
            "concurrency": 4, "batch_size": 2, "result_buffer_size": 2,
            "flush_interval_seconds": 100,
        })
        frame = tasks(pd.DatetimeIndex(["2021-01-01 00:00:00.123456789"] * 4, tz="Asia/Shanghai"))
        frame.index = pd.MultiIndex.from_arrays([
            frame.index.get_level_values("date"), ["2", "10", "3", "1"],
        ], names=["date", "id"])
        cli = FakeCli(frame)
        completed = []
        events = {}

        async def handler(request):
            text = request_text(request)
            position = int(text.split()[-1])
            # 在网络事件循环内建立事件，强制后提交的请求先完成。
            events.setdefault(position, asyncio.Event())
            if position < 3:
                events.setdefault(position + 1, asyncio.Event())
                await asyncio.wait_for(events[position + 1].wait(), timeout=2)
                await asyncio.sleep(0)
            completed.append(position)
            events[position].set()
            return success("answer " + text, usage={"total_tokens": position}, created=100 + position)

        summary = self.worker(cli, handler)
        self.assertEqual(completed, [3, 2, 1, 0])
        final_updates = [update for update in cli.updates if "ai_response_content" in update.columns]
        self.assertEqual([list(update.index.get_level_values("id")) for update in final_updates],
                         [["1", "3"], ["10", "2"]])
        for position, key in enumerate(frame.index):
            self.assertEqual(cli.frame.at[key, "ai_response_content"], "answer " + frame.at[key, "text"])
            self.assertEqual(json.loads(cli.frame.at[key, "usage_json"]), {"total_tokens": position})
        pd.testing.assert_frame_equal(cli.frame[["text", "user_name"]], frame[["text", "user_name"]])
        pd.testing.assert_index_equal(cli.frame.index, frame.index)
        self.assertEqual((summary.new_succeeded, summary.new_failed, summary.http_attempts_started), (4, 0, 4))

    def test_full_year_reads_and_saves_each_month_before_next(self):
        month_starts = pd.date_range("2021-01-01", "2022-01-01", freq="MS")
        dates = month_starts[:-1].union(month_starts[1:] - pd.Timedelta(nanoseconds=1))
        frame = tasks(dates.append(month_starts[-1:]))
        frame.at[frame.index[0], "status"] = "9"
        frame.at[frame.index[2], "status"] = "-1"
        frame.at[frame.index[1], "status"] = "1"
        frame.at[frame.index[1], "config_hash"] = config_hash(self.config())
        frame.at[frame.index[1], "ai_response_content"] = "previous success"
        cli = FakeCli(frame)

        def before_read(start, end):
            previous = cli.frame.index.get_level_values("date") < start
            self.assertTrue(cli.frame.loc[previous, "status"].eq("1").all())

        cli.read_hook = before_read
        summary = self.worker(cli, lambda request: success(), "2021-01-01", "2021-12-31")
        self.assertEqual(len(cli.reads), 12)
        self.assertEqual(cli.reads, [
            ("2021-01-01", "2021-01-31"), ("2021-02-01", "2021-02-28"),
            ("2021-03-01", "2021-03-31"), ("2021-04-01", "2021-04-30"),
            ("2021-05-01", "2021-05-31"), ("2021-06-01", "2021-06-30"),
            ("2021-07-01", "2021-07-31"), ("2021-08-01", "2021-08-31"),
            ("2021-09-01", "2021-09-30"), ("2021-10-01", "2021-10-31"),
            ("2021-11-01", "2021-11-30"), ("2021-12-01", "2021-12-31"),
        ])
        self.assertEqual(summary.total, 24)
        self.assertEqual(summary.already_succeeded, 1)
        self.assertEqual(summary.new_succeeded, 23)
        self.assertEqual(summary.http_attempts_started, 23)
        self.assertEqual(len(self.clients), 1)
        self.assertTrue(cli.frame.iloc[:-1]["status"].eq("1").all())
        self.assertEqual(cli.frame.iloc[-1]["status"], "0")
        self.assertEqual(len([update for update in cli.updates if list(update.columns) == ["status"]]), 2)
        segment_logs = [record for record in self.log_records
                        if record.getMessage().startswith("开始处理当前分段：")]
        self.assertEqual([record.args for record in segment_logs], cli.reads)
        self.assertEqual([record.getMessage() for record in segment_logs], [
            "开始处理当前分段：起点={}，终点={}".format(start, end)
            for start, end in cli.reads
        ])
        startup = next(record.getMessage() for record in self.log_records
                       if record.getMessage().startswith("worker 启动校验完成："))
        self.assertIn("起点=2021-01-01，终点=2021-12-31，", startup)
        self.assertNotIn("00:00:00", startup)
        read_logs = [record for record in self.log_records
                     if record.getMessage().startswith("当前分段读取完成：")]
        self.assertEqual([record.args[:2] for record in read_logs], cli.reads)

    def test_empty_months_and_failed_posts_do_not_restart_or_end_loop(self):
        cli = FakeCli(tasks(["2021-01-01", "2021-12-01"]))
        calls = []

        def handler(request):
            calls.append(request_text(request))
            if len(calls) == 1:
                return httpx.Response(200, json={"error": {"message": "content error"}})
            return success()

        summary = self.worker(cli, handler, "2021-01-01", "2021-12-31")
        self.assertEqual(len(cli.reads), 12)
        self.assertEqual(len(calls), 2)
        self.assertEqual(summary.new_failed, 1)
        self.assertEqual(summary.new_succeeded, 1)
        self.assertEqual(list(cli.frame["status"]), ["-1", "1"])
        self.assertFalse(any(list(update.columns) == ["status"] for update in cli.updates))

    def test_all_succeeded_empty_months_do_not_hash_or_open_client(self):
        frame = tasks(["2021-01-01", "2021-12-01"])
        frame["status"] = "1"
        frame["config_hash"] = "f" * 64
        frame["ai_response_content"] = "saved"
        cli = FakeCli(frame)
        with patch("ai_batch.tasks.config_hash", side_effect=AssertionError("early hash")):
            summary = self.worker(cli, lambda request: success(), "2021-01-01", "2021-12-31")
        self.assertEqual(summary.already_succeeded, 2)
        self.assertEqual(summary.http_attempts_started, 0)
        self.assertEqual(len(cli.reads), 12)
        self.assertFalse(self.clients)

    def test_partial_months_boundaries_and_original_index(self):
        frame = tasks([
            "2021-01-14 23:59:59.999999999", "2021-01-15", "2021-01-31 23:59:59.999999999",
            "2021-02-01", "2021-02-28 23:59:59.999999999", "2021-03-09", "2021-03-10",
            "2021-03-10 23:59:59.999999999", "2021-03-11", "2021-03-12",
        ])
        frame["status"] = ["9", "0", "0", "0", "0", "0", "-1", "0", "-1", "0"]
        cli = FakeCli(frame)
        summary = self.worker(cli, lambda request: success(), "2021-01-15", "2021-03-10")
        self.assertEqual(summary.total, 7)
        self.assertEqual(summary.new_succeeded, 7)
        self.assertEqual(summary.http_attempts_started, 7)
        self.assertEqual(len(cli.reads), 3)
        self.assertEqual(list(cli.frame["status"]), ["9", "1", "1", "1", "1", "1", "1", "1", "-1", "0"])
        self.assertTrue(cli.frame.index.equals(frame.index))
        self.assertEqual(cli.reads, [
            ("2021-01-15", "2021-01-31"),
            ("2021-02-01", "2021-02-28"),
            ("2021-03-01", "2021-03-10"),
        ])

    def test_single_day_includes_last_instant_and_excludes_adjacent_days(self):
        frame = tasks([
            "2020-02-28 23:59:59.999999999", "2020-02-29",
            "2020-02-29 23:59:59.999999999", "2020-03-01",
        ])
        frame["status"] = ["9", "0", "-1", "0"]
        cli = FakeCli(frame)
        summary = self.worker(cli, lambda request: success(), "2020-02-29", "2020-02-29")
        self.assertEqual(cli.reads, [("2020-02-29", "2020-02-29")])
        self.assertEqual(summary.total, 2)
        self.assertEqual(summary.new_succeeded, 2)
        self.assertEqual(summary.http_attempts_started, 2)
        self.assertEqual(list(cli.frame["status"]), ["9", "1", "1", "0"])
        self.assertTrue(cli.frame.index.equals(frame.index))

    def test_cutoff_on_next_month_first_day_includes_that_whole_day(self):
        frame = tasks([
            "2020-12-30", "2020-12-31", "2020-12-31 23:59:59.999999999",
            "2021-01-01", "2021-01-01 23:59:59.999999999", "2021-01-02",
        ])
        cli = FakeCli(frame)
        summary = self.worker(cli, lambda request: success(), "2020-12-31", "2021-01-01")
        self.assertEqual(cli.reads, [("2020-12-31", "2020-12-31"), ("2021-01-01", "2021-01-01")])
        self.assertEqual(summary.total, 4)
        self.assertEqual(summary.new_succeeded, 4)
        self.assertEqual(summary.http_attempts_started, 4)
        self.assertEqual(list(cli.frame["status"]), ["0", "1", "1", "1", "1", "0"])

    def test_invalid_ranges_fail_before_read_update_or_configuration(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        for start, end in ((None, "2021-02-01"), ("2021-01-01", None), ("bad", "2021-02-01"),
                           ("2021-02-01", "2021-01-01"), ("2021-01-01", "2021-02-29"),
                           ("2021-01-01", "2021-02-01T00:00:00+00:00"),
                           ("2021-01-01T00:00:00+08:00", "2021-02-01T00:00:00+00:00")):
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                run_worker(cli, self.task_path, "does-not-exist.yaml", start, end)
        self.assertFalse(cli.reads)
        self.assertFalse(cli.updates)

    def test_other_http_errors_stop_without_reading_future_month(self):
        self.document["run"]["concurrency"] = 1
        for status in (300, 302, 400, 401, 403, 404, 405, 408, 415, 422, 429, 499, 600):
            with self.subTest(status=status):
                cli = FakeCli(tasks(["2021-01-01", "2021-02-01"]))
                calls = []

                def handler(request):
                    calls.append(request)
                    return httpx.Response(status, headers={"Retry-After": "100"}, json={"error": {"message": "manual adjustment"}})

                with patch("ai_batch.runner._retry_after", side_effect=AssertionError("must not wait")):
                    with self.assertRaises(BatchRunError):
                        self.worker(cli, handler, "2021-01-01", "2021-03-01")
                self.assertEqual(len(calls), 1)
                self.assertEqual(len(cli.reads), 1)
                self.assertEqual(list(cli.frame["status"]), ["-1", "0"])
                self.assertNotIn("全部处理完了", self.last_output)

    def test_all_5xx_retry_then_stop_at_shared_budget(self):
        self.document["run"]["concurrency"] = 1
        for status in range(500, 600):
            with self.subTest(status=status):
                cli = FakeCli(tasks(["2021-01-01"]))
                calls = []

                def handler(request):
                    calls.append(request)
                    return httpx.Response(status, text="non-JSON upstream error")

                with self.assertRaises(BatchRunError):
                    self.worker(cli, handler)
                self.assertEqual(len(calls), 3)
                self.assertEqual(cli.frame.iloc[0]["status"], "-1")
                self.assertEqual(cli.frame.iloc[0]["error_message"], "HTTP {}".format(status))

    def test_mixed_retry_success_keeps_only_last_usage_and_created(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        calls = []

        def handler(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(503, json={"error": "upstream", "usage": {"total_tokens": 99}, "created": 1})
            if len(calls) == 2:
                raise httpx.ReadTimeout("temporary read timeout", request=request)
            return success("final", usage={"total_tokens": 5}, created=3)

        summary = self.worker(cli, handler)
        self.assertEqual(summary.http_attempts_started, 3)
        self.assertEqual(json.loads(cli.frame.iloc[0]["usage_json"]), {"total_tokens": 5})
        self.assertEqual(cli.frame.iloc[0]["response_created"], "1970-01-01 08:00:03")
        self.assertEqual(len([frame for frame in cli.updates if len(frame.columns) == 6]), 1)

    def test_invalid_created_fails_only_one_post_without_retry(self):
        cli = FakeCli(tasks(["2021-01-01", "2021-01-02"]))
        requested_texts = []

        def handler(request):
            text = request_text(request)
            requested_texts.append(text)
            created = "invalid" if text == "post 0" else 1791947560
            return success("new answer", usage={"total_tokens": 7}, created=created)

        summary = self.worker(cli, handler)
        self.assertEqual(summary.new_failed, 1)
        self.assertEqual(summary.new_succeeded, 1)
        self.assertEqual(summary.http_attempts_started, 2)
        self.assertCountEqual(requested_texts, ["post 0", "post 1"])
        failed = cli.frame.iloc[0]
        self.assertEqual(failed["status"], "-1")
        for column in ("ai_response_content", "reasoning_content", "usage_json", "response_created"):
            self.assertEqual(failed[column], "")
        self.assertTrue(failed["error_message"].startswith("response_created 无法转换："))
        self.assertEqual(cli.frame.iloc[1]["response_created"], "2026-10-14 11:12:40")

    def test_network_budget_failure_is_single_post_and_restarts_from_zero(self):
        self.document["run"]["concurrency"] = 1
        cli = FakeCli(tasks(["2021-01-01", "2021-01-02"]))
        count = {}

        def handler(request):
            text = request_text(request)
            count[text] = count.get(text, 0) + 1
            if text == "post 0":
                if count[text] == 1:
                    return httpx.Response(503, json={"error": "upstream"})
                raise httpx.ConnectError("network unavailable", request=request)
            return success()

        summary = self.worker(cli, handler)
        self.assertEqual(summary.new_failed, 1)
        self.assertEqual(summary.new_succeeded, 1)
        self.assertEqual(count["post 0"], 3)
        summary = self.worker(cli, lambda request: success())
        self.assertEqual(summary.already_succeeded, 1)
        self.assertEqual(summary.new_succeeded, 1)
        self.assertEqual(summary.http_attempts_started, 1)

    def test_network_exception_scope(self):
        self.document["run"]["concurrency"] = 1
        for exception_type in (httpx.ConnectError, httpx.ReadError, httpx.WriteError,
                               httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout):
            with self.subTest(exception=exception_type):
                cli = FakeCli(tasks(["2021-01-01"]))
                calls = []

                def handler(request):
                    calls.append(request)
                    raise exception_type("network", request=request)

                summary = self.worker(cli, handler)
                self.assertEqual(summary.new_failed, 1)
                self.assertEqual(len(calls), 3)
        for exception_type in (httpx.PoolTimeout, httpx.ProxyError, httpx.LocalProtocolError,
                               httpx.RemoteProtocolError, httpx.UnsupportedProtocol, httpx.InvalidURL):
            with self.subTest(exception=exception_type):
                cli = FakeCli(tasks(["2021-01-01"]))
                calls = []

                def handler(request):
                    calls.append(request)
                    raise exception_type("request setup")

                with self.assertRaises(BatchRunError):
                    self.worker(cli, handler)
                self.assertEqual(len(calls), 1)
                self.assertEqual(cli.frame.iloc[0]["status"], "-1")
        cli = FakeCli(tasks(["2021-01-01"]))

        def decoding_error(request):
            raise httpx.DecodingError("bad encoding", request=request)

        self.assertEqual(self.worker(cli, decoding_error).new_failed, 1)

    def test_stop_saves_completed_results_and_leaves_cancelled_tasks_at_9(self):
        cli = FakeCli(tasks(["2021-01-01"] * 3 + ["2021-02-01"] * 2))
        self.document["run"]["flush_interval_seconds"] = 10

        async def handler(request):
            text = request_text(request)
            if text == "post 0":
                return success("completed")
            if text == "post 1":
                await asyncio.sleep(0.02)
                return httpx.Response(429, json={"error": "adjust concurrency"})
            await asyncio.sleep(60)
            return success("must be cancelled")

        with self.assertRaises(BatchRunError):
            self.worker(cli, handler, "2021-01-01", "2021-03-01")
        self.assertEqual(cli.frame.iloc[0]["status"], "1")
        self.assertEqual(cli.frame.iloc[1]["status"], "-1")
        self.assertEqual(cli.frame.iloc[2]["status"], "9")
        self.assertEqual(cli.frame.iloc[3]["status"], "0")
        self.assertEqual(cli.frame.iloc[4]["status"], "0")
        self.assertEqual(len(cli.reads), 1)

    def test_saving_failure_does_not_call_ai_again_and_retains_patch(self):
        self.document["run"]["concurrency"] = 1
        cli = FakeCli(tasks(["2021-01-01"]))
        calls = []

        def update(frame):
            if len(frame.columns) == 6:
                raise RuntimeError("storage unavailable")
            cli.apply(frame)

        cli.update_hook = update

        def handler(request):
            calls.append(request)
            return success("preserved in memory")

        with self.assertRaises(BatchPersistenceError) as caught:
            self.worker(cli, handler)
        self.assertEqual(len(calls), 1)
        self.assertEqual(caught.exception.stage, "final")
        self.assertEqual(caught.exception.pending_patch.iloc[0]["ai_response_content"], "preserved in memory")
        self.assertEqual(len(caught.exception.unsaved_results), 1)
        self.assertEqual(cli.frame.iloc[0]["status"], "9")
        self.assertEqual(len([frame for frame in cli.updates if len(frame.columns) == 6]), 3)
        self.assertIn("RuntimeError: storage unavailable", self.last_output)

    def test_partial_final_save_only_retries_unconfirmed_keys_and_retains_results(self):
        self.document["run"].update({"batch_size": 3, "flush_interval_seconds": 100})
        cli = FakeCli(tasks(["2021-01-01"] * 3))
        final_calls = []

        def update(frame):
            if len(frame.columns) == 6:
                final_calls.append(frame.copy())
                if len(final_calls) == 1:
                    cli.apply(frame.iloc[:1])
                raise RuntimeError("部分结果保存失败")
            cli.apply(frame)

        cli.update_hook = update
        calls = []
        with self.assertRaises(BatchPersistenceError) as caught:
            self.worker(cli, lambda request: calls.append(request) or success())
        self.assertEqual(len(calls), 3)
        self.assertEqual([len(frame) for frame in final_calls], [3, 2, 2])
        self.assertEqual(list(cli.frame["status"]), ["1", "9", "9"])
        self.assertEqual(len(caught.exception.unsaved_results), 3)
        self.assertEqual(len(caught.exception.pending_patch), 2)

    def test_shutdown_save_failure_logs_reason_and_preserves_original_stop(self):
        self.document["run"]["concurrency"] = 1
        cli = FakeCli(tasks(["2021-01-01", "2021-01-02"]))

        def update(frame):
            if len(frame.columns) == 6:
                raise RuntimeError("收尾存储不可用")
            cli.apply(frame)

        cli.update_hook = update
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(400, json={"error": "bad request"})

        with self.assertRaises(BatchPersistenceError) as caught:
            self.worker(cli, handler)
        self.assertIsInstance(caught.exception.__cause__, BatchRunError)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(caught.exception.pending_patch), 1)
        self.assertEqual(len(caught.exception.unsaved_results), 1)
        self.assertIn("异常收尾时结果保存未确认", self.last_output)
        self.assertIn("RuntimeError: 收尾存储不可用", self.last_output)
        self.assertIn("INFO:ai_batch.runner:worker 收尾结束", self.last_output)
        self.assertNotIn("全部处理完了", self.last_output)

    def test_interrupt_after_claim_keeps_9_without_request_then_recovers(self):
        cli = FakeCli(tasks(["2021-01-01", "2021-01-02"]))
        calls = []

        def update(frame):
            cli.apply(frame)
            raise KeyboardInterrupt()

        cli.update_hook = update
        with self.assertRaises(KeyboardInterrupt) as caught:
            self.worker(cli, lambda request: calls.append(request) or success())
        self.assertFalse(calls)
        self.assertTrue(cli.frame["status"].eq("9").all())
        self.assertEqual(list(caught.exception.pending_patch.columns), ["status", "config_hash"])
        self.assertIn("INFO:ai_batch.runner:worker 收尾结束", self.last_output)
        self.assertNotIn("Traceback", self.last_output)
        cli.update_hook = None
        self.assertEqual(self.worker(cli, lambda request: success()).new_succeeded, 2)

    def test_unexpected_program_error_is_batch_error_without_fake_ai_failure(self):
        cli = FakeCli(tasks(["2021-01-01"]))

        def handler(request):
            raise RuntimeError("unexpected code bug")

        with self.assertRaises(BatchRunError):
            self.worker(cli, handler)
        self.assertEqual(cli.frame.iloc[0]["status"], "9")
        self.assertNotIn("全部处理完了", self.last_output)
        self.assertIn("RuntimeError: unexpected code bug", self.last_output)

    def test_sync_entry_inside_existing_event_loop(self):
        cli = FakeCli(tasks(["2021-01-01"]))

        async def caller():
            return self.worker(cli, lambda request: success())

        self.assertEqual(asyncio.run(caller()).new_succeeded, 1)

    def test_chinese_logs_trace_retries_and_storage_without_sensitive_values(self):
        """跨线程记录重试、保存与完成，同时避免外部异常原文泄露输入。"""
        secrets = ["TEXT_SECRET", "PROMPT_SECRET", "API_KEY_SECRET", "URL_SECRET",
                   "RESPONSE_SECRET", "STORAGE_SECRET", "USER_SECRET"]
        frame = tasks(["2021-01-01"])
        frame["text"] = secrets[0]
        frame["user_name"] = secrets[-1]
        cli = FakeCli(frame)
        self.document["prompts"]["system"] = secrets[1]
        self.document["api"]["headers"] = {"Authorization": "Bearer " + secrets[2]}
        self.document["api"]["url"] += "?token=" + secrets[3]
        self.document["run"]["progress_every"] = 1
        calls = []
        final_updates = []

        def handler(request):
            calls.append(request)
            if len(calls) == 1:
                raise httpx.ReadTimeout(" ".join(secrets))
            return success(secrets[4])

        def update(frame):
            if len(frame.columns) == 6:
                final_updates.append(frame)
                if len(final_updates) == 1:
                    raise RuntimeError(secrets[5])
            cli.apply(frame)

        cli.update_hook = update
        summary = self.worker(cli, handler)
        self.assertEqual(summary.new_succeeded, 1)
        self.assertEqual(len(calls), 2)
        self.assertIn("网络传输故障", self.last_output)
        self.assertIn("读回核对", self.last_output)
        self.assertIn("已确认最终结果 1 条", self.last_output)
        self.assertIn("任务全部处理完了", self.last_output)
        for secret in secrets:
            self.assertNotIn(secret, self.last_output)
        self.assertEqual({record.name for record in self.log_records},
                         {"ai_batch.config", "ai_batch.response", "ai_batch.tasks", "ai_batch.runner"})
        self.assertTrue(any(record.threadName == "ai-batch-network" for record in self.log_records))
        for record in self.log_records:
            self.assertRegex(record.getMessage(), r"[\u4e00-\u9fff]")

    def test_stop_logs_http_status_without_external_failure_message(self):
        """停止日志使用可排查的状态，具体接口说明仍仅保存到任务表。"""
        cli = FakeCli(tasks(["2021-01-01"]))
        message = "PRIVATE_RESPONSE_SECRET"
        with self.assertRaises(BatchRunError):
            self.worker(cli, lambda request: httpx.Response(400, json={"error": {"message": message}}))
        self.assertTrue(any(record.levelname == "ERROR" and "HTTP状态=400" in record.getMessage()
                            for record in self.log_records))
        self.assertIn("收尾结束", self.last_output)
        self.assertNotIn("全部处理完了", self.last_output)
        self.assertNotIn(message, self.last_output)
        self.assertIn(message, cli.frame.iloc[0]["error_message"])
        report = next(record for record in self.log_records if "worker 收尾结束" in record.getMessage())
        self.assertEqual(report.levelname, "INFO")
        self.assertEqual(report.getMessage(), "worker 收尾结束")

    def test_buffer_190_plus_20_stays_bounded_during_update_retry(self):
        self.document["run"].update({"concurrency": 20, "flush_interval_seconds": 100})
        cli = FakeCli(tasks(["2021-01-01"] * 210))
        store = TaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = []
        while store.candidates:
            batch = store.select(20)
            store.claim(batch)
            keys.extend(batch)
        network = SimpleNamespace(completions=queue.Queue(20), unpublished=[])
        coordinator = _MonthCoordinator(store, network)
        response = parse_response(200, success().text)
        coordinator.buffer = [(key, response) for key in keys[:190]]
        coordinator.first_buffer_at = time.monotonic()
        coordinator.submitted = set(keys[190:])
        for key in keys[190:]:
            network.completions.put_nowait(CompletionMessage(key, response, 1))
        observed = []

        def update(frame):
            observed.append((len(frame), len(coordinator.buffer), network.completions.qsize()))
            if len(observed) == 1:
                raise RuntimeError("transient storage error")
            cli.apply(frame)

        cli.update_hook = update
        with contextlib.redirect_stdout(io.StringIO()):
            coordinator.finish()
        self.assertEqual(observed, [(200, 200, 10), (200, 200, 10), (10, 10, 0)])
        self.assertEqual(store.new_succeeded, 210)
        self.assertFalse(coordinator.buffer)

    def test_rps_and_global_server_pause(self):
        self.document["run"]["concurrency"] = 2
        self.document["run"]["requests_per_second"] = 50
        cli = FakeCli(tasks(["2021-01-01", "2021-01-02"]))
        starts = []
        count = {}

        def handler(request):
            text = request_text(request)
            starts.append(time.monotonic())
            count[text] = count.get(text, 0) + 1
            if text == "post 0" and count[text] == 1:
                return httpx.Response(503, headers={"Retry-After": "10"}, json={"error": "pause"})
            return success()

        with patch("ai_batch.runner._retry_after", return_value=0.04):
            summary = self.worker(cli, handler)
        self.assertEqual(summary.http_attempts_started, 3)
        self.assertGreaterEqual(starts[1] - starts[0], 0.035)
        self.assertGreaterEqual(starts[2] - starts[1], 0.018)

    def test_calendar_leap_year_cross_year_and_retry_after_parsing(self):
        start, end = date(2020, 2, 15), date(2020, 3, 10)
        self.assertEqual(list(_month_ranges(start, end)), [
            ("2020-02-15", "2020-02-29"), ("2020-03-01", "2020-03-10"),
        ])
        start, end = date(2020, 12, 15), date(2021, 2, 10)
        self.assertEqual(list(_month_ranges(start, end)), [
            ("2020-12-15", "2020-12-31"),
            ("2021-01-01", "2021-01-31"),
            ("2021-02-01", "2021-02-10"),
        ])
        self.assertEqual(_retry_after("10"), 10)
        self.assertEqual(_retry_after("0"), 0)
        for value in ("-1", "invalid", "NaN"):
            self.assertIsNone(_retry_after(value))
        deadline = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=5), usegmt=True)
        self.assertTrue(3.8 <= _retry_after(deadline) <= 5.1)

    def test_real_client_startup_and_close_without_http_request(self):
        network = _NetworkWorker(self.config())
        try:
            network.start()
        finally:
            network.shutdown()
        self.assertIsNone(network.failure)
        self.assertEqual(network.http_attempts_started, 0)
        self.assertTrue(network.client.is_closed)
        self.assertFalse(network.thread.is_alive())

    def test_client_startup_failure_does_not_mark_post_failed(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        self.write_config()
        with patch("ai_batch.runner._create_client", side_effect=RuntimeError("startup error")):
            with self.assertRaises(BatchRunError):
                run_worker(cli, self.task_path, self.path)
        self.assertTrue(cli.frame["status"].eq("0").all())
        self.assertFalse(cli.updates)
        self.assertFalse(any(thread.name == "ai-batch-network" for thread in threading.enumerate()))

    def test_client_close_failure_logs_original_cause_and_keeps_saved_result(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        original_exit = httpx.AsyncClient.__aexit__

        async def fail_after_close(client, *args):
            await original_exit(client, *args)
            raise RuntimeError("关闭客户端失败")

        with patch.object(httpx.AsyncClient, "__aexit__", new=fail_after_close):
            with self.assertRaises(BatchRunError):
                self.worker(cli, lambda request: success())
        self.assertEqual(cli.frame.iloc[0]["status"], "1")
        self.assertIn("BatchRunError: 网络资源关闭失败", self.last_output)
        self.assertIn("RuntimeError: 关闭客户端失败", self.last_output)
        self.assertNotIn("全部处理完了", self.last_output)

    def test_stop_during_claim_does_not_submit_newly_claimed_post(self):
        self.document["run"]["concurrency"] = 2
        cli = FakeCli(tasks(["2021-01-01"] * 4))
        claim_started = threading.Event()
        error_returned = threading.Event()
        claims = 0
        requested = []

        def update(frame):
            nonlocal claims
            if list(frame.columns) == ["status", "config_hash"]:
                claims += 1
                if claims == 2:
                    claim_started.set()
                    self.assertTrue(error_returned.wait(2))
                    time.sleep(0.02)
            cli.apply(frame)

        async def handler(request):
            text = request_text(request)
            requested.append(text)
            if text == "post 0":
                return success()
            if text == "post 1":
                deadline = time.monotonic() + 2
                while not claim_started.is_set() and time.monotonic() < deadline:
                    await asyncio.sleep(0.001)
                self.assertTrue(claim_started.is_set())
                error_returned.set()
                return httpx.Response(401, json={"error": "credentials expired"})
            return success("must not be requested")

        cli.update_hook = update
        with self.assertRaises(BatchRunError):
            self.worker(cli, handler)
        self.assertEqual(requested, ["post 0", "post 1"])
        self.assertEqual(list(cli.frame["status"]), ["1", "-1", "9", "0"])


if __name__ == "__main__":
    unittest.main()
