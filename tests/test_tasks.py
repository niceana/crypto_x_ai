import json
import unittest
from dataclasses import replace
from datetime import datetime
from unittest.mock import call, patch

import pandas as pd

from ai_batch.config import config_hash
from ai_batch.response import parse_response, request_failure
from ai_batch.tasks import (BatchPersistenceError, RESULT_COLUMNS, TASK_COLUMNS, TaskStore,
                            TaskValidationError, initialize_tasks)
from tests.support import FakeCli, Fixture, success, tasks


class TaskTests(Fixture, unittest.TestCase):
    def test_initialize_once_preserves_index_and_empty_identity(self):
        source = tasks(pd.date_range("2021-01-01 00:00:00.123456789", periods=3, tz="Asia/Shanghai"))
        source = source[["text", "user_name"]].iloc[[2, 0, 1]]
        original = source.copy(deep=True)
        writes = []
        with patch("ai_batch.tasks.config_hash", side_effect=AssertionError("early identity")):
            summary = initialize_tasks(source, writes.append)
        self.assertEqual(summary.total, 3)
        self.assertEqual(len(writes), 1)
        pd.testing.assert_index_equal(writes[0].index, source.index)
        pd.testing.assert_frame_equal(writes[0][["text", "user_name"]], source)
        pd.testing.assert_frame_equal(source, original)
        self.assertTrue(writes[0]["config_hash"].eq("").all())
        self.assertTrue(writes[0]["status"].eq("0").all())

    def test_initialize_empty_source_writes_complete_empty_table(self):
        source = tasks([])[["text", "user_name"]]
        writes = []
        summary = initialize_tasks(source, writes.append)
        self.assertEqual((summary.total, summary.written), (0, 0))
        self.assertEqual(len(writes), 1)
        self.assertEqual(list(writes[0].columns), TASK_COLUMNS)
        pd.testing.assert_index_equal(writes[0].index, source.index)

    def test_initialize_invalid_source_fails_before_write(self):
        source = tasks(["2021-01-01", "2021-01-02"])[["text", "user_name"]]
        duplicate_ids = source.copy()
        duplicate_ids.index = pd.MultiIndex.from_arrays(
            [source.index.get_level_values("date"), ["same", "same"]],
            names=["date", "id"],
        )
        invalid_text = source.copy()
        invalid_text["text"] = [42, "valid"]
        invalid_user_name = source.assign(user_name=[42, "valid"])
        index_error = "索引必须为名称和顺序固定的 date/id MultiIndex"
        for label, invalid, expected_reason in (
            ("non_dataframe", None, "输入必须为 DataFrame"),
            ("missing_index", source.reset_index(drop=True), index_error),
            ("date_index_with_id_column", source.reset_index(level="id"), index_error),
            ("reversed_levels", source.reorder_levels(["id", "date"]), index_error),
            ("wrong_level_name", source.rename_axis(["date", "post_id"]), index_error),
            ("missing_level_name", source.rename_axis(["date", None]), index_error),
            ("duplicate_ids", duplicate_ids, "同一 id 不允许对应多行或不同 date"),
            ("invalid_text", invalid_text, "text 的所有单元格必须为字符串"),
            ("missing_user_name", source.drop(columns="user_name"), "任务缺少必要列"),
            ("legacy_username", source.rename(columns={"user_name": "username"}), "任务缺少必要列"),
            ("invalid_user_name", invalid_user_name, "user_name 的所有单元格必须为字符串"),
            ("duplicate_columns", source[["text", "user_name", "user_name"]], "索引和列名必须唯一"),
        ):
            with self.subTest(source=label):
                writes = []
                with self.assertRaises(TaskValidationError) as captured:
                    initialize_tasks(invalid, writes.append)
                self.assertEqual(str(captured.exception), expected_reason)
                self.assertFalse(writes)

    def test_initialize_write_failure_is_not_retried(self):
        source = tasks(["2021-01-01"])[["text", "user_name"]]
        calls = []

        def fail(frame):
            calls.append(frame)
            raise RuntimeError("partial write")

        with self.assertRaises(RuntimeError):
            initialize_tasks(source, fail)
        self.assertEqual(len(calls), 1)

    def test_pending_float_results_do_not_block_startup(self):
        frame = tasks(pd.date_range("2021-01-01", periods=3))
        frame[RESULT_COLUMNS[1:]] = float("nan")
        cli = FakeCli(frame)
        store = TaskStore(self.config(), cli, self.task_path)
        store.recover()
        self.assertEqual(store.already_succeeded, 0)
        self.assertEqual(list(store.candidates), list(frame.index))
        pd.testing.assert_frame_equal(store.df, frame)
        self.assertFalse(cli.updates)

    def test_old_result_tolerance_keeps_input_and_success_checks(self):
        config = self.config()
        digest = config_hash(config)
        cases = [("0", name, None, name + " 的所有单元格必须为字符串")
                 for name in ("text", "user_name", "config_hash", "status")]
        cases += [
            ("1", "ai_response_content", None, "ai_response_content 的所有单元格必须为字符串"),
            ("1", "reasoning_content", pd.NA, "reasoning_content 的所有单元格必须为字符串"),
            ("1", "ai_response_content", "  ", "成功任务的摘要、正文或错误列不一致"),
            ("1", "config_hash", "", "成功任务的摘要、正文或错误列不一致"),
            ("1", "error_message", "HTTP 429", "成功任务的摘要、正文或错误列不一致"),
            ("1", "usage_json", "invalid JSON", "usage_json 必须为标准 JSON"),
        ]
        for status, column, value, reason in cases:
            with self.subTest(status=status, column=column, value=value):
                frame = tasks(["2021-01-01"])
                frame["status"] = status
                frame["config_hash"] = digest
                if status == "1":
                    frame["ai_response_content"] = "saved answer"
                else:
                    frame[RESULT_COLUMNS[1:]] = float("nan")
                frame.at[frame.index[0], column] = value
                cli = FakeCli(frame)
                with self.assertRaisesRegex(TaskValidationError, reason):
                    TaskStore(config, cli, self.task_path)
                self.assertFalse(cli.updates)

    def test_reset_claim_and_final_patch_columns(self):
        frame = tasks(pd.date_range("2021-01-01", periods=3))
        frame["status"] = ["9", "-1", "0"]
        frame["ai_response_content"] = "old"
        frame["error_message"] = "old error"
        original_inputs = frame[["text", "user_name"]].copy()
        cli = FakeCli(frame)
        store = TaskStore(self.config(), cli, self.task_path)
        with patch("ai_batch.tasks.config_hash", side_effect=AssertionError("early identity")):
            store.recover()
        self.assertEqual(list(cli.updates[0].columns), ["status"])
        pd.testing.assert_frame_equal(cli.frame.drop(columns="status"), frame.drop(columns="status"))
        keys = store.select(3)
        inputs = store.claim(keys)
        self.assertEqual(inputs[0], (keys[0], "post 0"))
        self.assertEqual(list(cli.updates[-1].columns), ["status", "config_hash"])
        pd.testing.assert_frame_equal(cli.frame[["text", "user_name"]], original_inputs)
        pd.testing.assert_frame_equal(cli.frame[RESULT_COLUMNS[1:]], frame[RESULT_COLUMNS[1:]])
        result = parse_response(200, success("new", usage={"total_tokens": 7}, created=100).text)
        store.save_results([(keys[0], result), (keys[1], request_failure("network unavailable"))])
        self.assertEqual(list(cli.updates[-1].columns), RESULT_COLUMNS)
        self.assertEqual(cli.frame.at[keys[0], "error_message"], "")
        self.assertEqual(json.loads(cli.frame.at[keys[0], "usage_json"]), {"total_tokens": 7})
        self.assertEqual(cli.frame.at[keys[0], "response_created"], "1970-01-01 08:01:40")
        self.assertEqual(cli.frame.at[keys[1], "ai_response_content"], "")
        self.assertEqual(store.new_succeeded, 1)
        self.assertEqual(store.new_failed, 1)
        self.assertEqual(store.active_keys, {keys[2]})
        pd.testing.assert_frame_equal(cli.frame[["text", "user_name"]], original_inputs)
        self.assertTrue(cli.frame["config_hash"].eq(config_hash(store.config)).all())

    def test_all_update_stages_sort_whole_rows_and_preserve_original_keys(self):
        timestamp = "2021-01-01 00:00:00.123456789"
        frame = tasks(pd.DatetimeIndex([
            "2021-01-02 00:00:00.123456789", timestamp, timestamp, timestamp,
        ], tz="Asia/Shanghai"))
        frame.index = pd.MultiIndex.from_arrays([
            frame.index.get_level_values("date"), ["3", "2", "10", "1"],
        ], names=["date", "id"])
        frame["status"] = ["9", "-1", "9", "0"]
        cli = FakeCli(frame)
        store = TaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = store.select(4)
        self.assertEqual(keys, list(frame.index))
        self.assertEqual(store.claim(keys), [(key, frame.at[key, "text"]) for key in keys])
        responses = {
            key: parse_response(200, success(
                "answer " + frame.at[key, "text"], usage={"total_tokens": i}, created=100 + i,
            ).text)
            for i, key in enumerate(keys)
        }
        store.save_results([(key, responses[key]) for key in reversed(keys)])
        self.assertEqual([list(update.columns) for update in cli.updates],
                         [["status"], ["status", "config_hash"], RESULT_COLUMNS])
        pd.testing.assert_index_equal(cli.updates[0].index,
                                      frame.loc[frame["status"].isin(["9", "-1"])].sort_index().index)
        for update in cli.updates[1:]:
            pd.testing.assert_index_equal(update.index, frame.sort_index().index)
        self.assertEqual(list(cli.updates[-1].index.get_level_values("id")), ["1", "10", "2", "3"])
        for i, key in enumerate(keys):
            self.assertEqual(cli.frame.at[key, "ai_response_content"], "answer " + frame.at[key, "text"])
            self.assertEqual(json.loads(cli.frame.at[key, "usage_json"]), {"total_tokens": i})
            self.assertEqual(cli.frame.at[key, "response_created"], responses[key].response_created)
        pd.testing.assert_index_equal(store.df.index, frame.index)
        pd.testing.assert_frame_equal(cli.frame[["text", "user_name"]], frame[["text", "user_name"]])
        self.assertEqual(store.new_succeeded, 4)
        self.assertFalse(store.active_keys)

    def test_duplicate_result_keys_fail_before_client_update(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        store = TaskStore(self.config(), cli, self.task_path)
        store.recover()
        key = store.select(1)[0]
        store.claim([key])
        before = cli.frame.copy(deep=True)
        update_count = len(cli.updates)
        first = parse_response(200, success("first answer").text)
        second = parse_response(200, success("second answer").text)
        with self.assertRaisesRegex(TaskValidationError, "同一更新批次的.*不允许重复"):
            store.save_results([(key, first), (key, second)])
        self.assertEqual(len(cli.updates), update_count)
        self.assertIsNone(store.pending_patch)
        self.assertEqual(store.new_succeeded, 0)
        self.assertEqual(store.active_keys, {key})
        pd.testing.assert_frame_equal(cli.frame, before)

    def test_unordered_results_remain_aligned_and_sorted_after_partial_save(self):
        frame = tasks(["2021-01-01"] * 4)
        cli = FakeCli(frame)
        store = TaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = store.select(4)
        store.claim(keys)
        final_calls = []

        def update(patch_frame):
            final_calls.append(patch_frame.copy(deep=True))
            if len(final_calls) == 1:
                # 只写入排序后的第二行，重试还须保留其他三行的顺序和答复。
                cli.apply(patch_frame.iloc[[1]])
                raise RuntimeError("partial update")
            cli.apply(patch_frame)

        cli.update_hook = update
        store.save_results([
            (keys[i], parse_response(200, success("answer " + frame.at[keys[i], "text"]).text))
            for i in [2, 0, 3, 1]
        ])
        self.assertEqual([list(update.index) for update in final_calls],
                         [keys, [keys[0], keys[2], keys[3]]])
        for key in keys:
            self.assertEqual(cli.frame.at[key, "ai_response_content"], "answer " + frame.at[key, "text"])
        self.assertEqual(store.new_succeeded, 4)
        self.assertFalse(store.active_keys)
        self.assertIsNone(store.pending_patch)

    def test_invalid_created_clears_old_results_and_leaves_completed_history_unchanged(self):
        config = self.config()
        frame = tasks(pd.date_range("2021-01-01", periods=2))
        frame["config_hash"] = config_hash(config)
        frame["status"] = ["-1", "1"]
        frame["ai_response_content"] = "old answer"
        frame["reasoning_content"] = "old reasoning"
        frame["usage_json"] = '{"total_tokens": 99}'
        frame["response_created"] = "1791947560"
        frame["error_message"] = ["old error", ""]
        historical_success = frame.iloc[[1]].copy(deep=True)
        cli = FakeCli(frame)
        store = TaskStore(config, cli, self.task_path)
        store.recover()
        keys = store.select(2)
        self.assertEqual(keys, [frame.index[0]])
        store.claim(keys)
        result = parse_response(200, success("new answer", usage={"total_tokens": 7}, created="invalid").text)
        store.save_results([(keys[0], result)])
        saved = cli.frame.loc[keys[0]]
        self.assertEqual(saved["status"], "-1")
        for column in ("ai_response_content", "reasoning_content", "usage_json", "response_created"):
            self.assertEqual(saved[column], "")
        self.assertTrue(saved["error_message"].startswith("response_created 无法转换："))
        self.assertEqual(store.new_failed, 1)
        pd.testing.assert_frame_equal(cli.frame.iloc[[1]], historical_success)
        pd.testing.assert_frame_equal(cli.frame[["config_hash", "text", "user_name"]],
                                      frame[["config_hash", "text", "user_name"]])

    def test_final_type_error_names_field_and_type_before_any_batch_write(self):
        cases = [
            ("response_created", 1791947560),
            ("response_created", None),
            ("response_created", datetime(2026, 10, 14, 11, 12, 40)),
            ("ai_response_content", None),
            ("reasoning_content", {}),
            ("error_message", None),
        ]
        for column, value in cases:
            with self.subTest(column=column, actual_type=type(value).__name__):
                cli = FakeCli(tasks(pd.date_range("2021-01-01", periods=2)))
                store = TaskStore(self.config(), cli, self.task_path)
                store.recover()
                keys = store.select(2)
                store.claim(keys)
                before = cli.frame.copy(deep=True)
                update_count = len(cli.updates)
                valid = parse_response(200, success(created=1791947560).text)
                invalid = replace(request_failure("network unavailable"), **{column: value})
                with self.assertRaises(TaskValidationError) as captured:
                    store.save_results([(keys[0], valid), (keys[1], invalid)])
                self.assertEqual(str(captured.exception),
                                 "最终更新字段必须为字符串：字段={}，实际类型={}".format(
                                     column, type(value).__name__))
                self.assertEqual(len(cli.updates), update_count)
                pd.testing.assert_frame_equal(cli.frame, before)

    def test_partial_save_only_rewrites_unconfirmed_keys(self):
        cli = FakeCli(tasks(["2021-01-01", "2021-01-02"]))
        store = TaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = store.select(2)
        store.claim(keys)
        final_calls = []

        def update(frame):
            final_calls.append(frame.copy())
            if len(final_calls) == 1:
                cli.apply(frame.iloc[:1])
                frame["ai_response_content"] = "client mutation"
                raise RuntimeError("partial update")
            cli.apply(frame)

        cli.update_hook = update
        result = parse_response(200, success().text)
        store.save_results([(key, result) for key in keys])
        self.assertEqual([len(frame) for frame in final_calls], [2, 1])
        self.assertEqual(list(final_calls[1].index), [keys[1]])
        self.assertEqual(store.new_succeeded, 2)
        self.assertIsNone(store.pending_patch)
        self.assertFalse(store.active_keys)

    def test_partial_save_with_empty_old_results_only_rewrites_unconfirmed_keys(self):
        frame = tasks(pd.date_range("2021-01-01", periods=4))
        for column in RESULT_COLUMNS[1:]:
            frame[column] = [None, float("nan"), pd.NA, None]
        untouched = frame.iloc[3:].copy()
        cli = FakeCli(frame)
        store = TaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = store.select(3)
        store.claim(keys)
        final_calls = []

        def update(patch_frame):
            final_calls.append(patch_frame.copy(deep=True))
            if len(final_calls) == 1:
                cli.apply(patch_frame.iloc[:1])
                # 未写入的行仍有 NaN/pd.NA，应继续重试，不拒绝本月读回。
                raise RuntimeError("partial update")
            cli.apply(patch_frame)

        cli.update_hook = update
        result = parse_response(200, success("new answer", usage={"total_tokens": 7}).text)
        store.save_results([(key, result) for key in keys])
        self.assertEqual([len(patch_frame) for patch_frame in final_calls], [3, 2])
        self.assertEqual(list(final_calls[1].index), keys[1:])
        self.assertEqual(store.new_succeeded, 3)
        self.assertIsNone(store.pending_patch)
        self.assertTrue(cli.frame.loc[keys, "status"].eq("1").all())
        self.assertTrue(cli.frame.loc[keys, "config_hash"].eq(config_hash(store.config)).all())
        pd.testing.assert_frame_equal(cli.frame[["text", "user_name"]], frame[["text", "user_name"]])
        pd.testing.assert_frame_equal(cli.frame.iloc[3:], untouched)

    def test_thrown_update_is_confirmed_by_readback_including_new_hash(self):
        self.document["run"]["storage_max_attempts"] = 1
        for start, end in (
            (None, None),
            ("2021-01-01", "2021-01-31"),
        ):
            with self.subTest(start=start, end=end):
                cli = FakeCli(tasks(["2021-01-31 23:59:59.999999999"]))

                def update(frame):
                    cli.apply(frame)
                    raise RuntimeError("connection lost after commit")

                with patch.object(cli, "read", wraps=cli.read) as read:
                    store = TaskStore(self.config(), cli, self.task_path, start, end)
                    store.recover()
                    cli.update_hook = update
                    key = store.select(1)[0]
                    store.claim([key])
                    store.save_results([(key, parse_response(200, success().text))])
                self.assertEqual(len(cli.updates), 2)
                self.assertEqual(store.new_succeeded, 1)
                expected_read = (
                    call(self.task_path) if start is None
                    else call(self.task_path, start_time=start, end_time=end)
                )
                self.assertEqual((store.start_time, store.end_time), (start, end))
                self.assertEqual(read.call_args_list, [expected_read] * 3)
                self.assertEqual(cli.update_paths, [self.task_path] * 2)

    def test_day_read_preserves_index_precision_order_and_calendar_timezone(self):
        for day, next_day in (("2021-03-14", "2021-03-15"), ("2021-11-07", "2021-11-08")):
            with self.subTest(day=day):
                frame = tasks(pd.DatetimeIndex([
                    day + " 00:00", day + " 08:30", day + " 09:00:00.000000001",
                    day + " 10:30", day + " 22:00", day + " 23:59:59.999999999", next_day + " 00:00",
                ], tz="America/New_York")).iloc[[4, 2, 6, 0, 3, 5, 1]]
                cli = FakeCli(frame)
                store = TaskStore(self.config(), cli, self.task_path, day, day)
                self.assertIs(store.start_time, day)
                self.assertIs(store.end_time, day)
                pd.testing.assert_frame_equal(store.df, frame.iloc[[0, 1, 3, 4, 5, 6]])
                self.assertEqual(cli.reads, [(day, day)])
                pd.testing.assert_frame_equal(cli.frame, frame)

    def test_segment_rejects_incomparable_date_type_before_updates(self):
        frame = tasks(["2021-01-01"])
        frame.index = pd.MultiIndex.from_arrays([[1], ["0"]], names=["date", "id"])
        cli = FakeCli(frame)
        with patch.object(cli, "read", return_value=frame), self.assertRaisesRegex(
                TaskValidationError, "日期范围的类型或时区不兼容"):
            TaskStore(self.config(), cli, self.task_path, "2021-01-01", "2021-01-31")
        self.assertFalse(cli.updates)

    def test_readback_outside_segment_retains_patch_without_rewriting(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        store = TaskStore(self.config(), cli, self.task_path, "2021-01-01", "2021-01-31")
        store.recover()
        key = store.select(1)[0]

        def fail(frame):
            raise RuntimeError("update failed")

        cli.update_hook = fail
        with patch.object(cli, "read", return_value=tasks(["2021-01-01", "2021-02-01"])) as read:
            with self.assertRaises(BatchPersistenceError) as caught:
                store.claim([key])
        self.assertIsInstance(caught.exception.__cause__, TaskValidationError)
        self.assertEqual(caught.exception.stage, "claim")
        self.assertEqual(list(caught.exception.pending_patch.index), [key])
        self.assertEqual(len(cli.updates), 1)
        read.assert_called_once_with(self.task_path, start_time="2021-01-01", end_time="2021-01-31")

    def test_readback_input_conflict_stops_without_rewriting(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        store = TaskStore(self.config(), cli, self.task_path)
        store.recover()
        key = store.select(1)[0]

        def fail(frame):
            cli.frame.at[key, "user_name"] = "changed externally"
            raise RuntimeError("update failed")

        cli.update_hook = fail
        with self.assertRaises(BatchPersistenceError) as caught:
            store.claim([key])
        self.assertEqual(caught.exception.stage, "claim")
        self.assertEqual(len(cli.updates), 1)
        self.assertIsNotNone(store.pending_patch)

    def test_identity_conflict_before_request_and_duplicate_ids_rejected(self):
        frame = tasks(["2021-01-01", "2021-01-02"])
        frame.at[frame.index[0], "config_hash"] = "f" * 64
        cli = FakeCli(frame)
        store = TaskStore(self.config(), cli, self.task_path)
        store.recover()
        with self.assertRaises(TaskValidationError):
            store.claim(store.select(1))
        self.assertFalse(cli.updates)
        frame.index = pd.MultiIndex.from_arrays([frame.index.get_level_values("date"), ["same", "same"]], names=["date", "id"])
        with self.assertRaises(TaskValidationError):
            TaskStore(self.config(), FakeCli(frame), self.task_path)

    def test_succeeded_month_never_calculates_new_hash(self):
        frame = tasks(["2021-01-01"])
        frame["config_hash"] = config_hash(self.config())
        frame["status"] = "1"
        frame["ai_response_content"] = "old success"
        cli = FakeCli(frame)
        with patch("ai_batch.tasks.config_hash", side_effect=AssertionError("early identity")):
            store = TaskStore(self.config(), cli, self.task_path)
            store.recover()
        self.assertFalse(store.candidates)
        self.assertFalse(cli.updates)

    def test_readback_timezone_change_is_rejected_even_for_equal_instants(self):
        cli = FakeCli(tasks(pd.date_range("2021-01-01", periods=1, tz="Asia/Shanghai")))
        store = TaskStore(self.config(), cli, self.task_path)
        store.recover()
        key = store.select(1)[0]

        def fail(frame):
            cli.frame.index = pd.MultiIndex.from_arrays([
                cli.frame.index.get_level_values("date").tz_convert("UTC"),
                cli.frame.index.get_level_values("id")], names=["date", "id"])
            raise RuntimeError("uncertain commit")

        cli.update_hook = fail
        with self.assertRaises(BatchPersistenceError):
            store.claim([key])
        self.assertEqual(len(cli.updates), 1)


if __name__ == "__main__":
    unittest.main()
