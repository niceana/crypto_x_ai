import json
import unittest
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

    def test_reset_claim_and_final_patch_columns(self):
        frame = tasks(pd.date_range("2021-01-01", periods=3))
        frame["status"] = ["9", "-1", "0"]
        frame["ai_response_content"] = "old"
        frame["error_message"] = "old error"
        original_user_names = frame["user_name"].copy()
        cli = FakeCli(frame)
        store = TaskStore(self.config(), cli, self.task_path)
        with patch("ai_batch.tasks.config_hash", side_effect=AssertionError("early identity")):
            store.recover()
        self.assertEqual(list(cli.updates[0].columns), ["status"])
        self.assertTrue(cli.frame["ai_response_content"].eq("old").all())
        keys = store.select(3)
        inputs = store.claim(keys)
        self.assertEqual(inputs[0], (keys[0], "post 0"))
        self.assertEqual(list(cli.updates[-1].columns), ["status", "config_hash"])
        result = parse_response(200, success("new", usage={"total_tokens": 7}, created=100).text)
        store.save_results([(keys[0], result), (keys[1], request_failure("network unavailable"))])
        self.assertEqual(list(cli.updates[-1].columns), RESULT_COLUMNS)
        self.assertEqual(cli.frame.at[keys[0], "error_message"], "")
        self.assertEqual(json.loads(cli.frame.at[keys[0], "usage_json"]), {"total_tokens": 7})
        self.assertEqual(cli.frame.at[keys[0], "response_created"], "100")
        self.assertEqual(cli.frame.at[keys[1], "ai_response_content"], "")
        self.assertEqual(store.new_succeeded, 1)
        self.assertEqual(store.new_failed, 1)
        self.assertEqual(store.active_keys, {keys[2]})
        pd.testing.assert_series_equal(cli.frame["user_name"], original_user_names)

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
