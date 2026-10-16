import json
import unittest
from dataclasses import replace
from datetime import datetime
from unittest.mock import patch

import pandas as pd

from common.config import ai_config_hash
from common.persistence import BatchPersistenceError, TaskValidationError
from common.ai_protocol import parse_response, request_failure
from common.store import PostTaskStore
from common.validation import POST_RESULT_COLUMNS as RESULT_COLUMNS, TASK_COLUMNS
from initialization.post_tasks import initialize_tasks
from tests.support import FakeCli, Fixture, success, tasks


class TaskTests(Fixture, unittest.TestCase):
    def test_initialize_once_preserves_index_and_empty_identity(self):
        source = tasks(pd.date_range("2021-01-01 00:00:00.123456789", periods=3, tz="Asia/Shanghai"))
        source = source[["text", "user_name"]].iloc[[2, 0, 1]]
        original = source.copy(deep=True)
        writes = []
        with patch("common.store.ai_config_hash", side_effect=AssertionError("early identity")):
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
            raise RuntimeError("write not applied")

        with self.assertRaises(RuntimeError):
            initialize_tasks(source, fail)
        self.assertEqual(len(calls), 1)

    def test_pending_float_results_do_not_block_startup(self):
        frame = tasks(pd.date_range("2021-01-01", periods=3))
        frame[RESULT_COLUMNS[1:]] = float("nan")
        cli = FakeCli(frame)
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        self.assertEqual(store.already_completed, 0)
        self.assertEqual(list(store.candidates), list(frame.index))
        pd.testing.assert_frame_equal(store.df, frame)
        self.assertFalse(cli.updates)

    def test_read_cells_are_preserved_without_content_validation(self):
        cases = [("text", None), ("user_name", pd.NA), ("config_hash", None),
                 ("status", None), ("ai_response_content", None),
                 ("reasoning_content", float("nan")), ("ai_response_content", "  "),
                 ("config_hash", ""), ("error_message", "HTTP 429"),
                 ("usage_json", "invalid JSON")]
        for status in ("0", "1"):
            for column, value in cases:
                with self.subTest(status=status, column=column):
                    frame = tasks(["2021-01-01"])
                    frame["status"] = status
                    frame.at[frame.index[0], column] = value
                    cli = FakeCli(frame)
                    store = PostTaskStore(self.config(), cli, self.task_path)
                    pd.testing.assert_frame_equal(store.df, frame)
                    self.assertFalse(cli.updates or self.hash_reads)

    def test_reset_claim_and_final_patch_columns(self):
        frame = tasks(pd.date_range("2021-01-01", periods=3))
        frame["status"] = ["9", "-1", "0"]
        frame["ai_response_content"] = "old"
        frame["error_message"] = "old error"
        original_inputs = frame[["text", "user_name"]].copy()
        cli = FakeCli(frame)
        store = PostTaskStore(self.config(), cli, self.task_path)
        with patch("common.store.ai_config_hash", side_effect=AssertionError("early identity")):
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
        self.assertEqual(self.counts(store)["new_completed"], 1)
        self.assertEqual(self.counts(store)["failed"], 1)
        self.assertEqual(set(store.df.index[store.df["status"].eq("9")]), {keys[2]})
        pd.testing.assert_frame_equal(cli.frame[["text", "user_name"]], original_inputs)
        self.assertTrue(cli.frame["config_hash"].eq(ai_config_hash(store.config)).all())

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
        store = PostTaskStore(self.config(), cli, self.task_path)
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
        self.assertEqual(self.counts(store)["new_completed"], 4)
        self.assertFalse(store.df["status"].eq("9").any())

    def test_duplicate_result_keys_fail_before_client_update(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        store = PostTaskStore(self.config(), cli, self.task_path)
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
        self.assertEqual(self.counts(store)["new_completed"], 0)
        self.assertEqual(set(store.df.index[store.df["status"].eq("9")]), {key})
        pd.testing.assert_frame_equal(cli.frame, before)

    def test_unordered_results_remain_aligned_and_sorted_on_batch_retry(self):
        frame = tasks(["2021-01-01"] * 4)
        cli = FakeCli(frame)
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = store.select(4)
        store.claim(keys)
        final_calls = []
        def update(patch_frame):
            final_calls.append(patch_frame.copy(deep=True))
            if len(final_calls) == 1:
                raise RuntimeError("batch not written")
            cli.apply(patch_frame)
        cli.update_hook = update
        store.save_results([(keys[i], parse_response(200, success("answer " + frame.at[keys[i], "text"]).text))
                            for i in [2, 0, 3, 1]])
        self.assertEqual([list(update.index) for update in final_calls], [keys, keys])
        pd.testing.assert_frame_equal(final_calls[0], final_calls[1])
        for key in keys:
            self.assertEqual(cli.frame.at[key, "ai_response_content"], "answer " + frame.at[key, "text"])
        self.assertEqual(self.counts(store)["new_completed"], 4)
        self.assertFalse(store.df["status"].eq("9").any())
        self.assertEqual(len(cli.reads), 1)

    def test_invalid_created_clears_old_results_and_leaves_completed_history_unchanged(self):
        config = self.config()
        frame = tasks(pd.date_range("2021-01-01", periods=2))
        frame["config_hash"] = ai_config_hash(config)
        frame["status"] = ["-1", "1"]
        frame["ai_response_content"] = "old answer"
        frame["reasoning_content"] = "old reasoning"
        frame["usage_json"] = '{"total_tokens": 99}'
        frame["response_created"] = "1791947560"
        frame["error_message"] = ["old error", ""]
        historical_success = frame.iloc[[1]].copy(deep=True)
        cli = FakeCli(frame)
        store = PostTaskStore(config, cli, self.task_path)
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
        self.assertEqual(self.counts(store)["failed"], 1)
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
                store = PostTaskStore(self.config(), cli, self.task_path)
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

    def test_batch_retry_uses_unchanged_snapshot_and_succeeds_on_last_attempt(self):
        cli = FakeCli(tasks(["2021-01-01", "2021-01-02"]))
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = store.select(2)
        store.claim(keys)
        before = cli.frame.copy(deep=True)
        final_calls = []
        def update(frame):
            final_calls.append(frame.copy(deep=True))
            pd.testing.assert_frame_equal(store.df, before)
            pd.testing.assert_frame_equal(cli.frame, before)
            self.assertEqual((self.counts(store)["new_completed"], self.counts(store)["failed"]), (0, 0))
            if len(final_calls) < 3:
                frame["ai_response_content"] = "client mutation"
                raise RuntimeError("batch not written")
            cli.apply(frame)
        cli.update_hook = update
        result = parse_response(200, success().text)
        store.save_results([(key, result) for key in keys])
        self.assertEqual(len(final_calls), 3)
        for frame in final_calls[1:]:
            pd.testing.assert_frame_equal(frame, final_calls[0])
        self.assertEqual(self.counts(store)["new_completed"], 2)
        self.assertFalse(store.df["status"].eq("9").any())
        self.assertEqual(len(cli.reads), 1)

    def test_repeated_final_save_does_not_count_success_or_failure_twice(self):
        cli = FakeCli(tasks(["2021-01-01", "2021-01-02"]))
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = store.select(2)
        store.claim(keys)
        self.assertEqual((self.counts(store)["new_completed"], self.counts(store)["failed"]), (0, 0))
        results = [(keys[0], parse_response(200, success("saved").text)),
                   (keys[1], request_failure("failed"))]
        store.save_results(results)
        self.assertEqual((self.counts(store)["new_completed"], self.counts(store)["failed"]), (1, 1))
        store.save_results(results)
        self.assertEqual((self.counts(store)["new_completed"], self.counts(store)["failed"]), (1, 1))
        self.assertEqual(list(store.df["status"]), ["1", "-1"])
        self.assertEqual(store.select(2), [])

    def test_mixed_results_are_counted_only_after_whole_batch_is_saved(self):
        cli = FakeCli(tasks(["2021-01-01"] * 3))
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = store.select(3)
        store.claim(keys)
        observed = []
        def update(frame):
            observed.append((list(frame.index), self.counts(store)["new_completed"], self.counts(store)["failed"],
                             list(store.df.loc[keys, "status"])))
            if len(observed) == 1:
                raise RuntimeError("batch not written")
            cli.apply(frame)
        cli.update_hook = update
        result = parse_response(200, success("saved").text)
        store.save_results([(keys[0], result), (keys[1], request_failure("failed")), (keys[2], result)])
        self.assertEqual(observed, [(keys, 0, 0, ["9", "9", "9"])] * 2)
        self.assertEqual((self.counts(store)["new_completed"], self.counts(store)["failed"]), (2, 1))
        self.assertEqual(list(store.df.loc[keys, "status"]), ["1", "-1", "1"])

    def test_batch_retry_tolerates_empty_old_results_and_preserves_untouched_rows(self):
        frame = tasks(pd.date_range("2021-01-01", periods=4))
        for column in RESULT_COLUMNS[1:]:
            frame[column] = [None, float("nan"), pd.NA, None]
        untouched = frame.iloc[3:].copy()
        cli = FakeCli(frame)
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = store.select(3)
        store.claim(keys)
        final_calls = []
        def update(patch_frame):
            final_calls.append(patch_frame.copy(deep=True))
            if len(final_calls) == 1:
                raise RuntimeError("batch not written")
            cli.apply(patch_frame)
        cli.update_hook = update
        result = parse_response(200, success("new answer", usage={"total_tokens": 7}).text)
        store.save_results([(key, result) for key in keys])
        self.assertEqual([len(patch_frame) for patch_frame in final_calls], [3, 3])
        pd.testing.assert_frame_equal(final_calls[0], final_calls[1])
        self.assertEqual(self.counts(store)["new_completed"], 3)
        pd.testing.assert_frame_equal(cli.frame.iloc[3:], untouched)
        pd.testing.assert_frame_equal(cli.frame[["text", "user_name"]], frame[["text", "user_name"]])
        self.assertEqual(len(cli.reads), 1)

    def test_claim_failure_preserves_state_and_original_error_without_readback(self):
        for start, end in ((None, None), ("2021-01-01", "2021-01-31")):
            with self.subTest(start=start, end=end):
                cli = FakeCli(tasks(["2021-01-31 23:59:59.999999999"]))
                store = PostTaskStore(self.config(), cli, self.task_path, start, end)
                store.recover()
                before = cli.frame.copy(deep=True)
                error = RuntimeError("not written")
                def update(frame):
                    raise error
                cli.update_hook = update
                with self.assertRaises(BatchPersistenceError) as caught:
                    store.claim(store.select(1))
                self.assertIs(caught.exception.__cause__, error)
                self.assertEqual(vars(caught.exception), {"stage": "claim"})
                self.assertEqual(len(cli.updates), 3)
                self.assertEqual(cli.reads, [(start, end)])
                pd.testing.assert_frame_equal(store.df, before)
                pd.testing.assert_frame_equal(cli.frame, before)

    def test_day_read_preserves_index_precision_order_and_calendar_timezone(self):
        for day, next_day in (("2021-03-14", "2021-03-15"), ("2021-11-07", "2021-11-08")):
            with self.subTest(day=day):
                frame = tasks(pd.DatetimeIndex([
                    day + " 00:00", day + " 08:30", day + " 09:00:00.000000001",
                    day + " 10:30", day + " 22:00", day + " 23:59:59.999999999", next_day + " 00:00",
                ], tz="America/New_York")).iloc[[4, 2, 6, 0, 3, 5, 1]]
                cli = FakeCli(frame)
                store = PostTaskStore(self.config(), cli, self.task_path, day, day)
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
            PostTaskStore(self.config(), cli, self.task_path, "2021-01-01", "2021-01-31")
        self.assertFalse(cli.updates)

    def test_storage_waits_only_between_attempts(self):
        for budget in (1, 3):
            with self.subTest(budget=budget):
                self.document["run"]["storage_max_attempts"] = budget
                cli = FakeCli(tasks(["2021-01-01"]))
                store = PostTaskStore(self.config(), cli, self.task_path)
                store.recover()
                def update(frame):
                    raise RuntimeError("offline")
                cli.update_hook = update
                with patch("common.persistence.time.sleep") as sleep:
                    with self.assertRaises(BatchPersistenceError):
                        store.claim(store.select(1))
                self.assertEqual(len(cli.updates), budget)
                self.assertEqual(sleep.call_count, budget - 1)
                self.assertEqual(len(cli.reads), 1)
                for args in sleep.call_args_list:
                    self.assertEqual(args.args, (store.config.run.storage_retry_seconds,))

    def test_memory_sync_error_does_not_retry_successful_write(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = store.select(1)
        store.claim(keys)
        previous_count = len(cli.updates)
        error = RuntimeError("memory sync failed")
        with patch.object(store, "_apply_saved_patch", side_effect=error):
            with self.assertRaisesRegex(BatchPersistenceError, "CLI 已写入，但内存同步失败") as caught:
                store.save_results([(keys[0], parse_response(200, success().text))])
        self.assertIs(caught.exception.__cause__, error)
        self.assertEqual(len(cli.updates), previous_count + 1)
        self.assertEqual(cli.frame.iloc[0]["status"], "1")
        self.assertEqual(len(cli.reads), 1)

    def test_first_round_config_digest_is_checked_only_on_first_claim(self):
        cli = FakeCli(tasks(["2021-01-01", "2021-01-02"]))
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        with patch("common.store.ai_config_hash", wraps=ai_config_hash) as digest:
            for _ in range(2):
                store.claim(store.select(1))
        self.assertEqual(digest.call_count, 1)
        self.assertTrue(cli.frame["config_hash"].eq(ai_config_hash(store.config)).all())

    def test_identity_conflict_before_request_and_duplicate_ids_rejected(self):
        frame = tasks(["2021-01-01", "2021-01-02"])
        frame.at[frame.index[0], "config_hash"] = "f" * 64
        cli = FakeCli(frame)
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        with self.assertRaises(TaskValidationError):
            store.claim(store.select(1))
        self.assertFalse(cli.updates)
        frame.index = pd.MultiIndex.from_arrays([frame.index.get_level_values("date"), ["same", "same"]], names=["date", "id"])
        with self.assertRaises(TaskValidationError):
            PostTaskStore(self.config(), FakeCli(frame), self.task_path)

    def test_succeeded_month_never_calculates_new_hash(self):
        frame = tasks(["2021-01-01"])
        frame["config_hash"] = ai_config_hash(self.config())
        frame["status"] = "1"
        frame["ai_response_content"] = "old success"
        cli = FakeCli(frame)
        with patch("common.store.ai_config_hash", side_effect=AssertionError("early identity")):
            store = PostTaskStore(self.config(), cli, self.task_path)
            store.recover()
        self.assertFalse(store.candidates)
        self.assertFalse(cli.updates)

    def test_interrupt_inside_claim_does_not_write_or_retry(self):
        cli = FakeCli(tasks(pd.date_range("2021-01-01", periods=1, tz="Asia/Shanghai")))
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        before = cli.frame.copy(deep=True)
        def interrupted(frame):
            raise KeyboardInterrupt()
        cli.update_hook = interrupted
        with self.assertRaises(KeyboardInterrupt):
            store.claim(store.select(1))
        self.assertEqual(len(cli.updates), 1)
        self.assertEqual(len(cli.reads), 1)
        pd.testing.assert_frame_equal(cli.frame, before)
        pd.testing.assert_frame_equal(store.df, before)


if __name__ == "__main__":
    unittest.main()
