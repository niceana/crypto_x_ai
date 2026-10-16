"""验证研究流程的历史边界、任务状态、重试与续跑；不访问真实服务。"""

import json
import unittest
from unittest.mock import patch

import pandas as pd

from common.config import ai_config_hash
from common.persistence import BatchPersistenceError
from initialization.event_records import (
    import_event_records_from_posts,
    import_event_records_from_table,
    expand_first_round,
)
from common.validation import HASH_COLUMNS
from tests.event_support import EventFixture, source_posts, DataNotFound, Tables, impact_result


class EventImportTests(EventFixture, unittest.TestCase):
    def test_memory_import_appends_new_posts_without_reading_source_or_existing_rows(self):
        source = source_posts([("2021-01-01", "old", ["ETH"], "a"),
                               ("2021-01-02", "new", ["ETH"], "a")])
        backend = Tables(source, expand_first_round(source.iloc[:1], {}))
        self.relate(backend)
        saved = backend.tables["result"].copy(deep=True)
        backend.reads.clear()
        new_posts = source.iloc[1:].copy(deep=True)
        before = new_posts.copy(deep=True)
        self.assertEqual(import_event_records_from_posts(backend, self.settings(), new_posts=new_posts), 1)
        self.assertEqual(backend.reads, [])
        self.assertEqual(backend.meta_reads, ["result"])
        pd.testing.assert_frame_equal(backend.tables["result"].loc[saved.index], saved)
        pd.testing.assert_frame_equal(new_posts, before)
        self.assertEqual((len(backend.appends), len(backend.writes), len(backend.registers)), (1, 0, 0))

    def test_memory_recovery_only_checks_existing_indices_and_preserves_business_fields(self):
        source = source_posts([("2021-01-01", "old", ["ETH"], "a"),
                               ("2021-01-02", "new", ["ETH"], "a")])
        existing = expand_first_round(source.iloc[:1], {})
        existing.loc[:, "event"] = "保留已有业务字段，不重新比较输入"
        backend = Tables(source, existing)
        self.assertEqual(import_event_records_from_posts(backend, self.settings(),
                                                   new_posts=source.iloc[1:], previous_posts=source.iloc[:1]), 1)
        self.assertEqual(backend.reads, [("result", None, None)])
        self.assertEqual(backend.meta_reads, [])
        pd.testing.assert_frame_equal(backend.tables["result"].loc[existing.index], existing)
        self.assertEqual(import_event_records_from_posts(backend, self.settings(), previous_posts=source), 0)
        self.assertEqual(len(backend.appends), 1)

    def test_memory_recovery_checks_record_ids_across_dates_and_index_types(self):
        source = source_posts([("2021-01-02", "ready", ["ETH"], "a")])
        expected = expand_first_round(source, {})
        for kind in ("duplicate_id", "different_date_type"):
            with self.subTest(kind=kind):
                existing = expected.copy(deep=True)
                if kind == "duplicate_id":
                    existing.index = pd.MultiIndex.from_tuples([(pd.Timestamp("2021-01-01"), expected.index[0][1])],
                                                              names=expected.index.names)
                else:
                    existing.index = pd.MultiIndex.from_tuples([("2021-01-02 00:00:00", "different-id")],
                                                              names=expected.index.names)
                backend = Tables(source, existing)
                with self.assertRaisesRegex(ValueError, "其他日期|日期索引类型"):
                    import_event_records_from_posts(backend, self.settings(), previous_posts=source)
                self.assertFalse(backend.appends or backend.writes or backend.registers)

    def test_memory_import_metadata_errors_do_not_read_rows_or_create_table(self):
        source = source_posts([("2021-01-01", "ready", ["ETH"], "a")])
        for error in (ConnectionError("offline"), PermissionError("forbidden"), RuntimeError("DataNotFoundUnexpected")):
            with self.subTest(error=type(error).__name__):
                backend = Tables(source)
                def read_meta(path):
                    raise error
                backend.read_meta = read_meta
                with self.assertRaises(type(error)) as caught:
                    import_event_records_from_posts(backend, self.settings(), new_posts=source)
                self.assertIs(caught.exception, error)
                self.assertFalse(backend.reads or backend.registers or backend.writes or backend.appends)

    def test_memory_empty_events_create_schema_and_recover_failed_empty_write(self):
        source = source_posts([("2021-01-01", None, [], "unmapped")])
        backend = Tables(source)
        self.assertEqual(import_event_records_from_posts(backend, self.settings(), new_posts=source), 0)
        self.assertEqual((len(backend.registers), len(backend.writes)), (1, 1))
        pd.testing.assert_frame_equal(backend.tables["result"], expand_first_round(source, {}))
        self.assertEqual(import_event_records_from_posts(backend, self.settings(), new_posts=source), 0)
        self.assertEqual(len(backend.writes), 1)
        backend.tables["result"] = pd.DataFrame()
        self.assertEqual(import_event_records_from_posts(backend, self.settings(), previous_posts=source), 0)
        self.assertEqual((len(backend.registers), len(backend.writes)), (1, 2))
        pd.testing.assert_frame_equal(backend.tables["result"], expand_first_round(source, {}))

    def test_memory_import_rejects_duplicate_posts_before_any_target_access(self):
        source = source_posts([("2021-01-01", "ready", ["ETH"], "a")])
        backend = Tables(source)
        with self.assertRaisesRegex(ValueError, "唯一"):
            import_event_records_from_posts(backend, self.settings(), new_posts=source, previous_posts=source)
        self.assertFalse(backend.meta_reads or backend.reads or backend.registers or backend.writes or backend.appends)

    def test_expansion_mapping_duplicates_and_original_index(self):
        source = source_posts([("2018-01-01 12:12:12", "fact", [" eth ", "ETH", "LEND"], "a"),
                               ("2018-01-02 12:12:12", "mapped", [], "b"),
                               ("2018-01-03 12:12:12", None, [], "unmapped")])
        original = source.copy(deep=True)
        result = expand_first_round(source, {"b": ["ETH"]})
        self.assertEqual(list(result.index.get_level_values("record_id")), ["100:0:ETH", "100:0:LEND", "101:0:ETH"])
        self.assertEqual(result.index[0][0], source.index[0][0])
        pd.testing.assert_frame_equal(source, original)
        pd.testing.assert_frame_equal(result, expand_first_round(source, {"b": ["ETH"]}))

    def test_integer_id_and_string_date_preserved(self):
        source = source_posts([("2018-01-01 12:12:12", "fact", ["ETH"], "a")])
        source.index = pd.MultiIndex.from_tuples([("2018-01-01 12:12:12", 22222222)], names=["date", "id"])
        result = expand_first_round(source, {})
        self.assertEqual(result.index[0], ("2018-01-01 12:12:12", "22222222:0:ETH"))

    def test_unmapped_asset_and_unfinished_first_fail_before_registration(self):
        source = source_posts([("2021-01-01", "fact", [], "unmapped")])
        backend = Tables(source)
        with self.assertRaisesRegex(ValueError, "无法映射"):
            import_event_records_from_table(backend, self.settings())
        self.assertFalse(backend.registers)
        source.iloc[0, source.columns.get_loc("status")] = "0"
        with self.assertRaisesRegex(ValueError, "尚未成功"):
            expand_first_round(source, {"unmapped": ["ETH"]})

    def test_initialize_resume_preserves_results_and_is_idempotent(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
        backend = Tables(source)
        import_event_records_from_table(backend, self.settings(start_time="2021-01-01", end_time="2021-01-01"))
        key = backend.tables["result"].index[0]
        backend.tables["result"].loc[key, ["event_relation", "event_id", "round2_status"]] = ["new", key[1], "1"]
        backend.tables["result"].loc[key, "round2_config_hash"] = ai_config_hash(self.config())
        backend.tables["result"].loc[key, "round3_ai_response"] = json.dumps({"ai_response_content": json.dumps(impact_result())})
        backend.tables["result"].loc[key, ["round3_status", "round3_config_hash", "error_message"]] = ["1", ai_config_hash(self.config()), "旧错误"]
        previous = backend.tables["result"].copy(deep=True)
        resume = self.settings(initialize_mode="resume")
        summary = import_event_records_from_table(backend, resume)
        self.assertEqual(summary.written, 1)
        self.assertEqual(backend.tables["result"].loc[key, "round2_status"], "1")
        self.assertEqual(import_event_records_from_table(backend, resume).written, 0)
        self.assertEqual(len(backend.registers), 1)
        self.assertEqual(len(backend.writes), 1)
        self.assertEqual(len(backend.appends), 1)
        self.assertFalse(backend.appends[0].index.isin(previous.index).any())
        self.assertFalse(backend.updates)
        pd.testing.assert_frame_equal(backend.tables["result"].loc[previous.index], previous)

    def test_successful_only_import_skips_failed_posts_and_imports_them_after_retry(self):
        source = source_posts([("2021-01-01", "ready", ["ETH"], "a"),
                               ("2021-01-01", "retry", ["ETH"], "a")])
        backend = Tables(source)
        backend.tables["first"].iloc[1, source.columns.get_loc("status")] = "-1"
        summary = import_event_records_from_table(backend, self.settings(), successful_only=True)
        self.assertEqual(summary.written, 1)
        self.assertEqual(list(backend.tables["result"]["event"]), ["ready"])
        self.relate(backend)
        previous = backend.tables["result"].copy(deep=True)
        backend.tables["first"].iloc[1, source.columns.get_loc("status")] = "1"
        summary = import_event_records_from_table(backend, self.settings(initialize_mode="resume"), successful_only=True)
        self.assertEqual(summary.written, 1)
        pd.testing.assert_frame_equal(backend.tables["result"].loc[previous.index], previous)
        self.assertEqual(import_event_records_from_table(backend, self.settings(initialize_mode="resume"),
                                                 successful_only=True).written, 0)
        self.assertEqual(len(backend.appends), 1)

    def test_successful_only_import_uses_run_range_and_does_not_parse_failed_results(self):
        source = source_posts([("2021-01-01", "outside", [], "unmapped"),
                               ("2021-01-02", "ready", ["ETH"], "a"),
                               ("2021-01-02", "failed", [], "unmapped")])
        backend = Tables(source)
        frame = backend.tables["first"]
        frame.loc[frame.index[2], ["status", "ai_response_content", "text", "user_name"]] = ["-1", "invalid", None, None]
        settings = self.settings(start_time="2021-01-02", end_time="2021-01-02")
        summary = import_event_records_from_table(backend, settings, successful_only=True)
        self.assertEqual(summary.written, 1)
        self.assertEqual(backend.reads, [("first", "2021-01-02", "2021-01-02")])
        self.assertEqual(list(backend.tables["result"]["event"]), ["ready"])

    def test_successful_only_import_preserves_other_posts_and_still_rejects_input_conflicts(self):
        source = source_posts([("2021-01-01", "ready", ["ETH"], "a"),
                               ("2021-01-01", "other", ["ETH"], "a")])
        expected = expand_first_round(source, {})
        backend = Tables(source, expected)
        backend.tables["first"].loc[source.index[1], "status"] = "-1"
        before = backend.tables["result"].copy(deep=True)
        settings = self.settings(initialize_mode="resume")
        self.assertEqual(import_event_records_from_table(backend, settings, successful_only=True).written, 0)
        self.assertFalse(backend.appends or backend.updates)
        pd.testing.assert_frame_equal(backend.tables["result"], before)
        backend.tables["first"].loc[source.index[0], "ai_response_content"] = json.dumps(
            {"events": [{"event": "changed", "tickers": ["ETH"]}]})
        with self.assertRaisesRegex(ValueError, "固定输入不一致"):
            import_event_records_from_table(backend, settings, successful_only=True)
        self.assertFalse(backend.appends or backend.updates)

    def test_auto_import_creates_once_then_appends_and_preserves_finished_results(self):
        source = source_posts([("2021-01-01", "ready", ["ETH"], "a"),
                               ("2021-01-02", "retry", ["ETH"], "a")])
        backend = Tables(source)
        backend.tables["first"].loc[source.index[1], "status"] = "-1"
        settings = self.settings(initialize_mode="auto")
        self.assertEqual(import_event_records_from_table(backend, settings, successful_only=True).written, 1)
        self.relate(backend)
        before = backend.tables["result"].copy(deep=True)
        backend.tables["first"].loc[source.index[1], "status"] = "1"
        self.assertEqual(import_event_records_from_table(backend, settings, successful_only=True).written, 1)
        self.assertEqual(import_event_records_from_table(backend, settings, successful_only=True).written, 0)
        self.assertEqual((len(backend.registers), len(backend.writes), len(backend.appends)), (1, 1, 1))
        pd.testing.assert_frame_equal(backend.tables["result"].loc[before.index], before)

    def test_auto_import_only_treats_explicit_data_not_found_as_missing_table(self):
        errors = [DataNotFound("missing"), RuntimeError("DataNotFound: missing"),
                  ConnectionError("offline"), PermissionError("forbidden"),
                  RuntimeError("DataNotFoundUnexpected"), RuntimeError("unknown read failure")]
        for error in errors:
            with self.subTest(error_type=type(error).__name__, message=str(error)):
                backend = Tables(source_posts([("2021-01-01", "ready", ["ETH"], "a")]))
                def read(path, *args):
                    if path == "result":
                        raise error
                backend.read_hook = read
                settings = self.settings(initialize_mode="auto")
                if isinstance(error, DataNotFound) or str(error).startswith("DataNotFound:"):
                    self.assertEqual(import_event_records_from_table(backend, settings, successful_only=True).written, 1)
                    self.assertEqual((len(backend.registers), len(backend.writes)), (1, 1))
                else:
                    with self.assertRaises(type(error)) as caught:
                        import_event_records_from_table(backend, settings, successful_only=True)
                    self.assertIs(caught.exception, error)
                    self.assertFalse(backend.registers or backend.writes or backend.appends)

    def test_auto_import_retries_write_without_registering_again(self):
        backend = Tables(source_posts([("2021-01-01", "ready", ["ETH"], "a")]))
        def write(path, frame):
            if len(backend.writes) == 1:
                raise ConnectionError("first write not saved")
            backend.tables[path] = frame.copy(deep=True)
        backend.write_hook = write
        with patch("common.config.RunConfig", return_value=self.config().run):
            self.assertEqual(import_event_records_from_table(backend, self.settings(initialize_mode="auto"),
                                                     successful_only=True).written, 1)
        self.assertEqual((len(backend.registers), len(backend.writes)), (1, 2))
        self.assertFalse(backend.appends or backend.updates)

    def test_auto_import_recovers_empty_schema_after_failed_first_write_without_reregistering(self):
        source = source_posts([("2021-01-01", None, [], "unmapped")])
        backend = Tables(source)
        def fail_write(path, frame):
            raise ConnectionError("initial empty write not saved")
        backend.write_hook = fail_write
        settings = self.settings(initialize_mode="auto")
        with patch("common.config.RunConfig", return_value=self.config().run):
            with self.assertRaises(BatchPersistenceError):
                import_event_records_from_table(backend, settings, successful_only=True)
            backend.write_hook = None
            summary = import_event_records_from_table(backend, settings, successful_only=True)
        self.assertEqual((summary.total, summary.written), (0, 0))
        self.assertEqual((len(backend.registers), len(backend.writes)), (1, 4))
        self.assertFalse(backend.appends or backend.updates)
        pd.testing.assert_frame_equal(backend.tables["result"], expand_first_round(source, {}))

    def test_initialize_resume_rejects_incomplete_schema_without_writing(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
        expected = expand_first_round(source, {})
        old = expected.iloc[:1]
        cases = {
            "missing_hash_columns": old.drop(columns=HASH_COLUMNS),
            "rows_without_columns": old.iloc[:, :0],
            "empty_partial_schema": expected.iloc[:0].drop(columns=HASH_COLUMNS),
            "extra_column": old.assign(unexpected=""),
            "invalid_index": old.reset_index(),
        }
        for label, frame in cases.items():
            with self.subTest(label=label):
                backend = Tables(source, frame)
                with self.assertRaises(ValueError):
                    import_event_records_from_table(backend, self.settings(initialize_mode="resume"))
                self.assertFalse(backend.updates or backend.appends or backend.writes or backend.registers)
                pd.testing.assert_frame_equal(backend.tables["result"], frame)

    def test_initialize_resume_preserves_old_cells_and_compares_fixed_input(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
        expected = expand_first_round(source, {})
        for column in ("event", "round2_config_hash", "round3_status", "round3_ai_response"):
            for value in (None, float("nan"), pd.NA):
                with self.subTest(column=column, value=type(value).__name__):
                    old = expected.iloc[:1].copy(deep=True)
                    old.at[old.index[0], column] = value
                    backend = Tables(source, old)
                    if column == "event":
                        # 固定输入的实际比较仍保留，pd.NA 无法转为布尔值时自然失败。
                        with self.assertRaises((ValueError, TypeError)):
                            import_event_records_from_table(backend, self.settings(initialize_mode="resume"))
                        self.assertFalse(backend.appends)
                    else:
                        result = import_event_records_from_table(backend, self.settings(initialize_mode="resume"))
                        self.assertEqual(result.written, 1)
                    pd.testing.assert_frame_equal(backend.tables["result"].loc[old.index], old)

    def test_initialize_append_retries_same_new_indexes_without_readback(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a"),
                               ("2021-01-02", "two", ["ETH"], "a"),
                               ("2021-01-03", "three", ["ETH"], "a")])
        expected = expand_first_round(source, {})
        backend = Tables(source, expected.iloc[:1])
        def append(path, frame):
            if len(backend.appends) == 1:
                frame["event"] = "client mutation"
                raise RuntimeError("append not written")
            backend.append_rows(path, frame)
        backend.append_hook = append
        with patch("common.config.RunConfig", return_value=self.config().run):
            summary = import_event_records_from_table(backend, self.settings(initialize_mode="resume"))
        self.assertEqual(summary.written, 2)
        for frame in backend.appends:
            pd.testing.assert_frame_equal(frame, expected.iloc[1:])
        self.assertEqual(backend.reads, [("first", None, None), ("result", None, None)])
        pd.testing.assert_frame_equal(backend.tables["result"], expected)
        self.assertFalse(backend.writes or backend.updates or backend.registers)

    def test_initialize_create_and_append_succeed_on_last_attempt(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a")])
        expected = expand_first_round(source, {})
        for mode in ("create", "resume"):
            with self.subTest(mode=mode):
                backend = Tables(source, None if mode == "create" else expected.iloc[:0])
                def save(path, frame):
                    if len(backend.writes) + len(backend.appends) < 3:
                        raise RuntimeError("not written")
                    if mode == "create":
                        backend.tables[path] = frame.copy(deep=True)
                    else:
                        backend.append_rows(path, frame)
                backend.write_hook = backend.append_hook = save
                with patch("common.config.RunConfig", return_value=self.config().run):
                    summary = import_event_records_from_table(backend, self.settings(initialize_mode=mode))
                self.assertEqual(summary.written, 1)
                self.assertEqual(summary.total, 1)
                self.assertEqual(len(backend.writes) + len(backend.appends), 3)
                self.assertFalse(backend.updates)
                self.assertEqual(len(backend.reads), 1 if mode == "create" else 2)
                pd.testing.assert_frame_equal(backend.tables["result"], expected)

    def test_initialize_append_budget_stops_with_whole_batch_unwritten(self):
        self.document["run"]["storage_max_attempts"] = 1
        source = source_posts([("2021-01-01", "one", ["ETH"], "a"),
                               ("2021-01-02", "two", ["ETH"], "a")])
        expected = expand_first_round(source, {})
        backend = Tables(source, expected.iloc[:1])
        error = RuntimeError("append not written")
        def append(path, frame):
            raise error
        backend.append_hook = append
        with patch("common.config.RunConfig", return_value=self.config().run):
            with self.assertRaises(BatchPersistenceError) as caught:
                import_event_records_from_table(backend, self.settings(initialize_mode="resume"))
        self.assertEqual(vars(caught.exception), {"stage": "initialize"})
        self.assertIs(caught.exception.__cause__, error)
        pd.testing.assert_frame_equal(backend.tables["result"], expected.iloc[:1])
        self.assertEqual(len(backend.appends), 1)
        self.assertEqual(len(backend.reads), 2)

    def test_initialize_create_budget_stops_and_resume_does_not_reregister(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a")])
        expected = expand_first_round(source, {})
        backend = Tables(source)
        def write(path, frame):
            raise RuntimeError("write not written")
        backend.write_hook = write
        with patch("common.config.RunConfig", return_value=self.config().run):
            with self.assertRaises(BatchPersistenceError):
                import_event_records_from_table(backend, self.settings())
        self.assertEqual(len(backend.writes), 3)
        self.assertEqual(len(backend.registers), 1)
        self.assertEqual(backend.reads, [("first", None, None)])
        self.assertTrue(backend.tables["result"].empty)
        import_event_records_from_table(backend, self.settings(initialize_mode="resume"))
        self.assertEqual(len(backend.registers), 1)
        self.assertEqual(len(backend.writes), 3)
        self.assertEqual(len(backend.appends), 1)
        pd.testing.assert_frame_equal(backend.tables["result"], expected)

    def test_initialize_conflicting_input_stops_before_append(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
        expected = expand_first_round(source, {})
        old = expected.iloc[:1].copy(deep=True)
        old.loc[:, "event"] = "与第一轮不一致"
        backend = Tables(source, old)
        with self.assertRaisesRegex(ValueError, "固定输入不一致"):
            import_event_records_from_table(backend, self.settings(initialize_mode="resume"))
        self.assertFalse(backend.writes or backend.updates or backend.appends)
        pd.testing.assert_frame_equal(backend.tables["result"], old)

    def test_initialize_scoped_append_preserves_rows_outside_scope(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
        expected = expand_first_round(source, {})
        backend = Tables(source, expected.iloc[:1])
        import_event_records_from_table(backend, self.settings(initialize_mode="resume", start_time="2021-01-02", end_time="2021-01-02"))
        pd.testing.assert_frame_equal(backend.appends[0], expected.iloc[1:])
        pd.testing.assert_frame_equal(backend.tables["result"], expected)
        self.assertFalse(backend.writes or backend.updates)

    def test_initialize_new_record_id_cannot_duplicate_an_old_date_outside_scope(self):
        source = source_posts([("2021-01-02", "one", ["ETH"], "a")])
        expected = expand_first_round(source, {})
        old = expected.copy(deep=True)
        old.index = pd.MultiIndex.from_tuples([(pd.Timestamp("2021-01-01"), expected.index[0][1])],
                                             names=expected.index.names)
        backend = Tables(source, old)
        with self.assertRaisesRegex(ValueError, "record_id"):
            import_event_records_from_table(backend, self.settings(initialize_mode="resume",
                                                           start_time="2021-01-02", end_time="2021-01-02"))
        self.assertFalse(backend.appends or backend.updates or backend.writes)
        pd.testing.assert_frame_equal(backend.tables["result"], old)

    def test_initialize_append_rejects_different_date_index_types_before_writing(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
        source.index = source.index.set_levels(source.index.levels[0].tz_localize("Asia/Shanghai"), level="date")
        expected = expand_first_round(source, {})
        old = expected.iloc[:1].copy(deep=True)
        old.index = old.index.set_levels(old.index.levels[0].tz_convert("UTC"), level="date")
        backend = Tables(source, old)
        with self.assertRaisesRegex(ValueError, "日期索引类型、时区或精度不一致"):
            import_event_records_from_table(backend, self.settings(initialize_mode="resume",
                                                           start_time="2021-01-02", end_time="2021-01-02"))
        self.assertFalse(backend.appends or backend.updates or backend.writes)
        pd.testing.assert_frame_equal(backend.tables["result"], old)

    def test_initialize_registered_empty_table_only_appends(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a")])
        backend = Tables(source, pd.DataFrame())
        summary = import_event_records_from_table(backend, self.settings(initialize_mode="resume"))
        self.assertEqual(summary.written, 1)
        self.assertEqual(len(backend.appends), 1)
        self.assertFalse(backend.writes or backend.updates or backend.registers)
        pd.testing.assert_frame_equal(backend.tables["result"], expand_first_round(source, {}))

    def test_initialize_empty_events_create_and_resume_without_append(self):
        backend = Tables(source_posts([("2021-01-01", None, [], "a")]))
        import_event_records_from_table(backend, self.settings())
        import_event_records_from_table(backend, self.settings(initialize_mode="resume"))
        self.assertEqual(len(backend.writes), 1)
        self.assertEqual(len(backend.registers), 1)
        self.assertFalse(backend.appends or backend.updates)

    def test_initialize_empty_create_retries_same_schema_without_readback(self):
        source = source_posts([("2021-01-01", None, [], "a")])
        expected = expand_first_round(source, {})
        backend = Tables(source)
        def write(path, frame):
            if len(backend.writes) == 1:
                raise RuntimeError("empty schema not written")
            backend.tables[path] = frame.copy(deep=True)
        backend.write_hook = write
        with patch("common.config.RunConfig", return_value=self.config().run):
            import_event_records_from_table(backend, self.settings())
        self.assertEqual(len(backend.writes), 2)
        self.assertEqual(len(backend.registers), 1)
        self.assertEqual(backend.reads, [("first", None, None)])
        self.assertFalse(backend.appends or backend.updates)
        for frame in backend.writes:
            pd.testing.assert_frame_equal(frame, expected)
        pd.testing.assert_frame_equal(backend.tables["result"], expected)

    def test_initialize_retry_preserves_timezone_and_index_precision(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
        stamps = pd.date_range("2021-01-01 12:00:00.123456789", periods=2, tz="Asia/Shanghai")
        source.index = pd.MultiIndex.from_arrays([stamps, ["100", "101"]], names=["date", "id"])
        expected = expand_first_round(source, {})
        backend = Tables(source, expected.iloc[:1])
        def append(path, frame):
            if len(backend.appends) == 1:
                raise RuntimeError("not written")
            backend.append_rows(path, frame)
        backend.append_hook = append
        with patch("common.config.RunConfig", return_value=self.config().run):
            import_event_records_from_table(backend, self.settings(initialize_mode="resume"))
        for frame in backend.appends:
            pd.testing.assert_frame_equal(frame, expected.iloc[1:])
        self.assertEqual(len(backend.reads), 2)
        pd.testing.assert_frame_equal(backend.tables["result"], expected)

    def test_initialize_append_preserves_string_and_timezone_indexes(self):
        for dates in (["2021-01-01 12:12:12.123456", "2021-01-02 12:12:12.123456"],
                      list(pd.date_range("2021-01-01 12:12:12.123456", periods=2, tz="Asia/Shanghai"))):
            with self.subTest(dates=dates):
                source = source_posts([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
                source.index = pd.MultiIndex.from_arrays([dates, ["100", "101"]], names=["date", "id"])
                expected = expand_first_round(source, {})
                backend = Tables(source, expected.iloc[:1])
                import_event_records_from_table(backend, self.settings(initialize_mode="resume"))
                pd.testing.assert_frame_equal(backend.appends[0], expected.iloc[1:])
                pd.testing.assert_frame_equal(backend.tables["result"], expected)

    def test_initial_failed_write_retries_whole_table_without_reregistering(self):
        source = source_posts([("2021-01-01", "one", ["ETH"], "a"), ("2021-01-02", "two", ["ETH"], "a")])
        backend = Tables(source)
        def hook(path, frame):
            if len(backend.writes) == 1:
                raise RuntimeError("initial write not written")
            backend.tables[path] = frame.copy(deep=True)
        backend.write_hook = hook
        with patch("common.config.RunConfig", return_value=self.config().run):
            summary = import_event_records_from_table(backend, self.settings())
        self.assertEqual(summary.written, 2)
        self.assertEqual(len(backend.registers), 1)
        self.assertEqual(len(backend.writes), 2)
        self.assertFalse(backend.appends)
        pd.testing.assert_frame_equal(backend.writes[0], backend.writes[1])
        pd.testing.assert_frame_equal(backend.tables["result"], expand_first_round(source, {}))
