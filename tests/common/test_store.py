"""验证共用 Store 的运行边界，摘要只通过模拟列读取接入。"""

import json
import unittest
from dataclasses import replace
from unittest.mock import Mock, patch

import pandas as pd

from common.config import ai_config_hash, EventConfig
from common.persistence import BatchPersistenceError, TaskValidationError
from common.ai_protocol import parse_response
from round1.runner import ExtractionSummary
from common.store import PostTaskStore, EventRecordStore, read_event_history
from initialization.event_records import expand_first_round, import_event_records_from_table
from round2.runner import run_relation
from round3.runner import run_impact
from tests.support import FakeCli, Fixture, event_success, request_text, success, tasks
from tests.event_support import Tables, choose, impact_result, new_result, source_posts


class HistoryReadTests(unittest.TestCase):
    def frame(self):
        return expand_first_round(source_posts([
            ("2025-01-02T12:00:00.123456789+08:00", "later", ["ETH"], "a"),
            ("2025-01-01T00:00:00+08:00", "earlier", ["ETH"], "a"),
        ]), {})

    def test_full_history_read_preserves_values_original_index_and_query_arguments(self):
        frame = self.frame()
        frame.loc[:, "event"] = None
        frame.loc[:, "round2_status"] = "unknown"
        before = frame.copy(deep=True)
        cli = Mock()
        cli.read.return_value = frame
        actual = read_event_history(cli, "events", "2025-01-01", "2025-01-02")
        self.assertIs(actual, frame)
        pd.testing.assert_frame_equal(actual, before)
        cli.read.assert_called_once_with("events", start_time="2025-01-01", end_time="2025-01-02")

    def test_projected_history_read_needs_only_requested_columns_and_keeps_raw_values(self):
        columns = ["asset", "round2_status"]
        frame = self.frame()[columns]
        frame.iloc[0, 1] = pd.NA
        before = frame.copy(deep=True)
        cli = Mock()
        cli.read.return_value = frame
        actual = read_event_history(cli, "events", "2025-01-01", "2025-01-02", columns=columns)
        self.assertIs(actual, frame)
        pd.testing.assert_frame_equal(actual, before)
        cli.read.assert_called_once_with("events", start_time="2025-01-01", end_time="2025-01-02", columns=columns)

    def test_empty_full_and_projected_frames_are_valid(self):
        for columns in (None, ["asset", "round2_status"]):
            with self.subTest(columns=columns):
                frame = self.frame().iloc[:0]
                if columns is not None:
                    frame = frame[columns]
                cli = Mock()
                cli.read.return_value = frame
                self.assertIs(read_event_history(cli, "events", "2025-01-01", "2025-01-02", columns=columns), frame)
                self.assertEqual(cli.read.call_count, 1)

    def test_bad_full_structure_missing_projected_column_and_duplicate_index_stop(self):
        for problem in ("full-columns", "projected-columns", "projected-index"):
            with self.subTest(problem=problem):
                frame, columns = self.frame(), None
                if problem == "full-columns":
                    frame = frame.drop(columns="embedding")
                else:
                    columns = ["asset", "round2_status"]
                    frame = frame[columns]
                    if problem == "projected-columns":
                        frame = frame.drop(columns="asset")
                    else:
                        frame.index = pd.MultiIndex.from_tuples([frame.index[0]] * len(frame), names=frame.index.names)
                cli = Mock()
                cli.read.return_value = frame
                with self.assertRaises(ValueError):
                    read_event_history(cli, "events", "2025-01-01", "2025-01-02", columns=columns)
                self.assertEqual(cli.read.call_count, 1)

    def test_full_and_projected_dates_outside_query_fail_with_read_context(self):
        for columns in (None, ["asset", "round2_status"]):
            with self.subTest(columns=columns):
                frame = self.frame()
                if columns is not None:
                    frame = frame[columns]
                cli = Mock()
                cli.read.return_value = frame
                with self.assertRaisesRegex(ValueError, "测试读取返回了分段日期以外的行"):
                    read_event_history(cli, "events", "2025-01-02", "2025-01-02", columns=columns, label="测试读取")
                self.assertEqual(cli.read.call_count, 1)

    def test_read_failure_propagates_without_retry_or_full_table_fallback(self):
        cli = Mock()
        failure = RuntimeError("数据库读取失败")
        cli.read.side_effect = failure
        with self.assertRaises(RuntimeError) as captured:
            read_event_history(cli, "events", "2025-01-01", "2025-01-02", columns=["asset"])
        self.assertIs(captured.exception, failure)
        cli.read.assert_called_once_with("events", start_time="2025-01-01", end_time="2025-01-02", columns=["asset"])


class StoreTests(Fixture, unittest.TestCase):
    def backend(self, count=1):
        source = source_posts([("2021-01-{:02d}".format(i + 1), "event", ["ETH"], "a")
                               for i in range(count)])
        backend = Tables(source)
        settings = EventConfig("first", "result", "unused", "unused")
        import_event_records_from_table(backend, settings)
        return backend, settings

    def network(self, handler):
        from common.network import NetworkWorker
        import httpx
        return lambda config: NetworkWorker(config, client_factory=lambda config: httpx.AsyncClient(
            transport=httpx.MockTransport(handler), trust_env=False))

    def test_column_read_failure_stops_without_fallback_or_claim(self):
        self.hash_reader.stop()
        cli = FakeCli(tasks(["2021-01-01"]))
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        with patch.object(cli, "read", side_effect=RuntimeError("摘要读取失败")) as read:
            with self.assertRaisesRegex(RuntimeError, "摘要读取失败"):
                store.claim(store.select(1))
        read.assert_called_once_with(self.task_path, start_time="", end_time="", columns=["config_hash"])
        self.assertEqual(cli.reads, [(None, None)])
        self.assertFalse(cli.updates)

    def test_missing_hash_column_stops_without_fallback_or_claim(self):
        cli = FakeCli(tasks(["2021-01-01"]))
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        with patch.object(cli, "read", return_value=cli.frame.drop(columns="config_hash")) as read:
            with self.assertRaises(KeyError):
                store.claim(store.select(1))
        read.assert_called_once_with(self.task_path, start_time="", end_time="", columns=["config_hash"])
        self.assertEqual(cli.reads, [(None, None)])
        self.assertFalse(cli.updates)

    def test_first_round_column_check_and_digest_are_shared_across_months(self):
        cli = FakeCli(tasks(["2021-01-31", "2021-02-01", "2021-03-01"]))
        with patch("common.store.ai_config_hash", wraps=ai_config_hash) as calculate:
            summary = self.worker(cli, lambda request: event_success(), "2021-01-01", "2021-03-31")
        self.assertEqual(summary.new_succeeded, 3)
        self.assertEqual(calculate.call_count, 1)
        self.assertEqual(self.hash_reads, [(self.task_path, "config_hash")])
        self.assertEqual(cli.column_reads, [(self.task_path, "", "", ["config_hash"])])
        self.assertEqual(len(cli.reads), 3)

    def test_first_round_scope_does_not_hide_older_config_conflict(self):
        frame = tasks(["2021-01-01", "2021-02-01"])
        frame.loc[frame.index[0], ["status", "config_hash"]] = ["1", "f" * 64]
        cli = FakeCli(frame)
        requests = []
        with self.assertRaisesRegex(TaskValidationError, "全表已有配置摘要"):
            self.worker(cli, lambda request: requests.append(request) or event_success(),
                        "2021-02-01", "2021-02-28")
        self.assertFalse(requests or cli.updates)
        self.assertEqual(cli.reads, [("2021-02-01", "2021-02-28")])
        self.assertEqual(self.hash_reads, [(self.task_path, "config_hash")])
        self.assertEqual(cli.column_reads, [(self.task_path, "", "", ["config_hash"])])

    def test_unknown_statuses_remain_raw_and_are_not_counted_as_known_states(self):
        frame = tasks(pd.date_range("2021-01-01", periods=6))
        frame["status"] = ["1", "0", None, float("nan"), pd.NA, "unknown"]
        cli = FakeCli(frame)
        store = PostTaskStore(self.config(), cli, self.task_path)
        store.recover()
        keys = store.select(6)
        self.assertEqual(keys, [frame.index[1]])
        store.claim(keys)
        response = parse_response(200, event_success().text)
        store.save_results([(keys[0], response)])
        summary = ExtractionSummary()
        summary.add_month(store)
        self.assertEqual((summary.total, summary.already_succeeded, summary.new_succeeded), (6, 1, 1))
        self.assertEqual((summary.new_failed, summary.pending, summary.processing), (0, 0, 0))
        pd.testing.assert_frame_equal(store.df.iloc[2:], frame.iloc[2:])

    def test_event_recovery_batches_stop_after_failed_batch_without_clearing_other_values(self):
        for stage in ("round2", "round3"):
            with self.subTest(stage=stage):
                backend, settings = self.backend(5)
                frame = backend.tables["result"]
                frame[stage + "_status"] = ["9", "-1", "9", "-1", "1"]
                frame["error_message"] = "preserve error"
                frame["round3_ai_response"] = None
                before = frame.copy(deep=True)
                run = replace(self.config().run, batch_size=2, storage_max_attempts=2,
                              storage_retry_seconds=0)
                store = EventRecordStore(backend, settings, run)
                def update(path, batch):
                    if batch.index[0] == frame.index[2]:
                        raise RuntimeError("batch not written")
                    backend.apply(path, batch)
                backend.update_hook = update
                with self.assertRaises(BatchPersistenceError):
                    store.recover(stage, list(frame.index))
                self.assertEqual([len(batch) for batch in backend.updates], [2, 2, 2])
                self.assertEqual(list(frame[stage + "_status"]), ["0", "0", "9", "-1", "1"])
                pd.testing.assert_frame_equal(frame.drop(columns=stage + "_status"),
                                              before.drop(columns=stage + "_status"))
                pd.testing.assert_frame_equal(store.df, frame)

    def test_event_stages_do_not_read_source_text(self):
        for stage in ("round2", "round3"):
            with self.subTest(stage=stage):
                backend, settings = self.backend()
                config = self.config()
                relation_settings = replace(settings, start_time="2021-01-01", end_time="2021-01-01")
                if stage == "round3":
                    run_relation(backend, relation_settings, ai_config=config, retriever=choose,
                                 network_factory=self.network(lambda request: success(json.dumps(new_result()))))
                backend.tables["first"]["text"] = None
                backend.reads.clear()
                seen = []
                def handler(request):
                    seen.append(json.loads(request_text(request)))
                    return success(json.dumps(new_result() if stage == "round2" else impact_result()))
                kwargs = {"retriever": choose} if stage == "round2" else {}
                operation = run_relation if stage == "round2" else run_impact
                summary = operation(backend, relation_settings, ai_config=config,
                                    network_factory=self.network(handler), **kwargs)
                self.assertEqual(summary.new_completed, 1)
                if stage == "round3":
                    self.assertEqual(set(seen[0]), {"asset", "event"})
                    self.assertNotIn("new_information", seen[0])
                else:
                    self.assertNotIn("source_text", seen[0]["current"])
                self.assertFalse(any(path == "first" for path, _, _ in backend.reads))

    def test_event_snapshot_and_recovery_preserve_unusable_cells_without_quality_scan(self):
        backend, settings = self.backend()
        frame = backend.tables["result"]
        frame["round2_status"] = pd.NA
        frame["round3_status"] = float("nan")
        frame["round2_config_hash"] = None
        frame["event"] = None
        frame["round3_ai_response"] = pd.NA
        before = frame.copy(deep=True)
        store = EventRecordStore(backend, settings)
        for stage in ("round2", "round3"):
            store.recover(stage, list(frame.index))
            self.assertEqual(store.summarize(stage)["total"], 1)
            self.assertEqual(store.summarize(stage)["pending"], 0)
        self.assertFalse(backend.updates)
        pd.testing.assert_frame_equal(store.df, before)

    def test_raw_nonempty_hash_values_participate_in_config_comparison(self):
        for value in (None, float("nan"), pd.NA, "BAD"):
            with self.subTest(value=type(value).__name__):
                backend, settings = self.backend()
                backend.tables["result"]["round2_config_hash"] = value
                store = EventRecordStore(backend, settings)
                store.config = self.config()
                with self.assertRaisesRegex(ValueError, "配置摘要.*冲突"):
                    store.check_config("round2")
                self.assertFalse(backend.updates)
