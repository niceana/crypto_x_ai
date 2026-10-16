"""验证共用 Store 的运行边界，摘要只通过模拟列读取接入。"""

import json
import unittest
from dataclasses import replace
from unittest.mock import patch

import pandas as pd

from common.config import ai_config_hash, EventConfig
from common.persistence import BatchPersistenceError, TaskValidationError
from common.ai_protocol import parse_response
from round1.runner import ExtractionSummary
from common.store import PostTaskStore, EventRecordStore
from initialization.event_records import import_event_records_from_table
from round2.runner import run_relation
from round3.runner import run_impact
from tests.support import FakeCli, Fixture, event_success, request_text, success, tasks
from tests.event_support import Tables, choose, impact_result, new_result, source_posts


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

    def test_source_text_none_is_used_without_read_content_validation(self):
        for stage in ("round2", "round3"):
            with self.subTest(stage=stage):
                backend, settings = self.backend()
                config = self.config()
                relation_settings = replace(settings, start_time="2021-01-01", end_time="2021-01-01")
                if stage == "round3":
                    run_relation(backend, relation_settings, ai_config=config, retriever=choose,
                                 network_factory=self.network(lambda request: success(json.dumps(new_result()))))
                backend.tables["first"]["text"] = None
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
                    self.assertIsNone(seen[0]["current"]["source_text"])
                else:
                    self.assertNotIn("source_text", seen[0]["current"])

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
