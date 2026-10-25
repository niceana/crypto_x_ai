"""只读备份的范围、原始数据保留、完整性及失败行为，不连接真实数据库。"""

import csv
import hashlib
import json
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from entrypoints.backup_events import backup_events, main
from tests.event_support import EventFixture


class BackupEventsTests(EventFixture, unittest.TestCase):
    def setup_batch(self):
        backend = self.backend([
            ("2021-01-01", "outside", ["ETH"], "a"),
            ("2021-01-02", "done", ["ETH"], "a"),
            ("2021-01-02", "other asset", ["BTC"], "a"),
            ("2021-01-03", "pending", ["ETH"], "a"),
            ("2021-01-03", "failed", ["ETH"], "a"),
            ("2021-01-03", "processing", ["ETH"], "a"),
        ])
        frame = backend.tables["result"]
        frame["round2_status"] = ["1", "1", "1", "0", "-1", "9"]
        frame["round3_status"] = ["1", "0", "1", "0", "0", "0"]
        frame["embedding"] = '{"vector": [0.12345678901234567]}'
        frame["round2_ai_response"] = '{"answer": "原始回答"}'
        frame["round3_ai_response"] = '{"answer": "第三轮原始回答"}'
        backend.read_object = lambda path: b'{"InstrumentList": [" eth.usdt ", "ETH.other"]}'
        settings = self.settings(start_time="2021-01-02", end_time="2021-01-03",
                                 round2_asset_list_path="universe.json")
        output = Path(self.temporary.name) / "backup"
        return backend, settings, output

    def test_default_backup_preserves_all_states_full_rows_and_never_writes_database(self):
        backend, settings, output = self.setup_batch()
        before = backend.tables["result"].copy(deep=True)
        result = backup_events(backend, settings, output)
        expected = before.loc[before["event"].isin(["done", "pending", "failed", "processing"])].sort_index()
        pd.testing.assert_frame_equal(pd.read_pickle(output / "events.pkl"), expected)
        pd.testing.assert_frame_equal(backend.tables["result"], before)
        self.assertEqual((result["records"], result["round2_completed"], result["round3_completed"]), (4, 1, 0))
        self.assertTrue(result["completed"])
        self.assertFalse(backend.updates or backend.writes or backend.registers or backend.appends)
        self.assertEqual(backend.reads, [("result", "2021-01-02", "2021-01-03")])
        self.assertFalse(backend.column_reads)

    def test_completed_only_and_expected_count_include_only_second_round_successes(self):
        backend, settings, output = self.setup_batch()
        result = backup_events(backend, settings, output, completed_only=True, expected_records=1)
        data = pd.read_pickle(output / "events.pkl")
        self.assertEqual(data["event"].tolist(), ["done"])
        self.assertEqual((result["scope_records"], result["records"], result["round3_completed"]), (4, 1, 0))
        self.assertTrue(result["completed_only"])

    def test_no_asset_list_keeps_all_assets_in_dates_and_manifest_has_valid_file_digests(self):
        backend, _, output = self.setup_batch()
        settings = self.settings(start_time="2021-01-02", end_time="2021-01-03")
        result = backup_events(backend, settings, output)
        self.assertEqual(result["records"], 5)
        self.assertEqual(result["assets"], ["BTC", "ETH"])
        self.assertEqual(json.loads((output / "manifest.json").read_text(encoding="utf-8")), result)
        for name, metadata in result["files"].items():
            raw = (output / name).read_bytes()
            self.assertEqual(metadata, {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)})

    def test_full_multiline_text_timestamp_timezone_nanoseconds_and_vectors_round_trip(self):
        backend, settings, output = self.setup_batch()
        frame = backend.tables["result"].iloc[[1]].copy(deep=True)
        frame.index = pd.MultiIndex.from_tuples([
            (pd.Timestamp("2021-01-02 12:34:56.123456789", tz="Asia/Shanghai"), "precise")
        ], names=["date", "record_id"])
        frame["event"] = '含有逗号,引号"和换行\n完整正文' + "长文本" * 12000
        backend.tables["result"] = frame
        backup_events(backend, settings, output)
        pd.testing.assert_frame_equal(pd.read_pickle(output / "events.pkl"), frame, check_exact=True)
        with (output / "events.csv").open(encoding="utf-8-sig", newline="") as handle:
            row = list(csv.DictReader(handle))[0]
        self.assertEqual(row["event"], frame.iloc[0]["event"])
        self.assertEqual(row["embedding"], frame.iloc[0]["embedding"])

    def test_count_mismatch_empty_scope_and_missing_dates_never_create_successful_backup(self):
        backend, settings, output = self.setup_batch()
        with self.assertRaisesRegex(ValueError, "与预期 5000 不符"):
            backup_events(backend, settings, output, completed_only=True, expected_records=5000)
        self.assertFalse(output.exists())
        with self.assertRaisesRegex(ValueError, "没有可备份记录"):
            backup_events(backend, self.settings(start_time="2022-01-01", end_time="2022-01-01"), output)
        with self.assertRaisesRegex(ValueError, "start_time/end_time"):
            backup_events(backend, self.settings(), output)
        self.assertFalse(output.exists() or backend.updates)

    def test_export_failure_verification_failure_or_existing_output_has_no_completion_marker(self):
        for operation, error in (("to_csv", OSError("disk full")), ("read_pickle", ValueError("invalid pickle"))):
            backend, settings, _ = self.setup_batch()
            output = Path(self.temporary.name) / operation
            target = pd.DataFrame if operation == "to_csv" else pd
            with patch.object(target, operation, side_effect=error):
                with self.subTest(operation=operation), self.assertRaises(type(error)):
                    backup_events(backend, settings, output)
            self.assertFalse((output / "manifest.json").exists() or backend.updates)
            with self.assertRaises(FileExistsError):
                backup_events(backend, settings, output)

    def test_asset_list_read_failure_does_not_silently_export_other_assets(self):
        backend, settings, output = self.setup_batch()

        def fail(path):
            raise OSError("asset list unavailable")

        backend.read_object = fail
        with self.assertRaisesRegex(OSError, "asset list unavailable"):
            backup_events(backend, settings, output)
        self.assertFalse(output.exists() or backend.updates)

    def test_entrypoint_loads_scope_and_completed_only_without_changing_states(self):
        backend, _, output = self.setup_batch()
        config = Path(self.temporary.name) / "events.yaml"
        config.write_text(yaml.safe_dump({
            "first_round_task_path": "first", "result_path": "result",
            "round2_ai_config_path": "unused.yaml", "round3_ai_config_path": "unused.yaml",
            "start_time": "2021-01-02", "end_time": "2021-01-03",
        }), encoding="utf-8")
        result = main(["--config", str(config), "--completed-only", "--expected-records", "2",
                       "--output-dir", str(output)], cli=backend)
        self.assertEqual(result["records"], 2)
        self.assertTrue(result["completed"])
        self.assertFalse(backend.updates)


if __name__ == "__main__":
    unittest.main()
