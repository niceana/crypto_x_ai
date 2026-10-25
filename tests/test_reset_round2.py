"""重跑前的状态重置：预览、范围、下游依赖和写库前备份。"""

import json
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from entrypoints.reset_round2 import main, reset_round2_status
from tests.event_support import EventFixture


class ResetRound2Tests(EventFixture, unittest.TestCase):
    def setup_batch(self):
        backend = self.backend([
            ("2021-01-01", "earlier", ["ETH"], "a"),
            ("2021-01-02", "done", ["ETH"], "a"),
            ("2021-01-02", "pending", ["BTC"], "a"),
            ("2021-01-03", "failed", ["ETH"], "a"),
            ("2021-01-04", "later", ["BTC"], "a"),
        ])
        frame = backend.tables["result"]
        frame.loc[:, "round2_status"] = ["1", "1", "0", "-1", "1"]
        frame.loc[:, "round3_status"] = ["1", "1", "0", "0", "1"]
        for column in ("event_id", "event_relation", "new_information", "round2_ai_response",
                       "round3_ai_response", "embedding", "round2_config_hash", "round3_config_hash", "error_message"):
            frame.loc[:, column] = "saved-" + column
        settings = self.settings(start_time="2021-01-02", end_time="2021-01-03",
                                 round2_asset_list_path="universe.json")
        backend.read_object = lambda path: b'{"InstrumentList": [" eth.usdt ", "ETH.other"]}'
        return backend, settings

    def test_default_preview_is_read_only_and_keeps_incomplete_records(self):
        backend, settings = self.setup_batch()
        before = backend.tables["result"].copy(deep=True)
        output = Path(self.temporary.name) / "preview"
        result = reset_round2_status(backend, settings, backup_dir=output)
        self.assertEqual((result["records"], result["round2_completed"], result["round3_completed"]), (2, 1, 1))
        self.assertEqual(result["assets"], ["ETH"])
        self.assertFalse(result["applied"] or backend.updates or output.exists())
        pd.testing.assert_frame_equal(backend.tables["result"], before)

    def test_apply_backs_up_full_rows_before_writing_only_two_status_columns(self):
        backend, settings = self.setup_batch()
        before = backend.tables["result"].copy(deep=True)
        selected = before.loc[before["event"].isin(["done", "failed"])].sort_index()
        output = Path(self.temporary.name) / "backup"

        def update(path, patch_frame):
            pd.testing.assert_frame_equal(pd.read_pickle(output / "before_reset.pkl"), selected)
            self.assertFalse(json.loads((output / "manifest.json").read_text(encoding="utf-8"))["applied"])
            backend.apply(path, patch_frame)

        backend.update_hook = update
        result = reset_round2_status(backend, settings, apply=True, backup_dir=output)
        self.assertTrue(result["applied"])
        self.assertEqual(len(backend.updates), 1)
        self.assertEqual(set(backend.updates[0].columns), {"round2_status", "round3_status"})
        expected = before.copy(deep=True)
        expected.loc[selected.index, ["round2_status", "round3_status"]] = "0"
        pd.testing.assert_frame_equal(backend.tables["result"], expected)
        self.assertTrue(json.loads((output / "manifest.json").read_text(encoding="utf-8"))["applied"])

    def test_later_completed_same_asset_blocks_reset_beyond_configured_end(self):
        for apply in (False, True):
            backend, settings = self.setup_batch()
            backend.tables["result"].iloc[-1, backend.tables["result"].columns.get_loc("asset")] = "ETH"
            output = Path(self.temporary.name) / "blocked"
            with self.subTest(apply=apply), self.assertRaisesRegex(ValueError, "扩大至至少 2021-01-04"):
                reset_round2_status(backend, settings, apply=apply, backup_dir=output)
            self.assertFalse(backend.updates or output.exists())

    def test_no_asset_list_resets_all_assets_in_range_and_all_statuses(self):
        backend, _ = self.setup_batch()
        frame = backend.tables["result"]
        frame.iloc[2, frame.columns.get_loc("round2_status")] = "9"
        settings = self.settings(start_time="2021-01-01", end_time="2021-01-04")
        output = Path(self.temporary.name) / "all"
        result = reset_round2_status(backend, settings, apply=True, backup_dir=output)
        self.assertEqual(result["records"], 5)
        self.assertTrue(frame["round2_status"].eq("0").all() and frame["round3_status"].eq("0").all())

    def test_backup_failure_or_existing_directory_never_writes_database(self):
        backend, settings = self.setup_batch()
        output = Path(self.temporary.name) / "failed-backup"
        with patch.object(pd.DataFrame, "to_pickle", side_effect=OSError("disk full")):
            with self.assertRaisesRegex(OSError, "disk full"):
                reset_round2_status(backend, settings, apply=True, backup_dir=output)
        with self.assertRaises(FileExistsError):
            reset_round2_status(backend, settings, apply=True, backup_dir=output)
        self.assertFalse(backend.updates)

    def test_original_timestamp_timezone_and_nanoseconds_are_preserved(self):
        backend, _ = self.setup_batch()
        frame = backend.tables["result"].iloc[[1]].copy(deep=True)
        frame.index = pd.MultiIndex.from_tuples([
            (pd.Timestamp("2021-01-02 12:34:56.123456789", tz="Asia/Shanghai"), "precise")
        ], names=["date", "record_id"])
        backend.tables["result"] = frame
        output = Path(self.temporary.name) / "precise"
        reset_round2_status(backend, self.settings(start_time="2021-01-02", end_time="2021-01-02"),
                            apply=True, backup_dir=output)
        pd.testing.assert_index_equal(backend.updates[0].index, frame.index)
        pd.testing.assert_index_equal(pd.read_pickle(output / "before_reset.pkl").index, frame.index)

    def test_database_failure_leaves_backup_and_does_not_mark_applied(self):
        backend, settings = self.setup_batch()
        output = Path(self.temporary.name) / "database-failure"
        backend.update_hook = lambda path, frame: (_ for _ in ()).throw(RuntimeError("database failed"))
        with self.assertRaisesRegex(RuntimeError, "database failed"):
            reset_round2_status(backend, settings, apply=True, backup_dir=output)
        self.assertTrue((output / "before_reset.pkl").exists())
        self.assertFalse(json.loads((output / "manifest.json").read_text(encoding="utf-8"))["applied"])

    def test_missing_dates_rejected_before_read_and_empty_scope_never_writes(self):
        backend, _ = self.setup_batch()
        with self.assertRaisesRegex(ValueError, "start_time/end_time"):
            reset_round2_status(backend, self.settings(), apply=True)
        self.assertFalse(backend.reads or backend.column_reads or backend.updates)
        output = Path(self.temporary.name) / "empty"
        result = reset_round2_status(backend, self.settings(start_time="2022-01-01", end_time="2022-01-01"),
                                     apply=True, backup_dir=output)
        self.assertEqual(result["records"], 0)
        self.assertFalse(result["applied"] or backend.column_reads or backend.updates or output.exists())

    def test_entrypoint_uses_config_and_requires_explicit_apply(self):
        backend, _ = self.setup_batch()
        config = Path(self.temporary.name) / "events.yaml"
        config.write_text(yaml.safe_dump({
            "first_round_task_path": "first", "result_path": "result",
            "round2_ai_config_path": "unused.yaml", "round3_ai_config_path": "unused.yaml",
            "start_time": "2021-01-02", "end_time": "2021-01-04",
        }), encoding="utf-8")
        self.assertFalse(main(["--config", str(config)], cli=backend)["applied"])
        self.assertFalse(backend.updates)
        output = Path(self.temporary.name) / "cli-backup"
        self.assertTrue(main(["--config", str(config), "--apply", "--backup-dir", str(output)],
                             cli=backend)["applied"])


if __name__ == "__main__":
    unittest.main()
