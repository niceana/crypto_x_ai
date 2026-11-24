"""状态恢复使用原索引、仅提交两个状态列，异常时禁止写库。"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from entrypoints.restore_event_status import restore_statuses


class RestoreEventStatusTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.backup = self.root / "statuses.csv"
        self.frame = pd.DataFrame({
            "date": ["2026-06-05 07:19:31.123456789", "2026-06-05 17:24:52", "2026-06-06 10:00:00"],
            "record_id": ["2062796443300282773:0:SOL", "2062948785219670415:0:SOL", "other:0:ETH"],
            "asset": ["SOL", "SOL", "ETH"], "round2_status": ["1", "1", "1"],
            "round3_status": ["1", "0", "1"],
        })
        self.save_backup()
        self.current = self.frame.copy()
        self.current["date"] = pd.DatetimeIndex([pd.Timestamp(value) for value in self.current["date"]])
        self.current = self.current.set_index(["date", "record_id"])
        self.current["round2_status"] = "0"
        self.current["round3_status"] = "0"
        self.current["event_id"] = ["group-1", "group-1", "group-2"]
        self.current["round3_ai_response"] = "保留原 AI 回答"
        self.current["round2_config_hash"] = "保留原配置摘要"
        self.cli = Mock(spec=["read", "update_by_column"])
        self.cli.read.side_effect = lambda *args, **kwargs: self.current.copy(deep=True)

    def save_backup(self):
        self.frame.to_csv(self.backup, index=False, encoding="utf-8-sig")

    def run_restore(self, **kwargs):
        return restore_statuses(self.cli, "results", self.backup,
                                output_root=self.root / "plans", **kwargs)

    def test_preview_keeps_strings_and_precision_and_never_writes(self):
        result = self.run_restore()
        self.assertEqual((result["matched_rows"], result["changed_rows"], result["written_rows"]), (2, 2, 0))
        self.cli.update_by_column.assert_not_called()
        self.cli.read.assert_called_once_with("results", start_time="2026-06-05", end_time="2026-06-05")
        snapshot = pd.read_pickle(Path(result["output_dir"]) / "before_restore.pkl")
        pd.testing.assert_frame_equal(snapshot, self.current.iloc[:2], check_exact=True)

    def test_apply_backs_up_and_updates_only_sol_statuses_then_repeat_is_noop(self):
        original = self.current.copy(deep=True)

        def submit(path, batch):
            saved = list((self.root / "plans").glob("*/before_restore.pkl"))
            self.assertEqual(len(saved), 1)
            pd.testing.assert_frame_equal(pd.read_pickle(saved[0]), original.iloc[:2], check_exact=True)
            self.assertEqual(list(batch.columns), ["round2_status", "round3_status"])
            self.assertTrue(batch.index.is_monotonic_increasing)
            self.assertEqual(batch.index[0][0].nanosecond, 789)
            self.assertEqual(batch.index[0][1], "2062796443300282773:0:SOL")
            self.assertTrue(all(isinstance(value, str) for value in batch.to_numpy().ravel()))
            self.current.loc[batch.index, batch.columns] = batch

        self.cli.update_by_column.side_effect = submit
        result = self.run_restore(apply=True)
        self.assertEqual(result["written_rows"], 2)
        pd.testing.assert_frame_equal(self.current.drop(columns=["round2_status", "round3_status"]),
                                      original.drop(columns=["round2_status", "round3_status"]), check_exact=True)
        pd.testing.assert_frame_equal(self.current.iloc[2:], original.iloc[2:], check_exact=True)
        self.assertEqual(self.run_restore(apply=True)["written_rows"], 0)
        self.cli.update_by_column.assert_called_once()

    def test_missing_key_or_asset_mismatch_or_rerun_state_stops_writes(self):
        original = self.current.copy(deep=True)
        for scenario in ("missing", "asset", "rerun", "numeric"):
            with self.subTest(scenario=scenario):
                self.current = original.copy(deep=True)
                if scenario == "missing":
                    self.current = self.current.iloc[1:]
                elif scenario == "asset":
                    self.current.iloc[0, self.current.columns.get_loc("asset")] = "ETH"
                elif scenario == "rerun":
                    self.current.iloc[0, self.current.columns.get_loc("round2_status")] = "-1"
                else:
                    self.current.iloc[0, self.current.columns.get_loc("round2_status")] = 0
                with self.assertRaises(ValueError):
                    self.run_restore(apply=True)
                self.cli.update_by_column.assert_not_called()

    def test_bad_backup_stops_before_database_read(self):
        original = self.frame.copy(deep=True)
        for scenario in ("duplicate", "status", "date", "timezone", "no_sol"):
            with self.subTest(scenario=scenario):
                self.frame = original.copy(deep=True)
                if scenario == "duplicate":
                    self.frame = pd.concat([self.frame, self.frame.iloc[:1]], ignore_index=True)
                elif scenario == "status":
                    self.frame.loc[0, "round2_status"] = "1.0"
                elif scenario == "date":
                    self.frame.loc[0, "date"] = ""
                elif scenario == "timezone":
                    self.frame.loc[:1, "date"] = "2026-06-05T00:00:00Z"
                else:
                    self.frame["asset"] = "ETH"
                self.save_backup()
                with self.assertRaises(ValueError):
                    self.run_restore(apply=True)
                self.cli.read.assert_not_called()
                self.cli.update_by_column.assert_not_called()

    def test_snapshot_failure_stops_before_writing_statuses(self):
        with patch("entrypoints.restore_event_status.pd.DataFrame.to_pickle", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.run_restore(apply=True)
        self.cli.update_by_column.assert_not_called()


if __name__ == "__main__":
    unittest.main()
