"""从既有对账文件汇总资产差异，不启动回放。"""
from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd

WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORK / "tools/live_trial"))
from summarize_diffs import summarize


class DiffSummaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="diff-summary-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def json(self, path, data):
        path.write_text(json.dumps(data), encoding="utf-8")

    def batch(self, number, when, rows, *, status="success", comparison="full", flags=True):
        target = self.root / ("batch_{:04d}_".format(number) + pd.Timestamp(when).strftime("%Y%m%d_%H%M"))
        target.mkdir()
        self.json(target / "summary.json", {"status": status, "call_time": when})
        frame = pd.DataFrame(rows, columns=["asset", "new", "old", "equal"])
        frame["old_column_present"] = ~frame["asset"].eq("MISSING")
        if not flags:
            frame = frame.drop(columns="equal")
        frame.to_csv(target / ("comparison_" + comparison + ".csv"), index=False, encoding="utf-8-sig")
        return target

    def run_summary(self, **kwargs):
        with redirect_stdout(io.StringIO()):
            return summarize(self.root, **kwargs)

    def events(self, rows):
        index = pd.MultiIndex.from_arrays(
            [pd.DatetimeIndex([row[0] for row in rows]), [row[1] for row in rows]], names=["date", "record_id"])
        return pd.DataFrame([row[2:] for row in rows], index=index,
                            columns=["asset", "round2_status", "round3_status"])

    def event_data(self, old_rows, new_rows=None, *, hours=2, mode="full"):
        data = self.root / "data_240h"
        data.mkdir()
        reference = data / "reference_events.pkl"
        self.events(old_rows).to_pickle(reference)
        self.json(data / "manifest.json", {
            "dataset_id": "fixture", "first_time": "2025-01-01 00:47:00", "hours": hours,
            "files": {"reference_events.pkl": hashlib.sha256(reference.read_bytes()).hexdigest()}})
        self.json(self.root / "run.json", {"dataset_id": "fixture", "mode": mode, "hours": hours})
        if mode == "full":
            for number, call in enumerate(pd.date_range("2025-01-01 00:02", periods=hours * 4, freq="15min"), 1):
                target = self.root / ("batch_{:04d}_".format(number) + call.strftime("%Y%m%d_%H%M"))
                target.mkdir(exist_ok=True)
                self.json(target / "summary.json", {"status": "success", "call_time": str(call)})
                self.events((new_rows or {}).get(str(call), [])).to_pickle(target / "event_results.pkl")
        return data

    def test_counts_distinguish_different_hours_values_and_unique_assets(self):
        a = self.batch(4, "2025-01-01 00:47", [("ETH", 2, 1, False), ("BTC", 0, 0, True), ("MISSING", 0, 0, True)])
        b = self.batch(8, "2025-01-01 01:47", [("ETH", 3, 1, False), ("BTC", -1, 0, False), ("MISSING", 0, 0, True)])
        self.batch(12, "2025-01-01 02:47", [("ETH", 1, 1, True), ("BTC", 0, 0, True), ("MISSING", 0, 0, True)])
        self.json(self.root / "summary.json", {"status": "completed", "requested_hours": 3, "full_diff": 2})
        originals = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in self.root.rglob("*") if path.is_file()}
        result = self.run_summary()
        self.assertEqual(result["differing_hours"], 2)
        self.assertEqual(result["differing_asset_values"], 3)
        self.assertEqual(result["unique_differing_assets"], 2)
        self.assertEqual(result["compared_asset_values"], 9)
        self.assertEqual(result["warnings"], [])
        stats = pd.read_csv(self.root / "diff_stats_full/asset_diff_summary.csv", index_col="asset")
        self.assertEqual(stats.at["ETH", "diff_points"], 2)
        self.assertEqual(stats.at["BTC", "diff_points"], 1)
        self.assertEqual(stats.at["MISSING", "diff_points"], 0)
        self.assertEqual(stats.at["ETH", "max_abs_difference_on_diff"], 2)
        self.assertEqual(stats.at["ETH", "mean_abs_difference_on_diff"], 1.5)
        details = pd.read_csv(self.root / "diff_stats_full/diff_details.csv")
        self.assertEqual(len(details), 3)
        self.assertEqual(set(details["batch_dir"]), {a.name, b.name})
        for path, original in originals.items():
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), original)

    def test_saved_equal_flag_keeps_original_tolerance_for_small_nonzero_difference(self):
        self.batch(4, "2025-01-01 00:47", [("ETH", 1 + 1e-13, 1, "true"), ("BTC", 1, 0, "FALSE")])
        result = self.run_summary()
        self.assertEqual(result["differing_asset_values"], 1)
        self.assertEqual(result["unique_differing_assets"], 1)
        self.assertEqual(result["equal_flag_files"], {"saved": 1, "recomputed": 0})

    def test_older_csv_without_equal_uses_same_tolerance_and_keeps_nonfinite_differences(self):
        self.batch(4, "2025-01-01 00:47", [("SMALL", 1 + 1e-13, 1, None), ("NAN", 0, float("nan"), None),
                                         ("LARGE", 1.01, 1, None)], flags=False)
        result = self.run_summary()
        self.assertEqual(result["differing_asset_values"], 2)
        self.assertEqual(result["non_finite_asset_values"], 1)
        self.assertEqual(result["equal_flag_files"], {"saved": 0, "recomputed": 1})

    def test_failed_batch_files_are_not_counted_and_incomplete_scope_is_reported(self):
        self.batch(4, "2025-01-01 00:47", [("ETH", 1, 1, True)])
        self.batch(8, "2025-01-01 01:47", [("ETH", 2, 1, False)], status="failed")
        self.json(self.root / "summary.json", {"status": "failed", "requested_hours": 2, "full_diff": 0})
        result = self.run_summary()
        self.assertEqual(result["compared_hours"], 1)
        self.assertEqual(result["differing_asset_values"], 0)
        self.assertEqual(len(result["skipped_files"]), 1)
        self.assertEqual(len(result["warnings"]), 2)

    def test_weight_only_is_separate_and_summary_disagreement_is_visible(self):
        self.batch(4, "2025-01-01 00:47", [("ETH", 2, 1, False)], comparison="weight_only")
        self.json(self.root / "summary.json", {"status": "completed", "requested_hours": 1, "weight_only_diff": 0})
        result = self.run_summary(comparison="weight_only")
        self.assertEqual(result["differing_asset_values"], 1)
        self.assertEqual(len(result["warnings"]), 1)
        self.assertTrue((self.root / "diff_stats_weight_only/summary.json").is_file())

    def test_repeated_time_or_asset_is_rejected_instead_of_double_counting(self):
        self.batch(4, "2025-01-01 00:47", [("ETH", 2, 1, False)])
        self.batch(8, "2025-01-01 00:47", [("ETH", 2, 1, False)])
        with self.assertRaisesRegex(ValueError, "重复对账"):
            self.run_summary()

    def test_all_pass_still_saves_empty_detail_and_zero_asset_counts(self):
        self.batch(4, "2025-01-01 00:47", [("ETH", 1, 1, True)])
        result = self.run_summary()
        self.assertEqual(result["differing_asset_values"], 0)
        self.assertEqual(result["unique_differing_assets"], 0)
        self.assertTrue(pd.read_csv(self.root / "diff_stats_full/diff_details.csv").empty)
        stats = pd.read_csv(self.root / "diff_stats_full/asset_diff_summary.csv")
        self.assertEqual(stats.iloc[0]["max_abs_difference_on_diff"], 0)

    def test_information_denominator_uses_union_counts_zero_scores_and_exact_windows(self):
        for number, call, btc in ((4, "2025-01-01 00:47", (5, 0)), (8, "2025-01-01 01:47", (8, 2))):
            self.batch(number, call, [("ETH.USDT.UFUTURE.BINANCE", 0, 0, True),
                                      ("BTC.USDT.UFUTURE.BINANCE", btc[0], btc[1], False),
                                      ("ADA.USDT.UFUTURE.BINANCE", 0, 0, True),
                                      ("EMPTY.USDT.UFUTURE.BINANCE", 0, 0, True)])
        data = self.event_data([
            ("2024-12-31 15:45", "old-left", "ETH", "1", "1"),
            ("2024-12-31 16:45", "old-boundary", "ETH", "1", "1"),
            ("2024-12-31 17:10", "old-btc", "BTC", "1", "1"),
            ("2024-12-31 17:45", "old-right", "ADA", "1", "1"),
        ], {
            "2025-01-01 00:17:00": [
                ("2024-12-31 16:00", "new-btc-a", "BTC", "1", "1"),
                ("2024-12-31 16:01", "new-btc-b", "BTC", "1", "1"),
                ("2024-12-31 16:02", "new-zero", "ADA", "1", "1"),
            ],
            "2025-01-01 01:32:00": [
                ("2024-12-31 17:20", "new-btc-c", "BTC", "1", "1"),
                ("2024-12-31 17:10", "new-incomplete", "ADA", "1", "0"),
            ],
        })
        self.json(self.root / "summary.json", {"status": "completed", "requested_hours": 2, "full_diff": 2})
        originals = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.root.rglob("*") if p.is_file()}
        result = self.run_summary(scope="with_events", data_dir=data)
        self.assertEqual(result["all_compared_asset_values"], 8)
        self.assertEqual(result["compared_asset_values"], 5)
        self.assertEqual(result["differing_asset_values"], 2)
        self.assertEqual(result["diff_rate_pct"], 40)
        self.assertEqual(result["excluded_without_events"], 3)
        self.assertEqual(result["compared_hours"], 2)
        self.assertEqual(result["warnings"], [])
        target = self.root / "diff_stats_full_with_events"
        points = pd.read_csv(target / "compared_values.csv")
        self.assertEqual(len(points.loc[points["asset"].str.startswith("ADA")]), 1)
        self.assertTrue(points.loc[points["asset"].str.startswith("ADA"), "new"].eq(0).all())
        first_btc = points.loc[points["asset"].str.startswith("BTC")].iloc[0]
        self.assertEqual(first_btc["old_event_rows"], 0)
        self.assertEqual(first_btc["new_event_rows"], 2)
        stats = pd.read_csv(target / "asset_diff_summary.csv", index_col="asset")
        self.assertEqual(stats.at["ADA.USDT.UFUTURE.BINANCE", "compared_points"], 1)
        for path, original in originals.items():
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), original)

    def test_future_event_update_does_not_change_earlier_information_presence(self):
        for number, call in ((4, "2025-01-01 00:47"), (8, "2025-01-01 01:47")):
            self.batch(number, call, [("ETH", 0, 0, True)])
        data = self.event_data([], {
            "2025-01-01 00:17:00": [("2024-12-31 16:00", "updated", "ETH", "1", "1")],
            "2025-01-01 01:02:00": [("2024-12-31 16:00", "updated", "ETH", "1", "0")],
        })
        result = self.run_summary(scope="with_events", data_dir=data)
        self.assertEqual(result["compared_asset_values"], 1)
        self.assertEqual(result["hours_with_events"], 1)

    def test_weight_only_information_uses_reference_without_new_batch_tables(self):
        self.batch(4, "2025-01-01 00:47", [("ETH", 0, 0, True), ("EMPTY", 0, 0, True)], comparison="weight_only")
        data = self.event_data([("2024-12-31 16:00", "zero", "ETH", "1", "1")], hours=1, mode="weight")
        result = self.run_summary(comparison="weight_only", scope="with_events", data_dir=data)
        self.assertEqual(result["compared_asset_values"], 1)
        self.assertEqual(result["diff_rate_pct"], 0)
        self.assertTrue((self.root / "diff_stats_weight_only_with_events/compared_values.csv").is_file())

    def test_information_scope_requires_matching_unchanged_data_and_complete_batch_history(self):
        target = self.batch(4, "2025-01-01 00:47", [("ETH", 0, 0, True)])
        data = self.event_data([], hours=1)
        with self.assertRaisesRegex(ValueError, "DATA_DIR"):
            self.run_summary(scope="with_events")
        self.json(self.root / "run.json", {"dataset_id": "other", "mode": "full"})
        with self.assertRaisesRegex(ValueError, "dataset_id"):
            self.run_summary(scope="with_events", data_dir=data)
        self.json(self.root / "run.json", {"dataset_id": "fixture", "mode": "full"})
        reference = data / "reference_events.pkl"
        original = reference.read_bytes()
        reference.write_bytes(original + b"changed")
        with self.assertRaisesRegex(ValueError, "校验不一致"):
            self.run_summary(scope="with_events", data_dir=data)
        reference.write_bytes(original)
        missing = self.root / "batch_0002_20250101_0017/summary.json"
        self.json(missing, {"status": "failed", "call_time": "2025-01-01 00:17:00"})
        with self.assertRaisesRegex(ValueError, "缺少.*成功批次"):
            self.run_summary(scope="with_events", data_dir=data)
        self.assertTrue((target / "comparison_full.csv").is_file())

    def test_no_information_reports_no_denominator_and_retains_excluded_differences(self):
        self.batch(4, "2025-01-01 00:47", [("ETH", 2, 0, False), ("EMPTY", 0, 0, True)])
        data = self.event_data([], hours=1)
        self.json(self.root / "summary.json", {"status": "completed", "requested_hours": 1, "full_diff": 1})
        result = self.run_summary(scope="with_events", data_dir=data)
        self.assertEqual(result["compared_asset_values"], 0)
        self.assertIsNone(result["diff_rate_pct"])
        self.assertEqual(result["all_differing_asset_values"], 1)
        self.assertEqual(result["excluded_differing_asset_values"], 1)
        self.assertEqual(len(result["warnings"]), 1)
        target = self.root / "diff_stats_full_with_events"
        self.assertTrue(pd.read_csv(target / "compared_values.csv").empty)
        self.assertTrue(pd.read_csv(target / "asset_diff_summary.csv").empty)
        self.assertEqual(len(pd.read_csv(target / "excluded_diff_details.csv")), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
