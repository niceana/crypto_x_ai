"""验证集中度、缺失时间、实际映射的零分归因和同资产小时抵消。"""

import builtins
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from postprocessing.signal_diagnostics import analyze_events, analyze_weight
from postprocessing.weight_data import ASSET_SUFFIX
from postprocessing.weight_parameters import WeightParameters
from tests.test_weight_data import result_frame


def events():
    frame = result_frame([
        ("2023-01-01 00:10:00", "BTC", "new", "利好", "低"),
        ("2023-01-01 00:20:00", "BTC", "new", "利空", "低"),
        ("2023-01-01 00:30:00", "ETH", "new", "利好", "高"),
        ("2023-01-01 00:40:00", "ETH", "new", "利空", "低"),
        ("2023-01-01 01:00:00", "BTC", "new", "中性", "不确定"),
        ("2023-01-01 01:10:00", "ETH", "repeat", "利好", "不确定"),
    ])
    frame["round2_ai_response"] = [json.dumps({"ai_response_content": json.dumps({"event_relation": value})})
                                    for value in ("new", "new", "uncertain", "new", "new", "repeat")]
    frame.iloc[0, frame.columns.get_loc("round2_ai_response")] = ""
    return frame


class WeightDiagnosticsTests(unittest.TestCase):
    def test_heatmap_without_new_matplotlib_apis_preserves_global_colormap(self):
        import matplotlib
        matplotlib.use("Agg")
        from matplotlib import cm, colors, dates, pyplot as plt
        from postprocessing.signal_diagnostic_plots import _heatmap

        real_import = builtins.__import__

        def older_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "matplotlib" and "colormaps" in (fromlist or ()):
                raise ImportError("旧版 Matplotlib 不导出 colormaps")
            return real_import(name, globals, locals, fromlist, level)

        frame = pd.DataFrame({"BTC": [np.nan, 1.0]},
                             index=pd.date_range("2023-01-01", periods=2, freq="h"))
        original_bad = cm.RdBu_r(np.ma.masked)
        figure, axis = plt.subplots()
        try:
            with patch("builtins.__import__", side_effect=older_import), patch.object(
                    colors.Colormap, "with_extremes", side_effect=AttributeError("旧版无此方法"), create=True), patch.dict(
                    dates.__dict__):
                dates.__dict__.pop("ConciseDateFormatter", None)
                heat = _heatmap(axis, frame, ["BTC"])
                figure.canvas.draw()
            self.assertTrue(heat.get_array().mask[0, 0])
            np.testing.assert_allclose(heat.cmap(np.ma.masked), colors.to_rgba("#e8ebef"))
            np.testing.assert_allclose(cm.RdBu_r(np.ma.masked), original_bad)
        finally:
            plt.close(figure)

    def test_counts_concentration_empty_hour_and_no_mutation(self):
        frame = pd.DataFrame({"BTC": [3, 0, 1], "ETH": [-1, 0, -1]},
                             index=pd.date_range("2023-01-01", periods=3, freq="h"))
        original = frame.copy(deep=True)
        report = analyze_weight(frame)
        hourly = report["hourly"]
        self.assertEqual(list(hourly["active_assets"]), [2, 0, 2])
        self.assertAlmostEqual(hourly.iloc[0]["top1_share"], 0.75)
        self.assertAlmostEqual(hourly.iloc[0]["effective_assets"], 1.6)
        self.assertEqual(hourly.iloc[1]["dominant_asset"], "")
        self.assertTrue(np.isnan(hourly.iloc[1]["top1_share"]))
        self.assertEqual(report["flat_periods"].iloc[0]["hours"], 1)
        pd.testing.assert_frame_equal(frame, original)

    def test_missing_hour_is_not_counted_as_flat_or_connected_to_next_flat(self):
        frame = pd.DataFrame({"BTC": [0, 0]}, index=pd.to_datetime(["2023-01-01 00:00", "2023-01-01 02:00"]))
        report = analyze_weight(frame)
        self.assertEqual(report["summary"]["missing_hours"], 1)
        self.assertEqual(report["summary"]["longest_flat_hours"], 1)
        self.assertEqual(len(report["flat_periods"]), 2)
        for value in (float("nan"), float("inf")):
            with self.assertRaisesRegex(ValueError, "NaN"):
                analyze_weight(frame.replace(0, value))

    def test_threshold_changes_measurement_without_mutating_input(self):
        frame = pd.DataFrame({"BTC": [1e-12], "ETH": [1.0]}, index=pd.date_range("2023-01-01", periods=1, freq="h"))
        self.assertEqual(analyze_weight(frame)["hourly"].iloc[0]["active_assets"], 2)
        report = analyze_weight(frame, epsilon=1e-8)
        self.assertEqual(report["hourly"].iloc[0]["active_assets"], 1)
        self.assertEqual(report["hourly"].iloc[0]["top1_share"], 1)
        self.assertEqual(frame.iloc[0, 0], 1e-12)


class EventDiagnosticsTests(unittest.TestCase):
    def test_cancellation_is_per_asset_and_hour_and_uses_write_time(self):
        report = analyze_events(events())
        pairs = report["asset_hours"]
        btc = pairs.loc[(pd.Timestamp("2023-01-01 09:00"), "BTC")]
        eth = pairs.loc[(pd.Timestamp("2023-01-01 09:00"), "ETH")]
        self.assertTrue(btc["fully_cancelled"])
        self.assertEqual(btc["cancellation_ratio"], 1)
        self.assertEqual(eth["net_score"], 4)
        self.assertTrue(eth["partially_cancelled"])
        self.assertEqual(eth["cancellation_ratio"], 0.5)
        self.assertAlmostEqual(report["hourly"].iloc[0]["cancellation_ratio"], 2 / 3)

    def test_actual_nonzero_uncertain_mapping_not_misattributed_and_overlap_explicit(self):
        report = analyze_events(events(), parameters=WeightParameters(mappings={"materiality": {"不确定": 2}}))
        self.assertEqual(report["summary"]["zero_events"], 1)
        self.assertEqual(list(report["zero_causes"].index), ["direction=中性"])
        default = analyze_events(events())
        self.assertEqual(default["summary"]["zero_events"], 2)
        self.assertEqual(int(default["zero_causes"]["count"].sum()), 3)
        self.assertEqual(int(default["zero_combinations"]["count"].sum()), 2)

    def test_round2_uncertain_auto_new_and_repeat_scopes_remain_distinct(self):
        report = analyze_events(events())
        table = report["distributions"].set_index(["field", "scope", "value"])
        self.assertEqual(table.loc[("round2_ai_relation", "round2_completed", "uncertain"), "count"], 1)
        self.assertEqual(table.loc[("round2_ai_relation", "round2_completed", "automatic_new"), "count"], 1)
        self.assertEqual(table.loc[("direction", "all_disclosures", "利好"), "count"], 3)
        self.assertEqual(table.loc[("direction", "new_update", "利好"), "count"], 2)

    def test_opposite_assets_do_not_cancel_and_zero_coefficient_is_reported(self):
        source = events().iloc[[0, 3]].copy()
        report = analyze_events(source)
        self.assertEqual(report["hourly"].iloc[0]["cancellation_ratio"], 0)
        zero = analyze_events(source, parameters=WeightParameters(B=0))
        self.assertEqual(zero["summary"]["zero_events"], 2)
        self.assertEqual(zero["zero_causes"].loc["B=0", "count"], 2)
        self.assertEqual(zero["summary"]["fully_cancelled_asset_hours"], 0)

    def test_entrypoint_reads_only_configured_tables_and_source_boundary_day(self):
        from entrypoints import plot_signal_diagnostics as entry
        weight = pd.DataFrame({"BTC" + ASSET_SUFFIX: [0.0, 2.0]},
                              index=pd.date_range("2023-01-01", periods=2, freq="h"))
        source = events().iloc[:1].copy()
        source.index = pd.MultiIndex.from_tuples([("2022-12-31 16:10:00", "before_year")], names=["date", "record_id"])
        calls = []

        class Reader:
            def read(self, path, **kwargs):
                calls.append((path, kwargs))
                return weight.copy() if path == "weights" else source.copy()

        with tempfile.TemporaryDirectory(prefix=".test-diagnostics-", dir=Path.cwd()) as temporary, patch.multiple(
                entry, WEIGHT_PATH="weights", RESULT_PATH="results", START_TIME="2023-01-01",
                END_TIME="2023-01-01", OUTPUT_DIR=Path(temporary), TOP_N=1):
            report = entry.main(Reader())
            self.assertEqual([call[0] for call in calls], ["weights", "results"])
            self.assertEqual(calls[1][1]["start_time"], "2022-12-31")
            self.assertEqual(report["events"]["event_scores"].iloc[0]["target_hour"], pd.Timestamp("2023-01-01 01:00"))
            for name in ("weight_overview.png", "weight_hourly.csv", "event_distributions.png",
                         "event_zero_cancellation.png", "event_zero_combinations.csv"):
                self.assertTrue((Path(temporary) / name).exists(), name)


if __name__ == "__main__":
    unittest.main()
