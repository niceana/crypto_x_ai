"""验证持仓时机、跨月时段归属、贡献分母与价格边界，不用月均仓位近似收益。"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from postprocessing.return_contribution import analyze_monthly_contributions, analyze_price_contributions


class ReturnContributionTests(unittest.TestCase):
    def test_equal_monthly_mean_positions_can_have_different_contributions(self):
        index = pd.date_range("2024-01-01", periods=3, freq="h")
        prices = pd.DataFrame({"BTC": [100.0, 120.0, 120.0]}, index=index)
        early = pd.DataFrame({"BTC": [0.5, 0.0, 0.0]}, index=index)
        late = pd.DataFrame({"BTC": [0.0, 0.5, 0.0]}, index=index)
        before_prices, before_weights = prices.copy(deep=True), early.copy(deep=True)
        a = analyze_price_contributions(early, prices)["monthly_assets"].iloc[0]
        b = analyze_price_contributions(late, prices)["monthly_assets"].iloc[0]
        self.assertEqual(a["mean_weight"], b["mean_weight"])
        self.assertAlmostEqual(a["asset_return"], 0.2)
        self.assertAlmostEqual(a["return_contribution"], 0.2)
        self.assertEqual(b["return_contribution"], 0)
        pd.testing.assert_frame_equal(prices, before_prices)
        pd.testing.assert_frame_equal(early, before_weights)

    def test_asset_return_compounds_while_contributions_remain_arithmetic(self):
        index = pd.date_range("2024-01-01", periods=3, freq="h")
        prices = pd.DataFrame({"BTC": [100.0, 110.0, 121.0]}, index=index)
        weights = pd.DataFrame({"BTC": [1.0, 1.0, 999.0]}, index=index)
        report = analyze_price_contributions(weights, prices)
        row = report["monthly_assets"].iloc[0]
        self.assertEqual(row["mean_weight"], 1.0)
        self.assertAlmostEqual(row["asset_return"], 0.21)
        self.assertAlmostEqual(row["return_contribution"], 0.2)
        self.assertAlmostEqual(report["monthly_summary"].iloc[0]["portfolio_return_arithmetic"], 0.2)
        self.assertTrue(report["metadata"]["final_weight_snapshot_excluded"])

    def test_month_end_hour_uses_previous_snapshot_and_belongs_to_interval_start_month(self):
        index = pd.date_range("2024-01-31 23:00", periods=3, freq="h", tz="Asia/Shanghai")
        prices = pd.DataFrame({"BTC": [100.0, 110.0, 121.0]}, index=index)
        weights = pd.DataFrame({"BTC": [1.0, 2.0, 99.0]}, index=index)
        report = analyze_price_contributions(weights, prices)
        rows = report["monthly_assets"].set_index("month")
        self.assertAlmostEqual(rows.loc["2024-01", "return_contribution"], 0.1)
        self.assertAlmostEqual(rows.loc["2024-02", "return_contribution"], 0.1)
        self.assertEqual(rows.loc["2024-01", "mean_weight"], 1.0)
        self.assertEqual(rows.loc["2024-02", "mean_weight"], 1.0)
        self.assertTrue(report["monthly_summary"]["is_full_month"].eq(False).all())

    def test_full_leap_month_includes_next_month_midnight_price(self):
        index = pd.date_range("2024-02-01", "2024-03-01", freq="h")
        prices = pd.DataFrame({"BTC": np.ones(len(index)) * 100}, index=index)
        prices.iloc[-1, 0] = 120
        weights = pd.DataFrame({"BTC": np.ones(len(index) - 1) * 0.5}, index=index[:-1])
        report = analyze_price_contributions(weights, prices, start_time="2024-02-01", end_time="2024-02-29")
        row = report["monthly_summary"].iloc[0]
        self.assertEqual(row["observed_hours"], 29 * 24)
        self.assertTrue(row["is_full_month"])
        self.assertAlmostEqual(row["portfolio_return_arithmetic"], 0.2)
        with self.assertRaisesRegex(ValueError, "价格表缺少"):
            analyze_price_contributions(weights, prices.iloc[:-1], start_time="2024-02-01", end_time="2024-02-29")

    def test_ranking_sides_top_five_and_concentration_use_explicit_denominators(self):
        start = pd.date_range("2024-01-01", periods=1, freq="h")
        assets = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "ZERO"]
        weights = pd.DataFrame(1.0, index=start, columns=assets)
        returns = pd.DataFrame([[.10, .08, .06, .04, .02, .01, -.20, -.10, -.05, 0.0]],
                               index=start + pd.Timedelta(hours=1), columns=assets)
        report = analyze_monthly_contributions(weights, returns)
        ranking = report["monthly_rankings"].set_index("asset")
        self.assertEqual(ranking.index[ranking["is_top_positive"]].tolist(), ["A", "B", "C", "D", "E"])
        self.assertFalse(ranking.loc["F", "is_top_positive"])
        self.assertEqual(ranking.loc["G", "negative_rank"], 1)
        self.assertTrue(pd.isna(ranking.loc["ZERO", "positive_rank"]))
        self.assertAlmostEqual(ranking.loc["A", "share_of_net_return"], .1 / -.04)
        self.assertAlmostEqual(ranking["share_of_net_return"].sum(), 1)
        summary = report["monthly_summary"].iloc[0]
        self.assertAlmostEqual(summary["portfolio_return_arithmetic"], -.04)
        self.assertAlmostEqual(summary["top3_positive_share"], .24 / .31)
        self.assertAlmostEqual(summary["top3_absolute_contribution_share"], .40 / .66)
        self.assertEqual(summary["largest_negative_asset"], "G")

    def test_zero_net_and_all_zero_contributions_do_not_create_infinite_shares_or_false_leaders(self):
        start = pd.date_range("2024-01-01", periods=1, freq="h")
        weights = pd.DataFrame({"A": [1.0], "B": [1.0]}, index=start)
        returns = pd.DataFrame({"A": [.1], "B": [-.1]}, index=start + pd.Timedelta(hours=1))
        report = analyze_monthly_contributions(weights, returns)
        self.assertTrue(report["monthly_rankings"]["share_of_net_return"].isna().all())
        self.assertFalse(report["monthly_summary"].iloc[0]["net_return_share_defined"])
        self.assertTrue(report["monthly_summary"].iloc[0]["majority_positive_asset"])
        flat = analyze_monthly_contributions(weights * 0, returns)
        row = flat["monthly_summary"].iloc[0]
        self.assertEqual(row["largest_positive_asset"], "")
        self.assertEqual(row["largest_negative_asset"], "")
        self.assertTrue(pd.isna(row["top3_absolute_contribution_share"]))
        self.assertFalse(flat["monthly_rankings"]["is_top_positive"].any())

    def test_short_positions_and_leverage_are_preserved_without_normalization(self):
        start = pd.date_range("2024-01-01", periods=1, freq="h")
        weights = pd.DataFrame({"SHORT": [-.5], "LONG": [2.0]}, index=start)
        returns = pd.DataFrame({"LONG": [.1], "SHORT": [-.1]}, index=start + pd.Timedelta(hours=1))
        rows = analyze_monthly_contributions(weights, returns)["monthly_assets"].set_index("asset")
        self.assertAlmostEqual(rows.loc["SHORT", "return_contribution"], .05)
        self.assertAlmostEqual(rows.loc["LONG", "return_contribution"], .2)
        self.assertEqual(rows.loc["LONG", "mean_weight"], 2)

    def test_asset_summary_uses_hourly_positions_and_identifies_peak_short_month(self):
        starts = pd.date_range("2024-01-31 23:00", periods=4, freq="h")
        weights = pd.DataFrame({"A": [.2, -.8, -.8, -.8], "ZERO": 0.0}, index=starts)
        returns = pd.DataFrame({"A": [.1, -.02, .03, .04], "ZERO": 0.0}, index=starts + pd.Timedelta(hours=1))
        before = weights.copy(deep=True)
        report = analyze_monthly_contributions(weights, returns)
        summary = report["asset_summary"].set_index("asset")
        row = summary.loc["A"]
        self.assertAlmostEqual(row["mean_weight"], -.55)
        self.assertAlmostEqual(row["mean_absolute_weight"], .65)
        self.assertAlmostEqual(row["total_asset_return"], 1.1 * .98 * 1.03 * 1.04 - 1)
        self.assertAlmostEqual(row["total_return_contribution"], -.02)
        self.assertEqual(row["max_weight_month"], "2024-02")
        self.assertAlmostEqual(row["max_month_mean_weight"], -.8)
        self.assertAlmostEqual(row["max_month_mean_absolute_weight"], .8)
        self.assertAlmostEqual(row["max_weight_month_asset_return"], .98 * 1.03 * 1.04 - 1)
        self.assertAlmostEqual(row["max_weight_month_return_contribution"], -.04)
        self.assertEqual(row["best_contribution_month"], "2024-01")
        self.assertEqual(row["worst_contribution_month"], "2024-02")
        self.assertEqual(row["observed_hours"], 4)
        self.assertAlmostEqual(row["positive_month_share"], .5)
        self.assertEqual(summary.loc["ZERO", "max_weight_month"], "")
        pd.testing.assert_frame_equal(weights, before)
        tied = weights.copy()
        tied.iloc[0, 0] = .8
        tied_summary = analyze_monthly_contributions(tied, returns)["asset_summary"].set_index("asset")
        self.assertEqual(tied_summary.loc["A", "max_weight_month"], "2024-01")

    def test_total_price_return_uses_database_endpoints_even_when_month_boundary_is_invalid(self):
        index = pd.date_range("2024-01-31 23:00", periods=3, freq="h")
        weights = pd.DataFrame({"FLAT": 0.0, "HELD": 1.0}, index=index[:-1])
        prices = pd.DataFrame({"FLAT": [100.0, 0.0, 110.0], "HELD": 100.0}, index=index)
        report = analyze_price_contributions(weights, prices)
        self.assertTrue(report["monthly_assets"].loc[report["monthly_assets"]["asset"].eq("FLAT"), "asset_return"].isna().all())
        row = report["asset_summary"].set_index("asset").loc["FLAT"]
        self.assertAlmostEqual(row["total_asset_return"], .1)
        self.assertTrue(row["total_asset_return_available"])
        self.assertEqual(row["total_return_contribution"], 0)

    def test_asset_intersection_precedes_normalization_preserves_shorts_and_keeps_flat_hour(self):
        index = pd.date_range("2024-01-01", periods=3, freq="h")
        signals = pd.DataFrame({"LONG": [3.0, 0.0, 0.0], "NO_PRICE": [1000.0, np.nan, 0.0],
                                "SHORT": [-1.0, 0.0, 0.0]}, index=index)
        prices = pd.DataFrame({"SHORT": [100.0, 90.0, 45.0], "UNUSED": [10.0, 20.0, 30.0],
                               "LONG": [100.0, 110.0, 220.0]}, index=index)
        before_signals, before_prices = signals.copy(deep=True), prices.copy(deep=True)
        report = analyze_price_contributions(signals, prices)
        rows = report["monthly_assets"].set_index("asset")
        self.assertEqual(rows.index.tolist(), ["LONG", "SHORT"])
        self.assertAlmostEqual(rows.loc["LONG", "mean_weight"], .375)
        self.assertAlmostEqual(rows.loc["SHORT", "mean_weight"], -.125)
        self.assertAlmostEqual(rows.loc["LONG", "return_contribution"], .075)
        self.assertAlmostEqual(rows.loc["SHORT", "return_contribution"], .025)
        scaled = analyze_price_contributions(signals * 100, prices)
        pd.testing.assert_frame_equal(report["monthly_assets"], scaled["monthly_assets"])
        self.assertIn("sum(abs(weight))", report["metadata"]["normalization"])
        self.assertEqual(report["metadata"]["valid_assets"], ["LONG", "SHORT"])
        self.assertEqual(report["metadata"]["excluded_weight_assets"], ["NO_PRICE"])
        pd.testing.assert_frame_equal(signals, before_signals)
        pd.testing.assert_frame_equal(prices, before_prices)

    def test_zero_quotes_exclude_prelisting_weights_then_asset_can_enter_after_valid_price(self):
        index = pd.date_range("2024-01-01", periods=4, freq="h")
        weights = pd.DataFrame({"NEW": 3.0, "OLD": 1.0}, index=index[:-1])
        prices = pd.DataFrame({"NEW": [0.0, 0.0, 10.0, 11.0], "OLD": [100.0, 110.0, 121.0, 133.1]}, index=index)
        before = prices.copy(deep=True)
        report = analyze_price_contributions(weights, prices)
        rows = report["monthly_assets"].set_index("asset")
        self.assertAlmostEqual(rows.loc["NEW", "mean_weight"], .25)
        self.assertAlmostEqual(rows.loc["OLD", "mean_weight"], .75)
        self.assertAlmostEqual(rows.loc["NEW", "return_contribution"], .075)
        self.assertAlmostEqual(rows.loc["OLD", "return_contribution"], .225)
        self.assertTrue(pd.isna(rows.loc["NEW", "asset_return"]))
        self.assertFalse(rows.loc["NEW", "asset_return_available"])
        self.assertEqual(rows.loc["NEW", "valid_return_hours"], 1)
        self.assertEqual(rows.loc["NEW", "missing_return_hours"], 2)
        self.assertAlmostEqual(rows.loc["OLD", "asset_return"], .331)
        self.assertEqual(report["metadata"]["unavailable_entry_price_hours_by_asset"], {"NEW": 2})
        totals = report["asset_summary"].set_index("asset")
        self.assertFalse(totals.loc["NEW", "total_asset_return_available"])
        self.assertTrue(pd.isna(totals.loc["NEW", "total_asset_return"]))
        self.assertAlmostEqual(totals.loc["NEW", "total_return_contribution"], .075)
        pd.testing.assert_frame_equal(prices, before)

    def test_nan_weights_mean_zero_before_normalizing_and_all_nan_hour_stays_flat(self):
        index = pd.date_range("2024-01-01", periods=3, freq="h")
        weights = pd.DataFrame({"LONG": [3.0, np.nan, np.nan],
                                "FLAT": pd.Series([pd.NA] * 3, index=index, dtype="Float64")}, index=index)
        prices = pd.DataFrame({"LONG": [100.0, 110.0, 220.0], "FLAT": [100.0, 90.0, 180.0]}, index=index)
        before = weights.copy(deep=True)
        report = analyze_price_contributions(weights, prices)
        rows = report["monthly_assets"].set_index("asset")
        self.assertAlmostEqual(rows.loc["LONG", "mean_weight"], .5)
        self.assertAlmostEqual(rows.loc["LONG", "return_contribution"], .1)
        self.assertEqual(rows.loc["FLAT", "mean_absolute_weight"], 0)
        self.assertEqual(rows.loc["FLAT", "return_contribution"], 0)
        self.assertEqual(rows.loc["LONG", "observed_hours"], 2)
        self.assertEqual(report["metadata"]["nan_weight_cells_filled"], 3)
        pd.testing.assert_frame_equal(weights, before)

    def test_low_level_nan_position_is_zero_while_infinite_positions_still_fail(self):
        index = pd.date_range("2024-01-01", periods=2, freq="h")
        weights = pd.DataFrame({"BTC": [np.nan, .5]}, index=index)
        returns = pd.DataFrame({"BTC": [.1, .2]}, index=index + pd.Timedelta(hours=1))
        report = analyze_monthly_contributions(weights, returns)
        row = report["monthly_assets"].iloc[0]
        self.assertAlmostEqual(row["mean_weight"], .25)
        self.assertAlmostEqual(row["return_contribution"], .1)
        self.assertEqual(report["metadata"]["nan_weight_cells_filled"], 1)
        prices = pd.DataFrame({"BTC": [100.0, 110.0, 120.0]}, index=pd.date_range(index[0], periods=3, freq="h"))
        for value in (np.inf, -np.inf):
            with self.subTest(value=value):
                invalid = weights.copy()
                invalid.iloc[0, 0] = value
                with self.assertRaisesRegex(ValueError, "非有限"):
                    analyze_monthly_contributions(invalid, returns)
                with self.assertRaisesRegex(ValueError, "非有限"):
                    analyze_price_contributions(invalid, prices)

    def test_invalid_entry_quote_is_filtered_per_hour_without_removing_later_valid_hours(self):
        index = pd.date_range("2024-01-01", periods=3, freq="h")
        weights = pd.DataFrame({"NEW": 3.0, "OLD": 1.0}, index=index[:-1])
        for value in (0.0, -1.0, np.nan, np.inf):
            with self.subTest(value=value):
                prices = pd.DataFrame({"NEW": [value, 100.0, 110.0], "OLD": 100.0}, index=index)
                rows = analyze_price_contributions(weights, prices)["monthly_assets"].set_index("asset")
                self.assertAlmostEqual(rows.loc["NEW", "mean_weight"], .375)
                self.assertAlmostEqual(rows.loc["OLD", "mean_weight"], .625)
                self.assertAlmostEqual(rows.loc["NEW", "return_contribution"], .075)

    def test_all_zero_quotes_leave_cash_and_undefined_asset_return(self):
        index = pd.date_range("2024-01-01", periods=3, freq="h")
        weights = pd.DataFrame({"A": 100.0, "B": -50.0}, index=index[:-1])
        prices = pd.DataFrame({"A": 0.0, "B": 0.0}, index=index)
        report = analyze_price_contributions(weights, prices)
        self.assertTrue(report["monthly_assets"]["mean_absolute_weight"].eq(0).all())
        self.assertTrue(report["monthly_assets"]["asset_return"].isna().all())
        self.assertEqual(report["monthly_summary"].iloc[0]["portfolio_return_arithmetic"], 0)
        self.assertEqual(report["metadata"]["unavailable_entry_price_asset_hours"], 4)

    def test_unheld_price_gap_does_not_hide_known_month_boundary_return(self):
        index = pd.date_range("2024-01-01", periods=3, freq="h")
        weights = pd.DataFrame({"FLAT": 0.0, "HELD": 1.0}, index=index[:-1])
        prices = pd.DataFrame({"FLAT": [100.0, 0.0, 110.0], "HELD": 100.0}, index=index)
        rows = analyze_price_contributions(weights, prices)["monthly_assets"].set_index("asset")
        self.assertEqual(rows.loc["FLAT", "return_contribution"], 0)
        self.assertAlmostEqual(rows.loc["FLAT", "asset_return"], .1)
        self.assertTrue(rows.loc["FLAT", "asset_return_available"])
        self.assertEqual(rows.loc["FLAT", "missing_return_hours"], 2)

    def test_nan_prices_forward_fill_each_gap_from_history_without_mutating_input(self):
        index = pd.date_range("2024-01-31 23:00", periods=5, freq="h")
        prices = pd.DataFrame({"BTC": [100.0, np.nan, np.nan, 110.0, np.nan]}, index=index)
        weights = pd.DataFrame({"BTC": 1.0}, index=index[1:-1])
        before = prices.copy(deep=True)
        report = analyze_price_contributions(weights, prices)
        row = report["monthly_assets"].iloc[0]
        self.assertAlmostEqual(row["mean_weight"], 1)
        self.assertAlmostEqual(row["return_contribution"], .1)
        self.assertAlmostEqual(row["asset_return"], .1)
        self.assertEqual(row["valid_return_hours"], 3)
        self.assertEqual(row["missing_return_hours"], 0)
        self.assertEqual(report["metadata"]["nan_price_cells_filled"], 3)
        pd.testing.assert_frame_equal(prices, before)

    def test_missing_weight_or_price_hour_cannot_be_replaced_by_previous_available_row(self):
        index = pd.date_range("2024-01-01", periods=4, freq="h")
        weights = pd.DataFrame({"BTC": 1.0}, index=index)
        prices = pd.DataFrame({"BTC": [100.0, 110.0, 121.0, 133.1]}, index=index)
        for missing in index[:2]:
            with self.subTest(missing=missing), self.assertRaisesRegex(ValueError, "期初持仓时间行.*NaN"):
                analyze_price_contributions(weights.drop(missing), prices, start_time="2024-01-01")
        with self.assertRaisesRegex(ValueError, "价格表缺少"):
            analyze_price_contributions(weights, prices.drop(index[1]))
        returns = prices.pct_change(fill_method=None).iloc[1:].drop(index[2])
        with self.assertRaisesRegex(ValueError, "小时收益缺少"):
            analyze_monthly_contributions(weights, returns)

    def test_bad_prices_missing_assets_and_timezone_mismatch_fail_explicitly(self):
        index = pd.date_range("2024-01-01", periods=2, freq="h")
        weights = pd.DataFrame({"BTC": 1.0}, index=index)
        for values in ([100, np.inf], [100, 0], [100, -1]):
            with self.subTest(values=values), self.assertRaisesRegex(ValueError, "已持仓资产"):
                analyze_price_contributions(weights, pd.DataFrame({"BTC": values}, index=index))
        with self.assertRaisesRegex(ValueError, "没有共同资产"):
            analyze_price_contributions(weights, pd.DataFrame({"ETH": [100, 101]}, index=index))
        with self.assertRaisesRegex(ValueError, "时区"):
            analyze_price_contributions(weights, pd.DataFrame({"BTC": [100, 101]}, index=index.tz_localize("UTC")))

    def test_entrypoint_reads_next_day_price_and_exports_monthly_and_asset_tables(self):
        from entrypoints import audit_return_contribution as entry
        from matplotlib.figure import Figure
        index = pd.date_range("2024-01-31", "2024-02-01", freq="h")
        weights = pd.DataFrame({"BTC": .5, "ETH": 0.0, "NO_PRICE": 100.0}, index=index[:-1])
        weights.iloc[0, weights.columns.get_loc("ETH")] = np.nan
        prices = pd.DataFrame({"ETH": 200.0, "BTC": np.r_[np.ones(24) * 100, 120], "UNUSED": 1.0}, index=index)
        prices.iloc[5, prices.columns.get_loc("BTC")] = np.nan
        calls = []

        class Reader:
            def read(self, path, **kwargs):
                calls.append((path, kwargs))
                if path == "actual_weights":
                    return weights.copy()
                return prices.loc[:, kwargs["columns"]].copy() if "columns" in kwargs else prices.copy()

        with tempfile.TemporaryDirectory(prefix=".test-return-contribution-", dir=Path.cwd()) as temporary, patch.multiple(
                entry, WEIGHT_PATH="actual_weights", PRICE_PATH="close_price_60min",
                START_TIME="2024-01-31", END_TIME="2024-01-31", OUTPUT_DIR=Path(temporary),
                FIGURE_DIR=Path(temporary) / "figures"), patch.object(
                    Figure, "show", side_effect=AssertionError("服务器审计不能弹出窗口")):
            with patch("pandas.read_csv", side_effect=AssertionError("数据库审计不应读取已有 CSV")):
                report = entry.main([], cli=Reader())
            self.assertEqual(calls, [("actual_weights", {"start_time": "2024-01-31", "end_time": "2024-01-31"}),
                                     ("close_price_60min", {"start_time": "2024-01-31", "end_time": "2024-02-01"})])
            self.assertAlmostEqual(report["monthly_summary"].iloc[0]["portfolio_return_arithmetic"], .2)
            self.assertEqual(report["monthly_assets"]["asset"].tolist(), ["BTC", "ETH"])
            self.assertEqual(len(list(Path(temporary).glob("*.csv"))), 4)
            self.assertEqual({name for name, value in report.items() if isinstance(value, pd.DataFrame)},
                             {"monthly_assets", "monthly_rankings", "monthly_summary", "asset_summary"})
            exported = pd.read_csv(Path(temporary) / "asset_summary.csv").set_index("asset")
            self.assertAlmostEqual(exported.loc["BTC", "total_return_contribution"], .2)
            self.assertAlmostEqual(exported.loc["BTC", "total_asset_return"], .2)
            for name in ("return_contribution_positions.png", "return_contribution_assets.png", "return_contribution_summary.png"):
                image = (Path(temporary) / "figures" / name).read_bytes()
                self.assertTrue(image.startswith(b"\x89PNG\r\n\x1a\n"), name)
                self.assertGreater(len(image), 1000, name)
            metadata = json.loads((Path(temporary) / "method.json").read_text(encoding="utf-8"))
            self.assertEqual(metadata["price_path"], "close_price_60min")
            self.assertEqual(metadata["excluded_weight_assets"], ["NO_PRICE"])
            self.assertEqual(metadata["valid_assets"], ["BTC", "ETH"])
            self.assertEqual(metadata["nan_weight_cells_filled"], 1)
            self.assertEqual(metadata["nan_price_cells_filled"], 1)
            self.assertFalse(metadata["costs_included"])
            self.assertEqual(metadata["contribution_linking"], "none")

    def test_invalid_cli_dates_stop_before_database_access(self):
        from entrypoints.audit_return_contribution import main
        with patch("sys.stderr"), self.assertRaises(SystemExit):
            main(["--weight-path", "actual_weights", "--start-time", "2024-02-30", "--end-time", "2024-03-01"], cli=object())


if __name__ == "__main__":
    unittest.main()
