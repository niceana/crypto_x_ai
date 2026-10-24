"""验证仅按结果表 asset 统计，不依赖第二、三轮状态或事件组。"""

import unittest

import pandas as pd

from postprocessing.asset_frequency import asset_frequency


class AssetFrequencyTests(unittest.TestCase):
    def frame(self):
        return pd.DataFrame({
            "asset": ["ETH", "ETH", "BTC", "ETH", "eth", "", None],
            "event_id": ["e1", "e1", "b1", "", "e2", "x", "y"],
            "round2_status": ["1", "1", "1", "0", "1", "1", "1"],
            "round3_status": ["1", "1", "0", "0", "1", "1", "1"],
        })

    def test_disclosures_keep_repeats_and_original_asset_names_without_mutating_df(self):
        frame = self.frame()
        before = frame.copy(deep=True)
        result = asset_frequency(frame)
        self.assertEqual(result["count"].to_dict(), {"ETH": 3, "BTC": 1, "eth": 1})
        self.assertAlmostEqual(result.loc["ETH", "share"], 3 / 5)
        self.assertEqual(result["cumulative_share"].iloc[-1], 1)
        self.assertEqual(result.attrs["excluded_asset_rows"], 2)
        pd.testing.assert_frame_equal(frame, before)

    def test_only_asset_column_is_required_even_before_round2_and_round3(self):
        frame = self.frame()
        expected = asset_frequency(frame[["asset"]])
        frame["event_id"] = ""
        frame["round2_status"] = ["0", "9", "-1", None, "1", "invalid", "0"]
        frame["round3_status"] = "0"
        pd.testing.assert_frame_equal(asset_frequency(frame), expected)
        self.assertEqual(expected["count"].to_dict(), {"ETH": 3, "BTC": 1, "eth": 1})

    def test_empty_results_and_missing_columns_are_explicit(self):
        result = asset_frequency(pd.DataFrame({"asset": [None, " "]}))
        self.assertTrue(result.empty)
        self.assertEqual(result.attrs["counted_total"], 0)
        with self.assertRaisesRegex(ValueError, "asset"):
            asset_frequency(pd.DataFrame({"event_id": ["e1"]}))
