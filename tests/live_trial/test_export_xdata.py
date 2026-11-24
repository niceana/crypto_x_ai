"""导出工具专项验证；全部数据库调用使用内存假客户端。"""
import argparse
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORK))
sys.path.insert(0, str(WORK / "tools/live_trial"))
import pandas as pd
from pandas.testing import assert_frame_equal
import export_xdata as export
import check_weight
from crypto_x_ai_live import _compat as core
import _trial


class ReadOnlyClient:
    def __init__(self):
        self.calls = []
        self.assets = ["ETH.USDT.UFUTURE.BINANCE"]
        lower = pd.Timestamp("2025-01-01 08:47") - pd.Timedelta(days=100)
        values = [(lower - pd.Timedelta(nanoseconds=1), "too_old", "ETH", "1"),
                  (lower, "history_lower", "ETH", "1"),
                  ("2025-01-01 07:44:59", "before_window", "ETH", "1"),
                  ("2025-01-01 07:45", "window_lower", "ETH", "1"),
                  ("2025-01-01 08:29:59", "history_last", "ETH", "1"),
                  ("2025-01-01 08:30", "11", "ETH", "1"),
                  ("2025-01-01 08:44:59", "12", "ETH", "1"),
                  ("2025-01-01 08:45", "at_cutoff", "ETH", "0"),
                  ("2025-01-01 08:46", "after_cutoff", "ETH", "0"),
                  ("2025-01-01 08:00", "irrelevant", "XRP", "0")]
        keys, rows = [], []
        response = json.dumps({"ai_response_content": json.dumps({"direction": "利好", "materiality": "中",
                                                                 "execution_stage": "已完成", "persistence": "长期"})})
        for date, post_id, asset, status in values:
            record = post_id + ":0:" + asset
            keys.append((pd.Timestamp(date), record))
            row = dict.fromkeys(export.EVENT_COLUMNS, "")
            row.update(event_id=record, event="event " + post_id, asset=asset, source_post_id=post_id,
                       event_relation="new", round2_status=status, round3_status=status, round3_ai_response=response,
                       embedding=json.dumps({"kept_for": record, "vector": [1.0, 0.0]}))
            rows.append(row)
        self.events = pd.DataFrame(rows, index=pd.MultiIndex.from_tuples(keys, names=["date", "record_id"])).sort_index()
        source = [("2025-01-01 08:29:59", "before"), ("2025-01-01 08:30", 11),
                  ("2025-01-01 08:44:59", "12"), ("2025-01-01 08:45", "at_end"),
                  ("2025-01-01 08:46", "future")]
        self.posts = pd.DataFrame({"text": ["post " + str(value[1]) for value in source], "user_name": ["user"] * len(source)},
                                  index=pd.MultiIndex.from_tuples([(pd.Timestamp(date), post_id) for date, post_id in source], names=["date", "id"]))
        self.old = pd.DataFrame({"ETH.USDT.UFUTURE.BINANCE": [0.0, 60.0, 0.0]},
                                index=pd.DatetimeIndex(["2025-01-01 16:00", "2025-01-01 17:00", "2025-01-01 18:00"], name="date"))

    def read(self, path, **options):
        self.calls.append((path, options))
        # 故意返回范围外记录，验证工具自身的精确边界筛选。
        return {"events": self.events, "posts": self.posts, "old": self.old}[path].copy(deep=True)

    def read_object(self, path):
        self.calls.append((path, "read_object"))
        return json.dumps({"InstrumentList": self.assets}).encode("utf-8")


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.cli = ReadOnlyClient()
        self.args = argparse.Namespace(test_time="2025-01-01 16:47:00", mode="all", posts_path="posts",
                                        events_path="events", old_weight_path="old", assets_path="assets",
                                        method="sum", weight_param_id=None, output_dir=str(self.root))

    def run_export(self):
        with redirect_stdout(io.StringIO()):
            return export.export_data(self.cli, self.args)

    def load(self, name):
        return pd.read_pickle(self.root / (name + ".pkl"))

    def compare_exported_weight(self):
        argv = ["check_weight.py", "--config-dir", str(WORK / "configs"),
                "--method", self.args.method, "--test-time", self.args.test_time,
                "--old-weight", str(self.root / "old_weight.pkl"), "--events", str(self.root / "events_completed.pkl"),
                "--assets", str(self.root / "assets.json")]
        with patch.object(_trial, "ROOT", self.root), patch.object(sys, "argv", argv), redirect_stdout(io.StringIO()):
            code = check_weight.main()
        output = max((self.root / "outputs").glob("weight_*"), key=lambda item: item.name)
        metadata = json.loads((output / "summary.json").read_text(encoding="utf-8"))
        comparison = pd.read_csv(output / "comparison.csv", index_col="asset")
        return code, metadata, comparison

    def test_full_boundaries_and_frozen_history_roundtrip(self):
        original = self.cli.events.copy(deep=True)
        info = self.run_export()
        posts, history, events, old = [self.load(name) for name in ("posts", "history_before", "events_completed", "old_weight")]
        self.assertEqual(posts["id"].tolist(), ["11", "12"])
        self.assertEqual(posts["created_at"].tolist(), ["2025-01-01T08:30:00+00:00", "2025-01-01T08:44:59+00:00"])
        self.assertEqual(posts.index.name, "date")
        self.assertTrue((posts.index == pd.Timestamp("2025-01-01 16:45")).all())
        self.assertFalse(posts.index.is_unique)
        self.assertEqual(history["source_post_id"].tolist(), ["history_lower", "before_window", "window_lower", "history_last"])
        self.assertEqual(events["source_post_id"].tolist(), ["window_lower", "history_last", "11", "12"])
        self.assertEqual(list(old.index), [pd.Timestamp("2025-01-01 17:00")])
        assert_frame_equal(history, original.loc[history.index])
        assert_frame_equal(self.cli.events, original)
        self.assertEqual(info["rows"], {"events_completed": 4, "posts": 2, "history_before": 4, "old_weight": 1})
        source_call = next(options for path, options in self.cli.calls if path == "posts")
        self.assertEqual(source_call, {"start_time": "2025-01-01", "end_time": "2025-01-01", "columns": ["text", "user_name"]})
        self.assertEqual(json.loads((self.root / "assets.json").read_text()), ["ETH.USDT.UFUTURE.BINANCE"])

    def test_exported_events_match_actual_weight_scorer_for_both_methods(self):
        self.run_export()
        config = _trial.score_parameters(WORK / "configs", None)
        for method, expected in (("sum", 60.0), ("max_abs", 15.0)):
            weight = core.weights.build_live_weight(self.load("events_completed"), as_of=pd.Timestamp(self.args.test_time),
                                                    assets=["ETH"], aggregation_method=method, parameters=config)
            self.assertEqual(weight.iloc[0, 0], expected)

    def test_weight_only_reads_no_posts_or_hundred_day_history(self):
        self.args.mode = "weight"
        self.args.posts_path = None
        self.cli.assets.append("BTC.USDT.UFUTURE.BINANCE")
        info = self.run_export()
        self.assertEqual(info["old_weight_missing_assets"], ["BTC.USDT.UFUTURE.BINANCE"])
        self.assertFalse((self.root / "posts.pkl").exists())
        self.assertFalse((self.root / "history_before.pkl").exists())
        self.assertFalse(any(path == "posts" for path, _ in self.cli.calls))
        options = next(value for path, value in self.cli.calls if path == "events")
        self.assertEqual(options, {"start_time": "2025-01-01", "end_time": "2025-01-01"})

    def test_export_keeps_full_assets_and_original_old_weight_columns(self):
        self.cli.assets.append("BTC.USDT.UFUTURE.BINANCE")
        original = self.cli.old.copy(deep=True)
        info = self.run_export()
        self.assertEqual(info["old_weight_missing_assets"], ["BTC.USDT.UFUTURE.BINANCE"])
        self.assertEqual(json.loads((self.root / "assets.json").read_text()), self.cli.assets)
        assert_frame_equal(self.load("old_weight"), original.loc[[pd.Timestamp("2025-01-01 17:00")]])
        assert_frame_equal(self.cli.old, original)

    def test_missing_old_zero_asset_passes_for_both_aggregation_methods(self):
        self.cli.assets.append("BTC.USDT.UFUTURE.BINANCE")
        for method, expected in (("sum", 60.0), ("max_abs", 15.0)):
            with self.subTest(method=method):
                self.args.method = method
                self.cli.old.loc[pd.Timestamp("2025-01-01 17:00"), "ETH.USDT.UFUTURE.BINANCE"] = expected
                self.run_export()
                code, info, comparison = self.compare_exported_weight()
                self.assertEqual(code, 0)
                self.assertEqual(info["comparison"], "PASS")
                self.assertEqual(info["missing_old_assets"], ["BTC.USDT.UFUTURE.BINANCE"])
                self.assertEqual(comparison.loc["BTC.USDT.UFUTURE.BINANCE", ["new", "old", "difference"]].tolist(), [0.0, 0.0, 0.0])
                self.assertFalse(comparison.at["BTC.USDT.UFUTURE.BINANCE", "old_column_present"])
                self.assertTrue(comparison.at["ETH.USDT.UFUTURE.BINANCE", "old_column_present"])

    def test_missing_old_asset_with_nonzero_new_weight_is_still_a_difference(self):
        self.cli.assets.append("BTC.USDT.UFUTURE.BINANCE")
        self.cli.old.columns = ["BTC.USDT.UFUTURE.BINANCE"]
        self.cli.old.loc[:, :] = 0.0
        self.run_export()
        code, info, comparison = self.compare_exported_weight()
        self.assertEqual(code, 1)
        self.assertEqual(info["comparison"], "DIFF")
        self.assertEqual(comparison.at["ETH.USDT.UFUTURE.BINANCE", "difference"], 60.0)
        self.assertFalse(comparison.at["ETH.USDT.UFUTURE.BINANCE", "old_column_present"])

    def test_nan_in_existing_old_column_is_not_filled_with_zero(self):
        self.cli.assets.append("BTC.USDT.UFUTURE.BINANCE")
        self.cli.old.loc[pd.Timestamp("2025-01-01 17:00"), "ETH.USDT.UFUTURE.BINANCE"] = float("nan")
        self.run_export()
        code, info, comparison = self.compare_exported_weight()
        self.assertEqual(code, 1)
        self.assertEqual(info["comparison"], "DIFF")
        self.assertTrue(pd.isna(comparison.at["ETH.USDT.UFUTURE.BINANCE", "old"]))
        self.assertTrue(comparison.at["ETH.USDT.UFUTURE.BINANCE", "old_column_present"])
        self.assertEqual(comparison.at["BTC.USDT.UFUTURE.BINANCE", "old"], 0.0)

    def test_utc_aware_test_time_and_midnight_mapping(self):
        self.args.test_time = "2025-01-01T08:47:00Z"
        self.assertEqual(self.run_export()["test_time"], "2025-01-01 16:47:00")
        for frame in (self.cli.events, self.cli.posts):
            dates = frame.index.get_level_values("date") - pd.Timedelta(hours=17)
            frame.index = pd.MultiIndex.from_arrays([dates, frame.index.get_level_values(1)], names=frame.index.names)
        self.cli.old.index -= pd.Timedelta(hours=17)
        self.args.test_time = "2024-12-31 23:47:00"
        info = self.run_export()
        self.assertEqual(info["old_weight_time_utc8"], "2025-01-01 00:00:00")
        self.assertEqual(self.load("posts")["id"].tolist(), ["11", "12"])
        old_options = next(value for path, value in self.cli.calls[::-1] if path == "old")
        self.assertEqual(old_options, {"start_time": "2025-01-01", "end_time": "2025-01-01"})

    def test_bad_history_fails_instead_of_dropping_rows_or_reembedding(self):
        for column, value, message in (("round2_status", "0", "历史 B"), ("round3_status", 1, "历史 B"),
                                       ("embedding", "", "缺失向量")):
            with self.subTest(column=column):
                self.cli = ReadOnlyClient()
                key = (pd.Timestamp("2025-01-01 07:44:59"), "before_window:0:ETH")
                self.cli.events.at[key, column] = value
                with self.assertRaisesRegex(ValueError, message):
                    self.run_export()
                self.assertFalse((self.root / "posts.pkl").exists())

    def test_bad_window_missing_weight_and_empty_posts_stop_export(self):
        with self.subTest("unfinished window"):
            key = (pd.Timestamp("2025-01-01 08:30"), "11:0:ETH")
            self.cli.events.at[key, "round3_status"] = "-1"
            with self.assertRaisesRegex(ValueError, "weight 窗口"):
                self.run_export()
        with self.subTest("missing old row"):
            self.cli = ReadOnlyClient()
            self.cli.old = self.cli.old.drop(pd.Timestamp("2025-01-01 17:00"))
            with self.assertRaisesRegex(ValueError, "缺少"):
                self.run_export()
        with self.subTest("no posts"):
            self.cli = ReadOnlyClient()
            self.cli.posts = self.cli.posts.iloc[:0]
            with self.assertRaisesRegex(ValueError, "没有帖子"):
                self.run_export()
        self.assertFalse((self.root / "old_weight.pkl").exists())

    def test_partial_hour_is_rejected_before_any_read(self):
        self.args.test_time = "2025-01-01 16:17"
        with self.assertRaisesRegex(ValueError, "完整小时"):
            self.run_export()
        self.assertEqual(self.cli.calls, [])

    def test_no_arguments_uses_top_settings_and_unfilled_paths_fail_before_read(self):
        values = dict(TEST_TIME=self.args.test_time, POSTS_PATH="posts", EVENTS_PATH="events", OLD_WEIGHT_PATH="old",
                      ASSETS_PATH="assets", METHOD="sum", MODE="all", WEIGHT_PARAM_ID=None, OUTPUT_DIR=self.root)
        with patch.multiple(export, **values), patch.object(sys, "argv", ["export_xdata.py"]), redirect_stdout(io.StringIO()):
            info = export.main(cli=self.cli)
            self.assertEqual(info["test_time"], self.args.test_time)
            self.assertEqual(self.load("posts")["id"].tolist(), ["11", "12"])
            self.cli.calls.clear()
            with patch.object(export, "EVENTS_PATH", "填写旧第二三轮结果表路径"):
                with self.assertRaisesRegex(ValueError, "脚本顶部.*EVENTS_PATH"):
                    export.main(cli=self.cli)
                self.assertEqual(self.cli.calls, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
