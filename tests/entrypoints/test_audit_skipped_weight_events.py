"""权重跳过事件查询严格沿用名单与完整窗口；只读数据库，不解析 AI 回答。"""

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal

from common.config import EventConfig
from entrypoints.audit_skipped_weight_events import READ_COLUMNS, audit_skipped_weight_events, main
from postprocessing.weight_data import parse_weight_features, skipped_weight_events
from tests.test_weight_data import AssetTables, result_frame


class SkippedWeightEventsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix=".test-skipped-", dir=str(Path.cwd()))
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name) / "audit"
        self.settings = EventConfig("first", "result", "round2.yaml", "round3.yaml",
            start_time="2017-12-31", end_time="2017-12-31", round2_asset_list_path="assets.json")

    def backend(self, rows):
        source = result_frame(rows)
        source["event"] = ["事件{}".format(i) for i in range(len(source))]
        source["error_message"] = ["错误原因{}".format(i) for i in range(len(source))]
        return AssetTables(pd.DataFrame(), source, assets=["BTC", "ETH"])

    def test_thirteen_events_are_grouped_by_asset_and_status_and_exported_without_writes(self):
        assets = ["BTC"] * 6 + ["ETH"] * 7 + ["BTC", "SOL"]
        backend = self.backend([("2017-12-31 00:20:00.123456789", asset, "new", None, None)
                                for asset in assets])
        statuses = [("0", "1"), ("1", "0"), ("9", "1"), ("1", "9"), ("-1", "1"), ("1", "-1"), ("0", "0")]
        source = backend.tables["result"]
        source.loc[:, ["round2_status", "round3_status"]] = [statuses[i % len(statuses)] for i in range(13)] + [
            ("1", "1"), ("0", "0")]
        original = source.copy(deep=True)
        report = audit_skipped_weight_events(backend, self.settings, output_dir=self.output)
        self.assertEqual((report["selected_events"], report["successful_events"], report["skipped_events"]), (14, 1, 13))
        self.assertEqual(report["affected_assets"], [
            {"asset": "BTC", "total_events": 7, "skipped_events": 6, "successful_events": 1},
            {"asset": "ETH", "total_events": 7, "skipped_events": 7, "successful_events": 0}])
        self.assertEqual(sum(row["events"] for row in report["status_counts"]), 13)
        details = pd.read_csv(report["details_path"], dtype=str, keep_default_na=False, encoding="utf-8-sig")
        self.assertEqual(len(details), 13)
        self.assertEqual(list(details["record_id"]), ["record{}".format(i) for i in range(13)])
        self.assertEqual(details.iloc[0]["date"], "2017-12-31 00:20:00.123456789")
        self.assertEqual(details.iloc[0]["error_message"], "错误原因0")
        self.assertEqual(backend.object_reads, ["assets.json"])
        self.assertEqual(backend.column_reads, [("result", "2017-12-30", "2018-01-01", READ_COLUMNS)])
        self.assertFalse(backend.reads or backend.registers or backend.writes or backend.updates or backend.appends)
        assert_frame_equal(backend.tables["result"], original)

    def test_adjacent_day_window_matches_weight_inclusive_start_and_exclusive_end(self):
        backend = self.backend([(stamp, "BTC", "new", None, None) for stamp in (
            "2017-12-30 22:44:59.999999999", "2017-12-30 22:45:00",
            "2018-01-01 00:44:59.999999999", "2018-01-01 00:45:00")])
        backend.tables["result"]["round3_status"] = "0"
        report = audit_skipped_weight_events(backend, self.settings, output_dir=self.output)
        details = pd.read_csv(report["details_path"], dtype=str, encoding="utf-8-sig")
        self.assertEqual(report["skipped_events"], 2)
        self.assertEqual(list(details["record_id"]), ["record1", "record2"])
        self.assertEqual(report["event_window_start_inclusive"], "2017-12-30 22:45:00")
        self.assertEqual(report["event_window_end_exclusive"], "2018-01-01 00:45:00")

    def test_no_skips_emits_empty_reports_with_headers_without_parsing_responses(self):
        for rows in ([], [("2017-12-31 00:00:00", "BTC", "new", None, None)]):
            with self.subTest(rows=rows):
                backend = self.backend(rows)
                output = self.output / str(len(rows))
                report = audit_skipped_weight_events(backend, self.settings, output_dir=output)
                self.assertEqual(report["skipped_events"], 0)
                self.assertEqual(report["affected_assets"], [])
                details = pd.read_csv(report["details_path"], dtype=str, encoding="utf-8-sig")
                self.assertTrue(details.empty)
                self.assertIn("record_id", details.columns)
                self.assertTrue(pd.read_csv(report["assets_path"]).empty)
                self.assertTrue(pd.read_csv(report["statuses_path"]).empty)

    def test_invalid_scope_stops_before_reading_and_existing_reports_are_preserved(self):
        backend = self.backend([])
        for overrides in ({"start_time": None}, {"end_time": "2017-12-30"}, {"result_path": "填写地址"}):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                audit_skipped_weight_events(backend, replace(self.settings, **overrides), output_dir=self.output)
        self.assertFalse(backend.column_reads or backend.object_reads)
        self.output.mkdir()
        marker = self.output / "summary.json"
        marker.write_text("keep", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            audit_skipped_weight_events(backend, self.settings, output_dir=self.output)
        self.assertEqual(marker.read_text(encoding="utf-8"), "keep")

    def test_command_prints_asset_summary_and_uses_the_selected_config(self):
        backend = self.backend([("2017-12-31 00:00:00", "BTC", "new", None, None)])
        backend.tables["result"]["round3_status"] = "-1"
        with patch("entrypoints.audit_skipped_weight_events.load_event_config", return_value=self.settings) as load:
            with patch("builtins.print") as printed:
                report = main(["--config", "other.yaml", "--output-dir", str(self.output)], cli=backend)
        load.assert_called_once_with("other.yaml")
        self.assertEqual(json.loads(printed.call_args.args[0]), report)
        self.assertEqual(report["affected_assets"][0]["asset"], "BTC")

    def test_skip_log_lists_asset_and_status_without_changing_scores(self):
        backend = self.backend([("2017-12-31 00:20:00", asset, "new", "利好", "高")
                                for asset in ("BTC", "ETH", "BTC")])
        source = backend.tables["result"]
        source.loc[source.index[1], "round2_status"] = "0"
        source.loc[source.index[2], "round3_status"] = "-1"
        with self.assertLogs("postprocessing.weight_data", level="INFO") as logs:
            features = parse_weight_features(source)
        self.assertEqual(len(features), 1)
        self.assertEqual(len(skipped_weight_events(source)), 2)
        text = "\n".join(logs.output)
        self.assertIn("跳过未成功事件=2", text)
        self.assertIn("按资产及两轮状态统计", text)
        self.assertIn("'asset': 'BTC', 'round2_status': '1', 'round3_status': '-1'", text)
        self.assertIn("'asset': 'ETH', 'round2_status': '0', 'round3_status': '1'", text)


if __name__ == "__main__":
    unittest.main()
