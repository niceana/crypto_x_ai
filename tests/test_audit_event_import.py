"""离线核对来源版本冲突及后续影响，禁止调用数据库写入接口。"""

import json
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from entrypoints.audit_event_import import audit_event_import, main
from initialization.event_records import expand_first_round
from tests.event_support import EventFixture, Tables, source_posts


class AuditEventImportTests(EventFixture, unittest.TestCase):
    def readonly(self, source, existing):
        backend = Tables(source, existing)
        for name in ("register", "write", "update_by_column", "update_by_index", "read_object"):
            setattr(backend, name, Mock(side_effect=AssertionError("核对不能调用 " + name)))
        return backend

    def test_mixed_answer_versions_report_all_conflicts_and_future_completed_records(self):
        source = source_posts([
            ("2024-03-06", "old empty", ["ETH"], "a"),
            ("2024-03-07", "old shared", ["BTC"], "a"),
            ("2024-03-08", "old changed asset", ["SOL"], "a"),
            ("2024-04-01", "future ETH", ["ETH"], "a"),
            ("2024-04-02", "future BTC", ["BTC"], "a"),
            ("2024-03-05", "new historical record", ["ETH"], "a"),
        ])
        existing = expand_first_round(source.iloc[:5])
        existing.loc[:, ["round2_status", "round3_status"]] = "1"
        existing.loc[:, ["round2_ai_response", "round3_ai_response", "embedding"]] = "preserve saved values"
        source.at[source.index[0], "ai_response_content"] = json.dumps({"events": []})
        source.at[source.index[1], "ai_response_content"] = json.dumps(
            {"events": [{"event": "new shared", "tickers": ["BTC"]}]})
        source.at[source.index[2], "ai_response_content"] = json.dumps(
            {"events": [{"event": "old changed asset", "tickers": ["ETH"]}]})
        backend = self.readonly(source, existing)
        output = self.path.parent / "audit"
        summary = audit_event_import(backend, self.settings(
            start_time="2024-03-01", end_time="2024-03-31", round2_asset_list_path="unused-company-list"), output)
        for name, value in {"source_posts": 4, "expected_records": 3, "existing_compared": 3,
                            "extra_records": 2, "shared_records": 1, "input_conflict_records": 1,
                            "missing_records": 2, "missing_before_completed_round2": 2,
                            "conflict_posts": 3, "potential_recompute_assets": 3,
                            "source_snapshot_records": 6, "event_snapshot_records": 5}.items():
            self.assertEqual(summary[name], value, name)
        self.assertEqual(summary["extra_kinds"], {"post_now_has_no_asset_events": 1, "event_id_changed": 1})
        self.assertFalse(summary["database_modified"])
        pd.testing.assert_frame_equal(pd.read_pickle(output / "first_round_snapshot.pkl"), source)
        pd.testing.assert_frame_equal(pd.read_pickle(output / "event_snapshot.pkl"), existing)
        pd.testing.assert_frame_equal(backend.tables["first"], source)
        pd.testing.assert_frame_equal(backend.tables["result"], existing)
        changed = pd.read_csv(output / "input_conflicts.csv")
        self.assertEqual(list(changed["old_event"]), ["old shared"])
        self.assertEqual(list(changed["new_event"]), ["new shared"])
        impact = pd.read_csv(output / "potential_recompute.csv").set_index("asset")
        self.assertEqual(impact.at["ETH", "potential_recompute_from_date"], "2024-03-05")
        self.assertEqual(impact.at["ETH", "round2_completed_from_date"], 2)
        self.assertEqual(impact.at["BTC", "round3_completed_from_date"], 2)
        self.assertEqual(backend.reads, [("first", None, None), ("result", None, None)])
        self.assertEqual(json.loads((output / "summary.json").read_text(encoding="utf-8")), summary)

    def test_same_id_different_date_type_is_reported_without_reindexing(self):
        source = source_posts([("2024-03-06 15:10:24", "unchanged", ["ETH"], "a")])
        existing = expand_first_round(source)
        existing.index = pd.MultiIndex.from_tuples([(str(key[0]), key[1]) for key in existing.index],
                                                  names=existing.index.names)
        backend = self.readonly(source, existing)
        output = self.path.parent / "audit"
        summary = audit_event_import(backend, self.settings(), output)
        self.assertEqual(summary["extra_kinds"], {"date_mismatch": 1})
        self.assertEqual(summary["input_conflict_records"], 0)
        pd.testing.assert_frame_equal(backend.tables["result"], existing)

    def test_successful_only_scope_preserves_all_rows_in_the_snapshots(self):
        source = source_posts([("2024-03-06", "ready", ["ETH"], "a"),
                               ("2024-03-07", "old failed", ["BTC"], "a")])
        existing = expand_first_round(source)
        source.at[source.index[1], "status"] = "-1"
        source.at[source.index[1], "ai_response_content"] = "invalid output"
        backend = self.readonly(source, existing)
        output = self.path.parent / "audit"
        summary = audit_event_import(backend, self.settings(), output, successful_only=True)
        self.assertEqual((summary["source_posts"], summary["existing_compared"], summary["extra_records"]), (1, 1, 0))
        self.assertEqual(summary["potential_recompute_assets"], 0)
        pd.testing.assert_frame_equal(pd.read_pickle(output / "first_round_snapshot.pkl"), source)
        pd.testing.assert_frame_equal(pd.read_pickle(output / "event_snapshot.pkl"), existing)

    def test_strict_invalid_source_stops_without_creating_a_report(self):
        source = source_posts([("2024-03-06", "not ready", ["ETH"], "a")])
        existing = expand_first_round(source)
        source.at[source.index[0], "status"] = "0"
        backend = self.readonly(source, existing)
        output = self.path.parent / "audit"
        with self.assertRaisesRegex(ValueError, "第一轮尚未成功"):
            audit_event_import(backend, self.settings(), output)
        self.assertFalse(output.exists())
        self.assertEqual(backend.reads, [("first", None, None)])

    def test_cli_uses_injected_client_and_never_overwrites_an_existing_report(self):
        source = source_posts([("2024-03-06", "ready", ["ETH"], "a")])
        existing = expand_first_round(source)
        backend = self.readonly(source, existing)
        output = self.path.parent / "audit"
        with patch("entrypoints.audit_event_import.load_event_config", return_value=self.settings()), \
                patch("entrypoints.audit_event_import.logging.basicConfig"):
            summary = main(["--output-dir", str(output)], cli=backend)
        self.assertEqual((summary["extra_records"], summary["missing_records"], summary["input_conflict_records"]), (0, 0, 0))
        original = (output / "summary.json").read_bytes()
        with self.assertRaises(FileExistsError):
            audit_event_import(backend, self.settings(), output)
        self.assertEqual((output / "summary.json").read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
