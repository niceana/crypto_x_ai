"""按帖比较已保存的结果，忽略所有解释和请求元数据。"""
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

import pandas as pd

WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORK))
sys.path.insert(0, str(WORK / "tools/live_trial"))
from compare_post_results import compare_posts


def impact(direction="利好", reason="old reason"):
    return json.dumps({"ai_response_content": json.dumps(dict(direction=direction, materiality="中", execution_stage="已完成",
                                                              persistence="长期", materiality_reason=reason)),
                       "usage_json": {"tokens": 100}, "response_created": "old time"})


class PostResultTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="post-result-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.run = self.root / "run"
        self.data = self.root / "data"
        self.batch = self.run / "batch_0001_20250101_0002"
        self.batch.mkdir(parents=True)
        self.data.mkdir()
        self.json(self.run / "run.json", {"mode": "full", "dataset_id": "fixture"})
        self.json(self.data / "manifest.json", {"dataset_id": "fixture", "call_count": 1, "source_paths": {"POSTS_PATH": "oldtasks"}})
        self.json(self.data / "assets.json", ["ETH.USDT.UFUTURE.BINANCE"])
        self.json(self.batch / "summary.json", {"status": "success", "call_time": "2025-01-01 00:02"})
        date = pd.Timestamp("2024-12-31 15:52")
        keys = [(date, "p{}".format(i)) for i in range(4)]
        self.old_posts = pd.DataFrame(dict(status=["1"] * 4, text=["post " + key[1] for key in keys], user_name=["user"] * 4,
                                           ai_response_content=[json.dumps({"events": [{"event": "event " + key[1], "tickers": ["ETH"]}]}) for key in keys]),
                                      index=pd.MultiIndex.from_tuples(keys, names=["date", "id"]))
        self.new_posts = self.old_posts.copy(deep=True)
        rows = [dict(source_post_id=key[1], event="event " + key[1], asset="ETH", event_id=key[1] + ":0:ETH", event_relation="new",
                     new_information="", round2_status="1", round3_status="1", round3_ai_response=impact()) for key in keys]
        self.old_events = pd.DataFrame(rows, index=pd.MultiIndex.from_tuples([(date, key[1] + ":0:ETH") for key in keys], names=["date", "record_id"]))
        self.new_events = self.old_events.copy(deep=True)
        self.snapshot = self.root / "old_round1.pkl"

    def json(self, path, value):
        path.write_text(json.dumps(value), encoding="utf-8")

    def save(self):
        self.old_posts.to_pickle(self.snapshot)
        self.new_posts.to_pickle(self.batch / "round1_results.pkl")
        self.old_events.to_pickle(self.data / "reference_events.pkl")
        self.new_events.to_pickle(self.batch / "event_results.pkl")

    def compare(self, **kwargs):
        self.save()
        with redirect_stdout(io.StringIO()):
            return compare_posts(self.run, self.data, old_round1_pkl=kwargs.pop("old_round1_pkl", self.snapshot), **kwargs)

    def test_explanations_new_information_and_request_metadata_do_not_count(self):
        self.new_posts["usage_json"] = '{"tokens": 999}'
        self.new_posts["reasoning_content"] = "different reasoning"
        self.new_posts["response_created"] = "new time"
        self.new_events["new_information"] = "different explanation"
        self.new_events["round3_ai_response"] = impact(reason="changed reason")
        result = self.compare()
        self.assertEqual(result["changed_posts"], 0)
        self.assertEqual(result["completely_compared_posts"], 4)
        self.assertTrue(pd.read_csv(self.run / "ai_result_diff/changed_posts.csv").empty)

    def test_counts_unique_changed_posts_and_skips_downstream_when_events_changed(self):
        self.new_posts.iloc[0, self.new_posts.columns.get_loc("ai_response_content")] = json.dumps({"events": [{"event": "different extraction", "tickers": ["ETH"]}]})
        self.new_events.iloc[0, self.new_events.columns.get_loc("event")] = "different extraction"
        self.new_events.iloc[1, self.new_events.columns.get_loc("event_relation")] = "update"
        self.new_events.iloc[1, self.new_events.columns.get_loc("event_id")] = "historical-root"
        self.new_events.iloc[2, self.new_events.columns.get_loc("round3_ai_response")] = impact(direction="利空", reason="do not compare this")
        result = self.compare()
        self.assertEqual(result["changed_posts"], 3)
        self.assertEqual(result["round1_changed_posts"], 1)
        self.assertEqual(result["event_set_changed_posts"], 1)
        self.assertEqual(result["round2_changed_posts"], 1)
        self.assertEqual(result["round3_changed_posts"], 1)
        self.assertEqual(result["round2_comparable_posts"], 3)
        self.assertEqual(result["changed_rate_pct"], 75)
        details = (self.run / "ai_result_diff/changed_posts.csv").read_text(encoding="utf-8-sig")
        self.assertNotIn("do not compare this", details)
        self.assertNotIn("old reason", details)

    def test_event_and_ticker_order_changes_do_not_create_false_group_changes(self):
        self.old_posts = self.old_posts.iloc[:1].copy()
        self.new_posts = self.new_posts.iloc[:1].copy()
        self.old_posts.iloc[0, self.old_posts.columns.get_loc("ai_response_content")] = json.dumps({"events": [
            {"event": "A", "tickers": ["ETH", "BTC"]}, {"event": "B", "tickers": ["ETH"]}]})
        self.new_posts.iloc[0, self.new_posts.columns.get_loc("ai_response_content")] = json.dumps({"events": [
            {"event": "B", "tickers": ["eth"]}, {"event": "A", "tickers": ["BTC", "ETH"]}]})
        self.old_events = pd.concat([self.old_events.iloc[:1], self.old_events.iloc[:1]])
        date = self.old_events.index[0][0]
        self.old_events.index = pd.MultiIndex.from_tuples([(date, "p0:0:ETH"), (date, "p0:1:ETH")], names=["date", "record_id"])
        self.old_events["event"] = ["A", "B"]
        self.old_events["event_id"] = ["p0:0:ETH", "p0:1:ETH"]
        self.new_events = self.old_events.copy(deep=True)
        self.new_events["event"] = ["B", "A"]
        result = self.compare()
        self.assertEqual(result["changed_posts"], 0)
        self.assertEqual(result["round2_changed_posts"], 0)

    def test_missing_old_first_round_and_invalid_labels_are_not_treated_as_unchanged(self):
        self.old_posts = self.old_posts.iloc[1:].copy()
        self.new_events.iloc[1, self.new_events.columns.get_loc("round3_ai_response")] = "bad JSON"
        result = self.compare()
        self.assertEqual(result["issue_count"], 2)
        self.assertEqual(result["incomplete_comparison_posts"], 2)
        self.assertEqual(result["round1_comparable_posts"], 3)
        self.assertEqual(result["round3_comparable_posts"], 3)

    def test_round2_and_round3_can_be_compared_without_loading_old_round1(self):
        self.new_events.iloc[0, self.new_events.columns.get_loc("round3_ai_response")] = impact(direction="利空")
        cli = Mock()
        cli.read.side_effect = AssertionError("must not read xdata")
        result = self.compare(compare_round1=False, cli=cli)
        self.assertFalse(result["round1_compared"])
        self.assertEqual(result["round3_changed_posts"], 1)
        cli.read.assert_not_called()

    def test_old_round1_read_once_and_saved_snapshot_can_be_reused(self):
        self.save()
        cli = Mock()
        cli.read.return_value = self.old_posts
        with redirect_stdout(io.StringIO()):
            first = compare_posts(self.run, self.data, cli=cli)
        cli.read.assert_called_once_with("oldtasks", start_time="2024-12-31", end_time="2024-12-31",
                                         columns=["status", "text", "user_name", "ai_response_content"])
        saved = self.run / "ai_result_diff/old_round1_snapshot.pkl"
        self.assertEqual(list(pd.read_pickle(saved).index.names), ["date", "id"])
        with redirect_stdout(io.StringIO()):
            second = compare_posts(self.run, self.data, old_round1_pkl=saved)
        self.assertEqual(first["changed_posts"], second["changed_posts"])

    def test_changed_original_input_is_reported_separately(self):
        self.old_posts.iloc[0, self.old_posts.columns.get_loc("text")] = "different original input"
        result = self.compare()
        self.assertEqual(result["input_changed_posts"], 1)
        self.assertEqual(result["changed_posts"], 0)
        self.assertEqual(result["incomplete_comparison_posts"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
