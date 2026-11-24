"""按帖子＋资产验证最终评分比较，不要求事件文本或数量相同。"""
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd

WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORK))
sys.path.insert(0, str(WORK / "tools/live_trial"))
from compare_post_weights import compare_post_weights


def impact(direction="利好", reason="unused"):
    return json.dumps({"ai_response_content": json.dumps(dict(direction=direction, materiality="中", execution_stage="已完成",
                                                              persistence="长期", materiality_reason=reason))})


class PostWeightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="post-weights-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.run, self.data = self.root / "run", self.root / "data"
        self.batch = self.run / "batch_0001"
        self.batch.mkdir(parents=True)
        self.data.mkdir()
        self.date = pd.Timestamp("2025-01-01 08:05")
        self.manifest = dict(dataset_id="fixture", call_count=1, aggregation_method="sum", weight_param_id=None)
        self.json(self.run / "run.json", dict(mode="full", dataset_id="fixture"))
        self.json(self.batch / "summary.json", dict(status="success"))
        self.json(self.data / "assets.json", ["ETH", "BTC"])
        self.posts = pd.DataFrame(dict(text=["post0", "post1", "post2"], user_name=["user"] * 3),
                                   index=pd.MultiIndex.from_tuples([(self.date, "p" + str(i)) for i in range(3)], names=["date", "id"]))
        self.old = self.events([(i, 0, "ETH", "利好") for i in range(3)])
        self.new = self.old.copy(deep=True)

    def json(self, path, value):
        path.write_text(json.dumps(value), encoding="utf-8")

    def events(self, specs):
        rows, keys = [], []
        for number, event_number, asset, direction in specs:
            post_id = "p" + str(number)
            keys.append((self.date, "{}:{}:{}".format(post_id, event_number, asset)))
            rows.append(dict(source_post_id=post_id, asset=asset, event="event " + post_id, event_relation="new",
                             round2_status="1", round3_status="1", round3_ai_response=impact(direction)))
        return pd.DataFrame(rows, columns=["source_post_id", "asset", "event", "event_relation", "round2_status", "round3_status", "round3_ai_response"],
                            index=pd.MultiIndex.from_tuples(keys, names=["date", "record_id"]))

    def compare(self):
        self.json(self.data / "manifest.json", self.manifest)
        self.posts.to_pickle(self.batch / "round1_results.pkl")
        self.new.to_pickle(self.batch / "event_results.pkl")
        self.old.to_pickle(self.data / "reference_events.pkl")
        with redirect_stdout(io.StringIO()):
            return compare_post_weights(self.run, self.data, WORK / "configs")

    def details(self):
        return pd.read_csv(self.run / "post_weight_diff/post_asset_weights.csv", index_col=["post_id", "asset"])

    def test_changed_event_wording_and_explanation_do_not_reduce_comparable_pairs(self):
        self.new["event"] = ["rewritten one", "rewritten two", "rewritten three"]
        self.new["round3_ai_response"] = impact(reason="different explanation")
        result = self.compare()
        self.assertEqual(result["comparable_pairs"], 3)
        self.assertEqual(result["equal_pairs"], 3)
        self.assertEqual(result["diff_pairs"], 0)

    def test_different_event_counts_compare_aggregated_sum_or_max_abs(self):
        self.old = self.events([(0, 0, "ETH", "利好"), (0, 1, "ETH", "利好")])
        self.new = self.events([(0, 0, "ETH", "利好")])
        for method, old_weight, diffs in (("sum", 30.0, 1), ("max_abs", 15.0, 0)):
            with self.subTest(method=method):
                self.manifest["aggregation_method"] = method
                result = self.compare()
                self.assertEqual(result["comparable_pairs"], 1)
                self.assertEqual(result["diff_pairs"], diffs)
                row = self.details().loc[("p0", "ETH")]
                self.assertEqual(row["old_weight"], old_weight)
                self.assertEqual(row["new_weight"], 15.0)
                self.assertEqual(row["old_event_rows"], 2)
                self.assertEqual(row["new_event_rows"], 1)

    def test_same_post_multiple_changed_assets_count_one_changed_post(self):
        self.old = self.events([(0, 0, "ETH", "利好"), (0, 0, "BTC", "利空")])
        self.new = self.events([(0, 0, "ETH", "利空"), (0, 0, "BTC", "利好")])
        result = self.compare()
        self.assertEqual(result["diff_pairs"], 2)
        self.assertEqual(result["changed_posts"], 1)
        self.assertEqual(result["changed_assets"], 2)

    def test_old_only_and_new_only_pairs_are_separate_and_future_posts_excluded(self):
        self.old = self.events([(0, 0, "ETH", "利好"), (1, 0, "ETH", "利好"), (99, 0, "ETH", "利好")])
        self.new = self.events([(0, 0, "ETH", "利好"), (1, 0, "BTC", "利好")])
        result = self.compare()
        self.assertEqual(result["common_pairs"], 1)
        self.assertEqual(result["old_only_pairs"], 1)
        self.assertEqual(result["new_only_pairs"], 1)
        self.assertEqual(result["old_pairs"], 2)
        self.assertEqual(result["diff_pairs"], 0)
        self.assertTrue(pd.isna(self.details().loc[("p1", "ETH"), "new_weight"]))

    def test_failed_or_invalid_events_are_not_silently_scored_as_zero(self):
        self.new.iloc[0, self.new.columns.get_loc("round3_status")] = "-1"
        self.new.iloc[1, self.new.columns.get_loc("round3_ai_response")] = "bad JSON"
        result = self.compare()
        self.assertEqual(result["common_pairs"], 3)
        self.assertEqual(result["comparable_pairs"], 1)
        self.assertEqual(result["invalid_common_pairs"], 2)
        self.assertEqual(result["issue_count"], 2)
        self.assertEqual(result["diff_pairs"], 0)

    def test_max_abs_ties_keep_sign_using_original_stable_record_order(self):
        self.manifest["aggregation_method"] = "max_abs"
        self.old = self.events([(0, 0, "ETH", "利好"), (0, 1, "ETH", "利空")])
        self.new = self.events([(0, 0, "ETH", "利空"), (0, 1, "ETH", "利好")])
        result = self.compare()
        self.assertEqual(result["diff_pairs"], 1)
        row = self.details().loc[("p0", "ETH")]
        self.assertEqual(row["old_weight"], 15.0)
        self.assertEqual(row["new_weight"], -15.0)

    def test_no_asset_events_returns_zero_comparisons_not_false_changes(self):
        self.old = self.old.iloc[:0]
        self.new = self.new.iloc[:0]
        result = self.compare()
        self.assertEqual(result["comparable_pairs"], 0)
        self.assertEqual(result["diff_pairs"], 0)
        self.assertIsNone(result["diff_rate_pct"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
