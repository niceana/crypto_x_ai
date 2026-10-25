"""验证只读观察的候选来源、时间边界、阈值统计及本地导出，不访问真实服务。"""

import csv
import json
import math
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from common.config import EventConfig
from round2.similarity_audit import READ_COLUMNS, export_similarity_audit


def saved(vector, **changes):
    body = dict(model="bge-m3", model_version="v1", config_hash="a" * 64, vector=vector)
    body.update(changes)
    return json.dumps(body)


def records(*items):
    keys, rows = [], []
    for when, record_id, vector, group, relation, asset in items:
        keys.append((pd.Timestamp(when), record_id))
        rows.append(dict(asset=asset, event="事件正文：" + record_id, event_id=group,
                         event_relation=relation, new_information="新增事实" if relation == "update" else "",
                         round2_status="1", embedding=saved(vector)))
    index = pd.MultiIndex.from_tuples(keys, names=["date", "record_id"])
    return pd.DataFrame(rows, index=index, columns=READ_COLUMNS)


class ReadOnlyClient:
    """故意只有 read，任何数据库写入或元数据查询都会失败。"""

    def __init__(self, frame):
        self.frame = frame.copy(deep=True)
        self.reads = []

    def read(self, path, start_time, end_time, columns):
        self.reads.append((path, start_time, end_time, list(columns)))
        frame = self.frame.loc[[start_time <= key[0].date().isoformat() <= end_time for key in self.frame.index]]
        return frame[columns].copy(deep=True)


class SimilarityAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.settings = EventConfig("first", "result", "unused-round2.yaml", "unused-round3.yaml",
                                    start_time="2021-01-03", end_time="2021-01-03")

    def export(self, frame, name="report", **kwargs):
        client = ReadOnlyClient(frame)
        output = self.root / name
        manifest = export_similarity_audit(client, self.settings, output, **kwargs)
        return client, output, manifest

    def csv(self, output, name):
        with (output / name).open(encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))

    def test_group_maximum_points_to_update_record_and_never_writes_or_calls_models(self):
        frame = records(
            ("2021-01-01", "root", [1, 0], "root", "new", "ETH"),
            ("2021-01-02", "progress", [0, 1], "root", "update", "ETH"),
            ("2021-01-02 06:00:00", "other", [0.8, 0.6], "other", "new", "ETH"),
            ("2021-01-03", "current", [0, 1], "root", "update", "ETH"),
            ("2021-01-03", "same_time", [0, 1], "same_time", "new", "ETH"),
            ("2021-01-04", "future", [0, 1], "future", "new", "ETH"),
            ("2021-01-02", "btc", [0, 1], "btc", "new", "BTC"),
        )
        before = frame.copy(deep=True)
        with patch("httpx.Client", side_effect=AssertionError("不允许请求模型")), \
                patch("httpx.AsyncClient", side_effect=AssertionError("不允许请求模型")):
            client, output, manifest = self.export(frame)
        pd.testing.assert_frame_equal(frame, before)
        pd.testing.assert_frame_equal(client.frame, before)
        self.assertEqual(len(client.reads), 2)
        self.assertEqual(client.reads[0][1:3], ("2021-01-03", "2021-01-03"))
        self.assertEqual(client.reads[1][2], "2021-01-02")
        self.assertEqual(manifest["history_lookback_months"], 3)
        self.assertNotIn("history_lookback_years", manifest)
        self.assertTrue(all(call[3] == READ_COLUMNS for call in client.reads))
        self.assertEqual((manifest["events"], manifest["candidate_rows"]), (2, 4))
        current = self.csv(output, "events.csv")[0]
        self.assertEqual((current["record_id"], current["max_similarity_event_id"],
                          current["max_similarity_record_id"]), ("current", "root", "progress"))
        self.assertEqual(float(current["max_similarity"]), 1)
        self.assertEqual(current["stored_match_rank"], "1")
        self.assertIn("progress", current["max_similarity_event"])
        self.assertEqual(current["history_records"], "3")
        self.assertEqual(current["history_groups"], "2")
        self.assertEqual({pair["candidate_event_id"] for pair in self.csv(output, "candidates.csv")},
                         {"root", "other"})
        self.assertTrue((output / "events.csv").read_bytes().startswith(b"\xef\xbb\xbf"))
        self.assertEqual(json.loads((output / "manifest.json").read_text(encoding="utf-8")), manifest)

    def test_below_threshold_is_exported_and_equality_passes(self):
        frame = records(
            ("2021-01-01", "old", [0.5, math.sqrt(0.75)], "old", "new", "ETH"),
            ("2021-01-03", "now", [1, 0], "old", "repeat", "ETH"),
        )
        _, output, _ = self.export(frame, threshold=0.55, thresholds=(0.5, 0.55))
        event = self.csv(output, "events.csv")[0]
        self.assertEqual((event["recomputed_decision"], event["stored_match_similarity"]),
                         ("below_threshold_auto_new", "0.5"))
        pair = self.csv(output, "candidates.csv")[0]
        self.assertEqual((pair["passes_threshold"], pair["would_send_to_ai"], pair["is_stored_match"]),
                         ("False", "False", "True"))
        low, high = self.csv(output, "thresholds.csv")
        self.assertEqual((low["send_to_ai_events"], low["stored_matches_excluded"]), ("1", "0"))
        self.assertEqual((high["below_threshold_auto_new"], high["stored_matches_excluded"]), ("1", "1"))

    def test_no_history_has_blank_maximum_not_zero(self):
        frame = records(("2021-01-03", "only", [1, 0], "only", "new", "ETH"))
        _, output, _ = self.export(frame)
        event = self.csv(output, "events.csv")[0]
        self.assertEqual((event["max_similarity"], event["max_similarity_record_id"]), ("", ""))
        self.assertEqual(event["recomputed_decision"], "no_history_auto_new")
        self.assertEqual(event["tenth_candidate_similarity"], "")
        self.assertFalse(self.csv(output, "candidates.csv"))
        self.assertTrue(all(row["no_history"] == "1" and row["below_threshold_auto_new"] == "0"
                            for row in self.csv(output, "thresholds.csv")))

    def test_matched_group_can_be_second_and_top_k_does_not_change_threshold_statistics(self):
        frame = records(
            ("2021-01-01", "highest", [1, 0], "highest", "new", "ETH"),
            ("2021-01-02", "matched", [0.8, 0.6], "matched", "new", "ETH"),
            ("2021-01-03", "now", [1, 0], "matched", "update", "ETH"),
        )
        _, output, _ = self.export(frame, top_k=1)
        event = self.csv(output, "events.csv")[0]
        self.assertEqual((event["max_similarity_event_id"], event["stored_match_rank"]), ("highest", "2"))
        self.assertEqual(event["groups_sent_to_ai"], "2")
        self.assertEqual(event["tenth_candidate_similarity"], "")
        self.assertEqual(len(self.csv(output, "candidates.csv")), 1)

    def test_tenth_selected_candidate_is_independent_of_export_top_k(self):
        scores = [0.99 - 0.03 * index for index in range(12)]
        items = [("2021-01-01", "g{:02d}".format(index), [score, math.sqrt(1 - score ** 2)],
                  "g{:02d}".format(index), "new", "ETH") for index, score in enumerate(scores)]
        frame = records(*items, ("2021-01-03", "now", [1, 0], "now", "new", "ETH"))
        _, output, _ = self.export(frame, top_k=1)
        event = self.csv(output, "events.csv")[0]
        self.assertEqual((event["qualifying_groups"], event["groups_sent_to_ai"]), ("12", "10"))
        self.assertEqual(float(event["tenth_candidate_similarity"]), scores[9])
        self.assertEqual(len(self.csv(output, "candidates.csv")), 1)

    def test_tenth_candidate_below_threshold_is_blank_and_equality_passes(self):
        items = [("2021-01-01", "g{:02d}".format(index), [1, 0], "g{:02d}".format(index), "new", "ETH")
                 for index in range(9)]
        frame = records(*items,
                        ("2021-01-02", "tenth", [0.5, math.sqrt(0.75)], "tenth", "new", "ETH"),
                        ("2021-01-03", "now", [1, 0], "now", "new", "ETH"))
        for threshold in (0.5, 0.55):
            with self.subTest(threshold=threshold):
                _, output, _ = self.export(frame, name="tenth-{}".format(threshold), threshold=threshold)
                event = self.csv(output, "events.csv")[0]
                self.assertEqual(event["history_groups"], "10")
                self.assertEqual(event["tenth_candidate_similarity"], "0.5" if threshold == 0.5 else "")

    def test_top_ten_limit_and_tie_order_match_production(self):
        items = [("2021-01-01", "g{:02d}".format(i), [1, 0], "g{:02d}".format(i), "new", "ETH")
                 for i in reversed(range(12))]
        frame = records(*items, ("2021-01-03", "now", [1, 0], "g11", "repeat", "ETH"))
        _, output, _ = self.export(frame, top_k=12)
        pairs = self.csv(output, "candidates.csv")
        self.assertEqual([pair["candidate_event_id"] for pair in pairs], ["g{:02d}".format(i) for i in range(12)])
        self.assertEqual(sum(pair["would_send_to_ai"] == "True" for pair in pairs), 10)
        self.assertEqual(self.csv(output, "events.csv")[0]["stored_match_rank"], "12")
        self.assertTrue(all(row["candidate_groups_sent"] == "10" and row["stored_matches_excluded"] == "1"
                            for row in self.csv(output, "thresholds.csv")))

    def test_calendar_month_window_includes_lower_bound_and_excludes_same_time(self):
        self.settings = replace(self.settings, start_time="2024-05-31", end_time="2024-05-31")
        frame = records(
            ("2024-02-29 11:59:59", "too_old", [1, 0], "too_old", "new", "ETH"),
            ("2024-02-29 12:00:00", "lower", [1, 0], "lower", "new", "ETH"),
            ("2024-05-31 12:00:00", "now", [1, 0], "lower", "repeat", "ETH"),
            ("2024-05-31 12:00:00", "same", [1, 0], "same", "new", "ETH"),
        )
        _, output, _ = self.export(frame)
        self.assertEqual({pair["candidate_event_id"] for pair in self.csv(output, "candidates.csv")}, {"lower"})

    def test_missing_mixed_or_invalid_vectors_fail_without_model_fallback(self):
        cases = ["", saved([1, 0], model_version="different"), saved([1, 0, 0]), saved([2, 0]), "broken"]
        for number, raw in enumerate(cases):
            with self.subTest(raw=raw):
                frame = records(
                    ("2021-01-01", "old", [1, 0], "old", "new", "ETH"),
                    ("2021-01-03", "now", [1, 0], "old", "repeat", "ETH"),
                )
                frame.iloc[0, frame.columns.get_loc("embedding")] = raw
                with self.assertRaises((ValueError, RuntimeError)):
                    self.export(frame, name="invalid-{}".format(number))
                self.assertFalse((self.root / "invalid-{}".format(number) / "manifest.json").exists())

    def test_only_successful_current_and_history_records_are_counted(self):
        items = [
            ("2021-01-01", "old", [1, 0], "old", "new", "ETH"),
            ("2021-01-03", "first", [0, 1], "first", "new", "ETH"),
            ("2021-01-03 12:00:00", "now", [1, 0], "old", "repeat", "ETH"),
        ]
        statuses = ("0", "9", "-1", "", 1, None)
        for number in range(len(statuses)):
            for when, prefix in (("2021-01-02", "history"), ("2021-01-03 06:00:00", "current")):
                items.append((when, "{}-{}".format(prefix, number), [1, 0], "", "", "ETH"))
        frame = records(*items)
        for number, status in enumerate(statuses):
            mask = frame.index.get_level_values("record_id").isin(
                ["history-{}".format(number), "current-{}".format(number)])
            frame.loc[mask, "round2_status"] = status
            frame.loc[mask, ["event", "embedding"]] = ""
        before = frame.copy(deep=True)
        client, output, manifest = self.export(frame)
        pd.testing.assert_frame_equal(frame, before)
        pd.testing.assert_frame_equal(client.frame, before)
        self.assertEqual((manifest["events"], manifest["candidate_rows"]), (2, 3))
        events = self.csv(output, "events.csv")
        self.assertEqual([row["record_id"] for row in events], ["first", "now"])
        self.assertEqual([row["history_records"] for row in events], ["1", "2"])
        self.assertEqual({row["best_record_id"] for row in self.csv(output, "candidates.csv")},
                         {"old", "first"})
        self.assertTrue(all(row["events"] == "2" and row["stored_update_repeat_events"] == "1"
                            for row in self.csv(output, "thresholds.csv")))

    def test_no_successful_current_records_exports_empty_without_reading_history(self):
        frame = records(
            ("2021-01-01", "old", [1, 0], "old", "new", "ETH"),
            ("2021-01-03", "pending", [1, 0], "", "", "ETH"),
            ("2021-01-03", "processing", [1, 0], "", "", "ETH"),
            ("2021-01-03", "failed", [1, 0], "", "", "ETH"),
        )
        frame.loc[frame.index.get_level_values("date") == pd.Timestamp("2021-01-03"),
                  "round2_status"] = ["0", "9", "-1"]
        frame["embedding"] = ""
        client, output, manifest = self.export(frame)
        self.assertEqual(len(client.reads), 1)
        self.assertEqual((manifest["events"], manifest["candidate_rows"]), (0, 0))
        self.assertIsNone(manifest["embedding_identity"])
        self.assertTrue(manifest["completed"])
        self.assertFalse(self.csv(output, "events.csv"))
        self.assertFalse(self.csv(output, "candidates.csv"))
        self.assertTrue(all(all(row[column] == "0" for column in row if column != "threshold")
                            for row in self.csv(output, "thresholds.csv")))

    def test_asset_list_excludes_successful_and_unprocessed_assets(self):
        frame = records(
            ("2021-01-01", "old", [1, 0], "old", "new", "ETH"),
            ("2021-01-01", "skipped-history", [1, 0], "", "", "ZZZ"),
            ("2021-01-03", "now", [1, 0], "old", "update", "ETH"),
            ("2021-01-03", "skipped-current", [1, 0], "", "", "ZZZ"),
            ("2021-01-03", "skipped-success", [1, 0], "skipped-success", "new", "ZZZ"),
        )
        frame.loc[frame["event_relation"] == "", ["round2_status", "embedding"]] = ["0", ""]
        self.settings = replace(self.settings, round2_asset_list_path="asset-list.json")
        document = {"InstrumentList": [" eth.exchange", "ETH.other", "BTC.exchange"]}
        with patch.object(ReadOnlyClient, "read_object", return_value=json.dumps(document).encode("utf-8"),
                          create=True) as read:
            client, output, manifest = self.export(frame)
        read.assert_called_once_with("asset-list.json")
        pd.testing.assert_frame_equal(client.frame, frame)
        self.assertEqual((manifest["events"], manifest["candidate_rows"]), (1, 1))
        self.assertEqual([row["record_id"] for row in self.csv(output, "events.csv")], ["now"])
        self.assertEqual(self.csv(output, "events.csv")[0]["history_records"], "1")
        self.assertEqual([row["best_record_id"] for row in self.csv(output, "candidates.csv")], ["old"])
        self.assertTrue(all(row["events"] == "1" for row in self.csv(output, "thresholds.csv")))

    def test_asset_list_skips_unsuccessful_target_current_or_history(self):
        self.settings = replace(self.settings, round2_asset_list_path="asset-list.json")
        document = {"InstrumentList": ["ETH.exchange"]}
        for position in (0, 1):
            with self.subTest(position=position):
                frame = records(
                    ("2021-01-01", "old", [1, 0], "old", "new", "ETH"),
                    ("2021-01-03", "now", [1, 0], "old", "update", "ETH"),
                )
                frame.iloc[position, frame.columns.get_loc("round2_status")] = "0"
                frame.iloc[position, frame.columns.get_loc("embedding")] = ""
                with patch.object(ReadOnlyClient, "read_object", return_value=json.dumps(document).encode("utf-8"),
                                  create=True) as read:
                    client, output, manifest = self.export(frame, name="unfinished-target-{}".format(position))
                read.assert_called_once_with("asset-list.json")
                self.assertEqual((manifest["events"], manifest["candidate_rows"]), (1 - position, 0))
                self.assertEqual(len(client.reads), 2 - position)
                events = self.csv(output, "events.csv")
                if position == 0:
                    self.assertEqual((events[0]["record_id"], events[0]["history_records"],
                                      events[0]["recomputed_decision"]), ("now", "0", "no_history_auto_new"))
                else:
                    self.assertFalse(events)

    def test_asset_list_with_no_target_records_exports_empty_without_reading_history(self):
        frame = records(("2021-01-03", "skipped", [1, 0], "", "", "ZZZ"))
        frame["round2_status"], frame["embedding"] = "0", ""
        self.settings = replace(self.settings, round2_asset_list_path="asset-list.json")
        document = {"InstrumentList": ["ETH.exchange"]}
        with patch.object(ReadOnlyClient, "read_object", return_value=json.dumps(document).encode("utf-8"),
                          create=True) as read:
            client, output, manifest = self.export(frame)
        read.assert_called_once_with("asset-list.json")
        self.assertEqual(len(client.reads), 1)
        self.assertEqual((manifest["events"], manifest["candidate_rows"]), (0, 0))
        self.assertFalse(self.csv(output, "events.csv"))
        self.assertFalse(self.csv(output, "candidates.csv"))
        self.assertTrue(all(row["events"] == "0" for row in self.csv(output, "thresholds.csv")))

    def test_asset_list_read_failure_stops_without_exporting_all_assets(self):
        frame = records(("2021-01-03", "only", [1, 0], "only", "new", "ETH"))
        self.settings = replace(self.settings, round2_asset_list_path="asset-list.json")
        with patch.object(ReadOnlyClient, "read_object", side_effect=RuntimeError("名单读取失败"), create=True), \
                self.assertRaisesRegex(RuntimeError, "名单读取失败"):
            self.export(frame)
        self.assertFalse((self.root / "report").exists())

    def test_empty_batch_outputs_headers_and_zero_counts(self):
        frame = records()
        _, output, manifest = self.export(frame)
        self.assertEqual(manifest["events"], 0)
        self.assertFalse(self.csv(output, "events.csv"))
        self.assertTrue(all(row["events"] == "0" for row in self.csv(output, "thresholds.csv")))

    def test_rejects_custom_retriever_and_existing_output(self):
        frame = records(("2021-01-03", "only", [1, 0], "only", "new", "ETH"))
        self.settings = replace(self.settings, retriever="custom:search")
        with self.assertRaisesRegex(ValueError, "自定义"):
            self.export(frame)
        self.settings = replace(self.settings, retriever="")
        (self.root / "report").mkdir()
        with self.assertRaisesRegex(ValueError, "已存在"):
            self.export(frame)

    def test_entrypoint_uses_dates_and_threshold_without_api_configuration(self):
        from entrypoints.audit_similarity import main
        config = self.root / "event.yaml"
        config.write_text(
            'first_round_task_path: first\nresult_path: result\nround2_ai_config_path: unused.yaml\n'
            'round3_ai_config_path: unused.yaml\nembedding_config_path: embedding.yaml\n', encoding="utf-8")
        (self.root / "embedding.yaml").write_text('base_url: ""\nsimilarity_threshold: 0.55\n', encoding="utf-8")
        frame = records(
            ("2021-01-03", "only", [1, 0], "only", "new", "ETH"),
            ("2021-01-03", "pending", [1, 0], "", "", "ETH"),
        )
        frame.iloc[1, frame.columns.get_loc("round2_status")] = "9"
        frame.iloc[1, frame.columns.get_loc("embedding")] = ""
        client = ReadOnlyClient(frame)
        manifest = main(["--config", str(config), "--start-time", "2021-01-03", "--end-time", "2021-01-03",
                         "--output-dir", str(self.root / "command")], cli=client)
        self.assertEqual((manifest["events"], manifest["threshold"]), (1, 0.55))
        self.assertEqual(manifest["start_time"], "2021-01-03")

    def test_exported_selection_matches_production_retriever(self):
        import numpy as np
        from common.config import EmbeddingConfig
        from common.event_context import EventHistory
        from round2.embedding import EmbeddingRetriever
        frame = records(
            ("2021-01-01", "one", [0.5, math.sqrt(0.75)], "one", "new", "ETH"),
            ("2021-01-02", "two", [0.8, 0.6], "two", "new", "ETH"),
            ("2021-01-02 12:00:00", "update", [1, 0], "one", "update", "ETH"),
            ("2021-01-03", "now", [1, 0], "one", "repeat", "ETH"),
        )
        _, output, _ = self.export(frame, threshold=0.9)
        retriever = EmbeddingRetriever(EmbeddingConfig("https://unused.invalid/v1", "unused", similarity_threshold=0.9))
        retriever.vectors = {row["event"]: np.asarray(json.loads(row["embedding"])["vector"], dtype=np.float64)
                             for _, row in frame.iterrows()}
        searchable = EventHistory(frame).get_searchable_records("ETH", "2021-01-03 00:00:00")
        selection = retriever({"event": frame.iloc[-1]["event"]}, searchable)
        self.assertEqual(tuple(pair["candidate_event_id"] for pair in self.csv(output, "candidates.csv")
                               if pair["would_send_to_ai"] == "True"), selection.event_ids)

    def test_csv_preserves_full_text_with_quotes_commas_and_newlines(self):
        frame = records(
            ("2021-01-01", "old", [1, 0], "old", "new", "ETH"),
            ("2021-01-03", "now", [1, 0], "old", "repeat", "ETH"),
        )
        text = '此前计划为1000万美元，说明为 "A,B"。\n本次新增事实在下一行。'
        frame["event"] = text
        _, output, _ = self.export(frame)
        pair = self.csv(output, "candidates.csv")[0]
        self.assertEqual((pair["current_event"], pair["best_record_event"]), (text, text))


if __name__ == "__main__":
    unittest.main()
