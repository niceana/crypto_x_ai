"""全量候选模式的完整输入、时间边界和未请求 AI 的超限失败。"""

import json
import unittest
from unittest.mock import patch

from round2.retrieval import RetrievalSelection, select_all_groups, validate_retrieval_selection
from round2.runner import run_relation
from tests.event_support import EventFixture, new_result
from tests.support import request_text, success


class AllGroupsTests(EventFixture, unittest.TestCase):
    def complete_roots(self, frame, keys):
        for key in keys:
            frame.loc[key, ["event_id", "event_relation", "round2_status"]] = [key[1], "new", "1"]

    def test_more_than_ten_groups_reach_ai_and_uncertain_keeps_original_answer(self):
        posts = [("2021-01-01 00:{:02d}:00".format(i), "history-{}".format(i), ["BTC"], "media")
                 for i in range(13)]
        posts.append(("2021-01-02", "current", ["BTC"], "media"))
        backend = self.backend(posts)
        frame = backend.tables["result"]
        self.complete_roots(frame, list(frame.index)[:-1])
        seen = []

        def handler(request):
            seen.append(json.loads(request_text(request)))
            return success(json.dumps({"event_relation": "uncertain", "matched_candidate_id": None,
                                       "new_information": None}))

        with patch("round2.runner.EmbeddingRetriever", side_effect=AssertionError("不能使用向量")):
            result = run_relation(backend, self.settings(start_time="2021-01-02", end_time="2021-01-02",
                                                        history_lookback_months=1),
                                  ai_config=self.config(), retriever=select_all_groups, max_candidate_groups=None,
                                  network_factory=self.factory(handler))
        self.assertEqual(len(seen[0]["candidates"]), 13)
        self.assertEqual({g["history"][0]["event"] for g in seen[0]["candidates"]},
                         {"history-{}".format(i) for i in range(13)})
        key = frame.index[-1]
        self.assertEqual((frame.at[key, "event_relation"], frame.at[key, "event_id"]), ("new", key[1]))
        raw = json.loads(frame.at[key, "round2_ai_response"])["ai_response_content"]
        self.assertEqual(json.loads(raw)["event_relation"], "uncertain")
        self.assertEqual((result.new_completed, result.http_attempts_started), (1, 1))

    def test_one_calendar_month_excludes_other_objects_same_time_and_future(self):
        backend = self.backend([
            ("2021-02-28 11:59:59", "too-old", ["BTC"], "m"),
            ("2021-02-28 12:00:00", "boundary", ["BTC"], "m"),
            ("2021-03-01", "other-object", ["ETH"], "m"),
            ("2021-03-31 12:00:00", "current-a", ["BTC"], "m"),
            ("2021-03-31 12:00:00", "current-b", ["BTC"], "m"),
            ("2021-04-01", "future", ["BTC"], "m"),
        ])
        frame = backend.tables["result"]
        self.complete_roots(frame, list(frame.index)[1:3])
        seen = []

        def handler(request):
            seen.append(json.loads(request_text(request)))
            return success(json.dumps(new_result()))

        result = run_relation(backend, self.settings(start_time="2021-03-31", end_time="2021-03-31",
                                                     history_lookback_months=1),
                              ai_config=self.config(), retriever=select_all_groups, max_candidate_groups=None,
                              network_factory=self.factory(handler))
        self.assertEqual(result.new_completed, 2)
        for body in seen:
            self.assertEqual([event["event"] for g in body["candidates"] for event in g["history"]], ["boundary"])

    def test_empty_history_still_calls_ai(self):
        backend = self.backend([("2021-01-01", "current", ["MACRO"], "m")])
        seen = []

        def handler(request):
            seen.append(json.loads(request_text(request)))
            return success(json.dumps(new_result()))

        result = run_relation(backend, self.relation_settings(backend, history_lookback_months=1),
                              ai_config=self.config(), retriever=select_all_groups, max_candidate_groups=None,
                              network_factory=self.factory(handler))
        self.assertEqual(seen[0]["candidates"], [])
        self.assertEqual(result.http_attempts_started, 1)

    def test_input_limit_fails_without_truncating_or_requesting_ai(self):
        backend = self.backend([("2021-01-01", "history" * 50, ["INDUSTRY"], "m"),
                                ("2021-01-02", "current", ["INDUSTRY"], "m")])
        frame = backend.tables["result"]
        self.complete_roots(frame, [frame.index[0]])
        result = run_relation(backend, self.settings(start_time="2021-01-02", end_time="2021-01-02",
                                                     history_lookback_months=1),
                              ai_config=self.config(), retriever=select_all_groups, max_candidate_groups=None,
                              max_input_characters=50,
                              network_factory=self.factory(lambda req: self.fail("超限不能请求 AI")))
        self.assertEqual((result.failed, result.http_attempts_started), (1, 0))
        self.assertIn("未截断", frame.at[frame.index[-1], "error_message"])

    def test_official_limit_and_candidate_membership_remain_enforced(self):
        ids = tuple("g{}".format(i) for i in range(11))
        selection = RetrievalSelection(ids)
        with self.assertRaisesRegex(ValueError, "最多 10"):
            validate_retrieval_selection(selection, set(ids))
        self.assertIs(validate_retrieval_selection(selection, set(ids), max_groups=None), selection)
        with self.assertRaisesRegex(ValueError, "不存在、跨资产"):
            validate_retrieval_selection(selection, set(ids[:-1]), max_groups=None)
