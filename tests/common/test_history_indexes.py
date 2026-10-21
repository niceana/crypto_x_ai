"""验证历史读取范围、窗口资格和内存索引，不连接真实服务。"""

import copy
import json
import unittest
from datetime import date
from types import SimpleNamespace

import pandas as pd

from common.event_context import EventHistory, build_current_record_input, build_relation_input
from common.store import is_stored_relation_valid, validate_relation_history
from common.time_rules import history_read_bounds, history_window_start, is_history_needed
from initialization.event_records import expand_first_round
from tests.event_support import source_posts


def completed_records(posts):
    """根据 (时间, 事件, 代币) 构造已完成的独立事件组。"""
    source = source_posts([(stamp, event, [asset], "account") for stamp, event, asset in posts])
    frame = expand_first_round(source, {})
    frame["round2_status"] = "1"
    frame["event_relation"] = "new"
    frame["event_id"] = frame.index.get_level_values("record_id")
    return frame


class HistoryRangeTests(unittest.TestCase):
    def test_read_bounds_accept_unsorted_iterator_and_calendar_months(self):
        times = [pd.Timestamp("2026-01-01 00:00:00", tz="Asia/Shanghai"),
                 pd.Timestamp("2024-05-31 12:00:00.123456789", tz="Asia/Shanghai")]
        before = list(times)
        self.assertEqual(history_read_bounds(iter(times), "2024-06-01", 3),
                         (date(2024, 2, 29), date(2024, 5, 31)))
        self.assertEqual(history_read_bounds(iter(times), "2024-06-01", 15),
                         (date(2023, 2, 28), date(2024, 5, 31)))
        self.assertEqual(times, before)

    def test_read_bounds_skip_empty_or_already_covered_dates_and_keep_one_day(self):
        for times, scope, expected in (
            ([], "2025-01-01", None),
            (["2025-01-01 12:00:00"], "2024-10-01", None),
            (["2025-01-01 12:00:00"], "2024-01-01", None),
            (["2025-01-01 12:00:00"], "2024-10-02", (date(2024, 10, 1), date(2024, 10, 1))),
        ):
            with self.subTest(times=times, scope=scope):
                self.assertEqual(history_read_bounds(iter(times), scope, 3), expected)

    def test_needed_history_keeps_precise_left_closed_right_open_window(self):
        current = pd.Timestamp("2024-05-31 12:00:00.123456789", tz="Asia/Shanghai")
        begin = pd.Timestamp("2024-02-29 12:00:00.123456789", tz="Asia/Shanghai")
        for stamp, expected in ((begin - pd.Timedelta(1, "ns"), False), (begin, True),
                                (current - pd.Timedelta(1, "ns"), True), (current, False),
                                (current + pd.Timedelta(1, "ns"), False)):
            with self.subTest(stamp=stamp):
                self.assertEqual(is_history_needed(stamp, (current,), 3), expected)
        self.assertFalse(is_history_needed(begin, (), 3))
        self.assertFalse(is_history_needed(begin, (current,), 2))

    def test_needed_history_handles_later_tasks_and_duplicate_times_without_mutating(self):
        history = pd.Timestamp("2025-01-01 00:00:00")
        times = [history, history, pd.Timestamp("2025-02-01 00:00:00")]
        before = list(times)
        self.assertTrue(is_history_needed(history, times, 3))
        self.assertFalse(is_history_needed(history, times[:2], 3))
        self.assertFalse(is_history_needed(pd.Timestamp("2010-01-01"), times, 3))
        self.assertEqual(times, before)

    def test_needed_history_checks_later_clamped_month_end_windows(self):
        history = pd.Timestamp("2024-02-29 12:00:00")
        times = [pd.Timestamp("2024-05-29 23:00:00"), pd.Timestamp("2024-05-30 01:00:00")]
        # 第一个窗口不包含该历史，第二天截断至同一个月末后可以纳入。
        self.assertTrue(is_history_needed(history, times[1:], 3))
        self.assertFalse(is_history_needed(history, times[:1], 3))
        self.assertTrue(is_history_needed(history, times, 3))

    def test_needed_history_skips_same_day_tasks_and_checks_following_days(self):
        history = pd.Timestamp("2025-02-28 12:00:00.123456789", tz="Asia/Shanghai")
        times = [pd.Timestamp(value, tz="Asia/Shanghai") for value in (
            "2025-05-28 23:00:00", "2025-05-28 23:59:00", "2025-05-29 22:00:00",
            "2025-05-30 12:00:00.123456789", "2025-05-31 01:00:00")]
        self.assertFalse(is_history_needed(history, times[:3], 3))
        self.assertTrue(is_history_needed(history, times[:4], 3))
        self.assertFalse(is_history_needed(history - pd.Timedelta(days=1), times, 3))

    def test_preflight_checks_history_needed_only_by_later_month_end_task(self):
        frame = completed_records([
            ("2024-02-29 12:00:00", "gap", "ETH"),
            ("2024-05-29 23:00:00", "first", "ETH"),
            ("2024-05-30 01:00:00", "second", "ETH"),
        ])
        gap, first, second = frame.index
        frame["round2_status"] = "0"
        validate_relation_history(SimpleNamespace(df=frame), [first])
        with self.assertRaisesRegex(ValueError, gap[1]):
            validate_relation_history(SimpleNamespace(df=frame), [first, second])


class HistoryIndexTests(unittest.TestCase):
    def test_relation_input_keeps_index_dates_and_full_events_without_mutating_history(self):
        frame = completed_records([
            (pd.Timestamp("2021-01-01 12:00:00.123456789", tz="Asia/Shanghai"),
             "ABC 提出计划，原文保留日期2021-02-01及金额100万美元。", "ETH"),
            (pd.Timestamp("2021-01-02 13:00:00.234567891", tz="Asia/Shanghai"),
             "ABC 更新额度至200万美元。", "ETH"),
            (pd.Timestamp("2021-01-03 14:00:00.345678912", tz="Asia/Shanghai"),
             "ABC 更新额度至200万美元。", "ETH"),
        ])
        root, first, second = frame.index
        frame.loc[[first, second], ["event_id", "event_relation", "new_information"]] = [root[1], "update", "仅供第三轮的增量"]
        history = EventHistory(frame.iloc[::-1], truncate_group_history=True)
        current_date = pd.Timestamp("2021-01-04 15:00:00.456789123", tz="Asia/Shanghai")
        group = history.build_group_history("ETH", root[1], current_date)
        current = build_current_record_input((current_date, "current"),
                                            {"event": 'ABC 宣布“开始执行”，保留原文中的"引号"。',
                                             "source_post_id": "source", "asset": "ETH"})
        before = copy.deepcopy((current, group))

        text, candidate_event_ids = build_relation_input(current, [group])
        body = json.loads(text)

        self.assertEqual(body, {"current": {"published_at": str(current_date), "event": current["event"]}, "candidates": [
            {"candidate_id": "E1", "history": [{"published_at": str(key[0]), "event": row["event"]}
                                               for key, row in frame.iterrows()]}]})
        self.assertEqual(candidate_event_ids, {"E1": root[1]})
        self.assertEqual((current, group), before)
        self.assertNotIn("仅供第三轮的增量", json.dumps(body, ensure_ascii=False))

    def test_relation_candidate_numbers_follow_order_through_e10_and_restart_per_request(self):
        frame = completed_records([("2021-01-01", "fact-{}".format(index), "ETH") for index in range(10)])
        history = EventHistory(frame, truncate_group_history=True)
        event_ids = list(frame.index.get_level_values("record_id"))[::-1]
        groups = [history.build_group_history("ETH", event_id, "2021-01-02 00:00:00") for event_id in event_ids]
        text, mapping = build_relation_input({"date": "2021-01-02 00:00:00", "event": "current"}, groups)
        self.assertEqual([group["candidate_id"] for group in json.loads(text)["candidates"]],
                         ["E{}".format(index) for index in range(1, 11)])
        self.assertEqual(list(mapping.values()), event_ids)
        next_text, next_mapping = build_relation_input({"date": "2021-01-03 00:00:00", "event": "next"}, groups[-1:])
        self.assertEqual(json.loads(next_text)["candidates"][0]["candidate_id"], "E1")
        self.assertEqual(next_mapping, {"E1": event_ids[-1]})
        self.assertEqual(len(mapping), 10)

    def test_window_preserves_precise_boundaries_assets_statuses_and_tie_order(self):
        current = pd.Timestamp("2025-10-01 12:00:00.123456789", tz="Asia/Shanghai")
        begin = pd.Timestamp("2025-07-01 12:00:00.123456789", tz="Asia/Shanghai")
        frame = completed_records([
            (begin - pd.Timedelta(nanoseconds=1), "outside", "ETH"),
            (begin, "boundary-a", "ETH"),
            (begin, "boundary-b", "ETH"),
            (current - pd.Timedelta(nanoseconds=1), "last", "ETH"),
            (current, "peer", "ETH"),
            (current + pd.Timedelta(nanoseconds=1), "future", "ETH"),
            (begin, "other-asset", "BTC"),
            (begin, "unfinished", "ETH"),
        ])
        frame.loc[frame["event"].eq("unfinished"), "round2_status"] = "9"
        history = EventHistory(frame.iloc[::-1])
        self.assertEqual([record["event"] for record in history.get_searchable_records("ETH", current)],
                         ["boundary-a", "boundary-b", "last"])
        self.assertEqual([record["event"] for record in history.get_searchable_records("BTC", current)],
                         ["other-asset"])
        self.assertEqual(history.get_searchable_records("SOL", current), ())

    def test_incremental_and_batch_additions_keep_order_and_allow_backward_queries(self):
        frame = completed_records([
            ("2021-01-01", "origin", "ETH"),
            ("2021-01-02", "same-time-a", "ETH"),
            ("2021-01-02", "same-time-b", "ETH"),
            ("2021-01-03", "future", "ETH"),
            ("2020-12-31", "older-batch", "ETH"),
        ])
        keys = {row["event"]: key for key, row in frame.iterrows()}
        history = EventHistory(frame.loc[[keys["origin"], keys["future"]]])
        for event in ("same-time-b", "same-time-a"):
            key = keys[event]
            history.add(key, frame.loc[key])
        # 跨批重复加入不增加候选或组计数，旧批次可以按原时间插入。
        history.add_frame(frame.loc[[keys["older-batch"], keys["same-time-a"]]])
        for stamp, expected in (
            ("2021-01-04 00:00:00", ["older-batch", "origin", "same-time-a", "same-time-b", "future"]),
            ("2021-01-02 00:00:00", ["older-batch", "origin"]),
            ("2021-01-03 00:00:00", ["older-batch", "origin", "same-time-a", "same-time-b"]),
        ):
            self.assertEqual([record["event"] for record in history.get_searchable_records("ETH", stamp)], expected)
        self.assertEqual(history.build_group_history("ETH", keys["same-time-a"][1],
                                                     "2021-01-04 00:00:00")["disclosure_count"], 1)

    def test_window_does_not_trim_group_origin_and_returned_copies_are_isolated(self):
        frame = completed_records([
            ("2020-01-01", "origin", "ETH"),
            ("2025-09-01", "update", "ETH"),
            ("2025-10-01", "peer", "ETH"),
        ])
        origin, update, peer = frame.index
        frame.loc[[update, peer], "event_id"] = origin[1]
        frame.loc[[update, peer], "event_relation"] = "update"
        frame.loc[[update, peer], "new_information"] = "new fact"
        history = EventHistory(frame.iloc[::-1])
        current = "2025-10-01 00:00:00"
        candidates = history.get_searchable_records("ETH", current)
        self.assertEqual([record["event"] for record in candidates], ["update"])
        candidates[0]["event"] = "modified outside history"
        group = history.build_group_history("ETH", origin[1], current)
        self.assertEqual(group["first_disclosure"]["event"], "origin")
        self.assertEqual([record["event"] for record in group["timeline"]], ["update"])
        self.assertEqual(group["disclosure_count"], 2)
        group["timeline"][0]["event"] = "modified group copy"
        self.assertEqual(history.get_searchable_records("ETH", current)[0]["event"], "update")

    def test_unfinished_records_cannot_be_added_incrementally(self):
        for status in ("0", "9", "-1"):
            with self.subTest(status=status):
                frame = completed_records([("2021-01-01", "unfinished", "ETH")])
                key = frame.index[0]
                frame.at[key, "round2_status"] = status
                history = EventHistory()
                with self.assertRaisesRegex(ValueError, "未完成关联"):
                    history.add(key, frame.loc[key])
                self.assertEqual(history.get_searchable_records("ETH", "2021-01-02 00:00:00"), ())

    def test_preflight_preserves_original_positions_and_does_not_change_frame(self):
        frame = completed_records([
            ("2020-01-01", "origin", "ETH"),
            ("2021-01-01", "update", "ETH"),
            ("2021-01-02", "current", "ETH"),
            ("2021-01-03", "other-asset", "BTC"),
        ])
        origin, update, current = frame.index[:3]
        frame.loc[update, ["event_id", "event_relation"]] = [origin[1], "update"]
        frame.at[current, "round2_status"] = "0"
        frame = frame.iloc[::-1].copy(deep=True)
        before = frame.copy(deep=True)
        validate_relation_history(SimpleNamespace(df=frame), [current])
        pd.testing.assert_frame_equal(frame, before)

    def test_preflight_still_checks_window_gaps_and_later_completed_records(self):
        for problem in ("old-gap", "invalid-origin", "backfill"):
            with self.subTest(problem=problem):
                frame = completed_records([
                    ("2025-08-01", "old", "ETH"),
                    ("2025-10-01", "current", "ETH"),
                    ("2025-10-02", "later", "ETH"),
                ])
                old, current, later = frame.index
                frame.loc[[current, later], "round2_status"] = "0"
                if problem == "old-gap":
                    frame.at[old, "round2_status"] = "0"
                elif problem == "invalid-origin":
                    frame.loc[old, ["event_id", "event_relation"]] = [old[1], "update"]
                else:
                    frame.at[later, "round2_status"] = "1"
                reason = "疑似历史补录" if problem == "backfill" else "手动补跑"
                with self.assertRaisesRegex(ValueError, reason):
                    validate_relation_history(SimpleNamespace(df=frame), [current])

    def test_calendar_month_start_keeps_month_end_timezone_and_nanoseconds(self):
        for current, months, expected in (
            ("2024-05-31 12:00:00.123456789", 3, "2024-02-29 12:00:00.123456789"),
            ("2025-05-31 12:00:00.123456789", 3, "2025-02-28 12:00:00.123456789"),
            ("2025-01-31 12:00:00.123456789", 3, "2024-10-31 12:00:00.123456789"),
            ("2025-03-31 12:00:00.123456789", 1, "2025-02-28 12:00:00.123456789"),
            ("2025-05-31 12:00:00.123456789", 15, "2024-02-29 12:00:00.123456789"),
            ("2024-02-29 12:00:00.123456789", 60, "2019-02-28 12:00:00.123456789"),
        ):
            with self.subTest(current=current):
                stamp = pd.Timestamp(current, tz="Asia/Shanghai")
                self.assertEqual(history_window_start(stamp, months), pd.Timestamp(expected, tz="Asia/Shanghai"))

    def test_preflight_ignores_gaps_before_three_month_boundary_but_includes_boundary(self):
        current = pd.Timestamp("2025-10-01 12:00:00.123456789", tz="Asia/Shanghai")
        begin = pd.Timestamp("2025-07-01 12:00:00.123456789", tz="Asia/Shanghai")
        frame = completed_records([
            (begin - pd.Timedelta(nanoseconds=1), "old-gap", "ETH"),
            (begin, "boundary-gap", "ETH"),
            (current, "current", "ETH"),
        ])
        old, boundary, key = frame.index
        frame.loc[[old, boundary, key], "round2_status"] = "0"
        with self.assertRaisesRegex(ValueError, boundary[1]):
            validate_relation_history(SimpleNamespace(df=frame), [key])
        frame.at[boundary, "round2_status"] = "1"
        validate_relation_history(SimpleNamespace(df=frame), [key])

    def test_preflight_does_not_verify_origins_outside_three_months(self):
        frame = completed_records([
            ("2010-01-01", "origin", "ETH"),
            ("2024-12-01", "update", "ETH"),
            ("2025-01-01", "current", "ETH"),
        ])
        root, update, current = frame.index
        frame.loc[update, ["event_id", "event_relation"]] = [root[1], "update"]
        frame.at[current, "round2_status"] = "0"
        validate_relation_history(SimpleNamespace(df=frame), [current])
        frame.at[root, "round2_status"] = "0"
        validate_relation_history(SimpleNamespace(df=frame), [current])
        validate_relation_history(SimpleNamespace(df=frame.drop(index=root)), [current])

    def test_history_window_uses_each_query_time_and_configured_months(self):
        frame = completed_records([
            ("2010-11-01", "early-history", "ETH"),
            ("2020-11-01", "recent-history", "ETH"),
        ])
        history = EventHistory(frame)
        self.assertEqual([record["event"] for record in history.get_searchable_records(
            "ETH", "2011-01-01 00:00:00")], ["early-history"])
        self.assertEqual([record["event"] for record in history.get_searchable_records(
            "ETH", "2021-01-01 00:00:00")], ["recent-history"])
        shorter = EventHistory(frame, history_lookback_months=1)
        self.assertEqual(shorter.get_searchable_records("ETH", "2021-01-02 00:00:00"), ())

    def test_position_lookup_keeps_origin_validation(self):
        for problem in (None, "missing", "asset", "status", "relation", "identity", "same-time", "future"):
            with self.subTest(problem=problem):
                frame = completed_records([
                    ("2020-01-01", "origin", "ETH"),
                    ("2021-01-01", "update", "ETH"),
                ])
                origin, update = frame.index
                frame.loc[update, ["event_id", "event_relation"]] = [origin[1], "update"]
                if problem == "missing":
                    frame.at[update, "event_id"] = "missing"
                elif problem in ("asset", "status", "relation", "identity"):
                    column, value = {"asset": ("asset", "BTC"), "status": ("round2_status", "0"),
                                     "relation": ("event_relation", "repeat"), "identity": ("event_id", "wrong")}[problem]
                    frame.at[origin, column] = value
                elif problem in ("same-time", "future"):
                    stamp = update[0] if problem == "same-time" else update[0] + pd.Timedelta(seconds=1)
                    frame.index = pd.MultiIndex.from_tuples([(stamp, origin[1]), update], names=frame.index.names)
                positions = {key[1]: position for position, key in enumerate(frame.index)}
                row = frame.loc[update]
                self.assertEqual(is_stored_relation_valid(update, row, frame, record_positions=positions),
                                 problem is None)


if __name__ == "__main__":
    unittest.main()
