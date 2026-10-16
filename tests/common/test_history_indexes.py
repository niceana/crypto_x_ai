"""验证内存位置索引、按币时间窗口及历史资格，不连接真实服务。"""

import unittest
from types import SimpleNamespace

import pandas as pd

from common.event_context import EventHistory
from common.store import is_stored_relation_valid, validate_relation_history
from common.time_rules import history_window_start
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


class HistoryIndexTests(unittest.TestCase):
    def test_window_preserves_precise_boundaries_assets_statuses_and_tie_order(self):
        current = pd.Timestamp("2025-10-01 12:00:00.123456789", tz="Asia/Shanghai")
        begin = pd.Timestamp("2020-10-01 12:00:00.123456789", tz="Asia/Shanghai")
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
                    ("2024-01-01", "old", "ETH"),
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

    def test_calendar_year_start_keeps_leap_day_rules_timezone_and_nanoseconds(self):
        for current, expected in (
            ("2024-02-29 12:00:00.123456789", "2019-02-28 12:00:00.123456789"),
            ("2025-03-01 12:00:00.123456789", "2020-03-01 12:00:00.123456789"),
        ):
            with self.subTest(current=current):
                stamp = pd.Timestamp(current, tz="Asia/Shanghai")
                self.assertEqual(history_window_start(stamp, 5), pd.Timestamp(expected, tz="Asia/Shanghai"))

    def test_preflight_ignores_gaps_before_five_year_boundary_but_includes_boundary(self):
        current = pd.Timestamp("2025-10-01 12:00:00.123456789", tz="Asia/Shanghai")
        begin = pd.Timestamp("2020-10-01 12:00:00.123456789", tz="Asia/Shanghai")
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

    def test_preflight_does_not_verify_origins_outside_five_years(self):
        frame = completed_records([
            ("2010-01-01", "origin", "ETH"),
            ("2024-01-01", "update", "ETH"),
            ("2025-01-01", "current", "ETH"),
        ])
        root, update, current = frame.index
        frame.loc[update, ["event_id", "event_relation"]] = [root[1], "update"]
        frame.at[current, "round2_status"] = "0"
        validate_relation_history(SimpleNamespace(df=frame), [current])
        frame.at[root, "round2_status"] = "0"
        validate_relation_history(SimpleNamespace(df=frame), [current])
        validate_relation_history(SimpleNamespace(df=frame.drop(index=root)), [current])

    def test_history_window_uses_each_query_time_and_configured_years(self):
        frame = completed_records([
            ("2010-01-01", "early-history", "ETH"),
            ("2020-01-01", "recent-history", "ETH"),
        ])
        history = EventHistory(frame)
        self.assertEqual([record["event"] for record in history.get_searchable_records(
            "ETH", "2011-01-01 00:00:00")], ["early-history"])
        self.assertEqual([record["event"] for record in history.get_searchable_records(
            "ETH", "2021-01-01 00:00:00")], ["recent-history"])
        shorter = EventHistory(frame, history_lookback_years=1)
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
