"""用小规模事件验证权重公式、小时对齐及按时间索引续写。"""

import importlib
import itertools
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal

from common.persistence import BatchPersistenceError
from common.config import EventConfig
from postprocessing.weight_data import (
    ASSET_SUFFIX, SOURCE_COLUMNS, audit_weight_columns, build_basic_df, build_weight_data, write_weight_data,
)
from postprocessing.weight_parameters import prepare_weight_grid
from tests.event_support import Tables


READ_RANGE = {"start_time": "2017-12-30", "end_time": "2017-12-31"}


def result_frame(items, event_ids=None, *, execution_stage="未开始", persistence="长期"):
    """每项为 date、asset、relation、direction、materiality。"""
    rows, keys = [], []
    for number, (when, asset, relation, direction, materiality) in enumerate(items):
        response = "" if direction is None else json.dumps({"ai_response_content": json.dumps({
            "direction": direction, "materiality": materiality,
            "execution_stage": execution_stage, "persistence": persistence, "materiality_reason": "说明",
        }, ensure_ascii=False)}, ensure_ascii=False)
        keys.append((when, "record{}".format(number)))
        event_id = event_ids[number] if event_ids is not None else "group:" + asset
        rows.append([asset, event_id, relation, response, "1", "1"])
    index = pd.MultiIndex.from_tuples(keys, names=["date", "record_id"])
    return pd.DataFrame(rows, index=index, columns=[
        "asset", "event_id", "event_relation", "round3_ai_response", "round2_status", "round3_status"])


def sample_results():
    return result_frame([
        ("2017-12-30 23:59:59", "OLD", "new", "利好", "高"),
        ("2017-12-31 00:20:10", "BTC", "new", "利好", "高"),
        ("2017-12-31 00:59:59.999999999", "BTC", "update", "利空", "中"),
        ("2017-12-31 00:45:00", "BTC", "repeat", "利好", "高"),
        ("2017-12-31 02:00:00", "ETH", "new", "利空", "低"),
        ("2017-12-31 02:30:00", "ETH", "repeat", "利空", "低"),
    ])


class WeightDataTests(unittest.TestCase):
    def test_repeat_with_copied_response_can_compute_without_source_in_read_range(self):
        source = result_frame([
            ("2017-12-31 01:00:00", "BTC", "repeat", "利好", "高"),
            ("2017-12-31 02:00:00", "BTC", "repeat", "利好", "高"),
        ])
        self.assertEqual(list(build_basic_df(source)["data"]), [1.5, 1.5])

    def test_every_mapping_combination(self):
        directions = {"利好": 1, "利空": -1, "中性": 0, "不确定": 0, "其他": 0}
        materials = {"高": 3, "中": 2, "低": 1, "不确定": 0}
        relations = {"new": 2, "update": 1.5, "repeat": 0.5}
        combinations = list(itertools.product(directions, materials, relations))
        items = []
        for number, (direction, material, relation) in enumerate(combinations):
            asset = "ASSET{}".format(number)
            items.append(("2017-08-17 15:20:10", asset, relation, direction, material))
        source = result_frame(items)
        basic = build_basic_df(source)
        self.assertEqual(list(basic.columns), ["date", "asset", "data"])
        current = basic.loc[basic["date"].eq(pd.Timestamp("2017-08-17 15:20:10"))]
        self.assertEqual(list(current["data"]), [directions[d] * materials[m] * relations[r]
                                                 for d, m, r in combinations])

    def test_group_sum_continuous_hours_suffix_and_start_cutoff(self):
        source = sample_results()
        original = source.copy(deep=True)
        basic = build_basic_df(source)
        self.assertEqual(list(basic["data"]), [6, 6, -3, 1.5, -2, -0.5])
        expected = pd.DataFrame({"BTC" + ASSET_SUFFIX: ["0.0", "4.5", "0.0", "0.0"],
                                 "ETH" + ASSET_SUFFIX: ["0.0", "0.0", "0.0", "-2.5"],
                                 "OLD" + ASSET_SUFFIX: ["6.0", "0.0", "0.0", "0.0"]},
                                index=pd.date_range("2017-12-31 00:00:00", periods=4, freq="h", name="date"))
        assert_frame_equal(build_weight_data(basic, start_time="2017-12-31"), expected)
        assert_frame_equal(source, original)

    def test_timestamp_input_and_next_day(self):
        source = result_frame([(pd.Timestamp("2018-01-01 00:00:00"), "BTC", "new", "利好", "低")])
        weight = build_weight_data(build_basic_df(source), start_time="2018-01-01")
        self.assertEqual(len(weight), 2)
        self.assertEqual(weight.index[0], pd.Timestamp("2018-01-01 00:00:00"))
        self.assertEqual(weight.iloc[-1, 0], "2.0")
        self.assertTrue((weight.iloc[:-1, 0] == "0.0").all())

    def test_configured_read_start_replaces_the_previous_fixed_start(self):
        source = result_frame([("2017-08-17 15:20:10", "BTC", "new", "利好", "高")])
        weight = build_weight_data(build_basic_df(source), start_time="2017-08-17", end_time="2017-08-17")
        self.assertEqual(weight.index[0], pd.Timestamp("2017-08-17 00:00:00"))
        self.assertEqual(weight.index[-1], pd.Timestamp("2017-08-18 00:00:00"))
        self.assertEqual(weight.iloc[0, 0], "0.0")
        self.assertEqual(weight.loc["2017-08-17 16:00:00"].iloc[0], "6.0")

    def test_timezone_timestamp_keeps_source_clock_time(self):
        source = result_frame([(pd.Timestamp("2017-12-31 00:20:10", tz="Asia/Shanghai"),
                                "BTC", "new", "利好", "高")])
        basic = build_basic_df(source)
        self.assertEqual(basic.loc[0, "date"], pd.Timestamp("2017-12-31 00:20:10"))
        weight = build_weight_data(basic, start_time="2017-12-31")
        self.assertEqual(weight.iloc[0, 0], "0.0")
        self.assertEqual(weight.iloc[1, 0], "6.0")

    def test_forward_hour_alignment_for_minutes_exact_hours_and_day_boundary(self):
        basic = pd.DataFrame({
            "date": ["2017-12-31 15:00:00", "2017-12-31 15:20:10",
                     "2017-12-31 15:59:59.999999999", "2017-12-31 16:00:00",
                     "2017-12-31 23:59:59"],
            "asset": ["BTC"] * 5, "data": [2.0, 3.0, -0.5, 5.0, 6.0],
        })
        weight = build_weight_data(basic, start_time="2017-12-31")
        column = "BTC.USDT.UFUTURE.BINANCE"
        self.assertEqual(weight.loc["2017-12-31 15:00:00", column], "0.0")
        self.assertEqual(weight.loc["2017-12-31 16:00:00", column], "4.5")
        self.assertEqual(weight.loc["2017-12-31 17:00:00", column], "5.0")
        self.assertEqual(weight.loc["2018-01-01 00:00:00", column], "6.0")
        self.assertEqual(weight.index[-1], pd.Timestamp("2018-01-01 00:00:00"))
        self.assertEqual(weight[column].astype(float).sum(), 15.5)

    def test_repeats_use_own_response_and_keep_source_order(self):
        source = result_frame([
            ("2017-12-31 03:00:00", "BTC", "repeat", "利好", "低"),
            ("2017-12-30 23:59:59", "BTC", "new", "利好", "高"),
            ("2017-12-31 01:00:00", "BTC", "repeat", "利空", "高"),
            ("2017-12-31 02:00:00", "BTC", "update", "利空", "中"),
            ("2017-12-31 04:00:00", "BTC", "repeat", "中性", "低"),
        ])
        basic = build_basic_df(source)
        self.assertEqual(list(basic["data"]), [0.5, 6, -1.5, -3, 0])
        self.assertEqual(list(basic["date"]), list(pd.to_datetime(source.index.get_level_values("date"))))
        weight = build_weight_data(basic, start_time="2017-12-31")
        self.assertEqual(weight.loc["2017-12-31 02:00:00", "BTC" + ASSET_SUFFIX], "-1.5")

    def test_empty_repeat_is_rejected_even_with_past_same_time_or_future_response(self):
        for when in ("2017-12-30 23:59:59", "2017-12-31 00:00:00", "2017-12-31 00:00:01"):
            with self.subTest(when=when):
                source = result_frame([
                    (when, "BTC", "new", "利好", "高"),
                    ("2017-12-31 00:00:00", "BTC", "repeat", None, None),
                ])
                with self.assertRaisesRegex(ValueError, "record1.*无法计算权重"):
                    build_basic_df(source)

    def test_weight_features_do_not_require_event_group_column(self):
        source = result_frame([("2017-12-31 01:00:00", "BTC", "repeat", "利好", "高")])
        self.assertNotIn("event_id", SOURCE_COLUMNS)
        self.assertEqual(list(build_basic_df(source.drop(columns="event_id"))["data"]), [1.5])

    def test_repeat_response_is_independent_of_previous_neutral_analysis(self):
        source = result_frame([
            ("2017-12-31 00:00:00", "BTC", "new", "利好", "高"),
            ("2017-12-31 00:10:00", "BTC", "update", "中性", "高"),
            ("2017-12-31 00:20:00", "BTC", "repeat", "利好", "高"),
        ])
        self.assertEqual(list(build_basic_df(source)["data"]), [6, 0, 1.5])

    def test_simultaneous_and_later_repeats_use_their_own_analysis(self):
        source = result_frame([
            ("2017-12-31 00:00:00", "BTC", "new", "利好", "高"),
            ("2017-12-31 00:10:00", "BTC", "update", "利空", "中"),
            ("2017-12-31 00:10:00", "BTC", "repeat", "利好", "高"),
            ("2017-12-31 00:10:01", "BTC", "repeat", "利好", "高"),
        ])
        self.assertEqual(list(build_basic_df(source)["data"]), [6, -3, 1.5, 1.5])

    def test_only_both_successful_rows_contribute_assets_and_scores(self):
        statuses = list(itertools.product(("1", "0", "9", "-1"), repeat=2))
        source = result_frame([
            ("2017-12-31 00:20:00", "BTC" if number == 0 else "SKIP{}".format(number),
             "new" if number == 0 else "uncertain", "利好" if number == 0 else None, "高")
            for number in range(len(statuses))
        ])
        source["round2_status"] = [status2 for status2, _ in statuses]
        source["round3_status"] = [status3 for _, status3 in statuses]
        original = source.copy(deep=True)
        backend = Tables(pd.DataFrame(), source)
        weight = write_weight_data(backend, "result", "weights", **READ_RANGE)
        expected = write_weight_data(Tables(pd.DataFrame(), source.iloc[:1]),
                                     "result", "weights", **READ_RANGE)
        assert_frame_equal(weight, expected)
        assert_frame_equal(source, original)
        self.assertEqual(list(weight.columns), ["BTC" + ASSET_SUFFIX])
        self.assertEqual(weight.loc["2017-12-31 09:00:00", "BTC" + ASSET_SUFFIX], "6.0")

    def test_no_successful_rows_do_not_create_weight_table(self):
        source = sample_results()
        source["round3_status"] = "-1"
        source["round3_ai_response"] = ""
        backend = Tables(pd.DataFrame(), source)
        with self.assertRaisesRegex(ValueError, "没有事件"):
            write_weight_data(backend, "result", "weights", **READ_RANGE)
        self.assertFalse(backend.registers or backend.writes)

    def test_bad_successful_rows_stop_before_register(self):
        for column, value in (("asset", ""), ("event_relation", "uncertain"),
                              ("round3_ai_response", ""), ("round3_ai_response", "{broken"),
                              ("round3_ai_response", '{"ai_response_content":"{}"}')):
            with self.subTest(column=column, value=value):
                source = sample_results()
                source.iloc[1, source.columns.get_loc(column)] = value
                backend = Tables(pd.DataFrame(), source)
                with self.assertRaisesRegex(ValueError, "record1"):
                    write_weight_data(backend, "result", "weights", **READ_RANGE)
                self.assertFalse(backend.registers or backend.writes)

    def test_duplicate_source_not_counted_twice(self):
        source = sample_results()
        with self.assertRaises(ValueError):
            build_basic_df(pd.concat([source, source.iloc[:1]]))

    def test_column_names_come_only_from_assets_even_with_other_tickers_in_ids(self):
        source = result_frame([
            ("2017-12-31 00:20:00", "ETH", "new", "利空", "低"),
            ("2017-12-31 00:30:00", "BTC", "new", "利好", "高"),
            ("2017-12-31 01:00:00", "BTC", "new", "利好", "低"),
        ], event_ids=["CSS", "C20", "CSS"])
        source = source[source.columns[::-1]]
        weight = build_weight_data(build_basic_df(source), start_time="2017-12-31")
        self.assertEqual(list(weight.columns), ["BTC.USDT.UFUTURE.BINANCE", "ETH.USDT.UFUTURE.BINANCE"])
        self.assertEqual(weight.loc[pd.Timestamp("2017-12-31 01:00:00"), "BTC.USDT.UFUTURE.BINANCE"], "6.0")
        self.assertEqual(weight.loc[pd.Timestamp("2017-12-31 01:00:00"), "ETH.USDT.UFUTURE.BINANCE"], "-2.0")

    def test_incorrect_generated_column_stops_before_registration(self):
        backend = Tables(pd.DataFrame(), sample_results())
        incorrect = build_weight_data(build_basic_df(sample_results()), **READ_RANGE).rename(columns={
            "BTC.USDT.UFUTURE.BINANCE": "CSS.USDT.UFUTURE.BINANCE"})
        with patch("postprocessing.weight_data.build_weight_data", return_value=incorrect):
            with self.assertRaisesRegex(ValueError, "CSS.*BTC"):
                write_weight_data(backend, "result", "weights", **READ_RANGE)
        self.assertFalse(backend.registers or backend.writes)

    def test_readonly_audit_reports_css_c20_columns_absent_from_source(self):
        source = sample_results()
        backend = Tables(pd.DataFrame(), source)
        stored = build_weight_data(build_basic_df(source), start_time="2017-12-31").rename(columns={
            "OLD.USDT.UFUTURE.BINANCE": "CSS.USDT.UFUTURE.BINANCE"})
        stored["C20.USDT.UFUTURE.BINANCE"] = "0.0"
        backend.tables["weights"] = stored
        report = audit_weight_columns(backend, "result", "weights", **READ_RANGE)
        self.assertEqual(report["source_assets"], ["BTC", "ETH", "OLD"])
        self.assertEqual(report["unexpected_columns"], ["C20.USDT.UFUTURE.BINANCE", "CSS.USDT.UFUTURE.BINANCE"])
        self.assertEqual(report["missing_columns"], ["OLD.USDT.UFUTURE.BINANCE"])
        self.assertEqual(backend.column_reads, [("result", "2017-12-30", "2017-12-31", ["asset"])])
        self.assertEqual(backend.reads, [("weights", "2017-12-30", "2017-12-31")])
        self.assertFalse(backend.registers or backend.writes or backend.updates or backend.appends)

    def test_readonly_audit_preserves_c20_if_it_is_an_actual_source_asset(self):
        source = result_frame([("2017-12-31 00:00:00", "C20", "new", "利好", "高")])
        backend = Tables(pd.DataFrame(), source)
        backend.tables["weights"] = build_weight_data(build_basic_df(source), start_time="2017-12-31")
        report = audit_weight_columns(backend, "result", "weights", **READ_RANGE)
        self.assertEqual(report["source_assets"], ["C20"])
        self.assertEqual(report["expected_columns"], ["C20.USDT.UFUTURE.BINANCE"])
        self.assertEqual(report["unexpected_columns"], [])
        self.assertEqual(report["missing_columns"], [])
        self.assertFalse(backend.registers or backend.writes)

    def test_audit_entrypoint_loads_event_config_without_writing(self):
        module = importlib.import_module("entrypoints.audit_weight_columns")
        backend = Tables(pd.DataFrame(), sample_results())
        table_path = prepare_weight_grid("weights").variants[0].table_path
        backend.tables[table_path] = build_weight_data(build_basic_df(sample_results()), start_time="2017-12-31")
        settings = EventConfig("first", "result", "round2.yaml", "round3.yaml", weight_path="weights", **READ_RANGE)
        with patch.object(module, "load_event_config", return_value=settings), patch("builtins.print") as output:
            report = module.main([], cli=backend)
        self.assertEqual(json.loads(output.call_args.args[0]), report)
        self.assertEqual(report["unexpected_columns"], [])
        self.assertFalse(backend.registers or backend.writes)

    def test_reads_only_required_columns_and_writes_new_table(self):
        backend = Tables(pd.DataFrame(), sample_results())
        weight = write_weight_data(backend, "result", "weights", **READ_RANGE)
        self.assertEqual(backend.column_reads, [("result", "2017-12-30", "2017-12-31", SOURCE_COLUMNS)])
        self.assertEqual(backend.reads, [("weights", None, None)])
        self.assertEqual(backend.registers, ["weights"])
        self.assertEqual(len(backend.writes), 1)
        self.assertFalse(backend.updates or backend.appends)
        submitted = backend.writes[0]
        self.assertIsInstance(submitted.index, pd.DatetimeIndex)
        self.assertEqual(str(submitted.index.dtype), "datetime64[ns]")
        self.assertEqual(submitted.index.name, "date")
        self.assertTrue(submitted.index.equals(submitted.index.floor("h")))
        self.assertTrue(all(isinstance(value, str) for value in submitted.to_numpy().flat))
        self.assertEqual(submitted.index[0], pd.Timestamp("2017-12-30 08:00:00"))
        self.assertEqual(submitted.index[-1], pd.Timestamp("2018-01-01 08:00:00"))
        self.assertEqual(submitted.loc[pd.Timestamp("2017-12-31 08:00:00"), "BTC" + ASSET_SUFFIX], "0.0")
        self.assertEqual(submitted.loc[pd.Timestamp("2017-12-31 09:00:00"), "BTC" + ASSET_SUFFIX], "4.5")
        assert_frame_equal(backend.tables["weights"], weight)

    def test_source_path_cannot_be_used_as_weight_target(self):
        backend = Tables(pd.DataFrame(), sample_results())
        with self.assertRaises(ValueError):
            write_weight_data(backend, "result", "result", **READ_RANGE)
        self.assertFalse(backend.registers or backend.writes or backend.column_reads)

    def test_write_shifts_only_final_index_eight_hours_once_and_keeps_source_unchanged(self):
        source = result_frame([
            ("2017-12-31 15:20:00", "BTC", "new", "利好", "高"),
            ("2017-12-31 15:40:00", "BTC", "update", "利空", "低"),
        ])
        original = source.copy(deep=True)
        raw_weight = build_weight_data(build_basic_df(source), start_time="2017-12-31", end_time="2017-12-31")
        expected = raw_weight.copy()
        expected.index += pd.Timedelta(hours=8)
        backend = Tables(pd.DataFrame(), source)
        for _ in range(2):
            saved = write_weight_data(backend, "result", "weights", start_time="2017-12-31", end_time="2017-12-31")
            assert_frame_equal(saved, expected)
            assert_frame_equal(backend.tables["weights"], expected)
        self.assertEqual(saved.loc["2018-01-01 00:00:00", "BTC" + ASSET_SUFFIX], "4.5")
        self.assertEqual(raw_weight.loc["2017-12-31 16:00:00", "BTC" + ASSET_SUFFIX], "4.5")
        assert_frame_equal(source, original)
        self.assertEqual(len(backend.writes), 1)
        self.assertFalse(backend.updates or backend.appends)

    def test_target_connection_error_is_not_treated_as_missing_table(self):
        backend = Tables(pd.DataFrame(), sample_results())
        original_read = backend.read

        def read(path, **kwargs):
            if path == "weights":
                raise ConnectionError("offline")
            return original_read(path, **kwargs)

        with patch.object(backend, "read", side_effect=read):
            with self.assertRaises(ConnectionError):
                write_weight_data(backend, "result", "weights", **READ_RANGE)
        self.assertFalse(backend.registers or backend.writes or backend.appends or backend.updates)

    def test_failed_write_retries_same_df_without_registering_again(self):
        backend = Tables(pd.DataFrame(), sample_results())

        def fail_once(path, frame):
            if len(backend.writes) == 1:
                raise RuntimeError("temporary write failure")
            backend.tables[path] = frame.copy(deep=True)

        backend.write_hook = fail_once
        with patch("common.persistence.time.sleep"):
            weight = write_weight_data(backend, "result", "weights", **READ_RANGE)
        self.assertEqual(backend.registers, ["weights"])
        self.assertEqual(len(backend.writes), 2)
        for batch in backend.writes:
            assert_frame_equal(batch, weight)

    def test_empty_source_does_not_create_table(self):
        backend = Tables(pd.DataFrame(), sample_results().iloc[:0])
        with self.assertRaisesRegex(ValueError, "没有事件"):
            write_weight_data(backend, "result", "weights", **READ_RANGE)
        self.assertFalse(backend.registers or backend.writes)

    def test_write_failure_propagates(self):
        backend = Tables(pd.DataFrame(), sample_results())
        backend.write_hook = lambda path, frame: (_ for _ in ()).throw(RuntimeError("failure"))
        with patch("common.persistence.time.sleep"), self.assertRaises(BatchPersistenceError):
            write_weight_data(backend, "result", "weights", **READ_RANGE)
        self.assertEqual(backend.registers, ["weights"])
        self.assertEqual(len(backend.writes), 3)

    def test_updates_overlap_appends_hours_and_preserves_outside_rows_and_boundary(self):
        source = result_frame([
            ("2018-01-01 00:20:00", "BTC", "new", "利好", "低"),
            ("2018-01-01 01:10:00", "BTC", "new", "利好", "高"),
        ])
        backend = Tables(pd.DataFrame(), source)
        column = "BTC" + ASSET_SUFFIX
        backend.tables["weights"] = pd.DataFrame({column: ["7.0", "3.0", "99.0", "8.0"]},
            index=pd.DatetimeIndex(["2018-01-01 07:00:00", "2018-01-01 08:00:00",
                                    "2018-01-01 09:00:00", "2018-01-02 10:00:00"], name="date"))
        saved = write_weight_data(backend, "result", "weights", start_time="2018-01-01", end_time="2018-01-01")
        actual = backend.tables["weights"]
        self.assertEqual(saved.index[0], pd.Timestamp("2018-01-01 08:00:00"))
        self.assertEqual(saved.iloc[0, 0], "3.0")
        self.assertEqual(actual.loc["2018-01-01 07:00:00", column], "7.0")
        self.assertEqual(actual.loc["2018-01-01 09:00:00", column], "2.0")
        self.assertEqual(actual.loc["2018-01-01 10:00:00", column], "6.0")
        self.assertEqual(actual.loc["2018-01-02 10:00:00", column], "8.0")
        self.assertEqual(list(backend.updates[0].index), [pd.Timestamp("2018-01-01 09:00:00")])
        self.assertEqual(len(backend.appends[0]), 23)
        self.assertFalse(backend.registers or backend.writes)
        original = actual.copy(deep=True)
        write_weight_data(backend, "result", "weights", start_time="2018-01-01", end_time="2018-01-01")
        assert_frame_equal(backend.tables["weights"], original)
        self.assertEqual((len(backend.updates), len(backend.appends)), (1, 1))

    def test_adjacent_batches_keep_previous_last_hour_at_shifted_boundary(self):
        source = result_frame([
            ("2017-12-31 23:20:00", "BTC", "new", "利好", "高"),
            ("2018-01-01 00:20:00", "BTC", "new", "利空", "低"),
        ])
        backend = Tables(pd.DataFrame(), source)
        column = "BTC" + ASSET_SUFFIX
        write_weight_data(backend, "result", "weights", start_time="2017-12-31", end_time="2017-12-31")
        write_weight_data(backend, "result", "weights", start_time="2018-01-01", end_time="2018-01-01")
        actual = backend.tables["weights"]
        self.assertEqual(actual.loc["2018-01-01 08:00:00", column], "6.0")
        self.assertEqual(actual.loc["2018-01-01 09:00:00", column], "-2.0")
        self.assertEqual(len(actual), 49)
        self.assertTrue(actual.index.is_unique)
        self.assertEqual((len(backend.registers), len(backend.writes), len(backend.appends)), (1, 1, 1))

    def test_new_asset_column_has_text_zeros_in_history_and_old_columns_in_new_rows(self):
        source = result_frame([("2018-01-01 00:20:00", "ETH", "new", "利好", "低")])
        backend = Tables(pd.DataFrame(), source)
        btc, eth = "BTC" + ASSET_SUFFIX, "ETH" + ASSET_SUFFIX
        backend.tables["weights"] = pd.DataFrame({btc: ["5.0", "4.0", "99.0"]},
            index=pd.DatetimeIndex(["2018-01-01 07:00:00", "2018-01-01 08:00:00",
                                    "2018-01-01 09:00:00"], name="date"))
        saved = write_weight_data(backend, "result", "weights", start_time="2018-01-01", end_time="2018-01-01")
        actual = backend.tables["weights"]
        self.assertEqual(list(saved.columns), [btc, eth])
        self.assertEqual(actual.loc["2018-01-01 07:00:00", btc], "5.0")
        self.assertEqual(actual.loc["2018-01-01 07:00:00", eth], "0.0")
        self.assertEqual(actual.loc["2018-01-01 08:00:00", btc], "4.0")
        self.assertEqual(actual.loc["2018-01-01 09:00:00", btc], "0.0")
        self.assertEqual(actual.loc["2018-01-01 09:00:00", eth], "2.0")
        self.assertTrue(all(isinstance(value, str) for value in actual.to_numpy().flat))
        self.assertFalse(backend.registers or backend.writes)

    def test_append_failure_retries_same_new_indices_without_register_or_write(self):
        source = result_frame([("2018-01-01 00:20:00", "BTC", "new", "利好", "高")])
        backend = Tables(pd.DataFrame(), source)
        backend.tables["weights"] = pd.DataFrame({"BTC" + ASSET_SUFFIX: ["0.0"]},
            index=pd.DatetimeIndex(["2018-01-01 08:00:00"], name="date"))

        def fail_once(path, frame):
            if len(backend.appends) == 1:
                raise RuntimeError("temporary append failure")
            backend.append_rows(path, frame)

        backend.append_hook = fail_once
        with patch("common.persistence.time.sleep"):
            write_weight_data(backend, "result", "weights", start_time="2018-01-01", end_time="2018-01-01")
        self.assertEqual(len(backend.appends), 2)
        assert_frame_equal(backend.appends[0], backend.appends[1])
        self.assertEqual(len(backend.tables["weights"]), 25)
        self.assertFalse(backend.registers or backend.writes or backend.updates)

    def test_failed_overlap_update_stops_before_appending_new_hours(self):
        source = result_frame([("2018-01-01 00:20:00", "BTC", "new", "利好", "高")])
        backend = Tables(pd.DataFrame(), source)
        backend.tables["weights"] = pd.DataFrame({"BTC" + ASSET_SUFFIX: ["0.0", "99.0"]},
            index=pd.date_range("2018-01-01 08:00:00", periods=2, freq="h", name="date"))
        backend.update_hook = lambda path, frame: (_ for _ in ()).throw(RuntimeError("update failure"))
        with patch("common.persistence.time.sleep"), self.assertRaises(BatchPersistenceError):
            write_weight_data(backend, "result", "weights", start_time="2018-01-01", end_time="2018-01-01")
        self.assertEqual(len(backend.updates), 3)
        self.assertFalse(backend.appends or backend.registers or backend.writes)

    def test_registered_empty_target_is_initialized_without_registering_again(self):
        backend = Tables(pd.DataFrame(), sample_results())
        backend.tables["weights"] = pd.DataFrame()
        saved = write_weight_data(backend, "result", "weights", **READ_RANGE)
        assert_frame_equal(backend.tables["weights"], saved)
        self.assertEqual(len(backend.writes), 1)
        self.assertFalse(backend.registers)

    def test_existing_non_datetime_index_is_rejected_before_mutations(self):
        backend = Tables(pd.DataFrame(), sample_results())
        backend.tables["weights"] = pd.DataFrame({"BTC" + ASSET_SUFFIX: ["1.0"]},
            index=pd.Index(["2017-12-31 00:00:00"], name="date"))
        with self.assertRaisesRegex(ValueError, "DatetimeIndex"):
            write_weight_data(backend, "result", "weights", **READ_RANGE)
        self.assertFalse(backend.updates or backend.appends or backend.registers or backend.writes)

    def test_cli_uses_event_config_paths_and_dates(self):
        module = importlib.import_module("entrypoints.write_weight_data")
        backend = Tables(pd.DataFrame(), sample_results())
        settings = EventConfig("first", "result", "round2.yaml", "round3.yaml", weight_path="weights", **READ_RANGE)
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(module, "load_event_config", return_value=settings) as load:
                summary = module.main(["--report-dir", str(Path(temporary) / "run")], cli=backend)
        self.assertTrue(load.call_args.args[0].endswith("event_processing_config.yaml"))
        self.assertEqual(backend.column_reads, [("result", "2017-12-30", "2017-12-31", SOURCE_COLUMNS)])
        self.assertEqual(summary["combination_count"], 1)
        self.assertEqual(summary["tables"][0]["hours"], 49)
        self.assertEqual(summary["tables"][0]["assets"], 3)
        self.assertEqual(summary["tables"][0]["table_path"], prepare_weight_grid("weights").variants[0].table_path)
        self.assertNotIn("weights", backend.tables)

    def test_cli_requires_dates_and_weight_path_before_accessing_database(self):
        module = importlib.import_module("entrypoints.write_weight_data")
        settings = EventConfig("first", "result", "round2.yaml", "round3.yaml", weight_path="weights", **READ_RANGE)
        for overrides in ({"start_time": None, "end_time": None}, {"weight_path": ""}):
            with self.subTest(overrides=overrides):
                backend = Tables(pd.DataFrame(), sample_results())
                with patch.object(module, "load_event_config", return_value=replace(settings, **overrides)):
                    with self.assertRaises(ValueError):
                        module.main([], cli=backend)
                self.assertFalse(backend.column_reads or backend.meta_reads or backend.registers or backend.writes)

    def test_only_requested_dates_contribute_assets_and_scores(self):
        source = result_frame([
            ("2017-12-30 23:59:59", "OUTSIDE", "new", "利好", "高"),
            ("2017-12-31 00:00:00", "BTC", "new", "利好", "高"),
            ("2017-12-31 23:59:59.999999999", "BTC", "update", "利空", "中"),
            ("2018-01-01 00:00:00", "FUTURE", "new", "利好", "高"),
        ])
        source.iloc[0, source.columns.get_loc("round3_status")] = "-1"
        source.iloc[-1, source.columns.get_loc("round3_status")] = "-1"
        backend = Tables(pd.DataFrame(), source)
        weight = write_weight_data(backend, "result", "weights", start_time="2017-12-31", end_time="2017-12-31")
        self.assertEqual(list(weight.columns), ["BTC" + ASSET_SUFFIX])
        self.assertEqual(len(weight), 25)
        self.assertEqual(weight.iloc[0, 0], "0.0")
        self.assertEqual(weight.iloc[1, 0], "6.0")
        self.assertEqual(weight.iloc[-1, 0], "-3.0")
        self.assertEqual(backend.column_reads, [("result", "2017-12-31", "2017-12-31", SOURCE_COLUMNS)])

    def test_repeat_cannot_trigger_an_outside_range_history_read(self):
        source = result_frame([
            ("2017-12-30 23:59:59", "BTC", "new", "利好", "高"),
            ("2017-12-31 00:00:00", "BTC", "repeat", None, None),
        ])
        backend = Tables(pd.DataFrame(), source)
        with self.assertRaisesRegex(ValueError, "record1.*无法计算权重"):
            write_weight_data(backend, "result", "weights", start_time="2017-12-31", end_time="2017-12-31")
        self.assertEqual(backend.column_reads, [("result", "2017-12-31", "2017-12-31", SOURCE_COLUMNS)])
        self.assertFalse(backend.registers or backend.writes)

    def test_invalid_read_dates_stop_before_database_access(self):
        for start, end in ((None, None), ("", ""), ("2017-12-31", None),
                           ("bad", "2017-12-31"), ("2018-01-01", "2017-12-31")):
            with self.subTest(start=start, end=end):
                backend = Tables(pd.DataFrame(), sample_results())
                with self.assertRaises(ValueError):
                    write_weight_data(backend, "result", "weights", start_time=start, end_time=end)
                self.assertFalse(backend.column_reads or backend.meta_reads or backend.registers or backend.writes)

    def test_outside_range_returned_by_client_stops_before_register(self):
        backend = Tables(pd.DataFrame(), sample_results())
        with patch.object(backend, "read", return_value=sample_results()):
            with self.assertRaisesRegex(ValueError, "日期以外"):
                write_weight_data(backend, "result", "weights", start_time="2017-12-31", end_time="2017-12-31")
        self.assertFalse(backend.registers or backend.writes)


if __name__ == "__main__":
    unittest.main()
