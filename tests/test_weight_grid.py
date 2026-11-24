"""验证参数笛卡尔积、稳定目标、复用解析及逐表失败恢复；不连接数据库。"""

import importlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import PropertyMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from common.config import EventConfig
from common.persistence import BatchPersistenceError
from postprocessing import weight_data
from postprocessing.weight_data import ASSET_SUFFIX, SOURCE_COLUMNS, build_basic_df, build_weight_data
from postprocessing.weight_grid import run_weight_grid
from postprocessing.weight_parameters import DEFAULT_MAPPINGS, WeightParameters, prepare_weight_grid, select_weight_variant
from tests.test_weight_data import AssetTables as Tables, result_frame


def settings(grid=None, **overrides):
    values = dict(weight_path="weights", start_time="2017-12-31", end_time="2017-12-31", weight_grid=grid,
                  round2_asset_list_path="assets.json")
    values.update(overrides)
    return EventConfig("first", "result", "round2.yaml", "round3.yaml", **values)


def completed_events():
    return result_frame([
        ("2017-12-31 15:20:00", "BTC", "new", "利好", "高"),
        ("2017-12-31 15:40:00", "ETH", "new", "利空", "高"),
    ], execution_stage="已完成")


class WeightParameterTests(unittest.TestCase):
    def test_aggregation_axis_shares_score_identity_and_has_distinct_paths(self):
        summed = WeightParameters()
        maximum = WeightParameters(aggregation_method="max_abs")
        self.assertEqual(summed.param_id, "e795f558c8aacbb8")
        self.assertEqual(summed.parameter_hash, maximum.parameter_hash)
        self.assertEqual(summed.param_id, maximum.param_id)
        self.assertEqual(maximum.to_dict()["aggregation_method"], "max_abs")
        plan = prepare_weight_grid("weights", {"aggregation_methods": ["max_abs", "sum", "max_abs"],
                                                "coefficients": {"B": [1, 2]}})
        self.assertEqual(len(plan.variants), 4)
        self.assertEqual(len({v.table_path for v in plan.variants}), 4)
        self.assertEqual(len({v.parameters.param_id for v in plan.variants}), 2)
        for variant in plan.variants:
            parameters = variant.parameters
            self.assertEqual(parameters.param_id, WeightParameters(B=parameters.B).param_id)
            self.assertEqual(variant.table_path, "weights/{}__B{}_C1_D1__{}".format(
                parameters.aggregation_method, int(parameters.B), parameters.param_id))
        self.assertEqual(plan.candidates["aggregation_methods"], ["max_abs", "sum"])
        reordered = prepare_weight_grid("weights", {"aggregation_methods": ["sum", "max_abs"],
                                                     "coefficients": {"B": [2, 1]}})
        self.assertEqual({v.table_path for v in plan.variants}, {v.table_path for v in reordered.variants})
        explicit_sum = prepare_weight_grid("weights", {"aggregation_methods": ["sum"]})
        self.assertEqual(explicit_sum.variants[0].table_path, prepare_weight_grid("weights").variants[0].table_path)
        for entry in plan.to_dict()["variants"]:
            self.assertIn(entry["aggregation_method"], ("sum", "max_abs"))
        template = "{weight_path}__{aggregation_method}__{param_id}"
        named = prepare_weight_grid("weights", {"aggregation_methods": ["max_abs"],
                                                 "table_name_template": template})
        self.assertIn("__max_abs__", named.variants[0].table_path)

    def test_default_paths_are_children_of_weight_path_with_one_separator(self):
        for base in ("crypto/weights", "crypto/weights/"):
            with self.subTest(weight_path=base):
                plan = prepare_weight_grid(base, {"aggregation_methods": ["sum", "max_abs"],
                                                  "coefficients": {"B": [2], "C": [0.5]}})
                param_id = WeightParameters(B=2, C=0.5).param_id
                self.assertEqual([v.table_path for v in plan.variants], [
                    "crypto/weights/sum__B2_C0.5_D1__" + param_id,
                    "crypto/weights/max_abs__B2_C0.5_D1__" + param_id,
                ])

    def test_templates_require_aggregation_method_even_for_single_mode_runs(self):
        for methods in (["sum"], ["max_abs"], ["sum", "max_abs"]):
            with self.subTest(methods=methods), self.assertRaisesRegex(ValueError, "aggregation_method"):
                prepare_weight_grid("weights", {"aggregation_methods": methods,
                                                "table_name_template": "{weight_path}__{param_id}"})

    def test_select_variant_uses_parameter_id_and_aggregation_method(self):
        plan = prepare_weight_grid("weights", {"aggregation_methods": ["sum", "max_abs"],
                                               "coefficients": {"B": [1, 2]}})
        param_id = WeightParameters(B=2).param_id
        with self.assertRaisesRegex(ValueError, "aggregation-method"):
            select_weight_variant(plan, param_id=param_id)
        for method in ("sum", "max_abs"):
            selected = select_weight_variant(plan, param_id=param_id, aggregation_method=method)
            self.assertEqual(selected.parameters.B, 2)
            self.assertEqual(selected.parameters.aggregation_method, method)
        with self.assertRaises(ValueError):
            select_weight_variant(plan, param_id="missing", aggregation_method="sum")
        with self.assertRaises(ValueError):
            select_weight_variant(plan, param_id=param_id, aggregation_method="unknown")

    def test_invalid_aggregation_candidates_and_combination_limit_are_rejected(self):
        for methods in ([], None, "max_abs", ["unknown"], [True], [1], [[]], [{}]):
            with self.subTest(methods=methods), self.assertRaisesRegex(ValueError, "aggregation"):
                prepare_weight_grid("weights", {"aggregation_methods": methods})
        with self.assertRaisesRegex(ValueError, "aggregation_method"):
            WeightParameters(aggregation_method="unknown")
        with self.assertRaisesRegex(ValueError, "4.*3"):
            prepare_weight_grid("weights", {"aggregation_methods": ["sum", "max_abs"],
                                             "coefficients": {"B": [1, 2]}, "max_combinations": 3})

    def test_defaults_and_independent_coefficient_and_enum_axes(self):
        default = prepare_weight_grid("weights")
        self.assertEqual(len(default.variants), 1)
        self.assertEqual(default.variants[0].parameters.to_dict()["mappings"],
                         {key: value for key, value in DEFAULT_MAPPINGS.items() if key != "persistence"})
        plan = prepare_weight_grid("weights", {
            "coefficients": {"B": [1, 2], "C": [1, 2]},
            "mappings": {"materiality": {"高": [3, 4]}, "direction": {"利好": [1, 2]}},
        })
        self.assertEqual(len(plan.variants), 16)
        self.assertEqual(len({variant.table_path for variant in plan.variants}), 16)
        self.assertEqual(len({variant.parameters.param_id for variant in plan.variants}), 16)
        # B=1,C=2 和 B=2,C=1 分属独立方案，即使评分相同。
        self.assertEqual(sum(variant.parameters.B * variant.parameters.C == 2 for variant in plan.variants), 8)

    def test_array_reordering_numeric_dedup_and_key_order_keep_table_identity(self):
        config = {"coefficients": {"B": [2, 1.0, 1]},
                  "mappings": {"direction": {"利好": [2, 1]}, "materiality": {"高": [4, 3]}}}
        reordered = {"mappings": {"materiality": {"高": [3.0, 4]}, "direction": {"利好": [1, 2.0]}},
                     "coefficients": {"B": [1, 2]}}
        one, two = prepare_weight_grid("weights", config), prepare_weight_grid("weights", reordered)
        self.assertEqual(len(one.variants), 8)
        self.assertEqual({v.table_path for v in one.variants}, {v.table_path for v in two.variants})
        self.assertEqual(len(prepare_weight_grid("weights", {"coefficients": {"B": [-0.0, 0, 0.0]}}).variants), 1)

    def test_inactive_persistence_has_no_effect_on_score_identity_or_count(self):
        default = prepare_weight_grid("weights")
        changed = prepare_weight_grid("weights", {"mappings": {"persistence": {"长期": [-3, 1, 10000]}}})
        self.assertEqual(len(changed.variants), 1)
        self.assertEqual(default.variants[0].to_dict(), changed.variants[0].to_dict())
        self.assertNotIn("persistence", changed.variants[0].parameters.to_dict()["mappings"])
        self.assertEqual(changed.to_dict()["candidates"]["mappings"]["persistence"]["长期"], [-3, 1, 10000])
        assert_frame_equal(build_basic_df(completed_events(), default.variants[0].parameters),
                           build_basic_df(completed_events(), changed.variants[0].parameters))

    def test_full_effective_mapping_and_scoring_version_are_part_of_hash(self):
        original = WeightParameters()
        # 即使本批没有“低”事件，改变其映射仍须定位另一张表。
        changed = WeightParameters(mappings={"materiality": {"低": 2}})
        self.assertNotEqual(original.param_id, changed.param_id)
        with patch("postprocessing.weight_parameters.SCORING_VERSION", "future_version"):
            self.assertNotEqual(original.param_id, changed.param_id)
            future_id = original.param_id
        self.assertNotEqual(future_id, original.param_id)
        self.assertEqual(len(original.param_id), 16)
        self.assertEqual(original.param_id, original.parameter_hash[:16])

    def test_scalar_parameters_are_immutable_and_validate_finite_values(self):
        parameters = WeightParameters(mappings={"materiality": {"高": 9}})
        with self.assertRaises(TypeError):
            parameters.mappings["materiality"]["高"] = 2
        for value in (float("inf"), float("nan"), "2", True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                WeightParameters(C=value)

    def test_explicit_invalid_arrays_and_values_are_rejected_including_persistence(self):
        invalid = ([], None, 1, (1,), [True], ["1"], [float("inf")], [float("nan")], [10 ** 400])
        for value in invalid:
            for config in ({"coefficients": {"B": value}},
                           {"mappings": {"execution_stage": {"已完成": value}}},
                           {"mappings": {"persistence": {"长期": value}}}):
                with self.subTest(config=config), self.assertRaises(ValueError):
                    prepare_weight_grid("weights", config)

    def test_invalid_structure_unknown_keys_limit_and_templates_are_rejected(self):
        cases = [[], {"coefficients": None}, {"coefficients": {"A": [1]}},
                 {"mappings": {"typo": {}}}, {"mappings": {"direction": []}}, {"typo": 1},
                 {"mappings": {"direction": {3: [1]}}}]
        cases.extend({"max_combinations": value} for value in (0, -1, True, 2.5, "1000"))
        cases.extend({"table_name_template": template} for template in
                     ("", "{weight_path}", "{param_id}_{A}", "{param_id.x}", "{param_id!r}",
                      "{param_id:4}", "{param_id", "{{param_id}}"))
        for config in cases:
            with self.subTest(config=config), self.assertRaises(ValueError):
                prepare_weight_grid("weights", config)
        with self.assertRaisesRegex(ValueError, "1001.*1000"):
            prepare_weight_grid("weights", {"coefficients": {"B": list(range(1001))}})

    def test_custom_template_and_path_collisions_checked_before_execution(self):
        config = {"table_name_template": "{weight_path}/{aggregation_method}/{param_id}/B{B}C{C}D{D}"}
        variant = prepare_weight_grid("weights", config).variants[0]
        self.assertEqual(variant.table_path, "weights/sum/{}/B1C1D1".format(variant.parameters.param_id))
        with self.assertRaisesRegex(ValueError, "冲突"):
            prepare_weight_grid("weights", config, result_path=variant.table_path)
        with patch.object(WeightParameters, "param_id", new_callable=PropertyMock, return_value="same"):
            with self.assertRaisesRegex(ValueError, "冲突"):
                prepare_weight_grid("weights", {"coefficients": {"B": [1, 2]},
                                                "table_name_template": "{aggregation_method}/{param_id}"})
            with self.assertRaisesRegex(ValueError, "冲突"):
                prepare_weight_grid("sum/same", {"table_name_template": "{aggregation_method}/{param_id}"})


class WeightScoringTests(unittest.TestCase):
    def test_completed_high_new_positive_and_negative_default_scores_are_18(self):
        basic = build_basic_df(completed_events(), WeightParameters())
        self.assertEqual(list(basic["data"]), [18, -18])
        self.assertEqual(list(basic.columns), ["date", "asset", "data"])

    def test_all_execution_stages_uncertainty_zero_and_negative_parameters(self):
        for stage, factor in (("未开始", 1), ("执行中", 2), ("已完成", 3), ("不确定", 0)):
            source = result_frame([("2017-12-31 15:20:00", "BTC", "new", "利好", "高")], execution_stage=stage)
            self.assertEqual(build_basic_df(source).iloc[0]["data"], 6 * factor)
        for field, value in (("direction", "不确定"), ("materiality", "不确定"), ("direction", "中性")):
            source = completed_events().iloc[:1].copy()
            response = json.loads(source.iloc[0]["round3_ai_response"])
            body = json.loads(response["ai_response_content"])
            body[field] = value
            response["ai_response_content"] = json.dumps(body)
            source.iloc[0, source.columns.get_loc("round3_ai_response")] = json.dumps(response)
            self.assertEqual(build_basic_df(source).iloc[0]["data"], 0)
        self.assertEqual(list(build_basic_df(completed_events(), WeightParameters(B=-2, C=3, D=0.5))["data"]), [-54, 54])
        parameters = WeightParameters(mappings={"materiality": {"高": -2}, "execution_stage": {"已完成": 4}})
        self.assertEqual(list(build_basic_df(completed_events(), parameters)["data"]), [-16, 16])

    def test_zero_multiplier_short_circuits_intermediate_overflow(self):
        huge = {"materiality": {"高": 1e308}}
        for coefficient in ("B", "C", "D"):
            values = {"B": 1e308, "C": 1e308, "D": 1e308, coefficient: 0}
            parameters = WeightParameters(mappings=huge, **values)
            self.assertEqual(list(build_basic_df(completed_events(), parameters)["data"]), [0, 0])
        neutral = result_frame([("2017-12-31 15:20:00", "BTC", "new", "中性", "高")], execution_stage="已完成")
        self.assertEqual(build_basic_df(neutral, WeightParameters(B=1e308, mappings=huge)).iloc[0]["data"], 0)

    def test_unknown_direction_uses_configured_other_but_unmapped_stage_fails(self):
        source = result_frame([("2017-12-31 15:20:00", "BTC", "new", "额外方向", "高")])
        self.assertEqual(build_basic_df(source).iloc[0]["data"], 0)
        self.assertEqual(build_basic_df(source, WeightParameters(mappings={"direction": {"其他": 2}})).iloc[0]["data"], 12)
        source = result_frame([("2017-12-31 15:20:00", "BTC", "new", "利好", "高")], execution_stage="额外阶段")
        with self.assertRaisesRegex(ValueError, "record0.*execution_stage"):
            build_basic_df(source)
        self.assertEqual(build_basic_df(source, WeightParameters(mappings={"execution_stage": {"额外阶段": 4}})).iloc[0]["data"], 24)

    def test_repeat_uses_own_analysis_fields_and_relation(self):
        source = result_frame([
            ("2017-12-31 15:00:00", "BTC", "new", "利好", "高"),
            ("2017-12-31 15:20:00", "BTC", "repeat", "利空", "低"),
        ], execution_stage="已完成")
        self.assertEqual(list(build_basic_df(source)["data"]), [18, -1.5])
        own = result_frame([("2017-12-31 16:20:00", "BTC", "repeat", "利空", "低")], execution_stage="执行中")
        own.index = pd.MultiIndex.from_tuples([("2017-12-31 16:20:00", "own")], names=["date", "record_id"])
        self.assertEqual(list(build_basic_df(pd.concat([source, own]))["data"]), [18, -1.5, -1])

    def test_nonfinite_event_scores_and_hourly_sums_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "溢出"):
            build_basic_df(completed_events(), WeightParameters(B=1e308))
        basic = pd.DataFrame({"date": ["2017-12-31 15:20:00"] * 2,
                              "asset": ["BTC"] * 2, "data": [1e308, 1e308]})
        with self.assertRaisesRegex(ValueError, "溢出"):
            build_weight_data(basic, start_time="2017-12-31")
        basic.loc[0, "data"] = float("inf")
        with self.assertRaisesRegex(ValueError, "非有限"):
            build_weight_data(basic, start_time="2017-12-31")


class WeightGridExecutionTests(unittest.TestCase):
    def test_grid_loads_assets_once_filters_before_parse_and_preserves_host_columns(self):
        outside = completed_events().iloc[:1].copy()
        outside.index = pd.MultiIndex.from_tuples([("2017-12-31 15:30:00", "outside")], names=outside.index.names)
        outside["asset"] = "OUTSIDE"
        outside["round3_ai_response"] = "broken"
        backend = Tables(pd.DataFrame(), pd.concat([completed_events(), outside]),
                         assets=["ETH.USDT.CUSTOM", "SOL", "BTC"])
        config = settings({"coefficients": {"B": [1, 2]}, "aggregation_methods": ["sum", "max_abs"]})
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(weight_data, "_parse_impact", wraps=weight_data._parse_impact) as parse:
                summary = run_weight_grid(backend, config, report_dir=Path(temporary) / "run")
            manifest = json.loads(Path(summary["manifest_path"]).read_text(encoding="utf-8"))
        self.assertEqual(parse.call_count, 2)
        self.assertEqual(backend.object_reads, ["assets.json"])
        self.assertEqual(manifest["output_columns"], ["ETH.USDT.CUSTOM", "SOL" + ASSET_SUFFIX, "BTC" + ASSET_SUFFIX])
        for table in summary["tables"]:
            weight = backend.tables[table["table_path"]]
            self.assertEqual(list(weight.columns), manifest["output_columns"])
            self.assertTrue(weight["SOL" + ASSET_SUFFIX].eq(0).all())
            self.assertLess(weight.loc["2018-01-01 00:00", "ETH.USDT.CUSTOM"], 0)

    def test_empty_asset_window_produces_zero_grid_without_falling_back_to_events(self):
        backend = Tables(pd.DataFrame(), completed_events(), assets=["SOL", "ADA"])
        with tempfile.TemporaryDirectory() as temporary:
            summary = run_weight_grid(backend, settings({"aggregation_methods": ["sum", "max_abs"]}),
                                      report_dir=Path(temporary) / "run")
        for entry in summary["tables"]:
            weight = backend.tables[entry["table_path"]]
            self.assertEqual(weight.shape, (26, 2))
            self.assertEqual(list(weight.columns), ["SOL" + ASSET_SUFFIX, "ADA" + ASSET_SUFFIX])
            self.assertTrue(weight.eq(0).all().all())

    def test_explicit_assets_override_config_without_reading_asset_object(self):
        backend = Tables(pd.DataFrame(), completed_events())
        with tempfile.TemporaryDirectory() as temporary:
            summary = run_weight_grid(backend, settings(round2_asset_list_path=""), assets=["ETH", "BTC"],
                                      report_dir=Path(temporary) / "run")
            manifest = json.loads(Path(summary["manifest_path"]).read_text(encoding="utf-8"))
        self.assertEqual(backend.object_reads, [])
        self.assertIsNone(manifest["asset_list_path"])
        self.assertEqual(list(backend.tables[summary["tables"][0]["table_path"]].columns),
                         ["ETH" + ASSET_SUFFIX, "BTC" + ASSET_SUFFIX])

    def test_missing_or_invalid_asset_list_stops_before_source_and_target_access(self):
        for path, assets, error in (("", ["BTC"], ValueError), ("missing.json", ["BTC"], FileNotFoundError),
                                     ("assets.json", [], ValueError),
                                     ("assets.json", ["BTC", "btc.usdt.custom"], ValueError)):
            with self.subTest(path=path, assets=assets), tempfile.TemporaryDirectory() as temporary:
                backend = Tables(pd.DataFrame(), completed_events(), assets=assets)
                with self.assertRaises(error):
                    run_weight_grid(backend, settings(round2_asset_list_path=path),
                                    report_dir=Path(temporary) / "run")
                self.assertFalse(backend.column_reads or backend.reads or backend.registers or backend.writes)

    def test_both_aggregations_write_separate_tables_and_rerun_without_accumulating(self):
        source = result_frame([
            ("2017-12-31 15:10:00", "BTC", "new", "利好", "中"),
            ("2017-12-31 15:20:00", "BTC", "new", "利空", "高"),
            ("2017-12-31 15:30:00", "BTC", "update", "利好", "高"),
        ], execution_stage="已完成")
        backend = Tables(pd.DataFrame(), source)
        config = settings({"aggregation_methods": ["sum", "max_abs"]})
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with patch.object(weight_data, "_parse_impact", wraps=weight_data._parse_impact) as parse:
                summary = run_weight_grid(backend, config, report_dir=directory / "first")
            self.assertEqual(parse.call_count, 3)
            manifest = json.loads(Path(summary["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["candidates"]["aggregation_methods"], ["sum", "max_abs"])
            self.assertEqual(len({table["param_id"] for table in summary["tables"]}), 1)
            self.assertEqual({table["table_path"] for table in summary["tables"]}, {
                "weights/sum__B1_C1_D1__e795f558c8aacbb8",
                "weights/max_abs__B1_C1_D1__e795f558c8aacbb8",
            })
            for table in summary["tables"]:
                expected = 7.5 if table["aggregation_method"] == "sum" else -18.0
                saved = backend.tables[table["table_path"]]
                self.assertEqual(saved.loc["2018-01-01 00:00:00", "BTC" + ASSET_SUFFIX], expected)
                self.assertEqual(len(saved), 26)
                self.assertTrue(saved.dtypes.eq("float64").all())
            before = (len(backend.writes), len(backend.updates), len(backend.appends))
            run_weight_grid(backend, config, report_dir=directory / "again")
            self.assertEqual(before, (len(backend.writes), len(backend.updates), len(backend.appends)))
        self.assertEqual(summary["completed"], 2)
        self.assertEqual(len(backend.registers), 2)

    def test_grid_reads_and_parses_source_once_and_records_full_manifest(self):
        config = settings({"coefficients": {"B": [1, 2], "C": [1, 2]},
                           "mappings": {"materiality": {"高": [3, 4]}}})
        failed = result_frame([("2017-12-31 15:30:00", "FAILED", "uncertain", None, None)])
        failed = failed.rename(index={"record0": "failed"}, level="record_id")
        failed["round3_status"] = "-1"
        backend = Tables(pd.DataFrame(), pd.concat([completed_events(), failed]))
        backend.tables["weights"] = pd.DataFrame({"legacy": ["keep"]})
        original = backend.tables["weights"].copy()
        with patch("postprocessing.weight_parameters.SCORING_VERSION", "direction_product_v2"):
            old_path = prepare_weight_grid("weights").variants[0].table_path
        backend.tables[old_path] = pd.DataFrame({"BTC" + ASSET_SUFFIX: ["18.0"]},
            index=pd.DatetimeIndex(["2017-12-31 16:00:00"], name="date"))
        old_table = backend.tables[old_path].copy(deep=True)
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(weight_data, "_parse_impact", wraps=weight_data._parse_impact) as parse:
                summary = run_weight_grid(backend, config, report_dir=Path(temporary) / "run")
            manifest = json.loads(Path(summary["manifest_path"]).read_text(encoding="utf-8"))
        self.assertEqual(parse.call_count, 2)
        self.assertEqual(backend.column_reads, [("result", "2017-12-30", "2018-01-01", SOURCE_COLUMNS)])
        self.assertEqual(summary["combination_count"], 8)
        self.assertEqual(manifest["completed"], 8)
        self.assertEqual(manifest["status"], "completed")
        self.assertEqual(manifest["source_rows"], 2)
        self.assertEqual(manifest["output_time_offset_hours"], 8)
        self.assertEqual(len(backend.registers), 8)
        assert_frame_equal(backend.tables["weights"], original)
        assert_frame_equal(backend.tables[old_path], old_table)
        for entry in manifest["variants"]:
            self.assertEqual(entry["status"], "completed")
            self.assertIn("execution_stage", entry["parameters"]["mappings"])
            table = backend.tables[entry["table_path"]]
            coefficient = entry["parameters"]["coefficients"]
            expected = 6 * entry["parameters"]["mappings"]["materiality"]["高"] * coefficient["B"] * coefficient["C"]
            self.assertEqual(table.loc["2018-01-01 00:00:00", "BTC" + ASSET_SUFFIX], float(expected))
            self.assertEqual(table.loc["2018-01-01 00:00:00", "ETH" + ASSET_SUFFIX], float(-expected))
            self.assertEqual(entry["start_time"], "2017-12-31 08:00:00")
            self.assertEqual(entry["end_time"], "2018-01-01 09:00:00")
            self.assertIsInstance(table.index, pd.DatetimeIndex)
            self.assertEqual(table.index.name, "date")
            self.assertTrue(table.index.equals(table.index.floor("h")))
            self.assertTrue(table.dtypes.eq("float64").all())
            self.assertEqual(len(table), 26)

    def test_failed_second_table_stops_and_rerun_updates_same_targets(self):
        config = settings({"coefficients": {"B": [1, 2, 3]}})
        plan = prepare_weight_grid("weights", config.weight_grid)
        backend = Tables(pd.DataFrame(), completed_events())
        failed_path = plan.variants[1].table_path

        def fail_second(path, frame):
            if path == failed_path:
                raise RuntimeError("second table unavailable")
            backend.tables[path] = frame.copy(deep=True)

        backend.write_hook = fail_second
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with patch("common.persistence.time.sleep"), self.assertRaises(BatchPersistenceError):
                run_weight_grid(backend, config, report_dir=directory / "failed")
            manifest = json.loads((directory / "failed" / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "failed")
            self.assertEqual(manifest["completed"], 1)
            self.assertEqual([entry["status"] for entry in manifest["variants"]], ["completed", "failed", "pending"])
            self.assertEqual(manifest["variants"][1]["error"]["type"], "BatchPersistenceError")
            self.assertNotIn(plan.variants[2].table_path, backend.tables)
            preserved = backend.tables[plan.variants[0].table_path].copy(deep=True)
            backend.write_hook = None
            summary = run_weight_grid(backend, config, report_dir=directory / "retry")
            self.assertEqual(summary["completed"], 3)
            assert_frame_equal(backend.tables[plan.variants[0].table_path], preserved)
            before = {v.table_path: backend.tables[v.table_path].copy(deep=True) for v in plan.variants}
            changes = (len(backend.writes), len(backend.updates), len(backend.appends))
            run_weight_grid(backend, config, report_dir=directory / "again")
            self.assertEqual((len(backend.writes), len(backend.updates), len(backend.appends)), changes)
            for path, frame in before.items():
                assert_frame_equal(backend.tables[path], frame)
        self.assertEqual(backend.registers, [v.table_path for v in plan.variants])

    def test_different_date_ranges_share_parameter_tables_and_protect_shifted_boundary(self):
        source = result_frame([
            ("2017-12-31 23:20:00", "BTC", "new", "利好", "高"),
            ("2018-01-01 00:20:00", "BTC", "new", "利空", "低"),
        ], execution_stage="已完成")
        config = settings({"coefficients": {"B": [1, 2]}, "aggregation_methods": ["sum", "max_abs"]})
        backend = Tables(pd.DataFrame(), source)
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            first = run_weight_grid(backend, config, report_dir=directory / "first")
            second = run_weight_grid(backend, replace(config, start_time="2018-01-01", end_time="2018-01-01"),
                                     report_dir=directory / "second")
        self.assertEqual([t["table_path"] for t in first["tables"]], [t["table_path"] for t in second["tables"]])
        for variant in prepare_weight_grid("weights", config.weight_grid).variants:
            table = backend.tables[variant.table_path]
            self.assertEqual(len(table), 50)
            self.assertTrue(table.index.is_unique)
            self.assertEqual(table.loc["2018-01-01 08:00:00", "BTC" + ASSET_SUFFIX], 18 * variant.parameters.B)
            self.assertEqual(table.loc["2018-01-01 09:00:00", "BTC" + ASSET_SUFFIX], -6 * variant.parameters.B)

    def test_overflow_records_failure_without_creating_or_writing_target(self):
        backend = Tables(pd.DataFrame(), completed_events())
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "overflow"
            with self.assertRaisesRegex(ValueError, "溢出"):
                run_weight_grid(backend, settings({"coefficients": {"B": [1e308]}}), report_dir=output)
            manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "failed")
            self.assertEqual(manifest["variants"][0]["status"], "failed")
        self.assertFalse(backend.registers or backend.writes or backend.updates or backend.appends)

    def test_dry_run_never_imports_database_client_or_creates_reports(self):
        module = importlib.import_module("entrypoints.write_weight_data")
        config = settings({"aggregation_methods": ["sum", "max_abs"]}, start_time=None, end_time=None)
        with tempfile.TemporaryDirectory() as temporary:
            report = Path(temporary) / "must_not_exist"
            with patch.object(module, "load_event_config", return_value=config), patch("builtins.print") as output:
                with patch.dict("sys.modules", {"xdata": None, "xdata.client": None}):
                    preview = module.main(["--dry-run", "--report-dir", str(report)])
            self.assertFalse(report.exists())
        self.assertEqual(preview["combination_count"], 2)
        self.assertEqual({v["aggregation_method"] for v in preview["variants"]}, {"sum", "max_abs"})
        self.assertEqual(json.loads(output.call_args.args[0]), preview)

    def test_invalid_grid_and_excessive_combinations_stop_before_database_import(self):
        module = importlib.import_module("entrypoints.write_weight_data")
        grids = ({"coefficients": {"B": list(range(1001))}},
                 {"coefficients": {"B": [float("inf")]}},
                 {"aggregation_methods": ["unknown"]},
                 {"aggregation_methods": []},
                 {"table_name_template": "{weight_path}"})
        with patch.dict("sys.modules", {"xdata": None, "xdata.client": None}):
            for grid in grids:
                with self.subTest(grid=grid), patch.object(module, "load_event_config", return_value=settings(grid)):
                    with self.assertRaises(ValueError):
                        module.main([])


if __name__ == "__main__":
    unittest.main()
