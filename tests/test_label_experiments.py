"""验证标签实验的控制变量、等信号分组、小时对齐及逐表写入；不访问真实服务。"""

import importlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal

from common.config import EventConfig
from common.persistence import BatchPersistenceError
from postprocessing.label_experiments import (
    build_label_positions, prepare_label_experiments, run_label_experiments,
)
from postprocessing.weight_data import ASSET_SUFFIX, SOURCE_COLUMNS, parse_weight_features
from postprocessing.weight_parameters import WeightParameters, prepare_weight_grid
from tests.test_weight_data import AssetTables as Tables, result_frame


RANGE = {"start_time": "2023-01-01", "end_time": "2023-01-01"}


def source_events():
    source = result_frame([
        ("2023-01-01 00:10:00", "BTC", "new", "利好", "高"),
        ("2023-01-01 00:20:00", "BTC", "update", "利空", "低"),
        ("2023-01-01 00:30:00", "ETH", "repeat", "利好", "中"),
        ("2023-01-01 01:00:00", "ETH", "new", "中性", "高"),
        ("2023-01-01 01:10:00", "LTC", "new", "利好", "不确定"),
    ])
    for position, (stage, persistence) in enumerate([
        ("已完成", "长期"), ("未开始", "短期"), ("执行中", "中期"),
        ("已完成", "不确定"), ("不确定", "长期"),
    ]):
        body = json.loads(json.loads(source.iloc[position]["round3_ai_response"])["ai_response_content"])
        body.update(execution_stage=stage, persistence=persistence)
        source.iloc[position, source.columns.get_loc("round3_ai_response")] = json.dumps(
            {"ai_response_content": json.dumps(body, ensure_ascii=False)}, ensure_ascii=False)
    return source


def settings():
    return EventConfig("first", "result", "round2.yaml", "round3.yaml", weight_path="weights",
                       round2_asset_list_path="assets.json", **RANGE)


class LabelExperimentTests(unittest.TestCase):
    def setUp(self):
        self.plan = prepare_label_experiments("weights")
        self.entries = {entry["name"]: entry for entry in self.plan["variants"]}
        self.features = parse_weight_features(source_events())

    def test_runtime_uses_fixed_assets_for_all_label_groups_even_without_signals(self):
        for assets, successful in ((["SOL", "BTC"], 2), (["SOL", "ADA"], 0)):
            with self.subTest(assets=assets), tempfile.TemporaryDirectory() as temporary:
                backend = Tables(pd.DataFrame(), source_events(), assets=assets)
                summary = run_label_experiments(backend, settings(), report_dir=Path(temporary) / "run")
                manifest = json.loads(Path(summary["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(backend.object_reads, ["assets.json"])
            self.assertEqual(manifest["successful_events"], successful)
            for entry in summary["tables"]:
                weight = backend.tables[entry["table_path"]]
                self.assertEqual(list(weight.columns), [asset + ASSET_SUFFIX for asset in assets])
                self.assertTrue(weight["SOL" + ASSET_SUFFIX].eq(0).all())
                if not successful:
                    self.assertTrue(weight.eq(0).all().all())

    def positions(self, name):
        return build_label_positions(self.features, self.entries[name], **RANGE)

    def test_eight_ablation_scores_and_explicit_disabled_unknown_levels(self):
        expected = {"D": 0, "DR": 0.5, "DM": 2, "DE": 2,
                    "DRM": 4.5, "DRE": 4.5, "DME": 8, "DRME": 16.5}
        for name, value in expected.items():
            with self.subTest(name=name):
                matrix = self.positions(name)
                self.assertEqual(matrix.loc["2023-01-01 09:00:00", "BTC" + ASSET_SUFFIX], value)
                self.assertEqual(len(matrix), 26)
                self.assertTrue(matrix.dtypes.eq("float64").all())
        self.assertEqual(self.positions("D").loc["2023-01-01 10:00:00", "LTC" + ASSET_SUFFIX], 1)
        self.assertEqual(self.positions("DRME").loc["2023-01-01 10:00:00", "LTC" + ASSET_SUFFIX], 0)

    def test_level_groups_use_equal_event_signals_and_add_up_to_direction_only(self):
        self.assertEqual(self.plan["combination_count"], 25)
        for field in ("direction", "materiality", "execution_stage", "event_relation", "persistence"):
            entries = [entry for entry in self.plan["variants"] if entry["filter_field"] == field]
            matrices = [self.positions(entry["name"]) for entry in entries]
            assert_frame_equal(sum(matrices[1:], matrices[0].copy()), self.positions("D"))
        high = self.positions("level_materiality_high")
        low = self.positions("level_materiality_low")
        self.assertEqual(high.loc["2023-01-01 09:00:00", "BTC" + ASSET_SUFFIX], 1)
        self.assertEqual(low.loc["2023-01-01 09:00:00", "BTC" + ASSET_SUFFIX], -1)
        self.assertEqual(self.positions("level_materiality_uncertain").iloc[2]["LTC" + ASSET_SUFFIX], 1)
        self.assertFalse(self.positions("level_persistence_uncertain").to_numpy().any())
        self.assertEqual(list(high.columns), list(self.positions("D").columns))

    def test_custom_direction_magnitudes_are_removed_only_for_equal_groups(self):
        plan = prepare_label_experiments("weights", {"mappings": {"direction": {"利好": [5], "利空": [-2]}}})
        entries = {entry["name"]: entry for entry in plan["variants"]}
        ablation = build_label_positions(self.features, entries["D"], **RANGE)
        group = build_label_positions(self.features, entries["level_materiality_high"], **RANGE)
        self.assertEqual(ablation.iloc[1]["BTC" + ASSET_SUFFIX], 3)
        self.assertEqual(group.iloc[1]["BTC" + ASSET_SUFFIX], 1)

    def test_max_abs_reference_is_used_for_label_positions_and_table_identity(self):
        plan = prepare_label_experiments("weights", {"aggregation_methods": ["max_abs"]})
        entries = {entry["name"]: entry for entry in plan["variants"]}
        self.assertTrue(set(entry["table_path"] for entry in plan["variants"]).isdisjoint(
            entry["table_path"] for entry in self.plan["variants"]))
        for name, expected in (("D", 1.0), ("DR", 2.0), ("DRME", 18.0)):
            with self.subTest(name=name):
                self.assertEqual(entries[name]["parameters"]["aggregation_method"], "max_abs")
                matrix = build_label_positions(self.features, entries[name], **RANGE)
                self.assertEqual(matrix.loc["2023-01-01 09:00:00", "BTC" + ASSET_SUFFIX], expected)
        group = build_label_positions(self.features, entries["level_materiality_low"], **RANGE)
        self.assertEqual(group.loc["2023-01-01 09:00:00", "BTC" + ASSET_SUFFIX], -1.0)

    def test_scale_and_effective_parameters_have_distinct_stable_paths(self):
        scaled = prepare_label_experiments("weights", unit_position=0.01)
        self.assertTrue(set(e["table_path"] for e in self.plan["variants"]).isdisjoint(
            e["table_path"] for e in scaled["variants"]))
        matrix = build_label_positions(self.features, scaled["variants"][0], **RANGE)
        assert_frame_equal(matrix, self.positions("D") * 0.01)
        changed = prepare_label_experiments("weights", {"coefficients": {"B": [20], "C": [0], "D": [-3]}})
        self.assertEqual([e["table_path"] for e in changed["variants"]],
                         [e["table_path"] for e in self.plan["variants"]])
        mapped = prepare_label_experiments("weights", {"mappings": {"materiality": {"高": [10]}}})
        self.assertNotEqual(mapped["variants"][2]["table_path"], self.plan["variants"][2]["table_path"])
        self.assertEqual(mapped["variants"][0]["table_path"], self.plan["variants"][0]["table_path"])

    def test_paths_are_children_of_root_and_use_the_weight_parameter_hash(self):
        expected = WeightParameters()
        for root in ("crypto/weights", "crypto/weights/"):
            plan = prepare_label_experiments(root)
            for entry in plan["variants"]:
                parameters = WeightParameters(mappings=entry["parameters"]["mappings"],
                                              **entry["parameters"]["coefficients"])
                self.assertEqual(entry["param_id"], parameters.param_id)
                self.assertEqual(entry["parameter_hash"], parameters.parameter_hash)
                self.assertEqual(entry["table_path"], "crypto/weights/" + entry["table_name"])
                self.assertTrue(entry["table_name"].endswith("__" + parameters.param_id))
                self.assertNotIn("/", entry["table_name"])
            full = next(entry for entry in plan["variants"] if entry["name"] == "DRME")
            self.assertEqual(full["param_id"], expected.param_id)
            self.assertEqual(full["table_name"], "sum__labels__DRME__B1_C1_D1__" + expected.param_id)

    def test_filter_names_and_modes_distinguish_tables_without_changing_score_id(self):
        high = self.entries["level_materiality_high"]
        low = self.entries["level_materiality_low"]
        self.assertEqual(high["param_id"], low["param_id"])
        self.assertNotEqual(high["table_path"], low["table_path"])
        self.assertNotEqual(high["experiment_id"], low["experiment_id"])
        maximum = prepare_label_experiments("weights", {"aggregation_methods": ["max_abs"]})
        for summed, max_abs in zip(self.plan["variants"], maximum["variants"]):
            self.assertEqual(summed["param_id"], max_abs["param_id"])
            self.assertEqual(summed["parameter_hash"], max_abs["parameter_hash"])
            self.assertNotEqual(summed["table_path"], max_abs["table_path"])
            self.assertTrue(max_abs["table_name"].startswith("max_abs__"))

    def test_scale_is_hashed_and_applied_as_effective_B_once(self):
        plan = prepare_label_experiments("weights", unit_position=0.01)
        scaled = next(entry for entry in plan["variants"] if entry["name"] == "DRME")
        expected = WeightParameters(B=0.01)
        self.assertEqual(scaled["parameters"]["coefficients"], {"B": 0.01, "C": 1.0, "D": 1.0})
        self.assertEqual(scaled["param_id"], expected.param_id)
        self.assertEqual(scaled["table_name"], "sum__labels__DRME__B0.01_C1_D1__" + expected.param_id)
        positions = build_label_positions(self.features, scaled, **RANGE)
        assert_frame_equal(positions, self.positions("DRME") * 0.01)
        other_root = prepare_label_experiments("elsewhere", unit_position=0.01)
        self.assertEqual([entry["param_id"] for entry in plan["variants"]],
                         [entry["param_id"] for entry in other_root["variants"]])

    def test_colliding_source_paths_and_old_plan_are_rejected(self):
        path = self.entries["D"]["table_path"]
        with self.assertRaisesRegex(ValueError, "覆盖来源"):
            prepare_label_experiments("weights", result_path=path)
        old = dict(self.entries["D"], experiment_version="label_signals_v2")
        with self.assertRaisesRegex(ValueError, "旧版"):
            build_label_positions(self.features, old, **RANGE)

    def test_select_one_reference_and_reject_invalid_units(self):
        grid = {"mappings": {"materiality": {"高": [3, 10]}}}
        with self.assertRaisesRegex(ValueError, "param-id"):
            prepare_label_experiments("weights", grid)
        param_id = prepare_weight_grid("weights", grid).variants[1].parameters.param_id
        selected = prepare_label_experiments("weights", grid, param_id=param_id)
        self.assertEqual(selected["reference_param_id"], param_id)
        with self.assertRaises(ValueError):
            prepare_label_experiments("weights", grid, param_id="missing")
        for value in (0, -1, True, "1", float("nan"), float("inf"), 10 ** 400):
            with self.subTest(value=value), self.assertRaises(ValueError):
                prepare_label_experiments("weights", unit_position=value)

    def test_select_shared_reference_id_by_aggregation_mode_through_cli_and_run(self):
        module = importlib.import_module("entrypoints.write_label_experiments")
        grid = {"aggregation_methods": ["sum", "max_abs"]}
        param_id = self.plan["reference_param_id"]
        with self.assertRaisesRegex(ValueError, "aggregation-method"):
            prepare_label_experiments("weights", grid, param_id=param_id)
        config = replace(settings(), weight_grid=grid)
        with patch.object(module, "load_event_config", return_value=config), patch("builtins.print"):
            preview = module.main(["--dry-run", "--param-id", param_id, "--aggregation-method", "max_abs"])
        self.assertEqual(preview["reference_param_id"], param_id)
        self.assertEqual(preview["reference_parameters"]["aggregation_method"], "max_abs")
        backend = Tables(pd.DataFrame(), source_events())
        with tempfile.TemporaryDirectory() as temporary:
            result = run_label_experiments(backend, config, param_id=param_id, aggregation_method="max_abs",
                                           report_dir=Path(temporary) / "max_abs")
        self.assertEqual(result["completed"], preview["combination_count"])
        self.assertEqual(backend.tables[preview["variants"][0]["table_path"]].loc[
            "2023-01-01 09:00:00", "BTC" + ASSET_SUFFIX], 1.0)

    def test_run_reads_once_keeps_empty_groups_and_reruns_without_accumulating(self):
        source = source_events()
        failed = result_frame([("2023-01-01 02:10:00", "FAILED", "new", None, None)])
        failed.index = pd.MultiIndex.from_tuples([("2023-01-01 02:10:00", "failed")], names=source.index.names)
        failed["round3_status"] = "-1"
        backend = Tables(pd.DataFrame(), pd.concat([source, failed]))
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            summary = run_label_experiments(backend, settings(), report_dir=output / "first")
            manifest = json.loads(Path(summary["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "completed")
            self.assertEqual((manifest["source_rows"], manifest["successful_events"]), (5, 5))
            self.assertEqual(summary["completed"], 25)
            self.assertEqual(backend.column_reads, [("result", "2022-12-31", "2023-01-02", SOURCE_COLUMNS)])
            self.assertEqual(len(pd.read_csv(summary["tables_path"])), 25)
            for table in summary["tables"]:
                self.assertEqual(table["table_path"], "weights/" + table["table_name"])
                self.assertTrue(table["table_name"].endswith("__" + table["param_id"]))
            reference = backend.tables[summary["tables"][0]["table_path"]]
            for entry in manifest["variants"]:
                frame = backend.tables[entry["table_path"]]
                self.assertEqual(list(frame.columns), list(reference.columns))
                self.assertTrue(frame.index.equals(reference.index))
                self.assertTrue(frame.dtypes.eq("float64").all())
                if entry["name"] == "level_persistence_uncertain":
                    self.assertEqual((entry["selected_events"], entry["nonzero_events"]), (1, 0))
            before = (len(backend.writes), len(backend.updates), len(backend.appends))
            run_label_experiments(backend, settings(), report_dir=output / "again")
            self.assertEqual(before, (len(backend.writes), len(backend.updates), len(backend.appends)))

    def test_failure_stops_later_tables_and_records_progress(self):
        backend = Tables(pd.DataFrame(), source_events())
        failed_path = self.entries["DR"]["table_path"]

        def write(path, frame):
            if path == failed_path:
                raise RuntimeError("unavailable")
            backend.tables[path] = frame.copy(deep=True)

        backend.write_hook = write
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "failed"
            with patch("common.persistence.time.sleep"), self.assertRaises(BatchPersistenceError):
                run_label_experiments(backend, settings(), report_dir=output)
            manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["completed"], 1)
        self.assertEqual([e["status"] for e in manifest["variants"][:3]], ["completed", "failed", "pending"])
        self.assertNotIn(self.entries["DM"]["table_path"], backend.tables)

    def test_missing_group_label_stops_before_any_target_write(self):
        source = source_events()
        envelope = json.loads(source.iloc[0]["round3_ai_response"])
        body = json.loads(envelope["ai_response_content"])
        body.pop("persistence")
        envelope["ai_response_content"] = json.dumps(body)
        source.iloc[0, source.columns.get_loc("round3_ai_response")] = json.dumps(envelope)
        backend = Tables(pd.DataFrame(), source)
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, "persistence.*未配置"):
                run_label_experiments(backend, settings(), report_dir=Path(temporary) / "invalid")
        self.assertFalse(backend.registers or backend.writes)

    def test_consecutive_batches_preserve_previous_shifted_boundary(self):
        source = source_events()
        late = source.iloc[:1].copy()
        late.index = pd.MultiIndex.from_tuples([("2023-01-01 23:20:00", "late")], names=source.index.names)
        following = source.iloc[1:2].copy()
        following.index = pd.MultiIndex.from_tuples([("2023-01-02 00:20:00", "following")], names=source.index.names)
        backend = Tables(pd.DataFrame(), pd.concat([source, late, following]))
        second = EventConfig("first", "result", "round2.yaml", "round3.yaml", weight_path="weights",
                             start_time="2023-01-02", end_time="2023-01-02", round2_asset_list_path="assets.json")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_label_experiments(backend, settings(), report_dir=root / "first")
            boundary = {name: backend.tables[entry["table_path"]].loc["2023-01-02 08:00:00"].copy()
                        for name, entry in self.entries.items()}
            run_label_experiments(backend, second, report_dir=root / "second")
        for name, entry in self.entries.items():
            frame = backend.tables[entry["table_path"]]
            self.assertEqual(len(frame), 50)
            pd.testing.assert_series_equal(frame.loc["2023-01-02 08:00:00"], boundary[name])
        self.assertEqual(backend.tables[self.entries["DRME"]["table_path"]].loc[
            "2023-01-02 09:00:00", "BTC" + ASSET_SUFFIX], -1.5)

    def test_summary_export_failure_is_reported_after_completed_tables(self):
        backend = Tables(pd.DataFrame(), source_events())
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "failed_summary"
            with patch.object(pd.DataFrame, "to_csv", side_effect=OSError("export failed")):
                with self.assertRaisesRegex(OSError, "export failed"):
                    run_label_experiments(backend, settings(), report_dir=output)
            manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["completed"], 25)

    def test_manifest_retries_only_temporary_permission_errors_with_a_bound(self):
        module = importlib.import_module("postprocessing.label_experiments")
        with patch.object(module, "_write_manifest", side_effect=[PermissionError("busy"), None]) as write:
            with patch.object(module.time, "sleep"):
                module._save_manifest(Path("unused"), {})
        self.assertEqual(write.call_count, 2)
        with patch.object(module, "_write_manifest", side_effect=PermissionError("busy")) as write:
            with patch.object(module.time, "sleep"), self.assertRaises(PermissionError):
                module._save_manifest(Path("unused"), {})
        self.assertEqual(write.call_count, 3)

    def test_cli_preview_is_offline_and_does_not_create_reports(self):
        module = importlib.import_module("entrypoints.write_label_experiments")
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "not_created"
            with patch.object(module, "load_event_config", return_value=settings()), patch("builtins.print"):
                with patch.dict("sys.modules", {"xdata": None, "xdata.client": None}):
                    result = module.main(["--dry-run", "--report-dir", str(output)])
            self.assertFalse(output.exists())
        self.assertEqual(result["combination_count"], 25)
        self.assertEqual(result["position_unit"], "raw_signal")
        self.assertEqual(result["level_sizing"], "equal_signal_per_event")
        self.assertTrue(all(entry["output_unit"] == "raw_signal" for entry in result["variants"]))

    def test_cli_scale_aliases_generate_the_same_raw_signals(self):
        module = importlib.import_module("entrypoints.write_label_experiments")
        previews = []
        with patch.object(module, "load_event_config", return_value=settings()), patch("builtins.print"):
            for option in ("--signal-scale", "--unit-position"):
                previews.append(module.main(["--dry-run", option, "0.01"]))
        self.assertEqual(previews[0], previews[1])
        self.assertEqual(previews[0]["unit_position"], 0.01)


if __name__ == "__main__":
    unittest.main()
