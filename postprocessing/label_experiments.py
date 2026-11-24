"""为标签组合和等级分组生成小时原始信号；资金/数量换算由回测决定。"""

import hashlib
import json
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd

from postprocessing.weight_data import (
    SOURCE_COLUMNS, _save_weight_data, _validate_weight_request, build_weight_data,
    load_weight_assets, normalize_weight_assets, parse_weight_features, read_weight_events, score_weight_features,
)
from postprocessing.weight_grid import _now, _write_manifest
from postprocessing.weight_parameters import (
    ACTIVE_MAPPINGS, OUTPUT_TIME_OFFSET_HOURS, WeightParameters, _coefficient_text, _number,
    prepare_weight_grid, select_weight_variant,
)


logger = logging.getLogger(__name__)
EXPERIMENT_VERSION = "label_signals_v4"
TABLE_NAME_TEMPLATE = "{aggregation_method}__labels__{name}__B{B}_C{C}_D{D}__{param_id}"
ABLATIONS = (
    ("D", ()),
    ("DR", ("event_relation",)),
    ("DM", ("materiality",)),
    ("DE", ("execution_stage",)),
    ("DRM", ("event_relation", "materiality")),
    ("DRE", ("event_relation", "execution_stage")),
    ("DME", ("materiality", "execution_stage")),
    ("DRME", ("event_relation", "materiality", "execution_stage")),
)
LEVEL_NAMES = {
    "利好": "positive", "利空": "negative", "中性": "neutral", "其他": "other",
    "不确定": "uncertain", "高": "high", "中": "medium", "低": "low",
    "未开始": "not_started", "执行中": "in_progress", "已完成": "completed",
    "短期": "short", "中期": "medium", "长期": "long",
    "new": "new", "update": "update", "repeat": "repeat",
}


def _save_manifest(path, manifest):
    # Windows 上索引/杀毒进程短暂占用目标文件时，原子替换可能暂时被拒绝。
    for attempt in range(3):
        try:
            return _write_manifest(path, manifest)
        except PermissionError:
            if attempt == 2:
                raise
            time.sleep(0.05 * (attempt + 1))


def _parameters(experiment):
    values = experiment["parameters"]
    return WeightParameters(mappings=values["mappings"],
                            aggregation_method=values.get("aggregation_method", "sum"), **values["coefficients"])


def _variant(weight_path, name, kind, parameters, unit_position, *, field=None, value=None):
    # 公共倍率折入实际使用的 B，直接复用 write_weight 的评分参数哈希。
    parameters = WeightParameters(
        B=parameters.B * unit_position, C=parameters.C, D=parameters.D,
        mappings=parameters.mappings, aggregation_method=parameters.aggregation_method,
    )
    identity = {
        "experiment_version": EXPERIMENT_VERSION,
        "output_unit": "raw_signal",
        "output_time_offset_hours": OUTPUT_TIME_OFFSET_HOURS,
        "kind": kind, "parameters": parameters.to_dict(), "unit_position": unit_position,
        "filter_field": field, "filter_value": value,
    }
    encoded = json.dumps(identity, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    substitutions = {key: _coefficient_text(getattr(parameters, key)) for key in ("B", "C", "D")}
    substitutions.update(name=name, aggregation_method=parameters.aggregation_method, param_id=parameters.param_id)
    table_name = TABLE_NAME_TEMPLATE.format(**substitutions)
    return dict(identity, name=name, experiment_id=digest[:16], experiment_hash=digest,
                param_id=parameters.param_id, parameter_hash=parameters.parameter_hash,
                aggregation_method=parameters.aggregation_method, table_name=table_name,
                table_path=weight_path.rstrip("/") + "/" + table_name)


def prepare_label_experiments(weight_path, grid_config=None, *, param_id=None, aggregation_method=None,
                              unit_position=1.0, result_path=None):
    """只预览；unit_position 兼容旧 API，表示信号倍率，不是金额或资金比例。"""
    unit_position = _number(unit_position, "unit_position")
    if unit_position <= 0:
        raise ValueError("unit_position 必须为有限正数，表示原始信号倍率")
    grid = prepare_weight_grid(weight_path, grid_config, result_path=result_path)
    reference = select_weight_variant(grid, param_id=param_id, aggregation_method=aggregation_method).parameters
    # 关闭标签时包括“不确定”在内的所有枚举都显式设为 1，不能沿用默认的零值。
    ones = {field: {key: 1.0 for key in reference.mappings[field]}
            for field in ACTIVE_MAPPINGS if field != "direction"}
    variants = []
    for name, enabled in ABLATIONS:
        mappings = {"direction": dict(reference.mappings["direction"])}
        mappings.update({field: dict(reference.mappings[field]) if field in enabled else values
                         for field, values in ones.items()})
        parameters = WeightParameters(mappings=mappings, aggregation_method=reference.aggregation_method)
        variants.append(_variant(weight_path, name, "ablation", parameters, unit_position))
    # 分组只保留方向符号；各事件的信号绝对值相同，不据此假设投入金额相同。
    directions = {key: float(np.sign(value)) for key, value in reference.mappings["direction"].items()}
    equal = WeightParameters(mappings=dict(ones, direction=directions),
                             aggregation_method=reference.aggregation_method)
    levels = {field: list(reference.mappings[field]) for field in ACTIVE_MAPPINGS}
    levels["persistence"] = list(grid.candidates["mappings"]["persistence"])
    for field, values in levels.items():
        for value in sorted(values):
            if field == "direction" and directions[value] == 0:
                continue  # 零方向不建立无交易分组，其他分组内的零方向事件仍贡献零。
            suffix = LEVEL_NAMES.get(value, "v" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:12])
            variants.append(_variant(weight_path, "level_{}_{}".format(field, suffix), "level",
                                     equal, unit_position, field=field, value=value))
    paths = [variant["table_path"] for variant in variants]
    if len(paths) != len(set(paths)) or result_path in paths or weight_path in paths:
        raise ValueError("标签实验目标表路径重复或覆盖来源/基础表")
    return {
        "experiment_version": EXPERIMENT_VERSION, "weight_path": weight_path,
        "table_name_template": TABLE_NAME_TEMPLATE,
        "position_unit": "raw_signal", "unit_position": unit_position,
        "level_sizing": "equal_signal_per_event",
        "output_time_offset_hours": OUTPUT_TIME_OFFSET_HOURS,
        "reference_param_id": reference.param_id, "reference_parameters": reference.to_dict(),
        "group_levels": levels,
        "ablation_coefficients": {"B": 1.0, "C": 1.0, "D": 1.0},
        "baseline": "DR", "combination_count": len(variants), "variants": variants,
    }


def build_label_positions(features, experiment, *, start_time, end_time, assets=None):
    """返回最终时间轴的 float64 仓位矩阵；所有分组保留相同资产列及零小时。"""
    if experiment.get("experiment_version") != EXPERIMENT_VERSION:
        raise ValueError("请用 prepare_label_experiments 重新生成当前版本的方案，不能直接复用旧版清单")
    if experiment["kind"] not in ("ablation", "level"):
        raise ValueError("未知标签实验类型")
    parameters = _parameters(experiment)
    basic = score_weight_features(features, parameters)
    selected = np.ones(len(features), dtype=bool)
    if experiment["kind"] == "level":
        selected = features[experiment["filter_field"]].eq(experiment["filter_value"]).to_numpy()
    # 不删除未选事件，否则空分组、没有该等级的资产会丢失共同时间轴和资产列。
    # 公共倍率已折入 parameters.B，不能在此重复相乘。
    basic["data"] = basic["data"].where(selected, 0.0)
    positions = build_weight_data(basic, start_time=start_time, end_time=end_time,
                                 aggregation_method=parameters.aggregation_method, assets=assets)
    positions.index = positions.index + pd.Timedelta(hours=OUTPUT_TIME_OFFSET_HOURS)
    return positions


def run_label_experiments(cli, settings, *, param_id=None, aggregation_method=None, unit_position=1.0,
                          report_dir=None, assets=None):
    """一次读源，逐表写库；清单列出名称、筛选条件、事件数和回测输入路径。"""
    _validate_weight_request(settings.result_path, settings.weight_path, settings.start_time, settings.end_time)
    manifest = prepare_label_experiments(settings.weight_path, settings.weight_grid, param_id=param_id,
                                         aggregation_method=aggregation_method,
                                         unit_position=unit_position, result_path=settings.result_path)
    started = _now()
    root = Path(__file__).absolute().parent.parent
    output = (Path(report_dir).absolute() if report_dir is not None else
              root / "reports" / "label_experiments" / started.strftime("%Y%m%d_%H%M%S_%f"))
    output.mkdir(parents=True, exist_ok=False)
    manifest_path = output / "manifest.json"
    manifest.update(result_path=settings.result_path, start_time=settings.start_time, end_time=settings.end_time,
                    started_at=started.isoformat(), status="running", completed=0)
    for entry in manifest["variants"]:
        entry["status"] = "pending"
    _save_manifest(manifest_path, manifest)
    current = None
    try:
        manifest["asset_list_path"] = settings.round2_asset_list_path if assets is None else None
        columns = normalize_weight_assets(load_weight_assets(cli, settings.round2_asset_list_path)
                                         if assets is None else assets)
        assets = list(columns.values())
        manifest["output_columns"] = assets
        results = read_weight_events(cli, settings.result_path, settings.start_time, settings.end_time,
                                    columns=SOURCE_COLUMNS, label="标签实验来源读取")
        results = results.loc[results["asset"].isin(columns)]
        features = parse_weight_features(results)
        manifest["source_rows"] = len(results)
        del results
        manifest["successful_events"] = len(features)
        manifest["source_assets"] = sorted(features["asset"].unique())
        for field, levels in manifest["group_levels"].items():
            unknown = features.loc[~features[field].isin(levels)]
            if not unknown.empty:
                row = unknown.iloc[0]
                raise ValueError("事件 {} 的 {} 存在未配置的分组等级：{!r}".format(
                    row["record_id"], field, row[field]))
        for entry in manifest["variants"]:
            current = entry
            entry["status"] = "running"
            _save_manifest(manifest_path, manifest)
            positions = build_label_positions(features, entry, start_time=settings.start_time,
                                              end_time=settings.end_time, assets=assets)
            selected = (features[entry["filter_field"]].eq(entry["filter_value"]) if entry["kind"] == "level"
                        else np.ones(len(features), dtype=bool))
            event_scores = score_weight_features(features, _parameters(entry))["data"]
            saved = _save_weight_data(cli, entry["table_path"], positions, fixed_columns=True)
            entry.update(status="completed", selected_events=int(np.sum(selected)),
                         nonzero_events=int(np.sum(selected & event_scores.ne(0))),
                         hours=len(saved), assets=len(saved.columns),
                         nonzero_asset_hours=int(np.count_nonzero(saved.to_numpy())),
                         start_time=saved.index[0].strftime("%Y-%m-%d %H:%M:%S"),
                         end_time=saved.index[-1].strftime("%Y-%m-%d %H:%M:%S"))
            del positions, saved, event_scores
            manifest["completed"] += 1
            _save_manifest(manifest_path, manifest)
            logger.info("标签实验已保存 %d/%d：%s，目标=%s", manifest["completed"],
                        manifest["combination_count"], entry["name"], entry["table_path"])
            current = None
        tables = [{key: entry[key] for key in ("name", "kind", "filter_field", "filter_value", "param_id",
                                              "aggregation_method", "table_name", "table_path",
                                              "status", "selected_events", "nonzero_events")}
                  for entry in manifest["variants"]]
        # 独立 CSV 便于将名称/等级与现有回测任务逐一对应。
        pd.DataFrame(tables).to_csv(output / "tables.csv", index=False, encoding="utf-8-sig")
        manifest.update(status="completed", finished_at=_now().isoformat())
        _save_manifest(manifest_path, manifest)
    except BaseException as exc:
        status = "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "failed"
        error = {"type": type(exc).__name__, "message": str(exc)}
        manifest.update(status=status, error=error, finished_at=_now().isoformat())
        if current is not None:
            current.update(status=status, error=error)
        try:
            _save_manifest(manifest_path, manifest)
        except Exception:
            logger.exception("无法更新标签实验失败清单：%s", manifest_path)
        raise
    return {"combination_count": len(tables), "completed": manifest["completed"],
            "manifest_path": str(manifest_path), "tables_path": str(output / "tables.csv"), "tables": tables}
