"""一次读取及解析事件，逐参数组合续写权重表并维护运行清单。"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.store import read_event_history
from postprocessing.weight_data import SOURCE_COLUMNS, _validate_weight_request, _write_weight_features, parse_weight_features
from postprocessing.weight_parameters import WeightGridPlan, prepare_weight_grid


logger = logging.getLogger(__name__)
_BEIJING = timezone(timedelta(hours=8))


def _now():
    return datetime.now(_BEIJING)


def _write_manifest(path, manifest):
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


def run_weight_grid(cli, settings, *, prepared=None, report_dir=None):
    """返回各参数表摘要和 manifest_path；不在返回值中保留 DataFrame。"""
    _validate_weight_request(settings.result_path, settings.weight_path, settings.start_time, settings.end_time)
    plan = prepared if prepared is not None else prepare_weight_grid(
        settings.weight_path, settings.weight_grid, result_path=settings.result_path)
    if not isinstance(plan, WeightGridPlan) or plan.weight_path != settings.weight_path:
        raise ValueError("预展开参数网格与当前 weight_path 不一致")
    if any(variant.table_path == settings.result_path for variant in plan.variants):
        raise ValueError("参数结果表不能覆盖源结果表")
    started = _now()
    root = Path(__file__).absolute().parent.parent
    output = (Path(report_dir).absolute() if report_dir is not None else
              root / "reports" / "weight_grid" / started.strftime("%Y%m%d_%H%M%S_%f"))
    output.mkdir(parents=True, exist_ok=False)
    manifest_path = output / "manifest.json"
    manifest = plan.to_dict()
    manifest.update(result_path=settings.result_path, start_time=settings.start_time, end_time=settings.end_time,
                    started_at=started.isoformat(), status="running", completed=0)
    for entry in manifest["variants"]:
        entry["status"] = "pending"
    _write_manifest(manifest_path, manifest)
    logger.info("权重参数网格开始：组合=%d，清单=%s", len(plan.variants), manifest_path)
    current = None
    try:
        results = read_event_history(cli, settings.result_path, settings.start_time, settings.end_time,
                                     columns=SOURCE_COLUMNS, label="权重网格来源读取")
        features = parse_weight_features(results)
        del results
        if features.empty:
            raise ValueError("结果表没有事件，无法确定权重表的资产列")
        manifest["source_rows"] = len(features)
        manifest["source_assets"] = sorted(features["asset"].unique())
        for variant, entry in zip(plan.variants, manifest["variants"]):
            current = entry
            entry["status"] = "running"
            _write_manifest(manifest_path, manifest)
            logger.info("生成权重组合 %d/%d：参数=%s，目标=%s", manifest["completed"] + 1,
                        len(plan.variants), variant.parameters.param_id, variant.table_path)
            saved = _write_weight_features(cli, features, settings.result_path, variant.table_path,
                                            start_time=settings.start_time, end_time=settings.end_time,
                                            parameters=variant.parameters)
            entry.update(status="completed", hours=len(saved), assets=len(saved.columns),
                         start_time=saved.index[0].strftime("%Y-%m-%d %H:%M:%S"),
                         end_time=saved.index[-1].strftime("%Y-%m-%d %H:%M:%S"))
            del saved
            manifest["completed"] += 1
            _write_manifest(manifest_path, manifest)
            current = None
    except BaseException as exc:
        status = "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "failed"
        error = {"type": type(exc).__name__, "message": str(exc)}
        manifest.update(status=status, error=error, finished_at=_now().isoformat())
        if current is not None:
            current.update(status=status, error=error)
        try:
            _write_manifest(manifest_path, manifest)
        except Exception:
            logger.exception("未能更新权重网格失败清单：%s", manifest_path)
        logger.error("权重网格停止：已完成=%d，清单=%s", manifest["completed"], manifest_path)
        raise
    manifest.update(status="completed", finished_at=_now().isoformat())
    _write_manifest(manifest_path, manifest)
    tables = [{key: entry[key] for key in ("param_id", "table_path", "status", "hours", "assets", "start_time", "end_time")}
              for entry in manifest["variants"]]
    logger.info("权重网格完成：组合=%d，清单=%s", len(tables), manifest_path)
    return {"combination_count": len(tables), "completed": manifest["completed"],
            "manifest_path": str(manifest_path), "tables": tables}
