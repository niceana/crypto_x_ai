"""生成 8 组标签对照和按等级分组的原始信号表，供现有回测逐表运行。"""

import argparse
import json
import logging
from pathlib import Path

from common.config import load_event_config
from common.process_lock import stage_guard
from entrypoints.runtime import execute
from postprocessing.label_experiments import prepare_label_experiments, run_label_experiments
from postprocessing.weight_data import _validate_weight_request
from postprocessing.weight_parameters import AGGREGATION_METHODS


def main(argv=None, cli=None):
    root = Path(__file__).absolute().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(root / "configs" / "event_processing_config.yaml"))
    parser.add_argument("--param-id", help="weight_grid 有多套候选时，指定一套固定映射")
    parser.add_argument("--aggregation-method", choices=AGGREGATION_METHODS, help="有多个聚合模式时指定参考模式")
    parser.add_argument("--signal-scale", "--unit-position", dest="signal_scale", type=float, default=1.0,
                        help="原始信号的共同倍率，默认 1；旧名 --unit-position 仍支持。此值不代表资金比例")
    parser.add_argument("--dry-run", action="store_true", help="仅预览方案及完整目标表名，不连接数据库")
    parser.add_argument("--report-dir", help="尚不存在的清单目录，默认 reports/label_experiments/北京时间")
    args = parser.parse_args(argv)
    settings = load_event_config(args.config)
    preview = prepare_label_experiments(settings.weight_path, settings.weight_grid, param_id=args.param_id,
                                        aggregation_method=args.aggregation_method,
                                        unit_position=args.signal_scale, result_path=settings.result_path)
    if args.dry_run:
        print(json.dumps(preview, ensure_ascii=False, indent=2, allow_nan=False))
        return preview
    logging.basicConfig(**(settings.logging or {
        "level": "INFO", "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"}))
    _validate_weight_request(settings.result_path, settings.weight_path, settings.start_time, settings.end_time)
    with stage_guard("weights", settings.weight_path, start_time=settings.start_time, end_time=settings.end_time):
        if cli is None:
            from xdata.client import get_client
            cli = get_client()
        return run_label_experiments(cli, settings, param_id=args.param_id,
                                     aggregation_method=args.aggregation_method,
                                     unit_position=args.signal_scale, report_dir=args.report_dir)


def _cli_summary():
    result = main()
    return {key: value for key, value in result.items() if key not in ("tables", "variants")}


if __name__ == "__main__":
    execute(_cli_summary)
