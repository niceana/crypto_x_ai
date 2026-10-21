"""按参数网格生成多张小时权重表；--dry-run 仅预览参数和表名。"""

import argparse
import json
import logging
from pathlib import Path

from common.config import load_event_config
from entrypoints.runtime import execute
from postprocessing.weight_data import _validate_weight_request
from postprocessing.weight_grid import run_weight_grid
from postprocessing.weight_parameters import prepare_weight_grid


def main(argv=None, cli=None):
    root = Path(__file__).absolute().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(root / "configs" / "event_processing_config.yaml"), help="事件共用配置路径")
    parser.add_argument("--dry-run", action="store_true", help="只预览组合数量、完整参数和目标表名，不连接数据库")
    parser.add_argument("--report-dir", help="本次运行清单的新目录，默认 reports/weight_grid/北京时间")
    args = parser.parse_args(argv)
    settings = load_event_config(args.config)
    plan = prepare_weight_grid(settings.weight_path, settings.weight_grid, result_path=settings.result_path)
    if args.dry_run:
        preview = plan.to_dict()
        print(json.dumps(preview, ensure_ascii=False, indent=2, allow_nan=False))
        return preview
    logging.basicConfig(**(settings.logging or {
        "level": "INFO", "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"}))
    _validate_weight_request(settings.result_path, settings.weight_path, settings.start_time, settings.end_time)
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    return run_weight_grid(cli, settings, prepared=plan, report_dir=args.report_dir)


def _cli_summary():
    result = main()
    return {key: value for key, value in result.items() if key not in ("tables", "variants", "candidates")}


if __name__ == "__main__":
    execute(_cli_summary)
