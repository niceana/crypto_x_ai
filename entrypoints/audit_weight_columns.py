"""只读核对已有权重表列名与配置范围内源表 asset 的对应关系。"""

import argparse
import json
from pathlib import Path

from common.config import load_event_config
from postprocessing.weight_data import audit_weight_columns
from postprocessing.weight_parameters import AGGREGATION_METHODS, prepare_weight_grid, select_weight_variant


def main(argv=None, cli=None):
    root = Path(__file__).absolute().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(root / "configs" / "event_processing_config.yaml"), help="事件共用配置路径")
    parser.add_argument("--param-id", help="要核对的参数标识；存在多个组合时必须指定")
    parser.add_argument("--aggregation-method", choices=AGGREGATION_METHODS, help="存在多个聚合模式时指定要核对的模式")
    args = parser.parse_args(argv)
    settings = load_event_config(args.config)
    if not settings.start_time or not settings.end_time or not settings.weight_path.strip():
        raise ValueError("请先配置 weight_path、start_time 和 end_time")
    plan = prepare_weight_grid(settings.weight_path, settings.weight_grid, result_path=settings.result_path)
    try:
        selected = select_weight_variant(plan, param_id=args.param_id, aggregation_method=args.aggregation_method)
    except ValueError as exc:
        parser.error(str(exc))
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    report = audit_weight_columns(cli, settings.result_path, selected.table_path,
                                  start_time=settings.start_time, end_time=settings.end_time)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


if __name__ == "__main__":
    main()
