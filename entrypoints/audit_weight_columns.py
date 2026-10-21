"""只读核对已有权重表列名与配置范围内源表 asset 的对应关系。"""

import argparse
import json
from pathlib import Path

from common.config import load_event_config
from postprocessing.weight_data import audit_weight_columns
from postprocessing.weight_parameters import prepare_weight_grid


def main(argv=None, cli=None):
    root = Path(__file__).absolute().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(root / "configs" / "event_processing_config.yaml"), help="事件共用配置路径")
    parser.add_argument("--param-id", help="要核对的参数标识；存在多个组合时必须指定")
    args = parser.parse_args(argv)
    settings = load_event_config(args.config)
    if not settings.start_time or not settings.end_time or not settings.weight_path.strip():
        raise ValueError("请先配置 weight_path、start_time 和 end_time")
    plan = prepare_weight_grid(settings.weight_path, settings.weight_grid, result_path=settings.result_path)
    choices = [variant for variant in plan.variants if args.param_id is None or variant.parameters.param_id == args.param_id]
    if len(choices) != 1:
        parser.error("请通过 --param-id 指定一个有效组合；可先运行权重入口 --dry-run 查看标识")
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    report = audit_weight_columns(cli, settings.result_path, choices[0].table_path,
                                  start_time=settings.start_time, end_time=settings.end_time)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


if __name__ == "__main__":
    main()
