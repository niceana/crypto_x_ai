"""读取事件结果表的 asset 列并绘制频数分布，不检查第二、三轮状态。"""

import argparse
from pathlib import Path

from common.config import load_event_config
from postprocessing.asset_frequency import plot_asset_frequency


def main(argv=None, cli=None):
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(root / "configs" / "event_processing_config.yaml"),
                        help="事件配置路径，读取 result_path 和可选起止日期")
    parser.add_argument("--top-n", type=int, default=40, help="条形图展示的资产数，默认 40")
    parser.add_argument("--output-dir", default=str(root / "reports" / "asset_frequency"),
                        help="PNG 和 CSV 的保存目录")
    parser.add_argument("--show", action="store_true", help="额外弹出图形窗口，默认仅保存文件")
    args = parser.parse_args(argv)
    if args.top_n < 1:
        parser.error("--top-n 必须为正整数")
    settings = load_event_config(args.config)
    try:
        import matplotlib
    except ImportError as exc:
        raise RuntimeError("当前环境缺少 matplotlib，请先运行 python -m pip install matplotlib") from exc
    if not args.show:
        matplotlib.use("Agg")
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    frame = cli.read(settings.result_path, start_time=settings.start_time or "",
                     end_time=settings.end_time or "", columns=["asset"])
    frequency, figure = plot_asset_frequency(frame, top_n=args.top_n,
                                              output_dir=args.output_dir, show=args.show)
    if not args.show:
        from matplotlib import pyplot as plt
        plt.close(figure)
    output = Path(args.output_dir).resolve()
    print("已统计 {} 行、{} 个资产；不筛选任务状态。".format(frequency.attrs["counted_total"], len(frequency)))
    print("图表：{}".format(output / "asset_frequency.png"))
    print("频数表：{}".format(output / "asset_frequency.csv"))
    return frequency


if __name__ == "__main__":
    main()
