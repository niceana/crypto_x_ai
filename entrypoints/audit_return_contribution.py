"""只读 weight 与 close_price_60min，导出三张月度收益来源表及资产全期汇总。"""

import argparse
import math
from datetime import date, timedelta
from pathlib import Path

from postprocessing.return_contribution import _save_report, analyze_price_contributions
from postprocessing.return_contribution_plots import save_return_contribution_plots


# ==================== 修改这里后直接运行本模块 ====================
WEIGHT_PATH = ""                     # 必填：完整 weight 表路径（含参数后缀）
PRICE_PATH = "close_price_60min"      # 价格表完整路径
START_TIME = ""                      # 必填：YYYY-MM-DD，包含当天
END_TIME = ""                        # 必填：YYYY-MM-DD，包含当天
TOP_N = 5                            # 每月正、负贡献分别展示前 N 个
NET_RETURN_EPSILON = 1e-8             # 净收益接近零时，不计算净收益占比
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "reports" / "return_contribution"
FIGURE_DIR = Path(__file__).resolve().parent.parent  # PNG 保存到项目根目录；服务器无需图形窗口
# 命令行参数可临时覆盖上述配置；不传参数时使用这里的值。
# =================================================================


def main(argv=None, cli=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weight-path", default=WEIGHT_PATH, help="weight 表完整路径，默认使用顶部 WEIGHT_PATH")
    parser.add_argument("--price-path", default=PRICE_PATH, help="价格表路径，默认使用顶部 PRICE_PATH")
    parser.add_argument("--start-time", default=START_TIME, help="起点日期 YYYY-MM-DD，默认使用顶部 START_TIME")
    parser.add_argument("--end-time", default=END_TIME, help="结束日期 YYYY-MM-DD，默认使用顶部 END_TIME")
    parser.add_argument("--top-n", type=int, default=TOP_N, help="正负贡献分别标记前 N 个，默认使用顶部 TOP_N")
    parser.add_argument("--net-return-epsilon", type=float, default=NET_RETURN_EPSILON, help="净收益绝对值不超过此值时，净收益占比留空")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--figure-dir", default=str(FIGURE_DIR), help="PNG 保存目录，默认项目根目录；只保存，不弹窗")
    args = parser.parse_args(argv)
    if not args.weight_path.strip() or not args.price_path.strip():
        parser.error("请在脚本顶部填写 WEIGHT_PATH、PRICE_PATH，或通过命令行指定；路径不能为空")
    if args.top_n < 1:
        parser.error("--top-n 必须为正整数")
    try:
        begin, last = date.fromisoformat(args.start_time), date.fromisoformat(args.end_time)
        if begin.isoformat() != args.start_time or last.isoformat() != args.end_time or begin > last:
            raise ValueError("日期格式或范围不正确")
        price_end = (last + timedelta(days=1)).isoformat()
    except ValueError:
        parser.error("请填写顶部 START_TIME、END_TIME 或对应参数；日期必须为 YYYY-MM-DD，且起始日期不能晚于结束日期")
    if not math.isfinite(args.net_return_epsilon) or args.net_return_epsilon < 0:
        parser.error("--net-return-epsilon 必须为非负有限数值")
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    weights = cli.read(args.weight_path, start_time=args.start_time, end_time=args.end_time)
    # 先读取价格表真实拥有的列，再取交集，避免请求不存在的 weight 资产列。
    prices = cli.read(args.price_path, start_time=args.start_time, end_time=price_end)
    report = analyze_price_contributions(weights, prices, start_time=args.start_time, end_time=args.end_time,
                                         top_n=args.top_n, net_return_epsilon=args.net_return_epsilon)
    report["metadata"].update(weight_path=args.weight_path, price_path=args.price_path,
                               start_time=args.start_time, end_time=args.end_time)
    _save_report(report, args.output_dir)
    figures = save_return_contribution_plots(report, args.figure_dir)
    print("已导出 {} 个月、{} 个资产的月度贡献；逐小时绝对值归一化，费用前，小时加总，未复利链接。".format(
        len(report["monthly_summary"]), len(report["metadata"]["valid_assets"])))
    excluded = report["metadata"]["excluded_weight_assets"]
    if excluded:
        print("剔除价格表中不存在的权重资产（{} 个）：{}".format(len(excluded), ", ".join(excluded)))
    filled = report["metadata"]["nan_weight_cells_filled"]
    if filled:
        print("已有时间行中的 NaN 权重按空仓 0 处理：{} 个单元格。".format(filled))
    price_filled = report["metadata"]["nan_price_cells_filled"]
    if price_filled:
        print("NaN 价格以前一非空价格填充：{} 个单元格。".format(price_filled))
    unavailable = report["metadata"]["unavailable_entry_price_hours_by_asset"]
    if unavailable:
        print("按小时剔除无效起点报价：{} 个资产，共 {} 个资产小时；明细见 method.json。".format(
            len(unavailable), report["metadata"]["unavailable_entry_price_asset_hours"]))
    print(report["monthly_summary"].to_string(index=False))
    print("三张月度 CSV、asset_summary.csv 与计算口径：{}".format(Path(args.output_dir).resolve()))
    for path in figures.values():
        print("图表：{}".format(path))
    return report


if __name__ == "__main__":
    main()
