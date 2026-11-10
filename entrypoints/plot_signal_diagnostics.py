"""修改顶部配置后运行：python -X utf8 -m entrypoints.plot_signal_diagnostics。"""

from pathlib import Path

import pandas as pd


# ==================== 在这里配置；所有表统一用 cli.read 读取 ====================
WEIGHT_PATH = ""                 # 已生成的完整 weight 表路径（含参数后缀）
RESULT_PATH = ""                 # 第二、三轮共用结果表；留空只分析 weight
START_TIME = "2023-01-01"        # weight 表现有时间轴上的日期，包含首尾日全天
END_TIME = "2023-12-31"
ASSET_COLUMNS = None             # 可选完整合约列名列表；None 使用 weight 全部列
WEIGHT_EPSILON = 0.0             # 有信号定义为 abs(weight)>此值；原始 weight 默认精确非零
TOP_N = 30                       # 概览热图展示前 N 个；另存全部资产热图及明细
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "reports" / "signal_diagnostics"
SHOW = False                    # True 额外显示 matplotlib 图形窗口

# 只有 RESULT_PATH 非空才使用以下配置。评分映射必须与所分析的 weight 对应。
SCORING_CONFIG = Path(__file__).resolve().parent.parent / "configs" / "event_processing_config.yaml"
PARAM_ID = None                  # 配置只有一个组合时可留空；多个组合时填写参数标识
AGGREGATION_METHOD = None        # 配置有多个聚合模式时填写 "sum" 或 "max_abs"
EVENT_TIME_OFFSET_HOURS = 8      # 当前权重写库时间平移；已有 weight 本身绝不再次平移
CANCELLATION_EPSILON = 1e-10     # 多空均非零且 abs(净分数)<=此值，记为完全抵消
# ============================================================================


def main(cli=None):
    if not WEIGHT_PATH.strip():
        raise ValueError("请先在脚本顶部填写 WEIGHT_PATH")
    begin = pd.Timestamp(START_TIME)
    end = pd.Timestamp(END_TIME) + pd.Timedelta(days=1)
    if begin >= end:
        raise ValueError("START_TIME 不能晚于 END_TIME")
    import matplotlib
    if not SHOW:
        matplotlib.use("Agg")
    from postprocessing.signal_diagnostic_plots import plot_event_diagnostics, plot_weight_diagnostics
    from postprocessing.signal_diagnostics import EVENT_COLUMNS, event_assets_for_weight
    from postprocessing.weight_time import event_target_hours, event_window_bounds

    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    weight = cli.read(WEIGHT_PATH, start_time=START_TIME, end_time=END_TIME).fillna(0)
    if not isinstance(weight.index, pd.DatetimeIndex):
        raise ValueError("weight 表索引必须是 DatetimeIndex")
    weight_begin, weight_end = begin.tz_localize(weight.index.tz), end.tz_localize(weight.index.tz)
    weight = weight.loc[(weight.index >= weight_begin) & (weight.index < weight_end)]
    if ASSET_COLUMNS is not None:
        weight = weight.loc[:, ASSET_COLUMNS]
    weight_report, figures = plot_weight_diagnostics(
        weight, epsilon=WEIGHT_EPSILON, top_n=TOP_N, output_dir=OUTPUT_DIR, show=SHOW)
    summary = weight_report["summary"]
    print("weight：{} 小时，{} 个资产，有信号小时占比 {:.2%}；缺失小时 {}。".format(
        summary["hours"], summary["assets"], summary["active_hour_share"], summary["missing_hours"]))
    event_report = None
    if RESULT_PATH.strip():
        from common.config import load_event_config
        from postprocessing.weight_parameters import prepare_weight_grid, select_weight_variant
        settings = load_event_config(SCORING_CONFIG)
        plan = prepare_weight_grid(settings.weight_path or "diagnostic_weight", settings.weight_grid)
        try:
            selected = select_weight_variant(plan, param_id=PARAM_ID, aggregation_method=AGGREGATION_METHOD)
        except ValueError as exc:
            raise ValueError("请在顶部 PARAM_ID、AGGREGATION_METHOD 指定一个有效组合；"
                             "可先运行权重入口 --dry-run 查看") from exc
        # weight 是已平移后的时间轴。向前扩展源读取日期，保留年初/日初所需的上一日事件。
        offset = pd.Timedelta(hours=EVENT_TIME_OFFSET_HOURS)
        source_begin, source_limit = event_window_bounds(begin - offset, end - pd.Timedelta(hours=1) - offset)
        source_start = source_begin.date().isoformat()
        source_end = source_limit.date().isoformat()
        events = cli.read(RESULT_PATH, start_time=source_start, end_time=source_end, columns=EVENT_COLUMNS)
        dates = pd.to_datetime(events.index.get_level_values("date"), format="ISO8601")
        if dates.tz is not None:
            dates = dates.tz_localize(None)
        targets = event_target_hours(dates) + offset
        events = events.loc[(targets >= begin) & (targets < end)]
        event_report, event_figures = plot_event_diagnostics(
            events, parameters=selected.parameters, assets=event_assets_for_weight(weight),
            epsilon=CANCELLATION_EPSILON, output_time_offset_hours=EVENT_TIME_OFFSET_HOURS,
            output_dir=OUTPUT_DIR, show=SHOW)
        figures.update(event_figures)
        print("事件：{} 条参与评分，{} 条零分，{} 个资产小时完全抵消。".format(
            event_report["summary"]["scored_events"], event_report["summary"]["zero_events"],
            event_report["summary"]["fully_cancelled_asset_hours"]))
    if not SHOW:
        from matplotlib import pyplot as plt
        for figure in figures.values():
            plt.close(figure)
    print("图表和 CSV：{}".format(Path(OUTPUT_DIR).resolve()))
    return {"weight": weight_report, "events": event_report}


if __name__ == "__main__":
    main()
