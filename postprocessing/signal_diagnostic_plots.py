"""信号诊断图与 CSV 导出；matplotlib 仅在绘图时导入。"""

import json
from copy import copy
from pathlib import Path

import numpy as np
import pandas as pd

from postprocessing.signal_diagnostics import analyze_events, analyze_weight
from postprocessing.weight_data import ASSET_SUFFIX


LABELS = {"利好": "Positive", "利空": "Negative", "中性": "Neutral", "不确定": "Uncertain",
          "其他": "Other", "高": "High", "中": "Medium", "低": "Low", "未开始": "Not started",
          "执行中": "In progress", "已完成": "Completed", "短期": "Short", "中期": "Medium", "长期": "Long"}


def _save_report(report, output_dir, prefix):
    if output_dir is None:
        return None
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    for name, table in report.items():
        if isinstance(table, pd.DataFrame):
            table.to_csv(output / (prefix + "_" + name + ".csv"), encoding="utf-8-sig",
                         index=name not in ("distributions", "event_scores", "flat_periods"))
    (output / (prefix + "_summary.json")).write_text(
        json.dumps(report["summary"], ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    return output


def _heatmap(axis, signed_share, columns):
    import matplotlib.dates as mdates
    from matplotlib import cm

    full_index = pd.date_range(signed_share.index[0], signed_share.index[-1], freq="h")
    values = signed_share.reindex(full_index)[columns].to_numpy().T
    dates = mdates.date2num(full_index.to_pydatetime())
    # 使用旧版 Matplotlib 也提供的接口；复制后设置缺失色，不修改全局色图。
    cmap = copy(cm.RdBu_r)
    cmap.set_bad("#e8ebef")
    result = axis.imshow(np.ma.masked_invalid(values), aspect="auto", interpolation="nearest",
                         cmap=cmap, vmin=-1, vmax=1,
                         extent=(dates[0] - 1 / 48, dates[-1] + 1 / 48, len(columns) - 0.5, -0.5))
    labels = [asset[:-len(ASSET_SUFFIX)] if asset.endswith(ASSET_SUFFIX) else asset for asset in columns]
    axis.set_yticks(np.arange(len(columns)))
    axis.set_yticklabels(labels, fontsize=8)
    axis.xaxis_date()
    locator = axis.xaxis.get_major_locator()
    formatter = getattr(mdates, "ConciseDateFormatter", mdates.AutoDateFormatter)
    axis.xaxis.set_major_formatter(formatter(locator))
    return result


def plot_weight_diagnostics(weight, *, epsilon=0.0, top_n=30, output_dir=None, show=True):
    """四行共享时间轴：非零资产数、集中度、分数规模、带方向的资产占比热图。

    overview 的热图展示 Top N；另存完整资产热图和全部小时、资产明细。
    返回 (report, figures)。既不读取净值，也不将 weight 解释为策略实际持仓。
    """
    if isinstance(top_n, bool) or not isinstance(top_n, int) or top_n < 1:
        raise ValueError("top_n 必须为正整数")
    report = analyze_weight(weight, epsilon=epsilon)
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    hourly = report["hourly"]
    timeline = pd.date_range(hourly.index[0], hourly.index[-1], freq="h")
    display = hourly.reindex(timeline)
    top = list(report["assets"].head(top_n).index)
    figure, axes = plt.subplots(4, 1, sharex=True, figsize=(18, 10 + len(top) * 0.12),
                                gridspec_kw={"height_ratios": [1, 1, 1, max(2, len(top) * 0.12)]})
    for column, label, color in (("active_assets", "Nonzero assets", "#334e80"),
                                 ("positive_assets", "Positive", "#168273"),
                                 ("negative_assets", "Negative", "#d17842"),
                                 ("effective_assets", "Effective assets (1 / HHI)", "#9254a8")):
        axes[0].plot(timeline.to_pydatetime(), display[column].to_numpy(), label=label, color=color, linewidth=0.8)
    axes[0].set(ylabel="Asset count", title="Hourly signal coverage | epsilon={:g}".format(epsilon))
    for column, label in (("top1_share", "Top 1 absolute share"), ("top3_share", "Top 3 absolute share")):
        axes[1].plot(timeline.to_pydatetime(), display[column].to_numpy(), label=label,
                     linewidth=0.8, marker=".", markersize=2)
    axes[1].yaxis.set_major_formatter(PercentFormatter(1))
    axes[1].set(ylabel="Concentration", ylim=(-0.03, 1.03))
    for column in ("gross_score", "net_score"):
        axes[2].plot(timeline.to_pydatetime(), display[column].to_numpy(), label=column, linewidth=0.8)
    axes[2].set(ylabel="Weight score", title="Score scale (not capital or risk exposure)")
    for axis in axes[:3]:
        axis.grid(alpha=0.2)
        axis.legend(loc="upper right", fontsize=8)
    heat = _heatmap(axes[3], report["signed_share"], top)
    axes[3].set_title("Signed share: weight / sum(abs(weight)) | top {} of {} assets".format(len(top), len(weight.columns)))
    figure.colorbar(heat, ax=axes[3], orientation="horizontal", pad=0.12, fraction=0.05,
                    label="Negative / Positive weight share; gray = no signal or missing hour")
    figure.tight_layout()
    figures = {"weight_overview": figure}
    if len(top) < len(weight.columns):
        full, axis = plt.subplots(figsize=(18, max(6, len(weight.columns) * 0.16)))
        heat = _heatmap(axis, report["signed_share"], list(report["assets"].index))
        axis.set_title("All assets: signed weight share | same stored timestamps as weight table")
        full.colorbar(heat, ax=axis, orientation="horizontal", pad=0.025, fraction=0.02)
        full.tight_layout()
        figures["weight_all_assets"] = full
    output = _save_report(report, output_dir, "weight")
    if output is not None:
        for name, chart in figures.items():
            chart.savefig(output / (name + ".png"), dpi=150)
    if show:
        plt.show()
    return report, figures


def plot_event_diagnostics(results, *, parameters=None, assets=None, epsilon=1e-10,
                           output_time_offset_hours=8, output_dir=None, show=True):
    """保存第二三轮分布、按日的零分/抵消图，以及逐事件、逐资产小时明细。"""
    report = analyze_events(results, parameters=parameters, assets=assets, epsilon=epsilon,
                            output_time_offset_hours=output_time_offset_hours)
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    distributions = report["distributions"]
    figure, axes = plt.subplots(2, 3, figsize=(16, 9))
    for axis, field in zip(axes.flat, ("round2_final_relation", "round2_ai_relation", "direction",
                                      "materiality", "execution_stage", "persistence")):
        subset = distributions.loc[distributions["field"].eq(field)]
        scopes = list(subset["scope"].unique())
        values = list(subset["value"].unique())
        for offset, scope in enumerate(scopes):
            counts = subset.loc[subset["scope"].eq(scope)].set_index("value")["count"].reindex(values, fill_value=0)
            axis.barh(np.arange(len(values)) + offset * 0.35, counts.to_numpy(), height=0.35,
                      label=scope + " (n={})".format(int(counts.sum())))
        axis.set_yticks(np.arange(len(values)) + (len(scopes) - 1) * 0.175)
        axis.set_yticklabels([LABELS.get(value, value) for value in values])
        axis.set(title=field, xlabel="Event records")
        axis.legend(fontsize=7)
        axis.grid(axis="x", alpha=0.2)
    figure.suptitle("Round 2 / Round 3 distributions | repeat responses included only in all_disclosures")
    figure.tight_layout(rect=(0, 0, 1, 0.96))
    # 先逐资产、逐小时计算抵消，再合计；不让不同资产或不同小时的方向相互抵消。
    daily = report["hourly"].resample("1D").sum(min_count=1)
    daily["zero_event_share"] = daily["zero_events"] / daily["events"].replace(0, np.nan)
    daily["cancellation_ratio"] = 1 - daily["absolute_net_score"] / daily["gross_score"].replace(0, np.nan)
    time_figure, time_axes = plt.subplots(3, 1, sharex=True, figsize=(18, 9))
    for column in ("events", "zero_events"):
        time_axes[0].plot(daily.index.to_pydatetime(), daily[column].to_numpy(), label=column,
                          marker=".", markersize=3)
    for column in ("fully_cancelled", "partially_cancelled", "only_zero_events"):
        time_axes[1].plot(daily.index.to_pydatetime(), daily[column].to_numpy(), label=column,
                          marker=".", markersize=3)
    for column in ("zero_event_share", "cancellation_ratio"):
        time_axes[2].plot(daily.index.to_pydatetime(), daily[column].to_numpy(), label=column,
                          marker=".", markersize=3)
    time_axes[0].set(ylabel="Events / day", title="Zero scores and within-asset-hour cancellation (daily totals)")
    time_axes[1].set(ylabel="Asset-hours / day")
    time_axes[2].set(ylabel="Ratio", ylim=(-0.03, 1.03))
    time_axes[2].yaxis.set_major_formatter(PercentFormatter(1))
    time_axes[2].set_xlim(daily.index[0] - pd.Timedelta(hours=12), daily.index[-1] + pd.Timedelta(hours=12))
    for axis in time_axes:
        axis.grid(alpha=0.2)
        axis.legend(loc="upper right", fontsize=8)
    time_figure.tight_layout()
    figures = {"event_distributions": figure, "event_zero_cancellation": time_figure}
    output = _save_report(report, output_dir, "event")
    if output is not None:
        for name, chart in figures.items():
            chart.savefig(output / (name + ".png"), dpi=150)
    if show:
        plt.show()
    return report, figures
