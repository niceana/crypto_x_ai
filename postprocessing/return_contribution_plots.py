"""将归一化收益贡献报告保存为 PNG；使用 Agg 画布，适用于无桌面的服务器。"""

from pathlib import Path

import numpy as np


def save_return_contribution_plots(report, figure_dir=None):
    """保存月均归一化仓位、月度资产贡献热图和组合来源汇总，返回文件路径字典。

    默认写入项目根目录，所有图使用已计算的 report，不重新读取或变换原始数据。
    全部资产都会展示；图中的仓位以百分比表示，收益贡献以百分点表示。
    """
    try:
        from matplotlib.backends.backend_agg import FigureCanvasAgg
        from matplotlib.colors import LinearSegmentedColormap
        from matplotlib.figure import Figure
        from matplotlib.ticker import PercentFormatter
    except ImportError as exc:
        raise RuntimeError("保存审计图表需要 matplotlib，请先运行 python -m pip install matplotlib") from exc

    output = (Path(figure_dir) if figure_dir is not None else Path(__file__).resolve().parent.parent).resolve()
    output.mkdir(parents=True, exist_ok=True)
    assets = report["monthly_assets"]
    months = assets["month"].drop_duplicates().tolist()
    order = (assets.assign(absolute_contribution=assets["return_contribution"].abs())
             .groupby("asset", sort=False)["absolute_contribution"].sum()
             .sort_values(ascending=False, kind="stable").index)
    signed_map = LinearSegmentedColormap.from_list("signed_contribution", ["#2873a6", "#fafafa", "#c64145"])
    absolute_map = LinearSegmentedColormap.from_list("absolute_position", ["#f3f6fb", "#649bc3", "#17426d"])
    signed_map.set_bad("#e8ebef")
    absolute_map.set_bad("#e8ebef")
    paths = {}

    def new_figure(width, height):
        figure = Figure(figsize=(width, height))
        FigureCanvasAgg(figure)
        return figure

    def heatmap(figure, axis, field, title, label, *, signed=True):
        matrix = assets.pivot(index="asset", columns="month", values=field).reindex(index=order, columns=months)
        values = matrix.to_numpy(dtype=float) * 100
        finite = np.abs(values[np.isfinite(values)])
        limit = (float(finite.max()) if finite.size else 0.0) or 1.0
        heat = axis.imshow(np.ma.masked_invalid(values), aspect="auto", interpolation="nearest",
                           cmap=signed_map if signed else absolute_map,
                           vmin=-limit if signed else 0, vmax=limit)
        axis.set_xticks(np.arange(len(months)))
        axis.set_xticklabels(months, rotation=60, ha="right", fontsize=8)
        axis.set_yticks(np.arange(len(order)))
        axis.set_yticklabels(order.to_numpy(), fontsize=8)
        axis.set_title(title)
        figure.colorbar(heat, ax=axis, label=label)

    def save(figure, name):
        path = output / (name + ".png")
        try:
            figure.savefig(path, dpi=160)
        finally:
            figure.clear()
        paths[name] = path

    height = max(4.5, len(order) * 0.25 + 1.8)
    figure = new_figure(max(16, len(months) * 0.9), height)
    axes = figure.subplots(1, 2)
    heatmap(figure, axes[0], "mean_weight", "Monthly mean normalized weight", "Mean weight (%)")
    heatmap(figure, axes[1], "mean_absolute_weight", "Monthly mean absolute normalized weight",
            "Mean absolute weight (%)", signed=False)
    figure.suptitle("Hourly cross-sectional normalization | Zero-position hours included")
    figure.tight_layout(rect=(0, 0, 1, 0.95))
    save(figure, "return_contribution_positions")

    figure = new_figure(max(12, len(months) * 0.65 + 4), height)
    axis = figure.subplots()
    heatmap(figure, axis, "return_contribution", "Monthly asset return contribution",
            "Contribution (percentage points)")
    figure.suptitle("Beginning weight x next-hour return | Pre-cost, hourly sum")
    figure.tight_layout(rect=(0, 0, 1, 0.95))
    save(figure, "return_contribution_assets")

    summary = report["monthly_summary"].set_index("month").reindex(months)
    figure = new_figure(max(12, len(months) * 0.65 + 3), 8)
    axes = figure.subplots(2, 1, sharex=True)
    x = np.arange(len(months))
    axes[0].bar(x, summary["total_positive_contribution"].to_numpy() * 100,
                color="#208a77", label="Positive contributions")
    axes[0].bar(x, summary["total_negative_contribution"].to_numpy() * 100,
                color="#c64145", label="Negative contributions")
    axes[0].plot(x, summary["portfolio_return_arithmetic"].to_numpy() * 100,
                 color="#25364f", marker="o", markersize=4, label="Portfolio hourly sum")
    axes[0].axhline(0, color="#7b8797", linewidth=0.7)
    axes[0].set(ylabel="Contribution (percentage points)", title="Monthly return sources (pre-cost, no linking)")
    for field, label, color in (
            ("top1_positive_share", "Top 1 / positive contributions", "#208a77"),
            ("top3_positive_share", "Top 3 / positive contributions", "#5272b8"),
            ("top3_absolute_contribution_share", "Top 3 / absolute contributions", "#b77e29")):
        axes[1].plot(x, summary[field].to_numpy(), marker="o", markersize=3, label=label, color=color)
    axes[1].set(ylabel="Share", ylim=(-0.03, 1.05), title="Concentration of monthly contribution sources")
    axes[1].yaxis.set_major_formatter(PercentFormatter(1))
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(months, rotation=60, ha="right", fontsize=8)
    for axis in axes:
        axis.grid(axis="y", alpha=0.2)
        axis.legend(loc="best", fontsize=8)
    figure.tight_layout()
    save(figure, "return_contribution_summary")
    return paths
