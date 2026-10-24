"""从已读取的事件结果 df 分析 asset 频数；不连接数据库、不修改原表。

用法：
    from postprocessing.asset_frequency import plot_asset_frequency
    frequency, figure = plot_asset_frequency(df, top_n=30)

绘图需要 pandas、matplotlib；只统计时使用 asset_frequency，不需要 matplotlib。
asset 按原值分别统计，方便发现名称不统一，不自动合并大小写、空白或别名。
"""

from pathlib import Path

import numpy as np
import pandas as pd


def asset_frequency(df):
    """返回 count、share、cumulative_share；比例为 0～1，分母包含所有有效资产。

    只读取 asset 列，每行计一次；不检查任务状态或事件组，不要求第二、三轮运行。
    无有效 asset 的行排除，数量记入结果 attrs。
    仅统计 df 已包含的资产，未出现的交易标的不自动补零。
    """
    if not isinstance(df, pd.DataFrame) or not df.columns.is_unique:
        raise ValueError("df 必须为列名唯一的 DataFrame")
    if "asset" not in df.columns:
        raise ValueError("df 缺少 asset 列")
    selected = df[["asset"]].copy()
    valid_assets = selected["asset"].map(lambda value: isinstance(value, str) and bool(value.strip()))
    excluded_assets = int((~valid_assets).sum())
    selected = selected.loc[valid_assets]
    counts = selected.groupby("asset", sort=True).size().sort_values(ascending=False, kind="stable")
    frequency = counts.rename("count").to_frame()
    total = int(counts.sum())
    frequency["share"] = frequency["count"] / total if total else 0.0
    frequency["cumulative_share"] = frequency["count"].cumsum() / total if total else 0.0
    frequency.attrs.update(input_rows=len(df), excluded_asset_rows=excluded_assets,
                           counted_total=total)
    return frequency


def plot_asset_frequency(df, *, top_n=30, output_dir=None, show=True):
    """绘制 Top N 条形图、全资产排名频数、全资产累计覆盖率，返回 (frequency, figure)。

    output_dir 可选，保存 asset_frequency.csv 和 asset_frequency.png。
    累计占比始终按全部资产计算，不把 Top N 重新归一化为 100%。
    图中文字使用英文，避免依赖运行机器的中文字体。
    """
    if isinstance(top_n, bool) or not isinstance(top_n, int) or top_n < 1:
        raise ValueError("top_n 必须为正整数")
    frequency = asset_frequency(df)
    if frequency.empty:
        raise ValueError("df 中没有可统计的 asset")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import PercentFormatter

    top = frequency.head(top_n).iloc[::-1]
    figure, axes = plt.subplots(1, 3, figsize=(18, max(6, len(top) * 0.24)),
                                gridspec_kw={"width_ratios": [1.25, 1, 1]})
    # 旧版 Matplotlib 会对输入做 x[:, None]；Pandas 2 已不支持这种索引。
    # 绘图边界统一使用 NumPy 数组，返回的频数表仍保留为 DataFrame。
    axes[0].barh(top.index.to_numpy(), top["count"].to_numpy(), color="#3976B8")
    axes[0].set(title="Top {} assets".format(len(top)), xlabel="Count")
    axes[0].grid(axis="x", alpha=0.2)

    ranks = np.arange(1, len(frequency) + 1)
    axes[1].plot(ranks, frequency["count"].to_numpy(), color="#3976B8", marker=".", markersize=3)
    axes[1].set(xscale="log", yscale="log", title="All assets: rank vs count",
                xlabel="Asset rank (log)", ylabel="Count (log)")
    axes[1].grid(alpha=0.2)

    cumulative = frequency["cumulative_share"].to_numpy(dtype=float)
    axes[2].plot(np.arange(len(frequency) + 1), np.concatenate(([0.0], cumulative)), color="#168273")
    for threshold in (0.8, 0.9, 0.95):
        needed = int(cumulative.searchsorted(threshold)) + 1
        needed = min(needed, len(frequency))
        axes[2].axhline(threshold, linestyle="--", alpha=0.25, color="#667085")
        axes[2].scatter([needed], [cumulative[needed - 1]], s=18, color="#168273")
        axes[2].annotate("{:g}%: {} assets".format(threshold * 100, needed),
                         (needed, threshold), xytext=(3, -12), textcoords="offset points", fontsize=9)
    axes[2].set(title="Cumulative coverage: all assets", xlabel="Number of top assets", ylim=(0, 1.04))
    axes[2].yaxis.set_major_formatter(PercentFormatter(1))
    axes[2].grid(alpha=0.2)
    figure.suptitle("Asset frequency | {:,} rows / {:,} assets".format(
        frequency.attrs["counted_total"], len(frequency)), fontsize=14)
    # 使用旧版 Matplotlib 也支持的布局接口，并为总标题留出空间。
    figure.tight_layout(rect=(0, 0, 1, 0.95))
    if output_dir is not None:
        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        frequency.to_csv(output / "asset_frequency.csv", encoding="utf-8-sig")
        figure.savefig(output / "asset_frequency.png", dpi=160)
    if show:
        plt.show()
    return frequency, figure
