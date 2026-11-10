"""归一化 weight 并计算小时持仓收益的月度贡献；不对贡献做复利链接。"""

import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

HOUR = pd.Timedelta(hours=1)


def _day_boundary(value, tz, *, next_day=False):
    day = date.fromisoformat(value)
    if day.isoformat() != value:
        raise ValueError("日期必须为 YYYY-MM-DD")
    if next_day:
        day += timedelta(days=1)
    return pd.Timestamp(day).tz_localize(tz)


def _validate_matrix(frame, name):
    if not isinstance(frame, pd.DataFrame) or frame.empty or not frame.columns.is_unique:
        raise ValueError("{} 必须为非空、列名唯一的 DataFrame".format(name))
    if any(not isinstance(asset, str) or not asset.strip() for asset in frame.columns):
        raise ValueError("{} 资产列名必须为非空字符串".format(name))
    index = frame.index
    if (not isinstance(index, pd.DatetimeIndex) or index.hasnans or not index.is_unique
            or not index.is_monotonic_increasing
            or not index.tz_localize(None).equals(index.tz_localize(None).floor("h"))):
        raise ValueError("{} 索引必须为唯一、递增的整点 DatetimeIndex".format(name))


def _finite(frame, name):
    numeric = frame.astype("float64")
    invalid = ~np.isfinite(numeric.to_numpy())
    if invalid.any():
        row, column = np.argwhere(invalid)[0]
        raise ValueError("{} 在 {} / {} 缺失或非有限，不能补零或跳过".format(
            name, numeric.index[row], numeric.columns[column]))
    return numeric


def _require_weight_rows(weight, required):
    missing = required.difference(weight.index)
    if len(missing):
        raise ValueError("缺少实际期初持仓时间行：{}（共 {} 个小时）；weight 已有时间范围：{} 至 {}。"
                         "NaN 仓位单元格按 0 处理，但不能替代缺失的时间行".format(
                             missing[0], len(missing), weight.index[0], weight.index[-1]))


def _ratio(numerator, denominator):
    return float(numerator / denominator) if denominator > 0 else np.nan


def _build_asset_summary(weights, returns, contributions, monthly_assets):
    """直接从本次小时持仓、收益和贡献计算全期指标，月度中间结果仅用于选月份。"""
    with np.errstate(over="ignore", invalid="ignore"):
        total_returns = (1 + returns).prod() - 1
    if not np.isfinite(total_returns.to_numpy()).all():
        raise ValueError("全期资产收益发生数值溢出")
    mean_weights, absolute_weights = weights.mean(), weights.abs().mean()
    total_contributions = contributions.sum()
    rows = []
    for asset, block in monthly_assets.groupby("asset", sort=False):
        block = block.sort_values("month", kind="stable").reset_index(drop=True)
        peak_weight = block["mean_absolute_weight"].max()
        # 同样的小时仓位在不同长度月份求均值，可能只差浮点舍入误差。
        peak = block.loc[np.isclose(block["mean_absolute_weight"], peak_weight, rtol=1e-12, atol=0)].iloc[0]
        has_position = bool(peak["mean_absolute_weight"] > 0)
        best = block.loc[block["return_contribution"].idxmax()]
        worst = block.loc[block["return_contribution"].idxmin()]
        rows.append({
            "asset": asset, "first_month": block["month"].iloc[0], "last_month": block["month"].iloc[-1],
            "months": len(block), "observed_hours": len(weights),
            "mean_weight": float(mean_weights[asset]), "mean_absolute_weight": float(absolute_weights[asset]),
            "total_asset_return": float(total_returns[asset]), "total_asset_return_available": True,
            "total_return_contribution": float(total_contributions[asset]),
            "positive_months": int(block["return_contribution"].gt(0).sum()),
            "negative_months": int(block["return_contribution"].lt(0).sum()),
            "positive_month_share": float(block["return_contribution"].gt(0).mean()),
            "max_weight_month": peak["month"] if has_position else "",
            "max_month_mean_absolute_weight": float(peak["mean_absolute_weight"]),
            "max_month_mean_weight": float(peak["mean_weight"]),
            "max_weight_month_asset_return": float(peak["asset_return"]) if has_position else np.nan,
            "max_weight_month_return_contribution": float(peak["return_contribution"]) if has_position else 0.0,
            "best_contribution_month": best["month"], "best_month_return_contribution": float(best["return_contribution"]),
            "worst_contribution_month": worst["month"], "worst_month_return_contribution": float(worst["return_contribution"]),
        })
    result = pd.DataFrame(rows)
    if not np.isfinite(result[["mean_weight", "mean_absolute_weight", "total_return_contribution"]].to_numpy()).all():
        raise ValueError("全期仓位或收益贡献发生数值溢出")
    return result.sort_values("total_return_contribution", ascending=False, kind="stable").reset_index(drop=True)


def _boundary_price_returns(first, last):
    valid = np.isfinite(first) & (first > 0) & np.isfinite(last) & (last > 0)
    ratios = np.full(len(first), np.nan, dtype="float64")
    with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
        np.divide(last, first, out=ratios, where=valid)
    values = ratios - 1
    return np.where(valid & np.isfinite(values), values, np.nan)


def _save_report(report, output_dir):
    if output_dir is None:
        return
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    for name in ("monthly_assets", "monthly_rankings", "monthly_summary", "asset_summary"):
        report[name].to_csv(output / (name + ".csv"), index=False, encoding="utf-8-sig")
    (output / "method.json").write_text(json.dumps(report["metadata"], ensure_ascii=False, indent=2), encoding="utf-8")


def analyze_monthly_contributions(actual_weights, hourly_returns, *, start_time=None, end_time=None,
                                  top_n=5, net_return_epsilon=1e-8, output_dir=None):
    """返回三张月度表、asset_summary 资产全期汇总及 metadata。

    actual_weights[s] 是时刻 s 已生效、用于随后 [s,s+1h) 的实际净值权重（0.5=50%）。
    hourly_returns[t] 是 [t-1h,t) 的简单收益率；精确匹配 actual_weights[t-1h]，不按行 shift。
    月份与可选 YYYY-MM-DD 日期按区间起点 s 归属，跨月最后一小时计入前月。
    已有时间行中的 NaN 仓位按零处理，无穷仓位仍报错；缺失整行不会补零。
    所选区间必须连续完整，资产列必须对应；不填收益、不归一化或平移时区。
    贡献为费用前价格收益；假定小时内持仓不变。输出比例为小数，贡献为组合收益百分点。
    """
    if isinstance(top_n, bool) or not isinstance(top_n, int) or top_n < 1:
        raise ValueError("top_n 必须为正整数")
    if (isinstance(net_return_epsilon, bool) or not isinstance(net_return_epsilon, (int, float))
            or not np.isfinite(net_return_epsilon) or net_return_epsilon < 0):
        raise ValueError("net_return_epsilon 必须为非负有限数值")
    _validate_matrix(actual_weights, "actual_weights")
    _validate_matrix(hourly_returns, "hourly_returns")
    if str(actual_weights.index.tz) != str(hourly_returns.index.tz):
        raise ValueError("持仓与收益的时区必须一致，请在调用前显式统一")
    missing_assets = actual_weights.columns.difference(hourly_returns.columns).tolist()
    extra_assets = hourly_returns.columns.difference(actual_weights.columns).tolist()
    if missing_assets or extra_assets:
        raise ValueError("资产列不对应：收益缺少 {}，收益多出 {}".format(missing_assets, extra_assets))

    starts = hourly_returns.index - HOUR
    begin = _day_boundary(start_time, starts.tz) if start_time is not None else starts[0]
    end = (_day_boundary(end_time, starts.tz, next_day=True) if end_time is not None
           else starts[-1] + HOUR)
    if begin >= end:
        raise ValueError("start_time 不能晚于 end_time 或数据范围")
    returns = hourly_returns.loc[(starts >= begin) & (starts < end), actual_weights.columns]
    expected = pd.date_range(begin, end, freq="h", inclusive="left")
    selected_starts = returns.index - HOUR
    missing_hours = expected.difference(selected_starts)
    if len(missing_hours):
        raise ValueError("小时收益缺少 {} 个小时，首个区间起点为 {}".format(len(missing_hours), missing_hours[0]))
    if returns.empty:
        raise ValueError("审计范围内没有小时收益")
    _require_weight_rows(actual_weights, selected_starts)
    selected_weights = actual_weights.loc[selected_starts]
    nan_weight_cells = int(selected_weights.isna().to_numpy().sum())
    weights = _finite(selected_weights.fillna(0.0), "actual_weights")
    weights.index = returns.index.copy()
    returns = _finite(returns, "hourly_returns")
    if returns.lt(-1).any().any():
        raise ValueError("hourly_returns 必须为简单资产收益率，不能低于 -100%")
    with np.errstate(over="ignore", invalid="ignore"):
        contributions = _finite(weights * returns, "hourly_contributions")
    months = selected_starts.strftime("%Y-%m")
    mean_weights = weights.groupby(months).mean()
    absolute_weights = weights.abs().groupby(months).mean()
    monthly_returns = (1 + returns).groupby(months).prod() - 1
    monthly_contributions = contributions.groupby(months).sum()
    if not np.isfinite(monthly_returns.to_numpy()).all() or not np.isfinite(monthly_contributions.to_numpy()).all():
        raise ValueError("月度收益或贡献发生数值溢出")

    asset_rows, ranking_tables, summary_rows = [], [], []
    for month, values in monthly_contributions.iterrows():
        block_starts = selected_starts[months == month]
        net = float(values.sum())
        positive = values.loc[values > 0].sort_values(ascending=False, kind="stable")
        negative = values.loc[values < 0].sort_values(ascending=True, kind="stable")
        profit = float(positive.sum())
        loss = float(-negative.sum())
        gross = float(values.abs().sum())
        net_defined = abs(net) > net_return_epsilon
        for asset, contribution in values.items():
            asset_rows.append({"month": month, "asset": asset,
                               "mean_weight": mean_weights.at[month, asset],
                               "mean_absolute_weight": absolute_weights.at[month, asset],
                               "asset_return": monthly_returns.at[month, asset],
                               "return_contribution": contribution,
                               "observed_hours": len(block_starts)})
        ranking = values.rename("return_contribution").rename_axis("asset").reset_index()
        ranking.insert(0, "month", month)
        ranking["positive_rank"] = ranking["asset"].map({asset: rank for rank, asset in enumerate(positive.index, 1)}).astype("Int64")
        ranking["negative_rank"] = ranking["asset"].map({asset: rank for rank, asset in enumerate(negative.index, 1)}).astype("Int64")
        ranking["is_top_positive"] = ranking["positive_rank"].le(top_n).fillna(False)
        ranking["is_top_negative"] = ranking["negative_rank"].le(top_n).fillna(False)
        ranking["share_of_net_return"] = ranking["return_contribution"] / net if net_defined else np.nan
        ranking["share_of_positive_contribution"] = ranking["return_contribution"].where(
            ranking["return_contribution"] > 0) / profit if profit > 0 else np.nan
        ranking["share_of_negative_contribution"] = -ranking["return_contribution"].where(
            ranking["return_contribution"] < 0) / loss if loss > 0 else np.nan
        ranking["absolute_contribution_share"] = ranking["return_contribution"].abs() / gross if gross > 0 else np.nan
        ranking_tables.append(ranking.sort_values("return_contribution", ascending=False, kind="stable"))
        month_begin = block_starts[0].normalize().replace(day=1)
        next_month = month_begin + pd.offsets.MonthBegin(1)
        summary_rows.append({
            "month": month, "period_start": block_starts[0], "period_end_exclusive": block_starts[-1] + HOUR,
            "observed_hours": len(block_starts),
            "is_full_month": bool(block_starts[0] == month_begin and block_starts[-1] + HOUR == next_month),
            "portfolio_return_arithmetic": net, "total_positive_contribution": profit,
            "total_negative_contribution": -loss,
            "largest_positive_asset": positive.index[0] if len(positive) else "",
            "largest_positive_contribution": float(positive.iloc[0]) if len(positive) else 0.0,
            "largest_negative_asset": negative.index[0] if len(negative) else "",
            "largest_negative_contribution": float(negative.iloc[0]) if len(negative) else 0.0,
            "top1_positive_share": _ratio(positive.head(1).sum(), profit),
            "top3_positive_share": _ratio(positive.head(3).sum(), profit),
            "top1_negative_share": _ratio(-negative.head(1).sum(), loss),
            "top3_absolute_contribution_share": _ratio(values.abs().nlargest(3).sum(), gross),
            "majority_positive_asset": bool(profit > 0 and positive.iloc[0] / profit > 0.5),
            "net_return_share_defined": net_defined,
        })

    report = {"monthly_assets": pd.DataFrame(asset_rows),
              "monthly_rankings": pd.concat(ranking_tables, ignore_index=True),
              "monthly_summary": pd.DataFrame(summary_rows),
              "metadata": {"method": "sum_of_hourly_beginning_weight_times_simple_return",
                           "contribution_linking": "none", "costs_included": False,
                           "weight_nan_policy": "zero position for existing time rows",
                           "nan_weight_cells_filled": nan_weight_cells,
                           "weight_units": "fraction_of_beginning_portfolio_equity",
                           "return_units": "simple_fraction",
                           "contribution_units": "fraction_of_portfolio_return",
                           "weight_timestamp": "beginning of hourly interval, already effective",
                           "return_timestamp": "end of hourly interval",
                           "month_assignment": "interval start in input timezone",
                           "asset_summary_mean_weight": "mean of hourly positions over the audit interval",
                           "asset_summary_price_return": "compound hourly simple returns",
                           "asset_summary_peak_weight": "largest monthly mean_absolute_weight; earliest month on ties",
                           "timezone": str(starts.tz), "top_n": top_n,
                           "net_return_epsilon": net_return_epsilon}}
    report["asset_summary"] = _build_asset_summary(weights, returns, contributions, report["monthly_assets"])
    _save_report(report, output_dir)
    return report


def analyze_price_contributions(weight, close_prices, *, start_time=None, end_time=None,
                                top_n=5, net_return_epsilon=1e-8, output_dir=None):
    """逐小时归一化 weight，再用 close_price_60min 价格宽表计算月度贡献。

    先取资产列交集，再按当小时起点的有限正价格过滤权重，最后逐小时截面归一化。
    已有时间行中的 NaN 权重先按 0 处理；缺少整条时间行仍报错。
    各资产价格 NaN 用此前最近的非空价格前向填充；没有历史值的开头 NaN 保留。
    填充后仍无效的报价视为该时点不可持仓；不用下一小时价格决定当前仓位。
    若已经持仓却缺少有效结束价，仍报错。资产月收益使用有效边界价格，否则留空。
    prices[t] 是时点 t 的价格；w[s] 对应 P[s+1h]/P[s]-1。指定日期含首尾日，
    必须提供结束日次日 00:00 价格。省略日期且两表时间完全相同时，仅最后一行
    持仓因没有后续价格而不参与；若提供了最后持仓后一小时价格，则纳入该小时。
    """
    _validate_matrix(weight, "weight")
    _validate_matrix(close_prices, "close_prices")
    if str(weight.index.tz) != str(close_prices.index.tz):
        raise ValueError("持仓与价格的时区必须一致，请在调用前显式统一")
    input_weight_assets = weight.columns.tolist()
    valid_assets = weight.columns[weight.columns.isin(close_prices.columns)]
    excluded_weight_assets = weight.columns[~weight.columns.isin(close_prices.columns)].tolist()
    if valid_assets.empty:
        raise ValueError("weight 与价格表没有共同资产列，无法计算有效权重")
    # 必须先剔除无价格列的资产，再计算归一化分母；保留 weight 的资产顺序。
    weight = weight.loc[:, valid_assets]
    tz = weight.index.tz
    begin = _day_boundary(start_time, tz) if start_time is not None else weight.index[0]
    end = (_day_boundary(end_time, tz, next_day=True) if end_time is not None
           else weight.index[-1] + HOUR)
    if end_time is None and end not in close_prices.index:
        end = weight.index[-1]
    if begin >= end:
        raise ValueError("审计范围至少需要一个完整持仓小时及其两个端点价格")
    expected_prices = pd.date_range(begin, end, freq="h")
    missing_prices = expected_prices.difference(close_prices.index)
    if len(missing_prices):
        raise ValueError("价格表缺少 {} 个价格时点，首个为 {}".format(len(missing_prices), missing_prices[0]))
    price_history = close_prices.loc[:, weight.columns].astype("float64")
    prices = price_history.ffill().loc[expected_prices]
    nan_price_cells = int((price_history.loc[expected_prices].isna() & prices.notna()).to_numpy().sum())
    required = expected_prices[:-1]
    _require_weight_rows(weight, required)
    price_values = prices.to_numpy()
    price_valid = np.isfinite(price_values) & (price_values > 0)
    entry_valid = pd.DataFrame(price_valid[:-1], index=required, columns=weight.columns)
    # 当前报价不可用就不分配仓位；不能利用未来的结束价重新分配当前仓位。
    selected_weights = weight.loc[required]
    nan_weight_cells = int(selected_weights.isna().to_numpy().sum())
    signals = _finite(selected_weights.fillna(0.0).where(entry_valid, 0.0), "weight")
    gross = signals.abs().sum(axis=1)
    if not np.isfinite(gross.to_numpy()).all():
        raise ValueError("weight 截面绝对值之和发生数值溢出")
    prepared_weights = signals.div(gross.replace(0, np.nan), axis=0).fillna(0.0)

    pairs_valid = price_valid[:-1] & price_valid[1:]
    ratios = np.full(signals.shape, np.nan, dtype="float64")
    with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
        np.divide(price_values[1:], price_values[:-1], out=ratios, where=pairs_valid)
    hourly_values = ratios - 1
    return_valid = pairs_valid & np.isfinite(hourly_values)
    unknown_held = prepared_weights.ne(0).to_numpy() & ~return_valid
    if unknown_held.any():
        row, column = np.argwhere(unknown_held)[0]
        raise ValueError("已持仓资产 {} 的小时收益无法计算：{} 至 {}，起点价格={}，结束价格={}；"
                         "不能将未知持仓收益记为零".format(
                             weight.columns[column], required[row], expected_prices[row + 1],
                             price_values[row, column], price_values[row + 1, column]))
    # 未持仓资产贡献确定为零；此处的零仅供贡献核算，资产月收益另用真实边界价。
    returns = pd.DataFrame(np.where(return_valid, hourly_values, 0.0),
                           index=expected_prices[1:], columns=weight.columns)
    report = analyze_monthly_contributions(prepared_weights, returns, top_n=top_n,
                                           net_return_epsilon=net_return_epsilon)

    counts = pd.DataFrame(return_valid, index=required, columns=weight.columns).groupby(
        required.strftime("%Y-%m")).sum()
    asset_table = report["monthly_assets"]
    asset_table["valid_return_hours"] = [int(counts.at[row.month, row.asset])
                                          for row in asset_table.itertuples(index=False)]
    asset_table["missing_return_hours"] = asset_table["observed_hours"] - asset_table["valid_return_hours"]
    asset_table["asset_return_available"] = False
    for block in report["monthly_summary"].itertuples(index=False):
        selected = asset_table["month"].eq(block.month)
        labels = asset_table.loc[selected, "asset"].tolist()
        first = prices.loc[block.period_start, labels].to_numpy()
        last = prices.loc[block.period_end_exclusive, labels].to_numpy()
        monthly_values = _boundary_price_returns(first, last)
        asset_table.loc[selected, "asset_return"] = monthly_values
        asset_table.loc[selected, "asset_return_available"] = np.isfinite(monthly_values)

    # 全期价格回报直接用数据库价格的审计首末端点；重仓月价格回报取本次已计算的月度端点值。
    total_returns = pd.Series(_boundary_price_returns(prices.iloc[0].to_numpy(), prices.iloc[-1].to_numpy()),
                             index=prices.columns)
    asset_summary = report["asset_summary"]
    asset_summary["total_asset_return"] = asset_summary["asset"].map(total_returns)
    asset_summary["total_asset_return_available"] = asset_summary["total_asset_return"].notna()
    month_returns = asset_table.set_index(["asset", "month"])["asset_return"]
    asset_summary["max_weight_month_asset_return"] = [
        month_returns.loc[(row.asset, row.max_weight_month)] if row.max_weight_month else np.nan
        for row in asset_summary.itertuples(index=False)]

    unavailable_counts = (~entry_valid).sum()
    report["metadata"].update(price_return_formula="close[t] / close[t-1h] - 1",
                               asset_selection="intersection of weight and price columns before normalization",
                               input_weight_assets=input_weight_assets,
                               valid_assets=valid_assets.tolist(),
                               excluded_weight_assets=excluded_weight_assets,
                               nan_weight_cells_filled=nan_weight_cells,
                               price_nan_policy="forward fill from previous non-null price per asset; no backward fill",
                               nan_price_cells_filled=nan_price_cells,
                               normalization="weight / sum(abs(weight)) per hour over assets with valid entry prices; zero rows remain zero",
                               entry_price_filter="finite positive price at interval start; no future prices used for position selection",
                               unavailable_entry_price_asset_hours=int(unavailable_counts.sum()),
                               unavailable_entry_price_hours_by_asset={asset: int(count) for asset, count in unavailable_counts.items() if count},
                               missing_held_return_policy="error; never replace unknown held returns with zero",
                               asset_month_return="end price / start price - 1; undefined for invalid boundary prices",
                               asset_summary_price_return="last processed price / first processed price - 1",
                               final_weight_snapshot_excluded=bool(weight.index[-1] == end))
    _save_report(report, output_dir)
    return report
