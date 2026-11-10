"""只读诊断已有 weight 和事件结果；不运行策略、不平滑、不修改输入。"""

import json

import numpy as np
import pandas as pd

from common.validation import validate_index
from postprocessing.weight_data import ASSET_SUFFIX, parse_weight_features, score_weight_features
from postprocessing.weight_parameters import ACTIVE_MAPPINGS, OUTPUT_TIME_OFFSET_HOURS, WeightParameters
from postprocessing.weight_time import event_target_hours


EVENT_COLUMNS = ["asset", "event_relation", "round2_ai_response", "round3_ai_response",
                 "round2_status", "round3_status"]


def _threshold(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not np.isfinite(value) or value < 0:
        raise ValueError("epsilon 必须为非负有限数值")
    return float(value)


def analyze_weight(weight, *, epsilon=0.0):
    """返回 hourly、assets、signed_share、flat_periods 和 summary。

    active_assets 是 abs(weight)>epsilon 的资产数，不代表交易所可交易资产数。
    集中度按过滤 epsilon 后的 |weight| 占比计算，是信号集中度，不是资本或风险敞口。
    缺失小时不补零；NaN/无穷大报错。已有时间轴原样保留，不再平移 8 小时。
    """
    epsilon = _threshold(epsilon)
    if not isinstance(weight, pd.DataFrame) or weight.empty or not weight.columns.is_unique:
        raise ValueError("weight 必须为非空、列名唯一的 DataFrame")
    if (not isinstance(weight.index, pd.DatetimeIndex) or weight.index.hasnans
            or not weight.index.is_unique or not weight.index.is_monotonic_increasing
            or not weight.index.equals(weight.index.floor("h"))):
        raise ValueError("weight 索引必须为唯一、递增的整点 DatetimeIndex")
    if any(not isinstance(asset, str) or not asset for asset in weight.columns):
        raise ValueError("weight 列名必须为非空资产字符串")
    numeric = weight.astype("float64")
    if not np.isfinite(numeric.to_numpy()).all():
        raise ValueError("weight 含 NaN 或无穷大，不能将缺失值当作无信号")
    filtered = numeric.where(numeric.abs() > epsilon, 0.0)
    absolute = filtered.abs()
    gross = absolute.sum(axis=1)
    share = absolute.div(gross.replace(0, np.nan), axis=0)
    ordered = np.sort(share.fillna(0.0).to_numpy(), axis=1)
    active = filtered.ne(0)
    hourly = pd.DataFrame(index=weight.index.copy())
    hourly.index.name = "date"
    hourly["active_assets"] = active.sum(axis=1)
    hourly["positive_assets"] = filtered.gt(0).sum(axis=1)
    hourly["negative_assets"] = filtered.lt(0).sum(axis=1)
    hourly["gross_score"] = gross
    hourly["net_score"] = filtered.sum(axis=1)
    hourly["top1_share"] = share.max(axis=1)
    hourly["top3_share"] = pd.Series(ordered[:, -3:].sum(axis=1), index=weight.index).where(gross > 0)
    hourly["effective_assets"] = (1.0 / share.pow(2).sum(axis=1).replace(0, np.nan)).fillna(0.0)
    # 全零行留空，不把 idxmax 的第一个资产误标为主要资产。
    dominant = absolute.to_numpy().argmax(axis=1)
    hourly["dominant_asset"] = pd.Series(weight.columns.to_numpy()[dominant], index=weight.index).where(gross > 0, "")
    hourly["dominant_score"] = filtered.to_numpy()[np.arange(len(filtered)), dominant]
    assets = pd.DataFrame({"active_hours": active.sum(), "positive_hours": filtered.gt(0).sum(),
                           "negative_hours": filtered.lt(0).sum(), "absolute_score_sum": absolute.sum()})
    assets["active_share"] = assets["active_hours"] / len(weight)
    assets["dominant_hours"] = hourly["dominant_asset"].value_counts().reindex(assets.index, fill_value=0)
    assets = assets.sort_values("absolute_score_sum", ascending=False, kind="stable")
    assets = assets.rename_axis("asset")
    deltas = weight.index.to_series().diff()
    gaps = deltas.ne(pd.Timedelta(hours=1))
    flat = hourly["active_assets"].eq(0)
    groups = (flat.ne(flat.shift()) | gaps).cumsum()
    periods = []
    for _, block in hourly.loc[flat].groupby(groups.loc[flat]):
        periods.append({"start": block.index[0], "last_hour": block.index[-1], "hours": len(block)})
    flat_periods = pd.DataFrame(periods, columns=["start", "last_hour", "hours"])
    expected = int((weight.index[-1] - weight.index[0]) / pd.Timedelta(hours=1)) + 1
    summary = {"hours": len(weight), "assets": len(weight.columns), "epsilon": epsilon,
               "start": weight.index[0].isoformat(), "end": weight.index[-1].isoformat(),
               "missing_hours": expected - len(weight), "active_hour_share": float((~flat).mean()),
               "median_active_assets": float(hourly["active_assets"].median()),
               "longest_flat_hours": int(flat_periods["hours"].max()) if periods else 0}
    return {"hourly": hourly, "assets": assets,
            "signed_share": filtered.div(gross.replace(0, np.nan), axis=0).rename_axis("date"),
            "flat_periods": flat_periods, "summary": summary}


def _distribution(series, field, scope):
    counts = series.value_counts(dropna=False)
    return [{"field": field, "scope": scope, "value": str(value), "count": int(count),
             "share": float(count / len(series))} for value, count in counts.items()]


def analyze_events(results, *, parameters=None, assets=None, epsilon=1e-10,
                   output_time_offset_hours=OUTPUT_TIME_OFFSET_HOURS):
    """按实际评分映射解释零分及同资产、同目标小时的抵消。

    第二轮分布使用第二轮成功记录；第三轮/评分使用两轮均成功记录，不输出未完成占比。
    第三轮同时展示全部披露和 new/update 子集，便于辨别 repeat 复制响应的影响。
    zero_causes 的原因可重叠；zero_combinations 互斥。persistence 不参与零分归因。
    """
    epsilon = _threshold(epsilon)
    parameters = WeightParameters() if parameters is None else parameters
    validate_index(results, "record_id")
    if set(EVENT_COLUMNS) - set(results.columns):
        raise ValueError("事件结果表缺少列：{}".format(sorted(set(EVENT_COLUMNS) - set(results.columns))))
    selected = results if assets is None else results.loc[results["asset"].isin(assets)]
    round2 = selected.loc[selected["round2_status"].eq("1")]
    distributions = _distribution(round2["event_relation"], "round2_final_relation", "round2_completed")
    raw_relations = []
    for key, relation, raw in round2[["event_relation", "round2_ai_response"]].itertuples(name=None):
        if raw == "":
            raw_relations.append("automatic_new" if relation == "new" else "no_saved_response")
            continue
        try:
            body = json.loads(json.loads(raw)["ai_response_content"])
            raw_relation = body["event_relation"]
            if not isinstance(raw_relation, str) or not raw_relation:
                raise ValueError("event_relation 为空")
            raw_relations.append(raw_relation)
        except (TypeError, ValueError, KeyError) as exc:
            raise ValueError("第二轮成功记录 {} 的回答无法解析".format(key[1])) from exc
    distributions += _distribution(pd.Series(raw_relations, dtype=object), "round2_ai_relation", "round2_completed")
    features = parse_weight_features(selected)
    if features.empty:
        raise ValueError("范围内没有两轮均成功的记录可供评分诊断")
    for field in ("direction", "materiality", "execution_stage", "persistence"):
        distributions += _distribution(features[field], field, "all_disclosures")
        distributions += _distribution(features.loc[features["event_relation"].ne("repeat"), field], field, "new_update")
    detail = features.copy()
    detail["score"] = score_weight_features(features, parameters)["data"]
    detail["target_hour"] = event_target_hours(detail["date"]) + pd.Timedelta(hours=output_time_offset_hours)
    reasons = [[] for _ in range(len(detail))]
    for field in ACTIVE_MAPPINGS:
        mapped = detail[field].map(dict(parameters.mappings[field]))
        if field == "direction":
            mapped = mapped.fillna(parameters.mappings[field]["其他"])
        for position in np.flatnonzero(mapped.to_numpy() == 0):
            reasons[position].append("{}={}".format(field, detail.iloc[position][field]))
    for coefficient in ("B", "C", "D"):
        if getattr(parameters, coefficient) == 0:
            for row in reasons:
                row.append(coefficient + "=0")
    detail["zero_reasons"] = [" | ".join(row) for row in reasons]
    zero = detail["score"].eq(0)
    combinations = detail.loc[zero, "zero_reasons"].replace("", "numeric_underflow").value_counts()
    zero_combinations = combinations.rename_axis("reason").rename("count").to_frame()
    causes = pd.Series([reason for row in reasons for reason in row], dtype=object).value_counts()
    zero_causes = causes.rename_axis("reason").rename("count").to_frame()
    for table in (zero_causes, zero_combinations):
        table["share_of_events"] = table["count"] / len(detail)
        table["share_of_zero_events"] = table["count"] / int(zero.sum()) if zero.any() else 0.0
    contributions = detail.assign(positive_score=detail["score"].clip(lower=0),
                                  negative_score=-detail["score"].clip(upper=0), zero_events=zero.astype(int))
    pairs = contributions.groupby(["target_hour", "asset"], sort=True).agg(
        events=("score", "size"), zero_events=("zero_events", "sum"),
        positive_score=("positive_score", "sum"), negative_score=("negative_score", "sum"))
    pairs.index.names = ["date", "asset"]
    pairs["gross_score"] = pairs["positive_score"] + pairs["negative_score"]
    pairs["net_score"] = pairs["positive_score"] - pairs["negative_score"]
    pairs["absolute_net_score"] = pairs["net_score"].abs()
    pairs["cancellation_ratio"] = 1 - pairs["absolute_net_score"] / pairs["gross_score"].replace(0, np.nan)
    both = pairs["positive_score"].gt(0) & pairs["negative_score"].gt(0)
    pairs["both_sides"] = both
    pairs["fully_cancelled"] = both & pairs["absolute_net_score"].le(epsilon)
    pairs["partially_cancelled"] = both & ~pairs["fully_cancelled"]
    pairs["only_zero_events"] = pairs["gross_score"].eq(0)
    hourly = pairs.groupby(level="date").sum(numeric_only=True)
    hourly["asset_hours_with_events"] = pairs.groupby(level="date").size()
    hourly["cancellation_ratio"] = 1 - hourly["absolute_net_score"] / hourly["gross_score"].replace(0, np.nan)
    hourly["zero_event_share"] = hourly["zero_events"] / hourly["events"]
    return {"distributions": pd.DataFrame(distributions), "event_scores": detail,
            "zero_causes": zero_causes, "zero_combinations": zero_combinations,
            "asset_hours": pairs, "hourly": hourly,
            "summary": {"round2_completed": len(round2), "scored_events": len(detail),
                        "zero_events": int(zero.sum()), "asset_hours_with_events": len(pairs),
                        "fully_cancelled_asset_hours": int(pairs["fully_cancelled"].sum()),
                        "partially_cancelled_asset_hours": int(pairs["partially_cancelled"].sum()),
                        "output_time_offset_hours": output_time_offset_hours,
                        "parameters": parameters.to_dict()}}


def event_assets_for_weight(weight):
    """仅去掉已约定的完整后缀，不按第一个点猜测资产名称。"""
    return [column[:-len(ASSET_SUFFIX)] if column.endswith(ASSET_SUFFIX) else column for column in weight.columns]
