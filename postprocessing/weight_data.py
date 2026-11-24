"""将第二、三轮共用结果表转换为连续小时的资产权重。"""

import json
import logging
from datetime import date

import pandas as pd
import numpy as np

from common.config import RunConfig
from common.persistence import write_with_retry
from common.store import read_event_history
from common.time_rules import published_time
from common.validation import validate_index
from initialization.event_records import _is_data_not_found
from postprocessing.weight_parameters import (
    ACTIVE_MAPPINGS, OUTPUT_TIME_OFFSET_HOURS, WeightParameters, validate_aggregation_method,
)
from postprocessing.weight_time import event_target_hours, event_window_bounds, weight_output_bounds


logger = logging.getLogger(__name__)

ASSET_SUFFIX = ".USDT.UFUTURE.BINANCE"
EVENT_RELATIONS = ("new", "update", "repeat")
SOURCE_COLUMNS = ["asset", "event_relation", "round3_ai_response", "round2_status", "round3_status"]


def skipped_weight_events(results):
    """返回权重跳过的原始事件；两轮状态必须同时为字符串 1 才参与评分。"""
    validate_index(results, "record_id")
    missing = {"asset", "round2_status", "round3_status"} - set(results.columns)
    if missing:
        raise ValueError("结果表缺少字段：{}".format(sorted(missing)))
    successful = results["round2_status"].eq("1") & results["round3_status"].eq("1")
    return results.loc[~successful.fillna(False)].copy()


def _parse_impact(raw_response):
    envelope = json.loads(raw_response)
    if not isinstance(envelope, dict):
        raise ValueError("round3_ai_response 必须为 JSON 对象")
    body = json.loads(envelope["ai_response_content"])
    if not isinstance(body, dict):
        raise ValueError("ai_response_content 必须为 JSON 对象")
    for field in ("direction", "materiality", "execution_stage"):
        if not isinstance(body[field], str) or not body[field].strip():
            raise ValueError("{} 必须为非空字符串".format(field))
    return {field: body.get(field, "") for field in ("direction", "materiality", "execution_stage", "persistence")}


def parse_weight_features(results):
    """只解析第二、三轮均成功的记录；repeat 的响应填充由第三轮负责。"""
    validate_index(results, "record_id")
    missing = set(SOURCE_COLUMNS) - set(results.columns)
    if missing:
        raise ValueError("结果表缺少字段：{}".format(sorted(missing)))
    rows = []
    for key, asset, relation, response, status2, status3 in results[SOURCE_COLUMNS].itertuples(name=None):
        if status2 != "1" or status3 != "1":
            continue
        try:
            if not isinstance(asset, str) or not asset.strip():
                raise ValueError("asset 必须为非空字符串")
            if relation not in EVENT_RELATIONS:
                raise ValueError("event_relation 必须为 new/update/repeat")
            stamp = published_time(key[0])
            # 保留源表的钟面时间，不转换时区；小时归属在生成 weight_data 时处理。
            if stamp.tzinfo is not None:
                stamp = stamp.tz_localize(None)
            impact = _parse_impact(response)
        except (ValueError, TypeError, KeyError) as exc:
            raise ValueError("事件 {} 无法计算权重：{}".format(key[1], exc)) from exc
        rows.append((stamp, key[1], asset, impact["direction"], impact["materiality"],
                     impact["execution_stage"], impact["persistence"], relation))
    features = pd.DataFrame(rows, columns=["date", "record_id", "asset", "direction", "materiality",
                                          "execution_stage", "persistence", "event_relation"])
    logger.info("权重特征解析完成：参与事件=%d，跳过未成功事件=%d，全部使用本条已保存响应",
                len(features), len(results) - len(features))
    if len(features) < len(results):
        skipped = skipped_weight_events(results)
        counts = (skipped.groupby(["asset", "round2_status", "round3_status"], dropna=False)
                  .size().rename("events").reset_index())
        logger.info("权重跳过事件按资产及两轮状态统计：%s", counts.to_dict(orient="records"))
    return features


def score_weight_features(features, parameters):
    """对已解析枚举应用一个组合；零乘数优先归零，避免零与溢出中间值相乘。"""
    if not isinstance(parameters, WeightParameters):
        raise ValueError("parameters 必须为 WeightParameters 标量参数对象")
    factors = {}
    for field in ACTIVE_MAPPINGS:
        mapped = features[field].map(dict(parameters.mappings[field]))
        if field == "direction":
            mapped = mapped.fillna(parameters.mappings[field]["其他"])
        if mapped.isna().any():
            position = mapped.index[mapped.isna()][0]
            raise ValueError("事件 {} 的 {} 未配置映射：{}".format(
                features.at[position, "record_id"], field, features.at[position, field]))
        factors[field] = mapped.to_numpy(dtype="float64")
    scores = np.zeros(len(features), dtype="float64")
    nonzero = np.ones(len(features), dtype=bool)
    for values in factors.values():
        nonzero &= values != 0
    if parameters.B != 0 and parameters.C != 0 and parameters.D != 0:
        with np.errstate(over="ignore", invalid="ignore", under="ignore"):
            scores[nonzero] = (factors["direction"][nonzero]
                               * (parameters.B * factors["materiality"][nonzero])
                               * (parameters.C * factors["execution_stage"][nonzero])
                               * (parameters.D * factors["event_relation"][nonzero]))
    if not np.isfinite(scores).all():
        raise ValueError("参数 {} 的事件评分出现非有限值或数值溢出".format(parameters.param_id))
    basic_df = features[["date", "asset"]].copy()
    basic_df["data"] = scores
    return basic_df


def build_basic_df(results, parameters=None):
    """单组合接口，返回 date/asset/data；默认使用 B=C=D=1 的完整乘法评分。"""
    parameters = WeightParameters() if parameters is None else parameters
    return score_weight_features(parse_weight_features(results), parameters)


def normalize_weight_assets(assets):
    """与实盘一致：接受币名或完整资产名，保留传入顺序，拒绝重复币名。"""
    if isinstance(assets, (str, bytes, dict, set)):
        raise ValueError("assets 必须为有序的资产数组")
    try:
        values = list(assets)
    except TypeError as exc:
        raise ValueError("assets 必须为有序的资产数组") from exc
    columns = {}
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("assets 的每项必须为非空字符串")
        column = value.strip().upper()
        asset = column.split(".", 1)[0]
        if not asset or asset in columns:
            raise ValueError("assets 的币名不能为空或重复：{}".format(asset))
        columns[asset] = column if "." in column else column + ASSET_SUFFIX
    if not columns:
        raise ValueError("assets 不能为空")
    return columns


def load_weight_assets(cli, asset_list_path):
    """权重入口读取一次 InstrumentList，保留实盘宿主传入数组的顺序和列名。"""
    if not isinstance(asset_list_path, str) or not asset_list_path.strip():
        raise ValueError("权重入口必须配置 round2_asset_list_path，或显式传入 assets")
    document = json.loads(cli.read_object(asset_list_path).decode("utf-8"))
    if not isinstance(document, dict) or "InstrumentList" not in document:
        raise ValueError("资产名单必须包含 InstrumentList")
    return list(normalize_weight_assets(document["InstrumentList"]).values())


def build_weight_data(basic_df, *, start_time, end_time=None, aggregation_method="sum", assets=None):
    """按 [T-75分钟, T-15分钟) 聚合到整点 T；传 assets 时固定列及顺序，无信号补零。"""
    validate_aggregation_method(aggregation_method)
    columns = normalize_weight_assets(assets) if assets is not None else None
    if columns is not None:
        basic_df = basic_df.loc[basic_df["asset"].isin(columns)]
    if basic_df.empty and (columns is None or end_time is None):
        raise ValueError("结果表没有事件，无法确定权重表的资产列及结束时间")
    output_assets = list(columns) if columns is not None else sorted(basic_df["asset"].unique())
    if columns is None:
        columns = {asset: asset + ASSET_SUFFIX for asset in output_assets}
    # [16:45, 17:45) 的事件统一计入 18:00；17:45 归入 19:00。
    published_at = pd.to_datetime(basic_df["date"], format="ISO8601")
    target_hours = event_target_hours(published_at)
    begin = pd.Timestamp(start_time)
    end = weight_output_bounds(start_time, end_time)[1] if end_time is not None else max(begin, target_hours.max())
    if begin > end:
        raise ValueError("权重时间轴起点不能晚于终点")
    hours = pd.date_range(begin, end, freq="h", name="date")
    if basic_df.empty:
        return pd.DataFrame(0.0, index=hours, columns=list(columns.values()))
    data = basic_df["data"].astype("float64", copy=False)
    if not np.isfinite(data.to_numpy()).all():
        raise ValueError("basic_df.data 包含非有限值")
    if aggregation_method == "sum":
        aggregated = data.groupby([target_hours.rename("date"), basic_df["asset"]], sort=True).sum()
    else:
        # 使用独立的位置索引，兼容 basic_df 的重复行标签；稳定排序使并列规则可复现。
        events = pd.DataFrame({"date": target_hours.to_numpy(), "asset": basic_df["asset"].to_numpy(),
                               "data": data.to_numpy(), "published_at": published_at.to_numpy()})
        events = events.sort_values("published_at", kind="stable").reset_index(drop=True)
        selected = events["data"].abs().groupby([events["date"], events["asset"]], sort=True).idxmax()
        aggregated = events.loc[selected].set_index(["date", "asset"])["data"]
    hourly = aggregated.unstack("asset", fill_value=0.0)
    # 聚合、补零及写库均保留 float64；date 单独保留为 DatetimeIndex。
    weight_data = hourly.reindex(index=hours, columns=output_assets, fill_value=0.0).astype("float64")
    if not np.isfinite(weight_data.to_numpy()).all():
        raise ValueError("小时权重聚合结果出现非有限值或数值溢出")
    weight_data = weight_data.rename(columns=columns)
    weight_data.columns.name = None
    return weight_data


def _validate_weight_request(result_path, weight_path, start_time, end_time):
    for label, path in (("result_path", result_path), ("weight_path", weight_path)):
        if not isinstance(path, str) or not path.strip() or "填写" in path:
            raise ValueError("请配置实际的 {}".format(label))
    if result_path == weight_path:
        raise ValueError("结果表与权重表路径必须不同")
    if not isinstance(start_time, str) or not start_time or not isinstance(end_time, str) or not end_time:
        raise ValueError("权重表必须配置 start_time/end_time")
    start, end = date.fromisoformat(start_time), date.fromisoformat(end_time)
    if start.isoformat() != start_time or end.isoformat() != end_time:
        raise ValueError("起止日期必须为 YYYY-MM-DD 字符串")
    if start > end:
        raise ValueError("start_time 不能晚于 end_time")


def _column_comparison(results, columns, *, assets=None):
    """优先按外部资产名单核对列名及顺序；兼容未传名单的内存调用。"""
    source_assets = sorted(results["asset"].unique())
    expected = (list(normalize_weight_assets(assets).values()) if assets is not None else
                [asset + ASSET_SUFFIX for asset in source_assets])
    actual = list(columns)
    return {"source_assets": source_assets, "expected_columns": expected, "actual_columns": actual,
            "unexpected_columns": sorted(set(actual) - set(expected), key=str),
            "missing_columns": sorted(set(expected) - set(actual)),
            "columns_unique": columns.is_unique, "column_order_matches": actual == expected}


def read_weight_events(cli, result_path, start_time, end_time, *, columns, label):
    """补读首尾窗口所需日期，再按精确事件范围筛选，避免跨日批次漏算或覆盖。"""
    begin, end = event_window_bounds(*weight_output_bounds(start_time, end_time))
    results = read_event_history(cli, result_path, begin.date().isoformat(), end.date().isoformat(),
                                 columns=columns, label=label)
    dates = pd.to_datetime(results.index.get_level_values("date"), format="ISO8601")
    if dates.tz is not None:
        dates = dates.tz_localize(None)
    return results.loc[(dates >= begin) & (dates < end)].copy()


def _save_weight_data(cli, weight_path, weight_data, *, fixed_columns=False):
    """首次建表；已有表更新重叠行并追加新索引，保留范围外数据。"""
    missing = False
    try:
        existing = cli.read(weight_path)
    except Exception as exc:
        if not _is_data_not_found(exc):
            raise
        missing = True
        existing = pd.DataFrame()

    def save(operation, frame, stage):
        if not frame.empty:
            write_with_retry(lambda batch: operation(weight_path, batch), frame, RunConfig(),
                             stage, log=logger, failure_message="权重表保存预算耗尽")

    if missing or (isinstance(existing, pd.DataFrame) and existing.shape == (0, 0)):
        if missing:
            cli.register(weight_path, "timeseries", "")
        save(cli.write, weight_data, "weight_data_initialize")
        return weight_data

    if (not isinstance(existing, pd.DataFrame) or not isinstance(existing.index, pd.DatetimeIndex)
            or existing.index.name != "date" or existing.index.tz is not None
            or existing.index.hasnans or not existing.index.is_unique or not existing.columns.is_unique
            or not existing.index.equals(existing.index.floor("h"))):
        raise ValueError("已有权重表必须为唯一的 date 整点 DatetimeIndex，列名也须唯一")
    existing = existing.sort_index()
    new_columns = weight_data.columns.difference(existing.columns, sort=False)
    if fixed_columns:
        unexpected_columns = existing.columns.difference(weight_data.columns, sort=False)
        if len(unexpected_columns):
            raise ValueError("已有权重表含 assets 名单外的资产列，请使用新的 weight_path；"
                             "名单外列={}".format(list(unexpected_columns)))
        # 按列名对齐即可；已有表保留原顺序，返回值按 assets 排列。
        columns = weight_data.columns
    else:
        columns = existing.columns.union(weight_data.columns, sort=False).sort_values()
    batch = weight_data.reindex(columns=columns, fill_value=0.0)
    existing = existing.reindex(columns=columns, fill_value=0.0)
    # 新资产历史上没有列，先按列补浮点零，避免历史行因扩列出现空值。
    if len(new_columns) and not existing.empty:
        save(cli.update_by_column, existing[new_columns], "weight_data_add_columns")
    overlap = batch.index.intersection(existing.index)
    patch = batch.loc[overlap]
    patch = patch.loc[patch.ne(existing.loc[overlap]).any(axis=1)]
    save(cli.update_by_column, patch, "weight_data_update")
    additions = batch.loc[~batch.index.isin(existing.index)]
    save(cli.update_by_index, additions, "weight_data_append")
    logger.info("权重表续写完成：更新小时=%d，追加小时=%d，新增资产列=%d",
                len(patch), len(additions), len(new_columns))
    return batch


def _write_weight_features(cli, features, result_path, weight_path, *, start_time, end_time, parameters, assets=None):
    """保存一个组合，网格调用方复用同一份已解析特征。"""
    basic_df = score_weight_features(features, parameters)
    weight_data = build_weight_data(basic_df, start_time=start_time, end_time=end_time,
                                    aggregation_method=parameters.aggregation_method, assets=assets)
    comparison = _column_comparison(features, weight_data.columns, assets=assets)
    if (comparison["unexpected_columns"] or comparison["missing_columns"] or not comparison["columns_unique"]
            or not comparison["column_order_matches"]):
        raise ValueError("权重列名或顺序与预期不一致：多出={}，缺少={}，列名唯一={}，顺序一致={}".format(
            comparison["unexpected_columns"], comparison["missing_columns"], comparison["columns_unique"],
            comparison["column_order_matches"]))
    logger.info("权重列名来源核对通过：result_path=%s，weight_path=%s，asset去重原值=%s，输出列=%s",
                result_path, weight_path, comparison["source_assets"], comparison["expected_columns"])
    # 完成评分、聚合和补零后，仅平移最终索引。
    # 先平移再与已有表对齐，避免续写重复加时或破坏上一批边界值。
    weight_data.index = weight_data.index + pd.Timedelta(hours=OUTPUT_TIME_OFFSET_HOURS)
    logger.info("权重表计算完成：聚合=%s，小时=%d，资产=%d，起点=%s，终点=%s",
                parameters.aggregation_method, len(weight_data), len(weight_data.columns),
                weight_data.index[0], weight_data.index[-1])
    saved = _save_weight_data(cli, weight_path, weight_data, fixed_columns=assets is not None)
    logger.info("权重表已保存：%s", weight_path)
    return saved


def write_weight_data(cli, result_path, weight_path, *, start_time, end_time, parameters=None, assets=None):
    """单组合读取及续写；传 assets 时在解析前筛选事件，并固定输出列及顺序。"""
    _validate_weight_request(result_path, weight_path, start_time, end_time)
    if assets is not None:
        assets = list(normalize_weight_assets(assets).values())
    logger.info("权重来源读取：起点=%s，终点=%s", start_time, end_time)
    results = read_weight_events(cli, result_path, start_time, end_time,
                                columns=SOURCE_COLUMNS, label="权重来源读取")
    if assets is not None:
        results = results.loc[results["asset"].isin(normalize_weight_assets(assets))]
    features = parse_weight_features(results)
    parameters = WeightParameters() if parameters is None else parameters
    return _write_weight_features(cli, features, result_path, weight_path, start_time=start_time,
                                  end_time=end_time, parameters=parameters, assets=assets)
