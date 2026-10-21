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
from postprocessing.weight_parameters import ACTIVE_MAPPINGS, OUTPUT_TIME_OFFSET_HOURS, WeightParameters


logger = logging.getLogger(__name__)

ASSET_SUFFIX = ".USDT.UFUTURE.BINANCE"
EVENT_RELATIONS = ("new", "update", "repeat")
SOURCE_COLUMNS = ["asset", "event_relation", "round3_ai_response", "round2_status", "round3_status"]


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
    """逐行解析本条已保存的响应；repeat 的响应填充由第三轮负责。"""
    validate_index(results, "record_id")
    missing = set(SOURCE_COLUMNS) - set(results.columns)
    if missing:
        raise ValueError("结果表缺少字段：{}".format(sorted(missing)))
    rows = []
    for key, asset, relation, response, status2, status3 in results[SOURCE_COLUMNS].itertuples(name=None):
        try:
            if status2 != "1" or status3 != "1":
                raise ValueError("第二、三轮尚未全部成功完成")
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
    logger.info("权重特征解析完成：事件=%d，全部使用本条已保存响应", len(features))
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


def build_weight_data(basic_df, *, start_time, end_time=None):
    """在源时间轴归入下一小时并求和补零；写库入口最后再将索引加 8 小时。"""
    if basic_df.empty:
        raise ValueError("结果表没有事件，无法确定权重表的资产列及结束时间")
    assets = sorted(basic_df["asset"].unique())
    # [15:00, 16:00) 的事件统一计入 16:00，整点事件也顺延一小时。
    target_hours = pd.to_datetime(basic_df["date"], format="ISO8601").dt.floor("h") + pd.Timedelta(hours=1)
    begin = pd.Timestamp(start_time)
    end = pd.Timestamp(end_time) + pd.Timedelta(days=1) if end_time is not None else max(begin, target_hours.max())
    if begin > end:
        raise ValueError("权重时间轴起点不能晚于终点")
    hours = pd.date_range(begin, end, freq="h", name="date")
    data = basic_df["data"].astype("float64", copy=False)
    if not np.isfinite(data.to_numpy()).all():
        raise ValueError("basic_df.data 包含非有限值")
    hourly = data.groupby([target_hours.rename("date"), basic_df["asset"]], sort=True).sum().unstack("asset", fill_value=0.0)
    # 先用数值完成聚合和补零，最后才转文本；date 单独保留为 DatetimeIndex。
    numeric = hourly.reindex(index=hours, columns=assets, fill_value=0.0).astype("float64")
    if not np.isfinite(numeric.to_numpy()).all():
        raise ValueError("小时权重聚合结果出现非有限值或数值溢出")
    weight_data = numeric.astype(str)
    weight_data = weight_data.rename(columns={asset: asset + ASSET_SUFFIX for asset in assets})
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


def _column_comparison(results, columns):
    """直接从源表 asset 原值生成预期列名，供写入检查和已有表核对。"""
    assets = sorted(results["asset"].unique())
    expected = [asset + ASSET_SUFFIX for asset in assets]
    actual = list(columns)
    return {"source_assets": assets, "expected_columns": expected, "actual_columns": actual,
            "unexpected_columns": sorted(set(actual) - set(expected), key=str),
            "missing_columns": sorted(set(expected) - set(actual)),
            "columns_unique": columns.is_unique}


def audit_weight_columns(cli, result_path, weight_path, *, start_time, end_time):
    """只读比较配置范围内源表 asset 与已有权重表列名，不解析回答或写库。"""
    _validate_weight_request(result_path, weight_path, start_time, end_time)
    results = read_event_history(cli, result_path, start_time, end_time,
                                 columns=["asset"], label="权重列名核对来源读取")
    if any(not isinstance(asset, str) or not asset.strip() for asset in results["asset"]):
        raise ValueError("asset 必须为非空字符串")
    stored = cli.read(weight_path, start_time=start_time, end_time=end_time)
    if not isinstance(stored, pd.DataFrame):
        raise ValueError("权重表读取结果必须为 DataFrame")
    return dict(_column_comparison(results, stored.columns), result_path=result_path, weight_path=weight_path,
                start_time=start_time, end_time=end_time, source_rows=len(results))


def _save_weight_data(cli, weight_path, weight_data):
    """首次建表；已有表更新重叠行并追加新索引，保留范围外数据及批次起点边界。"""
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
    columns = existing.columns.union(weight_data.columns, sort=False).sort_values()
    batch = weight_data.reindex(columns=columns, fill_value="0.0")
    existing = existing.reindex(columns=columns, fill_value="0.0")
    boundary = batch.index[0]
    if boundary in existing.index:
        # 起点已整体加 8 小时；该时刻的已有值属于上一批，不能用补零覆盖。
        batch.loc[boundary] = existing.loc[boundary]

    # 新资产历史上没有列，先按列补零，避免历史行因扩列出现非文本空值。
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


def _write_weight_features(cli, features, result_path, weight_path, *, start_time, end_time, parameters):
    """保存一个组合，网格调用方复用同一份已解析特征。"""
    basic_df = score_weight_features(features, parameters)
    weight_data = build_weight_data(basic_df, start_time=start_time, end_time=end_time)
    comparison = _column_comparison(features, weight_data.columns)
    if comparison["unexpected_columns"] or comparison["missing_columns"] or not comparison["columns_unique"]:
        raise ValueError("权重列名与源表 asset 不一致：多出={}，缺少={}，列名唯一={}".format(
            comparison["unexpected_columns"], comparison["missing_columns"], comparison["columns_unique"]))
    logger.info("权重列名来源核对通过：result_path=%s，weight_path=%s，asset去重原值=%s",
                result_path, weight_path, comparison["source_assets"])
    # 完成评分、聚合、补零和文本转换后，仅平移最终索引，不改变源数据读取范围。
    # 先平移再与已有表对齐，避免续写重复加时或破坏上一批边界值。
    weight_data.index = weight_data.index + pd.Timedelta(hours=OUTPUT_TIME_OFFSET_HOURS)
    logger.info("权重表计算完成：小时=%d，资产=%d，起点=%s，终点=%s",
                len(weight_data), len(weight_data.columns), weight_data.index[0], weight_data.index[-1])
    saved = _save_weight_data(cli, weight_path, weight_data)
    logger.info("权重表已保存：%s", weight_path)
    return saved


def write_weight_data(cli, result_path, weight_path, *, start_time, end_time, parameters=None):
    """单组合读取及续写接口；网格入口为每个组合自动分配独立表路径。"""
    _validate_weight_request(result_path, weight_path, start_time, end_time)
    logger.info("权重来源读取：起点=%s，终点=%s", start_time, end_time)
    results = read_event_history(cli, result_path, start_time, end_time,
                                 columns=SOURCE_COLUMNS, label="权重来源读取")
    features = parse_weight_features(results)
    parameters = WeightParameters() if parameters is None else parameters
    return _write_weight_features(cli, features, result_path, weight_path, start_time=start_time,
                                  end_time=end_time, parameters=parameters)
