"""固定round1事件提取回答展开、去重和导入；自动导入与独立续建保留各自检查。"""

import logging
import re

import pandas as pd

from common.validation import (
    RESULT_COLUMNS,
    INPUT_COLUMNS,
    validate_index,
    validate_event_source_index,
)
from common.time_rules import in_scope, select_scope
from initialization.first_round_answers import parse_first_round_answer


logger = logging.getLogger(__name__)


def expand_first_round(source):
    """检查并展开有币种的事件；空 tickers 跳过，不按账号补全。"""
    validate_event_source_index(source)
    required = {"status", "ai_response_content", "user_name", "text"}
    if not required.issubset(source.columns):
        raise ValueError("第一轮缺少字段：{}".format(sorted(required - set(source.columns))))
    rows, keys, errors = [], [], []
    repaired_count = 0
    skipped_events = 0
    for (raw_date, raw_id), post in source.iterrows():
        post_id = str(raw_id)
        try:
            if post["status"] != "1":
                raise ValueError("第一轮尚未成功")
            if not isinstance(post["text"], str) or not isinstance(post["user_name"], str):
                raise ValueError("原文和用户名必须为字符串")
            post["text"].encode("utf-8")
            post["user_name"].encode("utf-8")
            body, repaired = parse_first_round_answer(post["ai_response_content"])
            if repaired:
                repaired_count += 1
                logger.warning("第一轮帖子 %s 已在导入内存中修复 event 引号；原始回答保留", post_id)
            for slot, event in enumerate(body["events"]):
                tickers = event["tickers"]
                if not tickers:
                    skipped_events += 1
                    continue
                for asset in dict.fromkeys(ticker.strip().upper() for ticker in tickers):
                    record_id = "{}:{}:{}".format(post_id, slot, asset)
                    row = dict.fromkeys(RESULT_COLUMNS, "")
                    row.update(event=event["event"], asset=asset, source_post_id=post_id,
                               round2_status="0", round3_status="0")
                    rows.append(row)
                    keys.append((raw_date, record_id))
        except (ValueError, TypeError, UnicodeError) as exc:
            errors.append((post_id, str(exc)))
            logger.error("第一轮帖子 %s 不合法：%s", post_id, exc)
    logger.info("第一轮事件导入校验完成：帖子数=%d，引号修复数=%d，空币种跳过事件数=%d，不合法数=%d",
                len(source), repaired_count, skipped_events, len(errors))
    if errors:
        post_id, reason = errors[0]
        raise ValueError("第一轮帖子 {} 不合法：{}；本批共 {} 条不合法记录，详情见日志；"
                         "本批在建表或写入前停止".format(post_id, reason, len(errors)))
    if keys:
        index = pd.MultiIndex.from_tuples(keys, names=["date", "record_id"])
    else:
        # 空表保留源时间索引的类型，便于真实客户端注册后完整写入。
        index = pd.MultiIndex.from_arrays([source.index.get_level_values("date")[:0], []], names=["date", "record_id"])
    result = pd.DataFrame(rows, index=index, columns=RESULT_COLUMNS, dtype=object).sort_index()
    return result


def _is_data_not_found(error):
    """只识别用户确认的缺表标识，其他连接、权限及读取错误不转换为建表。"""
    return type(error).__name__ == "DataNotFound" or re.search(r"\bDataNotFound\b", str(error)) is not None


def _save_record_batch(cli, result_path, batch, initial_write):
    """同一完整批次重试；注册由调用方完成，不在写入失败后重新注册。"""
    from common.config import RunConfig
    from common.persistence import write_with_retry

    operation = cli.write if initial_write else cli.update_by_index
    write_with_retry(lambda frame: operation(result_path, frame), batch, RunConfig(),
                     "initialize", failure_message="初始化写入预算耗尽")


def import_event_records_from_posts(cli, settings, new_posts=None, previous_posts=None):
    """导入round1事件提取交出的成功帖，返回本次新写入的事件行数。

    新批次只查元数据后直接追加，依赖原帖 ID 稳定唯一、成功状态不人为回退。
    有历史成功帖时才读旧表，仅核对索引去重；不重读源表或比较旧业务字段。
    不为统计全表总数而读取旧表。所有调用均在主入口线程，失败保留round1事件提取结果。
    """
    from common.validation import validate_event_table_structure

    sources = [frame for frame in (new_posts, previous_posts) if frame is not None and not frame.empty]
    if not sources:
        return 0
    source = sources[0] if len(sources) == 1 else pd.concat(sources)
    expected = expand_first_round(source)
    if any(not in_scope(key[0], settings) for key in source.index):
        raise ValueError("内存成功帖包含本次运行日期范围以外的行")
    validate_event_table_structure(expected)
    recovering = previous_posts is not None and not previous_posts.empty
    initial_write = False
    try:
        if recovering:
            existing = cli.read(settings.result_path)
        else:
            # 正常新批次只确认目标存在，不读取旧记录；元数据内容不参与业务判断。
            cli.read_meta(settings.result_path)
    except Exception as exc:
        if not _is_data_not_found(exc):
            raise
        cli.register(settings.result_path, "timeseries", "")
        initial_write = True

    batch = expected
    if recovering and not initial_write:
        uninitialized = isinstance(existing, pd.DataFrame) and len(existing.index) == 0 and len(existing.columns) == 0
        if uninitialized:
            # 注册后的首次写入失败：非空记录可追加，全部空事件则用 write 补齐结构。
            initial_write = expected.empty
        else:
            validate_index(existing, "record_id")
            batch = expected.loc[~expected.index.isin(existing.index)]
            if len(existing) and not batch.empty:
                if existing.index.levels[0].dtype != batch.index.levels[0].dtype:
                    raise ValueError("已有结果与新增记录的日期索引类型、时区或精度不一致")
                if batch.index.get_level_values("record_id").isin(existing.index.get_level_values("record_id")).any():
                    raise ValueError("新增记录的 record_id 已存在于其他日期")
    if batch.empty and not initial_write:
        return 0
    _save_record_batch(cli, settings.result_path, batch, initial_write)
    return len(batch)


def import_event_records_from_table(cli, settings, *, successful_only=False):
    """自动判断首次建表或续建；已有表严格校验后只追加不存在的新索引。

    缺表只注册一次，write 失败整批重试；已有表不补列或单元格，不写旧行。
    已注册且尚无行列的空目标可继续追加。CLI 抛异常即本次未写入，不读回。
    先读目标，仅明确 DataNotFound 时建表；其他读取错误直接传播。
    successful_only 可只展开当前范围内成功帖子；
    其他帖子及其已有事件不参与本批固定输入比较，也不会被修改。
    """
    from initialization.post_tasks import InitializationSummary
    from common.validation import validate_event_table_structure

    if successful_only and settings.start_time is not None:
        source = cli.read(settings.first_round_task_path,
                          start_time=settings.start_time, end_time=settings.end_time)
    else:
        source = cli.read(settings.first_round_task_path)
    validate_event_source_index(source)
    if successful_only and any(not in_scope(key[0], settings) for key in source.index):
        raise ValueError("第一轮读取返回了本次运行日期范围以外的行")
    source = select_scope(source, settings)
    if successful_only:
        if "status" not in source.columns or not source["status"].isin(["0", "9", "1", "-1"]).all():
            raise ValueError("第一轮 status 字段缺失或状态不合法")
        source = source.loc[source["status"].eq("1")].copy(deep=True)
    expected = expand_first_round(source)
    validate_event_table_structure(expected)
    initial_write = False
    try:
        existing = cli.read(settings.result_path)
    except Exception as exc:
        # 用户提供的真实缺表标识；不能把连接或权限错误当作首次建表。
        if not _is_data_not_found(exc):
            raise
        initial_write = True
    if initial_write:
        batch = expected
        total = len(batch)
        # 在注册前完成全部源数据校验；注册失败不继续写入。
        cli.register(settings.result_path, "timeseries", "")
    else:
        uninitialized = isinstance(existing, pd.DataFrame) and len(existing.index) == 0 and len(existing.columns) == 0
        if uninitialized:
            # 注册成功、首次 write 未成功时目标尚无结构；不放宽非空旧表校验。
            existing = expected.iloc[:0]
        validate_event_table_structure(existing)
        actual_scope = select_scope(existing, settings)
        if successful_only:
            # 同日其他帖子可能尚未成功，不把它们的已有记录当成本批多余输入。
            post_ids = {str(raw_id) for raw_id in source.index.get_level_values("id")}
            actual_scope = actual_scope.loc[actual_scope["source_post_id"].isin(post_ids)]
        if set(actual_scope.index) - set(expected.index):
            raise ValueError("已有结果包含固定第一轮输入之外的记录")
        for key in set(expected.index) & set(existing.index):
            if any(existing.loc[key, name] != expected.loc[key, name] for name in INPUT_COLUMNS):
                raise ValueError("已有记录与固定输入不一致：{}".format(key[1]))
        batch = expected.loc[~expected.index.isin(existing.index)]
        total = len(existing) + len(batch)
        if batch.empty:
            if uninitialized:
                # 注册已成功、首次空表 write 未成功时，也要补齐索引和完整列结构。
                # 目标已经注册，此处只选择 write，不会再次进入上面的注册步骤。
                initial_write = True
            else:
                return InitializationSummary(total, 0)
        if len(existing):
            if existing.index.levels[0].dtype != batch.index.levels[0].dtype:
                raise ValueError("已有结果与新增记录的日期索引类型、时区或精度不一致")
            # 复合键不同也不能让同一 record_id 出现在两个日期，范围外旧行同样检查。
            if batch.index.get_level_values("record_id").isin(existing.index.get_level_values("record_id")).any():
                raise ValueError("新增记录的 record_id 已存在于其他日期")
    _save_record_batch(cli, settings.result_path, batch, initial_write)
    return InitializationSummary(total, len(batch))
