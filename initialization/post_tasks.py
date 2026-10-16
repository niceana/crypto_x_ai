"""按完整源帖子建立round1帖子事件提取表，不读取目标或 AI 配置，不重试。"""

import logging
from dataclasses import dataclass

import pandas as pd

from common.validation import (
    TASK_COLUMNS,
    validate_post_index as _validate_index,
    validate_post_strings as _validate_strings,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InitializationSummary:
    """一次性完整初始化的确认数量，不表示部分写入成功。"""

    total: int
    written: int


def initialize_tasks(source_df, write_tasks) -> InitializationSummary:
    """根据全部源帖子构建完整 round1帖子事件提取表，并通过回调一次写入。

    source_df 必须保留原 date/id 双层索引及 text/user_name 字符串。
    write_tasks 接收完整初始化表且只调用一次，调用方提供新的目标。
    不读取目标、不加载 AI 配置，也不重试。
    正常返回 InitializationSummary；输入不合法或写入异常时继续抛错，
    不能据此认定目标未写入任何数据。
    """
    logger.info("开始一次性初始化round1帖子事件提取表")
    _validate_index(source_df)
    _validate_strings(source_df, ["text", "user_name"])
    # 配置摘要和历史结果初始为空；只保留原索引及输入快照。
    tasks = pd.DataFrame("", index=source_df.index, columns=TASK_COLUMNS, dtype=object)
    tasks["text"] = source_df["text"].to_numpy(copy=False)
    tasks["user_name"] = source_df["user_name"].to_numpy(copy=False)
    tasks["status"] = "0"
    logger.info("提交完整round1帖子事件提取表：任务数=%d", len(tasks))
    try:
        write_tasks(tasks)
    except BaseException as exc:
        logger.error("round1帖子事件提取表初始化未确认：异常类型=%s；目标可能已部分写入，须清空后重新初始化",
                     type(exc).__name__)
        raise
    logger.info("round1帖子事件提取表初始化完成：已确认=%d", len(tasks))
    return InitializationSummary(len(tasks), len(tasks))
