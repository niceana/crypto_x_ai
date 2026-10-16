"""共用存储重试和按列排序；业务转换与工作快照同步由 Store 负责。"""

import logging
import time

logger = logging.getLogger(__name__)


class TaskValidationError(ValueError):
    """任务表结构、输入快照或处理版本不符合约定。"""


class BatchPersistenceError(RuntimeError):
    """保存预算耗尽或提交后同步失败；阶段和原始原因供调用方停止与核查。"""

    def __init__(self, stage, message="任务保存失败"):
        super().__init__("{}：{}".format(stage, message))
        self.stage = stage


def write_with_retry(write_operation, batch, run, stage, log=None, failure_message="任务保存失败"):
    """按预算重试同一批存储写入；注册和内存同步由调用方执行。"""
    active_logger = log or logger
    for attempt in range(run.storage_max_attempts):
        active_logger.debug("提交存储更新：阶段=%s，任务数=%d，列=%s，尝试=%d/%d",
                            stage, len(batch), list(batch.columns), attempt + 1, run.storage_max_attempts)
        try:
            # 每次传副本，客户端即使改动入参，也不会污染下次重试的数据。
            write_operation(batch.copy(deep=True))
        except Exception as exc:
            if attempt + 1 >= run.storage_max_attempts:
                active_logger.error("更新尝试预算耗尽：阶段=%s，任务数=%d", stage, len(batch))
                raise BatchPersistenceError(stage, failure_message) from exc
            active_logger.warning("存储调用异常，本批未写入，准备整批重试：阶段=%s，任务数=%d，尝试=%d/%d，等待=%.3f秒，异常类型=%s",
                                  stage, len(batch), attempt + 1, run.storage_max_attempts,
                                  run.storage_retry_seconds, type(exc).__name__)
            time.sleep(run.storage_retry_seconds)
        else:
            return


def write_columns(self, patch, stage, log=None, prepare_write=None):
    """按原索引提交指定列；可在每次写入前补时间，返回实际提交值供同步内存。"""
    if not patch.index.is_unique:
        raise TaskValidationError("同一更新批次的 (date, id) 不允许重复")
    patch = patch.sort_index()
    if not patch.index.is_monotonic_increasing:
        raise TaskValidationError("更新行必须按 (date, id) 严格递增")
    accepted = patch

    def submit(batch):
        nonlocal accepted
        if prepare_write is not None:
            prepare_write(batch)
            # 提交前留副本，防止客户端修改入参；正常返回后才能同步到工作快照。
            accepted = batch.copy(deep=True)
        self.cli.update_by_column(self.task_path, batch)

    write_with_retry(submit, patch, self.config.run, stage, log)
    return accepted

