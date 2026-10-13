"""维护当前月份的任务快照，并确认按原索引提交的指定列更新。"""

import json
import logging
import re
import time
from collections import deque
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, List, Optional, Sequence, Tuple

import pandas as pd

from .config import Config, config_hash
from .response import ParsedResponse

logger = logging.getLogger(__name__)

# 日期沿用读取值；复合键只用于内部关联，不进入 AI 请求。
TaskKey = Tuple[Any, str]
RESULT_COLUMNS = ["status", "ai_response_content", "reasoning_content", "usage_json",
                  "response_created", "error_message"]
TASK_COLUMNS = ["config_hash", "text", "user_name"] + RESULT_COLUMNS


class TaskValidationError(ValueError):
    """任务表结构、输入快照或处理版本不符合约定。"""


class BatchPersistenceError(RuntimeError):
    """保存无法确认；保留阶段及未确认更新供调用方核对。"""

    def __init__(self, stage, pending_patch, message="任务保存未确认"):
        super().__init__("{}：{}".format(stage, message))
        self.stage = stage
        self.pending_patch = pending_patch.copy(deep=True)


@dataclass(frozen=True)
class InitializationSummary:
    """一次性完整初始化的确认数量，不表示部分写入成功。"""

    total: int
    written: int


def _reject_constant(value):
    raise ValueError("不允许非标准 JSON 数值")


def _validate_index(df):
    """校验原复合索引及 ID 唯一性，不转换日期类型或精度。"""
    if not isinstance(df, pd.DataFrame):
        raise TaskValidationError("输入必须为 DataFrame")
    if not isinstance(df.index, pd.MultiIndex) or list(df.index.names) != ["date", "id"]:
        raise TaskValidationError("索引必须为名称和顺序固定的 date/id MultiIndex")
    if not df.columns.is_unique or not df.index.is_unique:
        raise TaskValidationError("索引和列名必须唯一")
    dates = df.index.get_level_values("date")
    ids = df.index.get_level_values("id")
    if dates.isna().any() or any(not isinstance(value, str) or not value.strip() for value in ids):
        raise TaskValidationError("date 不能缺失，id 必须为非空白字符串")
    if not ids.is_unique:
        raise TaskValidationError("同一 id 不允许对应多行或不同 date")
    for name, values in (("date", dates), ("id", ids)):
        if name in df.columns and not all(a == b for a, b in zip(df[name], values)):
            raise TaskValidationError("索引字段与同名列不一致")


def _validate_strings(df, columns):
    if any(name not in df.columns for name in columns):
        raise TaskValidationError("任务缺少必要列")
    for name in columns:
        if any(not isinstance(value, str) for value in df[name]):
            raise TaskValidationError("{} 的所有单元格必须为字符串".format(name))


def _validate_range(df, start_time, end_time):
    """校验包含起止日全天的范围，保留原日期类型和时区。"""
    if start_time is None:
        return
    dates = df.index.get_level_values("date")
    try:
        # 下一日日界线只用于校验，cli 的截止日期仍原样传递。
        end_next_day = (date.fromisoformat(end_time) + timedelta(days=1)).isoformat()
        if not ((dates >= start_time) & (dates < end_next_day)).all():
            raise TaskValidationError("cli.read 返回了当前日期范围外的任务")
    except TypeError as exc:
        raise TaskValidationError("读取结果与日期范围的类型或时区不兼容") from exc


def _validate_tasks(df):
    """在恢复或认领前拒绝不完整的任务表与不一致的成功记录。"""
    _validate_index(df)
    if set(df.columns) != set(TASK_COLUMNS):
        raise TaskValidationError("任务表必须恰好包含约定的九个普通列")
    _validate_strings(df, TASK_COLUMNS)
    if not df["status"].isin(["0", "9", "1", "-1"]).all():
        raise TaskValidationError("任务状态只能为 0/9/1/-1")
    if any(value and re.fullmatch(r"[0-9a-f]{64}", value) is None for value in df["config_hash"]):
        raise TaskValidationError("config_hash 必须为空或 64 位小写十六进制摘要")
    for value in df["usage_json"]:
        if value:
            try:
                parsed = json.loads(value, parse_constant=_reject_constant)
                json.dumps(parsed, allow_nan=False)
            except (ValueError, TypeError) as exc:
                raise TaskValidationError("usage_json 必须为标准 JSON") from exc
    succeeded = df["status"].eq("1")
    if ((succeeded & df["config_hash"].eq("")).any()
            or (succeeded & df["ai_response_content"].str.strip().eq("")).any()
            or (succeeded & df["error_message"].ne("")).any()):
        raise TaskValidationError("成功任务的摘要、正文或错误列不一致")


def initialize_tasks(source_df, write_tasks) -> InitializationSummary:
    """根据全部源帖子构建完整初始任务表，并通过回调一次写入。

    source_df 必须保留原 date/id 双层索引及 text/user_name 字符串。
    write_tasks 接收完整初始化表且只调用一次，调用方提供新的目标。
    不读取目标、不加载 AI 配置，也不重试。
    正常返回 InitializationSummary；输入不合法或写入异常时继续抛错，
    不能据此认定目标未写入任何数据。
    """
    logger.info("开始一次性初始化任务表")
    _validate_index(source_df)
    _validate_strings(source_df, ["text", "user_name"])
    # 配置摘要和历史结果初始为空；只保留原索引及输入快照。
    tasks = pd.DataFrame("", index=source_df.index, columns=TASK_COLUMNS, dtype=object)
    tasks["text"] = source_df["text"].to_numpy(copy=False)
    tasks["user_name"] = source_df["user_name"].to_numpy(copy=False)
    tasks["status"] = "0"
    logger.info("提交完整任务表：任务数=%d", len(tasks))
    try:
        write_tasks(tasks)
    except BaseException as exc:
        logger.error("任务初始化未确认：异常类型=%s；目标可能已部分写入，须清空后重新初始化",
                     type(exc).__name__)
        raise
    logger.info("任务初始化完成：已确认=%d", len(tasks))
    return InitializationSummary(len(tasks), len(tasks))


class TaskStore:
    """只持有当前分段数据；状态与结果均在保存确认后更新内存。"""

    def __init__(self, config: Config, cli, task_path, start_time=None, end_time=None):
        self.config = config
        self.cli = cli
        self.task_path = task_path
        self.start_time = start_time
        self.end_time = end_time
        self.df = self._read_segment()
        _validate_tasks(self.df)
        _validate_range(self.df, start_time, end_time)
        self.positions = {key: position for position, key in enumerate(self.df.index)}
        self.hashes = set(self.df["config_hash"]) - {""}
        self.already_succeeded = int(self.df["status"].eq("1").sum())
        self.new_succeeded = 0
        self.new_failed = 0
        self.active_keys = set()
        self.candidates = deque()
        self.pending_patch = None  # type: Optional[pd.DataFrame]
        logger.info("当前分段读取完成：起点=%s，终点=%s，任务数=%d，已有成功=%d",
                    start_time, end_time, len(self.df), self.already_succeeded)

    def _read_segment(self):
        """直接以当前分段的年月日字符串读取任务。"""
        if self.start_time is None and self.end_time is None:
            return self.cli.read(self.task_path)
        return self.cli.read(
            self.task_path,
            start_time=self.start_time,
            end_time=self.end_time,
        )

    def _index(self, keys):
        """从原索引按位置取批次，避免重建导致日期时区或精度变化。"""
        return self.df.index.take([self.positions[key] for key in keys])

    def _confirm(self, patch, stage):
        """同步已确认列；每个终态仅释放一次占用并计入统计。"""
        self.df.loc[patch.index, patch.columns] = patch.to_numpy()
        if stage == "final":
            for key, status in zip(patch.index, patch["status"]):
                if key in self.active_keys:
                    self.active_keys.remove(key)
                    self.new_succeeded += status == "1"
                    self.new_failed += status == "-1"

    def _write(self, patch, stage):
        """保存指定列；异常后核对本段，只重写尚未确认的键。"""
        stage_name = {"reset": "恢复状态", "claim": "认领任务", "final": "保存终态"}[stage]
        if self.pending_patch is not None:
            raise BatchPersistenceError(stage, self.pending_patch, "存在尚未确认的更新")
        # 保留副本以应对部分落库或中断，客户端不能改动待确认快照。
        self.pending_patch = patch.copy(deep=True)
        for attempt in range(self.config.run.storage_max_attempts):
            logger.debug("提交指定列更新：阶段=%s，任务数=%d，列=%s，尝试=%d/%d",
                         stage_name, len(self.pending_patch), list(self.pending_patch.columns),
                         attempt + 1, self.config.run.storage_max_attempts)
            try:
                self.cli.update_by_index(self.task_path, self.pending_patch.copy(deep=True))
            except Exception as exc:
                logger.warning("cli.update_by_index 异常，开始同范围读回核对：阶段=%s，尝试=%d/%d，异常类型=%s",
                               stage_name, attempt + 1, self.config.run.storage_max_attempts,
                               type(exc).__name__)
                try:
                    observed = self._read_segment()
                    _validate_index(observed)
                    if observed.index.levels[0].dtype != self.df.index.levels[0].dtype:
                        raise TaskValidationError("读回时日期索引类型、时区或精度发生变化")
                    _validate_range(observed, self.start_time, self.end_time)
                    required = ["text", "user_name", "config_hash"] + list(self.pending_patch.columns)
                    _validate_strings(observed, set(required))
                    confirmed = []
                    for key in self.pending_patch.index:
                        if key not in observed.index:
                            raise TaskValidationError("读回缺少原任务键")
                        # 核对输入不变；认领阶段的摘要属于预期提交列。
                        for name in ("text", "user_name", "config_hash"):
                            if name == "config_hash" and name in self.pending_patch.columns:
                                continue
                            if observed.at[key, name] != self.df.at[key, name]:
                                raise TaskValidationError("读回时帖子输入或未提交摘要发生变化")
                        if all(observed.at[key, name] == self.pending_patch.at[key, name]
                               for name in self.pending_patch.columns):
                            confirmed.append(key)
                    if confirmed:
                        accepted = self.pending_patch.loc[confirmed]
                        self._confirm(accepted, stage)
                        self.pending_patch = self.pending_patch.drop(index=confirmed)
                        logger.debug("部分更新已确认：阶段=%s，确认数=%d，剩余数=%d",
                                     stage_name, len(confirmed), len(self.pending_patch))
                    if self.pending_patch.empty:
                        self.pending_patch = None
                        return
                except Exception as read_exc:
                    logger.error("读回核对失败，停止保存：阶段=%s，未确认数=%d，异常类型=%s",
                                 stage_name, len(self.pending_patch), type(read_exc).__name__)
                    raise BatchPersistenceError(stage, self.pending_patch, "读回核对失败") from read_exc
                if attempt + 1 >= self.config.run.storage_max_attempts:
                    logger.error("更新尝试预算耗尽：阶段=%s，未确认数=%d", stage_name, len(self.pending_patch))
                    raise BatchPersistenceError(stage, self.pending_patch) from exc
                logger.warning("准备重写未确认更新：阶段=%s，任务数=%d，等待=%.3f秒",
                               stage_name, len(self.pending_patch), self.config.run.storage_retry_seconds)
                time.sleep(self.config.run.storage_retry_seconds)
            else:
                self._confirm(self.pending_patch, stage)
                logger.debug("指定列更新已确认：阶段=%s，任务数=%d", stage_name, len(self.pending_patch))
                self.pending_patch = None
                return

    def recover(self):
        """本段初次进入时仅恢复遗留 9/-1，不覆盖摘要和旧结果。"""
        keys = list(self.df.index[self.df["status"].isin(["9", "-1"])])
        size = self.config.run.batch_size
        for offset in range(0, len(keys), size):
            patch = pd.DataFrame({"status": "0"}, index=self._index(keys[offset:offset + size]))
            self._write(patch, "reset")
        self.candidates = deque(self.df.index[self.df["status"] == "0"])
        logger.info("当前分段状态恢复完成：恢复数=%d，待处理数=%d", len(keys), len(self.candidates))

    def select(self, count: int) -> List[TaskKey]:
        """沿原读取顺序选取待处理键，名额仅由网络作业占用决定。"""
        keys = []
        while self.candidates and len(keys) < count:
            key = self.candidates.popleft()
            if key not in self.active_keys and self.df.at[key, "status"] == "0":
                keys.append(key)
        return keys

    def claim(self, keys: Sequence[TaskKey]):
        """先共同确认状态 9 与摘要，再提取单帖 text 交给网络线程。"""
        if not keys:
            return []
        digest = config_hash(self.config)
        if self.hashes - {digest}:
            logger.error("当前分段存在配置摘要冲突：拒绝认领本批任务，任务数=%d", len(keys))
            raise TaskValidationError("本段已有配置摘要与本轮提示词或 AI 参数冲突")
        # active_keys 保留到终态确认；收到网络结果并不代表已保存。
        self.active_keys.update(keys)
        patch = pd.DataFrame({"status": "9", "config_hash": digest}, index=self._index(keys))
        self._write(patch, "claim")
        self.hashes.add(digest)
        logger.debug("任务认领已确认：任务数=%d", len(keys))
        return [(key, self.df.at[key, "text"]) for key in keys]

    def save_results(self, results: Sequence[Tuple[TaskKey, ParsedResponse]]):
        """将状态与五个最终结果列一次保存，明确清空缺失结果和旧错误。"""
        if not results:
            return
        rows = []
        for key, result in results:
            # 只保存最终一次响应的用量和生成时间，不累计中间重试。
            usage = "" if result.usage is None else json.dumps(result.usage, ensure_ascii=False, allow_nan=False)
            created = result.response_created
            created = "" if created is None else created if isinstance(created, str) else json.dumps(created, ensure_ascii=False, allow_nan=False)
            row = ["1" if result.success else "-1", result.ai_response_content,
                   result.reasoning_content, usage, created, "" if result.success else result.error_message]
            for value in row:
                if not isinstance(value, str):
                    raise TaskValidationError("最终更新字段必须为字符串")
                value.encode("utf-8")
            rows.append(row)
        patch = pd.DataFrame(rows, index=self._index([key for key, _ in results]), columns=RESULT_COLUMNS, dtype=object)
        self._write(patch, "final")
