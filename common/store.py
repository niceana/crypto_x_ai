"""三轮工作表：共用状态与保存核心，分别保留帖子任务和事件结果的业务行为。"""

import json
import logging
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, List, Sequence, Tuple

import pandas as pd

from common.config import AIConfig, RunConfig, ai_config_hash
from common.execution import check_month_complete
from common.persistence import BatchPersistenceError, TaskValidationError, write_columns
from common.ai_protocol import ParsedResponse
from common.validation import (
    POST_RESULT_COLUMNS,
    RELATION_COLUMNS,
    validate_post_task_structure as _validate_tasks,
    validate_task_range as _validate_range,
    validate_event_table_structure,
)
from common.time_rules import in_scope, published_time

logger = logging.getLogger(__name__)

# 日期沿用原索引；复合键只用于内部关联，不进入 AI 请求。
TaskKey = Tuple[Any, str]


def read_stage_hashes(cli, task_path, hash_column):
    """按列读取全表摘要，返回可直接遍历原值的 Series。

    用户确认空字符串日期表示全表；只传所需列，不清洗值。读取失败或缺列
    直接向调用方报错，不回退整表读取或返回空集合绕过配置核对。
    """
    frame = cli.read(task_path, start_time="", end_time="", columns=[hash_column])
    return frame[hash_column]


def validate_stage_config_hash(config, cli, task_path, hash_column, stage, error_type=ValueError):
    """核对本阶段全表摘要；初始空字符串不参与，不检查或补写历史单元格。"""
    digest = ai_config_hash(config)
    if set(read_stage_hashes(cli, task_path, hash_column)) - {"", digest}:
        raise error_type("{} 全表已有配置摘要与本轮提示词或 AI 参数冲突".format(stage))
    return digest


class TableStore:
    """调用线程持有当前工作表；业务类提供结构校验和固定阶段字段。

    stages 的每项是 (状态列, 摘要列, 允许更新列)。只检查本程序准备提交的
    字段，数据库普通列原样使用。写入返回后再更新快照，统计由结束状态计算。
    """

    def __init__(self, config, cli, task_path, stages, validate_structure,
                 start_time=None, end_time=None, error_type=ValueError, log=None):
        self.config = config
        self.cli = cli
        self.task_path = task_path
        self.stages = stages
        self.error_type = error_type
        self.logger = log or logging.getLogger(__name__)
        self.start_time = start_time
        self.end_time = end_time
        self.df = self._read_segment()
        validate_structure(self.df)
        self.positions = {key: i for i, key in enumerate(self.df.index)}

    def _read_segment(self):
        if self.start_time is None and self.end_time is None:
            return self.cli.read(self.task_path)
        return self.cli.read(self.task_path, start_time=self.start_time, end_time=self.end_time)

    def _index(self, keys):
        """取原索引位置，保留日期类型、时区和精度。"""
        return self.df.index.take([self.positions[key] for key in keys])

    def _apply_saved_patch(self, patch, stage):
        self.df.loc[patch.index, patch.columns] = patch.to_numpy()

    def _statuses(self, stage, keys=None):
        """只取状态列，恢复和统计不复制整张工作表。"""
        statuses = self.df[self.stages[stage][0]]
        return statuses if keys is None else statuses.loc[self._index(keys)]

    def update_stage_columns(self, keys, values, stage, operation=None, prepare_write=None):
        """保存本阶段指定列；内存同步失败不重新进入数据库写入重试。"""
        if not keys:
            return
        patch = pd.DataFrame(values, index=self._index(keys), dtype=object)
        if not set(patch.columns).issubset(self.stages[stage][2]):
            raise self.error_type("当前阶段不能修改这些字段")
        for name in patch.columns:
            for value in patch[name]:
                if not isinstance(value, str):
                    raise self.error_type("最终更新字段必须为字符串：字段={}，实际类型={}".format(
                        name, type(value).__name__))
                value.encode("utf-8")
        write_stage = operation or stage
        accepted = write_columns(self, patch, write_stage, log=self.logger, prepare_write=prepare_write)
        try:
            self._apply_saved_patch(accepted, write_stage)
        except Exception as exc:
            raise BatchPersistenceError(write_stage, "CLI 已写入，但内存同步失败") from exc

    def reset_status(self, stage, keys=None, operation=None):
        """仅分批恢复遗留状态；失败前成功的批次保留，旧结果和错误不改。"""
        status = self.stages[stage][0]
        statuses = self._statuses(stage, keys)
        pending = list(statuses.index[statuses.isin(["9", "-1"])])
        size = self.config.run.batch_size
        for offset in range(0, len(pending), size):
            self.update_stage_columns(pending[offset:offset + size], {status: "0"}, stage, operation)
        return len(pending)

    def select_pending(self, candidates, count, stage):
        """选择端检查待处理状态；认领不再重复检查。"""
        status = self.stages[stage][0]
        keys = []
        while candidates and len(keys) < count:
            key = candidates.popleft()
            if self.df.at[key, status] == "0":
                keys.append(key)
        return keys

    def claim_stage(self, keys, stage, digest, operation=None):
        status, hash_column, _ = self.stages[stage]
        self.update_stage_columns(keys, {status: "9", hash_column: digest}, stage, operation)

    def check_config(self, stage):
        return validate_stage_config_hash(self.config, self.cli, self.task_path,
                                          self.stages[stage][1], stage, self.error_type)

    def count_completed(self, stage, keys=None):
        return int(self._statuses(stage, keys).eq("1").sum())

    def pending_keys(self, stage, keys=None):
        statuses = self._statuses(stage, keys)
        return list(statuses.index[statuses.isin(["0"])])

    def summarize(self, stage, keys=None, already=0):
        """结束时一次分类；未知状态保留在总数中，不新增质量检查。"""
        statuses = self._statuses(stage, keys)
        counts = statuses.value_counts(dropna=False)
        return {"total": len(statuses), "already_completed": already,
                "new_completed": int(counts.get("1", 0)) - already,
                "failed": int(counts.get("-1", 0)), "pending": int(counts.get("0", 0)),
                "processing": int(counts.get("9", 0))}


class PostTaskStore(TableStore):
    """只持有当前分段数据；状态与结果均在保存确认后更新内存。"""

    def __init__(self, config: AIConfig, cli, task_path, start_time=None, end_time=None):
        stages = {"round1": ("status", "config_hash", set(POST_RESULT_COLUMNS + ["config_hash"]))}
        super().__init__(config, cli, task_path, stages, _validate_tasks,
                         start_time, end_time, TaskValidationError, logger)
        _validate_range(self.df, start_time, end_time)
        self.digest = None
        self.already_completed = self.count_completed("round1")
        self.candidates = deque()
        logger.info("round1帖子事件提取表当前分段读取完成：起点=%s，终点=%s，任务数=%d，已有成功=%d",
                    start_time, end_time, len(self.df), self.already_completed)

    def recover(self):
        """本段初次进入时仅恢复遗留 9/-1，不覆盖摘要和旧结果。"""
        restored = self.reset_status("round1", operation="reset")
        self.candidates = deque(self.pending_keys("round1"))
        logger.info("round1帖子事件提取表当前分段状态恢复完成：恢复数=%d，待处理数=%d", restored, len(self.candidates))

    def select(self, count: int) -> List[TaskKey]:
        """沿原读取顺序选取待处理键，名额仅由网络作业占用决定。"""
        return self.select_pending(self.candidates, count, "round1")

    def claim(self, keys: Sequence[TaskKey]):
        """先共同确认状态 9 与摘要，再提取单帖 text 交给网络线程。"""
        if not keys:
            return []
        if self.digest is None:
            self.digest = self.check_config("round1")
        self.claim_stage(keys, "round1", self.digest, "claim")
        logger.debug("round1帖子事件提取表任务认领已确认：任务数=%d", len(keys))
        return [(key, self.df.at[key, "text"]) for key in keys]

    def save_results(self, results: Sequence[Tuple[TaskKey, ParsedResponse]]):
        """将状态与五个最终结果列一次保存，明确清空缺失结果和旧错误。"""
        if not results:
            return
        rows = []
        for key, result in results:
            # 只保存最终一次响应的用量和生成时间，不累计中间重试。
            usage = "" if result.usage is None else json.dumps(result.usage, ensure_ascii=False, allow_nan=False)
            # 生成时间已由响应解析转为北京时间文本，缺失或转换失败时为空。
            row = ["1" if result.success else "-1", result.ai_response_content,
                   result.reasoning_content, usage, result.response_created, "" if result.success else result.error_message]
            rows.append(dict(zip(POST_RESULT_COLUMNS, row)))
        self.update_stage_columns([key for key, _ in results], rows, "round1", "final")


@dataclass
class StageSummary:
    stage: str
    total: int = 0
    already_completed: int = 0
    new_completed: int = 0
    failed: int = 0
    pending: int = 0
    processing: int = 0
    http_attempts_started: int = 0
    blocked_assets: tuple = ()

    def add_month(self, month):
        """round3事件影响分析用已有 processing 计数检查本月结束，再累加标量，不额外读表。"""
        check_month_complete(month.processing)
        self.total += month.total
        self.already_completed += month.already_completed
        self.new_completed += month.new_completed
        self.failed += month.failed
        self.pending += month.pending
        self.processing += month.processing
        return month.new_completed, month.failed


def summarize(store, keys, stage, already, attempts=0, blocked=()):
    return StageSummary(stage, **store.summarize(stage, keys, already),
                        http_attempts_started=attempts, blocked_assets=tuple(sorted(blocked)))


def is_stored_relation_valid(key, row, frame, roots=None):
    """检查使用事件组所需的引用、阶段、资产及时间关系，不质检历史内容。"""
    relation, event_id = row["event_relation"], row["event_id"]
    if relation == "new":
        return event_id == key[1]
    if relation not in ("update", "repeat"):
        return False
    if roots is None:
        root = frame.loc[frame.index.get_level_values("record_id") == event_id]
        if len(root) != 1:
            return False
        root_key, origin = next(root.iterrows())
    else:
        if event_id not in roots:
            return False
        root_key, origin = roots[event_id]
    return (origin["round2_status"] == "1" and origin["event_relation"] == "new"
            and origin["event_id"] == root_key[1] and origin["asset"] == row["asset"]
            and published_time(root_key[0]) < published_time(key[0]))


def _event_response_json(response, attempts):
    """只打包最后一次可提取的响应；created 在真正提交前填写，不保存推理文本。"""
    if response is None:
        return ""
    return json.dumps({"ai_response_content": response.ai_response_content,
                       "usage": response.usage, "response_created": response.response_created,
                       "attempts": attempts, "created": ""}, ensure_ascii=False, allow_nan=False)


def _stamp_event_responses(batch):
    """每次存储尝试前取北京时间，同批共用；不改变回答、用量及请求次数。"""
    created = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    for column in ("round2_ai_response", "round3_ai_response"):
        if column not in batch.columns:
            continue
        for key, value in batch[column].items():
            if value:
                body = json.loads(value)
                body["created"] = created
                batch.at[key, column] = json.dumps(body, ensure_ascii=False, allow_nan=False)


class EventRecordStore(TableStore):
    """事件业务封装；数据库普通列原样使用，阶段写入和恢复由核心负责。"""

    def __init__(self, cli, settings, run=None, start_time=None, end_time=None):
        self.settings = settings
        stages = {
            "round2": ("round2_status", "round2_config_hash", set(RELATION_COLUMNS + [
                "round2_ai_response", "round2_status", "round2_config_hash", "round3_status", "error_message", "embedding"])),
            "round3": ("round3_status", "round3_config_hash", {
                "round3_ai_response", "round3_status", "round3_config_hash", "error_message"}),
        }
        super().__init__(SimpleNamespace(run=run or RunConfig()), cli, settings.result_path,
                         stages, validate_event_table_structure, start_time, end_time, log=logger)
        if start_time is not None:
            start, end = date.fromisoformat(start_time), date.fromisoformat(end_time)
            if any(not start <= published_time(key[0]).date() <= end for key in self.df.index):
                raise ValueError("结果表读取返回了分段日期以外的行")

    def keys_in_scope(self):
        return sorted((key for key in self.df.index if in_scope(key[0], self.settings)),
                      key=lambda key: (published_time(key[0]), key[1]))

    def require_relation(self, keys, roots=None):
        for key in keys:
            row = self.df.loc[key]
            if row["round2_status"] != "1" or not is_stored_relation_valid(key, row, self.df, roots):
                raise ValueError("第二轮未完成或结果不合法：{}".format(key[1]))

    def recover(self, stage, keys):
        """只分批恢复当前阶段 9/-1，不质检或修复历史结果。"""
        self.reset_status(stage, keys)

    def claim(self, keys, stage, digest):
        self.claim_stage(keys, stage, digest)

    def save_relation(self, key, result, response=None, attempts=0):
        """共同保存关联终态和最后一次回答；自动判新不伪造 AI 调用信息。"""
        if response is not None and not response.success:
            values = dict.fromkeys(RELATION_COLUMNS, "")
            values.update(round2_status="-1", error_message=response.error_message[:512],
                          round2_ai_response=_event_response_json(response, attempts))
            self.update_stage_columns([key], values, "round2", prepare_write=_stamp_event_responses)
            return
        relation = result["event_relation"]
        if relation == "uncertain":
            logger.info("round2事件去重归组正常 uncertain 按 new 建档：record_id=%s", key[1])
            relation = "new"
        values = {"event_relation": relation,
                  "event_id": key[1] if relation == "new" else result["matched_event_id"],
                  "new_information": result["new_information"] or "",
                  "round2_ai_response": _event_response_json(response, attempts),
                  "round2_status": "1", "error_message": "",
                  "round3_status": "1" if relation == "repeat" else "0"}
        self.update_stage_columns([key], values, "round2", prepare_write=_stamp_event_responses)

    def save_embeddings(self, values):
        """按原索引只保存向量 JSON；存储重试不重新调用 Embedding 接口。"""
        keys = list(values)
        size = self.config.run.batch_size
        for offset in range(0, len(keys), size):
            batch = keys[offset:offset + size]
            self.update_stage_columns(batch, [{"embedding": values[key]} for key in batch],
                                      "round2", operation="embedding")

    def save_impact_batch(self, items):
        """正文已在网络入口校验；按键整批保存最后一次原始回答、调用信息和终态。"""
        rows = []
        for key, response, attempts in items:
            row = {"round3_ai_response": _event_response_json(response, attempts),
                   "round3_status": "1" if response.success else "-1",
                   "error_message": "" if response.success else response.error_message[:512]}
            rows.append(row)
        self.update_stage_columns([key for key, _, _ in items], rows, "round3", prepare_write=_stamp_event_responses)


def validate_relation_history(store, keys):
    """只检查已导入任务的同币前史与补录冲突，不推断未导入的round1事件提取帖子。"""
    earliest = {}
    for key in keys:
        if store.df.loc[key, "round2_status"] != "1":
            asset = store.df.loc[key, "asset"]
            stamp = published_time(key[0])
            if asset not in earliest or stamp < published_time(earliest[asset][0]):
                earliest[asset] = key
    for asset, pending_key in earliest.items():
        stamp = published_time(pending_key[0])
        # 已导入的更早同币任务不可越过，即使它在检索窗口以外。
        for old_key, row in store.df.iterrows():
            if row["asset"] == asset and published_time(old_key[0]) > stamp and row["round2_status"] == "1":
                raise ValueError("疑似历史补录或历史状态缺口，请核查：asset={}，待处理={} / {}，更晚已完成={} / {}".format(
                    asset, pending_key[0], pending_key[1], old_key[0], old_key[1]))
            if row["asset"] == asset and published_time(old_key[0]) < stamp:
                if row["round2_status"] != "1" or not is_stored_relation_valid(old_key, row, store.df):
                    raise ValueError("所需前史未完成，请手动补跑：{} / {}".format(asset, old_key[1]))
