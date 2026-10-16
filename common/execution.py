"""round1事件提取与round3事件影响分析共用的自然月推进和独立任务执行，不依赖业务表结构。"""

import logging
import queue
import time
from calendar import monthrange
from datetime import date, timedelta

from common.network import BatchRunError
from common.persistence import BatchPersistenceError

logger = logging.getLogger(__name__)


def month_ranges(start, end):
    """生成包含两端日期的自然月分段；无日期时保留全表处理。"""
    if start is None:
        yield None, None
        return
    cursor = start
    while cursor <= end:
        month_end = date(cursor.year, cursor.month, monthrange(cursor.year, cursor.month)[1])
        segment_end = min(month_end, end)
        yield cursor.isoformat(), segment_end.isoformat()
        if segment_end == end:
            return
        cursor = segment_end + timedelta(days=1)


def check_month_complete(processing):
    """用已有状态 9 数量检查月末，不读取表或修改统计。"""
    if processing:
        raise BatchRunError("月份切换前存在未确认任务")


class SegmentCoordinator:
    """调用线程执行当前分段，限制网络作业和未保存结果的条数。

    candidates 只保存当前月候选键。claim_jobs(limit) 确认认领后返回本批键和
    逐条生成的 RequestJob；save_results 整批确认后返回。业务表、请求校验、
    结果转换、存储结束检查和统计由调用方提供，协调器只记录网络占用、
    结果缓冲与整批保存进度。
    """

    def __init__(self, run, network, candidates, claim_jobs, save_results,
                 confirmed_before=0, result_item=None, log=None):
        self.run_config = run
        self.network = network
        self.candidates = candidates
        self.claim_jobs = claim_jobs
        self.save_results = save_results
        self.result_item = result_item or (lambda message: (message.key, message.response))
        self.logger = log or logger
        # 收取完成消息即释放网络名额；保存完成由缓冲和存储状态判断。
        self.submitted = set()
        self.buffer = []
        self.first_buffer_at = None
        self.confirmed = confirmed_before
        step = run.progress_every
        self.next_progress = (confirmed_before // step + 1) * step

    def flush(self):
        """整批保存返回后才清空；保存失败时保留全部结果。"""
        if not self.buffer:
            return
        self.logger.debug("开始提交最终结果缓冲：结果数=%d", len(self.buffer))
        self.save_results(self.buffer)
        self.confirmed += len(self.buffer)
        self.buffer.clear()
        self.first_buffer_at = None
        if self.confirmed >= self.next_progress:
            self.logger.info("已确认最终结果 %d 条", self.confirmed)
            step = self.run_config.progress_every
            self.next_progress = (self.confirmed // step + 1) * step

    def should_flush(self):
        run = self.run_config
        return bool(self.buffer) and (
            len(self.buffer) >= run.batch_size
            or time.monotonic() - self.first_buffer_at >= run.flush_interval_seconds
            or (not self.submitted and not self.candidates)
        )

    def consume(self, message):
        """按原键收取并按条件保存；没有最终响应时不产生任务终态。"""
        if message.key not in self.submitted:
            raise BatchRunError("收到未提交或重复的作业完成消息")
        self.submitted.remove(message.key)
        if message.response is not None:
            if len(self.buffer) >= self.run_config.result_buffer_size:
                raise BatchRunError("结果缓冲容量不变量被破坏")
            item = self.result_item(message)
            if not self.buffer:
                self.first_buffer_at = time.monotonic()
            self.buffer.append(item)
        if self.should_flush():
            self.flush()

    def run(self):
        """先保存、逐条收取并检查容量，最后按空闲名额补充请求。"""
        run = self.run_config
        while True:
            self.network.check()
            if self.should_flush():
                self.flush()
            while True:
                try:
                    message = self.network.completions.get_nowait()
                except queue.Empty:
                    break
                self.consume(message)
                self.network.check()
            self.network.check()
            if not self.submitted and not self.candidates:
                self.flush()
                return
            keys, jobs = self.claim_jobs(run.concurrency - len(self.submitted))
            if keys:
                for job in jobs:
                    # 同步认领可能耗时；写入确认后仍要再次检查整体停止。
                    self.network.check()
                    self.submitted.add(job.key)
                    self.network.submit(job)
                continue
            timeout = 0.25
            if self.buffer:
                timeout = min(timeout, max(0.0, run.flush_interval_seconds
                              - (time.monotonic() - self.first_buffer_at)))
            try:
                message = self.network.completions.get(timeout=timeout)
            except queue.Empty:
                continue
            self.consume(message)

    def finish(self):
        """网络退出后保存已完成响应；取消及程序异常消息只释放网络名额。"""
        # 中断可能发生在保存过程中，留下满批缓冲；先保存，不能先取走下一条消息。
        if len(self.buffer) >= self.run_config.batch_size:
            self.flush()
        while not self.network.completions.empty():
            self.consume(self.network.completions.get_nowait())
        while self.network.unpublished:
            self.consume(self.network.unpublished.pop(0))
        self.flush()


def _retain_unsaved(error, coordinator, network):
    """保留进程内结果及完成队列，不把异常收尾当成保存成功。"""
    if coordinator is not None:
        error.unsaved_results = coordinator.buffer
    if network is not None:
        error.completed_results = network.completions
        error.unpublished_results = network.unpublished


def run_segments(ranges, load_segment, summary, network_factory,
                 result_item=None, log=None):
    """逐段加载和执行，复用一个网络线程，只跨段保留标量汇总。

    load_segment(start, end) 返回 store、候选队列、claim_jobs、save_results、
    summarize_month；最后一项先检查存储是否结束，再累加统计并返回本月
    新增成功和失败数。两轮都根据当前月已确认的状态 9 检查未结束任务。
    store 提供运行配置。检查整体停止后才加载下一段。
    各轮的全范围前置检查应在进入本函数之前完成。
    """
    active_logger = log or logger
    network = store = coordinator = None
    confirmed = 0
    try:
        try:
            for segment_start, segment_end in ranges:
                if network is not None:
                    network.check()
                active_logger.info("开始处理当前分段：起点=%s，终点=%s", segment_start, segment_end)
                store, candidates, claim_jobs, save_results, summarize_month = load_segment(segment_start, segment_end)
                if candidates:
                    if network is None:
                        network = network_factory()
                        network.start()
                    coordinator = SegmentCoordinator(store.config.run, network, candidates,
                                                     claim_jobs, save_results, confirmed, result_item, active_logger)
                    coordinator.run()
                else:
                    active_logger.info("当前分段无待处理任务：%s → %s", segment_start, segment_end)
                succeeded, failed = summarize_month()
                confirmed += succeeded + failed
                if succeeded or failed:
                    active_logger.info("当前分段 %s → %s：成功 %d，失败 %d", segment_start,
                                       segment_end, succeeded, failed)
                # 不在生成器或闭包中保留上一段的原帖、表和结果。
                coordinator = store = candidates = claim_jobs = save_results = summarize_month = None
        finally:
            if network is not None:
                network.shutdown()
        if network is not None:
            if network.failure is not None:
                raise BatchRunError("网络资源关闭失败") from network.failure
            summary.http_attempts_started = network.http_attempts_started
    except BaseException as error:
        if isinstance(error, KeyboardInterrupt):
            active_logger.warning("收到用户中断，开始停止请求并保存已完成结果")
        else:
            active_logger.exception("worker 异常停止，开始收尾")
        try:
            if coordinator is not None and not isinstance(error, BatchPersistenceError):
                coordinator.finish()
        except BaseException as persistence_error:
            _retain_unsaved(persistence_error, coordinator, network)
            active_logger.exception("异常收尾时结果保存失败")
            active_logger.info("worker 收尾结束")
            raise persistence_error from error
        _retain_unsaved(error, coordinator, network)
        active_logger.info("worker 收尾结束")
        raise
    return summary
