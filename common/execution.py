"""三轮共用进度统计，第一轮与第三轮共用自然月推进和独立任务执行。"""

import logging
import queue
import time
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta

from common.network import BatchRunError
from common.persistence import BatchPersistenceError
from common.preparation import RequestPreparer

logger = logging.getLogger(__name__)


@dataclass
class BatchStats:
    calls: int = 0
    records: int = 0
    seconds: float = 0.0

    def record(self, records, started_at):
        self.calls += 1
        self.records += records
        self.seconds += time.monotonic() - started_at

    def fields(self, label, batch_label=None):
        return (
            "{}平均每批耗时={:.3f}秒，{}平均批量={:.1f}条，{}折合每条耗时={:.3f}秒".format(
                label, self.seconds / self.calls if self.calls else 0.0,
                batch_label or label, self.records / self.calls if self.calls else 0.0,
                label, self.seconds / self.records if self.records else 0.0)
        )


class ProgressStats:
    """三轮共用进度节流和瓶颈日志；只持有标量，HTTP 统计读取网络快照。"""

    def __init__(self, run, log, label):
        self.run = run
        self.logger = log
        self.label = label
        self.started_at = self.last_report_at = time.monotonic()
        self.next_progress = run.progress_every
        self.confirmed = 0
        self.claim = BatchStats()
        self.save = BatchStats()

    def should_report(self, processed, force=False):
        now = time.monotonic()
        if not force and processed < self.next_progress and now - self.last_report_at < 10:
            return False
        self.last_report_at = now
        self.next_progress = (processed // self.run.progress_every + 1) * self.run.progress_every
        return True

    @property
    def elapsed_seconds(self):
        return time.monotonic() - self.started_at

    def report_bottlenecks(self, http_peak, http_average, details=""):
        self.logger.info(
            "%s瓶颈统计：HTTP平均并发=%.2f/%d，HTTP峰值=%d，%s%s，%s",
            self.label, http_average, self.run.concurrency, http_peak,
            details + "，" if details else "", self.save.fields("结果保存"), self.claim.fields("认领写库", "认领"))


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
    """调用线程执行当前分段，提前认领输入并分批保存结果。

    candidates 只保存当前月候选键。claim_jobs(limit) 确认认领后返回本批键和
    逐条生成的 RequestJob；save_results 整批确认后返回。业务表、请求校验、
    结果转换、存储结束检查和统计由调用方提供。准备与网络线程独立补货，
    concurrency 只限制 HTTP；完成队列允许积压，提交缓冲仍按批次保存。
    """

    def __init__(self, run, network, candidates, claim_jobs, save_results,
                 confirmed_before=0, result_item=None, log=None, progress=None, preparer=None):
        self.run_config = run
        self.network = network
        self.candidates = candidates
        self.claim_jobs = claim_jobs
        self.save_results = save_results
        self.result_item = result_item or (lambda message: (message.key, message.response))
        self.logger = log or logger
        self.progress = progress or ProgressStats(run, self.logger, "任务")
        self.preparer = preparer
        self.total = len(candidates)
        self.succeeded = self.failed = 0
        # 只跟踪已认领未收取的任务，不用它限制 HTTP 或备用请求补货。
        self.submitted = set()
        self.buffer = []
        self.first_buffer_at = None
        self.confirmed = confirmed_before
        step = run.progress_every
        self.next_progress = (confirmed_before // step + 1) * step

    def report_progress(self, force=False, phase="处理中"):
        """本段只统计启动时待执行的 AI 任务；写库确认后才计入已处理。"""
        if not self.progress.should_report(self.progress.confirmed, force):
            return
        with self.network.completions.mutex:
            waiting = sum(message.response is not None for message in self.network.completions.queue)
        waiting += len(self.buffer)
        http_in_flight, http_peak, http_average = self.network.http_snapshot()
        preparation_state, pending_preparation = self.preparer.snapshot() if self.preparer else ("未启动", 0)
        ready = self.preparer.ready_requests.qsize() if self.preparer else 0
        processed = self.succeeded + self.failed
        elapsed = self.progress.elapsed_seconds
        self.logger.info(
            "%s当前分段AI任务处理进度：阶段=%s，已处理=%d/%d（%.1f%%），本次成功=%d，失败=%d，"
            "剩余未处理=%d，待认领=%d，HTTP请求中=%d/%d，准备状态=%s，待准备=%d，就绪请求=%d/%d，"
            "已认领未落库=%d，待写库结果=%d，"
            "HTTP累计尝试=%d，耗时=%.1f秒，结果确认速度=%.2f条/秒",
            self.progress.label, phase, processed, self.total,
            processed * 100.0 / self.total if self.total else 0.0,
            self.succeeded, self.failed, self.total - processed, len(self.candidates),
            http_in_flight, self.run_config.concurrency, preparation_state, pending_preparation,
            ready, self.run_config.ready_request_buffer_size, len(self.submitted) + len(self.buffer), waiting,
            self.network.http_attempts_started, elapsed,
            self.progress.confirmed / elapsed if elapsed > 0 else 0.0)
        self.progress.report_bottlenecks(http_peak, http_average)

    def flush(self):
        """整批保存返回后才清空；保存失败时保留全部结果。"""
        if not self.buffer:
            return
        self.logger.debug("开始提交最终结果缓冲：结果数=%d", len(self.buffer))
        started_at = time.monotonic()
        try:
            self.save_results(self.buffer)
        finally:
            self.progress.save.record(len(self.buffer), started_at)
        self.succeeded += sum(item[1].success for item in self.buffer)
        self.failed += sum(not item[1].success for item in self.buffer)
        self.progress.confirmed += len(self.buffer)
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

    def check(self):
        self.preparer.check()
        try:
            self.network.check()
        except BatchRunError:
            # 准备失败可能发生在两次检查之间，优先保留其原始异常。
            self.preparer.check()
            raise

    def run(self):
        """分批提前认领；准备和网络补货不等待调用线程写库。"""
        run = self.run_config
        self.report_progress(force=True)
        while True:
            self.report_progress()
            self.check()
            if self.should_flush():
                self.flush()
            # 只取当前快照，避免持续到达的结果推迟下一批备用输入认领。
            for _ in range(self.network.completions.qsize()):
                try:
                    message = self.network.completions.get_nowait()
                except queue.Empty:
                    break
                self.consume(message)
                self.check()
            self.check()
            if not self.submitted and not self.candidates:
                self.flush()
                self.report_progress(force=True, phase="处理结束")
                return
            keys = []
            _, pending_preparation = self.preparer.snapshot()
            # 保留一批待准备输入；认领大小及补货与未结束 HTTP/待写库数量无关。
            measure_claim = bool(self.candidates) and pending_preparation < run.batch_size
            started_at = time.monotonic()
            try:
                if measure_claim:
                    keys, jobs = self.claim_jobs(run.batch_size)
            finally:
                if measure_claim:
                    self.progress.claim.record(len(keys), started_at)
            if keys:
                # 同步认领可能耗时；确认后再次检查停止，再交付固定输入副本。
                self.check()
                self.submitted.update(keys)
                self.preparer.submit_batch(list(jobs))
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
        self.flush()


def _retain_unsaved(error, coordinator, network):
    """保留进程内结果及完成队列，不把异常收尾当成保存成功。"""
    if coordinator is not None:
        error.unsaved_results = coordinator.buffer
    if network is not None:
        error.completed_results = network.completions


def run_segments(ranges, load_segment, summary, network_factory,
                 result_item=None, log=None, label="任务"):
    """逐段加载和执行，复用一个网络线程，只跨段保留标量汇总。

    load_segment(start, end) 返回 store、候选队列、claim_jobs、save_results、
    summarize_month；最后一项先检查存储是否结束，再累加统计并返回本月
    新增成功和失败数。两轮都根据当前月已确认的状态 9 检查未结束任务。
    store 提供运行配置。检查整体停止后才加载下一段。
    各轮的全范围前置检查应在进入本函数之前完成。
    """
    active_logger = log or logger
    network = store = coordinator = progress = preparer = None
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
                        preparer = RequestPreparer(store.config, network, log=active_logger, label=label)
                        network.bind_ready_queue(preparer.ready_requests, preparer.wake)
                        network.start()
                        preparer.start()
                        progress = ProgressStats(store.config.run, active_logger, label)
                    coordinator = SegmentCoordinator(store.config.run, network, candidates,
                                                     claim_jobs, save_results, confirmed, result_item,
                                                     active_logger, progress, preparer)
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
            if preparer is not None:
                preparer.request_stop()
            try:
                if network is not None:
                    network.shutdown()
            finally:
                if preparer is not None:
                    preparer.shutdown()
        if preparer is not None:
            preparer.check()
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
            coordinator.report_progress(force=True, phase="异常停止")
            _retain_unsaved(persistence_error, coordinator, network)
            active_logger.exception("异常收尾时结果保存失败")
            active_logger.info("worker 收尾结束")
            raise persistence_error from error
        _retain_unsaved(error, coordinator, network)
        if coordinator is not None:
            coordinator.report_progress(force=True, phase="异常停止")
        active_logger.info("worker 收尾结束")
        raise
    return summary
