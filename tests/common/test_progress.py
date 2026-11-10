"""离线核对三轮共用日志的队列口径、保存确认与跨月累计。"""

import json
import logging
import queue
import time
import unittest
from collections import deque
from types import SimpleNamespace
from unittest.mock import Mock

from common.ai_protocol import parse_response, request_failure
from common.config import RunConfig
from common.execution import ProgressStats, SegmentCoordinator
from common.network import CompletionMessage
from common.persistence import BatchPersistenceError
from round1.runner import run_extraction
from round3.runner import run_impact
from tests.event_support import EventFixture, impact_result
from tests.support import FakeCli, event_content, success, tasks


def memory_network(content, failed_keys=()):
    """请求仍执行真实响应解析与业务校验，不创建线程、事件循环或连接。"""
    network = SimpleNamespace(completions=queue.Queue(), unpublished=[], failure=None,
                              http_attempts_started=0, start=lambda: None,
                              shutdown=lambda: None, check=lambda: None,
                              http_snapshot=lambda: (0, 2, 1.25))

    def submit(job):
        network.http_attempts_started += 1
        response = request_failure("测试失败") if job.key in failed_keys else parse_response(200, success(content).text)
        value = job.validate_output(content) if response.success else None
        network.completions.put_nowait(CompletionMessage(job.key, response, 1, value=value))

    network.submit = submit
    return network


class ProgressTests(unittest.TestCase):
    def test_idle_progress_reports_every_ten_seconds_or_confirmed_threshold(self):
        log = Mock()
        progress = ProgressStats(RunConfig(progress_every=2), log, "测试")
        coordinator = SegmentCoordinator(progress.run, memory_network(event_content()),
                                         deque([1, 2]), None, None, log=log, progress=progress)
        coordinator.report_progress(force=True)
        self.assertEqual(log.info.call_count, 2)
        coordinator.report_progress()
        self.assertEqual(log.info.call_count, 2)
        progress.last_report_at -= 10
        coordinator.report_progress()
        self.assertEqual(log.info.call_count, 4)
        progress.confirmed = 2
        coordinator.report_progress()
        self.assertEqual(log.info.call_count, 6)

    def test_unsaved_queue_and_buffer_are_counted_without_counting_stopped_messages(self):
        network = memory_network(event_content())
        response = parse_response(200, success(event_content()).text)
        coordinator = SegmentCoordinator(RunConfig(concurrency=3), network, deque([1, 2, 3, 4]),
                                         None, None, log=logging.getLogger("test.progress"))
        coordinator.candidates.clear()
        coordinator.submitted = {2, 3, 4}
        coordinator.buffer = [(1, response)]
        network.completions.put_nowait(CompletionMessage(2, response, 1))
        network.completions.put_nowait(CompletionMessage(3, stopped=True))
        network.unpublished.append(CompletionMessage(4, response, 1))
        with self.assertLogs("test.progress", level="INFO") as logs:
            coordinator.report_progress(force=True)
        output = "\n".join(logs.output)
        for field in ("已处理=0/4", "HTTP请求中=0/3", "已认领未落库=4", "待写库结果=3",
                      "HTTP平均并发=1.25/3", "HTTP峰值=2"):
            self.assertIn(field, output)

    def test_failed_save_retains_results_and_does_not_report_committed_progress(self):
        def fail(items):
            raise BatchPersistenceError("final")

        network = memory_network(event_content())
        coordinator = SegmentCoordinator(RunConfig(), network, deque([1]), None, fail,
                                         log=logging.getLogger("test.progress"))
        coordinator.candidates.clear()
        coordinator.buffer = [(1, parse_response(200, success(event_content()).text))]
        coordinator.first_buffer_at = time.monotonic()
        with self.assertRaises(BatchPersistenceError):
            coordinator.flush()
        with self.assertLogs("test.progress", level="INFO") as logs:
            coordinator.report_progress(force=True, phase="异常停止")
        self.assertEqual((coordinator.confirmed, coordinator.progress.confirmed), (0, 0))
        self.assertEqual((coordinator.progress.save.calls, coordinator.progress.save.records), (1, 1))
        self.assertEqual(len(coordinator.buffer), 1)
        self.assertIn("阶段=异常停止，已处理=0/1", logs.output[0])
        self.assertIn("本次成功=0，失败=0", logs.output[0])
        self.assertIn("待写库结果=1", logs.output[0])


class ProgressIntegrationTests(EventFixture, unittest.TestCase):
    def test_round1_progress_and_storage_stats_accumulate_across_months(self):
        cli = FakeCli(tasks(["2021-01-01", "2021-01-02", "2021-02-01"]))
        network = self.add_memory_ready_queue(memory_network(event_content(), [cli.frame.index[1]]))
        with self.assertLogs("round1.runner", level="INFO") as logs:
            summary = run_extraction(cli, self.task_path, self.path, "2021-01-01", "2021-02-01",
                                     ai_config=self.config(), network_factory=lambda config: network)
        self.assertEqual((summary.new_succeeded, summary.new_failed, summary.http_attempts_started), (2, 1, 3))
        self.assertEqual(len(cli.reads), 2)
        finished = [line for line in logs.output if "阶段=处理结束" in line]
        self.assertEqual(len(finished), 2)
        self.assertIn("已处理=2/2（100.0%），本次成功=1，失败=1", finished[0])
        self.assertIn("已处理=1/1（100.0%），本次成功=1，失败=0", finished[1])
        bottleneck = [line for line in logs.output if "瓶颈统计" in line][-1]
        self.assertIn("结果保存平均批量=1.5条", bottleneck)
        self.assertIn("认领平均批量=1.5条", bottleneck)
        self.assertIn("HTTP平均并发=1.25/3", bottleneck)

    def test_round3_uses_same_progress_and_failure_counts_after_saving(self):
        backend = self.backend([("2021-01-01", "one", ["ETH"], "a"),
                                ("2021-02-01", "two", ["ETH"], "a")])
        frame = backend.tables["result"]
        frame["round2_status"] = "1"
        frame["event_relation"] = "new"
        frame["event_id"] = frame.index.get_level_values(1)
        network = self.add_memory_ready_queue(memory_network(json.dumps(impact_result()), [frame.index[1]]))
        with self.assertLogs("round3.runner", level="INFO") as logs:
            summary = run_impact(backend, self.relation_settings(backend), ai_config=self.config(),
                                 network_factory=lambda config: network)
        self.assertEqual((summary.new_completed, summary.failed, summary.http_attempts_started), (1, 1, 2))
        finished = [line for line in logs.output if "阶段=处理结束" in line]
        self.assertEqual(len(finished), 2)
        self.assertIn("round3事件影响分析当前分段AI任务处理进度", finished[0])
        self.assertIn("本次成功=0，失败=1", finished[1])
        self.assertIn("待写库结果=0", finished[1])
        bottleneck = [line for line in logs.output if "瓶颈统计" in line][-1]
        self.assertIn("结果保存平均批量=1.0条", bottleneck)
        self.assertIn("认领写库平均每批耗时=", bottleneck)

    def test_round1_failed_database_write_reports_exception_without_false_success(self):
        self.document["run"]["storage_max_attempts"] = 1
        cli = FakeCli(tasks(["2021-01-01"]))

        def update(frame):
            if "ai_response_content" in frame:
                raise RuntimeError("保存失败")
            cli.apply(frame)

        cli.update_hook = update
        network = self.add_memory_ready_queue(memory_network(event_content()))
        with self.assertLogs("round1.runner", level="INFO") as logs:
            with self.assertRaises(BatchPersistenceError) as caught:
                run_extraction(cli, self.task_path, self.path, ai_config=self.config(),
                               network_factory=lambda config: network)
        self.assertEqual(len(caught.exception.unsaved_results), 1)
        failed = next(line for line in logs.output if "阶段=异常停止" in line)
        self.assertIn("已处理=0/1", failed)
        self.assertIn("待写库结果=1", failed)
        self.assertFalse(any("阶段=处理结束" in line for line in logs.output))
