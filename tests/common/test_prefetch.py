"""第一、三轮慢写库期间持续起发、重试及准备失败的离线回归。"""

import asyncio
import json
import threading
import unittest
from unittest.mock import patch

from common.ai_protocol import TaskRenderError, render_payload
from round1.runner import run_extraction
from round3.runner import run_impact
from tests.event_support import EventFixture, impact_result
from tests.support import FakeCli, event_content, request_text, success, tasks


class PrefetchTests(EventFixture, unittest.TestCase):
    def run_stage(self, stage, count, handler, update=None):
        config = self.config()
        if stage == "round1":
            backend = FakeCli(tasks(["2021-01-01"] * count))
            frame, status, response = backend.frame, "status", "ai_response_content"
            if update is not None:
                backend.update_hook = lambda values: update(
                    frame, status, response, values, lambda: backend.apply(values))
            execute = lambda: run_extraction(backend, self.task_path, self.path, ai_config=config,
                                             network_factory=self.factory(handler))
        else:
            backend = self.backend([("2021-01-01", "post {}".format(i), ["ETH"], "a") for i in range(count)])
            frame, status, response = backend.tables["result"], "round3_status", "round3_ai_response"
            frame["round2_status"], frame["event_relation"] = "1", "new"
            frame["event_id"] = frame.index.get_level_values(1)
            if update is not None:
                backend.update_hook = lambda path, values: update(
                    frame, status, response, values, lambda: backend.apply(path, values))
            execute = lambda: run_impact(backend, self.relation_settings(backend), ai_config=config,
                                         network_factory=self.factory(handler))
        with self.assertLogs(level="INFO"):
            result = execute()
        self.assertTrue(frame[status].eq("1").all())
        self.assertEqual(set(backend.threads), {threading.get_ident()})
        self.assertFalse(any(thread.name in ("ai-request-prepare", "ai-batch-network")
                             for thread in threading.enumerate()))
        return result, frame

    @staticmethod
    def position(request):
        text = request_text(request)
        if text.startswith("{"):
            text = json.loads(text)["event"]
        return int(text.split()[-1])

    @staticmethod
    async def wait_event(event):
        async def wait():
            while not event.is_set():
                await asyncio.sleep(0.001)
        await asyncio.wait_for(wait(), 3)

    @staticmethod
    def answer(stage):
        return success(event_content() if stage == "round1" else json.dumps(impact_result()))

    def test_requests_keep_starting_during_result_save_with_a_one_item_ready_queue(self):
        self.document["run"].update(concurrency=2, ready_request_buffer_size=1, batch_size=20,
                                     flush_interval_seconds=0.001, storage_max_attempts=1)
        for stage in ("round1", "round3"):
            with self.subTest(stage=stage):
                saving, finished = threading.Event(), threading.Event()
                requests, snapshots = [], []
                completed, active, peak = [0], [0], [0]

                async def handler(request):
                    position = self.position(request)
                    requests.append(position)
                    active[0] += 1
                    peak[0] = max(peak[0], active[0])
                    if position:
                        await self.wait_event(saving)
                    completed[0] += 1
                    active[0] -= 1
                    if completed[0] == 9:
                        finished.set()
                    return self.answer(stage)

                def update(frame, status, response, values, apply):
                    if response in values and not saving.is_set():
                        before = len(requests)
                        saving.set()
                        self.assertTrue(finished.wait(3), "结果写库期间没有持续发送剩余请求")
                        snapshots.append((before, len(requests)))
                        self.assertTrue(frame[status].eq("9").all(), "返回结果尚未写库，不应提前完成")
                    apply()

                result, frame = self.run_stage(stage, 9, handler, update)
                self.assertEqual(sorted(requests), list(range(9)))
                self.assertLess(snapshots[0][0], snapshots[0][1])
                self.assertEqual(peak[0], 2)
                self.assertEqual(result.http_attempts_started, 9)

    def test_previously_claimed_requests_keep_starting_during_next_claim(self):
        self.document["run"].update(concurrency=2, ready_request_buffer_size=1, batch_size=3,
                                     storage_max_attempts=1)
        for stage in ("round1", "round3"):
            with self.subTest(stage=stage):
                claiming, finished = threading.Event(), threading.Event()
                claims, completed = [0], [0]
                requested = []

                async def handler(request):
                    position = self.position(request)
                    requested.append(position)
                    if position < 3:
                        await self.wait_event(claiming)
                        completed[0] += 1
                        if completed[0] == 3:
                            finished.set()
                    return self.answer(stage)

                def update(frame, status, response, values, apply):
                    if response not in values:
                        claims[0] += 1
                        if claims[0] == 2:
                            claiming.set()
                            self.assertTrue(finished.wait(3), "下批认领写库期间没有补发上一批请求")
                            self.assertEqual(set(requested), {0, 1, 2})
                    apply()

                result, frame = self.run_stage(stage, 9, handler, update)
                self.assertGreaterEqual(claims[0], 3)
                self.assertEqual(result.http_attempts_started, 9)

    def test_retry_backoff_does_not_prevent_other_tasks_using_the_http_slot(self):
        self.document["run"].update(concurrency=1, ready_request_buffer_size=1,
                                     backoff_base_seconds=0.1, backoff_max_seconds=0.1)
        for stage in ("round1", "round3"):
            with self.subTest(stage=stage):
                requests, bodies = [], []

                def handler(request):
                    position = self.position(request)
                    requests.append(position)
                    if position == 0:
                        bodies.append(request.content)
                        if len(bodies) == 1:
                            return success("{}")
                    return self.answer(stage)

                with patch("common.network.retry_backoff", return_value=0.1):
                    result, frame = self.run_stage(stage, 3, handler)
                self.assertEqual(requests, [0, 1, 2, 0])
                self.assertEqual(bodies[0], bodies[1])
                self.assertEqual(result.http_attempts_started, 4)

    def test_request_render_failure_is_local_and_does_not_start_http(self):
        for stage in ("round1", "round3"):
            with self.subTest(stage=stage):
                requested = []

                def render(config, text):
                    if "post 0" in text:
                        raise TaskRenderError("测试请求构造失败")
                    return render_payload(config, text)

                # 直接运行，单条构造失败应保存为失败，其他任务继续完成。
                if stage == "round1":
                    backend = FakeCli(tasks(["2021-01-01"] * 2))
                    execute = lambda: run_extraction(backend, self.task_path, self.path, ai_config=self.config(),
                        network_factory=self.factory(lambda request: requested.append(request) or self.answer(stage)))
                else:
                    backend = self.backend([("2021-01-01", "post {}".format(i), ["ETH"], "a") for i in range(2)])
                    frame = backend.tables["result"]
                    frame["round2_status"], frame["event_relation"] = "1", "new"
                    frame["event_id"] = frame.index.get_level_values(1)
                    execute = lambda: run_impact(backend, self.relation_settings(backend), ai_config=self.config(),
                        network_factory=self.factory(lambda request: requested.append(request) or self.answer(stage)))
                with patch("common.preparation.render_payload", side_effect=render), self.assertLogs(level="INFO"):
                    result = execute()
                self.assertEqual(result.http_attempts_started, 1)
                self.assertEqual(len(requested), 1)
                self.assertEqual(result.new_failed if stage == "round1" else result.failed, 1)

    def test_unexpected_preparation_error_stops_and_preserves_original_error(self):
        backend = FakeCli(tasks(["2021-01-01"] * 3))
        error = RuntimeError("测试准备线程异常")
        with patch("common.preparation.render_payload", side_effect=error), self.assertLogs(level="INFO"):
            with self.assertRaises(RuntimeError) as caught:
                run_extraction(backend, self.task_path, self.path, ai_config=self.config(),
                    network_factory=self.factory(lambda request: self.fail("不应发送 HTTP")))
        self.assertIs(caught.exception, error)
        self.assertTrue(backend.frame["status"].eq("9").all())
        self.assertTrue(backend.frame["ai_response_content"].eq("").all())
        self.assertFalse(any(thread.name in ("ai-request-prepare", "ai-batch-network")
                             for thread in threading.enumerate()))
