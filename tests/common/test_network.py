import asyncio
import queue
import threading
import unittest

import httpx

from common.network import BatchRunError, NetworkWorker, RequestJob
from tests.support import Fixture, request_text, success


class NetworkTests(Fixture, unittest.TestCase):
    def make_network(self, handler):
        def create(config):
            return httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)

        return NetworkWorker(self.config(), client_factory=create)

    def test_invalid_envelope_retry_logs_reason_without_response_content(self):
        calls = []
        def handler(request):
            calls.append(request)
            response = success("PRIVATE_RESPONSE_CONTENT")
            if len(calls) == 1:
                body = response.json()
                body["choices"][0]["finish_reason"] = "length"
                return httpx.Response(200, json=body)
            return response
        network = self.make_network(handler)
        try:
            with self.assertLogs("common.network", level="WARNING") as logs:
                network.start()
                network.submit(RequestJob(("2021-01-01", "one"), "private input", lambda content: content))
                message = network.completions.get(timeout=2)
            self.assertTrue(message.response.success)
            self.assertEqual(message.http_attempts_started, 2)
            output = "\n".join(logs.output)
            self.assertIn("原因=回答被截断", output)
            self.assertNotIn("PRIVATE_RESPONSE_CONTENT", output)
            self.assertNotIn("private input", output)
        finally:
            network.shutdown()

    def test_fatal_error_closes_without_caller_check_and_keeps_final_results(self):
        for failure_kind in ("http", "code"):
            with self.subTest(failure_kind=failure_kind):
                pending_started = threading.Event()
                pending_cancelled = threading.Event()

                async def handler(request):
                    text = request_text(request)
                    if text == "done":
                        return success("completed")
                    if text == "fatal":
                        # 先确认一条最终结果已入队、另一条请求已在等待，再触发停止。
                        while not pending_started.is_set() or network.completions.empty():
                            await asyncio.sleep(0.001)
                        if failure_kind == "http":
                            return httpx.Response(401, json={"error": "credentials expired"})
                        return success()
                    pending_started.set()
                    try:
                        await asyncio.sleep(60)
                    finally:
                        pending_cancelled.set()
                    return success("must not complete")

                def invalid_code(content):
                    raise RuntimeError("unexpected validator bug")

                network = self.make_network(handler)
                with self.assertLogs("common.network", level="ERROR"):
                    try:
                        network.start()
                        network.submit(RequestJob(("2021-01-01", "done"), "done"))
                        validator = invalid_code if failure_kind == "code" else None
                        network.submit(RequestJob(("2021-01-01", "fatal"), "fatal", validator))
                        network.submit(RequestJob(("2021-01-01", "pending"), "pending"))
                        # 此前不调用 check、request_stop 或 shutdown，证明后台自行退出。
                        network.thread.join(2)
                        self.assertFalse(network.thread.is_alive())
                        self.assertTrue(network.client.is_closed)
                        self.assertTrue(pending_cancelled.is_set())
                        messages = [network.completions.get_nowait() for _ in range(3)]
                        by_id = {message.key[1]: message for message in messages}
                        self.assertEqual(set(by_id), {"done", "fatal", "pending"})
                        self.assertTrue(by_id["done"].response.success)
                        self.assertTrue(by_id["pending"].stopped)
                        self.assertEqual(by_id["pending"].http_attempts_started, 1)
                        if failure_kind == "http":
                            self.assertEqual(by_id["fatal"].response.http_status, 401)
                            self.assertFalse(by_id["fatal"].response.success)
                            self.assertIsNone(network.failure)
                        else:
                            self.assertIs(by_id["fatal"].error, network.failure)
                            self.assertIsInstance(network.failure, RuntimeError)
                        self.assertTrue(network.completions.empty())
                        self.assertFalse(network.tasks)
                        self.assertFalse(network.unpublished)
                    finally:
                        network.shutdown()

    def test_external_stop_closes_and_late_submit_returns_one_stop_message(self):
        started = threading.Event()
        requested = []

        async def handler(request):
            requested.append(request_text(request))
            started.set()
            await asyncio.sleep(60)
            return success("must not complete")

        network = self.make_network(handler)
        try:
            network.start()
            network.submit(RequestJob(("2021-01-01", "pending"), "pending"))
            self.assertTrue(started.wait(2))
            network.request_stop("first reason")
            network.request_stop("second reason")
            network.thread.join(2)
            self.assertFalse(network.thread.is_alive())
            self.assertTrue(network.client.is_closed)
            self.assertEqual(network.stop_reason, "first reason")
            # 循环已经关闭，提交仍须交付一次停止消息，释放调用方的提交名额。
            network.submit(RequestJob(("2021-01-01", "late"), "late"))
            messages = [network.completions.get_nowait() for _ in range(2)]
            self.assertEqual({message.key[1] for message in messages}, {"pending", "late"})
            self.assertTrue(all(message.stopped for message in messages))
            late = next(message for message in messages if message.key[1] == "late")
            self.assertEqual(late.http_attempts_started, 0)
            self.assertEqual(requested, ["pending"])
            self.assertTrue(network.completions.empty())
            self.assertIsNone(network.failure)
        finally:
            network.shutdown()

    def test_queued_submit_before_stop_is_reported_without_http(self):
        loop_blocked = threading.Event()
        release_loop = threading.Event()
        requested = []

        def handler(request):
            requested.append(request)
            return success()

        def block_loop():
            loop_blocked.set()
            release_loop.wait(2)

        network = self.make_network(handler)
        try:
            network.start()
            network.loop.call_soon_threadsafe(block_loop)
            self.assertTrue(loop_blocked.wait(2))
            network.submit(RequestJob(("2021-01-01", "queued"), "queued"))
            network.request_stop()
            release_loop.set()
            network.thread.join(2)
            self.assertFalse(network.thread.is_alive())
            message = network.completions.get_nowait()
            self.assertEqual(message.key[1], "queued")
            self.assertTrue(message.stopped)
            self.assertEqual(message.http_attempts_started, 0)
            self.assertFalse(requested)
            self.assertTrue(network.completions.empty())
            self.assertIsNone(network.failure)
        finally:
            release_loop.set()
            network.shutdown()

    def test_stop_before_start_closes_after_initialization(self):
        network = self.make_network(lambda request: success())
        try:
            network.request_stop("stop before initialization")
            with self.assertRaises(BatchRunError):
                network.start()
            network.thread.join(2)
            self.assertFalse(network.thread.is_alive())
            self.assertTrue(network.client.is_closed)
            self.assertEqual(network.http_attempts_started, 0)
            self.assertEqual(network.stop_reason, "stop before initialization")
            self.assertIsNone(network.failure)
        finally:
            network.shutdown()

    def test_cancel_before_execution_still_reports_once_during_close(self):
        requested = []

        def handler(request):
            requested.append(request)
            return success()

        network = self.make_network(handler)
        job = RequestJob(("2021-01-01", "not_started"), "not_started")

        def launch_and_cancel():
            network._launch(job)
            next(iter(network.tasks)).cancel()
            network.request_stop()

        try:
            network.start()
            network.loop.call_soon_threadsafe(launch_and_cancel)
            network.thread.join(2)
            self.assertFalse(network.thread.is_alive())
            message = network.completions.get_nowait()
            self.assertEqual(message.key, job.key)
            self.assertTrue(message.stopped)
            self.assertEqual(message.http_attempts_started, 0)
            with self.assertRaises(queue.Empty):
                network.completions.get_nowait()
            self.assertFalse(requested)
            self.assertFalse(network.tasks)
            self.assertIsNone(network.failure)
        finally:
            network.shutdown()
