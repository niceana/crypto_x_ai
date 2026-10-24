import asyncio
import queue
import threading
import time
import unittest
from unittest.mock import patch

import httpx

from common.network import BatchRunError, NetworkWorker, RequestJob
from tests.support import Fixture, request_text, success


class NetworkTests(Fixture, unittest.TestCase):
    def make_network(self, handler):
        def create(config):
            return httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)

        return NetworkWorker(self.config(), client_factory=create)

    def test_queued_jobs_obey_http_concurrency_and_keep_all_completed_results(self):
        self.document["run"]["concurrency"] = 2
        entered, release = threading.Event(), threading.Event()
        busy, maximum, calls = 0, 0, []

        async def handler(request):
            nonlocal busy, maximum
            busy += 1
            maximum = max(maximum, busy)
            calls.append(request_text(request))
            if len(calls) == 2:
                entered.set()
            try:
                while not release.is_set():
                    await asyncio.sleep(0.001)
                return success()
            finally:
                busy -= 1

        network = self.make_network(handler)
        ready = queue.Queue(maxsize=4)
        network.bind_ready_queue(ready, lambda: None)

        async def wait_for_queue(size):
            while network.completions.qsize() < size:
                await asyncio.sleep(0.001)

        try:
            for index in range(4):
                ready.put_nowait(RequestJob(("2021-01-01", str(index)), str(index)))
            network.start()
            self.assertTrue(entered.wait(2))
            # 给所有已提交协程运行机会；慢调用线程不收取结果，也不额外起发 HTTP。
            asyncio.run_coroutine_threadsafe(asyncio.sleep(0.02), network.loop).result(2)
            self.assertEqual(len(calls), 2)
            self.assertEqual(ready.qsize(), 2)
            release.set()
            asyncio.run_coroutine_threadsafe(wait_for_queue(4), network.loop).result(2)
            network.check()
            self.assertEqual(maximum, 2)
            self.assertEqual(network.completions.maxsize, 0)
            messages = [network.completions.get_nowait() for _ in range(4)]
            self.assertEqual({message.key[1] for message in messages}, {"0", "1", "2", "3"})
            self.assertTrue(all(message.response.success for message in messages))
            self.assertFalse(network.unpublished)
        finally:
            release.set()
            network.shutdown()

    def test_retry_backoff_frees_http_slot_for_another_job(self):
        self.document["run"]["concurrency"] = 1
        calls = []

        async def handler(request):
            text = request_text(request)
            calls.append(text)
            if calls == ["retry"]:
                raise httpx.ReadTimeout("test timeout", request=request)
            return success()

        network = self.make_network(handler)
        try:
            with patch("common.network.retry_backoff", return_value=0.05):
                network.start()
                network.submit(RequestJob(("2021-01-01", "retry"), "retry"))
                network.submit(RequestJob(("2021-01-01", "next"), "next"))
                messages = [network.completions.get(timeout=3) for _ in range(2)]
            self.assertEqual(calls, ["retry", "next", "retry"])
            self.assertEqual({message.key[1]: message.http_attempts_started for message in messages},
                             {"next": 1, "retry": 2})
            self.assertTrue(all(message.response.success for message in messages))
        finally:
            network.shutdown()

    def test_stop_keeps_ready_job_without_starting_http(self):
        self.document["run"]["concurrency"] = 1
        started = threading.Event()
        calls = []

        async def handler(request):
            calls.append(request_text(request))
            started.set()
            await asyncio.sleep(60)
            return success()

        network = self.make_network(handler)
        ready = queue.Queue(maxsize=2)
        network.bind_ready_queue(ready, lambda: None)
        try:
            ready.put_nowait(RequestJob(("2021-01-01", "active"), "active"))
            ready.put_nowait(RequestJob(("2021-01-01", "waiting"), "waiting"))
            network.start()
            self.assertTrue(started.wait(2))
            network.request_stop()
            network.thread.join(2)
            self.assertFalse(network.thread.is_alive())
            message = network.completions.get_nowait()
            self.assertEqual(calls, ["active"])
            self.assertTrue(message.stopped)
            self.assertEqual((message.key[1], message.http_attempts_started), ("active", 1))
            self.assertEqual(ready.get_nowait().key[1], "waiting")
            self.assertEqual(network.http_attempts_started, 1)
            self.assertIsNone(network.failure)
            self.assertTrue(network.client.is_closed)
        finally:
            network.shutdown()

    def test_429_pauses_all_starts_and_recovers_with_same_input(self):
        for queued in (False, True):
            for retry_after in (None, "invalid", "0", "1"):
                with self.subTest(queued=queued, retry_after=retry_after):
                    self.document["run"]["concurrency"] = 2
                    paused = threading.Event()
                    calls, deadlines, waits = [], [], []

                    def handler(request):
                        text = request_text(request)
                        calls.append((text, time.monotonic(), request.content))
                        if len(calls) == 1:
                            headers = {} if retry_after is None else {"Retry-After": retry_after}
                            return httpx.Response(429, headers=headers)
                        return success()

                    network = self.make_network(handler)
                    ready = queue.Queue()
                    if queued:
                        network.bind_ready_queue(ready, lambda: None)

                    def submit(text):
                        job = RequestJob(("2021-01-01", text), text)
                        if queued:
                            ready.put_nowait(job)
                            network.notify_ready()
                        else:
                            network.submit(job)

                    def pause(seconds):
                        original_pause(seconds)
                        waits.append(seconds)
                        deadlines.append(network.gate.pause_until)
                        paused.set()

                    try:
                        network.start()
                        original_pause = network.gate.pause
                        with patch("common.network.retry_backoff", return_value=0.04), \
                                patch.object(network.gate, "pause", side_effect=pause):
                            submit("limited")
                            self.assertTrue(paused.wait(2))
                            submit("next")
                            messages = [network.completions.get(timeout=3) for _ in range(2)]
                        network.check()
                        self.assertEqual(waits, [1.0 if retry_after == "1" else 0.04])
                        self.assertEqual(len(calls), 3)
                        self.assertTrue(all(start >= deadlines[0] for _, start, _ in calls[1:]))
                        limited = [body for text, _, body in calls if text == "limited"]
                        self.assertEqual(limited, [calls[0][2], calls[0][2]])
                        self.assertEqual({msg.key[1]: msg.http_attempts_started for msg in messages},
                                         {"limited": 2, "next": 1})
                        self.assertTrue(all(msg.response.success for msg in messages))
                        self.assertEqual(network.http_attempts_started, 3)
                    finally:
                        network.shutdown()

    def test_stop_during_429_pause_cancels_wait_and_releases_http_slot(self):
        self.document["run"]["concurrency"] = 1
        paused = threading.Event()
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(429, headers={"Retry-After": "3600"})

        network = self.make_network(handler)
        try:
            network.start()
            original_pause = network.gate.pause

            def pause(seconds):
                original_pause(seconds)
                paused.set()

            with patch.object(network.gate, "pause", side_effect=pause):
                network.submit(RequestJob(("2021-01-01", "limited"), "limited"))
                self.assertTrue(paused.wait(2))
                network.request_stop("external stop during rate limit")
                network.thread.join(2)
            self.assertFalse(network.thread.is_alive())
            self.assertTrue(network.client.is_closed)
            self.assertEqual(len(calls), 1)
            message = network.completions.get_nowait()
            self.assertTrue(message.stopped)
            self.assertEqual(message.http_attempts_started, 1)
            self.assertEqual(network.http_slots._value, 1)
            self.assertIsNone(network.failure)
            self.assertTrue(network.completions.empty())
        finally:
            network.shutdown()

    def test_429_shares_budget_with_network_invalid_output_and_5xx(self):
        for final_status in (429, 503):
            with self.subTest(final_status=final_status):
                self.document["run"]["max_attempts"] = 4
                calls = []

                def handler(request):
                    calls.append(request.content)
                    if len(calls) == 1:
                        return httpx.Response(429)
                    if len(calls) == 2:
                        raise httpx.ReadTimeout("test timeout", request=request)
                    if len(calls) == 3:
                        return httpx.Response(200, text="invalid response")
                    return httpx.Response(final_status)

                network = self.make_network(handler)
                try:
                    network.start()
                    network.submit(RequestJob(("2021-01-01", "limited"), "limited", lambda content: content))
                    network.thread.join(2)
                    self.assertFalse(network.thread.is_alive())
                    message = network.completions.get_nowait()
                    self.assertEqual(message.http_attempts_started, 4)
                    self.assertEqual(message.response.http_status, final_status)
                    self.assertFalse(message.response.success)
                    self.assertEqual(len(calls), 4)
                    self.assertEqual(len(set(calls)), 1)
                    self.assertEqual(network.http_attempts_started, 4)
                    self.assertTrue(network.client.is_closed)
                    self.assertTrue(network.completions.empty())
                    with self.assertRaises(BatchRunError):
                        network.check()
                finally:
                    network.shutdown()

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
