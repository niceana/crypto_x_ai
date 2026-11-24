"""真实异步调度+MockTransport：阶段取消、队列清理、完整业务语义和输入隔离。"""
import asyncio
import inspect
import json
import threading
import time
import unittest
from dataclasses import replace
from unittest.mock import patch

import httpx
import pandas as pd

from common.config import EmbeddingConfig
from crypto_x_ai_live import LiveConfig, run_live_batch
from crypto_x_ai_live import _compat as core
from crypto_x_ai_live.memory import MemoryTables
from tests.event_support import EventFixture


def posts(times=("16:01", "16:10"), prefix="p", texts=None, assets=None):
    return pd.DataFrame({"id": [prefix + str(i) for i in range(len(times))],
        "created_at": [(pd.Timestamp("2021-03-20 " + value) - pd.Timedelta(hours=8)).tz_localize("UTC").isoformat()
                       for value in times],
        "text": texts or ["event " + str(i) for i in range(len(times))],
        "user_name": ["account"] * len(times)},
        index=pd.Index(["2021-03-20 16:16:00"] * len(times), name="date"))


class AsyncLiveTests(EventFixture, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host_loop = asyncio.get_running_loop()

    def setUp(self):
        super().setUp()
        self.ai = self.config()
        self.ai = replace(self.ai, run=replace(self.ai.run, concurrency=2, ready_request_buffer_size=1,
                                              max_attempts=2, requests_per_second=0))
        self.live = LiveConfig(self.ai, self.ai, self.ai,
            EmbeddingConfig(base_url="https://mock.invalid/v1", api_key="not-real", max_attempts=2,
                            backoff_base_seconds=0.001, backoff_max_seconds=0.001))
        self.calls, self.clients = [], []
        self.finished = []
        self.block = self.invalid_phase = self.http_error = None
        self.delay = 0
        self.close_delay = 0
        self.mode = "update"
        self.tickers = ["ETH"]
        self.transient = set()
        self.started = asyncio.Event()
        self.active = self.peak = self.cancelled = 0

    async def handle(self, request):
        body = json.loads(request.content)
        value = body if "input" in body else json.loads(next(
            message["content"] for message in body["messages"] if message["role"] == "user"))
        phase = "vectors" if "input" in body else "round1" if "text" in value else "round2" if "current" in value else "round3"
        self.calls.append((phase, value))
        transient = phase in self.transient
        self.transient.discard(phase)
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            if self.block == phase:
                self.host_loop.call_soon_threadsafe(self.started.set)
                try:
                    await asyncio.sleep(30)
                except asyncio.CancelledError:
                    self.cancelled += 1
                    raise
            if self.delay:
                await asyncio.sleep(self.delay)
            if self.http_error == phase:
                return httpx.Response(429, headers={"Retry-After": "60"}, json={"error": "slow down"})
            if phase == "vectors":
                data = [{"index": i, "embedding": [1.0, 0.0]} for i, _ in enumerate(body["input"])]
                return httpx.Response(200, json={"data": [] if self.invalid_phase == phase or transient else data})
            if phase == "round1":
                answer = {"events": [{"event": value["text"], "tickers": self.tickers}]}
            elif phase == "round2":
                answer = {"event_relation": self.mode, "matched_candidate_id": value["candidates"][0]["candidate_id"],
                          "new_information": "new detail" if self.mode == "update" else None}
            else:
                answer = {"direction": "利好", "materiality": "中", "execution_stage": "已完成",
                          "persistence": "长期", "materiality_reason": "test mechanism"}
            if self.invalid_phase == phase or transient:
                answer = {}
            return httpx.Response(200, json={"choices": [{"index": 0, "finish_reason": "stop", "message": {
                "role": "assistant", "content": json.dumps(answer, ensure_ascii=False)}}]})
        finally:
            self.active -= 1
            self.finished.append((phase, threading.current_thread().name))

    def client(self, config):
        owner = self
        class Client(httpx.AsyncClient):
            async def aclose(self):
                if owner.close_delay:
                    await asyncio.sleep(owner.close_delay)
                await super().aclose()
        client = Client(transport=httpx.MockTransport(self.handle), trust_env=False)
        self.clients.append(client)
        return client

    async def run_batch(self, source=None, old=None, at="2021-03-20 16:17", **kwargs):
        kwargs.setdefault("assets", ["ETH", "BTC"])
        kwargs.setdefault("config", self.live)
        kwargs.setdefault("client_factory", self.client)
        now = (pd.Timestamp(at) - pd.Timedelta(hours=8)).tz_localize("UTC")
        with patch("crypto_x_ai_live.runner._system_now", return_value=now):
            return await run_live_batch(posts() if source is None else source, old, **kwargs)

    def assert_clean(self):
        self.assertEqual(self.active, 0)
        self.assertTrue(all(client.is_closed for client in self.clients))
        self.assertFalse(any(thread.name in ("ai-batch-coordinator", "ai-batch-network", "ai-request-prepare")
                             for thread in threading.enumerate()))

    async def test_full_flow_preserves_fields_wave_order_scoring_and_inputs(self):
        source = posts()
        before = source.copy(deep=True)
        threads = set(threading.enumerate())
        result = await self.run_batch(source, aggregation_method="sum")
        self.assertEqual(result["status"], "success")
        self.assertEqual(set(result), {"status", "round1_results", "event_results", "weight"})
        events = result["event_results"]
        self.assertEqual(events["event_relation"].tolist(), ["new", "update"])
        self.assertTrue(events["round3_status"].eq("1").all())
        self.assertEqual(result["weight"].iloc[0, 0], 21.0)
        self.assertEqual(result["weight"].iloc[0, 1], 0)
        self.assertEqual(len(result["round1_results"].columns), 10)
        self.assertEqual(len(events.columns), 16)
        meta = json.loads(events.iloc[0]["round3_call_meta"])
        self.assertEqual(meta["attempts"], 1)
        self.assertEqual(set(threading.enumerate()), threads)
        self.assertEqual({role for phase, role in self.finished if phase == "vectors"}, {"ai-batch-coordinator"})
        self.assertTrue(inspect.iscoroutinefunction(core.vectors.EmbeddingEncoder.prepare))
        self.assertTrue(inspect.iscoroutinefunction(core.vectors.EmbeddingEncoder._request))
        self.assertFalse(hasattr(core.network, "AsyncClientBridge"))
        pd.testing.assert_frame_equal(source, before)
        self.assert_clean()

    async def test_repeat_uses_history_without_new_impact_request(self):
        old = (await self.run_batch(posts(("15:50",), "old"), at="2021-03-20 16:02"))["event_results"]
        before = old.copy(deep=True)
        count = len(self.calls)
        self.mode = "repeat"
        result = await self.run_batch(posts(("16:10",)), old, aggregation_method="sum")
        self.assertEqual(result["status"], "success")
        row = result["event_results"].iloc[0]
        self.assertEqual(row["event_relation"], "repeat")
        self.assertEqual(row["round3_ai_response"], old.iloc[0]["round3_ai_response"])
        self.assertNotIn("round3", [phase for phase, _ in self.calls[count:]])
        self.assertEqual(json.loads(row["round3_call_meta"])["attempts"], 0)
        pd.testing.assert_frame_equal(old, before)
        self.assert_clean()

    async def test_same_timestamp_posts_do_not_become_each_others_candidates(self):
        result = await self.run_batch(posts(("16:10", "16:10")))
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["event_results"]["event_relation"].tolist(), ["new", "new"])
        self.assertNotIn("round2", [phase for phase, _ in self.calls])

    async def test_event_merge_skips_history_and_same_time_but_keeps_newer_same_batch(self):
        old = (await self.run_batch(posts(("16:10:00.123456789",), "old")))["event_results"]
        source = posts(("16:10:00.123456788", "16:10:00.123456789",
                        "16:10:00.123456790", "16:10:00.123456790"), "new")
        before_source, before_old = source.copy(deep=True), old.copy(deep=True)
        count = len(self.calls)
        with self.assertLogs(core.event_records.logger.name, level="WARNING") as logged:
            result = await self.run_batch(source, old)
        self.assertEqual(result["status"], "success")
        self.assertEqual(len(result["round1_results"]), 4)
        self.assertEqual(set(result["event_results"]["source_post_id"]), {"new2", "new3"})
        self.assertTrue(result["event_results"]["round3_status"].eq("1").all())
        dates = result["event_results"].index.get_level_values("date")
        self.assertTrue((dates > old.index.get_level_values("date").max()).all())
        self.assertEqual(sum(len(value["input"]) for phase, value in self.calls[count:] if phase == "vectors"), 2)
        self.assertIn("数量=2", "\n".join(logged.output))
        pd.testing.assert_frame_equal(source, before_source)
        pd.testing.assert_frame_equal(old, before_old)
        self.assert_clean()

    async def test_global_history_cutoff_also_skips_earlier_events_for_another_asset(self):
        old = (await self.run_batch(posts(("16:10",), "old")))["event_results"]
        before = old.copy(deep=True)
        self.tickers = ["BTC"]
        count = len(self.calls)
        with self.assertLogs(core.event_records.logger.name, level="WARNING"):
            result = await self.run_batch(posts(("16:09", "16:10"), "new"), old)
        self.assertEqual(result["status"], "success")
        self.assertTrue(result["event_results"].empty)
        self.assertTrue(result["round1_results"]["status"].eq("1").all())
        self.assertEqual({phase for phase, _ in self.calls[count:]}, {"round1"})
        pd.testing.assert_frame_equal(old, before)
        self.assert_clean()

    async def test_skipped_existing_event_still_checks_fixed_input_conflicts(self):
        old = (await self.run_batch(posts(("16:10",), "old", texts=["original"])))["event_results"]
        before = old.copy(deep=True)
        result = await self.run_batch(posts(("16:10",), "old", texts=["changed"]), old)
        self.assertEqual(result, {"status": "failed"})
        pd.testing.assert_frame_equal(old, before)
        self.assert_clean()

    async def test_retry_metadata_uses_existing_response_and_reason_formats(self):
        old = (await self.run_batch(posts(("15:50",), "old")))["event_results"]
        self.transient = {"round1", "vectors", "round2", "round3"}
        result = await self.run_batch(posts(("16:10",)), old)
        self.assertEqual(result["status"], "success")
        for stage in ("round1", "round2", "round3"):
            table = result["round1_results" if stage == "round1" else "event_results"]
            meta = json.loads(table.iloc[0][stage + "_call_meta"])
            self.assertEqual((meta["attempts"], meta["retry_count"], meta["attempts_total"]), (2, 1, 2))
            self.assertEqual(len(meta["retry_reasons"]), 1)
            self.assertTrue(meta["retry_reasons"][0]["will_retry"])
            self.assertLessEqual(pd.Timestamp(meta["started_at"]), pd.Timestamp(meta["finished_at"]))
        self.assert_clean()

    async def test_failures_in_each_stage_only_return_status_and_close_resources(self):
        for phase in ("round1", "vectors", "round2", "round3"):
            with self.subTest(phase=phase):
                self.invalid_phase = phase
                result = await self.run_batch()
                self.assertEqual(result, {"status": "failed"})
                self.assert_clean()

    async def test_timeout_in_each_stage_cancels_requests_and_allows_fresh_next_call(self):
        for phase in ("round1", "vectors", "round2", "round3"):
            with self.subTest(phase=phase):
                self.block = phase
                self.started.clear()
                before_tasks = asyncio.all_tasks()
                start = time.monotonic()
                result = await self.run_batch(timeout_seconds=0.2)
                self.assertEqual(result, {"status": "timeout"})
                self.assertTrue(self.started.is_set())
                self.assertLess(time.monotonic() - start, 1.5)
                self.assertEqual(asyncio.all_tasks(), before_tasks)
                self.assert_clean()
        count = sum(phase == "round1" for phase, _ in self.calls)
        self.block = None
        result = await self.run_batch(posts(("16:01", "16:10", "16:46")), at="2021-03-20 17:02")
        self.assertEqual(result["status"], "success")
        self.assertEqual(sum(phase == "round1" for phase, _ in self.calls), count + 3)
        self.assertEqual(result["weight"].index[0], pd.Timestamp("2021-03-20 17:02"))
        self.assertEqual(result["weight"].iloc[0, 0], 9.0)
        self.assert_clean()

    async def test_timeout_interrupts_global_retry_after_without_blocking_sleep(self):
        self.http_error = "round1"
        with patch("time.sleep", side_effect=AssertionError("blocking sleep")):
            result = await self.run_batch(timeout_seconds=0.1)
        self.assertEqual(result, {"status": "timeout"})
        self.assertLessEqual(len(self.calls), 2)
        self.assert_clean()

    async def test_external_cancellation_waits_for_client_close_and_propagates(self):
        self.block = "vectors"
        self.close_delay = 0.02
        before = asyncio.all_tasks()
        task = asyncio.create_task(self.run_batch())
        await asyncio.wait_for(self.started.wait(), 2)
        task.cancel()
        await asyncio.sleep(0.005)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(asyncio.all_tasks(), before)
        self.assert_clean()

    async def test_embedding_retry_sleep_is_async_and_cancelled_by_batch_timeout(self):
        self.http_error = "vectors"
        with patch("time.sleep", side_effect=AssertionError("blocking sleep")):
            result = await self.run_batch(timeout_seconds=0.2)
        self.assertEqual(result, {"status": "timeout"})
        self.assertEqual(sum(phase == "vectors" for phase, _ in self.calls), 1)
        self.assert_clean()

    async def test_timeout_cancels_vector_client_startup_and_closes_it(self):
        owner = self
        class SlowClient(httpx.AsyncClient):
            async def __aenter__(self):
                await super().__aenter__()
                owner.host_loop.call_soon_threadsafe(owner.started.set)
                await asyncio.sleep(30)
                return self
        def factory(config):
            if not isinstance(config, EmbeddingConfig):
                return self.client(config)
            client = SlowClient(transport=httpx.MockTransport(self.handle), trust_env=False)
            self.clients.append(client)
            return client
        result = await self.run_batch(client_factory=factory, timeout_seconds=0.2)
        self.assertEqual(result, {"status": "timeout"})
        self.assertTrue(self.started.is_set())
        self.assertNotIn("vectors", [phase for phase, _ in self.calls])
        self.assert_clean()

    async def test_cancel_during_vector_close_waits_for_close_to_finish(self):
        owner = self
        class SlowCloseClient(httpx.AsyncClient):
            async def aclose(self):
                owner.host_loop.call_soon_threadsafe(owner.started.set)
                await asyncio.sleep(0.05)
                await super().aclose()
        def factory(config):
            if not isinstance(config, EmbeddingConfig):
                return self.client(config)
            client = SlowCloseClient(transport=httpx.MockTransport(self.handle), trust_env=False)
            self.clients.append(client)
            return client
        task = asyncio.create_task(self.run_batch(client_factory=factory))
        await asyncio.wait_for(self.started.wait(), 2)
        task.cancel()
        await asyncio.sleep(0.005)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertNotIn("round2", [phase for phase, _ in self.calls])
        self.assert_clean()

    async def test_concurrency_is_bounded_and_event_loop_remains_responsive(self):
        self.delay = 0.01
        source = posts(("16:01",) * 8)
        entered, heartbeat_seen = asyncio.Event(), asyncio.Event()
        release = threading.Event()
        original = self.handle

        async def hold_vectors(request):
            if "input" in json.loads(request.content):
                self.host_loop.call_soon_threadsafe(entered.set)
                while not release.is_set():
                    await asyncio.sleep(0.002)
            return await original(request)

        async def heartbeat():
            while True:
                if entered.is_set():
                    heartbeat_seen.set()
                await asyncio.sleep(0.002)
        ticker = asyncio.create_task(heartbeat())
        try:
            with patch.object(self, "handle", hold_vectors):
                task = asyncio.create_task(self.run_batch(source))
                try:
                    await asyncio.wait_for(entered.wait(), 2)
                    await asyncio.wait_for(heartbeat_seen.wait(), 2)
                    self.assertFalse(task.done())
                finally:
                    release.set()
                    result = await task
        finally:
            ticker.cancel()
            await asyncio.gather(ticker, return_exceptions=True)
        self.assertEqual(result["status"], "success")
        self.assertLessEqual(self.peak, 2)
        self.assertTrue(heartbeat_seen.is_set())
        self.assert_clean()

    async def wait_event(self, event, seconds=1.5):
        stop = asyncio.get_running_loop().time() + seconds
        while not event.is_set():
            if asyncio.get_running_loop().time() >= stop:
                self.fail("未收到预期线程事件")
            await asyncio.sleep(0.002)

    async def test_network_completes_requests_while_preparation_is_blocked(self):
        old = (await self.run_batch(posts(("15:50",), "old")))["event_results"]
        entered, release = threading.Event(), threading.Event()
        original = core.embedding.EmbeddingRetriever.__call__
        roles = []
        count = [0]
        def slow_prepare(retriever, current, searchable):
            count[0] += 1
            roles.append(threading.current_thread().name)
            if count[0] == 2:
                entered.set()
                release.wait(2)
            return original(retriever, current, searchable)
        self.delay = 0.03
        baseline = len([phase for phase, _ in self.finished if phase == "round2"])
        with patch.object(core.embedding.EmbeddingRetriever, "__call__", slow_prepare):
            task = asyncio.create_task(self.run_batch(posts(("16:10",) * 3), old))
            try:
                await self.wait_event(entered)
                stop = asyncio.get_running_loop().time() + 1
                while len([phase for phase, _ in self.finished if phase == "round2"]) == baseline:
                    self.assertLess(asyncio.get_running_loop().time(), stop)
                    await asyncio.sleep(0.002)
                self.assertFalse(release.is_set())
            finally:
                release.set()
                result = await task
        self.assertEqual(result["status"], "success")
        self.assertEqual(set(roles), {"ai-request-prepare"})
        self.assertTrue(all(role == "ai-batch-network" for phase, role in self.finished if phase == "round2"))
        self.assert_clean()

    async def test_network_drains_ready_queue_while_result_commit_is_blocked(self):
        old = (await self.run_batch(posts(("15:50",), "old")))["event_results"]
        entered, release = threading.Event(), threading.Event()
        original = MemoryTables.update_by_column
        roles = []
        def slow_commit(memory, path, patch):
            if "round2_status" in patch and patch["round2_status"].eq("1").any() and not entered.is_set():
                roles.append(threading.current_thread().name)
                entered.set()
                release.wait(2)
            return original(memory, path, patch)
        self.delay = 0.04
        baseline = len([phase for phase, _ in self.finished if phase == "round2"])
        with patch.object(MemoryTables, "update_by_column", slow_commit):
            task = asyncio.create_task(self.run_batch(posts(("16:10",) * 4), old))
            try:
                await self.wait_event(entered)
                stop = asyncio.get_running_loop().time() + 1
                while len([phase for phase, _ in self.finished if phase == "round2"]) < baseline + 4:
                    self.assertLess(asyncio.get_running_loop().time(), stop)
                    await asyncio.sleep(0.002)
                self.assertFalse(release.is_set())
            finally:
                release.set()
                result = await task
        self.assertEqual(result["status"], "success")
        self.assertEqual(roles, ["ai-batch-coordinator"])
        self.assert_clean()

    async def test_cancel_during_commit_waits_for_coordinator_before_returning(self):
        entered, release = threading.Event(), threading.Event()
        original = MemoryTables.update_by_column
        def slow_commit(memory, path, patch):
            if "status" in patch and patch["status"].eq("1").any():
                entered.set()
                release.wait(2)
            return original(memory, path, patch)
        with patch.object(MemoryTables, "update_by_column", slow_commit):
            task = asyncio.create_task(self.run_batch(posts(("16:10",))))
            try:
                await self.wait_event(entered)
                task.cancel()
                await asyncio.sleep(0.02)
                self.assertFalse(task.done(), "尚未退出的协调线程不能被丢到后台")
            finally:
                release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assert_clean()

    async def test_timeout_cancels_client_startup_and_closes_client(self):
        owner = self
        class SlowClient(httpx.AsyncClient):
            async def __aenter__(self):
                await super().__aenter__()
                owner.host_loop.call_soon_threadsafe(owner.started.set)
                await asyncio.sleep(30)
                return self
        def factory(config):
            client = SlowClient(transport=httpx.MockTransport(self.handle), trust_env=False)
            self.clients.append(client)
            return client
        result = await self.run_batch(client_factory=factory, timeout_seconds=0.05)
        self.assertEqual(result, {"status": "timeout"})
        self.assertTrue(self.started.is_set())
        self.assertEqual(self.calls, [])
        self.assert_clean()

    async def test_invalid_or_missing_historical_vectors_fail_without_repair(self):
        old = (await self.run_batch(posts(("15:50",), "old")))["event_results"]
        for value in ("", "broken", old.iloc[0]["embedding"].replace("bge-m3-v1", "wrong")):
            broken = old.copy(deep=True)
            broken["embedding"] = value
            count = sum(phase == "vectors" for phase, _ in self.calls)
            result = await self.run_batch(posts(("16:10",)), broken)
            self.assertEqual(result, {"status": "failed"})
            self.assertEqual(sum(phase == "vectors" for phase, _ in self.calls), count)
            self.assertTrue(broken["embedding"].eq(value).all())

    async def test_no_new_posts_keeps_history_and_empty_window_produces_zero(self):
        old = (await self.run_batch(posts(("15:50",), "old")))["event_results"]
        count = len(self.calls)
        result = await self.run_batch(pd.DataFrame(), old)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["weight"].iloc[0, 0], 12)
        self.assertEqual(len(self.calls), count)
        empty = await self.run_batch(pd.DataFrame(), None)
        self.assertTrue(empty["weight"].eq(0).all().all())

    async def test_nontradable_events_remain_unprocessed_and_zero_weight(self):
        self.tickers = ["OTHER"]
        result = await self.run_batch()
        self.assertEqual(result["status"], "success")
        self.assertTrue(result["event_results"]["round2_status"].eq("0").all())
        self.assertTrue(result["weight"].eq(0).all().all())
        self.assertEqual({phase for phase, _ in self.calls}, {"round1"})

    async def test_output_time_is_final_time_but_window_uses_call_start(self):
        old = (await self.run_batch(posts(("16:01",), "old")))["event_results"]
        values = [pd.Timestamp("2021-03-20 08:47", tz="UTC"), pd.Timestamp("2021-03-20 09:02", tz="UTC")]
        with patch("crypto_x_ai_live.runner._system_now", side_effect=values):
            result = await run_live_batch(pd.DataFrame(), old, assets=["ETH"], config=self.live, client_factory=self.client)
        self.assertEqual(result["weight"].index[0], pd.Timestamp("2021-03-20 17:02"))
        self.assertEqual(result["weight"].iloc[0, 0], 12)

    async def test_late_synchronous_snapshot_is_not_returned_as_success(self):
        original = MemoryTables.event_changes
        def delayed(memory):
            time.sleep(0.08)
            return original(memory)
        with patch.object(MemoryTables, "event_changes", delayed):
            result = await self.run_batch(pd.DataFrame(), timeout_seconds=0.03)
        self.assertEqual(result, {"status": "timeout"})

    async def test_public_api_is_async_without_resume_or_manual_time(self):
        self.assertTrue(inspect.iscoroutinefunction(run_live_batch))
        for name in ("resume_state", "as_of", "round1_state", "network_factory", "embedding_encoder"):
            self.assertNotIn(name, inspect.signature(run_live_batch).parameters)
        for timeout in (0, -1, True, float("inf"), float("nan")):
            self.assertEqual(await self.run_batch(timeout_seconds=timeout), {"status": "failed"})

    async def test_raw_input_validation_and_created_at_semantics(self):
        source = posts(("16:10",))
        result = await self.run_batch(source)
        self.assertEqual(result["round1_results"].index[0][0], pd.Timestamp("2021-03-20 08:10"))
        bad = source.drop(columns=["created_at"])
        count = len(self.calls)
        self.assertEqual(await self.run_batch(bad), {"status": "failed"})
        self.assertEqual(len(self.calls), count)
        for ids in ([123], [None], [""]):
            bad = source.copy()
            bad["id"] = ids
            self.assertEqual(await self.run_batch(bad), {"status": "failed"})


if __name__ == "__main__":
    unittest.main()
