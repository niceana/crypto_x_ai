"""离线验证原重试循环的十次预算；直接驱动协程，不创建事件循环或套接字。"""

import json
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pandas as pd
import yaml

from common.config import EmbeddingConfig, RunConfig, load_embedding_config
from common.network import NetworkWorker, RequestJob
from common.persistence import BatchPersistenceError, write_with_retry
from common.validation import validate_extraction_output
from round2.embedding import EmbeddingRetriever
from tests.support import Fixture, event_content, success
from vectorization.encoder import EmbeddingEncoder


class RetryLimitTests(Fixture, unittest.TestCase):
    def execute_job(self, handler, limit=10):
        config = self.config()
        config = replace(config, run=replace(config.run, max_attempts=limit))
        network = NetworkWorker(config)
        calls, waits = [], []

        async def post(*args, **kwargs):
            calls.append(kwargs)
            return handler(len(calls))

        async def acquire():
            return None

        async def wait_until(deadline):
            waits.append(deadline)

        network.client = SimpleNamespace(post=post)
        network.gate = SimpleNamespace(acquire=acquire, wait_until=wait_until, pause=lambda value: None)
        network.http_slots = SimpleNamespace(acquire=acquire, release=lambda: None)
        network._record_http = lambda delta: None
        coroutine = network._execute(RequestJob(("2021-01-01 00:00:00", "id"), "post", validate_extraction_output))
        try:
            coroutine.send(None)
        except StopIteration as complete:
            return complete.value, network, calls, waits
        finally:
            coroutine.close()
        self.fail("内存 HTTP 替身不应执行任何真实 I/O")

    def test_invalid_output_can_succeed_on_tenth_attempt(self):
        result, network, calls, waits = self.execute_job(
            lambda count: success(event_content() if count == 10 else "invalid JSON"))
        self.assertTrue(result.response.success)
        self.assertEqual((len(calls), result.http_attempts_started, len(waits)), (10, 10, 9))
        self.assertFalse(network.stopped.is_set())

    def test_invalid_output_only_fails_after_all_ten_attempts(self):
        result, network, calls, waits = self.execute_job(lambda count: success("invalid JSON"))
        self.assertFalse(result.response.success)
        self.assertEqual((len(calls), result.http_attempts_started, len(waits)), (10, 10, 9))

    def test_429_reuses_original_retry_loop_until_tenth_attempt(self):
        result, network, calls, waits = self.execute_job(
            lambda count: success(event_content()) if count == 10 else httpx.Response(429))
        self.assertTrue(result.response.success)
        self.assertEqual((len(calls), len(waits)), (10, 9))
        self.assertFalse(network.stopped.is_set())

    def test_429_only_stops_after_ten_attempts(self):
        result, network, calls, waits = self.execute_job(lambda count: httpx.Response(429))
        self.assertFalse(result.response.success)
        self.assertTrue(network.stopped.is_set())
        self.assertEqual((len(calls), len(waits)), (10, 9))

    def test_network_errors_recover_within_same_original_budget(self):
        def handle(count):
            if count < 10:
                raise httpx.ConnectError("temporary")
            return success(event_content())
        result, _, calls, _ = self.execute_job(handle)
        self.assertTrue(result.response.success)
        self.assertEqual(len(calls), 10)

    def test_storage_reuses_same_batch_until_tenth_attempt(self):
        batch = pd.DataFrame({"value": [1]})
        calls = []
        def write(frame):
            calls.append(frame)
            pd.testing.assert_frame_equal(frame, batch)
            if len(calls) < 10:
                raise ConnectionError("temporary")
        with patch("common.persistence.time.sleep") as sleep:
            write_with_retry(write, batch, RunConfig(), "test")
        self.assertEqual((len(calls), sleep.call_count), (10, 9))
        with patch("common.persistence.time.sleep") as sleep:
            with self.assertRaises(BatchPersistenceError):
                write_with_retry(lambda frame: (_ for _ in ()).throw(ConnectionError()), batch, RunConfig(), "test")
        self.assertEqual(sleep.call_count, 9)

    def test_vector_response_recovers_on_tenth_attempt(self):
        calls, saved = [], {}
        def handler(request):
            calls.append(request)
            return httpx.Response(200, json={"data": [{"index": 0, "embedding":
                                                       [1.0, 0.0] if len(calls) == 10 else [0.0, 0.0]}]})
        encoder = EmbeddingEncoder(EmbeddingConfig("https://mock.invalid/v1", "secret"),
                                  client_factory=lambda config: httpx.Client(transport=httpx.MockTransport(handler)))
        with patch("vectorization.encoder.time.sleep") as sleep:
            encoder.prepare([("key", "text", "")], saved.update)
        self.assertEqual((len(calls), sleep.call_count), (10, 9))
        self.assertIn("key", saved)

    def test_second_round_has_no_generation_methods_and_needs_no_embedding_key(self):
        path = self.path.parent / "embedding.yaml"
        path.write_text(json.dumps({"base_url": "https://mock.invalid/v1", "api_key": "${MISSING_KEY}"}), encoding="utf-8")
        with patch("common.config.load_dotenv", side_effect=AssertionError("不应加载密钥")):
            config = load_embedding_config(path, require_api_key=False)
        retriever = EmbeddingRetriever(config)
        self.assertEqual(config.api_key, "")
        for method in ("prepare", "_request", "client_factory"):
            self.assertFalse(hasattr(retriever, method))

    def test_project_configuration_uses_ten_attempts(self):
        self.assertEqual((RunConfig().max_attempts, RunConfig().storage_max_attempts), (10, 10))
        root = Path(__file__).resolve().parent.parent
        for name in ("round1", "round2", "round3"):
            document = yaml.safe_load((root / "configs" / (name + "_ai_config.yaml")).read_text(encoding="utf-8"))
            self.assertEqual((document["run"]["max_attempts"], document["run"]["storage_max_attempts"]), (10, 10))
        self.assertEqual(yaml.safe_load((root / "configs" / "embedding_config.yaml").read_text(encoding="utf-8"))["max_attempts"], 10)


if __name__ == "__main__":
    unittest.main()
