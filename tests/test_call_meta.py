"""调用统计原因合并；异步实盘重试覆盖见 test_async_live；全部使用模拟 HTTP。"""

import asyncio
import json
import unittest
from unittest.mock import patch

import httpx
import pandas as pd

from common.call_meta import CallRecorder, merge_call_meta, no_call
from common.config import EmbeddingConfig, ai_config_hash
from common.network import NetworkWorker
from tests.event_support import EventFixture, impact_result
from tests.test_live_integration import posts, history, utc_from_local
from tests.support import success
from vectorization.encoder import EmbeddingEncoder


class MetaMergeTests(unittest.TestCase):
    def test_reasons_append_with_absolute_attempt_numbers(self):
        first = CallRecorder()
        first.start_attempt()
        first.failure("ReadTimeout", will_retry=True)
        first.start_attempt()
        first.failure("HTTP 503", 503)
        old = merge_call_meta("", first.finish())
        second = CallRecorder()
        second.start_attempt()
        second.failure("无效枚举", 200, True)
        second.start_attempt()
        merged = json.loads(merge_call_meta(old, second.finish()))
        self.assertEqual((merged["attempts"], merged["retry_count"], merged["attempts_total"], merged["retry_count_total"]),
                         (2, 1, 4, 3))
        self.assertEqual([item["attempt"] for item in merged["retry_reasons"]], [1, 2, 3])
        self.assertEqual([item["reason"] for item in merged["retry_reasons"]], ["ReadTimeout", "HTTP 503", "无效枚举"])
        self.assertNotIn("mode", merged)
        self.assertNotIn("elapsed_ms", merged)

    def test_no_http_and_timeout_reason_does_not_repeat_request_body(self):
        recorder = CallRecorder()
        recorder.start_attempt()
        recorder.failure("ReadTimeout: slow\nuser=full original post")
        self.assertEqual(recorder.finish()["retry_reasons"][0]["reason"], "ReadTimeout: slow")
        empty = json.loads(merge_call_meta("", no_call("source")))
        self.assertEqual(empty["attempts_total"], 0)
        self.assertIsNone(empty["started_at"])
        self.assertEqual(empty["source_record_id"], "source")



if __name__ == "__main__":
    unittest.main()
