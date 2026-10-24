"""独立补货的回归场景；只使用内存历史和队列，不连接 HTTP 或数据库。"""

import json
import queue
import threading
import time
import unittest
from types import SimpleNamespace

from common.event_context import EventHistory
from round2.preparation import AutoNewResult, RelationPreparer
from round2.retrieval import RetrievalSelection
from tests.support import Fixture


class PreparationTests(Fixture, unittest.TestCase):
    def make_preparer(self, retrieve):
        self.document["run"]["ready_request_buffer_size"] = 2
        network = SimpleNamespace(stopped=threading.Event(), completions=queue.Queue(),
                                  changed=threading.Event())
        network.notify_ready = network.changed.set
        network.request_stop = lambda reason: network.stopped.set()
        preparer = RelationPreparer(EventHistory(), retrieve, self.config(), network)
        preparer.start()
        self.addCleanup(preparer.shutdown)
        return preparer, network

    def records(self, events):
        return [(("2021-01-01", str(index)), {
            "record_id": str(index), "date": "2021-01-01", "asset": "ASSET{}".format(index),
            "event": event, "source_post_id": str(index)}) for index, event in enumerate(events)]

    def wait_ready(self, preparer, network, count):
        deadline = time.monotonic() + 3
        while preparer.ready_requests.qsize() < count:
            preparer.check()
            remaining = deadline - time.monotonic()
            self.assertGreater(remaining, 0)
            network.changed.wait(remaining)
            network.changed.clear()

    def test_take_refills_ready_queue_without_any_result_being_saved(self):
        preparer, network = self.make_preparer(lambda current, history: RetrievalSelection())
        preparer.submit_batch(self.records(["one", "two", "three", "four"]))
        self.wait_ready(preparer, network, 2)
        first = preparer.ready_requests.get_nowait()
        preparer.wake()
        self.wait_ready(preparer, network, 2)
        remaining = [preparer.ready_requests.get_nowait() for _ in range(2)]
        self.assertEqual([first.key[1]] + [job.key[1] for job in remaining], ["0", "1", "2"])
        self.assertTrue(network.completions.empty())
        self.assertEqual(json.loads(remaining[-1].body.decode("utf-8"))["model"], "test")

    def test_auto_new_results_do_not_use_ready_request_capacity(self):
        def retrieve(current, history):
            if current["event"].startswith("auto"):
                return RetrievalSelection((), True, "test-auto-new")
            return RetrievalSelection()

        preparer, network = self.make_preparer(retrieve)
        preparer.submit_batch(self.records(["auto-one", "auto-two", "ai-one", "ai-two"]))
        self.wait_ready(preparer, network, 2)
        results = [network.completions.get_nowait() for _ in range(2)]
        self.assertTrue(all(isinstance(result, AutoNewResult) for result in results))
        self.assertEqual({result.key[1] for result in results}, {"0", "1"})
        self.assertEqual({preparer.ready_requests.get_nowait().key[1] for _ in range(2)}, {"2", "3"})

    def test_shutdown_wakes_producer_waiting_on_a_full_ready_queue(self):
        preparer, network = self.make_preparer(lambda current, history: RetrievalSelection())
        preparer.submit_batch(self.records(["one", "two", "three"]))
        self.wait_ready(preparer, network, 2)
        preparer.shutdown()
        self.assertFalse(preparer.thread.is_alive())
        self.assertEqual(preparer.ready_requests.qsize(), 2)
        self.assertTrue(network.completions.empty())
