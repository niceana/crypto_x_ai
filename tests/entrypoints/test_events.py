"""验证研究流程的历史边界、任务状态、重试与续跑；不访问真实服务。"""

import importlib
import unittest
from unittest.mock import patch


from tests.event_support import EventFixture


class EventEntryPointTests(EventFixture, unittest.TestCase):
    def test_entrypoints_import_without_xdata_client(self):
        for name, stage in (("import_events", "import_events"), ("prepare_vectors", "vectors"),
                            ("run_round2", "round2"), ("run_round3", "round3")):
            module = importlib.import_module("entrypoints." + name)
            with patch.object(module, "run_entrypoint", return_value="summary") as start:
                self.assertEqual(module.main(), "summary")
                start.assert_called_once()
                self.assertEqual(start.call_args.kwargs, {"stage": stage})
