"""验证共用入口的配置定位和启动失败边界，不连接真实数据系统。"""

import json
import logging
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

from entrypoints import runtime


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix=".test-runtime-", dir=str(Path.cwd()))
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.configs = self.root / "configs"
        self.configs.mkdir()
        self.path = self.configs / "event_config.yaml"
        self.document = {
            "first_round_task_path": "first", "result_path": "result",
            "round2_ai_config_path": "round2.yaml", "round3_ai_config_path": "round3.yaml",
            "start_time": "2021-01-01", "end_time": "2021-01-31",
            "logging": {"level": "ERROR"},
        }
        self.client = object()
        self.get_client = Mock(return_value=self.client)
        client_module = types.ModuleType("xdata.client")
        client_module.get_client = self.get_client
        package = types.ModuleType("xdata")
        package.__path__ = []
        package.client = client_module
        modules = patch.dict(sys.modules, {"xdata": package, "xdata.client": client_module})
        modules.start()
        self.addCleanup(modules.stop)
        location = patch.object(runtime, "__file__", str(self.root / "entrypoints" / "runtime.py"))
        location.start()
        self.addCleanup(location.stop)
        for name in ("httpx", "httpcore"):
            logger = logging.getLogger(name)
            self.addCleanup(logger.setLevel, logger.level)

    def test_loads_configs_from_entrypoint_location_and_creates_one_client(self):
        self.path.write_text(json.dumps(self.document), encoding="utf-8")
        working_directory = self.root / "other-directory"
        working_directory.mkdir()
        previous = Path.cwd()
        operation = Mock(return_value="summary")
        try:
            os.chdir(working_directory)
            with patch.object(logging, "basicConfig") as setup_logging:
                self.assertEqual(runtime.run_entrypoint(operation), "summary")
        finally:
            os.chdir(previous)
        self.get_client.assert_called_once_with()
        operation.assert_called_once()
        client, settings = operation.call_args.args
        self.assertIs(client, self.client)
        self.assertEqual(settings.first_round_task_path, "first")
        self.assertEqual(settings.result_path, "result")
        self.assertEqual((settings.start_time, settings.end_time), ("2021-01-01", "2021-01-31"))
        self.assertEqual(settings.round2_ai_config_path, str(self.configs / "round2.yaml"))
        self.assertEqual(settings.round3_ai_config_path, str(self.configs / "round3.yaml"))
        setup_logging.assert_called_once_with(level="ERROR")

    def test_invalid_config_stops_before_client_and_operation(self):
        self.document["end_time"] = None
        self.path.write_text(json.dumps(self.document), encoding="utf-8")
        operation = Mock()
        with patch.object(logging, "basicConfig") as setup_logging:
            with self.assertRaisesRegex(ValueError, "同时设置"):
                runtime.run_entrypoint(operation)
        self.get_client.assert_not_called()
        operation.assert_not_called()
        setup_logging.assert_not_called()
