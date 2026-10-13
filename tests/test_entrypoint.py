import importlib.util
import logging
import os
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import httpx
import yaml

from tests.support import FakeCli, Fixture, success, tasks


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAIN_SOURCE = PROJECT_ROOT / "main.py"


def load_entrypoint():
    """仅注入模拟 xdata，测试不会导入或连接真实数据客户端。"""
    client_module = types.ModuleType("xdata.client")
    client_module.get_client = Mock(return_value=object())
    package = types.ModuleType("xdata")
    package.__path__ = []
    package.client = client_module
    spec = importlib.util.spec_from_file_location("entrypoint_under_test", str(MAIN_SOURCE))
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"xdata": package, "xdata.client": client_module}):
        spec.loader.exec_module(module)
    return module, client_module.get_client


class EntryPointTests(Fixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.root = self.path.parent
        ai_path = self.root / "ai_config.yaml"
        self.path.rename(ai_path)
        self.path = ai_path
        self.job_path = self.root / "job_config.yaml"
        self.settings = {
            "source_path": "source-must-not-be-read",
            "task_path": "existing-task-table",
            "ai_config_path": "ai_config.yaml",
            "start_time": "2021-01-01",
            "end_time": "2021-12-31",
            "logging": {"level": "CRITICAL"},
        }
        self.write_job(self.settings)
        self.module, self.get_client = load_entrypoint()
        self.module.__file__ = str(self.root / "main.py")

        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]
        original_level = root_logger.level
        root_logger.handlers = []

        def restore_logging():
            for handler in root_logger.handlers:
                handler.close()
            root_logger.handlers = original_handlers
            root_logger.setLevel(original_level)

        self.addCleanup(restore_logging)
        for name in ("httpx", "httpcore"):
            logger = logging.getLogger(name)
            self.addCleanup(logger.setLevel, logger.level)

    def write_job(self, settings):
        self.job_path.write_text(yaml.safe_dump(settings), encoding="utf-8")

    def test_import_does_not_create_client(self):
        self.get_client.assert_not_called()

    def test_run_uses_script_root_and_one_client(self):
        other_directory = self.root / "other-directory"
        other_directory.mkdir()
        original_directory = Path.cwd()
        self.addCleanup(os.chdir, original_directory)
        os.chdir(other_directory)
        result = object()
        with patch.object(self.module, "run_worker", return_value=result) as run:
            self.assertIs(self.module.main(), result)
        self.get_client.assert_called_once_with()
        run.assert_called_once()
        client, task_path, config_path = run.call_args.args
        self.assertIs(client, self.get_client.return_value)
        self.assertEqual(task_path, self.settings["task_path"])
        self.assertEqual(config_path, str(self.path.resolve()))
        self.assertEqual(run.call_args.kwargs, {
            "start_time": self.settings["start_time"],
            "end_time": self.settings["end_time"],
        })

    def test_absolute_ai_path_is_supported(self):
        self.settings["ai_config_path"] = str(self.path.resolve())
        self.write_job(self.settings)
        with patch.object(self.module, "run_worker") as run:
            self.module.main()
        self.assertEqual(run.call_args.args[2], str(self.path.resolve()))

    def test_invalid_settings_do_not_create_client(self):
        invalid = [None, [], "invalid"]
        for field in ("task_path", "ai_config_path"):
            for value in (None, " ", 42):
                invalid.append(dict(self.settings, **{field: value}))
            missing = dict(self.settings)
            del missing[field]
            invalid.append(missing)
        invalid.append(dict(self.settings, logging=[]))
        invalid.append(dict(self.settings, logging={"level": "INVALID_LEVEL"}))
        with patch.object(self.module, "run_worker") as run:
            for settings in invalid:
                with self.subTest(settings=settings):
                    self.write_job(settings)
                    with self.assertRaises(ValueError):
                        self.module.main()
                    self.get_client.assert_not_called()
                    run.assert_not_called()

    def test_missing_or_unreadable_files_do_not_create_client(self):
        self.job_path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.module.main()
        for raw, error_type in ((b"\xff", UnicodeError), (b"task_path: [", yaml.YAMLError)):
            self.job_path.write_bytes(raw)
            with self.assertRaises(error_type):
                self.module.main()
        self.write_job(self.settings)
        self.path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.module.main()
        self.get_client.assert_not_called()

    def test_client_failure_is_not_retried(self):
        error = ConnectionError("client failed")
        self.get_client.side_effect = error
        with patch.object(self.module, "run_worker") as run:
            with self.assertRaises(ConnectionError) as captured:
                self.module.main()
        self.assertIs(captured.exception, error)
        self.get_client.assert_called_once_with()
        run.assert_not_called()

    def test_worker_failure_preserves_exception(self):
        error = RuntimeError("worker failed")
        error.unsaved_results = [object()]
        with patch.object(self.module, "run_worker", side_effect=error) as run:
            with self.assertRaises(RuntimeError) as captured:
                self.module.main()
        self.assertIs(captured.exception, error)
        self.assertIs(captured.exception.unsaved_results, error.unsaved_results)
        self.get_client.assert_called_once_with()
        run.assert_called_once()

    def test_real_worker_uses_only_task_table(self):
        backend = FakeCli(tasks(["2021-01-15", "2021-02-15"]))
        self.get_client.return_value = backend

        def create_http_client(config):
            return httpx.AsyncClient(transport=httpx.MockTransport(lambda request: success()))

        with patch("ai_batch.runner._create_client", side_effect=create_http_client):
            result = self.module.main()
        self.get_client.assert_called_once_with()
        self.assertEqual(result.total, 2)
        self.assertEqual(result.new_succeeded, 2)
        self.assertEqual(result.new_failed, 0)
        self.assertEqual(len(backend.reads), 12)
        self.assertTrue(backend.update_paths)
        self.assertEqual(set(backend.read_paths + backend.update_paths), {self.settings["task_path"]})
        self.assertEqual(list(backend.frame["status"]), ["1", "1"])

    def test_script_exit_codes_and_standard_exception_output(self):
        self.settings.pop("logging")
        self.write_job(self.settings)
        script = self.root / "main.py"
        script.write_text(MAIN_SOURCE.read_text(encoding="utf-8"), encoding="utf-8")
        bootstrap = """
import runpy
import sys
import types

sys.path.insert(0, sys.argv[1])
mode = sys.argv[3]
package = types.ModuleType('xdata')
package.__path__ = []
client_module = types.ModuleType('xdata.client')
def get_client():
    if mode == 'client_error':
        raise ConnectionError('数据库连接超时')
    return object()
client_module.get_client = get_client
package.client = client_module
sys.modules['xdata'] = package
sys.modules['xdata.client'] = client_module
import ai_batch
from ai_batch.tasks import TaskValidationError
def run(client, task_path, config_path, start_time=None, end_time=None):
    if mode == 'worker_error':
        raise RuntimeError('worker 执行失败')
    if mode == 'validation_error':
        raise TaskValidationError('索引必须为名称和顺序固定的 date/id MultiIndex')
    if mode == 'empty_error':
        raise RuntimeError()
    if mode == 'whitespace_error':
        raise RuntimeError('   ')
    if mode == 'interrupt':
        raise KeyboardInterrupt()
    return object()
ai_batch.run_worker = run
runpy.run_path(sys.argv[2], run_name='__main__')
"""
        for mode, expected_code, reason in (
            ("normal", 0, None),
            ("client_error", 1, "数据库连接超时"),
            ("worker_error", 1, "worker 执行失败"),
            ("validation_error", 1, "索引必须为名称和顺序固定的 date/id MultiIndex"),
            ("empty_error", 1, "RuntimeError"),
            ("whitespace_error", 1, "RuntimeError"),
            ("interrupt", 130, None),
        ):
            with self.subTest(mode=mode):
                result = subprocess.run(
                    [sys.executable, "-X", "utf8", "-B", "-c", bootstrap,
                     str(PROJECT_ROOT), str(script), mode],
                    cwd=str(self.root), capture_output=True, text=True,
                    encoding="utf-8", timeout=15,
                )
                self.assertEqual(result.returncode, expected_code, result.stderr)
                if expected_code == 1:
                    self.assertIn("批量处理未完成", result.stderr)
                    self.assertIn(reason, result.stderr)
                    self.assertIn("Traceback", result.stderr)
                else:
                    self.assertNotIn("Traceback", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
