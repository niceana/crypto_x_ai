import importlib.util
import logging
import os
import subprocess
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import yaml

from common.validation import TASK_COLUMNS
from initialization.post_tasks import InitializationSummary
from common.persistence import TaskValidationError
from tests.support import tasks


PROJECT_ROOT = Path(__file__).resolve().parents[2]
INITIALIZER_SOURCE = PROJECT_ROOT / "entrypoints" / "initialize_tasks.py"


def load_entrypoint():
    spec = importlib.util.spec_from_file_location(
        "initialization_entrypoint_under_test", str(INITIALIZER_SOURCE)
    )
    module = importlib.util.module_from_spec(spec)
    # 原接入函数在没有 xdata 的环境中也应能被导入。
    with patch.dict(sys.modules, {"xdata": None, "xdata.client": None}):
        spec.loader.exec_module(module)
    return module


class InitializationEntryPointTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(
            prefix=".test-initialization-", dir=str(PROJECT_ROOT)
        )
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "entrypoints").mkdir()
        (self.root / "configs").mkdir()
        self.job_path = self.root / "configs" / "job_config.yaml"
        self.settings = {
            "source_path": "all-posts",
            "task_path": "new-task-table",
            "ai_config_path": "does-not-exist.yaml",
            "start_time": "not-a-date",
            "end_time": "also-not-a-date",
            "logging": {"level": "CRITICAL"},
        }
        self.write_job(self.settings)
        self.module = load_entrypoint()
        self.module.__file__ = str(self.root / "entrypoints" / "initialize_tasks.py")
        self.source = tasks([
            pd.Timestamp("2020-12-31 23:59:59.123456789", tz="Asia/Shanghai"),
            pd.Timestamp("2023-03-01", tz="Asia/Shanghai"),
        ])[["text", "user_name"]]
        self.cli = Mock(spec=["read", "register", "write"])
        self.registered_paths = set()
        self.cli.register.side_effect = lambda path, data_type, extra: self.registered_paths.add(path)

        def write_registered(path, frame):
            self.assertIn(path, self.registered_paths)

        self.cli.write.side_effect = write_registered
        self.get_client = Mock(return_value=self.cli)
        package = types.ModuleType("xdata")
        package.__path__ = []
        client_module = types.ModuleType("xdata.client")
        client_module.get_client = self.get_client
        package.client = client_module
        client_patch = patch.dict(
            sys.modules, {"xdata": package, "xdata.client": client_module}
        )
        client_patch.start()
        self.addCleanup(client_patch.stop)

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

    def write_job(self, settings):
        self.job_path.write_text(yaml.safe_dump(settings), encoding="utf-8")

    def assert_complete_write(self):
        self.cli.register.assert_called_once_with(self.settings["task_path"], "timeseries", "")
        self.cli.write.assert_called_once()
        self.assertEqual([call[0] for call in self.cli.mock_calls], ["read", "register", "write"])
        path, frame = self.cli.write.call_args.args
        self.assertEqual(path, self.settings["task_path"])
        self.assertEqual(list(frame.columns), TASK_COLUMNS)
        pd.testing.assert_index_equal(frame.index, self.source.index)
        pd.testing.assert_frame_equal(frame[["text", "user_name"]], self.source)
        self.assertEqual(list(self.source.columns), ["text", "user_name"])
        self.assertEqual(list(frame["status"]), ["0"] * len(self.source))
        for field in set(TASK_COLUMNS) - {"text", "user_name", "status"}:
            self.assertTrue(frame[field].eq("").all(), field)

    def test_import_does_not_read_config_or_require_xdata(self):
        with patch("yaml.safe_load") as read_config:
            load_entrypoint()
        read_config.assert_not_called()
        self.get_client.assert_not_called()
        self.cli.read.assert_not_called()
        self.cli.register.assert_not_called()
        self.cli.write.assert_not_called()

    def test_main_reads_all_source_once_and_preserves_index(self):
        other_directory = self.root / "other-directory"
        other_directory.mkdir()
        original_directory = Path.cwd()
        self.addCleanup(os.chdir, original_directory)
        os.chdir(other_directory)
        io_threads = []

        def read(path):
            io_threads.append(threading.get_ident())
            self.assertEqual(path, self.settings["source_path"])
            return self.source

        self.cli.read.side_effect = read

        def register(path, data_type, extra):
            io_threads.append(threading.get_ident())
            self.registered_paths.add(path)

        def write(path, frame):
            self.assertIn(path, self.registered_paths)
            io_threads.append(threading.get_ident())

        self.cli.register.side_effect = register
        self.cli.write.side_effect = write
        with patch("logging.basicConfig", wraps=logging.basicConfig) as configure_logging:
            result = self.module.main()
        configure_logging.assert_called_once_with(**self.settings["logging"])
        self.assertEqual(result, InitializationSummary(total=2, written=2))
        self.get_client.assert_called_once_with()
        self.cli.read.assert_called_once_with(self.settings["source_path"])
        self.assert_complete_write()
        self.assertEqual(io_threads, [threading.get_ident()] * 3)

    def test_empty_source_creates_complete_empty_table(self):
        self.source = tasks([])[["text", "user_name"]]
        self.cli.read.return_value = self.source
        result = self.module.main()
        self.assertEqual(result, InitializationSummary(total=0, written=0))
        self.get_client.assert_called_once_with()
        self.cli.read.assert_called_once_with(self.settings["source_path"])
        self.assert_complete_write()

    def test_invalid_settings_fail_before_client_import(self):
        invalid = [None, [], "invalid"]
        for field in ("source_path", "task_path"):
            for value in (None, "", " ", 42):
                invalid.append(dict(self.settings, **{field: value}))
            missing = dict(self.settings)
            del missing[field]
            invalid.append(missing)
        invalid.append(dict(self.settings, task_path=self.settings["source_path"]))
        invalid.append(dict(self.settings, logging=[]))
        invalid.append(dict(self.settings, logging={"level": "INVALID_LEVEL"}))
        for settings in invalid:
            with self.subTest(settings=settings):
                self.write_job(settings)
                with patch.dict(sys.modules, {"xdata": None, "xdata.client": None}):
                    with self.assertRaises(ValueError):
                        self.module.main()
                self.get_client.assert_not_called()
                self.cli.read.assert_not_called()
                self.cli.register.assert_not_called()
                self.cli.write.assert_not_called()

    def test_missing_or_unreadable_config_does_not_create_client(self):
        self.job_path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.module.main()
        for raw, error_type in ((b"\xff", UnicodeError), (b"task_path: [", yaml.YAMLError)):
            self.job_path.write_bytes(raw)
            with self.assertRaises(error_type):
                self.module.main()
        self.get_client.assert_not_called()
        self.cli.read.assert_not_called()
        self.cli.register.assert_not_called()
        self.cli.write.assert_not_called()

    def test_client_failure_is_not_retried(self):
        error = ConnectionError("client failed")
        self.get_client.side_effect = error
        with self.assertRaises(ConnectionError) as captured:
            self.module.main()
        self.assertIs(captured.exception, error)
        self.get_client.assert_called_once_with()
        self.cli.read.assert_not_called()
        self.cli.register.assert_not_called()
        self.cli.write.assert_not_called()

    def test_source_read_failure_is_not_retried(self):
        error = OSError("source failed")
        self.cli.read.side_effect = error
        with self.assertRaises(OSError) as captured:
            self.module.main()
        self.assertIs(captured.exception, error)
        self.cli.read.assert_called_once_with(self.settings["source_path"])
        self.cli.register.assert_not_called()
        self.cli.write.assert_not_called()

    def test_invalid_source_is_never_written(self):
        invalid_index = pd.DataFrame({
            "text": ["no original index"], "user_name": ["user"],
        })
        for source in (invalid_index, None, self.source.assign(user_name=[42, "valid"])):
            with self.subTest(source_type=type(source).__name__):
                self.cli.read.reset_mock()
                self.cli.read.return_value = source
                with self.assertRaises(TaskValidationError):
                    self.module.main()
                self.cli.read.assert_called_once_with(self.settings["source_path"])
                self.cli.register.assert_not_called()
                self.cli.write.assert_not_called()

    def test_missing_source_user_name_is_rejected_without_fallback(self):
        for source in (
            self.source.drop(columns="user_name"),
            self.source.rename(columns={"user_name": "username"}),
        ):
            with self.subTest(columns=list(source.columns)):
                self.cli.read.reset_mock()
                self.cli.read.return_value = source
                with self.assertRaises(TaskValidationError) as captured:
                    self.module.main()
                self.assertEqual(str(captured.exception), "任务缺少必要列")
                self.cli.read.assert_called_once_with(self.settings["source_path"])
                self.cli.register.assert_not_called()
                self.cli.write.assert_not_called()

    def test_extra_username_column_is_ignored_without_mutating_source(self):
        source = self.source.assign(username="different user")
        original = source.copy(deep=True)
        self.cli.read.return_value = source
        result = self.module.main()
        self.assertEqual(result, InitializationSummary(total=2, written=2))
        pd.testing.assert_frame_equal(source, original)
        self.cli.read.assert_called_once_with(self.settings["source_path"])
        self.cli.register.assert_called_once_with(self.settings["task_path"], "timeseries", "")
        self.cli.write.assert_called_once()
        path, frame = self.cli.write.call_args.args
        self.assertEqual(path, self.settings["task_path"])
        self.assertNotIn("username", frame.columns)
        pd.testing.assert_frame_equal(frame[["text", "user_name"]], self.source)

    def test_duplicate_source_columns_are_rejected_without_mutating_source(self):
        source = self.source[["text", "user_name", "user_name"]]
        original = source.copy(deep=True)
        self.cli.read.return_value = source
        with self.assertRaisesRegex(TaskValidationError, "索引和列名必须唯一"):
            self.module.main()
        pd.testing.assert_frame_equal(source, original)
        self.cli.read.assert_called_once_with(self.settings["source_path"])
        self.cli.register.assert_not_called()
        self.cli.write.assert_not_called()

    def test_registration_failure_preserves_exception_and_does_not_write_or_retry(self):
        for error in (ConnectionError("registration failed"), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                self.cli.reset_mock()
                self.get_client.reset_mock()
                self.cli.read.return_value = self.source
                self.cli.register.side_effect = error
                with self.assertRaises(type(error)) as captured:
                    self.module.main()
                self.assertIs(captured.exception, error)
                self.get_client.assert_called_once_with()
                self.cli.read.assert_called_once_with(self.settings["source_path"])
                self.cli.register.assert_called_once_with(self.settings["task_path"], "timeseries", "")
                self.cli.write.assert_not_called()
                self.assertFalse(self.registered_paths)

    def test_write_failure_preserves_exception_and_does_not_retry(self):
        error = OSError("write may have partially completed")
        self.cli.read.return_value = self.source
        self.cli.write.side_effect = error
        with self.assertRaises(OSError) as captured:
            self.module.main()
        self.assertIs(captured.exception, error)
        self.get_client.assert_called_once_with()
        self.cli.read.assert_called_once_with(self.settings["source_path"])
        self.assert_complete_write()

    def test_adapter_accepts_caller_client_without_xdata_or_logging_setup(self):
        self.cli.read.return_value = self.source
        core_initialize = self.module.initialize_tasks
        constructed_frames = []

        def initialize_with_tracked_output(source, write_tasks):
            def track_output(frame):
                constructed_frames.append(frame)
                return write_tasks(frame)

            return core_initialize(source, track_output)

        with patch.dict(sys.modules, {"xdata": None, "xdata.client": None}), \
                patch("logging.basicConfig") as configure_logging, \
                patch.object(self.module, "initialize_tasks", side_effect=initialize_with_tracked_output) as initialize:
            result = self.module.initialize_existing_posts(self.cli, self.settings)
        self.assertEqual(result, InitializationSummary(total=2, written=2))
        self.get_client.assert_not_called()
        configure_logging.assert_not_called()
        self.cli.read.assert_called_once_with(self.settings["source_path"])
        initialize.assert_called_once()
        self.assertIs(initialize.call_args.args[0], self.source)
        self.assert_complete_write()
        self.assertEqual(len(constructed_frames), 1)
        self.assertIs(self.cli.write.call_args.args[1], constructed_frames[0])

    def run_module(self, mode):
        script = self.root / "entrypoints" / "initialize_tasks.py"
        script.write_text(INITIALIZER_SOURCE.read_text(encoding="utf-8"), encoding="utf-8")
        (script.parent / "runtime.py").write_text(
            (PROJECT_ROOT / "entrypoints" / "runtime.py").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (script.parent / "__init__.py").write_text("", encoding="utf-8")
        bootstrap = """
import runpy
import sys
import types
import pandas as pd

root, project, mode = sys.argv[1:4]
sys.path[:0] = [root, project]

class Client:
    def __init__(self):
        self.registered_paths = set()

    def read(self, path):
        if mode == 'read_error':
            raise ConnectionError('模拟源读取失败')
        if path == 'all-posts':
            source = pd.DataFrame(
                {'text': ['private-post-for-test'],
                 'user_name': ['private-user-for-test']},
                index=pd.MultiIndex.from_arrays(
                    [pd.DatetimeIndex(['2020-01-01']), ['one']], names=['date', 'id'],
                ),
            )
            if mode == 'invalid_source':
                source.index = pd.RangeIndex(len(source))
            if mode == 'missing_field':
                source = source.drop(columns='user_name')
            return source
        raise AssertionError('unexpected data path')

    def register(self, path, data_type, extra):
        assert mode not in ('invalid_source', 'missing_field'), 'unexpected registration'
        assert path == 'new-task-table'
        assert data_type == 'timeseries' and extra == ''
        assert path not in self.registered_paths, 'registration was retried'
        if mode == 'register_error':
            raise ConnectionError('模拟目标注册失败')
        if mode == 'register_interrupt':
            raise KeyboardInterrupt()
        self.registered_paths.add(path)

    def write(self, path, frame):
        assert path in self.registered_paths, 'target is not registered'
        if mode == 'write_error':
            raise OSError('模拟任务写入失败')
        if mode == 'empty_error':
            raise OSError()
        if mode == 'whitespace_error':
            raise OSError('   ')
        if mode == 'interrupt':
            raise KeyboardInterrupt()
        assert mode not in ('invalid_source', 'missing_field'), 'unexpected write'
        assert path == 'new-task-table'
        assert list(frame['status']) == ['0']
        assert list(frame['user_name']) == ['private-user-for-test']

def get_client():
    if mode == 'client_error':
        raise ConnectionError('模拟客户端连接失败')
    return Client()

if mode == 'missing_client':
    sys.modules['xdata'] = None
    sys.modules['xdata.client'] = None
else:
    package = types.ModuleType('xdata')
    package.__path__ = []
    client_module = types.ModuleType('xdata.client')
    client_module.get_client = get_client
    package.client = client_module
    sys.modules['xdata'] = package
    sys.modules['xdata.client'] = client_module

sys.argv = ['entrypoints.initialize_tasks']
runpy.run_module('entrypoints.initialize_tasks', run_name='__main__')
"""
        return subprocess.run(
            [sys.executable, "-X", "utf8", "-B", "-c", bootstrap,
             str(self.root), str(PROJECT_ROOT), mode],
            cwd=str(self.root), capture_output=True, text=True,
            encoding="utf-8", timeout=15,
        )

    def test_module_exit_codes_and_specific_failure_reasons(self):
        self.settings.pop("logging")
        self.write_job(self.settings)
        fallback = "异常未提供具体原因，请检查依赖、配置和数据连接"
        for mode, expected_code, expected_type, expected_reason in (
            ("normal", 0, None, None),
            ("missing_client", 1, "ModuleNotFoundError", "xdata.client"),
            ("client_error", 1, "ConnectionError", "模拟客户端连接失败"),
            ("read_error", 1, "ConnectionError", "模拟源读取失败"),
            ("register_error", 1, "ConnectionError", "模拟目标注册失败"),
            ("write_error", 1, "OSError", "模拟任务写入失败"),
            ("invalid_source", 1, "TaskValidationError", "索引必须为名称和顺序固定的 date/id MultiIndex"),
            ("missing_field", 1, "TaskValidationError", "任务缺少必要列"),
            ("empty_error", 1, "OSError", fallback),
            ("whitespace_error", 1, "OSError", fallback),
            ("interrupt", 130, None, None),
            ("register_interrupt", 130, None, None),
        ):
            with self.subTest(mode=mode):
                result = self.run_module(mode)
                output = result.stdout + result.stderr
                self.assertEqual(result.returncode, expected_code, result.stderr)
                self.assertNotIn("private-post-for-test", output)
                self.assertNotIn("private-user-for-test", output)
                self.assertNotIn("Traceback", output)
                if expected_code == 0:
                    self.assertIn("round1帖子事件提取表初始化完成", output)
                else:
                    self.assertNotIn("round1帖子事件提取表初始化完成", output)
                if expected_code == 1:
                    self.assertIn("初始化未完成", output)
                    self.assertIn("异常类型=" + expected_type, output)
                    self.assertIn("原因=", output)
                    self.assertIn(expected_reason, output)
                    if expected_reason != fallback:
                        self.assertNotIn(fallback, output)
                if expected_code == 130:
                    self.assertIn("初始化已中断", output)


if __name__ == "__main__":
    unittest.main()
