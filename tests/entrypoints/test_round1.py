import importlib.util
import logging
import os
import runpy
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import httpx
import pandas as pd
import yaml

from tests.support import (
    FakeCli,
    Fixture,
    event_success as success,
    tasks,
    request_text,
    success as raw_success,
)
from round1.runner import ExtractionSummary
from common.config import ConfigError, ai_config_hash
from common.persistence import BatchPersistenceError
from initialization.event_records import import_event_records_from_posts
from tests.event_support import Tables


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAIN_SOURCE = PROJECT_ROOT / "entrypoints" / "run_round1.py"


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
        (self.root / "configs").mkdir()
        ai_path = self.root / "configs" / "round1_ai_config.yaml"
        self.path.rename(ai_path)
        self.path = ai_path
        self.job_path = self.root / "configs" / "job_config.yaml"
        self.settings = {
            "source_path": "source-must-not-be-read",
            "task_path": "existing-task-table",
            "ai_config_path": "round1_ai_config.yaml",
            "start_time": "2021-01-01",
            "end_time": "2021-12-31",
            "logging": {"level": "CRITICAL"},
        }
        self.write_job(self.settings)
        self.event_path = self.root / "configs" / "event_config.yaml"
        self.event_settings = {
            "first_round_task_path": "ignored-source-path",
            "result_path": "event-result-table",
            "round2_ai_config_path": "round2_ai_config.yaml",
            "round3_ai_config_path": "round3_ai_config.yaml",
            "start_time": "1990-01-01", "end_time": "1990-01-02",
            "initialize_mode": "create", "user_name_to_tickers": {},
        }
        self.event_path.write_text(yaml.safe_dump(self.event_settings), encoding="utf-8")
        self.module, self.get_client = load_entrypoint()
        self.module.__file__ = str(self.root / "entrypoints" / "run_round1.py")
        importer = patch.object(self.module, "import_event_records_from_posts", return_value=1)
        self.import_events = importer.start()
        self.addCleanup(importer.stop)

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

    def test_duplicate_job_or_event_keys_stop_before_client_creation(self):
        for path, suffix in (
            (self.job_path, "\ntask_path: wrong-table\n"),
            (self.event_path, "\nuser_name_to_tickers:\n  account: [BTC]\n  account: [ETH]\n"),
        ):
            with self.subTest(config=path.name):
                original = path.read_text(encoding="utf-8")
                path.write_text(original + suffix, encoding="utf-8")
                try:
                    with self.assertRaisesRegex(ConfigError, "重复键"):
                        self.module.main()
                    self.get_client.assert_not_called()
                    self.import_events.assert_not_called()
                finally:
                    path.write_text(original, encoding="utf-8")

    def test_import_does_not_create_client(self):
        self.get_client.assert_not_called()

    def test_root_launcher_preserves_calls_error_logs_and_exit_codes(self):
        """运行真实根入口，检查转调后没有丢失执行、异常日志或中断退出。"""
        for mode, error, expected_code in (
            ("normal", None, 0),
            ("failure", RuntimeError("模拟第一轮失败"), 1),
            ("interrupt", KeyboardInterrupt(), 130),
        ):
            with self.subTest(mode=mode):
                self.get_client.reset_mock()
                self.import_events.reset_mock()
                with patch.dict(sys.modules, {"entrypoints.run_round1": self.module}), \
                        patch.object(self.module, "run_extraction", return_value=ExtractionSummary(),
                                     side_effect=error) as worker:
                    if expected_code == 0:
                        namespace = runpy.run_path(str(PROJECT_ROOT / "main.py"), run_name="__main__")
                        self.assertIs(namespace["main"], self.module.main)
                    elif expected_code == 1:
                        with self.assertLogs("__main__", level="ERROR") as captured:
                            with self.assertRaises(SystemExit) as caught:
                                runpy.run_path(str(PROJECT_ROOT / "main.py"), run_name="__main__")
                        self.assertEqual(caught.exception.code, expected_code)
                        self.assertIn("round1事件提取未完成", captured.output[0])
                        self.assertIn("模拟第一轮失败", captured.output[0])
                        self.assertIsNotNone(captured.records[0].exc_info)
                    else:
                        with self.assertRaises(SystemExit) as caught:
                            runpy.run_path(str(PROJECT_ROOT / "main.py"), run_name="__main__")
                        self.assertEqual(caught.exception.code, expected_code)
                self.get_client.assert_called_once_with()
                worker.assert_called_once()
                self.import_events.assert_not_called()

    def test_run_uses_script_root_and_one_client(self):
        other_directory = self.root / "other-directory"
        other_directory.mkdir()
        original_directory = Path.cwd()
        self.addCleanup(os.chdir, original_directory)
        os.chdir(other_directory)
        result = ExtractionSummary(total=1, new_succeeded=1)
        with patch.object(self.module, "run_extraction", return_value=result) as run:
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
            "on_successful_posts": run.call_args.kwargs["on_successful_posts"],
        })
        self.assertTrue(callable(run.call_args.kwargs["on_successful_posts"]))
        self.import_events.assert_called_once()
        import_cli, import_settings = self.import_events.call_args.args
        self.assertIs(import_cli, client)
        self.assertEqual(import_settings.first_round_task_path, task_path)
        self.assertEqual((import_settings.start_time, import_settings.end_time),
                         (self.settings["start_time"], self.settings["end_time"]))
        self.assertEqual(import_settings.result_path, self.event_settings["result_path"])
        self.assertEqual(import_settings.initialize_mode, self.event_settings["initialize_mode"])
        self.assertEqual(self.import_events.call_args.kwargs, {"new_posts": None, "previous_posts": None})

    def test_absolute_ai_path_is_supported(self):
        self.settings["ai_config_path"] = str(self.path.resolve())
        self.write_job(self.settings)
        with patch.object(self.module, "run_extraction", return_value=ExtractionSummary(new_succeeded=1)) as run:
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
        with patch.object(self.module, "run_extraction") as run:
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
        with patch.object(self.module, "run_extraction") as run:
            with self.assertRaises(ConnectionError) as captured:
                self.module.main()
        self.assertIs(captured.exception, error)
        self.get_client.assert_called_once_with()
        run.assert_not_called()

    def test_worker_failure_preserves_exception(self):
        error = RuntimeError("worker failed")
        error.unsaved_results = [object()]
        with patch.object(self.module, "run_extraction", side_effect=error) as run:
            with self.assertRaises(RuntimeError) as captured:
                self.module.main()
        self.assertIs(captured.exception, error)
        self.assertIs(captured.exception.unsaved_results, error.unsaved_results)
        self.get_client.assert_called_once_with()
        run.assert_called_once()
        self.import_events.assert_not_called()

    def test_no_successful_posts_do_not_read_or_create_event_table(self):
        for summary in (ExtractionSummary(), ExtractionSummary(total=2, new_failed=2)):
            with self.subTest(total=summary.total):
                with patch.object(self.module, "run_extraction", return_value=summary):
                    self.assertIs(self.module.main(), summary)
                self.import_events.assert_not_called()

    def test_auto_import_runs_after_worker_even_with_some_failed_posts(self):
        order = []
        summary = ExtractionSummary(total=2, new_succeeded=1, new_failed=1)
        def run(*args, **kwargs):
            order.append("first_saved")
            return summary
        def append(*args, **kwargs):
            order.append("import_successes")
            return 1
        self.import_events.side_effect = append
        with patch.object(self.module, "run_extraction", side_effect=run):
            self.assertIs(self.module.main(), summary)
        self.assertEqual(order, ["first_saved", "import_successes"])

    def test_import_failure_is_propagated_and_saved_successes_are_imported_on_next_run(self):
        error = ConnectionError("event import failed")
        self.import_events.side_effect = [error, 1]
        fresh = ExtractionSummary(total=1, new_succeeded=1)
        resumed = ExtractionSummary(total=1, already_succeeded=1)
        with patch.object(self.module, "run_extraction", side_effect=[fresh, resumed]) as run:
            with self.assertRaises(ConnectionError) as caught:
                self.module.main()
            self.assertIs(caught.exception, error)
            self.assertIs(self.module.main(), resumed)
        self.assertEqual(run.call_count, 2)
        self.assertEqual(self.import_events.call_count, 2)

    def test_event_config_missing_or_target_equal_to_source_stops_before_first_round(self):
        self.event_path.unlink()
        with patch.object(self.module, "run_extraction") as run:
            with self.assertRaises(FileNotFoundError):
                self.module.main()
            self.event_settings["result_path"] = self.settings["task_path"]
            self.event_path.write_text(yaml.safe_dump(self.event_settings), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "路径必须不同"):
                self.module.main()
        self.get_client.assert_not_called()
        self.import_events.assert_not_called()
        run.assert_not_called()

    def test_main_creates_event_tasks_and_imports_failed_posts_after_retry_without_repeating_ai(self):
        self.settings["task_path"] = "first"
        self.write_job(self.settings)
        self.event_settings["result_path"] = "result"
        self.event_path.write_text(yaml.safe_dump(self.event_settings), encoding="utf-8")
        backend = Tables(tasks(["2021-01-15", "2021-02-15"]))
        self.get_client.return_value = backend
        calls, fail_second = [], [True]
        def handler(request):
            text = request_text(request)
            calls.append(text)
            if text == "post 1" and fail_second[0]:
                return raw_success("{}")
            return success(text)
        def create(config):
            return httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
        with patch.object(self.module, "import_event_records_from_posts", wraps=import_event_records_from_posts), \
                patch("round1.runner._create_client", side_effect=create):
            first = self.module.main()
            self.assertEqual((first.new_succeeded, first.new_failed), (1, 1))
            self.assertFalse(any(path == "result" for path, _, _ in backend.reads))
            self.assertEqual(backend.meta_reads, ["result"])
            frame = backend.tables["result"]
            self.assertEqual(list(frame["event"]), ["post 0"])
            key = frame.index[0]
            frame.loc[key, ["event_relation", "event_id", "round2_status", "round2_config_hash"]] = [
                "new", key[1], "1", ai_config_hash(self.config())]
            saved = frame.copy(deep=True)
            fail_second[0] = False
            resumed = self.module.main()
        self.assertEqual((resumed.already_succeeded, resumed.new_succeeded, resumed.new_failed), (1, 1, 0))
        self.assertEqual(calls.count("post 0"), 1)
        self.assertEqual(calls.count("post 1"), self.config().run.max_attempts + 1)
        self.assertEqual(list(backend.tables["result"]["event"]), ["post 0", "post 1"])
        pd.testing.assert_frame_equal(backend.tables["result"].loc[saved.index], saved)
        self.assertEqual((len(backend.registers), len(backend.writes), len(backend.appends)), (1, 1, 1))
        self.assertFalse(any(path == "ignored-source-path" for path, _, _ in backend.reads))
        self.assertFalse(any(path == "first" and end == "2021-12-31" and start == "2021-01-01"
                             for path, start, end in backend.reads))
        self.assertEqual(len([row for row in backend.reads if row[0] == "first"]), 24)
        self.assertEqual([row for row in backend.reads if row[0] == "result"], [("result", None, None)])
        self.assertEqual(backend.meta_reads, ["result"])

    def test_main_recovers_failed_event_write_without_repeating_successful_first_round_ai(self):
        self.settings["task_path"] = "first"
        self.write_job(self.settings)
        self.event_settings["result_path"] = "result"
        self.event_path.write_text(yaml.safe_dump(self.event_settings), encoding="utf-8")
        backend = Tables(tasks(["2021-01-15"]))
        self.get_client.return_value = backend
        calls = []
        def create(config):
            return httpx.AsyncClient(transport=httpx.MockTransport(
                lambda req: calls.append(request_text(req)) or success("saved")), trust_env=False)
        def fail_write(path, frame):
            raise ConnectionError("event write unavailable")
        backend.write_hook = fail_write
        with patch.object(self.module, "import_event_records_from_posts", wraps=import_event_records_from_posts), \
                patch("round1.runner._create_client", side_effect=create):
            with self.assertRaises(BatchPersistenceError):
                self.module.main()
            self.assertTrue(backend.tables["first"]["status"].eq("1").all())
            self.assertEqual(len(backend.writes), 3)
            backend.write_hook = None
            resumed = self.module.main()
        self.assertEqual((resumed.already_succeeded, resumed.http_attempts_started), (1, 0))
        self.assertEqual(calls, ["post 0"])
        self.assertEqual(list(backend.tables["result"]["event"]), ["saved"])
        self.assertEqual((len(backend.registers), len(backend.appends)), (1, 1))

    def test_new_disjoint_batches_never_read_first_results_again_or_read_old_event_rows(self):
        self.settings.update(task_path="first", start_time="2021-01-01", end_time="2021-01-31")
        self.write_job(self.settings)
        self.event_settings["result_path"] = "result"
        self.event_path.write_text(yaml.safe_dump(self.event_settings), encoding="utf-8")
        backend = Tables(tasks(["2021-01-15", "2021-02-15"]))
        self.get_client.return_value = backend
        def create(config):
            return httpx.AsyncClient(transport=httpx.MockTransport(
                lambda req: success(request_text(req))), trust_env=False)
        with patch.object(self.module, "import_event_records_from_posts", wraps=import_event_records_from_posts), \
                patch("round1.runner._create_client", side_effect=create):
            self.assertEqual(self.module.main().new_succeeded, 1)
            saved = backend.tables["result"].copy(deep=True)
            self.settings.update(start_time="2021-02-01", end_time="2021-02-28")
            self.write_job(self.settings)
            self.assertEqual(self.module.main().new_succeeded, 1)
        self.assertEqual(backend.reads, [("first", "2021-01-01", "2021-01-31"),
                                         ("first", "2021-02-01", "2021-02-28")])
        self.assertEqual(backend.meta_reads, ["result", "result"])
        self.assertEqual((len(backend.registers), len(backend.writes), len(backend.appends)), (1, 1, 1))
        pd.testing.assert_frame_equal(backend.tables["result"].loc[saved.index], saved)

    def test_first_round_interruption_does_not_export_collected_earlier_successes(self):
        error = KeyboardInterrupt()
        def run(*args, **kwargs):
            frame = tasks(["2021-01-15"])
            frame.loc[:, "status"] = "1"
            kwargs["on_successful_posts"](frame, frame.iloc[:0])
            raise error
        with patch.object(self.module, "run_extraction", side_effect=run):
            with self.assertRaises(KeyboardInterrupt):
                self.module.main()
        self.import_events.assert_not_called()

    def test_real_worker_uses_only_task_table(self):
        backend = FakeCli(tasks(["2021-01-15", "2021-02-15"]))
        self.get_client.return_value = backend

        def create_http_client(config):
            return httpx.AsyncClient(transport=httpx.MockTransport(lambda request: success()))

        with patch("round1.runner._create_client", side_effect=create_http_client):
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
        (self.root / "entrypoints").mkdir(exist_ok=True)
        script = self.root / "entrypoints" / "run_round1.py"
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
import round1.runner
from common.store import TaskValidationError
from round1.runner import ExtractionSummary
import initialization.event_records
def import_events(*args, **kwargs):
    if mode == 'import_error':
        raise RuntimeError('自动导入执行失败')
    return 1
initialization.event_records.import_event_records_from_posts = import_events
def run(client, task_path, config_path, start_time=None, end_time=None, *, on_successful_posts=None):
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
    return ExtractionSummary(total=1, new_succeeded=1)
round1.runner.run_extraction = run
runpy.run_path(sys.argv[2], run_name='__main__')
"""
        for mode, expected_code, reason in (
            ("normal", 0, None),
            ("client_error", 1, "数据库连接超时"),
            ("worker_error", 1, "worker 执行失败"),
            ("import_error", 1, "自动导入执行失败"),
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
                    self.assertIn("round1事件提取未完成", result.stderr)
                    self.assertIn(reason, result.stderr)
                    self.assertIn("Traceback", result.stderr)
                else:
                    self.assertNotIn("Traceback", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
