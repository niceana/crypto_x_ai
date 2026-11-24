"""验证脚本日志在工作线程、重复初始化和文件追加时的实际输出。"""
from contextlib import redirect_stdout
import io
import logging
from pathlib import Path
import sys
import tempfile
import threading
import unittest

WORK = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORK / "tools/live_trial"))
import _trial


class TrialLoggingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="trial-logging-")
        self.addCleanup(self.tmp.cleanup)
        self.root = logging.getLogger()
        self.saved_handlers, self.saved_level = list(self.root.handlers), self.root.level
        self.root.handlers = []
        self.external_levels = {name: logging.getLogger(name).level for name in ("httpx", "httpcore")}
        self.addCleanup(self.restore_logging)
        self.filename = Path(self.tmp.name) / "run.log"

    def restore_logging(self):
        for handler in list(self.root.handlers):
            self.root.removeHandler(handler)
            handler.close()
        self.root.handlers = self.saved_handlers
        self.root.setLevel(self.saved_level)
        for name, level in self.external_levels.items():
            logging.getLogger(name).setLevel(level)
        _trial.set_log_batch()

    def test_source_info_from_worker_is_on_console_and_utf8_file_with_batch(self):
        output = io.StringIO()
        with redirect_stdout(output):
            _trial.init_logging("INFO")
            first = _trial.add_log_file(self.filename)
            self.assertIs(first, _trial.add_log_file(self.filename))
            _trial.set_log_batch("2021-03-20 16:17:00")
            source = logging.getLogger("crypto_x_ai_live._engine.round1.runner")
            thread = threading.Thread(target=lambda: source.info("第一轮原日志：中文成功"), name="network-test")
            thread.start()
            thread.join()
            source.debug("INFO模式不显示的详细记录")
        content = self.filename.read_text(encoding="utf-8")
        for text in ("第一轮原日志：中文成功", "crypto_x_ai_live._engine.round1.runner", "network-test", "batch=2021-03-20 16:17:00", "UTC+8"):
            self.assertIn(text, content)
            self.assertIn(text, output.getvalue())
        self.assertEqual(content.count("第一轮原日志：中文成功"), 1)
        self.assertNotIn("INFO模式不显示的详细记录", content)

    def test_reinitialize_enables_debug_and_appends_without_duplicate_handlers(self):
        output = io.StringIO()
        with redirect_stdout(output):
            _trial.init_logging("INFO")
            _trial.add_log_file(self.filename)
            source = logging.getLogger("crypto_x_ai_live._engine.common.network")
            source.info("第一次运行")
            _trial.init_logging("DEBUG")
            _trial.add_log_file(self.filename)
            _trial.set_log_batch("2021-03-20 16:32:00")
            source.debug("第二次运行的详细请求日志")
            logging.getLogger("httpx").info("第三方HTTP信息不显示")
            try:
                raise ValueError("模拟异常")
            except ValueError:
                source.exception("保留完整异常堆栈")
        content = self.filename.read_text(encoding="utf-8")
        self.assertEqual(content.count("第一次运行"), 1)
        self.assertEqual(content.count("第二次运行的详细请求日志"), 1)
        self.assertIn("batch=2021-03-20 16:32:00", content)
        self.assertIn("ValueError: 模拟异常", content)
        self.assertNotIn("第三方HTTP信息不显示", content)
        self.assertEqual(len([handler for handler in self.root.handlers if isinstance(handler, logging.FileHandler)]), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
