"""使用独立进程验证本机内核锁；不访问数据库、AI 或真实处理任务。"""

import errno
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from common import process_lock
from common.process_lock import ProcessLockBusy, lock_directory, stage_guard


PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Windows venv 的 python.exe 可能是启动器；使用实际解释器，才能直接强杀持锁进程。
CHILD_PYTHON = getattr(sys, "_base_executable", sys.executable)
CHILD = """
import json
import sys
from pathlib import Path
from common.process_lock import ProcessLockBusy, stage_guard
request = json.loads(sys.argv[1])
try:
    with stage_guard(request['stage'], request['table'],
                     start_time=request.get('start'), end_time=request.get('end')):
        if len(sys.argv) > 2:
            Path(sys.argv[2]).write_text('ready', encoding='utf-8')
            sys.stdin.read(1)
        print('acquired')
except ProcessLockBusy as exc:
    print(str(exc))
    sys.exit(2)
"""


class ProcessLockTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix=".test-lock-", dir=str(PROJECT_ROOT))
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        environment = patch.dict(os.environ, {process_lock.LOCK_DIRECTORY_ENV: str(self.root / "locks")})
        environment.start()
        self.addCleanup(environment.stop)
        self.environment = dict(os.environ)
        self.environment["PYTHONPATH"] = str(PROJECT_ROOT)
        self.children = []
        self.addCleanup(self.stop_children)

    def stop_children(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=10)

    def holder(self, request):
        ready = self.root / ("ready-" + str(len(self.children)))
        child = subprocess.Popen([CHILD_PYTHON, "-X", "utf8", "-B", "-c", CHILD,
                                  json.dumps(request), str(ready)], cwd=str(self.root),
                                 env=self.environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True, encoding="utf-8")
        self.children.append(child)
        deadline = time.monotonic() + 10
        while not ready.exists():
            if child.poll() is not None:
                self.fail("持锁进程提前退出：" + child.communicate(timeout=10)[1])
            if time.monotonic() > deadline:
                self.fail("持锁进程启动超时")
            time.sleep(0.02)
        return child

    def probe(self, request, expected_code):
        completed = subprocess.run([CHILD_PYTHON, "-X", "utf8", "-B", "-c", CHILD,
                                    json.dumps(request)], cwd=str(self.root), env=self.environment,
                                   capture_output=True, text=True, encoding="utf-8", timeout=10)
        self.assertEqual(completed.returncode, expected_code, completed.stdout + completed.stderr)
        return completed.stdout

    def test_same_stage_table_blocks_another_process_even_with_different_dates(self):
        child = self.holder({"stage": "round1", "table": "帖子表",
                             "start": "2024-01-01", "end": "2024-01-31"})
        message = self.probe({"stage": "round1", "table": "帖子表",
                              "start": "2025-01-01"}, 2)
        self.assertIn("PID=" + str(child.pid), message)
        self.assertIn("2024-01-01~2024-01-31", message)
        self.assertIn("当前进程退出", message)
        self.assertIn(".lock", message)

    def test_each_stage_same_table_is_exclusive_but_distinct_stages_and_tables_can_run(self):
        for stage in ("round2", "round3", "vectors", "import_events", "weights"):
            with self.subTest(stage=stage), stage_guard(stage, "events"):
                self.probe({"stage": stage, "table": "events"}, 2)
                self.probe({"stage": stage + "-other", "table": "events"}, 0)
                self.probe({"stage": stage, "table": "other-events"}, 0)

    def test_round1_vectors_round2_and_round3_can_run_together(self):
        children = [self.holder({"stage": stage, "table": table})
                    for stage, table in (("round1", "posts"), ("vectors", "events"),
                                         ("round2", "events"), ("round3", "events"))]
        self.assertTrue(all(child.poll() is None for child in children))

    def test_force_kill_releases_lock_without_removing_lock_file(self):
        request = {"stage": "round2", "table": "events"}
        child = self.holder(request)
        paths = sorted(lock_directory().glob("*.lock"))
        self.assertEqual(len(paths), 1)
        child.kill()
        child.communicate(timeout=10)
        self.assertEqual(sorted(lock_directory().glob("*.lock")), paths)
        self.probe(request, 0)

    def test_exception_releases_lock_and_duplicate_does_not_unlock_original(self):
        with self.assertRaisesRegex(RuntimeError, "worker failed"):
            with stage_guard("round2", "events"):
                raise RuntimeError("worker failed")
        with stage_guard("round2", "events"):
            with self.assertRaises(ProcessLockBusy):
                with stage_guard("round2", "events"):
                    self.fail("重复锁应失败")
            self.probe({"stage": "round2", "table": "events"}, 2)
        with stage_guard("round2", "events"):
            pass

    def test_lock_directory_is_independent_of_working_directory_and_stale_metadata(self):
        with stage_guard("round2", "events"):
            path = process_lock._lock_path("stage:round2", "events")
            with path.open("r+b", buffering=0) as handle:
                handle.seek(1)
                handle.write(b"not valid metadata")
                handle.truncate()
            with self.assertRaises(ProcessLockBusy):
                with stage_guard("round2", "events"):
                    pass
        self.probe({"stage": "round2", "table": "events"}, 0)

    def test_invalid_lock_directory_or_unwritable_location_stops_instead_of_running_unlocked(self):
        for value in ("", "relative-directory"):
            with patch.dict(os.environ, {process_lock.LOCK_DIRECTORY_ENV: value}):
                with self.assertRaises(ValueError):
                    with stage_guard("round1", "posts"):
                        self.fail("不能无锁运行")
        with patch.object(process_lock.os, "open", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                with stage_guard("round1", "posts"):
                    self.fail("不能无锁运行")

    def test_linux_backend_uses_nonblocking_exclusive_and_unlock_flags(self):
        fake = types.ModuleType("fcntl")
        fake.LOCK_NB, fake.LOCK_EX, fake.LOCK_UN = 4, 2, 8
        fake.flock = Mock()
        spec = importlib.util.spec_from_file_location("lock_posix_test", process_lock.__file__)
        module = importlib.util.module_from_spec(spec)
        with patch.object(os, "name", "posix"), patch.dict(sys.modules, {"fcntl": fake}):
            spec.loader.exec_module(module)
        handle = Mock()
        handle.fileno.return_value = 42
        module._acquire(handle)
        module._release(handle, None)
        self.assertEqual([call.args for call in fake.flock.call_args_list], [(42, 6), (42, 8)])
        fake.flock.side_effect = BlockingIOError(errno.EAGAIN, "busy")
        with self.assertRaises(BlockingIOError):
            module._acquire(handle)


if __name__ == "__main__":
    unittest.main()
