"""独立入口的本机进程锁；内核锁定文件，文件存在与否不代表占用。"""

import errno
import hashlib
import json
import logging
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


logger = logging.getLogger(__name__)
LOCK_DIRECTORY_ENV = "CRYPTO_X_AI_LOCK_DIR"


class ProcessLockBusy(RuntimeError):
    """同阶段占用目标；新启动的入口应立即退出。"""


if os.name == "nt":
    import ctypes
    import msvcrt
    from ctypes import wintypes

    class _Overlapped(ctypes.Structure):
        _fields_ = [("Internal", ctypes.c_size_t), ("InternalHigh", ctypes.c_size_t),
                    ("Offset", wintypes.DWORD), ("OffsetHigh", wintypes.DWORD),
                    ("hEvent", wintypes.HANDLE)]

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel32.LockFileEx.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD,
                                    wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(_Overlapped)]
    _kernel32.LockFileEx.restype = wintypes.BOOL
    _kernel32.UnlockFileEx.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.DWORD,
                                      wintypes.DWORD, ctypes.POINTER(_Overlapped)]
    _kernel32.UnlockFileEx.restype = wintypes.BOOL

    def _acquire(handle):
        overlapped = _Overlapped()
        # 只锁第 0 字节；第 1 字节起的诊断信息仍可由其他进程读取。
        flags = 0x00000001 | 0x00000002
        if not _kernel32.LockFileEx(msvcrt.get_osfhandle(handle.fileno()), flags, 0, 1, 0,
                                    ctypes.byref(overlapped)):
            error = ctypes.get_last_error()
            if error == 33:  # ERROR_LOCK_VIOLATION
                raise BlockingIOError(errno.EAGAIN, "process lock is busy")
            raise ctypes.WinError(error)
        return overlapped

    def _release(handle, token):
        if not _kernel32.UnlockFileEx(msvcrt.get_osfhandle(handle.fileno()), 0, 1, 0,
                                      ctypes.byref(token)):
            raise ctypes.WinError(ctypes.get_last_error())

else:
    import fcntl

    def _acquire(handle):
        fcntl.flock(handle.fileno(), fcntl.LOCK_NB | fcntl.LOCK_EX)

    def _release(handle, token):
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def lock_directory():
    """跨工作目录及代码副本保持一致；显式覆盖时要求绝对路径。"""
    configured = os.environ.get(LOCK_DIRECTORY_ENV)
    if configured is not None:
        location = Path(configured).expanduser()
        if not configured.strip() or not location.is_absolute():
            raise ValueError("{} 必须为所有处理入口共用的绝对目录".format(LOCK_DIRECTORY_ENV))
    elif os.name == "nt":
        location = Path(os.environ.get("LOCALAPPDATA", tempfile.gettempdir())) / "crypto_x_ai" / "locks"
    else:
        location = Path("/tmp/crypto_x_ai_locks")
    return location.resolve()


def _lock_path(namespace, table_path):
    if not isinstance(table_path, str) or not table_path.strip():
        raise ValueError("进程锁的目标表路径必须为非空字符串")
    # 数据库路径按原值区分，不转成本地路径，不将日期、配置文件或并发加入身份。
    identity = json.dumps([namespace, table_path], ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return lock_directory() / (digest + ".lock")


def _owner_text(handle):
    try:
        handle.seek(1)
        owner = json.loads(handle.read(16384).decode("utf-8"))
        return "；占用信息：阶段={stage}，PID={pid}，启动={started_at}，日期={start_time}~{end_time}".format(**owner)
    except (OSError, ValueError, KeyError, TypeError, UnicodeError):
        return "；占用进程信息暂不可读"


@contextmanager
def _file_lock(namespace, table_path, *, stage, start_time=None, end_time=None):
    path = _lock_path(namespace, table_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # 不截断已有文件，不删除锁文件；删除会使两个进程锁到不同 inode。
    descriptor = os.open(str(path), os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0), 0o600)
    with os.fdopen(descriptor, "r+b", buffering=0) as handle:
        try:
            token = _acquire(handle)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
            owner = _owner_text(handle)
            raise ProcessLockBusy(
                "无法启动 {}：同一阶段已有处理进程，当前进程退出；表={}{}；锁文件={}".format(
                    stage, table_path, owner, path)) from None
        try:
            metadata = {"stage": stage, "table_path": table_path, "pid": os.getpid(),
                        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        "start_time": start_time, "end_time": end_time}
            handle.seek(0)
            handle.write(b"\0" + json.dumps(metadata, ensure_ascii=False).encode("utf-8"))
            handle.truncate()
            yield
        finally:
            _release(handle, token)


@contextmanager
def stage_guard(stage, table_path, *, start_time=None, end_time=None):
    """同阶段同表互斥；不同阶段可按各自速度运行。"""
    with _file_lock("stage:" + stage, table_path, stage=stage, start_time=start_time, end_time=end_time):
        logger.info("进程锁已获取：阶段=%s，表=%s，PID=%d，日期=%s~%s",
                    stage, table_path, os.getpid(), start_time, end_time)
        yield
