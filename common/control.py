"""可选的整批停止协议；同步执行器和异步宿主共用，不改变默认旧入口。"""
import logging
import threading
import time

logger = logging.getLogger(__name__)


class ExecutionStopped(RuntimeError):
    pass


class ExecutionControl:
    def __init__(self, timeout_seconds):
        self.deadline = time.monotonic() + timeout_seconds
        self.stopped = threading.Event()
        self.timed_out = False
        self._lock = threading.Lock()
        self._callbacks = {}

    @property
    def remaining(self):
        return max(0.0, self.deadline - time.monotonic())

    def request_stop(self, *, timed_out=False):
        with self._lock:
            self.timed_out = self.timed_out or timed_out
            if self.stopped.is_set():
                return
            self.stopped.set()
            callbacks = tuple(self._callbacks.values())
        # 回调只通知停止，不做 join；不能占用宿主事件循环，也不持锁调用外部代码。
        for callback in callbacks:
            try:
                callback()
            except Exception:
                logger.exception("停止通知失败，继续通知其他执行部件")

    def register(self, callback):
        token = object()
        with self._lock:
            self._callbacks[token] = callback
            stopped = self.stopped.is_set()
        if stopped:
            callback()

        def remove():
            with self._lock:
                self._callbacks.pop(token, None)
        return remove

    def check(self):
        if self.stopped.is_set():
            raise ExecutionStopped("整批任务已停止")
        if self.remaining <= 0:
            self.request_stop(timed_out=True)
            raise ExecutionStopped("整批计算预算已耗尽")
