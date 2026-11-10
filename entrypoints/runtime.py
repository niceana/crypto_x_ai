"""新阶段入口共用的配置、日志和客户端创建；导入时不连接数据系统。"""

import logging
from contextlib import ExitStack
from pathlib import Path

from common.config import load_event_config
from common.process_lock import ProcessLockBusy, stage_guard


def run_entrypoint(operation, *, stage=None, config_path=None, config_loader=None):
    project_root = Path(__file__).absolute().parent.parent
    path = config_path if config_path is not None else project_root / "configs" / "event_processing_config.yaml"
    settings = (config_loader or load_event_config)(path)
    defaults = {"level": "INFO", "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"}
    logging.basicConfig(**(settings.logging or defaults))
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    with ExitStack() as stack:
        dates = {"start_time": settings.start_time, "end_time": settings.end_time}
        if stage == "import_events":
            # 与自动导入保持相同顺序，先锁第一轮来源，再锁导入。
            # 重复启动导入时不会暂时占用原第一轮即将使用的导入锁。
            stack.enter_context(stage_guard("round1", settings.first_round_task_path, **dates))
            stack.enter_context(stage_guard(stage, settings.result_path, **dates))
        elif stage is not None:
            stack.enter_context(stage_guard(stage, settings.result_path, **dates))
        from xdata.client import get_client
        return operation(get_client(), settings)


def execute(main):
    try:
        summary = main()
        logging.getLogger(__name__).info("当前阶段完成：%s", summary)
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except ProcessLockBusy as exc:
        logging.getLogger(__name__).warning("%s", exc)
        raise SystemExit(2) from None
    except Exception:
        logging.getLogger(__name__).exception("当前阶段未完成")
        raise SystemExit(1) from None


DEFAULT_LOGGING = {
    "level": logging.INFO,
    "format": "%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
    "datefmt": "%Y-%m-%d %H:%M:%S",
}
