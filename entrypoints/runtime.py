"""新阶段入口共用的配置、日志和客户端创建；导入时不连接数据系统。"""

import logging
from pathlib import Path

from common.config import load_event_config


def run_entrypoint(operation):
    project_root = Path(__file__).absolute().parent.parent
    settings = load_event_config(project_root / "configs" / "event_processing_config.yaml")
    defaults = {"level": "INFO", "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"}
    logging.basicConfig(**(settings.logging or defaults))
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    from xdata.client import get_client
    return operation(get_client(), settings)


def execute(main):
    try:
        summary = main()
        logging.getLogger(__name__).info("当前阶段完成：%s", summary)
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception:
        logging.getLogger(__name__).exception("当前阶段未完成")
        raise SystemExit(1) from None


DEFAULT_LOGGING = {
    "level": logging.INFO,
    "format": "%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
    "datefmt": "%Y-%m-%d %H:%M:%S",
}
