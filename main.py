"""读取任务配置，创建数据客户端，并启动已有任务表的批量处理。"""

import logging
from pathlib import Path

import yaml
from xdata.client import get_client

from ai_batch import run_worker


DEFAULT_LOGGING = {
    "level": logging.INFO,
    "format": "%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
    "datefmt": "%Y-%m-%d %H:%M:%S",
}


def main():
    """创建一次 cli，返回既有 worker 的统计；异常由调用方处理。"""
    project_root = Path(__file__).resolve().parent
    settings = yaml.safe_load(
        (project_root / "job_config.yaml").read_text(encoding="utf-8")
    )
    if not isinstance(settings, dict):
        raise ValueError("任务配置必须为字典")
    for field in ("task_path", "ai_config_path"):
        value = settings.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError("任务配置的 {} 必须为非空字符串".format(field))
    if "logging" in settings and not isinstance(settings["logging"], dict):
        raise ValueError("任务配置的 logging 必须为字典")

    ai_config_path = Path(settings["ai_config_path"])
    if not ai_config_path.is_absolute():
        ai_config_path = project_root / ai_config_path
    ai_config_path = ai_config_path.resolve()
    if not ai_config_path.is_file():
        raise FileNotFoundError("AI 配置文件不存在")
    settings["ai_config_path"] = str(ai_config_path)

    logging.basicConfig(**settings.get("logging", DEFAULT_LOGGING))
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # 客户端始终在当前调用线程使用，整轮处理复用同一个对象。
    cli = get_client()
    return run_worker(
        cli, settings["task_path"], settings["ai_config_path"],
        start_time=settings.get("start_time"), end_time=settings.get("end_time"),
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception:
        logging.getLogger(__name__).exception("批量处理未完成")
        raise SystemExit(1) from None
