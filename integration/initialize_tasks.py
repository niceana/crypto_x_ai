"""接入现有 cli 或直接启动，一次性初始化全部现有帖子。"""

import logging
from pathlib import Path

import yaml

from ai_batch import initialize_tasks


DEFAULT_LOGGING = {
    "level": logging.INFO,
    "format": "%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
    "datefmt": "%Y-%m-%d %H:%M:%S",
}


def initialize_existing_posts(cli, settings):
    """完整读取源帖子，绑定目标注册与完整写入回调。"""
    source = cli.read(settings["source_path"])
    task_path = settings["task_path"]

    def write_tasks(df):
        cli.register(task_path, "timeseries", "")
        cli.write(task_path, df)

    return initialize_tasks(source, write_tasks)


def main():
    """读取任务配置，创建一次客户端，返回既有初始化函数的统计。"""
    project_root = Path(__file__).resolve().parent.parent
    settings = yaml.safe_load(
        (project_root / "job_config.yaml").read_text(encoding="utf-8")
    )
    if not isinstance(settings, dict):
        raise ValueError("任务配置必须为字典")
    for field in ("source_path", "task_path"):
        value = settings.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError("任务配置的 {} 必须为非空字符串".format(field))
    if settings["source_path"] == settings["task_path"]:
        raise ValueError("源帖子路径与任务表路径必须不同")
    if "logging" in settings and not isinstance(settings["logging"], dict):
        raise ValueError("任务配置的 logging 必须为字典")

    logging.basicConfig(**settings.get("logging", DEFAULT_LOGGING))

    # 仅独立启动依赖 xdata；已有应用可继续传入自己管理的客户端。
    from xdata.client import get_client

    cli = get_client()
    return initialize_existing_posts(cli, settings)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.getLogger(__name__).warning(
            "初始化已中断；如已开始写入，目标可能残留部分数据，须清空后重跑"
        )
        raise SystemExit(130) from None
    except Exception as error:
        reason = str(error).strip() or "异常未提供具体原因，请检查依赖、配置和数据连接"
        logging.getLogger(__name__).error(
            "初始化未完成：异常类型=%s；原因=%s；"
            "如已开始写入，须清空目标后重跑",
            type(error).__name__,
            reason,
        )
        raise SystemExit(1) from None
