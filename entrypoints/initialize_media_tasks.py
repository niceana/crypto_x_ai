"""从本地 JSON 或媒体原始表初始化独立第一轮表；不读取 AI 配置。"""

import logging
from pathlib import Path

from common.media_config import load_media_job_config
from common.process_lock import stage_guard
from entrypoints.initialize_tasks import initialize_existing_posts
from entrypoints.runtime import DEFAULT_LOGGING, execute
from initialization.media_posts import load_media_json_posts
from initialization.post_tasks import initialize_tasks


def main():
    root = Path(__file__).resolve().parent.parent
    settings = load_media_job_config(root / "configs" / "media_post_extraction_config.yaml", require_ai=False)
    logging.basicConfig(**settings.get("logging", DEFAULT_LOGGING))
    with stage_guard("round1", settings["task_path"]):
        # 先完成本地文件校验，再连接 SDK 和注册目标，避免坏输入留下空表。
        source = load_media_json_posts(settings["json_files"]) if settings.get("json_files") else None
        from xdata.client import get_client
        cli = get_client()
        if source is None:
            return initialize_existing_posts(cli, settings)

        def write_tasks(frame):
            cli.register(settings["task_path"], "timeseries", "")
            cli.write(settings["task_path"], frame)

        return initialize_tasks(source, write_tasks)


if __name__ == "__main__":
    execute(main)
