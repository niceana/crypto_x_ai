"""从媒体原始帖子初始化独立第一轮表；不读取 AI 配置。"""

import logging
from pathlib import Path

from common.media_config import load_media_job_config
from common.process_lock import stage_guard
from entrypoints.initialize_tasks import initialize_existing_posts
from entrypoints.runtime import DEFAULT_LOGGING, execute


def main():
    root = Path(__file__).resolve().parent.parent
    settings = load_media_job_config(root / "configs" / "media_post_extraction_config.yaml", require_ai=False)
    logging.basicConfig(**settings.get("logging", DEFAULT_LOGGING))
    with stage_guard("round1", settings["task_path"]):
        from xdata.client import get_client
        return initialize_existing_posts(get_client(), settings)


if __name__ == "__main__":
    execute(main)
