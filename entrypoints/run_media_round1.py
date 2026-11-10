"""媒体第一轮：独立提示词与表，复用现有提取及自动导入。"""

from pathlib import Path

from common.config import ConfigError
from common.media_config import load_media_event_config, load_media_job_config
from entrypoints.runtime import execute


def main():
    root = Path(__file__).resolve().parent.parent
    settings = load_media_job_config(root / "configs" / "media_post_extraction_config.yaml")
    event_settings = load_media_event_config(root / "configs" / "media_event_processing_config.yaml")
    if event_settings.first_round_task_path != settings["task_path"]:
        raise ConfigError("两份媒体配置的 round1_result_path 必须一致")
    if settings["source_path"] == event_settings.result_path:
        raise ConfigError("媒体源帖子表与事件结果表路径必须不同")
    # 配置命名和日期先校验；只有正式启动时才接入已有 xdata 第一轮入口。
    from entrypoints.run_round1 import run_round1_job
    return run_round1_job(
        settings, lambda: event_settings,
        config_directory=root / "configs",
    )


if __name__ == "__main__":
    execute(main)
