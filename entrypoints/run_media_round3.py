"""媒体第三轮：具体资产、行业和宏观影响分析，复用原执行器。"""

from pathlib import Path

from common.media_config import load_media_event_config, load_media_target_assets
from entrypoints.runtime import execute, run_entrypoint
from round3.runner import run_impact


def run_stage(cli, settings):
    return run_impact(cli, settings, target_assets=load_media_target_assets(cli, settings))


def main():
    root = Path(__file__).resolve().parent.parent
    return run_entrypoint(run_stage, stage="round3", config_loader=load_media_event_config,
                          config_path=root / "configs" / "media_event_processing_config.yaml")


if __name__ == "__main__":
    execute(main)
