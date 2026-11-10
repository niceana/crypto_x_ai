"""媒体第二轮：同对象一个月全部候选组，不使用向量服务。"""

from pathlib import Path

from common.media_config import load_media_event_config, load_media_target_assets
from common.stage_gates import require_round1_complete
from entrypoints.runtime import execute, run_entrypoint
from round2.retrieval import select_all_groups
from round2.runner import run_relation


def run_stage(cli, settings):
    require_round1_complete(cli, settings)
    return run_relation(
        cli, settings, retriever=select_all_groups, max_candidate_groups=None,
        max_input_characters=settings.round2_max_input_characters,
        target_assets=load_media_target_assets(cli, settings),
    )


def main():
    root = Path(__file__).resolve().parent.parent
    return run_entrypoint(run_stage, stage="round2", config_loader=load_media_event_config,
                          config_path=root / "configs" / "media_event_processing_config.yaml")


if __name__ == "__main__":
    execute(main)
