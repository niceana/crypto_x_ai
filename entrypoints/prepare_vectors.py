"""独立准备事件向量，沿用事件配置的日期及资产范围。"""

from entrypoints.runtime import execute, run_entrypoint
from common.stage_gates import require_round1_complete
from vectorization.preparation import prepare_vectors


def run_stage(cli, settings):
    require_round1_complete(cli, settings)
    return prepare_vectors(cli, settings)


def main():
    return run_entrypoint(run_stage)


if __name__ == "__main__":
    execute(main)
