"""独立启动round2事件去重归组事件关联。"""

from round2.runner import run_relation
from common.stage_gates import require_round1_complete
from entrypoints.runtime import execute, run_entrypoint


def run_stage(cli, settings):
    require_round1_complete(cli, settings)
    return run_relation(cli, settings)


def main():
    return run_entrypoint(run_stage)


if __name__ == "__main__":
    execute(main)
