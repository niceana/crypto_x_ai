"""独立启动round3事件影响分析经济影响分析。"""

from round3.runner import run_impact
from entrypoints.runtime import execute, run_entrypoint


def main():
    return run_entrypoint(run_impact)


if __name__ == "__main__":
    execute(main)
