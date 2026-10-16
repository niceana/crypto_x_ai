"""独立启动round2事件去重归组事件关联。"""

from round2.runner import run_relation
from entrypoints.runtime import execute, run_entrypoint


def main():
    return run_entrypoint(run_relation)


if __name__ == "__main__":
    execute(main)
