"""独立导入事件去重分析表，自动判断建表或追加；日常由 round1 入口自动导入。"""

from initialization.event_records import import_event_records_from_table
from entrypoints.runtime import execute, run_entrypoint


def main():
    return run_entrypoint(import_event_records_from_table)


if __name__ == "__main__":
    execute(main)
