"""独立导入事件去重分析表，支持 auto/create/resume；日常由 main.py 自动调用导入模块。"""

from initialization.event_records import import_event_records_from_table
from entrypoints.runtime import execute, run_entrypoint


def main():
    return run_entrypoint(import_event_records_from_table)


if __name__ == "__main__":
    execute(main)
