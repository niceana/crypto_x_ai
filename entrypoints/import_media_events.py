"""媒体第一轮结果独立严格导入；复用原有固定输入校验及缺失追加。"""

from pathlib import Path

from common.media_config import load_media_event_config
from entrypoints.runtime import execute, run_entrypoint
from initialization.event_records import import_event_records_from_table


def main():
    root = Path(__file__).resolve().parent.parent
    return run_entrypoint(import_event_records_from_table, stage="import_events",
                          config_loader=load_media_event_config,
                          config_path=root / "configs" / "media_event_processing_config.yaml")


if __name__ == "__main__":
    execute(main)
