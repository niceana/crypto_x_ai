"""按月并行运行：第一轮 → 独立向量准备 → 第二轮 → 第三轮。"""

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.config import load_ai_config, load_embedding_config, load_event_config, load_job_config, require_prompts
from entrypoints.runtime import execute
from pipeline.runner import PipelineConfig, run_pipeline


def main(argv=None):
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--post-config", type=Path, default=root / "configs" / "post_extraction_config.yaml")
    parser.add_argument("--event-config", type=Path, default=root / "configs" / "event_processing_config.yaml")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--report-dir", type=Path, help="尚不存在的运行记录目录")
    group.add_argument("--resume", type=Path, help="已有运行记录目录；按数据库状态重新核验和续跑")
    args = parser.parse_args(argv)
    post = load_job_config(args.post_config, ("task_path", "ai_config_path"))
    path = Path(post["ai_config_path"])
    post["ai_config_path"] = str(path.resolve() if path.is_absolute() else (args.post_config.resolve().parent / path).resolve())
    settings = load_event_config(args.event_config)
    configs = tuple(load_ai_config(path) for path in (post["ai_config_path"], settings.round2_ai_config_path,
                                                     settings.round3_ai_config_path))
    for config in configs:
        require_prompts(config)
    embedding = load_embedding_config(settings.embedding_config_path)
    logging.basicConfig(**(settings.logging or {"level": "INFO", "format": "%(asctime)s | %(message)s"}))
    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    report = args.resume or args.report_dir or root / "reports" / "monthly_pipeline" / stamp
    result = run_pipeline(PipelineConfig(post, settings, configs, embedding), report, resume=args.resume is not None,
                          lock_path=root / "reports" / "monthly_pipeline" / ".run.lock")
    logging.getLogger(__name__).info("月流水线完成，清单：%s", report / "manifest.json")
    return {"status": result["status"], "months": len(result["months"]), "report_dir": str(report)}


if __name__ == "__main__":
    execute(main)
