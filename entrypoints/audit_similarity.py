"""复用事件表 embedding，只读导出指定批次的相似度对照文件。"""

import argparse
import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from common.config import load_event_config
from round2.similarity_audit import export_similarity_audit


def main(argv=None, cli=None):
    root = Path(__file__).absolute().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(root / "configs" / "event_processing_config.yaml"), help="事件配置路径")
    parser.add_argument("--start-time", help="本次批次起始日期，YYYY-MM-DD，需与 --end-time 同时指定")
    parser.add_argument("--end-time", help="本次批次结束日期，包含当天")
    parser.add_argument("--output-dir", help="新的本地输出目录，默认 reports/similarity_audit_北京时间")
    parser.add_argument("--threshold", type=float, help="观察阈值，默认只读取 embedding 配置的 similarity_threshold")
    parser.add_argument("--thresholds", type=float, nargs="+", default=[0.4, 0.45, 0.5, 0.55, 0.6],
                        help="对比阈值列表，另自动包含观察阈值")
    parser.add_argument("--top-k", type=int, default=10, help="每条事件导出的候选组数，包含低于阈值的组，默认 10")
    args = parser.parse_args(argv)
    settings = load_event_config(args.config)
    if bool(args.start_time) != bool(args.end_time):
        parser.error("--start-time 和 --end-time 必须同时指定")
    if args.start_time:
        settings = replace(settings, start_time=args.start_time, end_time=args.end_time)
    threshold = args.threshold
    if threshold is None:
        if not settings.embedding_config_path:
            parser.error("未配置 embedding_config_path，请显式指定 --threshold")
        # 只读阈值，不加载 API 配置或 .env，不需要模型地址、密钥或当前提示词。
        document = yaml.safe_load(Path(settings.embedding_config_path).read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            parser.error("Embedding 配置必须为字典，或用 --threshold 显式指定观察阈值")
        threshold = document.get("similarity_threshold", 0.5)
    logging.basicConfig(**(settings.logging or {"level": "INFO"}))
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    output = Path(args.output_dir).absolute() if args.output_dir else root / "reports" / ("similarity_audit_" + stamp)
    manifest = export_similarity_audit(cli, settings, output, threshold=threshold,
                                      thresholds=args.thresholds, top_k=args.top_k)
    logging.getLogger(__name__).info("相似度只读复算完成：事件=%d，对照行=%d，输出=%s",
                                     manifest["events"], manifest["candidate_rows"], output)
    return manifest


if __name__ == "__main__":
    main()
