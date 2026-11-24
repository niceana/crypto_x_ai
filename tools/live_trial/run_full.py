"""调用正式包，运行第一轮、向量、第二轮、第三轮及 weight；数据只写本地 outputs。"""

import asyncio
import logging
import sys

import pandas as pd
import crypto_x_ai_live
from crypto_x_ai_live import load_live_config, run_live_batch

from _trial import (add_log_file, arguments, assets, dump_json, init_logging, load_frame, merge_events,
                    output_dir, path, save_tables, set_log_batch, test_clock)

LOG_LEVEL = "INFO"  # 改成 "DEBUG" 可查看源模块已有的更详细日志
LOG_TO_FILE = True  # 在本次 outputs/full_.../run.log 中同时保存 UTF-8 日志
logger = logging.getLogger(__name__)


def main():
    parser = arguments(__doc__)
    parser.add_argument("--posts", default="data/posts.pkl")
    parser.add_argument("--history", default="data/history_before.pkl", help="本批开始前的历史 B；填 none 表示无历史")
    parser.add_argument("--timeout-seconds", type=float, default=840, help="整轮计算预算，另给清理和写回预留时间")
    parser.add_argument("--max-attempts", type=int, default=100)
    args = parser.parse_args()
    init_logging(LOG_LEVEL)
    set_log_batch(args.test_time or "system_time")
    target = output_dir("full")
    if LOG_TO_FILE:
        add_log_file(target / "run.log")
    posts = load_frame(args.posts)
    history = None if args.history.lower() == "none" else load_frame(args.history)
    instruments = assets(args.assets)
    config = load_live_config(path(args.config_dir), max_attempts=args.max_attempts, weight_param_id=args.weight_param_id)
    metadata = dict(package_version=crypto_x_ai_live.__version__, posts_rows=len(posts),
                    history_rows=0 if history is None else len(history), method=args.method,
                    test_time=args.test_time, assets=instruments, weight_parameters=config.weight_parameters.to_dict())
    print("package:", crypto_x_ai_live.__file__)
    print("posts={}, history={}, method={}".format(metadata["posts_rows"], metadata["history_rows"], args.method))
    print("模拟测试时刻: " + args.test_time if args.test_time else "使用真实系统时间")
    logger.info("单批完整流程启动：原帖=%d，历史=%d，聚合=%s", len(posts), metadata["history_rows"], args.method)
    with test_clock(args.test_time):
        result = asyncio.run(run_live_batch(posts, history, assets=instruments, config=config,
            aggregation_method=args.method, timeout_seconds=args.timeout_seconds))
    if result["status"] != "success":
        metadata.update(status=result["status"])
        dump_json(target / "summary.json", metadata)
        print("FAILED: {}；未保存中间表，原帖保留供下一轮重算。".format(result["status"]), file=sys.stderr)
        return 1
    save_tables(target, result)
    # 额外保存合并后的事件快照，方便与旧 weight 算法使用完全相同的事件输入。
    previous = pd.DataFrame() if history is None else history
    merge_events(previous, result["event_results"]).to_pickle(target / "events_merged.pkl")
    metadata.update(status="success", round1_rows=len(result["round1_results"]),
                    event_delta_rows=len(result["event_results"]), weight_time=str(result["weight"].index[0]))
    dump_json(target / "summary.json", metadata)
    logger.info("单批完整流程完成：第一轮=%d，事件更新=%d，权重时刻=%s", len(result["round1_results"]),
                len(result["event_results"]), result["weight"].index[0])
    print(result["weight"].to_string())
    print("输出目录:", target)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError, KeyError) as exc:
        logger.exception("调用脚本未完成")
        print("ERROR: {}".format(exc), file=sys.stderr)
        raise SystemExit(2)
