"""连续每 15 分钟调用业务函数，只在每小时 47 分对账；参数全部在顶部。"""

import asyncio
import hashlib
import json
import logging
from pathlib import Path
import sys

import pandas as pd
import crypto_x_ai_live
from crypto_x_ai_live import load_live_config, run_live_batch
from crypto_x_ai_live import _compat as core

from export_hours import digest
from _replay import compare, post_batch, schedule, select, write_json
from _trial import (DEFAULT_CONFIG_DIR, ROOT, add_log_file, assets, init_logging, merge_events, save_tables,
                    score_parameters, set_log_batch, test_clock)

# ==================== 运行前修改这里 ====================
DATA_DIR = ROOT / "data_240h"
CONFIG_DIR = DEFAULT_CONFIG_DIR
MODE = "full"       # "full"：960 批完整流程；"weight"：只计算 240 个时点，不调用 AI
MAX_ATTEMPTS = 100
RESUME_DIR = None    # 首次 None；失败后填本次输出目录绝对路径，跳过已成功批次并续跑
OUTPUT_ROOT = ROOT / "outputs"
LOG_LEVEL = "INFO"   # 改成 "DEBUG" 查看源模块已有的详细日志
LOG_TO_FILE = True   # 在本次输出目录的 run.log 保存；续跑时追加
# 测试时刻、小时数、资产、sum/max_abs 和 param_id 从导出数据 manifest.json 自动读取。
# ======================================================

logger = logging.getLogger(__name__)


def signature(manifest, mode, parameters, config):
    identity = dict(dataset_id=manifest["dataset_id"], mode=mode, package_version=crypto_x_ai_live.__version__,
                    parameters=parameters.to_dict(), aggregation_method=manifest["aggregation_method"],
                    first_time=manifest["first_time"], hours=manifest["hours"])
    if config is not None:
        identity.update(ai=[core.config.ai_config_hash(getattr(config, name)) for name in ("round1_ai", "round2_ai", "round3_ai")],
                        embedding={name: getattr(config.embedding, name) for name in ("base_url", "model", "model_version", "similarity_threshold")},
                        accounts=config.user_name_to_tickers)
    return hashlib.sha256(json.dumps(identity, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def replay(data_dir, config_dir, *, mode="full", max_attempts=100, resume_dir=None, output_root=None,
           config=None, batch_function=None, log_to_file=False):
    if mode not in ("full", "weight"):
        raise ValueError("MODE 必须为 full 或 weight")
    data_dir = Path(data_dir).expanduser().resolve()
    manifest = json.loads((data_dir / "manifest.json").read_text(encoding="utf-8"))
    calls = schedule(manifest["first_time"], manifest["hours"])
    names = ("reference_events.pkl", "old_weight.pkl", "assets.json")
    if mode == "full":
        names += ("posts.pkl", "history_seed.pkl")
    for name in names:
        if digest(data_dir / name) != manifest["files"][name]:
            raise ValueError("导出数据 {} 已变更，请重新导出或恢复原文件，不复用旧续跑进度".format(name))
    instruments = assets(data_dir / "assets.json")
    reference = pd.read_pickle(data_dir / "reference_events.pkl")
    old = pd.read_pickle(data_dir / "old_weight.pkl")
    if mode == "full":
        config = config or load_live_config(config_dir, max_attempts=max_attempts, weight_param_id=manifest["weight_param_id"])
        parameters = config.weight_parameters
        history = pd.read_pickle(data_dir / "history_seed.pkl")
        posts = pd.read_pickle(data_dir / "posts.pkl")
    else:
        config = None
        parameters = score_parameters(config_dir, manifest["weight_param_id"])
        history = posts = None
        calls = calls[calls.minute == 47]
    token = signature(manifest, mode, parameters, config)
    if resume_dir is None:
        stamp = pd.Timestamp.now(tz="UTC").tz_convert("Asia/Shanghai").strftime("%Y%m%d_%H%M%S_%f")
        target = Path(output_root or ROOT / "outputs") / ("replay_" + mode + "_" + stamp)
        target.mkdir(parents=True, exist_ok=False)
        write_json(target / "run.json", dict(signature=token, dataset_id=manifest["dataset_id"], mode=mode,
                                             first_time=manifest["first_time"], hours=manifest["hours"]))
    else:
        target = Path(resume_dir).expanduser().resolve()
        previous = json.loads((target / "run.json").read_text(encoding="utf-8"))
        if previous["signature"] != token:
            raise ValueError("续跑的数据、处理模式、AI/向量模型或评分参数已变更，不能混用旧进度")
    if log_to_file:
        add_log_file(target / "run.log")
    logger.info("连续回放启动：mode=%s，小时=%d，批次=%d，续跑=%s", mode, manifest["hours"], len(calls), resume_dir is not None)
    print("输出目录: {}".format(target), flush=True)
    print("模式 {}，{} 个小时，{} 次处理。".format(mode, manifest["hours"], len(calls)), flush=True)
    weight_rows, hourly = [], []
    empty_batches = processed_posts = recovered = 0
    batch_function = batch_function or run_live_batch
    summary = dict(status="running", mode=mode, output_dir=str(target), requested_hours=manifest["hours"],
                   requested_batches=len(calls), completed_batches=0, failed_batch=None)

    def report():
        summary.update(completed_hours=len(hourly), recovered_batches=recovered, empty_post_batches=empty_batches,
                       processed_posts=processed_posts,
                       weight_only_pass=sum(row["weight_only"] == "PASS" for row in hourly),
                       weight_only_diff=sum(row["weight_only"] == "DIFF" for row in hourly),
                       full_pass=sum(row.get("full") == "PASS" for row in hourly),
                       full_diff=sum(row.get("full") == "DIFF" for row in hourly))
        if hourly:
            pd.DataFrame(hourly).to_csv(target / "hourly_summary.csv", index=False, encoding="utf-8-sig")
        if weight_rows:
            weights = pd.concat(weight_rows).sort_index()
            weights.to_pickle(target / "weights.pkl")
            weights.to_csv(target / "weights.csv", encoding="utf-8-sig")
            weights.loc[weights.index.minute == 47].to_csv(target / "weights_47.csv", encoding="utf-8-sig")
        write_json(target / "summary.json", summary)

    for index, call in enumerate(calls, 1):
        set_log_batch(call)
        directory = target / ("batch_{:04d}_".format(index) + call.strftime("%Y%m%d_%H%M"))
        directory.mkdir(exist_ok=True)
        marker = directory / "summary.json"
        saved = json.loads(marker.read_text(encoding="utf-8")) if marker.exists() else None
        if mode == "full":
            history = history.loc[history.index.get_level_values("date") >= call - pd.Timedelta(hours=8, days=100)].copy(deep=True)
            batch = post_batch(posts, call)
            empty_batches += int(batch.empty)
            processed_posts += len(batch)
        try:
            if saved and saved.get("status") == "success":
                if saved["call_time"] != str(call):
                    raise ValueError("已保存批次时刻与当前调度不匹配")
                weight = pd.read_pickle(directory / "weight.pkl")
                if mode == "full":
                    history = merge_events(history, pd.read_pickle(directory / "event_results.pkl"))
                if saved.get("hourly") is not None:
                    hourly.append(saved["hourly"])
                recovered += 1
                logger.info("复用已完成批次 %d/%d，不重复调用 AI", index, len(calls))
            else:
                state = dict(status="running", call_time=str(call), batch_number=index)
                if mode == "full":
                    logger.info("调用完整流程：批次=%d/%d，原帖=%d，历史=%d", index, len(calls), len(batch), len(history))
                    with test_clock(str(call)):
                        result = asyncio.run(batch_function(batch, history, assets=instruments, config=config,
                                                           aggregation_method=manifest["aggregation_method"]))
                    if result["status"] != "success":
                        raise RuntimeError("整批处理返回 {}，本批下次从头重算".format(result["status"]))
                    save_tables(directory, result)
                    history = merge_events(history, result["event_results"])
                    weight = result["weight"]
                    state.update(posts=len(batch), event_updates=len(result["event_results"]))
                else:
                    weight = core.weights.build_live_weight(reference, as_of=call, assets=instruments,
                                                            aggregation_method=manifest["aggregation_method"], parameters=parameters)
                    weight.to_pickle(directory / "weight.pkl")
                if call.minute == 47:
                    begin = call.floor("h") - pd.Timedelta(minutes=15, hours=8)
                    end = call.floor("15min") - pd.Timedelta(hours=8)
                    window = select(reference, begin, end)
                    recomputed = core.weights.build_live_weight(window, as_of=call, assets=instruments,
                                                                aggregation_method=manifest["aggregation_method"], parameters=parameters)
                    recomputed.to_pickle(directory / "old_events_recomputed_weight.pkl")
                    info, diff = compare(recomputed, old, call)
                    diff.to_csv(directory / "comparison_weight_only.csv", encoding="utf-8-sig")
                    row = dict(date=str(call), old_reference_time=info["old_reference_time"], weight_only=info["comparison"],
                               weight_only_different_assets=info["different_assets"], reference_events=len(window),
                               missing_old_assets=json.dumps(info["missing_old_assets"]))
                    if mode == "full":
                        info, diff = compare(weight, old, call)
                        diff.to_csv(directory / "comparison_full.csv", encoding="utf-8-sig")
                        row.update(full=info["comparison"], full_different_assets=info["different_assets"],
                                   full_max_abs_difference=info["max_abs_difference"])
                    state["hourly"] = row
                    hourly.append(row)
                    logger.info("47分对账：weight_only=%s，full=%s", row["weight_only"], row.get("full", "未运行"))
                state["status"] = "success"
                write_json(marker, state)
            weight_rows.append(weight)
            summary["completed_batches"] = index
            report()
            if call.minute == 47:
                print("[{}/{}] {}: weight_only={}, full={}".format(len(hourly), manifest["hours"], call,
                      hourly[-1]["weight_only"], hourly[-1].get("full", "未运行")), flush=True)
            else:
                print("[{}/{}] {}: OK，原帖 {} 条".format(index, len(calls), call, len(batch)), flush=True)
        except (Exception, KeyboardInterrupt) as exc:
            logger.exception("回放批次失败：批次=%d/%d，时刻=%s", index, len(calls), call)
            failure = dict(status="failed", call_time=str(call), batch_number=index, error=str(exc))
            write_json(marker, failure)
            summary.update(status="failed", failed_batch=index, error=str(exc))
            report()
            print("FAILED: {}；设置 RESUME_DIR={} 后重试同一批。".format(exc, repr(str(target))), file=sys.stderr, flush=True)
            return summary
    if mode == "full":
        history.to_pickle(target / "final_history.pkl")
    summary["status"] = "completed"
    report()
    set_log_batch()
    logger.info("连续回放完成：批次=%d，小时=%d，weight_only PASS=%d/DIFF=%d，full PASS=%d/DIFF=%d",
                summary["completed_batches"], summary["completed_hours"], summary["weight_only_pass"],
                summary["weight_only_diff"], summary["full_pass"], summary["full_diff"])
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return summary


def main():
    init_logging(LOG_LEVEL)
    result = replay(DATA_DIR, CONFIG_DIR, mode=MODE, max_attempts=MAX_ATTEMPTS,
                    resume_dir=RESUME_DIR, output_root=OUTPUT_ROOT, log_to_file=LOG_TO_FILE)
    if result["status"] != "completed":
        return 2
    return int(result["weight_only_diff"] > 0 or result["full_diff"] > 0)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError, KeyError) as exc:
        logger.exception("回放脚本未完成")
        print("ERROR: {}".format(exc), file=sys.stderr)
        raise SystemExit(2)
