"""只读查询权重跳过的事件，按同一资产名单和完整窗口导出状态及错误原因。"""

import argparse
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from common.config import load_event_config
from postprocessing.weight_data import (
    load_weight_assets, normalize_weight_assets, read_weight_events, skipped_weight_events,
)
from postprocessing.weight_time import event_window_bounds, weight_output_bounds


READ_COLUMNS = ["asset", "round2_status", "round3_status", "event_relation", "event", "error_message"]
STATUS_MEANINGS = {"0": "待处理", "9": "已认领（处理中或中断后遗留）", "1": "成功", "-1": "最终失败"}


def audit_skipped_weight_events(cli, settings, *, output_dir=None):
    """只调用 read/read_object；不解析 AI 回答、不写库，输出当前状态的本地快照。"""
    if not isinstance(settings.result_path, str) or not settings.result_path.strip() or "填写" in settings.result_path:
        raise ValueError("请配置实际的 result_path")
    if not isinstance(settings.start_time, str) or not isinstance(settings.end_time, str):
        raise ValueError("请配置与权重运行一致的 start_time/end_time")
    start, end = date.fromisoformat(settings.start_time), date.fromisoformat(settings.end_time)
    if start.isoformat() != settings.start_time or end.isoformat() != settings.end_time or start > end:
        raise ValueError("日期必须为 YYYY-MM-DD，且 start_time 不能晚于 end_time")
    assets = normalize_weight_assets(load_weight_assets(cli, settings.round2_asset_list_path))
    results = read_weight_events(cli, settings.result_path, settings.start_time, settings.end_time,
                                columns=READ_COLUMNS, label="权重跳过事件只读查询")
    results = results.loc[results["asset"].isin(assets)]
    skipped = skipped_weight_events(results)
    counts = results.groupby("asset").size().rename("total_events").to_frame()
    counts["skipped_events"] = skipped.groupby("asset").size().reindex(counts.index, fill_value=0)
    counts["successful_events"] = counts["total_events"] - counts["skipped_events"]
    affected = counts.loc[counts["skipped_events"].gt(0)].reset_index()
    statuses = (skipped.groupby(["asset", "round2_status", "round3_status"], dropna=False)
                .size().rename("events").reset_index())

    root = Path(__file__).absolute().parent.parent
    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    output = Path(output_dir).absolute() if output_dir is not None else root / "reports" / "skipped_weight_events" / stamp
    output.mkdir(parents=True, exist_ok=False)
    details_path, assets_path, statuses_path = [output / name for name in
        ("skipped_events.csv", "assets.csv", "status_counts.csv")]
    # 原始时间和 record_id 一并导出，保留小数秒和错误原因，不只列币名。
    skipped.to_csv(details_path, encoding="utf-8-sig")
    affected.to_csv(assets_path, index=False, encoding="utf-8-sig")
    statuses.to_csv(statuses_path, index=False, encoding="utf-8-sig")
    begin, finish = event_window_bounds(*weight_output_bounds(settings.start_time, settings.end_time))
    report = {"result_path": settings.result_path, "asset_list_path": settings.round2_asset_list_path,
              "start_time": settings.start_time, "end_time": settings.end_time,
              "event_window_start_inclusive": str(begin), "event_window_end_exclusive": str(finish),
              "selected_events": len(results), "successful_events": len(results) - len(skipped),
              "skipped_events": len(skipped), "affected_assets": affected.to_dict(orient="records"),
              "status_counts": statuses.to_dict(orient="records"), "status_meanings": STATUS_MEANINGS,
              "details_path": str(details_path), "assets_path": str(assets_path), "statuses_path": str(statuses_path)}
    (output / "summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main(argv=None, cli=None):
    root = Path(__file__).absolute().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(root / "configs" / "event_processing_config.yaml"), help="与生成权重相同的配置")
    parser.add_argument("--output-dir", help="新的本地输出目录，默认 reports/skipped_weight_events/北京时间")
    args = parser.parse_args(argv)
    settings = load_event_config(args.config)
    logging.basicConfig(**(settings.logging or {"level": "INFO"}))
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    report = audit_skipped_weight_events(cli, settings, output_dir=args.output_dir)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


if __name__ == "__main__":
    main()
