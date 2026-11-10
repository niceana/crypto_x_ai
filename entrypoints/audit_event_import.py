"""只读核对第一轮回答与已有事件表，导出冲突、后续影响范围及完整本地快照。"""

import argparse
import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from common.config import load_event_config
from common.time_rules import published_time, select_scope
from common.validation import INPUT_COLUMNS, validate_event_source_index, validate_event_table_structure
from initialization.event_records import expand_first_round


logger = logging.getLogger(__name__)


def _ordered(keys):
    return sorted(keys, key=lambda key: (str(key[0]), str(key[1])))


def audit_event_import(cli, settings, output_dir=None, *, successful_only=False):
    """只调用 read；快照保留整表，核对范围沿用独立导入，不按公司资产名单过滤。"""
    logger.info("开始只读核对第一轮表与事件表；请先停止相关范围的写入进程")
    source_full = cli.read(settings.first_round_task_path)
    validate_event_source_index(source_full)
    source = select_scope(source_full, settings)
    if successful_only:
        if "status" not in source or not source["status"].isin(["0", "9", "1", "-1"]).all():
            raise ValueError("第一轮 status 字段缺失或状态不合法")
        source = source.loc[source["status"].eq("1")]
    expected = expand_first_round(source)
    existing = cli.read(settings.result_path)
    validate_event_table_structure(existing)
    actual = select_scope(existing, settings)
    source_dates = {str(post_id): raw_date for raw_date, post_id in source.index}
    if successful_only:
        actual = actual.loc[actual["source_post_id"].isin(source_dates)]
    old_keys, new_keys = set(actual.index), set(expected.index)
    extra, shared, missing = old_keys - new_keys, old_keys & new_keys, new_keys - old_keys
    logger.info("索引核对完成：当前展开=%d，已有范围=%d，多出=%d，共同=%d，缺失=%d；开始核对共同索引正文",
                len(expected), len(actual), len(extra), len(shared), len(missing))
    expected_by_post = defaultdict(list)
    expected_by_id = {key[1]: key for key in expected.index}
    for key, post_id in expected["source_post_id"].items():
        expected_by_post[post_id].append(key)

    conflict_posts, affected_from = set(), {}

    def affect(key, asset):
        day = published_time(key[0]).date()
        affected_from[asset] = min(affected_from.get(asset, day), day)

    extra_rows = []
    for key in _ordered(extra):
        row = actual.loc[key]
        post_id = row["source_post_id"]
        current = expected_by_post.get(post_id, [])
        if key[1] in expected_by_id:
            kind = "date_mismatch"
        elif post_id not in source_dates:
            kind = "post_not_in_current_source"
        elif not current:
            kind = "post_now_has_no_asset_events"
        else:
            kind = "event_id_changed"
        extra_rows.append({"date": str(key[0]), "date_repr": repr(key[0]), "record_id": key[1],
                           "source_post_id": post_id, "asset": row["asset"], "kind": kind,
                           "source_date_repr": repr(source_dates.get(post_id)),
                           "expected_event_count": len(current),
                           "expected_keys": json.dumps([(repr(item[0]), item[1]) for item in _ordered(current)],
                                                       ensure_ascii=False)})
        conflict_posts.add(post_id)
        affect(key, row["asset"])

    conflict_rows = []
    for key in _ordered(shared):
        old, new = actual.loc[key], expected.loc[key]
        changed = [name for name in INPUT_COLUMNS if not isinstance(old[name], str) or old[name] != new[name]]
        if not changed:
            continue
        conflict_rows.append(dict(date=str(key[0]), record_id=key[1], changed_fields=",".join(changed),
                                  **{"old_" + name: old[name] for name in INPUT_COLUMNS},
                                  **{"new_" + name: new[name] for name in INPUT_COLUMNS},
                                  round2_status=old["round2_status"], round3_status=old["round3_status"]))
        conflict_posts.update((old["source_post_id"], new["source_post_id"]))
        affect(key, old["asset"])
        affect(key, new["asset"])
    logger.info("固定输入核对完成：共同索引内容冲突=%d，涉及来源帖子=%d", len(conflict_rows), len(conflict_posts))

    # 同一帖的事件组成变化时，当前新事件与旧事件都纳入可能影响范围。
    for key, row in expected.loc[expected["source_post_id"].isin(conflict_posts), ["asset"]].iterrows():
        affect(key, row["asset"])

    latest_completed = {}
    for key, asset in existing.loc[existing["round2_status"].eq("1"), "asset"].items():
        stamp = published_time(key[0])
        latest_completed[asset] = max(latest_completed.get(asset, stamp), stamp)
    missing_rows = []
    for key in _ordered(missing):
        row = expected.loc[key]
        latest = latest_completed.get(row["asset"])
        uncertain = False
        try:
            historical = latest is not None and published_time(key[0]) < latest
        except TypeError:
            # 时区不兼容时不猜测转换规则，将后续影响标记为待核实。
            historical, uncertain = True, True
        missing_rows.append({"date": str(key[0]), "record_id": key[1], "source_post_id": row["source_post_id"],
                             "asset": row["asset"], "has_later_completed_round2": historical,
                             "time_comparison_uncertain": uncertain, "latest_completed_round2": repr(latest)})
        if historical:
            affect(key, row["asset"])

    # 后续记录包括配置范围之外的数据；逐资产统计，不能仅检查这 25 天。
    downstream = {asset: Counter() for asset in affected_from}
    for key, row in existing[["asset", "round2_status", "round3_status"]].iterrows():
        asset = row["asset"]
        if asset in affected_from and published_time(key[0]).date() >= affected_from[asset]:
            downstream[asset].update(records=1, round2_completed=int(row["round2_status"] == "1"),
                                     round3_completed=int(row["round3_status"] == "1"))
    impact_rows = [{"asset": asset, "potential_recompute_from_date": str(day),
                    "existing_records_from_date": downstream[asset]["records"],
                    "round2_completed_from_date": downstream[asset]["round2_completed"],
                    "round3_completed_from_date": downstream[asset]["round3_completed"]}
                   for asset, day in sorted(affected_from.items())]

    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    output = Path(output_dir) if output_dir is not None else Path(__file__).resolve().parent.parent / "reports" / (
        "event_import_audit_" + stamp)
    output = output.absolute()
    output.mkdir(parents=True, exist_ok=False)
    logger.info("开始写入完整本地快照与核对报告：%s", output)
    source_full.to_pickle(output / "first_round_snapshot.pkl")
    existing.to_pickle(output / "event_snapshot.pkl")
    for filename, rows, columns in (
        ("extra_records.csv", extra_rows, ["date", "date_repr", "record_id", "source_post_id", "asset", "kind",
                                           "source_date_repr", "expected_event_count", "expected_keys"]),
        ("input_conflicts.csv", conflict_rows, ["date", "record_id", "changed_fields"] +
         [prefix + name for prefix in ("old_", "new_") for name in INPUT_COLUMNS] + ["round2_status", "round3_status"]),
        ("missing_records.csv", missing_rows, ["date", "record_id", "source_post_id", "asset",
                                               "has_later_completed_round2", "time_comparison_uncertain",
                                               "latest_completed_round2"]),
        ("potential_recompute.csv", impact_rows, ["asset", "potential_recompute_from_date", "existing_records_from_date",
                                                  "round2_completed_from_date", "round3_completed_from_date"]),
    ):
        pd.DataFrame(rows, columns=columns).to_csv(output / filename, index=False, encoding="utf-8-sig")
    summary = {"first_round_task_path": settings.first_round_task_path, "result_path": settings.result_path,
               "start_time": settings.start_time, "end_time": settings.end_time, "successful_only": successful_only,
               "source_posts": len(source), "expected_records": len(expected), "existing_compared": len(actual),
               "extra_records": len(extra), "extra_kinds": dict(Counter(row["kind"] for row in extra_rows)),
               "shared_records": len(shared), "input_conflict_records": len(conflict_rows),
               "missing_records": len(missing), "missing_before_completed_round2": sum(
                   row["has_later_completed_round2"] for row in missing_rows),
               "conflict_posts": len(conflict_posts), "potential_recompute_assets": len(affected_from),
               "source_snapshot_records": len(source_full), "event_snapshot_records": len(existing),
               "database_modified": False, "output_dir": str(output)}
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("只读核对完成：%s", json.dumps(summary, ensure_ascii=False))
    return summary


def main(argv=None, cli=None):
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(root / "configs" / "event_processing_config.yaml"))
    parser.add_argument("--output-dir", help="尚不存在的新报告目录")
    parser.add_argument("--successful-only", action="store_true", help="与只导入成功帖的比较范围一致")
    args = parser.parse_args(argv)
    settings = load_event_config(args.config)
    logging.basicConfig(**(settings.logging or {"level": "INFO"}))
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    return audit_event_import(cli, settings, args.output_dir, successful_only=args.successful_only)


if __name__ == "__main__":
    main()
