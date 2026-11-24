"""从重置前的 CSV 恢复指定资产的两轮状态；默认预览，--apply 才写库。

适用于只重置状态、结果字段仍保留的场景。运行前停止同表第二、三轮程序。
"""

import argparse
import json
import tempfile
from pathlib import Path

import pandas as pd

from common.process_lock import stage_guard
from common.validation import validate_index


STATUS_COLUMNS = ["round2_status", "round3_status"]


def load_status_backup(path, asset):
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    required = ["date", "record_id", "asset"] + STATUS_COLUMNS
    missing = set(required) - set(frame.columns)
    if missing:
        raise ValueError("备份缺少字段：{}".format(sorted(missing)))
    frame = frame.loc[frame["asset"].str.strip().str.upper().eq(asset), required].copy()
    if frame.empty:
        raise ValueError("备份中没有 {} 记录".format(asset))
    # 逐个解析，兼容同一 CSV 中有/无小数秒的时间；不截断或转换时区。
    dates = pd.DatetimeIndex([pd.Timestamp(value) for value in frame.pop("date")])
    if dates.isna().any() or dates.tz is not None:
        raise ValueError("备份 date 必须为非空、无时区的原始发布时间")
    ids = frame.pop("record_id")
    if ids.str.strip().eq("").any():
        raise ValueError("备份 record_id 不能为空")
    frame.index = pd.MultiIndex.from_arrays([dates, ids], names=["date", "record_id"])
    validate_index(frame, "record_id")
    if not frame[STATUS_COLUMNS].isin(["0", "1", "9", "-1"]).all().all():
        raise ValueError("备份状态必须为字符串 0、1、9 或 -1")
    return frame.sort_index()


def restore_statuses(cli, result_path, backup_path, *, asset="SOL", apply=False,
                     output_root="reports/restore_event_status"):
    asset = asset.strip().upper()
    backup = load_status_backup(backup_path, asset)
    dates = backup.index.get_level_values("date")
    start, end = dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d")
    current = cli.read(result_path, start_time=start, end_time=end)
    validate_index(current, "record_id")
    if not set(["asset"] + STATUS_COLUMNS).issubset(current.columns):
        raise ValueError("目标表缺少 asset 或两轮状态列")
    current_dates = current.index.get_level_values("date")
    if not isinstance(current_dates, pd.DatetimeIndex) or current_dates.tz is not None:
        raise ValueError("目标表 date 必须为无时区的 datetime 索引")
    missing = backup.index.difference(current.index)
    if len(missing):
        raise ValueError("目标表缺少 {} 条备份索引，停止恢复；示例：{}".format(len(missing), list(missing[:3])))
    selected = current.loc[backup.index].copy(deep=True)
    if not selected["asset"].eq(asset).all():
        raise ValueError("目标表对应索引的 asset 与备份不一致")
    patch = backup[STATUS_COLUMNS].copy(deep=True)
    # 保留数据库读回的原索引类型和时间精度。
    patch.index = selected.index.copy()
    before = selected[STATUS_COLUMNS]
    conflicts = ~(before.eq("0") | before.eq(patch))
    if conflicts.any().any():
        keys = list(conflicts.index[conflicts.any(axis=1)][:3])
        raise ValueError("当前状态既不是 0，也不等于备份，可能已经重跑；停止恢复：{}".format(keys))
    changed = before.ne(patch).any(axis=1)
    patch = patch.loc[changed].sort_index()

    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix=asset + "_", dir=str(root)))
    snapshot = output / "before_restore.pkl"
    selected.to_pickle(snapshot)
    pd.testing.assert_frame_equal(selected, pd.read_pickle(snapshot), check_exact=True)
    review = selected[["asset"] + STATUS_COLUMNS].rename(
        columns={column: "current_" + column for column in STATUS_COLUMNS})
    for column in STATUS_COLUMNS:
        review["restore_" + column] = backup[column]
    review["will_update"] = changed
    review.to_csv(output / "status_changes.csv", encoding="utf-8-sig")
    summary = {"result_path": result_path, "backup_path": str(Path(backup_path).resolve()),
               "asset": asset, "start_time": start, "end_time": end,
               "matched_rows": len(backup), "changed_rows": len(patch),
               "mode": "apply" if apply else "preview", "output_dir": str(output)}
    (output / "plan.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if apply and not patch.empty:
        cli.update_by_column(result_path, patch.copy(deep=True))
    summary["written_rows"] = len(patch) if apply else 0
    if apply:
        (output / "apply_result.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup", required=True, help="重置前的状态 CSV 文件路径")
    parser.add_argument("--result-path", required=True, help="原事件去重分析表的 xdata 路径")
    parser.add_argument("--asset", default="SOL", help="要恢复的资产，默认 SOL")
    parser.add_argument("--output-dir", default="reports/restore_event_status", help="本次备份和清单的根目录")
    parser.add_argument("--apply", action="store_true", help="实际写回两个状态列；省略时只预览")
    args = parser.parse_args()
    # 同时占用两轮已有的本机进程锁，避免和同机运行中的处理程序交叉写入。
    with stage_guard("round2", args.result_path), stage_guard("round3", args.result_path):
        from xdata.client import get_client
        summary = restore_statuses(get_client(), args.result_path, args.backup,
                                   asset=args.asset, apply=args.apply, output_root=args.output_dir)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


if __name__ == "__main__":
    main()
