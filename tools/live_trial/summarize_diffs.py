"""从既有回放统计 weight 差异；可仅统计窗口内有事件的资产时点。"""

import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

# ==================== 运行前只需修改这里 ====================
RUN_DIR = "填写本次 outputs/replay_full_... 的绝对路径"
COMPARISON = "full"   # full：完整流程差异；weight_only：旧事件重算 weight 的差异
SCOPE = "with_events"  # with_events：新旧任一侧窗口内有已完成事件；all：全部资产
DATA_DIR = Path(__file__).resolve().parent / "data_240h"  # 当时导出的原始数据，必要时改为绝对路径
OUTPUT_DIR = None     # None：在 RUN_DIR 下生成 diff_stats_full_with_events/ 等独立目录
PRINT_TOP = 30        # 终端展示差异次数最多的前几个资产，CSV 保存全部资产
# ==========================================================


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def boolean(value):
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ("true", "1", "1.0"):
        return True
    if text in ("false", "0", "0.0"):
        return False
    raise ValueError("已保存的 equal 列含无法识别的值：{}".format(value))


def collect(run_dir, comparison):
    paths = sorted(run_dir.glob("batch_*/comparison_{}.csv".format(comparison)))
    if not paths:
        raise ValueError("没有找到 comparison_{}.csv，请把 RUN_DIR 指向那次回放输出目录".format(comparison))
    frames, skipped, seen_times, flag_sources = [], [], set(), dict(saved=0, recomputed=0)
    for path in paths:
        state = read_json(path.parent / "summary.json")
        if state.get("status") != "success":
            skipped.append(str(path.relative_to(run_dir)))
            continue
        call = pd.Timestamp(state["call_time"])
        if pd.isna(call) or call.minute != 47:
            raise ValueError("对账批次的时刻不是有效的 47 分：{}".format(path))
        if call in seen_times:
            raise ValueError("同一个时刻存在重复对账文件，不能重复统计：{}".format(call))
        seen_times.add(call)
        frame = pd.read_csv(path, encoding="utf-8-sig", dtype={"asset": str})
        if frame.empty or not {"asset", "new", "old"}.issubset(frame.columns):
            raise ValueError("对账文件为空或缺少 asset/new/old：{}".format(path))
        if frame["asset"].isna().any() or frame["asset"].duplicated().any() or frame["asset"].str.strip().eq("").any():
            raise ValueError("对账文件的资产名为空或重复：{}".format(path))
        frame["new"] = frame["new"].astype("float64")
        frame["old"] = frame["old"].astype("float64")
        if "equal" in frame:
            frame["equal"] = frame["equal"].map(boolean).astype(bool)
            flag_sources["saved"] += 1
        else:
            finite = np.isfinite(frame["new"].to_numpy()) & np.isfinite(frame["old"].to_numpy())
            frame["equal"] = finite & np.isclose(frame["new"].to_numpy(), frame["old"].to_numpy(), rtol=1e-9, atol=1e-12)
            flag_sources["recomputed"] += 1
        frame["old_column_present"] = frame["old_column_present"].map(boolean) if "old_column_present" in frame else True
        frame["date"] = call
        frame["old_reference_time"] = state.get("hourly", {}).get("old_reference_time", str(call.floor("h") + pd.Timedelta(hours=1)))
        frame["difference"] = frame["new"] - frame["old"]
        frame["abs_difference"] = frame["difference"].abs()
        frame["different"] = ~frame["equal"]
        frame["non_finite"] = ~(np.isfinite(frame["new"]) & np.isfinite(frame["old"]))
        frame["batch_dir"] = path.parent.name
        frames.append(frame[["date", "old_reference_time", "asset", "new", "old", "difference", "abs_difference",
                             "old_column_present", "equal", "different", "non_finite", "batch_dir"]])
    if not frames:
        raise ValueError("没有已成功完成的对账批次，不能据此统计资产差异")
    return pd.concat(frames, ignore_index=True).sort_values(["date", "asset"]), skipped, flag_sources


def read_events(path):
    """只取计数所需字段；日期与正式函数一致，按 UTC 原帖发布时间解释。"""
    frame = pd.read_pickle(path)
    columns = ["asset", "round2_status", "round3_status"]
    if (not isinstance(frame, pd.DataFrame) or not frame.columns.is_unique
            or not isinstance(frame.index, pd.MultiIndex)
            or list(frame.index.names) != ["date", "record_id"]
            or not frame.index.is_unique or not frame.index.get_level_values("record_id").is_unique
            or not set(columns).issubset(frame.columns)):
        raise ValueError("事件文件须含唯一 (date, record_id) 索引及 asset/round2_status/round3_status：{}".format(path))
    dates = pd.DatetimeIndex(pd.to_datetime(frame.index.get_level_values("date"), utc=True)).tz_localize(None)
    if dates.hasnans or not frame["asset"].map(lambda value: isinstance(value, str) and bool(value.strip())).all():
        raise ValueError("事件文件含缺失时间或无效资产名：{}".format(path))
    result = frame[columns].copy()
    result.index = pd.MultiIndex.from_arrays([dates, frame.index.get_level_values("record_id")], names=["date", "record_id"])
    return result


def event_counts(frame, begin, end):
    dates = frame.index.get_level_values("date")
    selected = ((dates >= begin) & (dates < end)
                & frame["round2_status"].eq("1") & frame["round3_status"].eq("1"))
    return frame.loc[selected].groupby("asset").size()


def with_event_presence(values, run_dir, data_dir, comparison):
    """按每个已完成对账时点重建新侧历史，避免用后续批次修改过的快照回看。"""
    if data_dir is None:
        raise ValueError("with_events 口径须填写 DATA_DIR，指向当时导出的 data_240h")
    data_dir = Path(data_dir).expanduser().resolve()
    manifest = read_json(data_dir / "manifest.json")
    run = read_json(run_dir / "run.json")
    if not manifest.get("dataset_id") or run.get("dataset_id") != manifest["dataset_id"]:
        raise ValueError("RUN_DIR 与 DATA_DIR 的 dataset_id 不一致或缺失，不能混用回放数据")
    if comparison == "full" and run.get("mode") != "full":
        raise ValueError("full 的有事件口径须使用 full 回放输出")
    reference_path = data_dir / "reference_events.pkl"
    expected = manifest.get("files", {}).get("reference_events.pkl")
    if not expected:
        raise ValueError("manifest.json 缺少 reference_events.pkl 的文件校验信息")
    digest = hashlib.sha256()
    with reference_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise ValueError("reference_events.pkl 与原导出校验不一致，请恢复原数据，不重新导出覆盖")
    reference = read_events(reference_path)
    new_events = reference.iloc[:0].copy()
    calls = sorted(values["date"].unique())
    batches = {}
    if comparison == "full":
        for directory in sorted(run_dir.glob("batch_*")):
            state = read_json(directory / "summary.json")
            if state.get("status") != "success":
                continue
            call = pd.Timestamp(state["call_time"])
            if call in batches:
                raise ValueError("同一个调用时刻有重复成功批次：{}".format(call))
            batches[call] = directory
        first = pd.Timestamp(manifest["first_time"])
        hours = manifest["hours"]
        if (first.tzinfo is not None or first.minute != 47 or first.second or first.microsecond
                or type(hours) is not int or hours <= 0):
            raise ValueError("导出的 first_time/hours 不是有效的 UTC+8 回放区间")
        schedule = pd.date_range(first.floor("h") + pd.Timedelta(minutes=2), periods=hours * 4, freq="15min")
        if any(pd.Timestamp(call) not in schedule for call in calls):
            raise ValueError("对账时点不属于 DATA_DIR 的回放区间")
        cursor = 0
    frames = []
    for call in calls:
        call = pd.Timestamp(call)
        if comparison == "full":
            updates = []
            while cursor < len(schedule) and schedule[cursor] <= call:
                pending = schedule[cursor]
                if pending not in batches:
                    raise ValueError("缺少 {} 的成功批次，无法完整还原有事件资产范围".format(pending))
                updates.append(read_events(batches[pending] / "event_results.pkl"))
                cursor += 1
            if updates:
                new_events = pd.concat([new_events] + updates)
                new_events = new_events.loc[~new_events.index.duplicated(keep="last")]
        # 与正式 weight 一致：16:47 对应本地 [15:45, 16:45)，事件索引仍为 UTC。
        begin = call.floor("h") - pd.Timedelta(minutes=15, hours=8)
        end = call.floor("15min") - pd.Timedelta(hours=8)
        old_counts = event_counts(reference, begin, end)
        new_counts = old_counts if comparison == "weight_only" else event_counts(new_events, begin, end)
        frame = values.loc[values["date"].eq(call)].copy()
        coins = frame["asset"].str.split(".", n=1, regex=False).str[0]
        frame["old_event_rows"] = coins.map(old_counts).fillna(0).astype("int64")
        frame["new_event_rows"] = coins.map(new_counts).fillna(0).astype("int64")
        frame["has_events"] = frame["old_event_rows"].gt(0) | frame["new_event_rows"].gt(0)
        frames.append(frame)
        if comparison == "full":
            new_events = new_events.loc[new_events.index.get_level_values("date") >= begin]
    return pd.concat(frames, ignore_index=True), data_dir


def summarize(run_dir, *, comparison="full", scope="all", data_dir=None, output_dir=None, print_top=30):
    if comparison not in ("full", "weight_only"):
        raise ValueError("COMPARISON 须为 full 或 weight_only")
    if type(print_top) is not int or print_top <= 0:
        raise ValueError("PRINT_TOP 须为正整数")
    if scope not in ("all", "with_events"):
        raise ValueError("SCOPE 须为 all 或 with_events")
    run_dir = Path(run_dir).expanduser().resolve()
    if not run_dir.is_dir():
        raise ValueError("RUN_DIR 目录不存在：{}".format(run_dir))
    all_values, skipped, flag_sources = collect(run_dir, comparison)
    if scope == "with_events":
        all_values, data_dir = with_event_presence(all_values, run_dir, data_dir, comparison)
        values = all_values.loc[all_values["has_events"]].copy()
        excluded_differences = all_values.loc[~all_values["has_events"] & all_values["different"]].copy()
    else:
        values = all_values
        excluded_differences = all_values.iloc[:0].copy()
    differences = values.loc[values["different"]].copy()
    grouped = values.groupby("asset", sort=True)
    asset_summary = grouped.agg(compared_points=("equal", "size"), diff_points=("different", "sum"),
                                non_finite_points=("non_finite", "sum"))
    asset_summary["pass_points"] = asset_summary["compared_points"] - asset_summary["diff_points"]
    asset_summary["diff_rate_pct"] = 100 * asset_summary["diff_points"] / asset_summary["compared_points"]
    metrics = differences.groupby("asset")["abs_difference"].agg(max_abs_difference_on_diff="max", mean_abs_difference_on_diff="mean")
    asset_summary = asset_summary.join(metrics)
    metric_columns = ["max_abs_difference_on_diff", "mean_abs_difference_on_diff"]
    asset_summary.loc[asset_summary["diff_points"].eq(0), metric_columns] = 0.0
    asset_summary = asset_summary.sort_values(["diff_points", "max_abs_difference_on_diff"], ascending=False, kind="stable")
    source_summary = read_json(run_dir / "summary.json")
    run_manifest = read_json(run_dir / "run.json")
    reported_hours = source_summary.get(comparison + "_diff")
    hours_compared = int(all_values["date"].nunique())
    diff_hours = int(differences["date"].nunique())
    all_diff_hours = int(all_values.loc[all_values["different"], "date"].nunique())
    expected_hours = source_summary.get("requested_hours", run_manifest.get("hours"))
    warnings = []
    if reported_hours is not None and int(reported_hours) != all_diff_hours:
        warnings.append("原汇总的差异小时数 {} 与当前已保存明细统计 {} 不一致，请核对目录是否完整。".format(reported_hours, all_diff_hours))
    if expected_hours is not None and int(expected_hours) != hours_compared:
        warnings.append("预期 {} 小时，当前只有 {} 个已完成时点的明细。".format(expected_hours, hours_compared))
    if skipped:
        warnings.append("{} 个明细文件没有成功完成标记，未纳入统计。".format(len(skipped)))
    if not excluded_differences.empty:
        warnings.append("{} 条差异的新旧窗口均无已完成事件，未计入有事件口径，另存 excluded_diff_details.csv，请核对。".format(len(excluded_differences)))
    result = dict(comparison=comparison, scope=scope, run_dir=str(run_dir), replay_status=source_summary.get("status", "unknown"),
                  expected_hours=expected_hours, compared_hours=hours_compared, differing_hours=diff_hours,
                  compared_assets=int(values["asset"].nunique()), compared_asset_values=len(values),
                  differing_asset_values=len(differences), unique_differing_assets=int(differences["asset"].nunique()),
                  diff_rate_pct=100 * len(differences) / len(values) if len(values) else None,
                  non_finite_asset_values=int(values["non_finite"].sum()),
                  reported_diff_hours=reported_hours, skipped_files=skipped, equal_flag_files=flag_sources, warnings=warnings)
    if scope == "with_events":
        result.update(data_dir=str(data_dir), information_definition="窗口内新旧任一侧有二三轮成功事件，零分事件也计入，每个资产时点只计一次",
                      hours_with_events=int(values["date"].nunique()),
                      all_compared_asset_values=len(all_values), all_differing_asset_values=int(all_values["different"].sum()),
                      all_diff_rate_pct=100 * int(all_values["different"].sum()) / len(all_values),
                      excluded_without_events=len(all_values) - len(values), excluded_differing_asset_values=len(excluded_differences))
    suffix = "_with_events" if scope == "with_events" else ""
    target = Path(output_dir).expanduser().resolve() if output_dir is not None else run_dir / ("diff_stats_" + comparison + suffix)
    if target == run_dir:
        raise ValueError("统计输出目录须为单独子目录，不能覆盖原回放目录")
    target.mkdir(parents=True, exist_ok=True)
    asset_summary.to_csv(target / "asset_diff_summary.csv", encoding="utf-8-sig")
    values.to_csv(target / "compared_values.csv", index=False, encoding="utf-8-sig")
    differences.to_csv(target / "diff_details.csv", index=False, encoding="utf-8-sig")
    if scope == "with_events":
        excluded_differences.to_csv(target / "excluded_diff_details.csv", index=False, encoding="utf-8-sig")
    (target / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print("比较 {} 个时点、{} 个资产，共 {} 条资产值。".format(hours_compared, result["compared_assets"], len(values)))
    rate = "{:.4f}%".format(result["diff_rate_pct"]) if len(values) else "无可比资产时点"
    print("差异时点 {} 个；差异资产值 {} 条；涉及 {} 个不同资产；差异占比 {}。".format(
        diff_hours, len(differences), result["unique_differing_assets"], rate))
    if scope == "with_events":
        print("有事件口径：新旧任一侧窗口内有已完成事件即计入，分数为 0 也保留；每个资产时点只计一次。")
        print("全部资产时点 {} 条，排除两边都无事件的 {} 条，当前分母 {} 条。".format(
            len(all_values), result["excluded_without_events"], len(values)))
    if reported_hours is not None:
        print("原日志 {}_diff={}；全资产明细差异时点={}。".format(comparison, reported_hours, all_diff_hours))
    for warning in warnings:
        print("提示：" + warning)
    changed = asset_summary.loc[asset_summary["diff_points"] > 0]
    if not changed.empty:
        print("差异次数最多的资产（完整列表已保存 CSV）：")
        print(changed.head(print_top).to_string())
    print("统计输出目录：{}".format(target))
    return result


def main():
    if "填写" in str(RUN_DIR):
        raise ValueError("请先在脚本顶部填写 RUN_DIR，指向刚才打印 full_diff 的那次回放输出目录")
    return summarize(RUN_DIR, comparison=COMPARISON, scope=SCOPE, data_dir=DATA_DIR,
                     output_dir=OUTPUT_DIR, print_top=PRINT_TOP)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, FileNotFoundError) as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        raise SystemExit(2)
