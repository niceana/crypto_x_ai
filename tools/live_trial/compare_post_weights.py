"""按同一原帖 ID＋资产比较事件聚合权重，不匹配事件正文，不调用 AI 或数据库。"""

import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from crypto_x_ai_live import _compat as core
import _trial

ROOT = _trial.ROOT

# ==================== 运行前修改这里 ====================
RUN_DIR = "填写那次 outputs/replay_full_... 的绝对路径"
DATA_DIR = ROOT / "data_240h"
CONFIG_DIR = getattr(_trial, "DEFAULT_CONFIG_DIR", ROOT / "configs")  # 须与当次回放评分配置一致
OUTPUT_DIR = None  # 默认 RUN_DIR/post_weight_diff/
# sum/max_abs 和 param_id 自动读取 DATA_DIR/manifest.json。
# ======================================================


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def collect(run_dir):
    posts, events, batches = [], [], 0
    for directory in sorted(run_dir.glob("batch_*")):
        marker = directory / "summary.json"
        if not marker.is_file() or read_json(marker).get("status") != "success":
            continue
        r1 = pd.read_pickle(directory / "round1_results.pkl")
        result = pd.read_pickle(directory / "event_results.pkl")
        if not r1.empty:
            posts.append(r1[["text", "user_name"]])
        if not result.empty:
            events.append(result)
        batches += 1
    if not posts:
        raise ValueError("没有已完成的帖子，请核对 RUN_DIR 是否为 full 回放输出目录")
    posts = pd.concat(posts)
    if list(posts.index.names) != ["date", "id"] or not posts.index.get_level_values("id").is_unique:
        raise ValueError("已保存的第一轮输出须为 (date,id) 且帖子 ID 唯一")
    if events:
        events = pd.concat(events)
        events = events.loc[~events.index.duplicated(keep="last")].sort_index()
    else:
        events = pd.DataFrame(columns=core.validation.RESULT_COLUMNS,
                              index=pd.MultiIndex.from_arrays([[], []], names=["date", "record_id"]))
    return posts, events, batches


def score_pairs(frame, parameters, method, label):
    core.validation.validate_index(frame, "record_id")
    keys = ["post_id", "asset"]
    columns = ["weight", "event_rows", "valid"]
    if frame.empty:
        return pd.DataFrame(columns=columns, index=pd.MultiIndex.from_arrays([[], []], names=keys)), []
    frame = frame.sort_index()
    records, issues = [], []

    def calculate(source):
        if not source["round2_status"].eq("1").all() or not source["round3_status"].eq("1").all():
            raise ValueError("第二或第三轮存在非成功记录，不能按 0 处理")
        basic = core.weights.build_basic_df(source, parameters)
        if len(basic) != len(source):
            raise ValueError("评分记录数与原事件数不一致")
        basic["post_id"] = source["source_post_id"].to_numpy()
        counts = basic.groupby(keys, sort=True).size().rename("event_rows")
        if method == "sum":
            weights = basic.groupby(keys, sort=True)["data"].sum().rename("weight")
        else:
            basic = basic.sort_values("date", kind="stable").reset_index(drop=True)
            positions = basic["data"].abs().groupby([basic["post_id"], basic["asset"]], sort=False).idxmax()
            weights = basic.loc[positions].set_index(keys)["data"].rename("weight")
        result = counts.to_frame().join(weights)
        if not np.isfinite(result["weight"].to_numpy()).all():
            raise ValueError("聚合权重含非有限值")
        result["valid"] = True
        return result[columns]

    try:
        return calculate(frame), issues
    except (ValueError, TypeError, KeyError):
        # 正常数据一次评分；有坏记录时定位具体帖子资产组，不将坏组误当作 0。
        for pair, group in frame.groupby(["source_post_id", "asset"], sort=True):
            try:
                result = calculate(group)
                records.append(result)
            except (ValueError, TypeError, KeyError) as exc:
                records.append(pd.DataFrame([[np.nan, len(group), False]], columns=columns,
                                             index=pd.MultiIndex.from_tuples([pair], names=keys)))
                issues.append(dict(side=label, post_id=pair[0], asset=pair[1], issue=str(exc)))
        return pd.concat(records).sort_index(), issues


def compare_post_weights(run_dir, data_dir, config_dir, *, output_dir=None, parameters=None):
    run_dir, data_dir = Path(run_dir).expanduser().resolve(), Path(data_dir).expanduser().resolve()
    run, manifest = read_json(run_dir / "run.json"), read_json(data_dir / "manifest.json")
    if run.get("mode") != "full" or run["dataset_id"] != manifest["dataset_id"]:
        raise ValueError("RUN_DIR 与 DATA_DIR 不属于同一次 full 回放")
    method = manifest["aggregation_method"]
    core.weight_parameters.validate_aggregation_method(method)
    parameters = parameters or _trial.score_parameters(config_dir, manifest["weight_param_id"])
    posts, new, batches = collect(run_dir)
    old = pd.read_pickle(data_dir / "reference_events.pkl")
    post_ids = posts.index.get_level_values("id")
    instruments = read_json(data_dir / "assets.json")
    instruments = instruments["InstrumentList"] if isinstance(instruments, dict) else instruments
    assets = core.weights.normalize_live_assets(instruments)
    old = old.loc[old["source_post_id"].isin(post_ids) & old["asset"].isin(assets)].copy()
    new = new.loc[new["source_post_id"].isin(post_ids) & new["asset"].isin(assets)].copy()
    old, old_issues = score_pairs(old, parameters, method, "old")
    new, new_issues = score_pairs(new, parameters, method, "new")
    output = old.add_prefix("old_").join(new.add_prefix("new_"), how="outer")
    for name in ("old_weight", "new_weight"):
        output[name] = output[name].astype("float64")
    output["old_present"] = output.index.isin(old.index)
    output["new_present"] = output.index.isin(new.index)
    output["presence"] = np.where(output["old_present"] & output["new_present"], "both", np.where(output["old_present"], "old_only", "new_only"))
    output["comparable"] = output["presence"].eq("both") & output["old_valid"].eq(True) & output["new_valid"].eq(True)
    output["equal"] = None
    valid = output["comparable"]
    output.loc[valid, "equal"] = np.isclose(output.loc[valid, "new_weight"].to_numpy(), output.loc[valid, "old_weight"].to_numpy(), rtol=1e-9, atol=1e-12)
    output["difference"] = output["new_weight"] - output["old_weight"]
    output["abs_difference"] = output["difference"].abs()
    dates = pd.Series(posts.index.get_level_values("date"), index=post_ids)
    users = pd.Series(posts["user_name"].to_numpy(), index=post_ids)
    output["date_utc"] = output.index.get_level_values("post_id").map(dates)
    output["user_name"] = output.index.get_level_values("post_id").map(users)
    common = output[output["presence"].eq("both")]
    compared = output[valid]
    changed = output[valid & output["equal"].eq(False)]
    unmatched = output[output["presence"].ne("both")]
    result = dict(processed_posts=len(posts), aggregation_method=method, weight_parameters=parameters.to_dict(),
                  old_pairs=len(old), new_pairs=len(new), common_pairs=len(common), comparable_pairs=len(compared),
                  equal_pairs=int(compared["equal"].eq(True).sum()), diff_pairs=len(changed),
                  diff_rate_pct=100 * len(changed) / len(compared) if len(compared) else None,
                  posts_with_comparable_weights=int(compared.index.get_level_values("post_id").nunique()),
                  changed_posts=int(changed.index.get_level_values("post_id").nunique()),
                  changed_assets=int(changed.index.get_level_values("asset").nunique()),
                  old_only_pairs=int(output["presence"].eq("old_only").sum()), new_only_pairs=int(output["presence"].eq("new_only").sum()),
                  invalid_common_pairs=int((common["comparable"].eq(False)).sum()), issue_count=len(old_issues) + len(new_issues),
                  completed_batches=batches, expected_batches=manifest["call_count"],
                  scope="同一帖子＋资产的事件聚合权重；非跨帖子时间窗口的最终持仓；只比较共同组合，单边组合另列")
    stats = compared.reset_index().groupby("asset").agg(compared_pairs=("comparable", "size"), diff_pairs=("equal", lambda values: int(values.eq(False).sum())),
                                                        max_abs_difference=("abs_difference", "max"))
    stats["diff_rate_pct"] = 100 * stats["diff_pairs"] / stats["compared_pairs"]
    stats = stats.sort_values(["diff_pairs", "max_abs_difference"], ascending=False, kind="stable")
    target = Path(output_dir).expanduser().resolve() if output_dir is not None else run_dir / "post_weight_diff"
    if target == run_dir or target == data_dir:
        raise ValueError("输出须为独立统计目录")
    target.mkdir(parents=True, exist_ok=True)
    output.to_csv(target / "post_asset_weights.csv", encoding="utf-8-sig")
    changed.to_csv(target / "diff_pairs.csv", encoding="utf-8-sig")
    unmatched.to_csv(target / "unmatched_pairs.csv", encoding="utf-8-sig")
    stats.to_csv(target / "asset_summary.csv", encoding="utf-8-sig")
    pd.DataFrame(old_issues + new_issues, columns=["side", "post_id", "asset", "issue"]).to_csv(target / "comparison_issues.csv", index=False, encoding="utf-8-sig")
    (target / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print("旧结果：帖子＋资产组合 {} 个。".format(result["old_pairs"]))
    print("新结果：帖子＋资产组合 {} 个。".format(result["new_pairs"]))
    print("原帖 {} 条；共同帖子＋资产组合 {} 个，其中可比 {} 个。".format(len(posts), len(common), len(compared)))
    print("权重相同 {} 个、不同 {} 个；涉及 {} 篇帖子、{} 个资产；差异比例 {}。".format(
          result["equal_pairs"], len(changed), result["changed_posts"], result["changed_assets"],
          "{:.2f}%".format(result["diff_rate_pct"]) if len(compared) else "无可比组合"))
    print("旧有新无 {} 个、新有旧无 {} 个；共同但结果无效 {} 个。".format(result["old_only_pairs"], result["new_only_pairs"], result["invalid_common_pairs"]))
    if batches != manifest["call_count"]:
        print("提示：仅统计 {} / {} 个已完成批次。".format(batches, manifest["call_count"]))
    print("统计输出目录：{}".format(target))
    return result


def main():
    if "填写" in str(RUN_DIR):
        raise ValueError("请先在顶部填写 RUN_DIR")
    return compare_post_weights(RUN_DIR, DATA_DIR, CONFIG_DIR, output_dir=OUTPUT_DIR)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, FileNotFoundError) as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        raise SystemExit(2)
