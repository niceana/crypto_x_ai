"""按原帖比较已保存的新旧三轮结果；不调用 AI，不比较解释或请求元数据。"""

import json
from pathlib import Path
import sys

import pandas as pd
from crypto_x_ai_live import _compat as core

ROOT = Path(__file__).resolve().parent

# ==================== 运行前修改这里 ====================
RUN_DIR = "填写那次 outputs/replay_full_... 的绝对路径"
DATA_DIR = ROOT / "data_240h"     # 当时 export_hours 导出的数据目录
COMPARE_ROUND1 = True
OLD_ROUND1_PKL = None            # 已有旧第一轮结果 PKL 时填路径，不读 xdata
OLD_ROUND1_PATH = None           # 否则只读旧第一轮任务表；None 沿用 manifest 中的 POSTS_PATH
OUTPUT_DIR = None               # 默认 RUN_DIR/ai_result_diff/
# ======================================================

LABELS = ("direction", "materiality", "execution_stage", "persistence")


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def posts_by_id(frame):
    if not isinstance(frame.index, pd.MultiIndex) or list(frame.index.names) != ["date", "id"]:
        raise ValueError("第一轮结果须保留 (date,id) 索引")
    required = {"status", "text", "user_name", "ai_response_content"}
    if not required.issubset(frame.columns):
        raise ValueError("缺少旧第一轮回答字段，请将 OLD_ROUND1_PATH 填为旧第一轮任务表，而不是原始帖子表")
    frame = frame[list(sorted(required))].copy()
    frame["date_utc"] = pd.DatetimeIndex(pd.to_datetime(frame.index.get_level_values("date"), utc=True)).tz_localize(None)
    ids = [str(value) for value in frame.index.get_level_values("id")]
    if len(ids) != len(set(ids)):
        raise ValueError("第一轮结果存在重复原帖 ID，不能重复计数")
    frame.index = pd.Index(ids, name="post_id")
    return frame


def extraction(content):
    body, _ = core.event_records.parse_first_round_answer(content)
    # 事件顺序、ticker 顺序或大小写不影响事件集合；正文不做语义改写。
    events = [dict(event=item["event"], tickers=sorted(set(value.strip().upper() for value in item["tickers"]))) for item in body["events"]]
    return sorted(events, key=encoded)


def collect_new(run_dir):
    posts, events, batches = [], [], 0
    for directory in sorted(run_dir.glob("batch_*")):
        marker = directory / "summary.json"
        if not marker.is_file() or read_json(marker).get("status") != "success":
            continue
        r1, event = directory / "round1_results.pkl", directory / "event_results.pkl"
        if not r1.is_file() or not event.is_file():
            raise ValueError("已完成批次缺少处理结果文件：{}；须选择 full 模式的回放目录".format(directory))
        frame = pd.read_pickle(r1)
        if not frame.empty:
            posts.append(frame)
        frame = pd.read_pickle(event)
        if not frame.empty:
            events.append(frame)
        batches += 1
    if not posts:
        raise ValueError("没有找到已成功处理的帖子，请核对 RUN_DIR 是否为完整流程回放目录")
    new_posts = posts_by_id(pd.concat(posts))
    if events:
        new_events = pd.concat(events)
        new_events = new_events.loc[~new_events.index.duplicated(keep="last")].copy()
    else:
        new_events = pd.DataFrame(columns=["asset", "event", "event_id", "source_post_id", "event_relation", "round2_status", "round3_status", "round3_ai_response"],
                                  index=pd.MultiIndex.from_arrays([[], []], names=["date", "record_id"]))
    return new_posts, new_events, batches


def old_first_round(new_posts, manifest, *, snapshot=None, table_path=None, cli=None):
    if snapshot is not None:
        return posts_by_id(pd.read_pickle(Path(snapshot).expanduser())), str(snapshot)
    table_path = table_path or manifest["source_paths"]["POSTS_PATH"]
    if not table_path or "填写" in table_path:
        raise ValueError("请填写 OLD_ROUND1_PATH 为旧第一轮任务表路径，或提供 OLD_ROUND1_PKL")
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    print("只读旧第一轮任务表：{}".format(table_path), flush=True)
    result = cli.read(table_path, start_time=new_posts["date_utc"].min().date().isoformat(),
                      end_time=new_posts["date_utc"].max().date().isoformat(),
                      columns=["status", "text", "user_name", "ai_response_content"])
    return posts_by_id(result), table_path


def event_groups(frame, coins):
    frame = frame.loc[frame["asset"].isin(coins)].copy()
    anchors = {str(key[1]): (str(row["source_post_id"]), row["asset"], row["event"]) for key, row in frame.iterrows()}
    grouped = {str(post_id): group for post_id, group in frame.groupby("source_post_id", sort=False)}
    return grouped, anchors


def input_signature(rows):
    return sorted((row["asset"], row["event"]) for row in rows)


def relation_result(rows, anchors):
    output = []
    for row in rows:
        if row["round2_status"] != "1":
            raise ValueError("第二轮未成功，不能计为结果相同")
        group_id = str(row["event_id"])
        if row["event_relation"] not in ("new", "update", "repeat") or not group_id:
            raise ValueError("第二轮关系或匹配组为空/非法")
        # 回放期内根事件用帖子/资产/事件作为锚点，避免只换事件顺序导致编号变化。
        group = ("event",) + anchors[group_id] if group_id in anchors else ("historical_id", group_id)
        output.append((row["asset"], row["event"], row["event_relation"], group))
    return sorted(output, key=encoded)


def impact_result(rows):
    output = []
    for row in rows:
        if row["round3_status"] != "1":
            raise ValueError("第三轮未成功，不能计为结果相同")
        body = json.loads(row["round3_ai_response"])
        if "ai_response_content" in body:
            body = body["ai_response_content"]
            body = json.loads(body) if isinstance(body, str) else body
        if not isinstance(body, dict) or any(not isinstance(body.get(name), str) or not body[name] for name in LABELS):
            raise ValueError("第三轮缺少有效的四项结果标签")
        output.append((row["asset"], row["event"], tuple(body[name] for name in LABELS)))
    return sorted(output, key=encoded)


def result_snapshot(rows):
    values = []
    for row in rows:
        value = {name: row.get(name, "") for name in ("asset", "event", "event_relation", "event_id")}
        try:
            result = impact_result([row])[0][2]
            value.update(dict(zip(LABELS, result)))
        except (ValueError, TypeError, KeyError):
            value["labels_unavailable"] = True
        values.append(value)
    return sorted(values, key=encoded)


def compare_posts(run_dir, data_dir, *, compare_round1=True, old_round1_pkl=None, old_round1_path=None,
                  cli=None, output_dir=None):
    run_dir, data_dir = Path(run_dir).expanduser().resolve(), Path(data_dir).expanduser().resolve()
    manifest = read_json(data_dir / "manifest.json")
    run = read_json(run_dir / "run.json")
    if run.get("mode") != "full" or run["dataset_id"] != manifest["dataset_id"]:
        raise ValueError("RUN_DIR 必须是使用该 DATA_DIR 的 full 回放，不能混用数据")
    new_posts, new_events, batches = collect_new(run_dir)
    instruments = read_json(data_dir / "assets.json")
    instruments = instruments["InstrumentList"] if isinstance(instruments, dict) else instruments
    coins = {value.strip().upper().split(".", 1)[0] for value in instruments}
    reference = pd.read_pickle(data_dir / "reference_events.pkl")
    old_groups, old_anchors = event_groups(reference, coins)
    new_groups, new_anchors = event_groups(new_events, coins)
    old_posts, old_source = (None, None)
    if compare_round1:
        old_posts, old_source = old_first_round(new_posts, manifest, snapshot=old_round1_pkl, table_path=old_round1_path, cli=cli)
    comparisons, details, issues = [], [], []
    for post_id, post in new_posts.iterrows():
        row = dict(post_id=post_id, date_utc=str(post["date_utc"]), user_name=post["user_name"],
                   round1_comparable=False, round1_changed=None, event_set_changed=False,
                   round2_comparable=False, round2_changed=None, round3_comparable=False, round3_changed=None,
                   input_changed=False, any_result_changed=False, comparable=False)
        old_r1 = new_r1 = None
        if compare_round1:
            try:
                if post_id not in old_posts.index:
                    raise ValueError("旧第一轮表没有该原帖 ID")
                previous = old_posts.loc[post_id]
                if any(previous[name] != post[name] for name in ("date_utc", "text", "user_name")):
                    row["input_changed"] = True
                    raise ValueError("新旧原帖发布时间、正文或账号不同，不计为相同输入下的 AI 变化")
                if previous["status"] != "1" or post["status"] != "1":
                    raise ValueError("第一轮新旧结果尚未全部成功")
                old_r1, new_r1 = extraction(previous["ai_response_content"]), extraction(post["ai_response_content"])
                row.update(round1_comparable=True, round1_changed=old_r1 != new_r1)
            except (ValueError, TypeError, KeyError) as exc:
                issues.append(dict(post_id=post_id, stage="round1", issue=str(exc)))
        old_rows = old_groups[post_id].to_dict("records") if post_id in old_groups else []
        new_rows = new_groups[post_id].to_dict("records") if post_id in new_groups else []
        row.update(old_event_rows=len(old_rows), new_event_rows=len(new_rows))
        if not row["input_changed"]:
            row["event_set_changed"] = input_signature(old_rows) != input_signature(new_rows)
            if not row["event_set_changed"] and old_rows:
                for stage, function in (("round2", lambda rows, anchors: relation_result(rows, anchors)),
                                         ("round3", lambda rows, anchors: impact_result(rows))):
                    try:
                        changed = function(old_rows, old_anchors) != function(new_rows, new_anchors)
                        row.update({stage + "_comparable": True, stage + "_changed": changed})
                    except (ValueError, TypeError, KeyError) as exc:
                        issues.append(dict(post_id=post_id, stage=stage, issue=str(exc)))
            row["comparable"] = row["round1_comparable"] or row["event_set_changed"] or row["round2_comparable"] or row["round3_comparable"]
            row["any_result_changed"] = any(row[name] is True for name in ("round1_changed", "event_set_changed", "round2_changed", "round3_changed"))
        row["comparison_complete"] = bool(row["comparable"] and not row["input_changed"]
            and (not compare_round1 or row["round1_comparable"])
            and (row["event_set_changed"] or not old_rows and not new_rows or row["round2_comparable"] and row["round3_comparable"]))
        comparisons.append(row)
        if row["any_result_changed"]:
            details.append(dict(row, text=post["text"], old_round1=encoded(old_r1), new_round1=encoded(new_r1),
                                old_event_results=encoded(result_snapshot(old_rows)), new_event_results=encoded(result_snapshot(new_rows))))
    frame = pd.DataFrame(comparisons)
    total, comparable = len(frame), int(frame["comparable"].sum())
    changed = int(frame["any_result_changed"].sum())
    result = dict(processed_posts=total, comparable_posts=comparable, changed_posts=changed,
                  changed_rate_pct=100 * changed / comparable if comparable else None,
                  completely_compared_posts=int(frame["comparison_complete"].sum()),
                  incomplete_comparison_posts=int((~frame["comparison_complete"]).sum()),
                  round1_compared=bool(compare_round1), old_round1_source=old_source,
                  round1_comparable_posts=int(frame["round1_comparable"].sum()), round1_changed_posts=int(frame["round1_changed"].eq(True).sum()),
                  event_set_changed_posts=int(frame["event_set_changed"].sum()),
                  round2_comparable_posts=int(frame["round2_comparable"].sum()), round2_changed_posts=int(frame["round2_changed"].eq(True).sum()),
                  round3_comparable_posts=int(frame["round3_comparable"].sum()), round3_changed_posts=int(frame["round3_changed"].eq(True).sum()),
                  input_changed_posts=int(frame["input_changed"].sum()), issue_count=len(issues), completed_batches=batches,
                  expected_batches=manifest["call_count"], ignored_fields=["new_information", "materiality_reason", "reasoning_content", "usage_json", "response_created"],
                  scope="第一轮覆盖原帖全部提取结果；第二三轮仅统计指定资产，事件集合变化时不强行对齐后续事件")
    target = Path(output_dir).expanduser().resolve() if output_dir is not None else run_dir / "ai_result_diff"
    if target == run_dir or target == data_dir:
        raise ValueError("统计输出须为独立目录，不能覆盖原回放或导出目录")
    target.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target / "post_comparison.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(details, columns=list(frame.columns) + ["text", "old_round1", "new_round1", "old_event_results", "new_event_results"]).to_csv(target / "changed_posts.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(issues, columns=["post_id", "stage", "issue"]).to_csv(target / "comparison_issues.csv", index=False, encoding="utf-8-sig")
    (target / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    if old_posts is not None:
        snapshot = old_posts.loc[old_posts.index.intersection(new_posts.index)].copy()
        snapshot.index = pd.MultiIndex.from_arrays([snapshot.pop("date_utc"), snapshot.index], names=["date", "id"])
        snapshot.to_pickle(target / "old_round1_snapshot.pkl")
    print("已处理原帖 {} 条，可比较 {} 条，结果有变化 {} 条，占可比较帖子 {}。".format(total, comparable, changed,
          "{:.2f}%".format(result["changed_rate_pct"]) if comparable else "无可比结果"))
    for stage in ("round1", "round2", "round3"):
        print("{}：可比 {} 条，结果变化 {} 条。".format(stage, result[stage + "_comparable_posts"], result[stage + "_changed_posts"]))
    print("目标事件集合变化 {} 条；输入变化 {} 条；待核对问题 {} 项。".format(result["event_set_changed_posts"], result["input_changed_posts"], len(issues)))
    if result["incomplete_comparison_posts"]:
        print("提示：{} 条帖子未能完整比较，不能把这些帖子直接视为三轮结果全部相同。".format(result["incomplete_comparison_posts"]))
    if batches != manifest["call_count"]:
        print("提示：仅统计 {} / {} 个已完成批次，不是整段回放结果。".format(batches, manifest["call_count"]))
    print("统计输出目录：{}".format(target))
    return result


def main():
    if "填写" in str(RUN_DIR):
        raise ValueError("请先在顶部填写 RUN_DIR")
    return compare_posts(RUN_DIR, DATA_DIR, compare_round1=COMPARE_ROUND1, old_round1_pkl=OLD_ROUND1_PKL,
                         old_round1_path=OLD_ROUND1_PATH, output_dir=OUTPUT_DIR)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, FileNotFoundError, ImportError) as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        raise SystemExit(2)
