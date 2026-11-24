"""只计算 weight，并可与旧权重表对应整点比较；不请求 AI 或向量服务。"""

import sys

import numpy as np
import pandas as pd
from crypto_x_ai_live import _compat as core

from _trial import BEIJING, arguments, assets, current_time, dump_json, load_frame, output_dir, score_parameters


def main():
    parser = arguments(__doc__)
    parser.add_argument("--events", default="data/events_completed.pkl", help="已完成二三轮的事件结果表")
    parser.add_argument("--old-weight", help="可选：旧算法产出的 weight DataFrame pkl")
    args = parser.parse_args()
    events = load_frame(args.events)
    instruments = assets(args.assets)
    parameters = score_parameters(args.config_dir, args.weight_param_id)
    _, now = current_time(args.test_time)
    stamp, begin, cutoff = core.weights.live_weight_window(now)
    result = core.weights.build_live_weight(events, as_of=stamp, assets=instruments,
                                             aggregation_method=args.method, parameters=parameters)
    _, written_at = current_time(args.test_time)
    result.index = pd.DatetimeIndex([written_at], name="date")
    target = output_dir("weight")
    result.to_pickle(target / "weight.pkl")
    result.to_csv(target / "weight.csv", encoding="utf-8-sig")
    metadata = dict(current_time=str(now), begin_inclusive=str(begin), cutoff_exclusive=str(cutoff),
                    weight_time=str(written_at),
                    method=args.method, event_rows=len(events), weight_parameters=parameters.to_dict(),
                    comparison="not_requested")
    print("筛选基准时刻（UTC+8）:", stamp)
    print("weight 输出时刻（UTC+8）:", written_at)
    print("created_at + 8h 的筛选区间: [{}, {})".format(begin, cutoff))
    print(result.to_string())
    if args.old_weight:
        # 当前保存的旧版整点 T 对应 [T-75min, T-15min)，所以完整窗口对应下一整点。
        if cutoff.minute != 45:
            metadata["comparison"] = "incomplete_hour_window"
            dump_json(target / "summary.json", metadata)
            print("已计算当前 weight，但当前不是完整小时窗口，不能直接与旧小时表比较。\n"
                  "请在 xx:45~xx:59 试跑，或使用 --test-time '历史日期 16:47:00'。", file=sys.stderr)
            return 2
        old = load_frame(args.old_weight).copy(deep=True)
        if not isinstance(old.index, pd.DatetimeIndex) or not old.index.is_unique:
            raise ValueError("old-weight 必须使用唯一的 DatetimeIndex")
        if not old.columns.is_unique:
            raise ValueError("old-weight 的资产列名必须唯一")
        if old.index.tz is not None:
            old.index = old.index.tz_convert(BEIJING).tz_localize(None)
        reference_time = cutoff.floor("h") + pd.Timedelta(hours=1)
        if reference_time not in old.index:
            raise ValueError("旧表没有对应时刻 {}；请核对旧表日期范围和时间口径".format(reference_time))
        fresh = result.iloc[0]
        missing = [asset for asset in fresh.index if asset not in old.columns]
        # 旧表仅包含事件资产与指定名单的交集。只为不存在的列补 0，原列中的 NaN 不补。
        previous = old.loc[reference_time].reindex(fresh.index, fill_value=0.0).astype("float64")
        diff = pd.DataFrame({"new": fresh, "old": previous, "difference": fresh - previous,
                             "old_column_present": fresh.index.isin(old.columns)})
        diff.index.name = "asset"
        diff.to_csv(target / "comparison.csv", encoding="utf-8-sig")
        passed = bool(np.isfinite(previous.to_numpy()).all()
                      and np.allclose(fresh.to_numpy(), previous.to_numpy(), rtol=1e-9, atol=1e-12))
        metadata.update(comparison="PASS" if passed else "DIFF", old_reference_time=str(reference_time),
                        missing_old_assets=missing, missing_old_assets_policy="zero_for_absent_columns_only")
        dump_json(target / "summary.json", metadata)
        print("旧表对应时刻:", reference_time)
        if missing:
            print("旧表没有以下资产列，本次对账按 0 比较（仍保留这些资产的新结果）：", missing)
        print(diff.to_string())
        print("PASS: 数值一致" if passed else "DIFF: 有差异；请查看 comparison.csv 和 summary.json")
        print("输出目录:", target)
        return 0 if passed else 1
    dump_json(target / "summary.json", metadata)
    print("输出目录:", target)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError, KeyError) as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        raise SystemExit(2)
