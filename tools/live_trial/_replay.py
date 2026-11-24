"""批量回放的数据切片及对账辅助；不改变业务包。"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

from export_xdata import BEIJING, timestamp


def schedule(first_time, hours):
    if type(hours) is not int or hours <= 0:
        raise ValueError("HOURS 必须为正整数")
    first = timestamp(first_time, BEIJING).tz_convert(BEIJING).tz_localize(None)
    if first.minute != 47 or first.second or first.microsecond or first.nanosecond:
        raise ValueError("FIRST_TIME 须为首个小时的 xx:47:00（UTC+8）")
    return pd.date_range(first.floor("h") + pd.Timedelta(minutes=2), periods=hours * 4, freq="15min", name="date")


def bounds(call_time):
    cutoff = call_time.floor("15min")
    end = cutoff - pd.Timedelta(hours=8)
    return end - pd.Timedelta(minutes=15), end


def select(frame, begin, end):
    dates = frame.index.get_level_values("date")
    return frame.loc[(dates >= begin) & (dates < end)].copy(deep=True)


def post_batch(source, call_time):
    begin, end = bounds(call_time)
    source = select(source, begin, end)
    return pd.DataFrame({"id": list(source.index.get_level_values("id")),
                         "created_at": [value.tz_localize("UTC").isoformat() for value in source.index.get_level_values("date")],
                         "text": source["text"].to_numpy(), "user_name": source["user_name"].to_numpy()},
                        index=pd.DatetimeIndex([call_time.floor("15min")] * len(source), name="date"))


def compare(weight, old, call_time):
    reference = call_time.floor("h") + pd.Timedelta(hours=1)
    if reference not in old.index:
        raise ValueError("旧 weight 缺少对应时刻 {}".format(reference))
    fresh = weight.iloc[0].astype("float64")
    previous = old.loc[reference].reindex(fresh.index, fill_value=0.0).astype("float64")
    finite = np.isfinite(previous.to_numpy()) & np.isfinite(fresh.to_numpy())
    matches = finite & np.isclose(fresh.to_numpy(), previous.to_numpy(), rtol=1e-9, atol=1e-12)
    diff = pd.DataFrame({"new": fresh, "old": previous, "difference": fresh - previous,
                         "old_column_present": fresh.index.isin(old.columns), "equal": matches})
    diff.index.name = "asset"
    info = dict(comparison="PASS" if bool(matches.all()) else "DIFF", old_reference_time=str(reference),
                different_assets=int((~matches).sum()), missing_old_assets=[item for item in fresh.index if item not in old.columns],
                max_abs_difference=float(diff["difference"].abs().max()) if bool(finite.all()) else None)
    return info, diff


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)

