"""只读 xdata 第一轮 AI 结果，统计月度 round2 任务量和逐日 ticker 回看分布。"""

import argparse
import csv
import json
import logging
import math
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from common.execution import month_ranges
from common.time_rules import published_time, table_date_bounds
from common.validation import validate_event_source_index
from initialization.first_round_answers import parse_first_round_answer


logger = logging.getLogger(__name__)
READ_COLUMNS = ["status", "ai_response_content"]
COUNT_COLUMNS = [
    "total_posts", "successful_posts", "pending_posts", "processing_posts", "failed_posts",
    "unknown_status_posts", "invalid_answers", "repaired_answers", "posts_without_events",
    "extracted_events", "events_without_tickers", "round2_records", "round2_asset_records",
    "round2_industry_records", "round2_macro_records",
]
MONTHLY_COLUMNS = ["month", "start_time", "end_time"] + COUNT_COLUMNS
ERROR_COLUMNS = ["date", "id", "error"]
STATUS_COUNTS = {"1": "successful_posts", "0": "pending_posts",
                 "9": "processing_posts", "-1": "failed_posts"}


PERCENTILES = {"p50": 0.50, "p75": 0.75, "p90": 0.90, "p95": 0.95, "p99": 0.99}
STAT_COLUMNS = ["sample_days", "zero_days", "mean", "min"] + list(PERCENTILES) + ["max"]
DAILY_COLUMNS = ["date", "ticker", "lookback_days", "window_start", "window_end",
                 "today_events", "history_events", "has_full_history"]
TICKER_COLUMNS = ["ticker", "lookback_days", "sample", "partial_window_days"] + STAT_COLUMNS
OVERALL_COLUMNS = ["scope", "lookback_days", "sample", "partial_window_days"] + STAT_COLUMNS
SCOPES = ("ALL", "ASSETS", "INDUSTRY", "MACRO")
SAMPLES = ("all_days", "active_days")


def normalize_lookback_days(values):
    if not values or any(type(value) is not int or value <= 0 for value in values):
        raise ValueError("--lookback-days 须为一个或多个正整数天数")
    return tuple(sorted(set(values)))


def distribution_stats(histogram):
    """用频数计算经验分位数：排序后取 ceil(p * N) 位，结果为整数事件量。"""
    size = sum(histogram.values())
    if not size:
        return dict(sample_days=0, zero_days=0, mean=None, min=None,
                    **dict.fromkeys(list(PERCENTILES) + ["max"]))
    values = sorted(histogram)
    result = {"sample_days": size, "zero_days": histogram.get(0, 0),
              "mean": sum(value * histogram[value] for value in values) / size,
              "min": values[0], "max": values[-1]}
    cumulative, position = 0, 0
    for name, percentile in PERCENTILES.items():
        rank = math.ceil(percentile * size)
        while cumulative < rank:
            cumulative += histogram[values[position]]
            position += 1
        result[name] = values[position - 1]
    return result


def write_history_reports(daily_counts, start, end, data_start, lookback_days, output):
    """逐 ticker 输出日初窗口明细；分布仅使用时间跨度完整的窗口，保留零事件日。

    每个 ticker 从读取范围内首次事件日期开始采样；日历由原 date 日期定义。
    仅保留稀疏日计数及事件量频数，不构造全量 ticker × 日期明细 DataFrame。
    """
    windows = normalize_lookback_days(lookback_days)
    by_ticker = defaultdict(dict)
    for (day, ticker), count in daily_counts.items():
        by_ticker[ticker][day] = count
    overall, partial = defaultdict(Counter), Counter()
    ticker_rows, detail_rows = [], 0
    daily_path = output / "daily_ticker_history.csv"
    with daily_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=DAILY_COLUMNS)
        writer.writeheader()
        for ticker_index, ticker in enumerate(sorted(by_ticker), 1):
            counts = by_ticker[ticker]
            first = min(counts)
            last = (end - first).days + 1
            values = np.zeros(last, dtype=np.int64)
            for day, count in counts.items():
                values[(day - first).days] = count
            cumulative = np.concatenate(([0], np.cumsum(values)))
            sample_start = max(start, first)
            for window in windows:
                histograms, partial_days = {sample: Counter() for sample in SAMPLES}, Counter()
                for offset in range((sample_start - first).days, last):
                    day = first + timedelta(days=offset)
                    window_start = day - timedelta(days=window)
                    history = int(cumulative[offset] - cumulative[max(0, offset - window)])
                    today = int(values[offset])
                    full = window_start >= data_start
                    writer.writerow({"date": day.isoformat(), "ticker": ticker,
                                     "lookback_days": window, "window_start": window_start.isoformat(),
                                     "window_end": day.isoformat(), "today_events": today,
                                     "history_events": history, "has_full_history": full})
                    detail_rows += 1
                    scopes = ("ALL", ticker if ticker in ("INDUSTRY", "MACRO") else "ASSETS")
                    for sample in SAMPLES if today else ("all_days",):
                        if full:
                            histograms[sample][history] += 1
                            for scope in scopes:
                                overall[(scope, window, sample)][history] += 1
                        else:
                            partial_days[sample] += 1
                            for scope in scopes:
                                partial[(scope, window, sample)] += 1
                for sample in SAMPLES:
                    ticker_rows.append(dict(ticker=ticker, lookback_days=window, sample=sample,
                                            partial_window_days=partial_days[sample],
                                            **distribution_stats(histograms[sample])))
            if ticker_index % 100 == 0:
                logger.info("逐日回看统计：已处理 %d/%d 个 ticker", ticker_index, len(by_ticker))
    pd.DataFrame(ticker_rows, columns=TICKER_COLUMNS).to_csv(
        output / "ticker_history_distribution.csv", index=False, encoding="utf-8-sig")
    overall_rows = [dict(scope=scope, lookback_days=window, sample=sample,
                         partial_window_days=partial[(scope, window, sample)],
                         **distribution_stats(overall[(scope, window, sample)]))
                    for window in windows for scope in SCOPES for sample in SAMPLES]
    overall_frame = pd.DataFrame(overall_rows, columns=OVERALL_COLUMNS)
    overall_frame.to_csv(output / "history_distribution.csv", index=False, encoding="utf-8-sig")
    return {"lookback_days": list(windows), "tickers": len(by_ticker), "daily_rows": detail_rows,
            "daily_file": str(daily_path),
            "window_rule": "每日0点 [date-lookback_days, date)，不含当天；固定天数，不转换时区。",
            "sampling_rule": "每个ticker从读取范围内首次事件日开始，至统计结束日逐日采样；"
                             "all_days包含无新事件日，active_days仅包含当天有新事件的日期；"
                             "分布只纳入has_full_history=True样本；"
                             "表头时间跨度不足的窗口仅保留在明细并计入partial_window_days。",
            "percentile_rule": "经验分位数：排序后取ceil(p*N)位，返回整数；空样本分位数留空。",
            "overall_sampling_rule": "总体按ticker-day等权，不按当天事件数加权；"
                                     "ASSETS排除INDUSTRY/MACRO，两桶分别报告。",
            "distributions": overall_rows}


def _date_range(start_time, end_time):
    if (start_time is None) != (end_time is None):
        raise ValueError("--start-time 和 --end-time 必须同时填写")
    if start_time is None:
        return None, None
    start, end = date.fromisoformat(start_time), date.fromisoformat(end_time)
    if start.isoformat() != start_time or end.isoformat() != end_time:
        raise ValueError("日期必须为 YYYY-MM-DD")
    if start > end:
        raise ValueError("起始日期不能晚于结束日期")
    return start, end


def summarize_month(frame, start_time, end_time, *, daily_counts=None):
    """只计 status='1' 的合法回答；对象规范化、去重及引号修复沿用事件导入规则。"""
    validate_event_source_index(frame)
    if not set(READ_COLUMNS).issubset(frame.columns):
        raise ValueError("第一轮结果缺少 status 或 ai_response_content")
    start, end = _date_range(start_time, end_time)
    if start is None or (start.year, start.month) != (end.year, end.month):
        raise ValueError("统计分段必须位于同一自然月")
    stamps = [published_time(key[0]) for key in frame.index]
    if any(not start <= stamp.date() <= end for stamp in stamps):
        raise ValueError("xdata 返回了本次统计日期范围以外的记录")

    counts = dict.fromkeys(COUNT_COLUMNS, 0)
    counts.update(month=start.strftime("%Y-%m"), start_time=start_time, end_time=end_time,
                  total_posts=len(frame))
    errors = []
    for key, status, content in zip(frame.index, frame["status"], frame["ai_response_content"]):
        status_column = STATUS_COUNTS.get(status) if isinstance(status, str) else None
        counts[status_column or "unknown_status_posts"] += 1
        if status_column != "successful_posts":
            continue
        try:
            body, repaired = parse_first_round_answer(content)
        except (ValueError, TypeError, UnicodeError) as exc:
            counts["invalid_answers"] += 1
            errors.append({"date": str(key[0]), "id": str(key[1]), "error": str(exc)})
            continue
        counts["repaired_answers"] += int(repaired)
        counts["posts_without_events"] += int(not body["events"])
        counts["extracted_events"] += len(body["events"])
        for event in body["events"]:
            assets = set(ticker.strip().upper() for ticker in event["tickers"])
            if daily_counts is not None:
                day = published_time(key[0]).date()
                for ticker in assets:
                    daily_counts[(day, ticker)] += 1
            counts["events_without_tickers"] += int(not assets)
            counts["round2_records"] += len(assets)
            counts["round2_industry_records"] += int("INDUSTRY" in assets)
            counts["round2_macro_records"] += int("MACRO" in assets)
            counts["round2_asset_records"] += len(assets - {"INDUSTRY", "MACRO"})
    return counts, errors


def stat_media_round1(cli, source_path, *, start_time=None, end_time=None, output_dir=None,
                     lookback_days=(30,)):
    """按月只读两列，输出 CSV 与汇总；不读取 round2 表，不调用 AI，不写 xdata。"""
    if not isinstance(source_path, str) or not source_path.strip():
        raise ValueError("请填写 media_round1 的完整 xdata 路径")
    start, end = _date_range(start_time, end_time)
    windows = normalize_lookback_days(lookback_days)
    data_start, data_end = table_date_bounds(cli.read_meta(source_path))
    if start is None:
        start, end = data_start, data_end
    rows, errors, seen_ids = [], [], set()
    daily_counts = Counter()
    history_totals = dict.fromkeys(COUNT_COLUMNS, 0)
    read_start = min(start, max(data_start, start - timedelta(days=max(windows)))) if data_start is not None else None
    if start is not None:
        # 空源表仍按显式范围输出零值月份；无需读取任何前史或行。
        for begin, finish in month_ranges(read_start or start, end):
            logger.info("读取 %s：%s 至 %s", source_path, begin, finish)
            if data_start is None:
                frame = pd.DataFrame(columns=READ_COLUMNS, index=pd.MultiIndex.from_arrays(
                    [pd.DatetimeIndex([]), []], names=["date", "id"]))
            else:
                frame = cli.read(source_path, start_time=begin, end_time=finish, columns=READ_COLUMNS)
            # 整个读取分段先校验，不能因分割范围而漏掉越界数据或重复索引。
            validate_event_source_index(frame)
            stamps = [published_time(key[0]).date() for key in frame.index]
            if any(not date.fromisoformat(begin) <= day <= date.fromisoformat(finish) for day in stamps):
                raise ValueError("xdata 返回了本次统计日期范围以外的记录")
            ids = {str(value) for value in frame.index.get_level_values("id")}
            if seen_ids & ids:
                raise ValueError("不同月份存在重复原帖 ID：{}".format(sorted(seen_ids & ids)[:3]))
            seen_ids.update(ids)
            if date.fromisoformat(begin) < start:
                history_end = min(date.fromisoformat(finish), start - timedelta(days=1)).isoformat()
                prior = frame.loc[[day < start for day in stamps]]
                history_counts, invalid = summarize_month(prior, begin, history_end, daily_counts=daily_counts)
                for name in COUNT_COLUMNS:
                    history_totals[name] += history_counts[name]
                errors.extend(dict(row, scope="prehistory") for row in invalid)
            if date.fromisoformat(finish) >= start:
                scope_start = max(start, date.fromisoformat(begin)).isoformat()
                current = frame.loc[[day >= start for day in stamps]]
                counts, invalid = summarize_month(current, scope_start, finish, daily_counts=daily_counts)
                rows.append(counts)
                errors.extend(dict(row, scope="report") for row in invalid)
                logger.info("%s：帖子=%d，成功=%d，事件=%d，round2任务=%d，非法回答=%d",
                            counts["month"], counts["total_posts"], counts["successful_posts"],
                            counts["extracted_events"], counts["round2_records"], counts["invalid_answers"])
            del frame

    monthly = pd.DataFrame(rows, columns=MONTHLY_COLUMNS)
    totals = {name: sum(row[name] for row in rows) for name in COUNT_COLUMNS}
    stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
    output = (Path(output_dir) if output_dir is not None else
              Path(__file__).resolve().parent.parent / "reports" / ("media_round1_stats_" + stamp)).resolve()
    output.mkdir(parents=True, exist_ok=False)
    monthly.to_csv(output / "monthly_counts.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(errors, columns=["scope"] + ERROR_COLUMNS).to_csv(
        output / "invalid_answers.csv", index=False, encoding="utf-8-sig")
    logger.info("开始逐日 ticker 回看分布统计：窗口天数=%s", list(windows))
    # 表尾之后尚无观测，不能把这些日期的零值混入选参分布。
    history_end = min(end, data_end) if data_end is not None else end
    history_report = write_history_reports(daily_counts, start, history_end, data_start, windows, output)
    history_report["sample_end_time"] = history_end.isoformat() if history_end is not None else None
    summary = {
        "source_path": source_path,
        "start_time": start.isoformat() if start is not None else None,
        "end_time": end.isoformat() if end is not None else None,
        "months": len(rows), "totals": totals, "database_modified": False,
        "output_dir": str(output),
        "source_start_time": data_start.isoformat() if data_start is not None else None,
        "source_end_time": data_end.isoformat() if data_end is not None else None,
        "history_read_start_time": read_start.isoformat() if read_start is not None else None,
        "prehistory_totals": history_totals, "history_report": history_report,
        "counting_rule": "按原帖 date 所在自然月（不转换时区）；仅统计 status='1' 的合法回答；"
                         "每个 event × 去空白转大写后去重的 ticker 计一条 round2 任务，"
                         "包含 INDUSTRY/MACRO；空 tickers 不生成任务；不跨帖去重。"
                         "这是第一轮结果可展开的任务总量，未按资产名单或已有表尾截断，"
                         "不代表实际新增入库数或 round2 归组后的独立事项数。",
    }
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(monthly[["month", "total_posts", "successful_posts", "extracted_events",
                   "round2_records", "invalid_answers"]].to_string(index=False))
    print("合计 round2 任务数：{}".format(totals["round2_records"]))
    print("日初回看分布（完整窗口、当天有新事件的ticker-day）：")
    distributions = pd.DataFrame(history_report["distributions"])
    print(distributions.loc[distributions["sample"].eq("active_days"),
                            ["scope", "lookback_days", "sample_days", "p50", "p90", "p95", "p99", "max"]]
          .to_string(index=False))
    if any(part["successful_posts"] != part["total_posts"] or part["invalid_answers"]
           for part in (totals, history_totals)):
        print("存在未成功帖子或非法回答；任务数仅覆盖当前成功且可解析的 AI 结果，详见报告。")
    print("报告目录：{}".format(output))
    return summary


def main(argv=None, cli=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-path", required=True, help="media_round1 的完整 xdata 路径")
    parser.add_argument("--start-time", help="可选起始日 YYYY-MM-DD，含当天；省略时统计全表")
    parser.add_argument("--end-time", help="可选结束日 YYYY-MM-DD，含当天；须与起始日同时填写")
    parser.add_argument("--output-dir", help="尚不存在的本地报告目录；默认 reports 下北京时间戳目录")
    parser.add_argument("--lookback-days", type=int, nargs="+", default=[30],
                        help="回看天数，默认30；可填写多个值对比，例如 7 14 30 60")
    args = parser.parse_args(argv)
    try:
        _date_range(args.start_time, args.end_time)
        normalize_lookback_days(args.lookback_days)
        if not args.source_path.strip():
            raise ValueError("--source-path 不能为空")
    except ValueError as exc:
        parser.error(str(exc))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    return stat_media_round1(cli, args.source_path, start_time=args.start_time,
                            end_time=args.end_time, output_dir=args.output_dir, lookback_days=args.lookback_days)


if __name__ == "__main__":
    main()
