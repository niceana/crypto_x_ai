"""月度统计与实际事件展开口径一致，并且只读 xdata。"""

import contextlib
import io
import json
import tempfile
import unittest
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import Mock

import pandas as pd

from entrypoints.stat_media_round1 import (
    distribution_stats, main, normalize_lookback_days, stat_media_round1,
    summarize_month, write_history_reports,
)
from initialization.event_records import expand_first_round


def source(rows):
    index = pd.MultiIndex.from_arrays(
        [pd.DatetimeIndex([row[0] for row in rows]), [str(i) for i in range(len(rows))]],
        names=["date", "id"])
    return pd.DataFrame({"status": [row[1] for row in rows],
                         "ai_response_content": [row[2] for row in rows],
                         "text": "原始正文", "user_name": "media"}, index=index)


def answer(*tickers):
    return json.dumps({"events": [{"event": "具体事件", "tickers": values} for values in tickers]},
                      ensure_ascii=False)


class MediaRound1StatsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / "report"
        self.cli = Mock(spec=["read", "read_meta"])

    def run_stats(self, **kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            return stat_media_round1(self.cli, "media_round1", output_dir=self.output, **kwargs)

    def serve(self, frame):
        dates = frame.index.get_level_values("date")

        def read(path, start_time, end_time, columns):
            begin = pd.Timestamp(start_time)
            end = pd.Timestamp(end_time) + pd.Timedelta(days=1)
            return frame.loc[(dates >= begin) & (dates < end), columns].copy(deep=True)

        self.cli.read.side_effect = read
        self.cli.read_meta.return_value = {"meta": {"start_time": str(dates.min()),
                                                    "end_time": str(dates.max())}}

    def test_counts_match_import_with_normalization_buckets_and_repeated_disclosures(self):
        frame = source([
            ("2026-01-01", "1", answer([" btc ", "BTC", "eth", "industry", "MACRO"], [], ["BTC"])),
            ("2026-01-02", "1", answer(["BTC"])),
            ("2026-01-03", "1", answer()),
        ])
        original = frame.copy(deep=True)
        counts, errors = summarize_month(frame, "2026-01-01", "2026-01-31")
        expanded = expand_first_round(frame)
        self.assertEqual(counts["round2_records"], len(expanded))
        self.assertEqual(counts["round2_records"], 6)
        self.assertEqual((counts["round2_asset_records"], counts["round2_industry_records"],
                          counts["round2_macro_records"]), (4, 1, 1))
        self.assertEqual((counts["extracted_events"], counts["events_without_tickers"],
                          counts["posts_without_events"]), (4, 1, 1))
        self.assertEqual(errors, [])
        pd.testing.assert_frame_equal(frame, original)

    def test_incomplete_statuses_and_bad_success_answers_are_explicitly_reported(self):
        frame = source([("2026-01-01", status, content) for status, content in [
            ("1", answer(["ETH"])), ("1", "invalid JSON"),
            ("0", answer(["BTC"])), ("9", "invalid JSON"), ("-1", answer(["BTC"])),
            (1, answer(["BTC"])), (None, answer(["BTC"])),
        ]])
        self.serve(frame)
        summary = self.run_stats()
        counts = summary["totals"]
        self.assertEqual((counts["total_posts"], counts["successful_posts"],
                          counts["invalid_answers"], counts["round2_records"]), (7, 2, 1, 1))
        self.assertEqual((counts["pending_posts"], counts["processing_posts"],
                          counts["failed_posts"], counts["unknown_status_posts"]), (1, 1, 1, 2))
        invalid = pd.read_csv(self.output / "invalid_answers.csv", dtype=str)
        self.assertEqual(list(invalid["id"]), ["1"])
        self.assertFalse(summary["database_modified"])

    def test_historical_quote_repair_matches_import_without_mutating_source(self):
        content = '{"events":[{"event":"机构称加密货币"正常化"","tickers":[" btc ","BTC"]}]}'
        frame = source([("2026-01-01", "1", content)])
        counts, errors = summarize_month(frame, "2026-01-01", "2026-01-31")
        with self.assertLogs("initialization.event_records", level="WARNING"):
            expected = expand_first_round(frame)
        self.assertEqual((counts["repaired_answers"], counts["round2_records"]), (1, len(expected)))
        self.assertEqual(errors, [])
        self.assertEqual(frame.iloc[0]["ai_response_content"], content)

    def test_month_boundaries_leap_year_empty_month_and_only_two_columns_read(self):
        frame = source([
            ("2024-02-29 23:59:59.999999999", "1", answer(["BTC", "ETH"])),
            ("2024-04-01 00:00:00", "1", answer(["MACRO"])),
        ])
        self.serve(frame)
        summary = self.run_stats()
        monthly = pd.read_csv(self.output / "monthly_counts.csv")
        self.assertEqual(list(monthly["month"]), ["2024-02", "2024-03", "2024-04"])
        self.assertEqual(list(monthly["round2_records"]), [2, 0, 1])
        self.assertEqual(summary["totals"]["round2_records"], 3)
        self.assertEqual(json.loads((self.output / "summary.json").read_text(encoding="utf-8")), summary)
        self.cli.read_meta.assert_called_once_with("media_round1")
        self.assertEqual([call.kwargs for call in self.cli.read.call_args_list], [
            {"start_time": "2024-02-29", "end_time": "2024-02-29", "columns": ["status", "ai_response_content"]},
            {"start_time": "2024-03-01", "end_time": "2024-03-31", "columns": ["status", "ai_response_content"]},
            {"start_time": "2024-04-01", "end_time": "2024-04-01", "columns": ["status", "ai_response_content"]},
        ])

    def test_cli_optional_range_is_inclusive_and_reads_prehistory(self):
        frame = source([
            ("2026-01-14 23:59:59", "1", answer(["BTC"])),
            ("2026-01-15 00:00:00", "1", answer(["ETH"])),
            ("2026-02-10 23:59:59.999999999", "1", answer(["MACRO"])),
            ("2026-02-11 00:00:00", "1", answer(["BTC"])),
        ])
        self.serve(frame)
        with contextlib.redirect_stdout(io.StringIO()) as console:
            summary = main(["--source-path", "media_round1", "--start-time", "2026-01-15",
                            "--end-time", "2026-02-10", "--output-dir", str(self.output)], cli=self.cli)
        self.assertEqual(summary["totals"]["round2_records"], 2)
        self.assertIn("round2_records", console.getvalue())
        self.cli.read_meta.assert_called_once_with("media_round1")
        self.assertEqual(summary["prehistory_totals"]["round2_records"], 1)
        monthly = pd.read_csv(self.output / "monthly_counts.csv")
        self.assertEqual(list(monthly["round2_records"]), [1, 1])

    def test_empty_table_outputs_headers_and_zero_totals_without_reading_rows(self):
        self.cli.read_meta.return_value = {"meta": {"start_time": None, "end_time": None}}
        summary = self.run_stats()
        self.assertEqual((summary["months"], summary["totals"]["round2_records"]), (0, 0))
        self.assertTrue(pd.read_csv(self.output / "monthly_counts.csv").empty)
        self.assertTrue(pd.read_csv(self.output / "invalid_answers.csv").empty)
        self.cli.read.assert_not_called()

    def test_invalid_ranges_or_source_structure_stop_without_publishing_report(self):
        for start, end in [("2026-01-01", None), ("2026-02-01", "2026-01-01"),
                           ("2026-02-30", "2026-03-01")]:
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                self.run_stats(start_time=start, end_time=end)
        self.cli.read_meta.assert_not_called()
        self.cli.read.assert_not_called()
        self.cli.read_meta.return_value = {"meta": {"start_time": "2026-01-01", "end_time": "2026-02-01"}}
        out_of_range = source([("2026-02-01", "1", answer(["BTC"]))])
        for frame in [out_of_range, out_of_range.drop(columns=["status"]),
                      out_of_range.reset_index()]:
            self.cli.read.return_value = frame
            with self.subTest(columns=list(frame.columns)), self.assertRaises(ValueError):
                self.run_stats(start_time="2026-01-01", end_time="2026-01-31")
        self.assertFalse(self.output.exists())

    def test_prehistory_and_report_counts_are_separate_for_multiple_windows(self):
        frame = source([
            ("2026-01-01", "1", answer(["BTC"])),
            ("2026-01-31 23:59:59.999999999", "1", answer(["BTC", "ETH"])),
            ("2026-02-01", "1", answer(["BTC"])),
        ])
        self.serve(frame)
        summary = self.run_stats(start_time="2026-02-01", end_time="2026-02-01", lookback_days=[7, 30])
        self.assertEqual(summary["totals"]["round2_records"], 1)
        self.assertEqual(summary["prehistory_totals"]["round2_records"], 2)
        self.assertEqual(self.cli.read.call_args_list[0].kwargs["start_time"], "2026-01-02")
        daily = pd.read_csv(self.output / "daily_ticker_history.csv")
        btc = daily.loc[daily["ticker"].eq("BTC")]
        self.assertEqual(list(btc["history_events"]), [1, 1])
        self.assertEqual(list(btc["today_events"]), [1, 1])
        self.assertTrue(btc["has_full_history"].all())
        monthly = pd.read_csv(self.output / "monthly_counts.csv")
        self.assertEqual(list(monthly["month"]), ["2026-02"])

    def test_invalid_lookback_days_stop_before_reading_database(self):
        for values in [[], [0], [-1], [True], [1.5]]:
            with self.subTest(values=values), self.assertRaises(ValueError):
                self.run_stats(lookback_days=values)
        self.cli.read_meta.assert_not_called()
        self.cli.read.assert_not_called()

    def test_explicit_range_keeps_empty_months_but_does_not_sample_after_source_end(self):
        frame = source([("2026-02-01", "1", answer(["BTC"]))])
        self.serve(frame)
        summary = self.run_stats(start_time="2026-01-01", end_time="2026-03-31")
        monthly = pd.read_csv(self.output / "monthly_counts.csv")
        self.assertEqual(list(monthly["month"]), ["2026-01", "2026-02", "2026-03"])
        self.assertEqual(list(monthly["round2_records"]), [0, 1, 0])
        daily = pd.read_csv(self.output / "daily_ticker_history.csv")
        self.assertEqual(list(daily["date"]), ["2026-02-01"])
        self.assertEqual(summary["history_report"]["sample_end_time"], "2026-02-01")

    def test_bad_prehistory_is_reported_separately_and_does_not_enter_monthly_totals(self):
        frame = source([("2026-01-01", "1", answer(["BTC"])),
                        ("2026-01-31", "1", "bad JSON"),
                        ("2026-02-01", "1", answer(["ETH"]))])
        self.serve(frame)
        summary = self.run_stats(start_time="2026-02-01", end_time="2026-02-01")
        self.assertEqual(summary["totals"]["invalid_answers"], 0)
        self.assertEqual(summary["prehistory_totals"]["invalid_answers"], 1)
        invalid = pd.read_csv(self.output / "invalid_answers.csv")
        self.assertEqual(list(invalid["scope"]), ["prehistory"])

    def test_same_post_id_in_different_months_is_rejected(self):
        frame = source([("2026-01-01", "1", answer(["BTC"])),
                        ("2026-02-01", "1", answer(["ETH"]))])
        frame.index = pd.MultiIndex.from_arrays([frame.index.get_level_values("date"), ["same", "same"]],
                                               names=["date", "id"])
        self.serve(frame)
        with self.assertRaisesRegex(ValueError, "不同月份存在重复原帖 ID"):
            self.run_stats()
        self.assertFalse(self.output.exists())


class MediaHistoryDistributionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name)

    def test_day_start_includes_lower_bound_excludes_today_and_handles_leap_day(self):
        counts = Counter({(date(2024, 1, 30), "BTC"): 100,
                          (date(2024, 1, 31), "BTC"): 2,
                          (date(2024, 2, 29), "BTC"): 3,
                          (date(2024, 3, 1), "BTC"): 7})
        write_history_reports(counts, date(2024, 3, 1), date(2024, 3, 1),
                              date(2024, 1, 1), [30], self.output)
        daily = pd.read_csv(self.output / "daily_ticker_history.csv")
        self.assertEqual(list(daily["history_events"]), [5])
        self.assertEqual(list(daily["today_events"]), [7])
        self.assertEqual(list(daily["window_start"]), ["2024-01-31"])

    def test_integer_percentiles_use_nearest_rank_and_include_zeros(self):
        result = distribution_stats(Counter({0: 90, 2: 4, 10: 1, 20: 4, 100: 1}))
        self.assertEqual((result["sample_days"], result["zero_days"]), (100, 90))
        self.assertEqual((result["p50"], result["p90"], result["p95"], result["p99"],
                          result["max"]), (0, 0, 10, 20, 100))
        self.assertEqual(result["mean"], 1.98)
        self.assertIsNone(distribution_stats(Counter())["p95"])
        self.assertEqual(distribution_stats(Counter({8: 1}))["p99"], 8)

    def test_partial_windows_are_retained_in_details_but_excluded_from_percentiles(self):
        counts = Counter({(date(2026, 1, 1), "BTC"): 3,
                          (date(2026, 1, 31), "BTC"): 1})
        report = write_history_reports(counts, date(2026, 1, 1), date(2026, 2, 1),
                                       date(2026, 1, 1), [30], self.output)
        daily = pd.read_csv(self.output / "daily_ticker_history.csv")
        self.assertEqual(len(daily), 32)
        self.assertEqual(int(daily["has_full_history"].sum()), 2)
        ticker = pd.read_csv(self.output / "ticker_history_distribution.csv").set_index("sample")
        self.assertEqual((ticker.at["all_days", "sample_days"], ticker.at["all_days", "partial_window_days"]),
                         (2, 30))
        self.assertEqual((ticker.at["active_days", "sample_days"], ticker.at["active_days", "p95"]), (1, 3))
        self.assertEqual(ticker.at["all_days", "min"], 1)
        self.assertEqual(report["daily_rows"], 32)

    def test_active_days_and_scope_distributions_do_not_mix_industry_with_assets(self):
        counts = Counter({(date(2026, 1, 1), "BTC"): 3,
                          (date(2026, 1, 31), "BTC"): 1,
                          (date(2026, 1, 31), "INDUSTRY"): 100,
                          (date(2026, 2, 1), "INDUSTRY"): 1})
        write_history_reports(counts, date(2026, 1, 31), date(2026, 2, 2),
                              date(2026, 1, 1), [30, 7, 30], self.output)
        overall = pd.read_csv(self.output / "history_distribution.csv").set_index(
            ["scope", "lookback_days", "sample"])
        self.assertEqual(overall.at[("ASSETS", 30, "active_days"), "p95"], 3)
        self.assertEqual(overall.at[("INDUSTRY", 30, "active_days"), "p95"], 100)
        self.assertEqual(overall.at[("ALL", 30, "active_days"), "sample_days"], 3)
        self.assertEqual(overall.at[("ALL", 30, "all_days"), "sample_days"], 6)
        self.assertEqual(overall.at[("MACRO", 30, "all_days"), "sample_days"], 0)
        self.assertTrue(pd.isna(overall.at[("MACRO", 30, "all_days"), "p95"]))
        daily = pd.read_csv(self.output / "daily_ticker_history.csv")
        self.assertEqual(len(daily), 12)

    def test_daily_counts_match_naive_reference_for_multiple_windows_and_tickers(self):
        first = date(2026, 1, 1)
        counts = Counter({(first + timedelta(days=day), ticker): (day * 7 + len(ticker)) % 11 + 1
                          for ticker in ["BTC", "ETH", "MACRO"] for day in range(60)
                          if (day + len(ticker)) % 3 != 0})
        write_history_reports(counts, first + timedelta(days=20), first + timedelta(days=59),
                              first, [7, 14, 30], self.output)
        daily = pd.read_csv(self.output / "daily_ticker_history.csv")
        for row in daily.itertuples():
            day = date.fromisoformat(row.date)
            begin = day - timedelta(days=row.lookback_days)
            expected = sum(count for (event_day, ticker), count in counts.items()
                           if ticker == row.ticker and begin <= event_day < day)
            self.assertEqual(row.history_events, expected)
            self.assertEqual(row.has_full_history, begin >= first)

    def test_future_ticker_is_not_sampled_before_first_appearance_and_zero_days_remain(self):
        counts = Counter({(date(2026, 2, 2), "NEW"): 1})
        write_history_reports(counts, date(2026, 2, 1), date(2026, 3, 5),
                              date(2026, 1, 1), [7], self.output)
        daily = pd.read_csv(self.output / "daily_ticker_history.csv")
        self.assertEqual(daily.iloc[0]["date"], "2026-02-02")
        last = daily.iloc[-1]
        self.assertEqual((last["today_events"], last["history_events"]), (0, 0))

    def test_empty_input_writes_headers_and_undefined_percentiles(self):
        report = write_history_reports(Counter(), None, None, None, [30], self.output)
        self.assertEqual((report["tickers"], report["daily_rows"]), (0, 0))
        self.assertTrue(pd.read_csv(self.output / "daily_ticker_history.csv").empty)
        self.assertTrue(pd.read_csv(self.output / "ticker_history_distribution.csv").empty)
        overall = pd.read_csv(self.output / "history_distribution.csv")
        self.assertTrue(overall["sample_days"].eq(0).all())
        self.assertTrue(overall["p95"].isna().all())
        self.assertEqual(normalize_lookback_days([30, 7, 30]), (7, 30))


if __name__ == "__main__":
    unittest.main()
