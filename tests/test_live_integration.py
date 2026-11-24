"""离线验证实盘函数的历史交接、严格失败、增量输出和固定刻度窗口。"""

import json
import unittest
from dataclasses import replace
from unittest.mock import patch

import httpx
import pandas as pd

from common.ai_protocol import request_failure
from common.config import EmbeddingConfig, ai_config_hash
from common.network import CompletionMessage
from crypto_x_ai_live import LiveConfig, load_live_config, run_live_batch
from crypto_x_ai_live.memory import EVENTS, POSTS, MemoryTables, empty_events
from initialization.event_records import expand_first_round
from postprocessing.weight_data import build_live_weight, live_weight_window
from postprocessing.weight_parameters import WeightParameters
from round3.runner import run_impact
from tests.event_support import EventFixture, impact_result, source_posts
from tests.test_independent_stages import ImmediateNetwork
from vectorization.encoder import EmbeddingEncoder


def response(direction="利好", materiality="中"):
    body = dict(impact_result(), direction=direction, materiality=materiality)
    return json.dumps({"ai_response_content": json.dumps(body, ensure_ascii=False)}, ensure_ascii=False)


def utc_from_local(value):
    stamp = pd.Timestamp(value)
    return stamp - pd.Timedelta(hours=8) if stamp.tzinfo is None else stamp.tz_convert("UTC").tz_localize(None)


def posts(dates, texts=None):
    index = pd.DatetimeIndex(dates, name="date")
    return pd.DataFrame({"id": ["live-{}".format(i) for i in range(len(dates))],
                         "created_at": [utc_from_local(date).tz_localize("UTC").isoformat() for date in dates],
                         "text": texts or ["event"] * len(dates), "user_name": ["account"] * len(dates)}, index=index)


def history(dates, assets=None):
    source = source_posts([(utc_from_local(date), "old {}".format(i), [(assets or ["ETH"] * len(dates))[i]], "account")
                           for i, date in enumerate(dates)])
    frame = expand_first_round(source)
    frame["round2_status"] = frame["round3_status"] = "1"
    frame["event_relation"] = "new"
    frame["event_id"] = frame.index.get_level_values("record_id")
    frame["round3_ai_response"] = response()
    frame["round3_config_hash"] = "a" * 64
    encoder = EmbeddingEncoder(EmbeddingConfig(base_url="https://mock.invalid/v1", api_key="not-real"))
    frame["embedding"] = json.dumps({"model": encoder.config.model, "model_version": encoder.config.model_version,
                                     "config_hash": encoder.config_hash, "vector": [1.0, 0.0]})
    return frame


class LiveWeightTests(unittest.TestCase):
    def test_calls_use_fixed_quarter_hours_and_previous_hour_45(self):
        events = history(["2021-03-20 " + value for value in ("15:45", "16:00", "16:15", "16:30", "16:45", "17:00")])
        for clock, begin, cutoff, expected in (
                ("16:02:03", "15:45", "16:00", 12), ("16:17:03", "15:45", "16:15", 24),
                ("16:32:03", "15:45", "16:30", 36), ("16:47:03", "15:45", "16:45", 48),
                ("17:02:03", "16:45", "17:00", 12)):
            with self.subTest(clock=clock):
                stamp = pd.Timestamp("2021-03-20 " + clock)
                actual = live_weight_window(stamp)
                self.assertEqual(actual, (stamp, pd.Timestamp("2021-03-20 " + begin), pd.Timestamp("2021-03-20 " + cutoff)))
                weight = build_live_weight(events, as_of=stamp, assets=["ETH", "BTC"], aggregation_method="sum")
                self.assertEqual(weight.index[0], stamp)
                self.assertEqual(list(weight.iloc[0]), [expected, 0.0])
                self.assertTrue(all(str(dtype) == "float64" for dtype in weight.dtypes))

    def test_max_abs_preserves_sign_and_ties_choose_earliest(self):
        events = history(["2021-03-20 15:45", "2021-03-20 16:01", "2021-03-20 16:02"])
        events.iloc[1, events.columns.get_loc("round3_ai_response")] = response("利空", "高")
        events.iloc[2, events.columns.get_loc("round3_ai_response")] = response("利好", "高")
        weight = build_live_weight(events.iloc[::-1], as_of="2021-03-20 16:17", assets=["ETH"])
        self.assertEqual(weight.iloc[0, 0], -18.0)
        summed = build_live_weight(events, as_of="2021-03-20 16:17", assets=["ETH"], aggregation_method="sum")
        self.assertEqual(summed.iloc[0, 0], 12.0)

    def test_normal_empty_window_emits_zero_row_with_host_columns(self):
        weight = build_live_weight(empty_events(), as_of="2021-03-20 16:17", assets=["BTC.USDT.UFUTURE.BINANCE", "ETH"])
        self.assertEqual(weight.shape, (1, 2))
        self.assertEqual(list(weight.columns), ["BTC.USDT.UFUTURE.BINANCE", "ETH.USDT.UFUTURE.BINANCE"])
        self.assertTrue(weight.eq(0).all().all())

    def test_bad_response_outside_window_or_asset_scope_is_not_used(self):
        events = history(["2021-03-20 15:44:59", "2021-03-20 16:01"], ["ETH", "BTC"])
        events["round3_ai_response"] = "not JSON"
        self.assertEqual(build_live_weight(events, as_of="2021-03-20 16:17", assets=["ETH"]).iloc[0, 0], 0)

    def test_assets_and_timezone_must_be_unambiguous(self):
        for assets in ([], "ETH", ["ETH", "ETH.USDT.UFUTURE.BINANCE"]):
            with self.subTest(assets=assets), self.assertRaises(ValueError):
                build_live_weight(empty_events(), as_of="2021-03-20 16:17", assets=assets)
        with self.assertRaisesRegex(ValueError, "UTC\\+8"):
            build_live_weight(history(["2021-03-20 16:01"]), as_of="2021-03-20 16:17+08:00", assets=["ETH"])
        with self.assertRaisesRegex(ValueError, "当前时刻"):
            live_weight_window(None)

    def test_utc_aware_events_are_shifted_exactly_eight_hours(self):
        events = history(["2021-03-20 16:01+08:00"])
        events.index = pd.MultiIndex.from_arrays([events.index.get_level_values("date").tz_localize("UTC"),
                                                 events.index.get_level_values("record_id")], names=["date", "record_id"])
        stamp = pd.Timestamp("2021-03-20 16:17")
        weight = build_live_weight(events, as_of=stamp, assets=["ETH"])
        self.assertEqual(weight.index[0], stamp)
        self.assertEqual(weight.iloc[0, 0], 12)


class LiveConfigTests(EventFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ai = self.config()
        self.embedding = EmbeddingConfig(base_url="https://mock.invalid/v1", api_key="not-real")

    def test_loader_raises_only_live_retry_limit_and_preserves_yaml(self):
        from pathlib import Path
        directory = Path(__file__).resolve().parent.parent / "configs"
        before = (directory / "round1_ai_config.yaml").read_bytes()
        with patch("crypto_x_ai_live._compat.config.load_ai_config", return_value=self.ai), patch(
                "crypto_x_ai_live._compat.config.load_embedding_config", return_value=self.embedding):
            loaded = load_live_config(directory)
        self.assertEqual(loaded.round1_ai.run.max_attempts, 100)
        self.assertEqual(loaded.embedding.max_attempts, 100)
        self.assertEqual(loaded.round1_ai.run.storage_max_attempts, 1)
        self.assertEqual(self.ai.run.max_attempts, 3)
        self.assertEqual((directory / "round1_ai_config.yaml").read_bytes(), before)
        self.assertEqual(loaded.weight_parameters.mappings["event_relation"]["repeat"], 0.2)

    def test_repeat_limit_is_optional_and_old_default_remains_unbounded(self):
        events = history(["2020-12-15 15:50", "2021-03-20 16:10"])
        current = events.index[-1]
        events.at[current, "event_id"] = events.index[0][1]
        events.at[current, "event_relation"] = "repeat"
        events.at[current, "round3_status"] = "0"
        events.at[current, "round3_ai_response"] = ""
        settings = self.settings(start_time="2021-03-20", end_time="2021-03-20")
        # 原 fixture 路径与实盘适配器的逻辑表名分别指定。
        settings = replace(settings, first_round_task_path=POSTS, result_path=EVENTS)
        limited = MemoryTables(posts([]), events)
        run_impact(limited, settings, ai_config=self.ai, target_assets={"ETH"}, repeat_lookback_months=3)
        self.assertEqual(limited.tables[EVENTS].at[current, "round3_status"], "-1")
        legacy = MemoryTables(posts([]), events)
        run_impact(legacy, settings, ai_config=self.ai, target_assets={"ETH"})
        self.assertEqual(legacy.tables[EVENTS].at[current, "round3_status"], "1")


if __name__ == "__main__":
    unittest.main()
