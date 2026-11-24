"""连续回放专项检查；不访问真实 xdata 或 AI。"""
import argparse
from contextlib import contextmanager, redirect_stdout
from dataclasses import replace
import io
import json
import logging
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

WORK = Path(__file__).resolve().parents[2]
KIT = WORK / "tools/live_trial"
sys.path.insert(0, str(WORK))
sys.path.insert(0, str(KIT))

import httpx
import pandas as pd
import yaml
from crypto_x_ai_live import load_live_config, run_live_batch, runner
from crypto_x_ai_live import _compat as core
from export_hours import export_hours
from export_xdata import EVENT_COLUMNS
from replay_hours import replay
from _replay import bounds, schedule
from _trial import init_logging, merge_events, set_log_batch


@contextmanager
def trial_logs():
    root = logging.getLogger()
    previous_handlers, previous_level = list(root.handlers), root.level
    external_levels = {name: logging.getLogger(name).level for name in ("httpx", "httpcore")}
    root.handlers = []
    try:
        with redirect_stdout(io.StringIO()):
            init_logging("INFO")
            yield
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        root.handlers = previous_handlers
        root.setLevel(previous_level)
        for name, level in external_levels.items():
            logging.getLogger(name).setLevel(level)
        set_log_batch()


class Fixture:
    def __init__(self, config, hours=2, empty=(), no_seed=False):
        self.calls = schedule("2025-01-01 00:47:00", hours)
        self.config = config
        self.settings = argparse.Namespace(POSTS_PATH="posts", EVENTS_PATH="events", OLD_WEIGHT_PATH="old", ASSETS_PATH="assets",
                                           METHOD="sum", WEIGHT_PARAM_ID=None)
        self.reads = []
        self.assets = ["ETH.USDT.UFUTURE.BINANCE", "BTC.USDT.UFUTURE.BINANCE"]
        self.impact = json.dumps({"ai_response_content": json.dumps({"direction": "利好", "materiality": "中", "execution_stage": "已完成",
                                                                   "persistence": "长期", "materiality_reason": "mock"})})
        keys, values = [], []
        for number, call in enumerate(self.calls, 1):
            if number not in empty:
                keys.append((bounds(call)[0] + pd.Timedelta(minutes=7), "p{:04d}".format(number)))
                values.append(["post {:04d}".format(number), "mock-user"])
        self.posts = pd.DataFrame(values, index=pd.MultiIndex.from_tuples(keys, names=["date", "id"]), columns=["text", "user_name"])
        self.reference = self.events_from_posts(self.posts, prefix="OLD ")
        seed_source = self.posts.iloc[:1].copy()
        if not seed_source.empty:
            seed_source.index = pd.MultiIndex.from_tuples([(bounds(self.calls[0])[0] - pd.Timedelta(minutes=1), "seed")], names=["date", "id"])
        seed = self.events_from_posts(seed_source, prefix="SEED ")
        if no_seed:
            seed = seed.iloc[:0]
        self.events = pd.concat([seed, self.reference]).sort_index()
        self.update_old()

    def events_from_posts(self, posts, prefix="LIVE "):
        keys, rows = [], []
        for (date, post_id), row in posts.iterrows():
            record = post_id + ":0:ETH"
            keys.append((date, record))
            value = dict.fromkeys(EVENT_COLUMNS, "")
            value.update(event_id=record, event=prefix + row["text"], asset="ETH", source_post_id=post_id,
                         event_relation="new", round2_status="1", round3_status="1", round3_ai_response=self.impact,
                         embedding='{"kept":"original"}')
            rows.append(value)
        return pd.DataFrame(rows, columns=EVENT_COLUMNS,
                            index=pd.MultiIndex.from_tuples(keys, names=["date", "record_id"]))

    def update_old(self):
        basic = core.weights.build_basic_df(self.events, self.config.weight_parameters)
        self.old = core.weights.build_weight_data(basic, start_time=self.events.index.get_level_values("date").min().date().isoformat(),
                                                  end_time=bounds(self.calls[-1])[1].date().isoformat(), aggregation_method=self.settings.METHOD)
        self.old.index += pd.Timedelta(hours=8)

    def read(self, path, **options):
        self.reads.append((path, options))
        return {"posts": self.posts, "events": self.events, "old": self.old}[path].copy(deep=True)

    def read_object(self, path):
        self.reads.append((path, "read_object"))
        return json.dumps({"InstrumentList": self.assets}).encode()


class SequentialBatch:
    def __init__(self, case, fail_call=None):
        self.case = case
        self.fail_call = fail_call
        self.attempts = []
        self.checkpoints = []

    async def __call__(self, posts, history, *, assets, config, aggregation_method):
        call = runner._system_now().tz_convert("UTC").tz_localize(None) + pd.Timedelta(hours=8)
        self.attempts.append(call)
        begin, end = bounds(call)
        if len(history):
            assert (history.index.get_level_values("date") < begin).all(), "future history leak"
            replayed = history["source_post_id"].str.startswith("p")
            assert history.loc[replayed, "event"].str.startswith("LIVE ").all(), "old reference was injected into history"
        published = pd.DatetimeIndex(pd.to_datetime(posts["created_at"], utc=True)).tz_localize(None)
        assert ((published >= begin) & (published < end)).all()
        assert (posts.index == call.floor("15min")).all()
        source = posts[["text", "user_name"]].copy()
        source.index = pd.MultiIndex.from_arrays([published, posts["id"]], names=["date", "id"])
        delta = self.case.events_from_posts(source)
        r1 = pd.DataFrame("", index=source.index, columns=core.validation.TASK_COLUMNS, dtype=object)
        r1["status"] = "1"
        r1[["text", "user_name"]] = source[["text", "user_name"]]
        if self.fail_call == call:
            self.fail_call = None
            return {"status": "failed"}
        weight = core.weights.build_live_weight(merge_events(history, delta), as_of=call, assets=assets,
                                                aggregation_method=aggregation_method, parameters=config.weight_parameters)
        return {"status": "success", "round1_results": r1, "event_results": delta, "weight": weight}


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="hours-replay-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.config_dir = self.root / "configs"
        self.config_dir.mkdir()
        for path in (WORK / "configs").glob("*.yaml"):
            doc = yaml.safe_load(path.read_text(encoding="utf-8"))
            if path.name.startswith("round"):
                doc["api"].update(url="https://mock.invalid/v1/chat/completions", headers={})
                doc["api"]["payload"]["model"] = "mock-model"
                doc["run"].update(concurrency=2, batch_size=4, result_buffer_size=4, ready_request_buffer_size=4,
                                   flush_interval_seconds=0.01, backoff_base_seconds=0.001, backoff_max_seconds=0.001)
            elif path.name == "embedding_config.yaml":
                doc.update(base_url="https://mock.invalid/v1", api_key="not-real")
            (self.config_dir / path.name).write_text(yaml.safe_dump(doc, allow_unicode=True), encoding="utf-8")
        self.config = load_live_config(self.config_dir, max_attempts=1)
        self.data = self.root / "data"
        self.output = self.root / "outputs"

    def export(self, case):
        with redirect_stdout(io.StringIO()):
            return export_hours(case, first_time="2025-01-01 00:47:00", hours=len(case.calls) // 4,
                                output_dir=self.data, configuration=case.settings)

    def run_case(self, case, **kwargs):
        with redirect_stdout(io.StringIO()):
            return replay(self.data, self.config_dir, output_root=self.output, config=self.config, **kwargs)

    def test_schedule_240_hours_exact_boundaries_and_old_table_alignment(self):
        calls = schedule("2025-01-01 00:47", 240)
        self.assertEqual(len(calls), 960)
        self.assertEqual(len(calls[calls.minute == 47]), 240)
        self.assertEqual(calls[0], pd.Timestamp("2025-01-01 00:02"))
        self.assertEqual(calls[-1], pd.Timestamp("2025-01-10 23:47"))
        self.assertEqual(bounds(calls[0])[0] + pd.Timedelta(hours=8), pd.Timestamp("2024-12-31 23:45"))
        self.assertEqual(bounds(calls[-1])[1] - bounds(calls[0])[0], pd.Timedelta(hours=240))

    def test_export_reads_once_and_keeps_seed_separate_from_future_reference(self):
        case = Fixture(self.config)
        info = self.export(case)
        self.assertEqual(len(case.reads), 4)
        self.assertEqual(info["call_count"], 8)
        seed = pd.read_pickle(self.data / "history_seed.pkl")
        self.assertEqual(seed["source_post_id"].tolist(), ["seed"])
        self.assertEqual(info["old_weight_missing_assets"], ["BTC.USDT.UFUTURE.BINANCE"])
        self.assertEqual(pd.read_pickle(self.data / "reference_events.pkl")["source_post_id"].tolist(), case.reference["source_post_id"].tolist())

    def test_full_240_hours_runs_960_serial_batches_and_carries_only_new_results(self):
        case = Fixture(self.config, hours=240, empty=(1, 5, 6, 7, 8))
        self.export(case)
        batch = SequentialBatch(case)
        result = self.run_case(case, batch_function=batch)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["completed_batches"], 960)
        self.assertEqual(result["completed_hours"], 240)
        self.assertEqual(result["full_pass"], 240)
        self.assertEqual(result["weight_only_pass"], 240)
        self.assertEqual(result["empty_post_batches"], 5)
        self.assertEqual(batch.attempts, list(case.calls))
        root = Path(result["output_dir"])
        self.assertEqual(len(pd.read_csv(root / "weights_47.csv")), 240)
        self.assertEqual(len(pd.read_pickle(root / "weights.pkl")), 960)
        final = pd.read_pickle(root / "final_history.pkl")
        replayed = final["source_post_id"].str.startswith("p")
        self.assertTrue(final.loc[replayed, "event"].str.startswith("LIVE ").all())

    def test_failure_stops_next_batch_then_resume_skips_successes_and_restores_partial(self):
        case = Fixture(self.config)
        self.export(case)
        batch = SequentialBatch(case, fail_call=case.calls[5])
        first = self.run_case(case, batch_function=batch)
        self.assertEqual(first["status"], "failed")
        self.assertEqual(first["completed_batches"], 5)
        self.assertEqual(first["failed_batch"], 6)
        self.assertEqual(batch.attempts, list(case.calls[:6]))
        resumed = self.run_case(case, batch_function=batch, resume_dir=first["output_dir"])
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(resumed["recovered_batches"], 5)
        self.assertEqual(resumed["full_pass"], 2)
        self.assertEqual(batch.attempts, list(case.calls[:6]) + list(case.calls[5:]))
        self.assertEqual(batch.checkpoints, [])

    def test_information_summary_after_max_abs_replay_uses_active_asset_hours(self):
        from summarize_diffs import summarize
        case = Fixture(self.config, hours=2, empty=(5, 6, 7, 8))
        case.settings.METHOD = "max_abs"
        case.update_old()
        self.export(case)
        result = self.run_case(case, batch_function=SequentialBatch(case))
        self.assertEqual(result["status"], "completed")
        with redirect_stdout(io.StringIO()):
            stats = summarize(result["output_dir"], scope="with_events", data_dir=self.data)
        self.assertEqual(stats["all_compared_asset_values"], 4)
        self.assertEqual(stats["compared_asset_values"], 1)
        self.assertEqual(stats["hours_with_events"], 1)
        self.assertEqual(stats["compared_hours"], 2)
        self.assertEqual(stats["diff_rate_pct"], 0)
        self.assertEqual(stats["warnings"], [])

    def test_weight_only_240_hours_never_loads_ai_or_calls_pipeline(self):
        case = Fixture(self.config, hours=240)
        self.export(case)
        with patch("replay_hours.load_live_config", side_effect=AssertionError("AI config loaded")):
            result = self.run_case(case, mode="weight", batch_function=lambda *a, **k: self.fail("AI pipeline called"))
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["weight_only_pass"], 240)
        self.assertEqual(result["completed_batches"], 240)

    def test_changed_data_or_model_rejects_resume_before_new_calls(self):
        case = Fixture(self.config)
        self.export(case)
        batch = SequentialBatch(case)
        first = self.run_case(case, batch_function=batch)
        attempts = len(batch.attempts)
        self.config = replace(self.config, embedding=replace(self.config.embedding, model_version="changed"))
        with self.assertRaisesRegex(ValueError, "不能混用旧进度"):
            self.run_case(case, batch_function=batch, resume_dir=first["output_dir"])
        self.assertEqual(len(batch.attempts), attempts)
        with (self.data / "posts.pkl").open("ab") as handle:
            handle.write(b"changed")
        with self.assertRaisesRegex(ValueError, "已变更"):
            self.run_case(case, batch_function=batch)

    def test_missing_old_hour_is_not_silently_skipped(self):
        case = Fixture(self.config)
        case.old = case.old.drop(pd.Timestamp("2025-01-01 02:00"))
        with self.assertRaisesRegex(ValueError, "缺少 1 个"):
            self.export(case)
        self.assertFalse((self.data / "manifest.json").exists())

    def test_comparison_differences_are_reported_without_stopping_later_batches(self):
        case = Fixture(self.config)
        self.export(case)
        base = SequentialBatch(case)

        async def changed(*args, **kwargs):
            result = await base(*args, **kwargs)
            if result["weight"].index[0].minute == 47:
                result["weight"].iloc[0, 0] += 1.0
            return result

        result = self.run_case(case, batch_function=changed)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["completed_batches"], 8)
        self.assertEqual(result["weight_only_pass"], 2)
        self.assertEqual(result["full_diff"], 2)
        self.assertEqual(base.attempts, list(case.calls))

    def test_real_pipeline_four_quarters_with_mock_http(self):
        case = Fixture(self.config, hours=1, no_seed=True)
        case.events.iloc[1:, case.events.columns.get_loc("event_relation")] = "update"
        case.update_old()
        self.export(case)
        ai_calls, vector_calls = [], []

        def ai_handler(request):
            payload = json.loads(request.content)
            value = json.loads(next(message["content"] for message in payload["messages"] if message["role"] == "user"))
            ai_calls.append(value)
            if "text" in value:
                answer = {"events": [{"event": value["text"], "tickers": ["ETH"]}]}
            elif "current" in value:
                answer = {"event_relation": "update", "matched_candidate_id": value["candidates"][0]["candidate_id"], "new_information": "updated detail"}
            else:
                answer = {"direction": "利好", "materiality": "中", "execution_stage": "已完成", "persistence": "长期", "materiality_reason": "mock"}
            return httpx.Response(200, json={"choices": [{"index": 0, "finish_reason": "stop", "message": {
                "role": "assistant", "content": json.dumps(answer, ensure_ascii=False)}}]})

        def vector_handler(request):
            values = json.loads(request.content)["input"]
            vector_calls.extend(values)
            return httpx.Response(200, json={"data": [{"index": i, "embedding": [1.0, 0.0]} for i in range(len(values))]})

        async def actual(*args, **kwargs):
            def dispatch(request):
                return vector_handler(request) if "input" in json.loads(request.content) else ai_handler(request)
            return await run_live_batch(*args, client_factory=lambda cfg:
                httpx.AsyncClient(transport=httpx.MockTransport(dispatch), trust_env=False), **kwargs)

        with trial_logs():
            result = self.run_case(case, batch_function=actual, log_to_file=True)
            source_log = (Path(result["output_dir"]) / "run.log").read_text(encoding="utf-8")
            for module in (core.extraction, core.relation, core.impact):
                self.assertIn(module.__name__, source_log)
            self.assertIn(core.vectors.__name__, source_log)
            for call in case.calls:
                self.assertIn("batch=" + str(call), source_log)
            self.assertIn("47分对账：weight_only=PASS，full=PASS", source_log)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["full_pass"], 1)
        self.assertEqual(result["weight_only_pass"], 1)
        self.assertEqual(len(vector_calls), 4)
        self.assertEqual(sum("text" in value for value in ai_calls), 4)
        self.assertEqual(sum("current" in value for value in ai_calls), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
