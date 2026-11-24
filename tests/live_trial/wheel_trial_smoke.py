"""安装后试跑工具检查：python -I tests/live_trial/wheel_trial_smoke.py <安装目录>。"""
import json
import faulthandler
import sys
from pathlib import Path
from unittest.mock import patch

def main():
    if len(sys.argv) != 2:
        raise SystemExit("用法：python -I tests/live_trial/wheel_trial_smoke.py <pip --target 安装目录>")
    install_root = Path(sys.argv[1]).expanduser().resolve()
    if not (install_root / "crypto_x_ai_live/__init__.py").is_file():
        raise SystemExit("安装目录缺少 crypto_x_ai_live，请先安装 wheel")
    faulthandler.dump_traceback_later(45, exit=True)
    WORK = Path(__file__).resolve().parents[2]
    KIT = WORK / "tools/live_trial"
    sys.path.insert(0, str(install_root))
    sys.path.insert(0, str(KIT))

    import httpx
    import pandas as pd
    import yaml
    import _trial
    import run_full
    import check_weight
    from crypto_x_ai_live import _compat as core

    ROOT = WORK / "reports/linux_trial_validation"
    ROOT.mkdir(parents=True, exist_ok=True)
    (ROOT / "configs").mkdir(exist_ok=True)
    (ROOT / "data").mkdir(exist_ok=True)
    _trial.ROOT = ROOT
    for source in (WORK / "configs").glob("*.yaml"):
        cfg = yaml.safe_load(source.read_text(encoding="utf-8"))
        if source.name.startswith("round"):
            cfg["api"]["url"] = "https://mock.invalid/v1/chat/completions"
            cfg["api"]["headers"] = {}
            cfg["api"]["payload"]["model"] = "mock-model"
            cfg["run"].update(concurrency=2, batch_size=4, result_buffer_size=4,
                              ready_request_buffer_size=4, flush_interval_seconds=0.01,
                              backoff_base_seconds=0.001, backoff_max_seconds=0.001)
        elif source.name == "embedding_config.yaml":
            cfg.update(base_url="https://mock.invalid/v1", api_key="not-real")
        (ROOT / "configs" / source.name).write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")

    (ROOT / "data/assets.json").write_text(json.dumps({"InstrumentList": ["ETH"]}), encoding="utf-8")
    posts = pd.DataFrame({"id": ["one", "two"], "created_at": ["2025-01-01T08:01:00Z", "2025-01-01T08:20:00Z"],
                          "text": ["first", "second"], "user_name": ["mock-account"] * 2},
                         index=pd.DatetimeIndex(["2025-01-01 16:30:00"] * 2, name="date"))
    posts.to_pickle(ROOT / "data/posts.pkl")
    calls, vectors = [], []
    fail_impact = False


    def ai_handler(request):
        payload = json.loads(request.content)
        value = json.loads(next(msg["content"] for msg in payload["messages"] if msg["role"] == "user"))
        calls.append(value)
        if "text" in value:
            answer = {"events": [{"event": value["text"], "tickers": ["ETH"]}]}
        elif "current" in value:
            answer = {"event_relation": "update", "matched_candidate_id": value["candidates"][0]["candidate_id"],
                      "new_information": "new detail"}
        else:
            answer = {} if fail_impact else {"direction": "利好", "materiality": "中", "execution_stage": "已完成",
                                            "persistence": "长期", "materiality_reason": "mock mechanism"}
        return httpx.Response(200, json={"choices": [{"index": 0, "finish_reason": "stop", "message": {
            "role": "assistant", "content": json.dumps(answer, ensure_ascii=False)}}]})


    def vector_handler(request):
        values = json.loads(request.content)["input"]
        vectors.extend(values)
        return httpx.Response(200, json={"data": [{"index": i, "embedding": [1.0, 0.0]} for i in range(len(values))]})


    original_async, original_sync = httpx.AsyncClient, httpx.Client


    def async_client(*args, **kwargs):
        kwargs.update(transport=httpx.MockTransport(lambda request:
            vector_handler(request) if "input" in json.loads(request.content) else ai_handler(request)), trust_env=False)
        return original_async(*args, **kwargs)


    def sync_client(*args, **kwargs):
        kwargs.update(transport=httpx.MockTransport(vector_handler), trust_env=False)
        return original_sync(*args, **kwargs)


    def invoke(function, arguments):
        with patch.object(sys, "argv", ["trial"] + arguments):
            return function()


    def latest(prefix):
        return max((ROOT / "outputs").glob(prefix + "_*"), key=lambda value: value.name)


    common = ["--config-dir", str(ROOT / "configs"), "--method", "sum", "--test-time", "2025-01-01 16:47:00"]
    full = common + ["--history", "none", "--max-attempts", "1"]
    with patch.object(httpx, "AsyncClient", side_effect=async_client), patch.object(httpx, "Client", side_effect=sync_client):
        assert invoke(run_full.main, full) == 0
        successful = latest("full")
        source_log = (successful / "run.log").read_text(encoding="utf-8")
        assert "crypto_x_ai_live._engine.round1.runner" in source_log
        assert "crypto_x_ai_live._engine.round2.runner" in source_log
        assert "crypto_x_ai_live._engine.round3.runner" in source_log
        assert "batch=2025-01-01 16:47:00" in source_log
        events = pd.read_pickle(successful / "events_merged.pkl")
        weight = pd.read_pickle(successful / "weight.pkl")
        assert len(events) == 2 and events["round3_status"].eq("1").all()
        assert weight.iloc[0, 0] == 22.5, weight
        assert weight.index[0] == pd.Timestamp("2025-01-01 16:47:00")
        fail_impact = True
        assert invoke(run_full.main, full) == 1
        failed = latest("full")
        assert not (failed / "resume_state.pkl").exists()
        assert not (failed / "weight.pkl").exists()
        fail_impact = False
        prior_calls, prior_vectors = len(calls), len(vectors)
        assert invoke(run_full.main, full) == 0
        assert len(vectors) > prior_vectors and len(calls) > prior_calls + 2
        assert pd.read_pickle(latest("full") / "weight.pkl").iloc[0, 0] == 22.5

    events.to_pickle(ROOT / "data/events_completed.pkl")
    parameters = _trial.score_parameters("configs", None)
    basic = core.weights.build_basic_df(events, parameters)
    with patch.object(core.config, "load_ai_config", side_effect=AssertionError("Weight-only loaded AI config")), \
            patch.object(httpx, "AsyncClient", side_effect=AssertionError("Weight-only requested AI")), \
            patch.object(httpx, "Client", side_effect=AssertionError("Weight-only requested vectors")):
        for method, expected in (("sum", 22.5), ("max_abs", 15.0)):
            old = core.weights.build_weight_data(basic, start_time="2025-01-01", end_time="2025-01-01", aggregation_method=method)
            old.index += pd.Timedelta(hours=8)
            old.to_pickle(ROOT / "data/old_weight.pkl")
            args = ["--config-dir", str(ROOT / "configs"), "--method", method, "--test-time", "2025-01-01 16:47:00", "--old-weight", "data/old_weight.pkl"]
            assert invoke(check_weight.main, args) == 0
            check = latest("weight")
            assert pd.read_pickle(check / "weight.pkl").iloc[0, 0] == expected
            info = json.loads((check / "summary.json").read_text(encoding="utf-8"))
            assert info["comparison"] == "PASS" and info["old_reference_time"] == "2025-01-01 17:00:00"
            old.loc[pd.Timestamp("2025-01-01 17:00:00"), old.columns[0]] += 1
            old.to_pickle(ROOT / "data/old_weight.pkl")
            assert invoke(check_weight.main, args) == 1
        assert invoke(check_weight.main, ["--config-dir", str(ROOT / "configs"), "--method", "sum", "--test-time", "2025-01-01 16:17:00", "--old-weight", "data/old_weight.pkl"]) == 2

    summary = dict(full_flow="PASS", fresh_batch_retry="PASS", sum_comparison="PASS", max_abs_comparison="PASS",
                   detect_difference="PASS", partial_window_guard="PASS", weight_without_ai="PASS", platform=sys.platform)
    _trial.dump_json(ROOT / "validation.json", summary)
    print(json.dumps(summary))
    faulthandler.cancel_dump_traceback_later()


if __name__ == "__main__":
    main()
