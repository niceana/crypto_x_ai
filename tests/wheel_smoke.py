"""安装包集成检查：python -I tests/wheel_smoke.py <pip --target 的安装目录>。"""

import json
import asyncio
import sys
import tempfile
import types
from pathlib import Path
from unittest.mock import patch


async def main():
    install_root = Path(sys.argv[1]).resolve()
    sys.path.insert(0, str(install_root))
    # 模拟宿主已有同名通用模块，发行包应只使用自身私有命名空间。
    names = ("common", "initialization", "round1", "round2", "round3", "vectorization", "postprocessing")
    host_modules = {name: types.ModuleType(name) for name in names}
    sys.modules.update(host_modules)

    import httpx
    import pandas as pd
    import yaml
    import crypto_x_ai_live
    from crypto_x_ai_live import load_live_config, run_live_batch
    from crypto_x_ai_live import _compat as core

    Path(crypto_x_ai_live.__file__).resolve().relative_to(install_root)
    for module in (core.config, core.extraction, core.relation, core.impact, core.weights, core.vectors):
        Path(module.__file__).resolve().relative_to(install_root)
        assert module.__name__.startswith("crypto_x_ai_live._engine.")
    assert "xdata" not in sys.modules and "redis" not in sys.modules
    calls = []
    vector_calls = []
    fail_impact = False
    block_requests = False

    def ai_handler(request):
        payload = json.loads(request.content)
        current = json.loads(next(message["content"] for message in payload["messages"] if message["role"] == "user"))
        calls.append(current)
        if "text" in current:
            answer = {"events": [{"event": current["text"], "tickers": ["ETH"]}]}
        elif "current" in current:
            answer = {"event_relation": "repeat", "matched_candidate_id": current["candidates"][0]["candidate_id"],
                      "new_information": None}
        else:
            answer = {} if fail_impact else {"direction": "利好", "materiality": "中", "execution_stage": "已完成",
                                             "persistence": "长期", "materiality_reason": "mock mechanism"}
        return httpx.Response(200, json={"choices": [{"index": 0, "finish_reason": "stop", "message": {
            "role": "assistant", "content": json.dumps(answer, ensure_ascii=False)}}]})

    def vector_handler(request):
        vector_calls.extend(json.loads(request.content)["input"])
        size = len(json.loads(request.content)["input"])
        return httpx.Response(200, json={"data": [{"index": i, "embedding": [1.0, 0.0]} for i in range(size)]})

    def source(date, post_id, text):
        index = pd.DatetimeIndex([pd.Timestamp(date) + pd.Timedelta(seconds=3)], name="date")
        published = (pd.Timestamp(date) - pd.Timedelta(hours=8)).tz_localize("UTC").isoformat()
        return pd.DataFrame({"id": [post_id], "created_at": [published], "text": [text], "user_name": ["mock-account"]}, index=index)

    async def run_at(local_time, *args, **kwargs):
        now = (pd.Timestamp(local_time) - pd.Timedelta(hours=8)).tz_localize("UTC")
        with patch("crypto_x_ai_live.runner._system_now", return_value=now):
            return await run_live_batch(*args, **kwargs)

    with tempfile.TemporaryDirectory(prefix="live-wheel-smoke-") as temporary:
        directory = Path(temporary)
        bundled = Path(crypto_x_ai_live.__file__).parent / "configs"
        for source_path in bundled.glob("*.yaml"):
            document = yaml.safe_load(source_path.read_text(encoding="utf-8"))
            if source_path.name.startswith("round"):
                document["api"]["url"] = "https://mock.invalid/v1/chat/completions"
                document["api"]["headers"] = {}
                document["api"]["payload"]["model"] = "mock-model"
                document["run"].update(concurrency=2, batch_size=4, result_buffer_size=4, ready_request_buffer_size=4,
                                       flush_interval_seconds=0.01, backoff_base_seconds=0.001, backoff_max_seconds=0.001)
            elif source_path.name == "embedding_config.yaml":
                document.update(base_url="https://mock.invalid/v1", api_key="not-real")
            (directory / source_path.name).write_text(yaml.safe_dump(document, allow_unicode=True), encoding="utf-8")
        config = load_live_config(directory, max_attempts=3)

        async def dispatch(request):
            if block_requests:
                await asyncio.sleep(30)
            return vector_handler(request) if "input" in json.loads(request.content) else ai_handler(request)

        def client_factory(config):
            return httpx.AsyncClient(transport=httpx.MockTransport(dispatch), trust_env=False)

        first = await run_at("2021-03-20 16:17", source("2021-03-20 16:02", "one", "first"), None, config=config,
                        assets=["ETH", "BTC"], client_factory=client_factory)
        second = await run_at("2021-03-20 16:32", source("2021-03-20 16:11", "two", "repeat"), first["event_results"],
                         config=config, assets=["ETH", "BTC"], client_factory=client_factory)
        assert len(first["event_results"]) == len(second["event_results"]) == 1
        assert second["event_results"].iloc[0]["event_relation"] == "repeat"
        assert second["event_results"].iloc[0]["round3_status"] == "1"
        assert second["weight"].index[0] == pd.Timestamp("2021-03-20 16:32")
        assert second["weight"].iloc[0, 0] == first["weight"].iloc[0, 0] > 0
        assert second["weight"].iloc[0, 1] == 0
        assert sum("text" in value for value in calls) == 2
        assert sum("current" in value for value in calls) == 1
        assert sum("asset" in value for value in calls) == 1
        assert first["event_results"].index[0][0] == pd.Timestamp("2021-03-20 08:02")
        assert json.loads(first["round1_results"].iloc[0]["round1_call_meta"])["attempts"] == 1
        assert json.loads(first["event_results"].iloc[0]["round2_call_meta"])["attempts"] == 0
        inherited_meta = json.loads(second["event_results"].iloc[0]["round3_call_meta"])
        assert inherited_meta["attempts"] == 0
        assert inherited_meta["source_record_id"] == first["event_results"].index[0][1]
        before = pd.Timestamp.now(tz="UTC").tz_localize(None) + pd.Timedelta(hours=8)
        empty = await run_live_batch(pd.DataFrame(), None, config=config, assets=["ETH"])
        after = pd.Timestamp.now(tz="UTC").tz_localize(None) + pd.Timedelta(hours=8)
        assert before <= empty["weight"].index[0] <= after
        assert empty["weight"].iloc[0, 0] == 0
        broken = first["event_results"].copy(deep=True)
        broken["embedding"] = ""
        count = len(vector_calls)
        failed = await run_at("2021-03-20 16:32", pd.DataFrame(), broken, config=config,
                              assets=["ETH"], client_factory=client_factory)
        assert failed == {"status": "failed"} and len(vector_calls) == count
        raw = source("2021-03-20 16:21", "three", "new batch")
        fail_impact = True
        failed = await run_at("2021-03-20 16:32", raw, None, config=config, assets=["ETH"], client_factory=client_factory)
        assert failed == {"status": "failed"}
        fail_impact = False
        block_requests = True
        before_tasks = asyncio.all_tasks()
        timed_out = await run_at("2021-03-20 16:32", raw, None, config=config, assets=["ETH"],
                                 client_factory=client_factory, timeout_seconds=0.05)
        assert timed_out == {"status": "timeout"} and asyncio.all_tasks() == before_tasks
        block_requests = False
        call_count, vector_count = len(calls), len(vector_calls)
        retried = await run_at("2021-03-20 17:02", raw, None, config=config, assets=["ETH"], client_factory=client_factory)
        assert retried["status"] == "success" and len(calls) == call_count + 2
        assert len(vector_calls) == vector_count + 1
        assert retried["weight"].iloc[0, 0] == 0  # 新一轮窗口不再沿用失败轮。
        shared = pd.concat([source("2021-03-20 16:22", "crawl-a", "first in shared crawl"),
                            source("2021-03-20 16:23", "crawl-b", "second in shared crawl")])
        shared.index = pd.DatetimeIndex(["2021-03-20 16:24"] * 2, name="date")
        same_crawl = await run_at("2021-03-20 16:32", shared, None, config=config, assets=["ETH"],
                            client_factory=client_factory)
        assert len(same_crawl["event_results"]) == 2
        assert same_crawl["event_results"]["round3_status"].eq("1").all()
        # 安装后的入口也必须最后取时间，跨整点仍按本批起始窗口计算。
        with patch("crypto_x_ai_live.runner._system_now", side_effect=[
                pd.Timestamp("2021-03-20 08:17:00Z"), pd.Timestamp("2021-03-20 09:02:00Z")]) as clock:
            late = await run_live_batch(pd.DataFrame(), first["event_results"], config=config, assets=["ETH"],
                                  client_factory=client_factory)
        assert clock.call_count == 2
        assert late["weight"].index[0] == pd.Timestamp("2021-03-20 17:02:00")
        assert late["weight"].iloc[0, 0] == first["weight"].iloc[0, 0] > 0
        # 必须验证安装后的代码也执行严格的全表时间截止，而不只是仓库源码。
        before_history = first["event_results"].copy(deep=True)
        mixed = pd.concat([source("2021-03-20 16:01", "late-earlier", "skip earlier"),
                           source("2021-03-20 16:02", "late-same", "skip same time"),
                           source("2021-03-20 16:03", "tail-new", "append newer")])
        vector_count = len(vector_calls)
        tail_only = await run_at("2021-03-20 16:17", mixed, first["event_results"], config=config,
                                 assets=["ETH"], client_factory=client_factory)
        assert tail_only["status"] == "success"
        assert len(tail_only["round1_results"]) == 3
        assert tail_only["event_results"]["source_post_id"].tolist() == ["tail-new"]
        assert len(vector_calls) == vector_count + 1
        pd.testing.assert_frame_equal(first["event_results"], before_history)
    for name, module in host_modules.items():
        assert sys.modules[name] is module
    assert "xdata" not in sys.modules and "redis" not in sys.modules
    print("Installed wheel passed strict event-time cutoff, created_at+8 windows, automatic UTC+8 time, original executor thread isolation, native async vectors, cooperative cancellation and fresh retries, strict historical vectors, namespace isolation, and appended per-round call metadata.")


if __name__ == "__main__":
    asyncio.run(main())
