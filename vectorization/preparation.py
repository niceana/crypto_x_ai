"""独立准备固定事件文本的向量；不认领、不归组、不修改 AI 状态。"""

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date

import pandas as pd

from common.config import RunConfig, load_embedding_config
from common.persistence import write_with_retry
from common.store import read_event_history
from common.time_rules import history_read_bounds, is_history_needed, published_time
from vectorization.encoder import EmbeddingEncoder


@dataclass
class VectorSummary:
    current_records: int = 0
    prepared_records: int = 0
    saved_records: int = 0


def read_target_assets(cli, settings):
    if not settings.round2_asset_list_path:
        return None
    universe = json.loads(cli.read_object(settings.round2_asset_list_path).decode("utf-8"))["InstrumentList"]
    return frozenset(instrument.split(".", 1)[0].strip().upper() for instrument in universe)


async def prepare_vectors(cli, settings, *, embedding_config=None, encoder=None, target_assets=None, run_config=None):
    """准备当前记录及必要前史，包括尚未归组的前月记录，允许领先第二轮运行。

    时间范围仍由第二轮逐条查询时裁剪。这里只提前编码事实，不能将尚未归组
    或同刻/未来的记录加入候选历史。所有数据库操作都在调用线程执行。
    """
    if not settings.start_time or not settings.end_time or settings.start_time > settings.end_time:
        raise ValueError("向量准备必须配置合法的 start_time/end_time")
    for value in (settings.start_time, settings.end_time):
        if date.fromisoformat(value).isoformat() != value:
            raise ValueError("向量准备日期必须为 YYYY-MM-DD")
    if settings.retriever or not settings.embedding_config_path:
        raise ValueError("独立向量准备要求配置默认 Embedding 检索器")
    if target_assets is None:
        target_assets = read_target_assets(cli, settings)
    columns = ["asset", "event", "embedding"]
    current = read_event_history(cli, settings.result_path, settings.start_time, settings.end_time,
                                 columns=columns, label="独立向量当前记录读取")
    if target_assets is not None:
        current = current.loc[current["asset"].isin(target_assets)]
    summary = VectorSummary(current_records=len(current))
    if current.empty:
        return summary
    frames = [current]
    bounds = history_read_bounds((key[0] for key in current.index), settings.start_time,
                                 settings.history_lookback_months)
    if bounds is not None:
        history = read_event_history(cli, settings.result_path, bounds[0].isoformat(), bounds[1].isoformat(),
                                     columns=columns, label="独立向量前史读取")
        times = defaultdict(list)
        for key, asset in current["asset"].items():
            times[asset].append(published_time(key[0]))
        for stamps in times.values():
            stamps.sort()
        history = history.loc[history["asset"].isin(times)]
        frames.append(history.loc[[is_history_needed(key[0], times[row["asset"]], settings.history_lookback_months)
                                   for key, row in history.iterrows()]])
    frame = pd.concat(frames).sort_index()
    engine = encoder or EmbeddingEncoder(embedding_config or load_embedding_config(settings.embedding_config_path))

    def save(values):
        patch = pd.DataFrame({"embedding": values}, dtype=object)
        patch.index = pd.MultiIndex.from_tuples(patch.index, names=frame.index.names)
        patch = patch.sort_index()
        write_with_retry(lambda batch: cli.update_by_column(settings.result_path, batch), patch,
                         run_config or RunConfig(), "prepare_vectors", failure_message="独立向量保存预算耗尽")
        summary.saved_records += len(patch)

    keys = sorted(frame.index, key=lambda key: (published_time(key[0]), key[1]))
    await engine.prepare(((key, frame.at[key, "event"], frame.at[key, "embedding"]) for key in keys), save)
    summary.prepared_records = len(keys)
    return summary
