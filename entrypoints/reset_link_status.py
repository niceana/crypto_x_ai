"""一次性重置 LINK 状态：从配置范围内最早未完成时点起，按原索引更新。"""

import logging

import pandas as pd

from common.time_rules import in_scope, published_time
from entrypoints.runtime import execute, run_entrypoint

logger = logging.getLogger(__name__)


def reset_link_status(cli, settings):
    if not settings.start_time or not settings.end_time:
        raise ValueError("请先配置本次第二轮任务的 start_time/end_time，再重置 LINK 状态")
    if settings.start_time > settings.end_time:
        raise ValueError("start_time 不能晚于 end_time")

    # 读取全表两列，才能包含配置结束日期之后可能已完成的 LINK 记录。
    df = cli.read(settings.result_path, start_time="", end_time="",
                  columns=["asset", "round2_status"])
    link = df.loc[df["asset"] == "LINK"].sort_index()
    pending = link.loc[link["round2_status"].isin(["0", "9", "-1"])]
    pending_times = [published_time(key[0]) for key in pending.index if in_scope(key[0], settings)]
    if not pending_times:
        logger.info("配置日期范围内 LINK 没有未完成记录，未修改数据库")
        return {"asset": "LINK", "reset_records": 0}

    start = min(pending_times)
    selected = [published_time(key[0]) >= start for key in link.index]
    patch = pd.DataFrame({"round2_status": "0", "round3_status": "0"},
                         index=link.index[selected]).sort_index()
    logger.info("重置 LINK 状态：表=%s，起点=%s，记录数=%d；包含该时点及之后全部 LINK 记录",
                settings.result_path, start, len(patch))
    cli.update_by_column(settings.result_path, patch)
    logger.info("LINK 状态重置完成：记录数=%d", len(patch))
    return {"asset": "LINK", "start": str(start), "reset_records": len(patch)}


def main():
    return run_entrypoint(reset_link_status)


if __name__ == "__main__":
    execute(main)
