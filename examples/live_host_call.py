"""接入示例：失败整批重算；宿主管理超时、积压原帖及成功结果写回。"""

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from crypto_x_ai_live import run_live_batch


def init_logging(log_dir="logs"):
    """启动时执行一次：终端及 UTF-8 文件输出，按天轮转，保留 30 份历史日志。"""
    directory = Path(log_dir).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    log_file = directory / "crypto_x_ai_live.log"
    root = logging.getLogger()
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
               for h in root.handlers):
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(formatter)
        root.addHandler(console)
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == str(log_file)
               for h in root.handlers):
        file_handler = TimedRotatingFileHandler(
            str(log_file), when="midnight", backupCount=30, encoding="utf-8",
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    return log_file


async def process_batch(new_posts, history_results, assets, *, config, timeout_seconds=840):
    """宿主 await 调用；整轮超时后丢弃结果，下次合并原帖重算。"""
    try:
        if not new_posts.empty:
            new_posts = new_posts.loc[~new_posts["text"].str.startswith(("RT @", "JUST IN"), na=False)]
        result = await run_live_batch(
            new_posts=new_posts,              # 本次新帖加所有未成功批次原帖，宿主按 id 去重。
            history_results=history_results,  # 第二、三轮 AI 共用结果表中过去 100 天的数据。
            assets=assets,                    # 宿主从 xdata 获取的当前可交易资产字符串数组。
            config=config,
            aggregation_method="max_abs",  # 按业务方要求选择 max_abs 或 sum。
            timeout_seconds=timeout_seconds,
        )
    except Exception:
        logging.getLogger(__name__).exception("实盘批次输入校验或调用失败")
        return {"status": "failed"}

    if result["status"] != "success":
        # timeout/failed 只含 status；宿主保留原帖，下一轮合并新帖从头重算。
        return result

    # 在宿主时限内成功后，统一提交以下三项，再清除本轮积压/推进成功批次标记：
    # result["round1_results"]：按 (date, id) 合并更新。
    # result["event_results"]：按 (date, record_id) 合并更新。
    # result["weight"]：追加一行权重，date 为包内最终输出该行时的 UTC+8 系统时间。
    return result
