"""预览或重置配置日期、资产范围内的第二、三轮状态；执行前须停止相关任务。"""

import argparse
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from common.config import load_event_config
from common.store import read_event_history
from common.time_rules import published_time
from common.validation import validate_index

logger = logging.getLogger(__name__)


def reset_round2_status(cli, settings, *, apply=False, backup_dir=None):
    """仅更新两列状态，保留向量、固定输入、旧回答和配置摘要。"""
    if not settings.start_time or not settings.end_time:
        raise ValueError("请先配置 start_time/end_time，明确需要重跑的日期范围")
    if date.fromisoformat(settings.start_time) > date.fromisoformat(settings.end_time):
        raise ValueError("start_time 不能晚于 end_time")

    frame = read_event_history(cli, settings.result_path, settings.start_time, settings.end_time)
    if settings.round2_asset_list_path:
        universe = json.loads(cli.read_object(settings.round2_asset_list_path).decode("utf-8"))["InstrumentList"]
        assets = {instrument.split(".", 1)[0].strip().upper() for instrument in universe}
        frame = frame.loc[frame["asset"].isin(assets)]
    frame = frame.sort_index()
    assets = sorted(set(frame["asset"]))
    summary = {
        "result_path": settings.result_path,
        "start_time": settings.start_time, "end_time": settings.end_time,
        "assets": assets, "records": len(frame),
        "round2_completed": int(frame["round2_status"].eq("1").sum()),
        "round3_completed": int(frame["round3_status"].eq("1").sum()),
        "applied": False,
    }
    if not frame.empty:
        # 全表只读两列，防止重跑早期记录后留下依赖旧分组的较晚成功结果。
        statuses = cli.read(settings.result_path, start_time="", end_time="",
                            columns=["asset", "round2_status"])
        validate_index(statuses, "record_id")
        if not {"asset", "round2_status"}.issubset(statuses.columns):
            raise ValueError("后续记录检查缺少 asset/round2_status")
        later = statuses.loc[statuses["asset"].isin(assets) & statuses["round2_status"].eq("1")]
        end = date.fromisoformat(settings.end_time)
        later_dates = [published_time(key[0]).date() for key in later.index
                       if published_time(key[0]).date() > end]
        if later_dates:
            raise ValueError("范围之外仍有同币种第二轮已完成记录，请将 end_time 扩大至至少 {} 后再重置；"
                             "未修改数据库".format(max(later_dates).isoformat()))
    logger.info("第二轮重置%s：%s", "执行范围" if apply else "预览（不写数据库）",
                json.dumps(summary, ensure_ascii=False))
    if not apply or frame.empty:
        return summary

    if backup_dir is None:
        stamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d_%H%M%S_%f")
        backup_dir = Path(__file__).absolute().parent.parent / "reports" / ("round2_reset_" + stamp)
    output = Path(backup_dir).absolute()
    # 必须新建目录；备份任一步失败都不会开始数据库更新。
    output.mkdir(parents=True, exist_ok=False)
    frame.to_pickle(output / "before_reset.pkl")
    frame[["asset", "round2_status", "round3_status"]].to_csv(output / "scope.csv", encoding="utf-8-sig")
    summary["backup_dir"] = str(output)
    manifest_path = output / "manifest.json"
    manifest_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("重置前完整记录已备份：%s", output)

    patch = pd.DataFrame({"round2_status": "0", "round3_status": "0"}, index=frame.index)
    cli.update_by_column(settings.result_path, patch)
    summary["applied"] = True
    manifest_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("重置完成：记录=%d，仅将第二、三轮状态设为 0；备份=%s", len(patch), output)
    return summary


def main(argv=None, cli=None):
    root = Path(__file__).absolute().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(root / "configs" / "event_processing_config.yaml"),
                        help="事件配置路径；读取其中的结果表、起止日期和资产名单")
    parser.add_argument("--apply", action="store_true", help="备份后执行重置；默认只预览，不写数据库")
    parser.add_argument("--backup-dir", help="新的本地备份目录；默认 reports/round2_reset_北京时间/")
    args = parser.parse_args(argv)
    settings = load_event_config(args.config)
    if not settings.start_time or not settings.end_time:
        parser.error("请先在事件配置中填写 start_time/end_time")
    logging.basicConfig(**(settings.logging or {"level": "INFO"}))
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    return reset_round2_status(cli, settings, apply=args.apply, backup_dir=args.backup_dir)


if __name__ == "__main__":
    main()
