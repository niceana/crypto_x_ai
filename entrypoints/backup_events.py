"""只读备份配置范围内的事件结果，保留原索引、全部字段和向量，不重置状态。"""

import argparse
import hashlib
import json
import logging
import platform
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from common.config import load_event_config
from common.store import read_event_history

logger = logging.getLogger(__name__)


def backup_events(cli, settings, output_dir=None, *, completed_only=False, expected_records=None):
    if not settings.start_time or not settings.end_time:
        raise ValueError("请先配置 start_time/end_time，明确本次备份日期范围")
    if date.fromisoformat(settings.start_time) > date.fromisoformat(settings.end_time):
        raise ValueError("start_time 不能晚于 end_time")
    if expected_records is not None and (type(expected_records) is not int or expected_records <= 0):
        raise ValueError("expected_records 必须为正整数")

    frame = read_event_history(cli, settings.result_path, settings.start_time, settings.end_time)
    if settings.round2_asset_list_path:
        universe = json.loads(cli.read_object(settings.round2_asset_list_path).decode("utf-8"))["InstrumentList"]
        assets = {instrument.split(".", 1)[0].strip().upper() for instrument in universe}
        frame = frame.loc[frame["asset"].isin(assets)]
    scope_records = len(frame)
    if completed_only:
        frame = frame.loc[frame["round2_status"] == "1"]
    frame = frame.sort_index()
    if expected_records is not None and len(frame) != expected_records:
        raise ValueError("备份记录数为 {}，与预期 {} 不符；请核对日期、资产名单和完成状态，未生成备份".format(
            len(frame), expected_records))
    if frame.empty:
        raise ValueError("筛选范围内没有可备份记录，请核对日期、资产名单和完成状态")

    now = datetime.now(timezone(timedelta(hours=8)))
    if output_dir is None:
        output_dir = (Path(__file__).absolute().parent.parent / "reports" /
                      ("events_backup_" + now.strftime("%Y%m%d_%H%M%S_%f")))
    output = Path(output_dir).absolute()
    output.mkdir(parents=True, exist_ok=False)
    logger.info("开始只读备份：表=%s，日期=%s 至 %s，资产筛选后=%d，备份记录=%d，目录=%s",
                settings.result_path, settings.start_time, settings.end_time, scope_records, len(frame), output)

    frame.to_pickle(output / "events.pkl")
    # 主备份读回后逐值、逐索引和 dtype 校验；不从数据库再次读取变化中的数据。
    pd.testing.assert_frame_equal(frame, pd.read_pickle(output / "events.pkl"), check_exact=True)
    frame.to_csv(output / "events.csv", encoding="utf-8-sig")
    files = {}
    for name in ("events.pkl", "events.csv"):
        path = output / name
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        files[name] = {"sha256": digest.hexdigest(), "bytes": path.stat().st_size}

    manifest = {
        "completed": True, "created_at": now.isoformat(), "result_path": settings.result_path,
        "start_time": settings.start_time, "end_time": settings.end_time,
        "round2_asset_list_path": settings.round2_asset_list_path,
        "completed_only": completed_only, "scope_records": scope_records, "records": len(frame),
        "assets": sorted(set(frame["asset"])),
        "round2_completed": int(frame["round2_status"].eq("1").sum()),
        "round3_completed": int(frame["round3_status"].eq("1").sum()),
        "index_names": list(frame.index.names), "columns": list(frame.columns),
        "python_version": platform.python_version(), "pandas_version": pd.__version__,
        "files": files, "output_dir": str(output),
    }
    # 完成说明最后写入；导出或校验中断时不会留下成功标记。
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("备份完成并通过读回校验：records=%d，第二轮成功=%d，第三轮成功=%d，目录=%s",
                len(frame), manifest["round2_completed"], manifest["round3_completed"], output)
    return manifest


def main(argv=None, cli=None):
    root = Path(__file__).absolute().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(root / "configs" / "event_processing_config.yaml"),
                        help="事件配置路径；读取其中的结果表、起止日期和资产名单")
    parser.add_argument("--completed-only", action="store_true", help="仅备份第二轮已成功的记录；默认保留所有状态")
    parser.add_argument("--expected-records", type=int, help="预期备份条数；不符时停止，不截取或补足")
    parser.add_argument("--output-dir", help="新的本地备份目录；默认 reports/events_backup_北京时间/")
    args = parser.parse_args(argv)
    settings = load_event_config(args.config)
    if not settings.start_time or not settings.end_time:
        parser.error("请先在事件配置中填写 start_time/end_time")
    if args.expected_records is not None and args.expected_records <= 0:
        parser.error("--expected-records 必须为正整数")
    logging.basicConfig(**(settings.logging or {"level": "INFO"}))
    if cli is None:
        from xdata.client import get_client
        cli = get_client()
    return backup_events(cli, settings, args.output_dir, completed_only=args.completed_only,
                         expected_records=args.expected_records)


if __name__ == "__main__":
    main()
