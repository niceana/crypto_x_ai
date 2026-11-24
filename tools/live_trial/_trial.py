"""独立试跑工具的文件和测试时钟辅助；不修改已安装的业务包。"""

import argparse
import json
import logging
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
_candidate = SCRIPT_DIR.parent.parent
PROJECT_ROOT = (_candidate if SCRIPT_DIR.parent.name == "tools" and SCRIPT_DIR.name == "live_trial"
                and (_candidate / "pyproject.toml").is_file() else None)
# 仓库运行时，产物进入忽略目录；解压交付包后仍沿用原来的 data/outputs 布局。
ROOT = PROJECT_ROOT / "reports" / "live_trial" if PROJECT_ROOT is not None else SCRIPT_DIR
DEFAULT_CONFIG_DIR = PROJECT_ROOT / "configs" if PROJECT_ROOT is not None else SCRIPT_DIR / "configs"
BEIJING = timezone(timedelta(hours=8))


class _BatchLogContext(logging.Filter):
    batch_time = "-"

    def filter(self, record):
        record.batch_time = self.batch_time
        return True


class _TrialFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        return datetime.fromtimestamp(record.created, BEIJING).strftime(datefmt or "%Y-%m-%d %H:%M:%S")


_log_context = _BatchLogContext()
_log_formatter = _TrialFormatter("%(asctime)s UTC+8 | %(levelname)s | %(name)s:%(lineno)d | "
                                 "%(threadName)s | batch=%(batch_time)s | %(message)s")


def set_log_batch(value=None):
    """串行批次共享上下文，使工作线程日志也能显示当前模拟时刻。"""
    _log_context.batch_time = "-" if value is None else str(value)


def init_logging(level="INFO"):
    """仅由独立调用脚本显式执行；不在业务包导入或函数内部修改宿主日志。"""
    if str(level).upper() not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        raise ValueError("LOG_LEVEL 必须为 DEBUG/INFO/WARNING/ERROR/CRITICAL")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_log_formatter)
    handler.addFilter(_log_context)
    # 独立脚本统一接管本进程日志，避免已有 handler 让 basicConfig 静默失效。
    logging.basicConfig(level=str(level).upper(), handlers=[handler], force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    set_log_batch()


def add_log_file(filename):
    filename = Path(filename).expanduser().resolve()
    filename.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, logging.FileHandler) and handler.baseFilename == str(filename):
            return handler
    handler = logging.FileHandler(filename, mode="a", encoding="utf-8")
    handler.setFormatter(_log_formatter)
    handler.addFilter(_log_context)
    root.addHandler(handler)
    logging.getLogger("live_trial").info("源模块日志已启用：level=%s，file=%s", logging.getLevelName(root.level), filename)
    return handler


def path(value):
    value = Path(value).expanduser()
    return value if value.is_absolute() else ROOT / value


def load_frame(value):
    frame = pd.read_pickle(path(value))
    if not isinstance(frame, pd.DataFrame):
        raise ValueError("{} 必须保存 pandas.DataFrame".format(value))
    return frame


def assets(value):
    document = json.loads(path(value).read_text(encoding="utf-8"))
    if isinstance(document, dict):
        document = document.get("InstrumentList")
    if not isinstance(document, list) or not document:
        raise ValueError("assets.json 必须为非空字符串数组，或含 InstrumentList 的对象")
    return document


def arguments(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config-dir", default=str(DEFAULT_CONFIG_DIR), help="配置目录；默认使用仓库或交付包的 configs/")
    parser.add_argument("--assets", default="data/assets.json")
    parser.add_argument("--method", choices=("sum", "max_abs"), required=True, help="必须与要比较的旧表一致")
    parser.add_argument("--weight-param-id", help="旧配置有多组评分参数时选择对应 param_id")
    parser.add_argument("--test-time", help="仅测试：模拟 UTC+8 当前时刻，如 2025-01-01 16:47:00")
    return parser


def current_time(test_time=None):
    if test_time is None:
        now = pd.Timestamp.now(tz="UTC")
    else:
        now = pd.Timestamp(test_time)
        if pd.isna(now):
            raise ValueError("test-time 不能缺失")
        if now.tzinfo is None:
            now = now.tz_localize(BEIJING)
    return now.tz_convert("UTC"), now.tz_convert(BEIJING).tz_localize(None)


@contextmanager
def test_clock(test_time):
    if test_time is None:
        yield
    else:
        utc, _ = current_time(test_time)
        # 只在这个测试脚本的调用期间模拟时钟，正式接口没有时间参数。
        with patch("crypto_x_ai_live.runner._system_now", return_value=utc):
            yield


def output_dir(prefix):
    _, local = current_time()
    target = ROOT / "outputs" / (prefix + "_" + local.strftime("%Y%m%d_%H%M%S_%f"))
    target.mkdir(parents=True, exist_ok=False)
    return target


def dump_json(target, document):
    target.write_text(json.dumps(document, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def save_tables(target, result):
    for name in ("round1_results", "event_results", "weight"):
        result[name].to_pickle(target / (name + ".pkl"))
        result[name].to_csv(target / (name + ".csv"), encoding="utf-8-sig")


def merge_events(history, delta):
    if history.shape == (0, 0):
        return delta.copy(deep=True).sort_index()
    if delta.empty:
        return history.copy(deep=True).sort_index()
    merged = pd.concat([history, delta])
    return merged.loc[~merged.index.duplicated(keep="last")].sort_index()


def score_parameters(config_directory, param_id):
    # 只读评分配置，不加载 AI 接口及密钥，也不检查或生成向量。
    from crypto_x_ai_live import _compat as core
    settings = core.config.load_event_config(path(config_directory) / "event_processing_config.yaml")
    plan = core.weight_parameters.prepare_weight_grid("trial_weight", settings.weight_grid)
    variants = {variant.parameters.param_id: variant.parameters for variant in plan.variants}
    if param_id is not None:
        if param_id not in variants:
            raise ValueError("weight-param-id 不在配置网格中")
        return variants[param_id]
    if len(variants) != 1:
        raise ValueError("配置有多个评分组合，请用 --weight-param-id 选择旧表对应的一组")
    return next(iter(variants.values()))
