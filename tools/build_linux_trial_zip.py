"""从纳入 Git 的脚本、文档和配置构建试跑 ZIP；不从 reports 读取源码。"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import zipfile

WORK = Path(__file__).resolve().parents[1]
VERSION = re.search(r'(?m)^version\s*=\s*"([^"]+)"', (WORK / "pyproject.toml").read_text(encoding="utf-8")).group(1)
WHEEL_NAME = "crypto_x_ai_live-{}-py3-none-any.whl".format(VERSION)
SCRIPTS = ("_trial.py", "_replay.py", "run_full.py", "check_weight.py", "export_xdata.py", "export_hours.py", "replay_hours.py",
           "summarize_diffs.py", "compare_post_results.py", "compare_post_weights.py")
DOCUMENTS = ("README.md", "EXPORT_XDATA.md", "BATCH_REPLAY.md", "LOGGING.md", "DIFF_SUMMARY.md", "AI_RESULT_COMPARISON.md",
             "POST_WEIGHT_COMPARISON.md")
CONFIGS = (".env.example", "event_processing_config.yaml", "round1_ai_config.yaml", "round2_ai_config.yaml",
           "round3_ai_config.yaml", "embedding_config.yaml")


def bundle_sources(wheel_path):
    """交付包中的相对路径到仓库唯一源文件的映射。"""
    entries = {name: WORK / "tools" / "live_trial" / name for name in SCRIPTS}
    entries.update({name: WORK / "docs" / "live_trial" / name for name in DOCUMENTS})
    entries.update({"configs/" + name: WORK / "configs" / name for name in CONFIGS})
    entries["data/assets.example.json"] = WORK / "tools" / "live_trial" / "assets.example.json"
    entries["wheels/" + WHEEL_NAME] = Path(wheel_path)
    return entries


def _write_bundle(target, entries, prefix=""):
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, source in entries.items():
            archive.write(source, prefix + name)
    with zipfile.ZipFile(target) as archive:
        if archive.testzip() is not None or set(archive.namelist()) != {prefix + name for name in entries}:
            raise RuntimeError("ZIP 完整性或成员清单不匹配：{}".format(target))
        for name, source in entries.items():
            if archive.read(prefix + name) != source.read_bytes():
                raise RuntimeError("ZIP 内容与源文件不匹配：{}".format(name))
    return dict(zip=str(target), files=len(entries), size=target.stat().st_size,
                sha256=hashlib.sha256(target.read_bytes()).hexdigest())


def _write_index(output):
    candidates = (
        (WHEEL_NAME, "业务安装包：安装到实盘 Python 解释器"),
        ("crypto_x_ai_live-{}.tar.gz".format(VERSION), "当前版本业务源码发行包"),
        ("crypto_x_ai_linux_trial_{}.zip".format(VERSION), "完整 Linux 试跑包：导出、全流程、240 小时回放、日志及配置模板"),
        ("crypto_x_ai_live_handoff_{}.zip".format(VERSION), "宿主接入交付包：调用示例、配置和接入说明"),
    )
    lines = ["# 当前交付文件（{}）".format(VERSION), "", "| 文件 | 用途 |", "| --- | --- |"]
    for name, purpose in candidates:
        if (output / name).is_file():
            lines.append("| [{}]({}) | {} |".format(name, name, purpose))
    lines.extend(["", "完整试跑包已包含导出、回放和日志脚本，一般不需要另外下载功能补充包。",
                  "如显式生成补充包，它们位于 supplements/；历史构建归档在仓库 reports/dist_archive/。",
                  "维护源码和打包规则见 [发行文件说明](../docs/DISTRIBUTIONS.md)。", ""])
    (output / "README.md").write_text("\n".join(lines), encoding="utf-8")


def build_bundles(output_dir=None, wheel_path=None, include_supplements=False):
    wheel_path = Path(wheel_path or WORK / "dist" / WHEEL_NAME).expanduser().resolve()
    entries = bundle_sources(wheel_path)
    for source in entries.values():
        if not source.is_file():
            raise FileNotFoundError("缺少构建输入 {}；业务 wheel 请先构建到 dist/ 或用 --wheel 指定".format(source))
    output = Path(output_dir or WORK / "dist").expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    root = "crypto_x_ai_linux_trial_{}/".format(VERSION)
    result = [_write_bundle(output / (root.rstrip("/") + ".zip"), entries, prefix=root)]
    supplements = {
        "xdata_export": ("export_xdata.py", "check_weight.py", "EXPORT_XDATA.md"),
        "240h_replay": ("_replay.py", "export_hours.py", "replay_hours.py", "BATCH_REPLAY.md", "_trial.py", "LOGGING.md"),
        "logging": ("_trial.py", "run_full.py", "replay_hours.py", "LOGGING.md"),
        "diff_summary": ("summarize_diffs.py", "DIFF_SUMMARY.md"),
        "ai_result_compare": ("compare_post_results.py", "AI_RESULT_COMPARISON.md"),
        "post_weight_compare": ("compare_post_weights.py", "POST_WEIGHT_COMPARISON.md"),
    }
    if include_supplements:
        supplement_dir = output / "supplements"
        supplement_dir.mkdir(exist_ok=True)
        for kind, names in supplements.items():
            target = supplement_dir / "crypto_x_ai_{}_tools_{}.zip".format(kind, VERSION)
            result.append(_write_bundle(target, {name: entries[name] for name in names}))
    _write_index(output)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", help="业务 wheel 路径，默认 dist/ 下当前版本")
    parser.add_argument("--output-dir", help="ZIP 输出目录，默认 dist/")
    parser.add_argument("--with-supplements", action="store_true", help="额外按功能生成补充包，放到输出目录的 supplements/ 下")
    args = parser.parse_args(argv)
    for result in build_bundles(args.output_dir, args.wheel, args.with_supplements):
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
