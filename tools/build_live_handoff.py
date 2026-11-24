"""生成当前版本的精简实盘交接包；业务 wheel 须提前构建。"""
from pathlib import Path
import json
import sys

WORK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORK))
from tools.build_linux_trial_zip import CONFIGS, VERSION, WHEEL_NAME, _write_bundle, _write_index


def build():
    entries = {
        "README.md": WORK / "docs/LIVE_HOST_HANDOFF.md",
        "LIVE_INTEGRATION.md": WORK / "docs/LIVE_INTEGRATION.md",
        "call_example.py": WORK / "examples/live_host_call.py",
        "wheels/" + WHEEL_NAME: WORK / "dist" / WHEEL_NAME,
    }
    entries.update({"configs/" + name: WORK / "configs" / name for name in CONFIGS})
    prefix = "crypto_x_ai_live_handoff_{}/".format(VERSION)
    result = _write_bundle(WORK / "dist" / (prefix.rstrip("/") + ".zip"), entries, prefix)
    _write_index(WORK / "dist")
    return result


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False))
