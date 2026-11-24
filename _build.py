"""发行构建：从原源码生成私有命名空间，只改导入路径，不维护第二份业务实现。"""

import ast
import re
from pathlib import Path

from setuptools.command.build_py import build_py


ROOTS = ("common", "initialization", "round1", "round2", "round3", "vectorization", "postprocessing")
WEIGHT_FILES = {"__init__.py", "weight_data.py", "weight_parameters.py", "weight_time.py"}
CONFIG_FILES = ("event_processing_config.yaml", "round1_ai_config.yaml", "round2_ai_config.yaml",
                "round3_ai_config.yaml", "embedding_config.yaml")
PREFIX = "crypto_x_ai_live._engine"


def isolate_imports(source):
    """通过 AST 定位真实 import，保留所有业务语句、字符串及原始格式。"""
    lines = source.splitlines(keepends=True)
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and not node.level and node.module.split(".", 1)[0] in ROOTS:
            line = lines[node.lineno - 1]
            lines[node.lineno - 1] = re.sub(
                r"\bfrom\s+" + re.escape(node.module) + r"\s+import\b",
                "from " + PREFIX + "." + node.module + " import", line, count=1)
        elif isinstance(node, ast.Import) and any(alias.name.split(".", 1)[0] in ROOTS for alias in node.names):
            raise ValueError("内部模块请使用 from ... import ...，以保证发行包命名空间隔离")
    return "".join(lines)


class LiveBuild(build_py):
    def run(self):
        super().run()
        root = Path(__file__).resolve().parent
        target = Path(self.build_lib) / "crypto_x_ai_live"
        engine = target / "_engine"
        engine.mkdir(parents=True, exist_ok=True)
        (engine / "__init__.py").write_text('"""由同一仓库源码构建的内部业务模块。"""\n', encoding="utf-8")
        for package in ROOTS:
            for source in (root / package).glob("*.py"):
                if package == "postprocessing" and source.name not in WEIGHT_FILES:
                    continue
                destination = engine / package / source.name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text(isolate_imports(source.read_text(encoding="utf-8")), encoding="utf-8")
        for name in CONFIG_FILES:
            destination = target / "configs" / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes((root / "configs" / name).read_bytes())
