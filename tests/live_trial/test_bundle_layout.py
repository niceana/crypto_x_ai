"""确认交付包只取正式源码，并同时支持仓库和解压后的目录。"""

import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile

WORK = Path(__file__).resolve().parents[2]


def load_file(name, filename):
    spec = importlib.util.spec_from_file_location(name, filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = load_file("live_trial_bundle_builder", WORK / "tools/build_linux_trial_zip.py")


class BundleLayoutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="trial-bundle-layout-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.wheel = self.root / builder.WHEEL_NAME
        with zipfile.ZipFile(self.wheel, "w") as archive:
            archive.writestr("fixture.txt", "Only tests archive layout; never installed.")

    def test_build_inputs_have_no_report_source_dependency(self):
        for name, source in builder.bundle_sources(self.wheel).items():
            if name.startswith("wheels/"):
                continue
            self.assertTrue(source.is_file(), source)
            self.assertIn(source.relative_to(WORK).parts[0], ("tools", "docs", "configs"))

    def test_default_builds_only_complete_bundle_without_secrets_or_caches(self):
        output = self.root / "dist"
        results = builder.build_bundles(output, self.wheel)
        self.assertEqual(len(results), 1)
        self.assertFalse((output / "supplements").exists())
        self.assertIn("完整 Linux 试跑包", (output / "README.md").read_text(encoding="utf-8"))
        full = output / "crypto_x_ai_linux_trial_{}.zip".format(builder.VERSION)
        prefix = "crypto_x_ai_linux_trial_{}/".format(builder.VERSION)
        with zipfile.ZipFile(full) as archive:
            self.assertEqual(len(archive.namelist()), 25)
            self.assertEqual(archive.read(prefix + "wheels/" + builder.WHEEL_NAME), self.wheel.read_bytes())
            self.assertEqual(archive.read(prefix + "run_full.py"), (WORK / "tools/live_trial/run_full.py").read_bytes())
            self.assertEqual(archive.read(prefix + "summarize_diffs.py"), (WORK / "tools/live_trial/summarize_diffs.py").read_bytes())
            self.assertEqual(archive.read(prefix + "README.md"), (WORK / "docs/live_trial/README.md").read_bytes())
            self.assertEqual(archive.read(prefix + "configs/round1_ai_config.yaml"), (WORK / "configs/round1_ai_config.yaml").read_bytes())
            for name in archive.namelist():
                self.assertNotIn("__pycache__", name)
                self.assertNotIn("reports/", name)
                self.assertFalse(name.endswith("/.env"))
        for result in results:
            self.assertEqual(len(result["sha256"]), 64)

    def test_supplements_are_explicit_and_written_outside_top_level(self):
        output = self.root / "dist"
        results = builder.build_bundles(output, self.wheel, include_supplements=True)
        self.assertEqual(len(results), 7)
        self.assertEqual(len(list(output.glob("*.zip"))), 1)
        self.assertEqual(len(list((output / "supplements").glob("*.zip"))), 6)
        for item in results[1:]:
            self.assertEqual(Path(item["zip"]).parent, output / "supplements")

    def test_runtime_outputs_and_config_defaults_follow_repository_or_flat_layout(self):
        source = load_file("trial_source_layout", WORK / "tools/live_trial/_trial.py")
        self.assertEqual(source.PROJECT_ROOT, WORK)
        self.assertEqual(source.ROOT, WORK / "reports/live_trial")
        self.assertEqual(source.DEFAULT_CONFIG_DIR, WORK / "configs")
        flat_file = self.root / "_trial.py"
        flat_file.write_bytes((WORK / "tools/live_trial/_trial.py").read_bytes())
        flat = load_file("trial_flat_layout", flat_file)
        self.assertIsNone(flat.PROJECT_ROOT)
        self.assertEqual(flat.ROOT, self.root)
        self.assertEqual(flat.DEFAULT_CONFIG_DIR, self.root / "configs")


if __name__ == "__main__":
    unittest.main()
