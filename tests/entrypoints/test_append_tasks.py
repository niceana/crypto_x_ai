"""已补录批次只能追加新的第一轮任务，不覆盖历史结果。"""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from common.validation import TASK_COLUMNS
from entrypoints.append_tasks import append_tasks_from_plan, main


def posts(rows):
    return pd.DataFrame({"text": [row[2] for row in rows], "user_name": ["alice"] * len(rows)},
                        index=pd.MultiIndex.from_arrays([
                            pd.DatetimeIndex([row[0] for row in rows]), [row[1] for row in rows]], names=["date", "id"]))


def tasks(frame):
    result = pd.DataFrame("", index=frame.index, columns=TASK_COLUMNS)
    result[["text", "user_name"]] = frame[["text", "user_name"]]
    result["status"] = "1"
    result["config_hash"] = "existing-hash"
    result["ai_response_content"] = "已有 AI 结果"
    return result


class AppendTasksTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name)
        self.batch = posts([("2026-09-01 12:00:00.123456789", "shared", "已建任务帖子"),
                            ("2026-11-06 08:11:07", "new", "本次新任务")])
        self.existing = tasks(pd.concat([posts([("2026-08-01", "old", "历史帖")]), self.batch.iloc[:1]]))
        self.settings = {"source_path": "raw-posts", "task_path": "round1-tasks"}
        self.cli = Mock(spec=["read", "update_by_index"])
        self.cli.read.side_effect = lambda path, **kwargs: self.batch.copy() if path == "raw-posts" else self.existing.copy()
        self.save_plan()

    def save_plan(self):
        path = self.output / "new_posts.pkl"
        self.batch.to_pickle(path)
        plan = {"version": 1, "source_path": "raw-posts", "summary": {"new_rows": len(self.batch)},
                "sha256": {"new_posts.pkl": hashlib.sha256(path.read_bytes()).hexdigest()}}
        (self.output / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
        (self.output / "apply_result.json").write_text(json.dumps({"source_path": "raw-posts", "written_rows": len(self.batch)}), encoding="utf-8")

    def run_append(self, **kwargs):
        return append_tasks_from_plan(self.cli, self.settings, self.output, **kwargs)

    def test_preview_shows_exact_missing_tasks_and_dates_without_writing(self):
        result = self.run_append()
        self.assertEqual(result["already_in_tasks"], 1)
        self.assertEqual(result["new_tasks"], 1)
        self.assertEqual(result["start_time"], "2026-09-01")
        self.assertEqual(result["end_time"], "2026-11-06")
        self.assertEqual(result["written_rows"], 0)
        self.cli.update_by_index.assert_not_called()
        frame = pd.read_csv(self.output / "round1_tasks_to_append.csv", dtype=str, keep_default_na=False)
        self.assertEqual(list(frame["id"]), ["new"])
        self.assertEqual(frame.iloc[0]["status"], "0")

    def test_apply_backs_up_and_appends_only_new_tasks_then_repeat_is_noop(self):
        before = self.existing.copy(deep=True)
        def submit(path, frame):
            backups = list((self.output / "round1_backups").glob("*/tasks.pkl"))
            self.assertEqual(len(backups), 1)
            pd.testing.assert_frame_equal(pd.read_pickle(backups[0]), before, check_exact=True)
            self.assertEqual(path, "round1-tasks")
        self.cli.update_by_index.side_effect = submit
        result = self.run_append(apply=True)
        self.assertEqual(result["written_rows"], 1)
        frame = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(list(frame.index.get_level_values("id")), ["new"])
        self.assertEqual(list(frame.columns), TASK_COLUMNS)
        self.assertEqual(frame.iloc[0]["status"], "0")
        self.assertTrue(frame.drop(columns=["text", "user_name", "status"]).eq("").all().all())
        pd.testing.assert_frame_equal(self.existing, before, check_exact=True)
        self.existing = pd.concat([self.existing, frame])
        self.cli.update_by_index.reset_mock()
        result = self.run_append(apply=True)
        self.assertEqual(result["written_rows"], 0)
        self.cli.update_by_index.assert_not_called()

    def test_refuses_missing_success_marker_or_changed_snapshot_before_database_access(self):
        (self.output / "apply_result.json").unlink()
        with self.assertRaises(FileNotFoundError):
            self.run_append(apply=True)
        self.save_plan()
        path = self.output / "new_posts.pkl"
        path.write_bytes(path.read_bytes() + b"changed")
        with self.assertRaisesRegex(ValueError, "与补录时不一致"):
            self.run_append(apply=True)
        self.cli.read.assert_not_called()
        self.cli.update_by_index.assert_not_called()

    def test_refuses_different_source_and_same_source_target(self):
        for settings in ({"source_path": "different", "task_path": "round1-tasks"},
                         {"source_path": "raw-posts", "task_path": "raw-posts"}):
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                append_tasks_from_plan(self.cli, settings, self.output, apply=True)
        self.cli.read.assert_not_called()
        self.cli.update_by_index.assert_not_called()

    def test_refuses_missing_or_changed_raw_posts(self):
        changed = self.batch.copy()
        changed.iloc[0, 0] = "被修改的正文"
        for raw in (self.batch.iloc[:1], changed):
            self.cli.read.side_effect = lambda path, **kwargs: raw
            with self.subTest(rows=len(raw)), self.assertRaisesRegex(ValueError, "原表缺少|与补录批次不一致"):
                self.run_append(apply=True)
        self.cli.update_by_index.assert_not_called()

    def test_refuses_existing_task_input_or_id_date_conflicts(self):
        changed = self.existing.copy()
        changed.loc[self.batch.index[0], "text"] = "任务旧输入被改"
        conflicting_id = tasks(posts([("2026-10-01", "new", "相同 ID 不同日期")]))
        for old in (changed, pd.concat([self.existing, conflicting_id])):
            self.cli.read.side_effect = lambda path, **kwargs: self.batch if path == "raw-posts" else old
            with self.subTest(rows=len(old)), self.assertRaisesRegex(ValueError, "不一致|不同 date"):
                self.run_append(apply=True)
        self.cli.update_by_index.assert_not_called()

    def test_backup_failure_and_database_failure_stop_without_touching_old_tasks(self):
        with patch("entrypoints.append_tasks.pd.DataFrame.to_pickle", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.run_append(apply=True)
        self.cli.update_by_index.assert_not_called()
        self.cli.update_by_index.side_effect = ConnectionError("offline")
        with self.assertRaises(ConnectionError):
            self.run_append(apply=True)
        self.cli.update_by_index.assert_called_once()

    def test_main_uses_round1_lock_and_does_not_call_ai(self):
        with patch("entrypoints.append_tasks.load_job_config", return_value=self.settings), \
                patch("entrypoints.append_tasks.stage_guard") as guard, \
                patch("entrypoints.append_tasks.logging.basicConfig"):
            result = main(self.cli, argv=["preview", str(self.output)])
        guard.assert_called_once_with("round1", "round1-tasks")
        self.assertEqual(result["new_tasks"], 1)
        self.cli.update_by_index.assert_not_called()


if __name__ == "__main__":
    unittest.main()
