"""追加缺失任务并按状态刷新待处理正文，保留已成功任务和所有非 text 字段。"""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from common.validation import TASK_COLUMNS
from entrypoints.append_tasks import append_tasks_from_plan, append_tasks_from_source, main


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
        self.output = Path(self.temp.name).resolve()
        self.batch = posts([("2026-09-01 12:00:00.123456789", "shared", "已建任务帖子"),
                            ("2026-11-06 08:11:07", "new", "本次新任务")])
        self.existing = tasks(pd.concat([posts([("2026-08-01", "old", "历史帖")]), self.batch.iloc[:1]]))
        self.settings = {"source_path": "raw-posts", "task_path": "round1-tasks"}
        self.cli = Mock(spec=["read", "update_by_index", "update_by_column"])
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
        changed.loc[self.batch.index[0], "status"] = "9"
        conflicting_id = tasks(posts([("2026-10-01", "new", "相同 ID 不同日期")]))
        for old in (changed, pd.concat([self.existing, conflicting_id])):
            self.cli.read.side_effect = lambda path, **kwargs: self.batch if path == "raw-posts" else old
            with self.subTest(rows=len(old)), self.assertRaisesRegex(ValueError, "不一致|不同 date") as caught:
                self.run_append(apply=True)
            if old is changed:
                path = self.output / "round1_task_conflicts.csv"
                self.assertIn(str(path), str(caught.exception))
                report = pd.read_csv(path, dtype=str, keep_default_na=False)
                self.assertEqual(list(report["id"]), ["shared"])
                self.assertEqual(report.iloc[0]["existing_text"], "任务旧输入被改")
                self.assertEqual(report.iloc[0]["incoming_text"], "已建任务帖子")
        self.cli.update_by_index.assert_not_called()

    def test_plan_mode_refreshes_only_pending_text_and_also_appends_new_tasks(self):
        key = self.batch.index[0]
        self.existing.loc[key, "status"] = "0"
        self.existing.loc[key, "text"] = "旧待处理正文"
        before = self.existing.copy(deep=True)

        def update_text(path, frame):
            self.assertEqual(path, "round1-tasks")
            self.assertEqual(list(frame.columns), ["text"])
            self.assertEqual(list(frame.index), [key])
            backup = next((self.output / "round1_backups").glob("*/tasks.pkl"))
            pd.testing.assert_frame_equal(pd.read_pickle(backup), before, check_exact=True)
            self.existing.loc[frame.index, frame.columns] = frame.to_numpy()

        self.cli.update_by_column.side_effect = update_text
        result = self.run_append(apply=True)
        self.assertEqual(result["written_text_updates"], 1)
        self.assertEqual(result["written_rows"], 1)
        self.assertEqual(self.existing.loc[key, "text"], "已建任务帖子")
        pd.testing.assert_frame_equal(self.existing.drop(columns="text"), before.drop(columns="text"), check_exact=True)
        self.cli.update_by_column.assert_called_once()
        self.assertEqual(list(self.cli.update_by_index.call_args.args[1].index.get_level_values("id")), ["new"])

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


def crawler(rows):
    """每行是采集时间、普通 id、原帖 created_at、正文。"""
    return pd.DataFrame({"id": [row[1] for row in rows], "created_at": [row[2] for row in rows],
                         "text": [row[3] for row in rows], "username": ["alice"] * len(rows)},
                        index=pd.DatetimeIndex([row[0] for row in rows], name="date"))


class CrawlerAppendTasksTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name).resolve()
        self.settings = {"source_path": "crawler-posts", "task_path": "round1-tasks",
                         "start_time": "2026-11-04", "end_time": "2026-11-15"}
        self.raw = crawler([
            ("2026-11-16 12:00", "shared", "2026-11-04T12:00:00Z", "已有任务"),
            ("2026-11-16 12:00", "late", "2026-11-03T23:59:59Z", "范围之前"),
            ("2026-11-16 12:00", "new", "2026-11-15T23:59:59.123456789Z", "新增任务"),
            ("2026-11-17 12:00", "new", "2026-11-15T23:59:59.123456789Z", "新增任务"),
            ("2026-11-16 12:00", "next", "2026-11-16T00:00:00Z", "留给实盘"),
        ])
        self.existing = tasks(posts([("2026-11-04 12:00:00", "shared", "已有任务")]))
        self.cli = Mock(spec=["read", "update_by_index", "update_by_column"])
        self.cli.read.side_effect = lambda path, **kwargs: (
            self.raw.copy(deep=True) if path == "crawler-posts" else self.existing.copy(deep=True))

    def run_append(self, **kwargs):
        return append_tasks_from_source(self.cli, self.settings, report_dir=self.output, **kwargs)

    def add_target_account(self, username):
        seed = tasks(posts([("2020-01-01", "account-seed-" + username, "账号范围来源")]))
        seed["user_name"] = username
        self.existing = pd.concat([self.existing, seed]).sort_index()

    def test_preview_uses_publication_range_not_capture_date_and_deduplicates(self):
        before = self.raw.copy(deep=True)
        result = self.run_append()
        self.assertEqual((result["batch_rows"], result["already_in_tasks"], result["new_tasks"]), (2, 1, 1))
        self.cli.read.assert_any_call("crawler-posts", start_time="", end_time="",
                                      columns=["id", "created_at", "text", "username"])
        self.cli.update_by_index.assert_not_called()
        frame = pd.read_csv(Path(result["report_dir"]) / "round1_tasks_to_append.csv", dtype=str)
        self.assertEqual(list(frame["id"]), ["new"])
        self.assertEqual(frame.iloc[0]["date"], "2026-11-15 23:59:59.123456789")
        self.assertEqual(frame.iloc[0]["status"], "0")
        pd.testing.assert_frame_equal(self.raw, before, check_exact=True)

    def test_apply_preserves_old_results_backs_up_and_repeat_skips_existing(self):
        before = self.existing.copy(deep=True)

        def submit(path, frame):
            backups = list(self.output.glob("*/round1_backups/*/tasks.pkl"))
            self.assertEqual(len(backups), 1)
            pd.testing.assert_frame_equal(pd.read_pickle(backups[0]), before, check_exact=True)
            self.assertEqual(path, "round1-tasks")
            self.assertEqual(frame.index[0], (pd.Timestamp("2026-11-15 23:59:59.123456789"), "new"))
            self.assertTrue(frame.index.is_unique and frame.index.is_monotonic_increasing)
            self.assertEqual(list(frame.columns), TASK_COLUMNS)
            self.assertEqual(frame.iloc[0]["status"], "0")
            self.assertEqual(frame.iloc[0]["user_name"], "alice")
            self.assertTrue(frame.drop(columns=["text", "user_name", "status"]).eq("").all().all())
            self.existing = pd.concat([self.existing, frame]).sort_index()

        self.cli.update_by_index.side_effect = submit
        self.assertEqual(self.run_append(apply=True)["written_rows"], 1)
        pd.testing.assert_frame_equal(self.existing.loc[before.index], before, check_exact=True)
        self.cli.update_by_index.reset_mock()
        self.assertEqual(self.run_append(apply=True)["written_rows"], 0)
        self.cli.update_by_index.assert_not_called()

    def test_normalizes_timezone_and_twitter_text_preserving_nanoseconds(self):
        self.raw = crawler([
            ("2026-11-16", "offset", "2026-11-16T07:59:59.123456789+08:00", "UTC仍在15日"),
            ("2026-11-16", "x-date", "Wed Nov 04 00:00:00 +0000 2026", "X时间格式"),
            ("2026-11-16", "naive", "2026-11-15 23:59:59", "无时区按UTC"),
            ("2026-11-16", "boundary", "2026-11-16T08:00:00+08:00", "UTC已到16日"),
        ])
        self.run_append(apply=True)
        frame = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(set(frame.index.get_level_values("id")), {"offset", "x-date", "naive"})
        self.assertEqual(frame.xs("offset", level="id").index[0], pd.Timestamp("2026-11-15 23:59:59.123456789"))
        self.assertIsNone(frame.index.get_level_values("date").tz)

    def test_shared_publication_time_and_earlier_missing_posts_are_still_appended(self):
        self.raw = crawler([
            ("2026-11-16", "same-time", "2026-11-04T12:00:00Z", "同刻新ID"),
            ("2026-11-16", "earlier", "2026-11-04T00:00:00Z", "较早漏帖"),
        ])
        self.run_append(apply=True)
        frame = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(list(frame.index.get_level_values("id")), ["earlier", "same-time"])

    def test_excludes_wublockchain_before_validation_and_keeps_other_authors_with_same_id(self):
        self.add_target_account("WuBlockchain")
        self.add_target_account("WuBlockchain12")
        self.raw = crawler([
            ("2026-11-16", "same", "invalid", None),
            ("2026-11-16", "same", "2026-11-05T00:00:00Z", "保留真实作者"),
            ("2026-11-16", "excluded-2", "2026-11-05T00:00:00Z", "排除"),
            ("2026-11-16", "excluded-3", "2026-11-05T00:00:00Z", "排除"),
            ("2026-11-16", "similar", "2026-11-05T00:00:00Z", "保留相似账号"),
            ("2026-11-16", "mention", "2026-11-05T00:00:00Z", "正文提及 @WuBlockchain"),
        ])
        self.raw["username"] = ["WuBlockchain", "alice", " wublockchain ", "WUBLOCKCHAIN", "WuBlockchain12", "alice"]
        before = self.raw.copy(deep=True)
        with self.assertLogs("entrypoints.append_tasks", level="INFO") as logs:
            result = self.run_append(apply=True)
        self.assertEqual(result["excluded_rows"], 3)
        self.assertEqual(result["written_rows"], 3)
        self.assertTrue(any("剔除记录=3" in line for line in logs.output))
        frame = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(set(frame.index.get_level_values("id")), {"same", "similar", "mention"})
        self.assertEqual(frame.xs("same", level="id").iloc[0]["text"], "保留真实作者")
        self.assertEqual(set(frame["user_name"]), {"alice", "WuBlockchain12"})
        self.assertEqual(self.cli.update_by_index.call_args.args[0], "round1-tasks")
        pd.testing.assert_frame_equal(self.raw, before, check_exact=True)

    def test_all_excluded_posts_read_tasks_for_targets_but_do_not_write(self):
        self.add_target_account("WuBlockchain")
        self.raw["username"] = "WuBlockchain"
        result = self.run_append(apply=True)
        self.assertEqual(result["excluded_rows"], len(self.raw))
        self.assertEqual(result["written_rows"], 0)
        self.assertEqual(self.cli.read.call_count, 2)
        self.cli.update_by_index.assert_not_called()

    def test_excludes_literal_text_prefixes_before_validation_without_double_counting_accounts(self):
        self.add_target_account("WuBlockchain")
        self.raw = crawler([
            ("2026-11-16", "same", "invalid", "RT @alice: 转推"),
            ("2026-11-16", "same", "2026-11-05T00:00:00Z", "保留同 ID 原帖"),
            ("2026-11-16", "just-in", "invalid", "JUST IN 最新消息"),
            ("2026-11-16", "account", "invalid", "RT @alice: 账号与前缀同时命中"),
        ] + [("2026-11-16", "keep-{}".format(i), "2026-11-05T00:00:00Z", text)
             for i, text in enumerate(("rt @alice: 小写", "just in 小写", " RT @alice: 前导空格",
                                       "JUST IN", "JUST INFO", "RT@alice", "正文提及 RT @alice",
                                       "JUST IN\n没有空格"))])
        self.raw.iloc[3, self.raw.columns.get_loc("username")] = "WuBlockchain"
        before = self.raw.copy(deep=True)
        with self.assertLogs("entrypoints.append_tasks", level="INFO") as logs:
            result = self.run_append(apply=True)
        self.assertEqual((result["excluded_rows"], result["excluded_prefix_rows"], result["written_rows"]), (1, 2, 9))
        self.assertTrue(any("正文前缀剔除记录=2" in line for line in logs.output))
        self.assertTrue(any("重复采集合并=0" in line for line in logs.output))
        frame = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(set(frame.index.get_level_values("id")), {"same"} | {"keep-{}".format(i) for i in range(8)})
        self.assertEqual(frame.xs("same", level="id").iloc[0]["text"], "保留同 ID 原帖")
        pd.testing.assert_frame_equal(self.raw, before, check_exact=True)

    def test_prefix_filtered_input_does_not_refresh_or_delete_existing_tasks(self):
        for status, text in (("0", "RT @alice: 转推"), ("1", "JUST IN 最新消息")):
            self.existing["status"] = status
            before = self.existing.copy(deep=True)
            self.raw = crawler([("2026-11-16", "shared", "2026-11-04T12:00:00Z", text)])
            self.cli.read.reset_mock()
            with self.subTest(status=status):
                result = self.run_append(apply=True)
                self.assertEqual((result["excluded_prefix_rows"], result["written_rows"], result["written_text_updates"]), (1, 0, 0))
                self.assertEqual(self.cli.read.call_count, 2)
                pd.testing.assert_frame_equal(self.existing, before, check_exact=True)
        self.cli.update_by_column.assert_not_called()
        self.cli.update_by_index.assert_not_called()

    def test_conflict_report_after_filters_keeps_original_source_row_numbers(self):
        self.raw = crawler([
            ("2026-11-16", "ignored", "invalid", "排除"),
            ("2026-11-16", "ignored-prefix", "invalid", "JUST IN 排除"),
            ("2026-11-16", "conflict", "2026-11-05T00:00:00Z", "正文"),
            ("2026-11-16", "conflict", "2026-11-05T00:00:00Z", "正文变化"),
        ])
        self.raw["username"] = ["WuBlockchain", "alice", "alice", "alice"]
        with self.assertRaisesRegex(ValueError, "同一 ID"):
            self.run_append(apply=True)
        report = pd.read_csv(next(self.output.glob("*/crawler_conflicts.csv")), dtype=str)
        self.assertEqual(list(report["source_row"]), ["3", "4"])
        self.assertEqual(set(report["username"]), {"alice"})
        self.cli.update_by_index.assert_not_called()

    def test_same_id_conflicting_publication_text_or_account_stops_before_write(self):
        self.add_target_account("bob")
        base = crawler([("2026-11-16", "new", "2026-11-05T00:00:00Z", "正文")])
        for column, value in (("created_at", "2026-11-06T00:00:00Z"), ("text", "正文变化"),
                              ("username", "bob")):
            changed = base.copy(deep=True)
            changed[column] = value
            self.raw = pd.concat([base, changed])
            with self.subTest(column=column), self.assertRaisesRegex(ValueError, "同一 ID") as caught:
                self.run_append(apply=True)
            self.assertIn("new [{}]".format(column), str(caught.exception))
            report_path = Path(str(caught.exception).split("完整明细=", 1)[1])
            self.assertIn(str(report_path), str(caught.exception))
            report = pd.read_csv(report_path, dtype=str, keep_default_na=False)
            self.assertEqual(list(report["id"]), ["new", "new"])
            self.assertEqual(set(report["conflict_fields"]), {column})
        self.cli.update_by_index.assert_not_called()

    def test_conflict_report_keeps_all_capture_versions_and_exposes_invisible_text_differences(self):
        self.add_target_account("bob")
        self.raw = crawler([
            ("2026-11-16 12:00", "a", "2026-11-05T00:00:00.123456789Z", "正文\n第二行"),
            ("2026-11-16 12:01", "a", "2026-11-05T08:00:00.123456789+08:00", "正文\n第二行 "),
            ("2026-11-16 12:02", "a", "2026-11-05T00:00:00.123456789Z", "正文\n第二行"),
            ("2026-11-16 12:00", "b", "2026-11-06T00:00:00Z", "正文"),
            ("2026-11-16 12:00", "b", "2026-11-06T00:00:00Z", "正文"),
            ("2026-11-16 12:00", "safe", "2026-11-06T00:00:00Z", "无冲突帖子"),
        ])
        self.raw.iloc[4, self.raw.columns.get_loc("username")] = "bob"
        before = self.raw.copy(deep=True)
        with self.assertRaisesRegex(ValueError, "冲突 ID=2，采集记录=5"):
            self.run_append(apply=True)
        report = pd.read_csv(next(self.output.glob("*/crawler_conflicts.csv")), dtype=str, keep_default_na=False)
        self.assertEqual(list(report["id"]), ["a", "a", "a", "b", "b"])
        self.assertEqual(list(report["source_row"]), ["1", "2", "3", "4", "5"])
        self.assertEqual(set(report.loc[report["id"].eq("a"), "conflict_fields"]), {"text"})
        self.assertEqual(set(report.loc[report["id"].eq("b"), "conflict_fields"]), {"username"})
        self.assertEqual(report.iloc[1]["capture_date"], "2026-11-16 12:01:00")
        self.assertEqual(report.iloc[1]["created_at"], "2026-11-05T08:00:00.123456789+08:00")
        self.assertEqual(report.iloc[1]["published_at_utc"], "2026-11-05 00:00:00.123456789")
        self.assertEqual(json.loads(report.iloc[1]["text_repr"]), "正文\n第二行 ")
        self.assertIn("\\n", report.iloc[1]["text_repr"])
        self.assertEqual(json.loads(report.iloc[4]["username_repr"]), "bob")
        self.cli.update_by_index.assert_not_called()
        self.assertEqual(self.cli.read.call_count, 2)
        pd.testing.assert_frame_equal(self.raw, before, check_exact=True)

    def test_existing_task_input_or_publication_conflicts_are_rejected(self):
        self.existing["status"] = "9"
        for published, text in (("2026-11-04T12:00:00Z", "正文变化"),
                                ("2026-11-05T12:00:00Z", "已有任务")):
            self.raw = crawler([("2026-11-16", "shared", published, text)])
            with self.subTest(published=published), self.assertRaisesRegex(ValueError, "不一致|不同 date"):
                self.run_append(apply=True)
        self.cli.update_by_index.assert_not_called()

    def test_task_conflict_report_contains_both_inputs_and_stops_preview_and_apply(self):
        self.existing = tasks(posts([
            ("2026-11-04 01:00", "text-change", "旧正文\n末尾 "),
            ("2026-11-04 02:00", "account-change", "正文一致"),
            ("2026-11-04 03:00", "both-change", "旧正文"),
            ("2026-11-04 04:00", "unchanged", "完全一致"),
        ]))
        self.existing.loc[(pd.Timestamp("2026-11-04 01:00"), "text-change"), "status"] = "9"
        self.add_target_account("bob")
        self.raw = crawler([
            ("2026-11-16", "text-change", "2026-11-04T01:00:00Z", "新正文\n末尾"),
            ("2026-11-16", "account-change", "2026-11-04T02:00:00Z", "正文一致"),
            ("2026-11-16", "both-change", "2026-11-04T03:00:00Z", "新正文"),
            ("2026-11-16", "unchanged", "2026-11-04T04:00:00Z", "完全一致"),
            ("2026-11-16", "new", "2026-11-05T00:00:00Z", "待追加"),
        ])
        self.raw["username"] = ["alice", "alice ", "bob", "alice", "alice"]
        before = self.existing.copy(deep=True)
        for apply in (False, True):
            with self.subTest(apply=apply), self.assertRaisesRegex(ValueError, "冲突帖子=3") as caught:
                self.run_append(apply=apply)
            path = Path(str(caught.exception).split("完整明细=", 1)[1])
            self.assertIn(str(path), str(caught.exception))
            report = pd.read_csv(path, dtype=str, keep_default_na=False).set_index("id")
            self.assertEqual(list(report.index), ["text-change", "account-change", "both-change"])
            self.assertEqual(report.at["text-change", "conflict_fields"], "text")
            self.assertEqual(report.at["account-change", "conflict_fields"], "user_name")
            self.assertEqual(report.at["both-change", "conflict_fields"], "text, user_name")
            self.assertEqual(report.at["text-change", "date"], "2026-11-04 01:00:00")
            self.assertEqual(report.at["text-change", "existing_text"], "旧正文\n末尾 ")
            self.assertEqual(report.at["text-change", "incoming_text"], "新正文\n末尾")
            self.assertEqual(report.at["account-change", "existing_user_name"], "alice")
            self.assertEqual(report.at["account-change", "incoming_user_name"], "alice ")
            self.assertEqual(json.loads(report.at["text-change", "existing_text_repr"]), "旧正文\n末尾 ")
            self.assertIn("\\n", report.at["text-change", "incoming_text_repr"])
            self.assertEqual(json.loads(report.at["account-change", "incoming_user_name_repr"]), "alice ")
            self.assertEqual(set(report["existing_status"]), {"1", "9"})
        self.assertEqual(list(self.output.glob("*/round1_backups")), [])
        self.cli.update_by_index.assert_not_called()
        pd.testing.assert_frame_equal(self.existing, before, check_exact=True)

    def test_refreshes_pending_text_preserves_completed_results_and_repeat_is_noop(self):
        self.existing = tasks(posts([
            ("2026-11-04 01:00:00.123456789", "pending", "待处理旧正文\n末尾 "),
            ("2026-11-04 02:00", "done", "已成功旧正文"),
            ("2026-11-04 03:00", "same", "一致"),
            ("2026-11-04 04:00", "processing", "一致"),
            ("2026-11-04 05:00", "failed", "一致"),
            ("2026-11-03 01:00", "outside", "范围外旧正文"),
        ])).sort_index()
        for post_id, status in (("pending", "0"), ("same", "0"), ("processing", "9"),
                                ("failed", "-1"), ("outside", "0")):
            self.existing.loc[self.existing.index.get_level_values("id") == post_id, "status"] = status
        self.raw = crawler([
            ("2026-11-16", "pending", "2026-11-04T01:00:00.123456789Z", "待处理新正文\n末尾"),
            ("2026-11-16", "done", "2026-11-04T02:00:00Z", "已成功新正文"),
            ("2026-11-16", "same", "2026-11-04T03:00:00Z", "一致"),
            ("2026-11-16", "processing", "2026-11-04T04:00:00Z", "一致"),
            ("2026-11-16", "failed", "2026-11-04T05:00:00Z", "一致"),
            ("2026-11-16", "outside", "2026-11-03T01:00:00Z", "范围外新正文"),
            ("2026-11-16", "new", "2026-11-05T00:00:00Z", "新增任务"),
        ])
        before, raw_before = self.existing.copy(deep=True), self.raw.copy(deep=True)
        preview = self.run_append()
        self.assertEqual((preview["text_updates"], preview["new_tasks"], preview["skipped_completed_texts"]), (1, 1, 1))
        self.assertEqual(preview["written_text_updates"], 0)
        self.cli.update_by_column.assert_not_called()
        self.cli.update_by_index.assert_not_called()
        report = pd.read_csv(Path(preview["report_dir"]) / "round1_text_updates.csv", dtype=str)
        self.assertEqual(list(report["id"]), ["pending"])
        self.assertEqual(list(report["existing_status"]), ["0"])
        self.assertEqual(report.iloc[0]["existing_text"], "待处理旧正文\n末尾 ")
        self.assertEqual(report.iloc[0]["incoming_text"], "待处理新正文\n末尾")

        def update_text(path, frame):
            self.assertEqual(path, "round1-tasks")
            self.assertEqual(list(frame.columns), ["text"])
            self.assertEqual(list(frame.index), [(pd.Timestamp("2026-11-04 01:00:00.123456789"), "pending")])
            self.assertTrue(frame.index.is_monotonic_increasing and frame.index.is_unique)
            backup = next(self.output.glob("*/round1_backups/*/tasks.pkl"))
            pd.testing.assert_frame_equal(pd.read_pickle(backup), before, check_exact=True)
            self.existing.loc[frame.index, frame.columns] = frame.to_numpy()

        def append_new(path, frame):
            self.assertEqual(path, "round1-tasks")
            self.assertEqual(self.cli.update_by_column.call_count, 1)
            self.assertEqual(list(frame.index.get_level_values("id")), ["new"])
            self.existing = pd.concat([self.existing, frame]).sort_index()

        self.cli.update_by_column.side_effect, self.cli.update_by_index.side_effect = update_text, append_new
        result = self.run_append(apply=True)
        self.assertEqual((result["written_text_updates"], result["written_rows"]), (1, 1))
        self.assertEqual(self.existing.xs("pending", level="id").iloc[0]["text"], "待处理新正文\n末尾")
        self.assertEqual(self.existing.xs("done", level="id").iloc[0]["text"], "已成功旧正文")
        self.assertEqual(self.existing.xs("outside", level="id").iloc[0]["text"], "范围外旧正文")
        pd.testing.assert_frame_equal(self.existing.loc[before.index].drop(columns="text"), before.drop(columns="text"), check_exact=True)
        pd.testing.assert_frame_equal(self.raw, raw_before, check_exact=True)
        self.cli.update_by_column.reset_mock()
        self.cli.update_by_index.reset_mock()
        repeated = self.run_append(apply=True)
        self.assertEqual((repeated["written_text_updates"], repeated["written_rows"], repeated["skipped_completed_texts"]), (0, 0, 1))
        self.cli.update_by_column.assert_not_called()
        self.cli.update_by_index.assert_not_called()

    def test_text_only_refresh_backs_up_and_does_not_append_rows(self):
        self.raw = self.raw.iloc[:1]
        self.existing["status"] = "0"
        self.existing["text"] = "旧正文"
        result = self.run_append(apply=True)
        self.assertEqual((result["new_tasks"], result["written_rows"], result["written_text_updates"]), (0, 0, 1))
        self.assertEqual(list(self.cli.update_by_column.call_args.args[1].columns), ["text"])
        self.assertEqual(len(list(self.output.glob("*/round1_backups/*/tasks.pkl"))), 1)
        self.cli.update_by_index.assert_not_called()

    def test_text_refresh_failures_stop_and_append_failure_can_be_retried(self):
        self.existing["status"] = "0"
        self.existing["text"] = "旧正文"
        with patch("entrypoints.append_tasks.pd.DataFrame.to_pickle", side_effect=OSError("backup failed")):
            with self.assertRaises(OSError):
                self.run_append(apply=True)
        self.cli.update_by_column.assert_not_called()
        self.cli.update_by_index.assert_not_called()
        self.cli.update_by_column.side_effect = ConnectionError("text write failed")
        with self.assertRaises(ConnectionError):
            self.run_append(apply=True)
        self.cli.update_by_index.assert_not_called()

        def update_text(path, frame):
            self.existing.loc[frame.index, frame.columns] = frame.to_numpy()

        self.cli.update_by_column.side_effect = update_text
        self.cli.update_by_index.side_effect = ConnectionError("append failed")
        with self.assertRaises(ConnectionError):
            self.run_append(apply=True)
        self.assertEqual(self.existing.iloc[0]["text"], "已有任务")
        self.cli.update_by_column.reset_mock()
        self.cli.update_by_index.side_effect = None
        result = self.run_append(apply=True)
        self.assertEqual((result["written_text_updates"], result["written_rows"]), (0, 1))
        self.cli.update_by_column.assert_not_called()

    def test_changed_text_in_other_states_is_still_reported_without_writing(self):
        for status in ("9", "-1", "unknown"):
            self.existing["status"] = status
            self.existing["text"] = "旧正文"
            with self.subTest(status=status), self.assertRaisesRegex(ValueError, "冲突帖子=1"):
                self.run_append(apply=True)
        self.cli.update_by_column.assert_not_called()
        self.cli.update_by_index.assert_not_called()

    def test_target_accounts_come_from_all_tasks_once_and_filter_before_input_validation(self):
        self.existing = tasks(posts([
            ("2020-01-01", "seed-alice", "历史任务"),
            ("2020-01-02", "seed-alice-duplicate", "另一状态"),
            ("2026-11-16", "seed-bob", "日期范围外任务"),
            ("2020-01-03", "seed-carol", "失败任务"),
            ("2020-01-04", "seed-empty", "空账号"),
        ]))
        self.existing["user_name"] = ["Alice", " ALICE ", "Bob", "Carol", " "]
        self.existing["status"] = ["1", "9", "0", "-1", "1"]
        self.raw = crawler([
            ("2026-11-16", "alice-post", "2026-11-05T00:00:00Z", "保留 Alice"),
            ("2026-11-16", "alice-post", "invalid", None),
            ("2026-11-16", "bob-post", "2026-11-05T00:00:00Z", "保留 Bob"),
            ("2026-11-16", "carol-post", "2026-11-05T00:00:00Z", "保留 Carol"),
            ("2026-11-16", "outside", "invalid", None),
            ("2026-11-16", "rt", "invalid", "RT @alice: 排除"),
        ])
        self.raw["username"] = [" alice ", "outsider", "BOB", "Carol", "unknown", "Alice"]
        before, raw_before = self.existing.copy(deep=True), self.raw.copy(deep=True)
        with self.assertLogs("entrypoints.append_tasks", level="INFO") as logs:
            result = self.run_append(apply=True)
        self.assertEqual((result["target_account_count"], result["excluded_non_target_rows"], result["excluded_prefix_rows"]), (3, 2, 1))
        self.assertEqual(result["written_rows"], 3)
        self.assertTrue(any("非目标账号剔除记录=2" in line for line in logs.output))
        frame = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(set(frame.index.get_level_values("id")), {"alice-post", "bob-post", "carol-post"})
        self.assertEqual(set(frame["user_name"]), {" alice ", "BOB", "Carol"})
        self.assertEqual(self.cli.read.call_args_list[0].args, ("round1-tasks",))
        self.assertEqual(self.cli.read.call_count, 2)
        self.assertEqual(sum(call.args[0] == "round1-tasks" for call in self.cli.read.call_args_list), 1)
        pd.testing.assert_frame_equal(self.existing, before, check_exact=True)
        pd.testing.assert_frame_equal(self.raw, raw_before, check_exact=True)

    def test_empty_target_set_never_falls_back_to_all_crawler_accounts(self):
        for existing in (tasks(posts([])), self.existing.assign(user_name=" ")):
            self.existing = existing
            with self.subTest(rows=len(existing)):
                result = self.run_append(apply=True)
                self.assertEqual(result["target_account_count"], 0)
                self.assertEqual(result["excluded_non_target_rows"], len(self.raw))
                self.assertEqual((result["written_rows"], result["written_text_updates"]), (0, 0))
        self.cli.update_by_column.assert_not_called()
        self.cli.update_by_index.assert_not_called()

    def test_non_target_account_cannot_refresh_existing_pending_text(self):
        self.existing["status"] = "0"
        before = self.existing.copy(deep=True)
        self.raw = crawler([("2026-11-16", "shared", "2026-11-04T12:00:00Z", "非目标账号的新正文")])
        self.raw["username"] = "outsider"
        result = self.run_append(apply=True)
        self.assertEqual((result["excluded_non_target_rows"], result["written_text_updates"]), (1, 0))
        self.cli.update_by_column.assert_not_called()
        self.cli.update_by_index.assert_not_called()
        pd.testing.assert_frame_equal(self.existing, before, check_exact=True)

    def test_equal_task_inputs_with_different_string_dtypes_are_not_conflicts(self):
        self.raw = self.raw.iloc[:1]
        self.existing[["text", "user_name"]] = self.existing[["text", "user_name"]].astype("string")
        result = self.run_append(apply=True)
        self.assertEqual(result["already_in_tasks"], 1)
        self.assertEqual(result["written_rows"], 0)
        self.assertEqual(list(self.output.glob("*/round1_task_conflicts.csv")), [])
        self.cli.update_by_index.assert_not_called()

    def test_integer_ids_are_exact_strings_and_unsafe_ids_are_rejected(self):
        self.raw = crawler([("2026-11-16", 2102052034471965054, "2026-11-05T00:00:00Z", "正文")])
        self.run_append(apply=True)
        frame = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(frame.index.get_level_values("id")[0], "2102052034471965054")
        self.cli.update_by_index.reset_mock()
        for value in (None, True, 2102052034471965054.0, " "):
            self.raw["id"] = value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "id 须"):
                self.run_append(apply=True)
        self.cli.update_by_index.assert_not_called()

    def test_invalid_or_missing_created_at_never_falls_back_to_capture_time(self):
        for value in (None, "", "无效日期", 1793836800, pd.NaT):
            self.raw = crawler([("2026-11-05", "new", value, "正文")])
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "created_at 无效"):
                self.run_append(apply=True)
        self.raw = self.raw.drop(columns=["created_at"])
        with self.assertRaisesRegex(ValueError, "普通列"):
            self.run_append(apply=True)
        self.cli.update_by_index.assert_not_called()

    def test_empty_or_out_of_range_source_does_not_write(self):
        for raw in (crawler([]), self.raw.iloc[-1:]):
            self.raw = raw
            with self.subTest(rows=len(raw)):
                self.assertEqual(self.run_append(apply=True)["written_rows"], 0)
        self.cli.update_by_index.assert_not_called()

    def test_rejects_wrong_index_or_duplicate_columns(self):
        original = self.raw
        for raw in (original.rename_axis("captured_at"), original.set_index("id", append=True),
                    pd.concat([original, original[["id"]]], axis=1)):
            self.raw = raw
            with self.subTest(index=raw.index.names), self.assertRaisesRegex(ValueError, "单层 date"):
                self.run_append(apply=True)
        self.cli.update_by_index.assert_not_called()

    def test_none_dates_read_all_publications(self):
        self.settings.update(start_time=None, end_time=None)
        self.assertEqual(self.run_append()["new_tasks"], 3)

    def test_invalid_ranges_are_rejected_before_reading(self):
        for begin, end in ((None, "2026-11-15"), ("2026-11-04", None), ("", ""),
                           ("2026-11-16", "2026-11-15"), ("2026/11/04", "2026-11-15")):
            self.settings.update(start_time=begin, end_time=end)
            with self.subTest(begin=begin, end=end), self.assertRaises(ValueError):
                self.run_append(apply=True)
        self.cli.read.assert_not_called()

    def test_main_without_plan_directory_selects_crawler_mode_and_uses_round1_lock(self):
        with patch("entrypoints.append_tasks.load_job_config", return_value=self.settings), \
                patch("entrypoints.append_tasks.stage_guard") as guard, \
                patch("entrypoints.append_tasks.logging.basicConfig"), \
                patch("entrypoints.append_tasks.append_tasks_from_source", return_value={"written_rows": 0}) as direct:
            self.assertEqual(main(self.cli, argv=["apply"]), {"written_rows": 0})
        guard.assert_called_once_with("round1", "round1-tasks")
        direct.assert_called_once_with(self.cli, self.settings, apply=True)


if __name__ == "__main__":
    unittest.main()
