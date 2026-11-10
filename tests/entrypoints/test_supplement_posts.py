"""本地 JSON 补录原始帖子表：只提交新索引，失败前不写入。"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from entrypoints.supplement_posts import apply_plan, load_json_posts, main, preview_json_posts


def post(post_id="new", date="2026-09-01 12:00:00", text="正文", user="alice"):
    return {"id": post_id, "created_at": date, "text": text,
            "user": {"id": "867100084248469505", "screen_name": user},
            "retweet_count": 34, "favorite_count": 246}


SOURCE_COLUMNS = ["user_id", "user_name", "text", "created_at", "retweet_count", "like_count"]


def existing(rows=None):
    rows = [("2026-08-01", "old", "历史正文", "alice")] if rows is None else rows
    return pd.DataFrame({"user_id": ["867100084248469505"] * len(rows),
                         "user_name": [row[3] for row in rows], "text": [row[2] for row in rows],
                         "created_at": [row[0] for row in rows],
                         "retweet_count": ["0"] * len(rows), "like_count": ["0"] * len(rows)},
                        index=pd.MultiIndex.from_arrays([
                            pd.DatetimeIndex([row[0] for row in rows]), [row[1] for row in rows]],
                            names=["date", "id"]))


class ImportJsonPostsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cli = Mock(spec=["read", "update_by_index"])
        self.source = existing()
        self.cli.read.return_value = self.source

    def file(self, records, name="posts.jsonl"):
        path = self.root / name
        path.write_text("\n".join(json.dumps(record, ensure_ascii=False) for record in records), encoding="utf-8-sig")
        return path

    def preview(self, files, **kwargs):
        return preview_json_posts(self.cli, "original", files, report_dir=self.root / "plans", **kwargs)

    def run_files(self, files, *, dry_run=False, **kwargs):
        summary = self.preview(files, **kwargs)
        self.cli.update_by_index.assert_not_called()
        if not dry_run:
            result = apply_plan(self.cli, summary["plan_dir"])
            summary["written_rows"] = result["written_rows"]
        return summary

    def run_import(self, records, **kwargs):
        return self.run_files([self.file(records)], **kwargs)

    def test_multiple_jsonl_files_with_crawler_schema_and_twitter_dates(self):
        path = self.root / "crawler.JSONL"
        record = post(2102052034471965054, "Mon Sep 21 15:07:13 +0000 2026", text="原文\n第二行")
        record["fetched_at"] = "2026-11-05T08:47:25.579597+00:00"
        path.write_text("\n" + json.dumps(record, ensure_ascii=False) + "\n\n", encoding="utf-8-sig")
        other = self.file([post("other", "2026-09-22T00:00:00Z")])
        result = self.run_files([path, other])
        self.assertEqual(result["json_rows"], 2)
        self.assertEqual(result["written_rows"], 2)
        batch = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(batch.index[0], (pd.Timestamp("2026-09-21 15:07:13"), "2102052034471965054"))
        self.assertEqual(batch.iloc[0]["text"], "原文\n第二行")
        self.assertEqual(batch.iloc[0]["user_name"], "alice")
        self.assertEqual(list(batch.columns), SOURCE_COLUMNS)
        self.assertEqual(batch.iloc[0]["user_id"], "867100084248469505")
        self.assertEqual(batch.iloc[0]["created_at"], "Mon Sep 21 15:07:13 +0000 2026")
        self.assertEqual(batch.iloc[0]["retweet_count"], "34")
        self.assertEqual(batch.iloc[0]["like_count"], "246")
        self.assertIsInstance(batch.index.get_level_values("date"), pd.DatetimeIndex)
        self.assertTrue(all(isinstance(value, str) for value in batch.index.get_level_values("id")))
        self.assertTrue(all(isinstance(value, str) for value in batch.to_numpy().flat))
        self.assertNotIn("favorite_count", batch.columns)
        self.assertNotIn("fetched_at", batch.columns)

    def test_user_ids_preserve_integer_precision_and_reject_invalid_values(self):
        record = post()
        record["user"]["id"] = 1999999999999999999
        self.run_import([record])
        batch = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(batch.iloc[0]["user_id"], "1999999999999999999")
        self.cli.reset_mock()
        for value in (None, True, 1.5, " "):
            record["user"]["id"] = value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "user_id"):
                self.run_import([record])
        self.assertEqual(self.cli.read.call_count, 4)
        self.cli.update_by_index.assert_not_called()

    def test_jsonl_syntax_and_field_errors_include_real_line_number_after_backup(self):
        for bad_line, message in (("{broken", "JSON 语法错误"), ("[]", "JSON 对象"),
                                  ('{"id":"missing"}', "字段不合法")):
            with self.subTest(bad_line=bad_line):
                path = self.root / "bad.jsonl"
                path.write_text(json.dumps(post()) + "\n\n\n" + bad_line, encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "bad.jsonl 第 4 行.*" + message):
                    self.preview([path])
        self.assertEqual(self.cli.read.call_count, 3)
        self.cli.update_by_index.assert_not_called()

    def test_empty_jsonl_file(self):
        path = self.root / "empty.jsonl"
        path.write_text("\n \n", encoding="utf-8-sig")
        result = self.run_files([path])
        self.assertEqual(result["json_rows"], 0)
        self.cli.update_by_index.assert_not_called()

    def test_combines_files_preserves_big_integer_ids_and_nanoseconds(self):
        paths = [self.file([post(1999999999999999999, "2026-09-01 12:00:00.123456789")]),
                 self.file([post("0007", "2026-09-02 00:00:00")], "more.jsonl")]
        before = self.source.copy(deep=True)
        result = self.run_files(paths)
        self.assertEqual(result["written_rows"], 2)
        self.assertEqual([item[0] for item in self.cli.mock_calls], ["read", "read", "update_by_index"])
        self.assertEqual(self.cli.read.call_count, 2)
        target, batch = self.cli.update_by_index.call_args.args
        self.assertEqual(target, "original")
        self.assertEqual(list(batch.index.names), ["date", "id"])
        self.assertEqual(list(batch.columns), SOURCE_COLUMNS)
        self.assertEqual(list(batch.index.get_level_values("id")), ["1999999999999999999", "0007"])
        self.assertEqual(batch.index[0][0].nanosecond, 789)
        self.assertEqual(batch.iloc[0]["text"], "正文")
        pd.testing.assert_frame_equal(self.source, before)

    def test_nested_fields_and_extra_target_columns(self):
        self.source["language"] = "en"
        self.cli.read.return_value = self.source[["user_name", "language", "text"]]
        path = self.file([{"post": post(), "user": {"screen_name": "alice"}, "lang": "zh"}])
        fields = {"date": "post.created_at", "id": "post.id", "text": "post.text",
                  "user_name": "user.screen_name", "language": "lang"}
        result = self.run_files([path], field_map=fields)
        self.assertEqual(result["written_rows"], 1)
        batch = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(list(batch.columns), ["user_name", "language", "text"])
        self.assertEqual(batch.iloc[0]["language"], "zh")

    def test_skips_existing_deduplicates_json_and_repeat_run_is_noop(self):
        records = [post("old", "2026-08-01", "JSON 中不同正文也不覆盖旧帖"), post(), post()]
        result = self.run_import(records)
        self.assertEqual(result["duplicate_rows_removed"], 1)
        self.assertEqual(result["existing_rows_skipped"], 1)
        self.assertEqual(result["written_rows"], 1)
        batch = self.cli.update_by_index.call_args.args[1]
        self.cli.read.return_value = pd.concat([self.source, batch])
        self.cli.update_by_index.reset_mock()
        result = self.run_import(records)
        self.assertEqual(result["existing_rows_skipped"], 2)
        self.assertEqual(result["new_rows"], 0)
        self.cli.update_by_index.assert_not_called()

    def test_imports_all_dates_accounts_and_text_prefixes_without_filtering(self):
        result = self.run_import([
            post("early", "2026-08-31 23:59:59.999999999"),
            post("start", "2026-09-01 00:00:00", user=" @ALICE "),
            post("end", "2026-09-02 23:59:59.999999999", text="普通正文包含 RT @x"),
            post("late", "2026-09-03 00:00:00"),
            post("other", user="bob"), post("retweet", text="  rt @x 原文"),
            post("news", text="\nJust In: 消息"), post("reply", text="@x 回复")])
        self.assertEqual(result["json_rows"], 8)
        self.assertEqual(result["written_rows"], 8)
        batch = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(set(batch.index.get_level_values("id")),
                         {"early", "start", "end", "late", "other", "retweet", "news", "reply"})
        self.assertEqual(batch.loc[(pd.Timestamp("2026-09-01"), "start"), "user_name"], " @ALICE ")

    def test_relative_jsonl_path_is_resolved_from_project_root(self):
        path = self.root / "_ALL_new.jsonl"
        path.write_text(json.dumps(post()) + "\n", encoding="utf-8")
        with patch("entrypoints.supplement_posts.PROJECT_ROOT", self.root):
            result = self.preview(["_ALL_new.jsonl"])
        self.assertEqual(result["new_rows"], 1)
        plan = json.loads((Path(result["plan_dir"]) / "plan.json").read_text(encoding="utf-8"))
        self.assertEqual(plan["json_files"], [str(path.resolve())])
        self.assertNotIn("filters", plan)
        self.cli.update_by_index.assert_not_called()

    def test_dry_run_never_submits(self):
        result = self.run_import([post()], dry_run=True)
        self.assertEqual(result["new_rows"], 1)
        self.assertEqual(result["written_rows"], 0)
        self.cli.update_by_index.assert_not_called()

    def test_empty_files_do_not_submit(self):
        result = self.run_import([])
        self.assertEqual(result["new_rows"], 0)
        self.cli.update_by_index.assert_not_called()

    def test_duplicate_payload_conflicts_stop_before_write(self):
        for records in ([post(), post(text="不同正文")],
                        [post(), post(date="2026-09-02")],
                        [post("old", "2026-09-01")]):
            with self.subTest(records=records), self.assertRaisesRegex(ValueError, "冲突|不同 date|date 不同"):
                self.run_import(records)
        self.cli.update_by_index.assert_not_called()

    def test_missing_target_columns_stop_before_write(self):
        self.source["language"] = "en"
        with self.assertRaisesRegex(ValueError, "缺少=.*language"):
            self.run_import([post()])
        self.cli.update_by_index.assert_not_called()

    def test_malformed_records_stop_after_backup_before_any_database_write(self):
        invalid = [post(None), post(1.5), post(True), post(text=None), post(user=" "),
                   post(date=None), post(date=1735689600), post(date="NaT"),
                   {"id": "missing-fields"}, ["not an object"], {"data": []}]
        for record in invalid:
            document = [record]
            with self.subTest(document=document), self.assertRaises(ValueError):
                self.run_import(document)
        self.assertEqual(self.cli.read.call_count, len(invalid))
        self.cli.update_by_index.assert_not_called()

    def test_valid_file_then_corrupt_file_keeps_original_backup(self):
        valid = self.file([post()])
        broken = self.root / "broken.jsonl"
        broken.write_text("{broken", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "broken.jsonl 第 1 行 JSON 语法错误"):
            self.preview([valid, broken])
        self.cli.read.assert_called_once_with("original", start_time="", end_time="")
        backups = list((self.root / "plans").glob("*/original.pkl"))
        self.assertEqual(len(backups), 1)
        pd.testing.assert_frame_equal(pd.read_pickle(backups[0]), self.source)
        self.cli.update_by_index.assert_not_called()

    def test_json_timezone_markers_removed_without_changing_clock_or_precision(self):
        result = self.run_import([post("utc", "2026-09-01T12:00:00Z"),
                                  post("offset", "2026-09-01T12:00:00.123456789+08:00"),
                                  post("naive", "2026-09-01 12:00:00")])
        self.assertEqual(result["written_rows"], 3)
        batch = self.cli.update_by_index.call_args.args[1]
        dates = batch.index.get_level_values("date")
        self.assertIsNone(dates.tz)
        self.assertEqual(list(dates.hour), [12, 12, 12])
        self.assertEqual(batch.loc[(pd.Timestamp("2026-09-01 12:00:00.123456789"), "offset"), "created_at"],
                         "2026-09-01T12:00:00.123456789+08:00")

    def test_naive_original_compares_with_json_publication_clock(self):
        self.cli.read.return_value = existing([("2026-09-01 08:00:00", "shared", "旧文", "alice")])
        result = self.run_import([post("shared", "2026-09-01T08:00:00+08:00"),
                                  post("new", "2026-09-02T00:00:00Z")])
        self.assertEqual(result["existing_rows_skipped"], 1)
        self.assertEqual(self.cli.update_by_index.call_args.args[1].index[0][0].hour, 0)

    def test_unexpected_timezone_in_original_stops_after_backup(self):
        self.cli.read.return_value = existing([("2026-09-01T08:00:00+08:00", "old", "旧文", "alice")])
        with self.assertRaisesRegex(ValueError, "原表 date 应为无时区时间"):
            self.preview([self.file([post()])])
        self.cli.update_by_index.assert_not_called()
        self.assertEqual(len(list((self.root / "plans").glob("*/original.pkl"))), 1)

    def test_redundant_index_columns_follow_new_index(self):
        self.source["date"] = self.source.index.get_level_values("date")
        self.source["id"] = self.source.index.get_level_values("id")
        self.run_import([post()])
        batch = self.cli.update_by_index.call_args.args[1]
        self.assertEqual(batch.iloc[0]["date"], batch.index[0][0])
        self.assertEqual(batch.iloc[0]["id"], batch.index[0][1])
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(batch["date"]))
        self.assertTrue(all(isinstance(value, str) for value in batch.drop(columns="date").to_numpy().flat))

    def test_invalid_configuration_never_writes(self):
        path = self.file([post()])
        for kwargs in ({"field_map": {"date": "created_at"}},):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.preview([path], **kwargs)
        self.assertEqual(self.cli.read.call_count, 1)
        self.cli.update_by_index.assert_not_called()

    def test_read_and_append_errors_propagate_without_other_mutations(self):
        self.cli.read.side_effect = ConnectionError("read offline")
        with self.assertRaises(ConnectionError):
            self.run_import([post()])
        self.cli.update_by_index.assert_not_called()
        self.cli.read.side_effect = None
        self.cli.update_by_index.side_effect = ConnectionError("append offline")
        with self.assertRaises(ConnectionError):
            self.run_import([post()])
        self.cli.update_by_index.assert_called_once()

    def test_loader_needs_no_client_and_main_passes_configuration(self):
        path = self.file([post()])
        frame = load_json_posts([path])
        self.assertEqual(frame.index[0][1], "new")
        with patch.multiple("entrypoints.supplement_posts", SOURCE_PATH="original", JSON_FILES=[path],
                            REPORT_DIR=self.root / "plans"), \
                patch("entrypoints.supplement_posts.logging.basicConfig"):
            result = main(self.cli, argv=[])
        self.assertEqual(result["written_rows"], 0)
        self.assertEqual(result["new_rows"], 1)
        self.cli.update_by_index.assert_not_called()

    def test_preview_backs_up_first_and_exports_overlap_and_new_rows(self):
        self.source = existing([("2026-08-01", "older", "更早的历史帖", "alice"),
                                ("2026-09-01 12:00:00", "shared", "原表正文", "alice"),
                                ("2026-09-03", "original-only", "原表独有", "alice")])
        self.cli.read.return_value = self.source
        path = self.file([post("shared", text="JSON 新正文"), post("0007", "2026-09-02"),
                          post("0007", "2026-09-02")])
        def check_backup_before_loading(*args, **kwargs):
            backups = list((self.root / "plans").glob("*/original.pkl"))
            self.assertEqual(len(backups), 1)
            pd.testing.assert_frame_equal(pd.read_pickle(backups[0]), self.source, check_exact=True)
            return load_json_posts(*args, **kwargs)
        with patch("entrypoints.supplement_posts.load_json_posts", side_effect=check_backup_before_loading):
            summary = self.preview([path])
        self.cli.update_by_index.assert_not_called()
        self.assertEqual(summary["original_rows"], 3)
        self.assertEqual(summary["overlap_rows"], 1)
        self.assertEqual(summary["overlap_changed_rows"], 1)
        self.assertEqual(summary["original_only_rows"], 2)
        self.assertEqual(summary["new_rows"], 1)
        self.assertEqual(summary["duplicate_rows_removed"], 1)
        output = Path(summary["plan_dir"])
        self.assertEqual({path.name for path in output.iterdir()}, {
            "original.pkl", "new_posts.pkl", "incoming_posts.csv", "new_posts.csv", "overlap_comparison.csv",
            "original_only.csv", "id_date_conflicts.csv", "plan.json"})
        new_rows = pd.read_csv(output / "new_posts.csv", dtype=str, keep_default_na=False)
        self.assertEqual(list(new_rows["id"]), ["0007"])
        self.assertEqual(new_rows.iloc[0]["like_count"], "246")
        overlap = pd.read_csv(output / "overlap_comparison.csv", dtype=str, keep_default_na=False)
        self.assertEqual(set(overlap["source"]), {"original", "incoming"})
        self.assertEqual(set(overlap["text"]), {"原表正文", "JSON 新正文"})
        old_only = pd.read_csv(output / "original_only.csv", dtype=str, keep_default_na=False)
        self.assertEqual(list(old_only["id"]), ["older", "original-only"])
        self.assertEqual(len(pd.read_csv(output / "id_date_conflicts.csv")), 0)

    def test_apply_command_uses_frozen_batch_and_does_not_reload_json_or_configuration(self):
        path = self.file([post()])
        summary = self.preview([path])
        output = Path(summary["plan_dir"])
        expected = pd.read_pickle(output / "new_posts.pkl")
        path.write_text("{broken after preview", encoding="utf-8")
        with patch.multiple("entrypoints.supplement_posts", SOURCE_PATH="different-table", JSON_FILES=[],
                            JSON_FIELD_MAP={}), patch("entrypoints.supplement_posts.logging.basicConfig"):
            result = main(self.cli, argv=["apply", str(output)])
        self.assertEqual(result["written_rows"], 1)
        self.assertEqual(self.cli.update_by_index.call_args.args[0], "original")
        pd.testing.assert_frame_equal(self.cli.update_by_index.call_args.args[1], expected, check_exact=True)
        self.assertTrue((output / "apply_result.json").exists())
        before_calls = len(self.cli.mock_calls)
        with self.assertRaisesRegex(ValueError, "已执行 apply"):
            apply_plan(self.cli, output)
        self.assertEqual(len(self.cli.mock_calls), before_calls)

    def test_apply_refuses_source_changes_since_preview(self):
        added = pd.concat([self.source, existing([("2026-08-02", "concurrent", "另一程序新增", "alice")])])
        changed = self.source.copy()
        changed.iloc[0, changed.columns.get_loc("text")] = "旧帖被改动"
        changed_dtype = self.source.copy()
        changed_dtype["like_count"] = 0
        for modified in (added, changed, changed_dtype, self.source.iloc[:0]):
            with self.subTest(modified_rows=len(modified)):
                self.cli.read.return_value = self.source
                summary = self.preview([self.file([post()])])
                self.cli.read.return_value = modified
                with self.assertRaisesRegex(ValueError, "preview 后已变化"):
                    apply_plan(self.cli, summary["plan_dir"])
                self.assertFalse((Path(summary["plan_dir"]) / "apply_result.json").exists())
        self.cli.update_by_index.assert_not_called()

    def test_apply_checks_backup_and_batch_checksums_before_database_access(self):
        for filename in ("original.pkl", "new_posts.pkl"):
            with self.subTest(filename=filename):
                summary = self.preview([self.file([post()])])
                output = Path(summary["plan_dir"])
                path = output / filename
                path.write_bytes(path.read_bytes() + b"changed")
                self.cli.reset_mock()
                with self.assertRaisesRegex(ValueError, "与预览时不一致"):
                    apply_plan(self.cli, output)
                self.cli.read.assert_not_called()
                self.cli.update_by_index.assert_not_called()

    def test_preview_lists_id_date_conflicts_and_apply_is_blocked(self):
        summary = self.preview([self.file([post("old", "2026-09-01")])])
        self.assertFalse(summary["can_apply"])
        self.assertEqual(summary["id_date_conflicts"], 1)
        output = Path(summary["plan_dir"])
        conflicts = pd.read_csv(output / "id_date_conflicts.csv", dtype=str, keep_default_na=False)
        self.assertEqual(set(conflicts["source"]), {"original", "incoming"})
        self.assertEqual(conflicts["date"].nunique(), 2)
        self.cli.reset_mock()
        with self.assertRaisesRegex(ValueError, "冲突"):
            apply_plan(self.cli, output)
        self.cli.read.assert_not_called()
        self.cli.update_by_index.assert_not_called()

    def test_backup_failure_stops_before_json_processing(self):
        with patch("entrypoints.supplement_posts.pd.DataFrame.to_pickle", side_effect=OSError("disk full")), \
                patch("entrypoints.supplement_posts.load_json_posts") as loader:
            with self.assertRaises(OSError):
                self.preview([self.file([post()])])
        loader.assert_not_called()
        self.cli.update_by_index.assert_not_called()
        self.assertEqual(list((self.root / "plans").glob("*/plan.json")), [])

    def test_apply_accepts_identical_original_in_different_row_order(self):
        self.source = pd.concat([self.source, existing([("2026-08-02", "second", "第二条旧帖", "alice")])])
        self.cli.read.return_value = self.source
        summary = self.preview([self.file([post()])])
        self.cli.read.return_value = self.source.iloc[::-1]
        result = apply_plan(self.cli, summary["plan_dir"])
        self.assertEqual(result["written_rows"], 1)


if __name__ == "__main__":
    unittest.main()
