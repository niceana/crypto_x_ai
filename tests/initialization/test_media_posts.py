"""媒体本地 JSON 初始化输入：字段映射、时间、精度与跨文件冲突。"""

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from initialization.media_posts import load_media_json_posts


def post(**changes):
    record = {
        "id": "2023554387919122594", "created_at": "Tue Feb 17 00:25:36 +0000 2026",
        "text": "Vitalik Buterin discussed Ethereum.\nhttps://t.co/STgBtbrhRi",
        "user": {"id": "111533746", "screen_name": "WuBlockchain", "name": "Wu Blockchain"},
        "fetched_at": "2026-09-30T12:00:26.403852+00:00", "is_quote_status": True,
        "quote_tweet_id": "2023433964082602009", "favorite_count": 90,
    }
    record.update(changes)
    return record


class MediaJsonPostsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix=".test-media-json-", dir=str(Path.cwd()))
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def file(self, records, name="posts.jsonl"):
        path = self.root / name
        path.write_text("\n".join(json.dumps(item, ensure_ascii=False) for item in records), encoding="utf-8-sig")
        return path

    def test_sample_maps_publication_and_account_and_preserves_text(self):
        record = post()
        source = load_media_json_posts([self.file([record])])
        self.assertEqual(list(source.index.names), ["date", "id"])
        self.assertEqual(source.index[0], (pd.Timestamp("2026-02-17 00:25:36"), record["id"]))
        self.assertIsNone(source.index.get_level_values("date").tz)
        self.assertEqual(source.iloc[0].to_dict(), {"text": record["text"], "user_name": "WuBlockchain"})

    def test_json_formats_bom_blank_lines_and_multiple_files(self):
        records = [post(id=str(number)) for number in range(5)]
        array = self.root / "array.json"
        array.write_text(json.dumps(records[:2], indent=2), encoding="utf-8-sig")
        single = self.root / "single.json"
        single.write_text(json.dumps(records[2], indent=2), encoding="utf-8")
        lines = self.file(records[3:4], "lines.json")
        ndjson = self.file(records[4:], "posts.NDJSON")
        ndjson.write_text("\n\n" + ndjson.read_text(encoding="utf-8-sig") + "\n \n", encoding="utf-8-sig")
        source = load_media_json_posts([array, single, lines, ndjson])
        self.assertEqual(list(source.index.get_level_values("id")), list("01234"))
        # .json 扩展名也允许每行一条帖子。
        self.assertEqual(len(load_media_json_posts([self.file(records, "many.json")])), 5)

    def test_utc_conversion_naive_dates_and_nanoseconds(self):
        records = [post(id="offset", created_at="2026-02-17T08:25:36.123456789+08:00"),
                   post(id="naive", created_at="2026-02-17 00:25:36.123456789")]
        source = load_media_json_posts([self.file(records)])
        self.assertEqual(list(source.index.get_level_values("date")),
                         [pd.Timestamp("2026-02-17 00:25:36.123456789")] * 2)

    def test_big_integer_ids_duplicates_and_prefix_filtering(self):
        first = post(id=2023554387919122594, text="Original news")
        same = post(id="2023554387919122594", text=first["text"], favorite_count=999,
                    fetched_at="2026-11-16T00:00:00Z")
        records = [same, post(id="0007", text="JUST IN news"), post(id="0008", text="RT @alice news"),
                   post(id="0009", text="JUST IN: news"), post(id="0010", text=" JUST IN news"),
                   post(id="0011", text="rt @alice news"), post(id=first["id"], text="RT @alice conflicting text")]
        source = load_media_json_posts([self.file([first]), self.file(records, "other.jsonl")])
        self.assertEqual(list(source.index.get_level_values("id")), ["0010", "0011", "2023554387919122594"])
        self.assertEqual(list(source["text"]), [" JUST IN news", "rt @alice news", "Original news"])
        self.assertTrue(source["user_name"].eq("WuBlockchain").all())

    def test_conflicting_inputs_include_id_fields_and_both_locations(self):
        original = self.file([post()])
        for field, changes in (("date", {"created_at": "2026-02-18"}),
                               ("text", {"text": "different text"}),
                               ("user_name", {"user": {"screen_name": "other"}})):
            with self.subTest(field=field):
                other = self.file([post(**changes)], "other.jsonl")
                with self.assertRaises(ValueError) as captured:
                    load_media_json_posts([original, other])
                message = str(captured.exception)
                for expected in ("2023554387919122594", field, "posts.jsonl 第 1 行", "other.jsonl 第 1 行"):
                    self.assertIn(expected, message)

    def test_invalid_fields_are_rejected_with_location(self):
        records = [[], {"id": "missing"}]
        records += [post(id=value) for value in (None, True, 1.5, " ")]
        records += [post(created_at=value) for value in (None, 123, "not-a-date", "NaT", "")]
        records += [post(user=value) for value in (None, [], {}, {"screen_name": None}, {"screen_name": " "})]
        records += [post(text=value) for value in (None, 123, [])]
        for record in records:
            with self.subTest(record=record):
                with self.assertRaisesRegex(ValueError, "posts.jsonl 第 1 行.*字段不合法"):
                    load_media_json_posts([self.file([record])])

    def test_empty_invalid_json_and_missing_files(self):
        for content in ("", " \n", "[]", json.dumps(post(text="RT @alice news")), json.dumps(post(text="JUST IN"))):
            path = self.root / "empty.json"
            path.write_text(content, encoding="utf-8")
            with self.subTest(content=content), self.assertRaises(ValueError):
                load_media_json_posts([path])
        broken = self.root / "broken.jsonl"
        broken.write_text(json.dumps(post()) + "\n\n{broken", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "broken.jsonl 第 3 行 JSON 语法错误"):
            load_media_json_posts([broken])
        with self.assertRaises(FileNotFoundError):
            load_media_json_posts([self.root / "missing.jsonl"])
        for files in ([], None, "posts.jsonl", [None], [""]):
            with self.subTest(files=files), self.assertRaisesRegex(ValueError, "json_files"):
                load_media_json_posts(files)
