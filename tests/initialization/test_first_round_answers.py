"""验证历史回答只修复正文引号，保留文字和已有转义，不补全错误结构。"""

import json
import unittest

from common.validation import OutputValidationError, validate_extraction_output
from initialization.first_round_answers import parse_first_round_answer


class FirstRoundAnswerTests(unittest.TestCase):
    def test_valid_answers_do_not_need_repair(self):
        for body in (
            {"events": []},
            {"events": [{"event": "机构表态：加密货币“正常化”", "tickers": []}]},
            {"events": [{"event": '  声明："正常化"，路径 C:\\data\n下一行  ', "tickers": [" eth "]}]},
        ):
            with self.subTest(body=body):
                content = json.dumps(body, ensure_ascii=False)
                self.assertEqual(parse_first_round_answer(content), (body, False))

    def test_repairs_user_example_and_preserves_empty_tickers(self):
        content = '{"events":[{"event":"机构表态：加密货币"正常化"","tickers":[]}]}'
        body, repaired = parse_first_round_answer(content)
        self.assertTrue(repaired)
        self.assertEqual(body, {"events": [{"event": '机构表态：加密货币"正常化"', "tickers": []}]})
        # 第一轮新请求继续严格校验，历史导入兼容不能改变该入口。
        with self.assertRaises(OutputValidationError):
            validate_extraction_output(content)

    def test_repairs_multiple_events_and_accepts_both_field_orders(self):
        content = (' { "events" : [ {"event":"声明："一"和"二"。", "tickers":["BTC"]},'
                   '{"tickers":["ETH"], "event":"另一个"事实""} ] } ')
        body, repaired = parse_first_round_answer(content)
        self.assertTrue(repaired)
        self.assertEqual(body, {"events": [
            {"event": '声明："一"和"二"。', "tickers": ["BTC"]},
            {"event": '另一个"事实"', "tickers": ["ETH"]},
        ]})

    def test_preserves_existing_escapes_backslashes_unicode_and_spacing(self):
        content = (r'{"events":[{"event":"  机构"正常化"；路径 C:\\data；保留 \"原引号\"；'
                   r'换行\n下一行；\u4e8b实  ","tickers":["BTC"]}]}')
        body, repaired = parse_first_round_answer(content)
        self.assertTrue(repaired)
        self.assertEqual(body["events"][0]["event"],
                         '  机构"正常化"；路径 C:\\data；保留 "原引号"；换行\n下一行；事实  ')

    def test_semantic_errors_do_not_enter_repair(self):
        for content in (
            '{"events":[{"event":"合法文本","tickers":null}]}',
            '{"events":[],"events":[]}',
            '{"events":[{"event":"合法文本","tickers":[],"other":1}]}',
            '{"events":[{"event":"合法文本"}]}',
            '{"events":NaN}',
        ):
            with self.subTest(content=content), self.assertRaises(OutputValidationError) as caught:
                parse_first_round_answer(content)
            self.assertNotIn("引号修复失败", str(caught.exception))

    def test_does_not_complete_or_hide_invalid_fields_and_structure(self):
        for content in (
            '{"events":[{"event":"机构"正常化"","tickers":[]}',
            '{"events":[{"event":"机构"正常化""}]}',
            '{"events":[{"event":"机构"正常化"","event":"重复字段","tickers":[]}]}',
            '{"events":[{"event":"机构"正常化"","tickers":[],"tickers":[]}]}',
            '{"events":[{"event":"机构"正常化"","other":1,"tickers":[]}]}',
            '{"events":[{"event":"机构"正常化"","tickers":[null]}]}',
            '{"events":[{"event":"机构"正常化"","tickers":[],}]}',
            '{"events":[{"event":"机构"正常化"","tickers":[]}]} extra',
            '{"events":[{"tickers":[],"event":"机构"正常化"" "event":"缺逗号"}]}',
            '{"events":[{"event":"合法文本","tickers":["B"TC"]}]}',
            r'{"events":[{"event":"机构"正常化"\q","tickers":[]}]}',
            r'{"events":[{"event":"机构"正常化"\ud800","tickers":[]}]}',
            '{"events":[{"event":"机构"正常化"\n未转义换行","tickers":[]}]}',
        ):
            with self.subTest(content=content), self.assertRaises(OutputValidationError):
                parse_first_round_answer(content)

    def test_rejects_quotes_with_unclear_field_or_container_boundaries(self):
        for content in (
            '{"events":[{"event":"引述"a", "b"后文","tickers":[]}]}',
            '{"events":[{"event":"引述"a"}后文","tickers":[]}]}',
            '{"events":[{"event":"引述"a": 后文","tickers":[]}]}',
            '{"events":[{"event":"引述"a" "tickers": 后文","tickers":[]}]}',
        ):
            with self.subTest(content=content), self.assertRaises(OutputValidationError):
                parse_first_round_answer(content)


if __name__ == "__main__":
    unittest.main()
