"""验证第一轮账号背景的输入边界，不请求网络或数据库。"""

import json
import unittest

from common.event_context import build_extraction_input, prepare_account_tickers


class ExtractionInputTests(unittest.TestCase):
    def test_account_context_uses_case_insensitive_accounts_and_canonical_symbols(self):
        mapping = {"chainlink": [" link ", "LINK", "ETH"]}
        prepared = prepare_account_tickers(mapping)
        body = json.loads(build_extraction_input(
            {"user_name": "ChAiNlInK", "text": '正文 {{task.text}} 与 "引号"'}, prepared))
        self.assertEqual(body, {"user_name": "ChAiNlInK", "account_tickers": ["LINK", "ETH"],
                                "text": '正文 {{task.text}} 与 "引号"'})
        self.assertEqual(mapping, {"chainlink": [" link ", "LINK", "ETH"]})

    def test_missing_context_is_omitted_without_guessing_or_loose_account_matching(self):
        prepared = prepare_account_tickers({"chainlink": ["LINK"]})
        for account in ("public-news", "@chainlink", " chainlink", "chainlink "):
            with self.subTest(account=account):
                self.assertEqual(json.loads(build_extraction_input(
                    {"user_name": account, "text": "post"}, prepared)),
                    {"user_name": account, "text": "post"})
        self.assertEqual(prepare_account_tickers(None), {})

    def test_duplicate_or_invalid_account_context_is_rejected(self):
        for mapping in ({"a": ["ETH"], "A": ["BTC"]}, {"a": []}, {"a": [None]}, []):
            with self.subTest(mapping=mapping), self.assertRaises(ValueError):
                prepare_account_tickers(mapping)
