import json
import unittest

from common.validation import OutputValidationError, validate_extraction_output


class FirstRoundOutputTests(unittest.TestCase):
    def test_valid_empty_and_populated_events_are_preserved(self):
        for body in (
            {"events": []},
            {"events": [{"event": "事实", "tickers": []}]},
            {"events": [{"event": "  事实  ", "tickers": [" eth ", "ETH", "LEND"]},
                        {"event": "另一事件", "tickers": ["BTC"]}]},
        ):
            with self.subTest(body=body):
                self.assertEqual(validate_extraction_output(json.dumps(body)), body)

    def test_rejects_invalid_root_events_and_fields(self):
        for body in (
            None, [], {}, {"events": None}, {"events": {}}, {"events": "[]"},
            {"events": [], "extra": 1}, {"events": [None]}, {"events": ["事实"]},
            {"events": [{"event": "事实"}]},
            {"events": [{"event": "事实", "tickers": [], "extra": 1}]},
            *({"events": [{"event": event, "tickers": []}]} for event in (None, 1, True, "", " \t\n")),
            *({"events": [{"event": "事实", "tickers": tickers}]}
              for tickers in (None, "ETH", {}, [None], [1], [True], [""], ["  "], ["ETH", None])),
        ):
            with self.subTest(body=body), self.assertRaises(OutputValidationError):
                validate_extraction_output(json.dumps(body))

    def test_rejects_invalid_json_duplicate_keys_and_invalid_text(self):
        for content in (
            "", "not JSON", '```json\n{"events":[]}\n```', '{"events":[]} explanation',
            '{"events":[],"events":[]}',
            '{"events":[{"event":"a","event":"b","tickers":[]}]}',
            '{"events":[{"event":"a","tickers":[],"tickers":[]}]}',
            '{"events":NaN}', '{"events":Infinity}', '{"events":1e999}',
            '{"events":[{"event":"\\ud800","tickers":[]}]}',
            "[" * 2000 + "]" * 2000,
        ):
            with self.subTest(content=content[:60]), self.assertRaises(OutputValidationError):
                validate_extraction_output(content)


if __name__ == "__main__":
    unittest.main()
