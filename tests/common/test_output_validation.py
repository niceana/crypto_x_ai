import json
import unittest

from common.validation import OutputValidationError, validate_extraction_output, validate_relation_output


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


class RelationOutputTests(unittest.TestCase):
    def test_candidate_ids_are_mapped_to_real_event_ids_for_all_relations(self):
        mapping = {"E1": "1234567890123456789:0:ETH", "E2": "9876543210987654321:1:ETH"}
        for relation, candidate_id, information in (("new", None, None), ("uncertain", None, None),
                                                    ("repeat", "E1", None), ("update", "E2", "提案已通过")):
            with self.subTest(relation=relation):
                raw = json.dumps({"event_relation": relation, "matched_candidate_id": candidate_id,
                                  "new_information": information})
                result = validate_relation_output(raw, mapping)
                self.assertEqual(result, {"event_relation": relation, "matched_event_id": mapping.get(candidate_id),
                                          "new_information": information})

    def test_unknown_alias_real_id_and_invalid_field_combinations_are_rejected(self):
        mapping = {"E1": "1234567890123456789:0:ETH"}
        valid = {"event_relation": "update", "matched_candidate_id": "E1", "new_information": "提案已通过"}
        cases = [dict(valid, matched_candidate_id=value)
                 for value in ("E2", "E01", "e1", mapping["E1"], "", 1, True, None, ["E1"])]
        cases.extend(dict(valid, new_information=value) for value in (None, "", " \n", 1))
        cases.extend((None, [], {}, dict(valid, extra=1), dict(valid, event_relation="invalid"),
                      {"event_relation": "update", "matched_event_id": mapping["E1"], "new_information": "提案已通过"},
                      dict(valid, event_relation="repeat"), dict(valid, event_relation="new", new_information=None),
                      dict(valid, event_relation="uncertain", new_information=None)))
        for value in cases:
            with self.subTest(value=value), self.assertRaises(OutputValidationError):
                validate_relation_output(json.dumps(value), mapping)
        for relation in ("new", "uncertain"):
            with self.subTest(relation=relation), self.assertRaises(OutputValidationError):
                validate_relation_output(json.dumps(dict(valid, event_relation=relation, matched_candidate_id=None)), mapping)


if __name__ == "__main__":
    unittest.main()
