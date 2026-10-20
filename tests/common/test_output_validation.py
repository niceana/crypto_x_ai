import json
import re
import unittest
from pathlib import Path

import yaml

from common.validation import (
    IMPACT_FIELDS, IMPACT_ENUMS,
    OutputValidationError, validate_extraction_output, validate_relation_output, validate_impact_output,
)


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


class ImpactOutputTests(unittest.TestCase):
    def setUp(self):
        self.valid = {"direction": "利好", "materiality": "不确定", "execution_stage": "未开始",
                      "persistence": "短期", "materiality_reason": "  已批准回购，未披露执行及相对规模。\n"}

    def test_accepts_all_allowed_values_without_rewriting_text(self):
        choices = {
            "direction": ("利好", "利空", "中性", "不确定"),
            "materiality": ("高", "中", "低", "不确定"),
            "execution_stage": ("未开始", "执行中", "已完成", "不确定"),
            "persistence": ("短期", "中期", "长期", "不确定"),
        }
        for field, allowed in choices.items():
            for value in allowed:
                body = dict(self.valid, **{field: value})
                with self.subTest(field=field, value=value):
                    self.assertEqual(validate_impact_output(json.dumps(body, ensure_ascii=False)), body)
                    self.assertEqual(validate_impact_output(body), body)
        uncertain = {field: "不确定" for field in choices}
        uncertain["materiality_reason"] = "未披露代币关联、规模、阶段及机制。"
        self.assertEqual(validate_impact_output(json.dumps(uncertain)), uncertain)

    def test_schema_and_allowed_values_match_current_prompt(self):
        path = Path(__file__).resolve().parents[2] / "configs" / "round3_ai_config.yaml"
        prompt = yaml.safe_load(path.read_text(encoding="utf-8"))["prompts"]["system"]
        output_spec = prompt.split("# 输出要求", 1)[1]
        declared = dict(re.findall(r"^- `([^`]+)`：(.+?)。$", output_spec, flags=re.MULTILINE))
        self.assertEqual(set(declared), set(self.valid))
        self.assertEqual(set(IMPACT_FIELDS), set(declared))
        self.assertEqual(set(IMPACT_ENUMS), set(declared) - {"materiality_reason"})
        for field, values in IMPACT_ENUMS.items():
            with self.subTest(field=field):
                self.assertEqual(values, tuple(declared[field].split("、")))

    def test_storage_envelope_is_distinct_from_validated_content(self):
        saved = {"ai_response_content": json.dumps(self.valid, ensure_ascii=False),
                 "usage": {"total_tokens": 10}, "response_created": "2026-10-20 12:00:00",
                 "attempts": 1, "created": "2026-10-20 12:00:01"}
        self.assertEqual(validate_impact_output(saved["ai_response_content"]), self.valid)
        with self.assertRaises(OutputValidationError) as caught:
            validate_impact_output(json.dumps(saved, ensure_ascii=False))
        message = str(caught.exception)
        self.assertIn("缺少=['direction', 'materiality', 'execution_stage', 'persistence', 'materiality_reason']", message)
        self.assertIn("多出=['ai_response_content', 'usage', 'response_created', 'attempts', 'created']", message)

    def test_wrong_root_and_field_mismatch_report_actual_structure(self):
        for body in ([], None, json.dumps(self.valid)):
            with self.subTest(body=body), self.assertRaisesRegex(OutputValidationError, "实际类型="):
                validate_impact_output(json.dumps(body))
        body = dict(self.valid)
        body["reason"] = body.pop("materiality_reason")
        with self.assertRaises(OutputValidationError) as caught:
            validate_impact_output(json.dumps(body))
        self.assertIn("缺少=['materiality_reason']；多出=['reason']", str(caught.exception))

    def test_rejects_noncanonical_values_and_identifies_field(self):
        invalid = {
            "direction": ("看涨", "positive", "利好、利空", "高", "混合"),
            "materiality": ("中等", "high", "3", "利好"),
            "execution_stage": ("高", "计划或提案", "已批准", "已发生", "确定", "approved"),
            "persistence": ("一次性", "持续性", "结构性", "持续", "structural"),
        }
        for field, values in invalid.items():
            for value in values + (" " + self.valid[field], self.valid[field] + "\n"):
                body = dict(self.valid, **{field: value})
                with self.subTest(field=field, value=value), self.assertRaisesRegex(OutputValidationError, field):
                    validate_impact_output(json.dumps(body))

    def test_legacy_certainty_field_is_rejected_without_renaming_content(self):
        body = dict(self.valid)
        body["certainty"] = body.pop("execution_stage")
        with self.assertRaises(OutputValidationError) as caught:
            validate_impact_output(json.dumps(body))
        self.assertIn("缺少=['execution_stage']；多出=['certainty']", str(caught.exception))
        self.assertNotIn("execution_stage", body)

    def test_rejects_invalid_root_missing_extra_and_nonstring_fields(self):
        cases = [None, [], {}, dict(self.valid, extra="额外字段")]
        for field in self.valid:
            missing = dict(self.valid)
            del missing[field]
            cases.append(missing)
            cases.extend(dict(self.valid, **{field: value})
                         for value in (None, 0, True, [], {}, "", " \t\n"))
        for body in cases:
            with self.subTest(body=body), self.assertRaises(OutputValidationError):
                validate_impact_output(json.dumps(body))

    def test_rejects_invalid_json_duplicate_fields_and_invalid_encoding(self):
        raw = json.dumps(self.valid, ensure_ascii=False)
        invalid = ["", "not JSON", "```json\n" + raw + "\n```", raw + " 额外解释",
                   raw + raw, "[" * 2000 + "]" * 2000]
        invalid.extend(raw[:-1] + ',"{}":{}}}'.format(field, json.dumps(value, ensure_ascii=False))
                       for field, value in self.valid.items())
        invalid.extend(raw.replace('"不确定"', value, 1) for value in ("NaN", "Infinity", "1e999"))
        invalid.append(json.dumps(dict(self.valid, materiality_reason="\ud800")))
        for content in invalid:
            with self.subTest(content=content[:80]), self.assertRaises(OutputValidationError):
                validate_impact_output(content)


if __name__ == "__main__":
    unittest.main()
