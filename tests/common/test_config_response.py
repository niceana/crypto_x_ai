import copy
import json
import os
import unittest
from unittest.mock import patch

from common.config import ConfigError, ai_config_hash, load_job_config
from common.ai_protocol import TaskRenderError, render_payload, parse_response, request_failure
from tests.support import Fixture, success


class ConfigTests(Fixture, unittest.TestCase):
    def test_job_config_rejects_duplicate_root_and_nested_keys(self):
        base = "source_path: source\ntask_path: tasks\nai_config_path: ai.yaml\n"
        cases = ("task_path: other-tasks\n", "task_path: tasks\n",
                 "logging:\n  level: INFO\n  level: DEBUG\n",
                 "logging:\n  level: INFO\n  level: INFO\n")
        for suffix in cases:
            with self.subTest(suffix=suffix):
                self.path.write_text(base + suffix, encoding="utf-8")
                with self.assertRaisesRegex(ConfigError, "重复键"):
                    load_job_config(self.path, ("source_path", "task_path", "ai_config_path"))

    def test_render_is_text_only_and_single_pass(self):
        config = self.config()
        original = copy.deepcopy(config.api.payload)
        text = '汉字\n👀 {{task.text}} ${KEY} "quoted"'
        payload = render_payload(config, text)
        self.assertEqual(payload["messages"][1]["content"], text)
        self.assertEqual(config.api.payload, original)
        payload["messages"][0]["content"] = "changed"
        self.assertEqual(render_payload(config, text)["messages"][0]["content"], "analyse")
        with self.assertRaises(TaskRenderError):
            render_payload(config, 123)
        with self.assertRaises(TaskRenderError):
            render_payload(config, "\ud800")

    def test_hash_excludes_operational_settings(self):
        baseline = ai_config_hash(self.config())
        self.document["api"]["url"] = "https://another.invalid/ai"
        self.document["api"]["headers"] = {"Authorization": "secret"}
        self.document["run"]["max_attempts"] = 5
        self.assertEqual(ai_config_hash(self.config()), baseline)
        self.document["api"]["payload"]["model"] = "another-model"
        self.assertNotEqual(ai_config_hash(self.config()), baseline)

    def test_bad_templates_types_and_unknown_settings(self):
        for variable in ("{{task.user_name}}", "{{task.username}}", "{{task.id}}", "{{task_json}}", "{{task.text", "{{prompts.user}}"):
            with self.subTest(variable=variable):
                self.document["prompts"]["user"] = variable
                with self.assertRaises(ConfigError):
                    self.config()
        self.document["prompts"]["user"] = "{{task.text}}"
        for name, value in (("concurrency", True), ("max_attempts", 0), ("requests_per_second", -1),
                            ("timeout_seconds", float("inf")), ("obsolete", 1)):
            with self.subTest(name=name):
                previous = self.document["run"].copy()
                self.document["run"][name] = value
                with self.assertRaises(ConfigError):
                    self.config()
                self.document["run"] = previous

    def test_duplicate_keys_unsafe_yaml_and_credentials_in_url(self):
        from common.config import load_ai_config
        for raw in ("prompts: {}\nprompts: {}", "!!python/object/apply:os.system ['echo bad']"):
            self.path.write_text(raw, encoding="utf-8")
            with self.assertRaises(ConfigError):
                load_ai_config(self.path)
        self.document["api"]["url"] = "https://secret:password@mock.invalid"
        with self.assertRaises(ConfigError) as caught:
            self.config()
        self.assertNotIn("password@", str(caught.exception))

    def test_dotenv_environment_priority_and_no_prompt_expansion(self):
        (self.path.parent / ".env").write_text("TEST_AI_TOKEN=file-value\n", encoding="utf-8")
        self.document["api"]["headers"] = {"Authorization": "Bearer ${TEST_AI_TOKEN}"}
        self.document["prompts"]["system"] = "${TEST_AI_TOKEN}"
        with patch.dict(os.environ, {"TEST_AI_TOKEN": "process-value"}):
            config = self.config()
            self.assertEqual(config.api.headers["Authorization"], "Bearer process-value")
            self.assertEqual(render_payload(config, "x")["messages"][0]["content"], "${TEST_AI_TOKEN}")
        with patch.dict(os.environ, {"TEST_AI_TOKEN": ""}):
            with self.assertRaises(ConfigError):
                self.config()

    def test_startup_does_not_calculate_hash(self):
        with patch("common.config.ai_config_hash", side_effect=AssertionError("early hash")):
            self.config()

    def test_invalid_host_and_port_fail_at_configuration_loading(self):
        for url in ("https://bad host/ai", "https://mock.invalid:not-a-port/ai", "https://mock.invalid:99999/ai"):
            with self.subTest(url=url):
                self.document["api"]["url"] = url
                with self.assertRaises(ConfigError):
                    self.config()


class ResponseTests(unittest.TestCase):
    def body(self):
        return json.loads(success("  汉字\n ", usage={"total_tokens": 8}, created=1700000000).text)

    def parse(self, body, status=200):
        return parse_response(status, json.dumps(body, ensure_ascii=False))

    def test_success_preserves_text_and_final_metadata(self):
        parsed = self.parse(self.body())
        self.assertTrue(parsed.success)
        self.assertEqual(parsed.ai_response_content, "  汉字\n ")
        self.assertEqual(parsed.usage, {"total_tokens": 8})
        self.assertEqual(parsed.response_created, "2023-11-15 06:13:20")
        self.assertEqual(parsed.error_message, "")

    def test_created_formats_unix_seconds_as_beijing_text(self):
        cases = [
            (1791947560, "2026-10-14 11:12:40"),
            (1791947560.875, "2026-10-14 11:12:40"),
            (0, "1970-01-01 08:00:00"),
            (-1, "1970-01-01 07:59:59"),
            (-0.5, "1970-01-01 07:59:59"),
            (-62135596800, "0001-01-01 08:00:00"),
            (253402271999, "9999-12-31 23:59:59"),
        ]
        for value, expected in cases:
            with self.subTest(created=value):
                body = self.body()
                body["created"] = value
                parsed = self.parse(body)
                self.assertTrue(parsed.success, parsed.error_message)
                self.assertEqual(parsed.response_created, expected)

    def test_missing_or_null_created_does_not_fail_valid_answer(self):
        for missing in (True, False):
            with self.subTest(missing=missing):
                body = self.body()
                if missing:
                    body.pop("created")
                else:
                    body["created"] = None
                parsed = self.parse(body)
                self.assertTrue(parsed.success)
                self.assertEqual(parsed.response_created, "")
                self.assertEqual(parsed.ai_response_content, "  汉字\n ")
                self.assertEqual(parsed.usage, {"total_tokens": 8})

    def test_invalid_created_fails_and_clears_all_ai_results(self):
        for value in (True, False, "1791947560", "", "2026-10-14 11:12:40", [], {},
                      1791947560000, 253402272000, -62135596801, 1e100, -1e100, 10 ** 100):
            with self.subTest(created=value):
                body = self.body()
                body["created"] = value
                body["choices"][0]["message"]["reasoning_content"] = "本次推理"
                parsed = self.parse(body)
                self.assertFalse(parsed.success)
                self.assertEqual(parsed.http_status, 200)
                self.assertEqual(parsed.ai_response_content, "")
                self.assertEqual(parsed.reasoning_content, "")
                self.assertIsNone(parsed.usage)
                self.assertEqual(parsed.response_created, "")
                self.assertTrue(parsed.error_message.startswith("response_created 无法转换："))

    def test_invalid_created_preserves_prior_error_reason(self):
        cases = [(503, "http"), (200, "api"), (200, "length"), (200, "encoding")]
        for status, kind in cases:
            with self.subTest(status=status, kind=kind):
                body = self.body()
                body["created"] = "invalid"
                if kind in ("http", "api"):
                    body["error"] = {"message": "具体错误"}
                    expected = "HTTP 503：具体错误" if kind == "http" else "具体错误"
                elif kind == "length":
                    body["choices"][0]["finish_reason"] = "length"
                    expected = "回答被截断"
                else:
                    body["choices"][0]["message"]["content"] = "\ud800"
                    expected = "ai_response_content 无法编码："
                parsed = self.parse(body, status)
                self.assertFalse(parsed.success)
                self.assertTrue(parsed.error_message.startswith(expected))
                self.assertEqual(parsed.ai_response_content, "")
                self.assertEqual(parsed.reasoning_content, "")
                self.assertIsNone(parsed.usage)
                self.assertEqual(parsed.response_created, "")

    def test_other_failures_keep_current_available_results(self):
        body = self.body()
        body["choices"][0]["finish_reason"] = "length"
        body["choices"][0]["message"]["reasoning_content"] = "本次推理"
        parsed = self.parse(body)
        self.assertFalse(parsed.success)
        self.assertEqual(parsed.error_message, "回答被截断")
        self.assertEqual(parsed.ai_response_content, "  汉字\n ")
        self.assertEqual(parsed.reasoning_content, "本次推理")
        self.assertEqual(parsed.usage, {"total_tokens": 8})
        self.assertEqual(parsed.response_created, "2023-11-15 06:13:20")

    def test_http_errors_preserve_status_and_only_message(self):
        for status in (400, 429, 500, 501, 505, 599):
            parsed = self.parse({"error": {"message": "具体错误", "code": "ignored"}, "usage": 3}, status)
            self.assertFalse(parsed.success)
            self.assertEqual(parsed.http_status, status)
            self.assertEqual(parsed.error_message, "HTTP {}：具体错误".format(status))
            self.assertEqual(parsed.usage, 3)
            self.assertEqual(parse_response(status, "<html>bad</html>").error_message, "HTTP {}".format(status))
        self.assertFalse(self.parse(self.body(), 503).success)

    def test_choice_and_content_contract(self):
        for change in ("missing-index", "boolean-index", "duplicate-index", "wrong-role", "empty", "length", "refusal", "tool"):
            with self.subTest(change=change):
                body = self.body()
                choice = body["choices"][0]
                if change == "missing-index":
                    choice.pop("index")
                elif change == "boolean-index":
                    choice["index"] = False
                elif change == "duplicate-index":
                    body["choices"].append(copy.deepcopy(choice))
                elif change == "wrong-role":
                    choice["message"]["role"] = "user"
                elif change == "empty":
                    choice["message"]["content"] = "  "
                elif change == "length":
                    choice["finish_reason"] = "length"
                elif change == "refusal":
                    choice["message"]["provider_specific_fields"] = {"refusal": "不能回答"}
                else:
                    choice["finish_reason"] = "tool_calls"
                parsed = self.parse(body)
                self.assertFalse(parsed.success)
                self.assertEqual(parsed.http_status, 200)

    def test_json_errors_encoding_and_reason_length(self):
        for raw in ("not-json", "[]", '{"usage": NaN}', '{"usage": 1e999}', "[" * 2000 + "]" * 2000):
            parsed = parse_response(200, raw)
            self.assertFalse(parsed.success)
            self.assertEqual(parsed.http_status, 200)
        body = self.body()
        body["choices"][0]["message"]["content"] = "\ud800"
        parsed = parse_response(200, json.dumps(body))
        self.assertFalse(parsed.success)
        self.assertEqual(parsed.ai_response_content, "")
        self.assertEqual(len(request_failure("x" * 600).error_message), 512)
        parsed = self.parse({"error": {"message": "错误", "code": "not-a-retry-rule"}})
        self.assertFalse(parsed.success)
        self.assertEqual(parsed.error_message, "错误")


if __name__ == "__main__":
    unittest.main()
