"""导入已有第一轮回答；只在内存中兼容 event 正文未转义的英文引号。"""

import json
import re

from common.validation import OutputValidationError, validate_extraction_output


_WHITESPACE = re.compile(r"[ \t\r\n]*")
_MEMBER_START = re.compile(r'[ \t\r\n]*"(?:[^"\\]|\\.)*"[ \t\r\n]*:')


class _EventQuoteRepair:
    """按 events/event/tickers 结构扫描，只记录正文引号前的反斜杠插入位置。

    不改写已有字符，也不尝试补字段或结构。正文中疑似字段、容器边界的
    引号按边界处理；后续结构不合法时立即失败，不回退并把字段吞进正文。
    """

    def __init__(self, content):
        self.content = content
        self.position = 0
        self.quote_positions = []
        self.decoder = json.JSONDecoder()

    def _skip_space(self):
        self.position = _WHITESPACE.match(self.content, self.position).end()

    def _take(self, token):
        self._skip_space()
        if not self.content.startswith(token, self.position):
            raise ValueError("字段或事件边界不明确，或 JSON 结构不完整")
        self.position += len(token)

    def _at(self, token):
        self._skip_space()
        return self.content.startswith(token, self.position)

    def _read_value(self):
        self._skip_space()
        value, self.position = self.decoder.raw_decode(self.content, self.position)
        return value

    def _read_event(self):
        self._take('"')
        while self.position < len(self.content):
            char = self.content[self.position]
            if char == "\\":
                # 已有转义原样保留；最终严格校验仍会拒绝非法转义或孤立反斜杠。
                self.position += 2
                continue
            if char == '"':
                following = _WHITESPACE.match(self.content, self.position + 1).end()
                next_char = self.content[following:following + 1]
                if (not next_char or next_char in ",}:][{"
                        or _MEMBER_START.match(self.content, following)):
                    # 逗号或结构字符后的内容也可能是正文，无法确定时不继续猜测。
                    self.position += 1
                    return
                self.quote_positions.append(self.position)
            self.position += 1
        raise ValueError("event 正文或 JSON 结构不完整，不能仅通过引号转义修复")

    def _read_record(self):
        self._take("{")
        fields = set()
        while True:
            name = self._read_value()
            if not isinstance(name, str) or name not in ("event", "tickers"):
                raise ValueError("事件须仅包含 event 和 tickers，或字段边界不明确")
            if name in fields:
                raise ValueError("JSON 字段重复：{}".format(name))
            fields.add(name)
            self._take(":")
            if name == "event":
                self._read_event()
            else:
                tickers = self._read_value()
                if not isinstance(tickers, list) or any(
                        not isinstance(ticker, str) or not ticker.strip() for ticker in tickers):
                    raise ValueError("tickers 须为非空字符串组成的数组，允许空数组")
            if self._at("}"):
                self._take("}")
                break
            self._take(",")
        if fields != {"event", "tickers"}:
            raise ValueError("事件须包含 event 和 tickers")

    def repair(self):
        self._take("{")
        if self._read_value() != "events":
            raise ValueError("第一轮 JSON 必须只包含 events 数组")
        self._take(":")
        self._take("[")
        if not self._at("]"):
            while True:
                self._read_record()
                if self._at("]"):
                    break
                self._take(",")
        self._take("]")
        self._take("}")
        self._skip_space()
        if self.position != len(self.content) or not self.quote_positions:
            raise ValueError("不能仅通过 event 正文引号转义修复")
        parts, start = [], 0
        for position in self.quote_positions:
            parts.extend((self.content[start:position], "\\"))
            start = position
        parts.append(self.content[start:])
        return "".join(parts)


def parse_first_round_answer(content):
    """返回 (已校验对象, 是否修复)，不修改原回答；仅供历史事件导入使用。"""
    try:
        return validate_extraction_output(content), False
    except OutputValidationError as original:
        # 结构、重复字段、非法数值等校验失败不属于引号语法问题，不启用修复。
        if not isinstance(original.__cause__, json.JSONDecodeError):
            raise
        try:
            repaired = _EventQuoteRepair(content).repair()
            return validate_extraction_output(repaired), True
        except (ValueError, TypeError, UnicodeError, RecursionError) as repair_error:
            raise OutputValidationError(
                "{}；event 引号修复失败：{}".format(original, repair_error)
            ) from original
