"""小时权重的标量评分参数、候选数组校验及稳定的参数表名。"""

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from itertools import product
from string import Formatter
from types import MappingProxyType


# v2 的写库时间轴整体加 8 小时，使用新标识避免与旧时间口径表混写。
SCORING_VERSION = "direction_product_v2"
OUTPUT_TIME_OFFSET_HOURS = 8
FORMULA = "direction * (B * materiality) * (C * execution_stage) * (D * event_relation)"
COEFFICIENTS = ("B", "C", "D")
ACTIVE_MAPPINGS = ("direction", "materiality", "execution_stage", "event_relation")
DEFAULT_MAPPINGS = {
    "direction": {"利好": 1, "利空": -1, "中性": 0, "不确定": 0, "其他": 0},
    "materiality": {"高": 3, "中": 2, "低": 1, "不确定": 0},
    "execution_stage": {"未开始": 1, "执行中": 2, "已完成": 3, "不确定": 0},
    "event_relation": {"new": 2, "update": 1.5, "repeat": 0.5},
    "persistence": {"短期": 1, "中期": 100, "长期": 10000, "不确定": 0},
}
DEFAULT_TABLE_TEMPLATE = "{weight_path}__B{B}_C{C}_D{D}__{param_id}"


def _number(value, label):
    if type(value) not in (int, float):
        raise ValueError("{} 必须为有限数值，不能是布尔值或文本".format(label))
    try:
        number = float(value)
    except (ValueError, OverflowError) as exc:
        raise ValueError("{} 必须为有限数值".format(label)) from exc
    if not math.isfinite(number):
        raise ValueError("{} 必须为有限数值".format(label))
    return 0.0 if number == 0 else number


def _mapping(value, label):
    if not isinstance(value, Mapping):
        raise ValueError("{} 必须为字典".format(label))
    if any(not isinstance(key, str) or not key.strip() for key in value):
        raise ValueError("{} 的键必须为非空字符串".format(label))
    return value


def _reject_unknown(document, allowed, label):
    unknown = set(document) - set(allowed)
    if unknown:
        raise ValueError("{} 包含未知字段：{}".format(label, sorted(unknown)))


@dataclass(frozen=True)
class WeightParameters:
    """一个组合的标量参数；省略项采用默认映射，不含未启用的 persistence。"""

    B: float = 1
    C: float = 1
    D: float = 1
    mappings: object = None

    def __post_init__(self):
        for name in COEFFICIENTS:
            object.__setattr__(self, name, _number(getattr(self, name), name))
        overrides = _mapping({} if self.mappings is None else self.mappings, "mappings")
        _reject_unknown(overrides, ACTIVE_MAPPINGS, "评分参数 mappings")
        normalized = {}
        for field in ACTIVE_MAPPINGS:
            values = dict(DEFAULT_MAPPINGS[field])
            values.update(_mapping(overrides.get(field, {}), "mappings." + field))
            normalized[field] = MappingProxyType({
                key: _number(value, "mappings.{}.{}".format(field, key)) for key, value in values.items()
            })
        object.__setattr__(self, "mappings", MappingProxyType(normalized))

    def to_dict(self):
        return {"scoring_version": SCORING_VERSION,
                "coefficients": {name: getattr(self, name) for name in COEFFICIENTS},
                "mappings": {field: dict(self.mappings[field]) for field in ACTIVE_MAPPINGS}}

    @property
    def parameter_hash(self):
        encoded = json.dumps(self.to_dict(), sort_keys=True, ensure_ascii=False,
                             separators=(",", ":"), allow_nan=False).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @property
    def param_id(self):
        return self.parameter_hash[:16]


@dataclass(frozen=True)
class WeightVariant:
    parameters: WeightParameters
    table_path: str

    def to_dict(self):
        return {"param_id": self.parameters.param_id, "parameter_hash": self.parameters.parameter_hash,
                "table_path": self.table_path, "parameters": self.parameters.to_dict()}


@dataclass(frozen=True)
class WeightGridPlan:
    weight_path: str
    table_name_template: str
    max_combinations: int
    candidates: dict
    variants: tuple

    def to_dict(self):
        return {"scoring_version": SCORING_VERSION, "formula": FORMULA,
                "output_time_offset_hours": OUTPUT_TIME_OFFSET_HOURS,
                "combination_count": len(self.variants), "weight_path": self.weight_path,
                "table_name_template": self.table_name_template, "max_combinations": self.max_combinations,
                "candidates": self.candidates, "inactive_mappings": ["persistence"],
                "variants": [variant.to_dict() for variant in self.variants]}


def _candidates(value, label):
    if not isinstance(value, list) or not value:
        raise ValueError("{} 必须为非空数值数组".format(label))
    values, seen = [], set()
    for item in value:
        number = _number(item, label)
        if number not in seen:
            seen.add(number)
            values.append(number)
    return values


def _validate_template(template):
    if not isinstance(template, str) or not template.strip():
        raise ValueError("table_name_template 必须为非空字符串")
    names = set()
    try:
        for _, name, format_spec, conversion in Formatter().parse(template):
            if name is None:
                continue
            if name not in {"weight_path", "B", "C", "D", "param_id"} or format_spec or conversion:
                raise ValueError("表名模板只支持 weight_path、B、C、D、param_id 简单占位符")
            names.add(name)
    except ValueError as exc:
        raise ValueError("table_name_template 不合法：{}".format(exc)) from exc
    if "param_id" not in names:
        raise ValueError("table_name_template 必须包含 {param_id}")


def _coefficient_text(number):
    return str(int(number)) if number.is_integer() and abs(number) < 1e16 else str(number)


def prepare_weight_grid(weight_path, config=None, *, result_path=None):
    """只从配置展开参数并验证表名，不读取源表、不连接数据库或写本地文件。"""
    if not isinstance(weight_path, str) or not weight_path.strip():
        raise ValueError("请在事件配置中填写 weight_path，作为各参数表的基础路径")
    config = _mapping({} if config is None else config, "weight_grid")
    _reject_unknown(config, ("coefficients", "mappings", "table_name_template", "max_combinations"), "weight_grid")
    raw_coefficients = _mapping(config.get("coefficients", {}), "weight_grid.coefficients")
    _reject_unknown(raw_coefficients, COEFFICIENTS, "weight_grid.coefficients")
    coefficients = {name: _candidates(raw_coefficients.get(name, [1]), "coefficients." + name)
                    for name in COEFFICIENTS}
    raw_mappings = _mapping(config.get("mappings", {}), "weight_grid.mappings")
    _reject_unknown(raw_mappings, DEFAULT_MAPPINGS, "weight_grid.mappings")
    mappings = {}
    for field, defaults in DEFAULT_MAPPINGS.items():
        overrides = _mapping(raw_mappings.get(field, {}), "mappings." + field)
        values = {key: [value] for key, value in defaults.items()}
        values.update(overrides)
        mappings[field] = {key: _candidates(value, "mappings.{}.{}".format(field, key))
                           for key, value in values.items()}
    limit = config.get("max_combinations", 1000)
    if type(limit) is not int or limit <= 0:
        raise ValueError("max_combinations 必须为正整数")
    template = config.get("table_name_template", DEFAULT_TABLE_TEMPLATE)
    _validate_template(template)
    axes = [("coefficient", name) for name in COEFFICIENTS]
    axes.extend((field, key) for field in ACTIVE_MAPPINGS for key in sorted(mappings[field]))
    choices = [coefficients[key] if field == "coefficient" else mappings[field][key] for field, key in axes]
    count = math.prod(len(values) for values in choices)
    if count > limit:
        raise ValueError("参数组合数 {} 超过 max_combinations={}；请缩小候选数组或调高上限".format(count, limit))
    variants, paths = [], set()
    for values in product(*choices):
        scalar_coefficients = {}
        scalar_mappings = {field: {} for field in ACTIVE_MAPPINGS}
        for (field, key), value in zip(axes, values):
            if field == "coefficient":
                scalar_coefficients[key] = value
            else:
                scalar_mappings[field][key] = value
        parameters = WeightParameters(mappings=scalar_mappings, **scalar_coefficients)
        substitutions = {name: _coefficient_text(getattr(parameters, name)) for name in COEFFICIENTS}
        substitutions.update(weight_path=weight_path, param_id=parameters.param_id)
        table_path = template.format(**substitutions)
        if not table_path.strip() or table_path in (weight_path, result_path) or table_path in paths:
            raise ValueError("参数表路径为空、冲突或覆盖来源/无参数基础表：{}".format(table_path))
        paths.add(table_path)
        variants.append(WeightVariant(parameters, table_path))
    return WeightGridPlan(weight_path, template, limit,
                          {"coefficients": coefficients, "mappings": mappings}, tuple(variants))
