"""源码运行复用原模块；发行包使用构建时隔离的同一份业务代码。"""

from importlib import import_module
from importlib.util import find_spec

_prefix = __package__ + "._engine." if find_spec(__package__ + "._engine") is not None else ""


def _module(name):
    return import_module(_prefix + name)


config = _module("common.config")
validation = _module("common.validation")
call_meta = _module("common.call_meta")
network = _module("common.network")
execution = _module("common.execution")
control = _module("common.control")
protocol = _module("common.ai_protocol")
store = _module("common.store")
context = _module("common.event_context")
retrieval = _module("round2.retrieval")
embedding = _module("round2.embedding")
relation_history = _module("round2.history")
vector_storage = _module("vectorization.storage")
time_rules = _module("common.time_rules")
post_tasks = _module("initialization.post_tasks")
event_records = _module("initialization.event_records")
extraction = _module("round1.runner")
relation = _module("round2.runner")
impact = _module("round3.runner")
vectors = _module("vectorization.encoder")
weights = _module("postprocessing.weight_data")
weight_parameters = _module("postprocessing.weight_parameters")
