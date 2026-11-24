"""复用原配置及提示词，实盘运行参数不回写旧 YAML。"""

from dataclasses import dataclass, field, replace
from pathlib import Path

from . import _compat as core


@dataclass(frozen=True)
class LiveConfig:
    round1_ai: object
    round2_ai: object
    round3_ai: object
    embedding: object
    weight_parameters: object = field(default_factory=core.weight_parameters.WeightParameters)
    user_name_to_tickers: dict = field(default_factory=dict)
    history_days: int = 100
    history_lookback_months: int = 3

    def __post_init__(self):
        for name in ("round1_ai", "round2_ai", "round3_ai"):
            if not isinstance(getattr(self, name), core.config.AIConfig):
                raise ValueError("{} 必须为 AIConfig".format(name))
        if not isinstance(self.embedding, core.config.EmbeddingConfig):
            raise ValueError("embedding 必须为 EmbeddingConfig")
        if not isinstance(self.weight_parameters, core.weight_parameters.WeightParameters):
            raise ValueError("weight_parameters 必须为单组 WeightParameters")
        if self.history_days != 100 or self.history_lookback_months != 3:
            raise ValueError("当前实盘协议固定预加载 100 天，并按三个日历月检索")


def load_live_config(config_directory=None, *, max_attempts=100, weight_param_id=None):
    """从原 YAML 或随包配置加载；max_attempts 包含首次请求，默认 100。"""
    if type(max_attempts) is not int or max_attempts <= 0:
        raise ValueError("max_attempts 必须为正整数")
    if config_directory is None:
        bundled = Path(__file__).resolve().parent / "configs"
        directory = bundled if bundled.is_dir() else Path(__file__).resolve().parent.parent / "configs"
    else:
        directory = Path(config_directory).resolve()
    settings = core.config.load_event_config(directory / "event_processing_config.yaml")
    plan = core.weight_parameters.prepare_weight_grid("live_weight", settings.weight_grid)
    variants = {variant.parameters.param_id: variant.parameters for variant in plan.variants}
    if weight_param_id is not None:
        if weight_param_id not in variants:
            raise ValueError("weight_param_id 不在配置的参数网格中")
        parameters = variants[weight_param_id]
    elif len(variants) == 1:
        parameters = next(iter(variants.values()))
    else:
        raise ValueError("实盘每次只使用一组评分参数；请填写 weight_param_id")

    def load_ai(name):
        ai = core.config.load_ai_config(directory / name)
        return replace(ai, run=replace(ai.run, max_attempts=max_attempts, storage_max_attempts=1))

    embedding = core.config.load_embedding_config(directory / "embedding_config.yaml")
    return LiveConfig(
        round1_ai=load_ai("round1_ai_config.yaml"), round2_ai=load_ai("round2_ai_config.yaml"),
        round3_ai=load_ai("round3_ai_config.yaml"), embedding=replace(embedding, max_attempts=max_attempts),
        weight_parameters=parameters, user_name_to_tickers=settings.user_name_to_tickers,
    )
