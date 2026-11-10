# 标签贡献实验：生成仓位表供现有回测使用

入口生成 8 组固定评分对照，以及每个标签内部按等级筛选的事件信号。不读取行情、不计算收益、不执行回测、不调用 AI。输出是**原始信号值**；回测可以直接把它当币数，也可以按自身规则归一化。当前等级分组固定的是每条事件的信号绝对值，尚未完成按价格换算的等金额研究。

## 运行

沿用 `configs/event_processing_config.yaml` 的 `result_path`、`weight_path`、`start_time/end_time` 和 `weight_grid.mappings`。先将路径及源事件日期填写完整。预览不要求日期，也不连接数据库：

```powershell
python -X utf8 -m entrypoints.write_label_experiments --dry-run
python -X utf8 -m entrypoints.write_label_experiments
```

支持 `--config "事件配置文件路径"`。`weight_grid` 有多个候选组合时，先用 `entrypoints.write_weight_data --dry-run` 查出一套参数的标识，再给新入口加 `--param-id 标识`；同时配置了多种聚合模式时，还需加 `--aggregation-method sum` 或 `--aggregation-method max_abs`，因为相同评分参数在不同模式下共享标识。入口不会继续展开所有分数方案。已填写的 B/C/D 只保留在参考参数说明中，实际实验统一 B=C=D=1。

小时聚合沿用所选参考参数的算法。默认 `sum` 相加；选择 `max_abs` 后，各方案每个资产每小时只保留绝对值最大的原信号，保留正负号，并列取最早发布时间、同刻取首条。不同聚合模式在表名中区分，相同评分参数共享 `param_id`。下面的相加与多空抵消示例适用于 `sum`；`max_abs` 下，各等级分组之和也不保证等于方向方案。

`--signal-scale` 是所有方案共用的正数信号倍率，默认 1，旧名 `--unit-position` 继续支持。它不是资金比例：直接按币数下单时，0.01 表示 0.01 个币；若回测采用 `signal / sum(abs(signal))`，共同倍率会被约掉。生成端不归一化，也不默认将 1 解释为 100% 资金。

归一化后的数值之和为 1，不足以说明它代表资金比例。例如归一化结果是 BTC=0.5、另一币=0.5：若直接作为币数，两个币各买 0.5 个，投入金额随价格不同；若作为资金比例，则数量还须计算 `0.5 × 总资金 / 价格`，两者的投入金额才相等。多空信号也须核对系统是否按绝对值之和归一化。

`weight_path` 与 `write_weight_data` 一样表示 root 路径；默认输出 25 张独立的数据库表，实际地址为：

```text
{weight_path}/{aggregation_method}__labels__{方案名}__B{B}_C{C}_D{D}__{param_id}
```

root 末尾已有 `/` 时只保留一个分隔符。例如 root 为 `a`、倍率为 1、模式为 `sum`：

```text
a/sum__labels__DR__B1_C1_D1__{param_id}
a/sum__labels__DRME__B1_C1_D1__{param_id}
a/sum__labels__level_materiality_high__B1_C1_D1__{param_id}
```

`param_id` 和 `parameter_hash` **直接复用 `WeightParameters` 的计算方法**，与 `write_weight_data` 相同：将评分版本、B/C/D、四个有效评分字段的完整映射按规范化 JSON 编码，计算 SHA-256，`param_id` 取前 16 位。root、方案名、等级筛选、聚合模式和日期不参与评分参数标识；聚合模式和方案名由表名独立区分，避免不同分组互相覆盖。

公共倍率折入实际评分参数 B。例如 `--signal-scale 0.01` 时，实际参数为 B=0.01、C=D=1，倍率只应用一次；其评分参数标识与 `write_weight_data` 使用完全相同的系数、映射时一致。配置中的参考 B/C/D 仍不改变实验的基础评分。

等级分组使用相同的方向符号与全 1 映射，因此不同等级通常共享 `param_id`，但方案名及完整表地址不同。清单另保留 `experiment_id/experiment_hash`，它们包括筛选条件及实验元数据，只用于追踪实验，不用于表名后缀。

当前版本为 `label_signals_v4`，采用 45 分钟窗口，清单明确标记 `raw_signal`。评分版本更新使参数表路径与旧窗口区分。旧表不自动迁移或删除；内存调用也须重新生成方案，不能直接把旧版清单传给构建函数。

## 第一类：8 张标签组合仓位表

方向 D 使用选中参数的方向映射。M=重要性，E=执行阶段，R=更新状态。

| 名称 | 事件评分 | 用途 |
| --- | --- | --- |
| D | direction | 只有方向 |
| DR | direction × event_relation | 原策略基准 |
| DM | direction × materiality | 单独启用重要性 |
| DE | direction × execution_stage | 单独启用执行阶段 |
| DRM | direction × event_relation × materiality | 基准加入重要性 |
| DRE | direction × event_relation × execution_stage | 基准加入执行阶段 |
| DME | direction × materiality × execution_stage | 完整方案移除更新状态 |
| DRME | direction × event_relation × materiality × execution_stage | 完整方案 |

启用字段使用所选配置的固定映射；关闭字段把**全部枚举（含“不确定”）设为 1**。公共倍率 `unit_position` 已折入实际参数 B，在事件评分中应用一次。持续时间不进入这 8 个公式。

重点配对：DRM/DR、DRE/DR，以及 DRME/DRE、DRME/DRM、DRME/DME。回测读取 `tables.csv` 中各方案的完整 `table_path`。

## 第二类：17 张等级分组仓位表

每次只筛选一个标签的某个等级，其他等级贡献零；选中事件只使用方向符号乘 `unit_position`，不再乘任何标签分数。

| 字段 | 默认分组 |
| --- | --- |
| direction | 利好、利空（方向映射为零的等级不单独生成表） |
| materiality | 高、中、低、不确定 |
| execution_stage | 未开始、执行中、已完成、不确定 |
| event_relation | new、update、repeat |
| persistence | 短期、中期、长期、不确定 |

即使“重要性=不确定”的评分为 0，该等级内的利好事件仍贡献 `+unit_position`；因为这里检验的是该等级事件本身的表现。方向为零的事件始终不交易。自定义非零方向分数在此只取正负号，保持单事件信号绝对值一致。自定义枚举也会生成分组，数量可能超过 25。Python API 中的 `unit_position` 同样仅表示信号倍率。

例如 `unit_position=0.01` 时：

- 高重要性利好事件给“高”组贡献 `+0.01`，低重要性利空事件给“低”组贡献 `-0.01`。
- 同币同小时两条“高”利好事件相加为 `+0.02`；一条利好与一条利空相抵为 0。
- 没有匹配事件的分组也生成完整零矩阵，不缩短时间或删除资产。

这里统一的是**每条事件的原始信号**。直接作为币数交易时，不同资产每单位价格不同，金额并不相等。不同组事件数也不同，累计收益不能直接代表单事件预测能力；清单提供选中事件数及非零事件数，便于解释回测结果。实际成交成本仍由现有回测按聚合后的持仓计算。

若数量接口需要每条事件固定名义金额 N，应在实际入场价格 P 上换算 `数量 = 方向 × N / P`，然后按相同币种和时点汇总；8 组评分对照则为 `数量 = 事件分数 × N / P`。当前入口没有读取价格，因此不执行这一换算。需要先确认回测的资金、价格、成交时点及归一化公式，再接入对应的价格来源。

持续时间只用来筛选分组，不使用当前配置中的 1/100/10000 映射作金额乘数。

## 共同数据口径与清单

源表读取一次，回答解析一次，仅使用第二、三轮均成功的事件。本条 repeat 作为一条披露参与，沿用本条已保存的响应，不额外去重或继承。分组中出现未配置或缺失的等级时，在创建任何目标表前报错，避免静默遗漏事件。

所有方案沿用现有权重的“整点 T 聚合 `[T-75分钟, T-15分钟)`，再整体 +8 小时”口径、首尾完整窗口补读、资产后缀、连续补零和 float64 类型。它们是小时仓位输入，不在生成端增加持有期限；回测沿用已有的持有及成交规则。

已有目标通过同一套权重续写逻辑更新重叠行、追加新小时，保留范围外数据；首尾窗口完整补读后，边界行也按重新计算值更新。重复运行不累加。运行中途失败会停止，已成功表保留，重跑按目标实际数据继续对齐。

本地默认保存到新的 `reports/label_experiments/北京时间/`，也可用 `--report-dir "尚不存在的新目录"`：

- `manifest.json`：完整参数、参考映射、筛选条件、`param_id/parameter_hash`、实验追踪标识、聚合模式、表名和完整路径、事件数量、非零资产小时数、逐表状态。
- `tables.csv`：全部完成后输出的简明回测输入清单，包含 `name/kind/filter_field/filter_value/param_id/aggregation_method/table_name/table_path/status/selected_events/nonzero_events`。

空组和多空完全抵消组的仓位都可能全零，需结合事件数量区分。

## 已有 notebook 中直接获取仓位矩阵

如果已有事件 DataFrame `results`，可以直接把矩阵传给自己的回测，不必写数据库：

```python
from common.config import load_event_config
from postprocessing.label_experiments import prepare_label_experiments, build_label_positions
from postprocessing.weight_data import parse_weight_features

settings = load_event_config("configs/event_processing_config.yaml")
plan = prepare_label_experiments(settings.weight_path, settings.weight_grid)
features = parse_weight_features(results)
for experiment in plan["variants"]:
    positions = build_label_positions(
        features, experiment, start_time=settings.start_time, end_time=settings.end_time,
    )
    # positions 已是最终时间轴；将此矩阵交给现有回测。
    # backtest(positions)
```

`results` 须已按相同源日期及资产范围读取。生成矩阵逐张交给回测后释放即可，无需同时保留全部矩阵。
