# 从已有回放结果统计资产差异

full_diff=82 代表 82 个 47 分时点至少有一个资产不一致，不代表只有 82 条资产值，也不代表有 82 个不同资产。

本脚本读取已保存的对账 CSV，默认仅统计窗口内有事件的“资产 × 时点”，排除没有事件而补零的资产。它只依赖 pandas 和 numpy，可单独复制到服务器；读取已有文件，不调用 AI、向量、业务函数或数据库，不需要重跑、重新导出数据或安装 wheel。

## 使用

修改 summarize_diffs.py 顶部：

```python
RUN_DIR = "/你的试跑目录/outputs/replay_full_那次启动时间"
COMPARISON = "full"
SCOPE = "with_events"
DATA_DIR = Path("/当时导出的完整数据目录/data_240h")
OUTPUT_DIR = None
PRINT_TOP = 30
```

RUN_DIR 填那次打印 full_diff 的完整回放输出目录，不是 data_240h，也不是单个 batch 子目录。DATA_DIR 填当时导出的原数据目录；默认是脚本同级的 data_240h/。新统计写入 RUN_DIR/diff_stats_full_with_events/，原来的 diff_stats_full/ 和回放结果保留。

执行：

```bash
/opt/ap/python/anaconda3/bin/python summarize_diffs.py
```

这条命令使用与实盘一致的解释器，读取已有文件、汇总并打印结果。无需修改原导出脚本或回放配置。第一次使用更新后的脚本，只需替换 summarize_diffs.py，再填写顶部的 RUN_DIR、DATA_DIR。

## 有事件口径

以每小时 47 分为对账时点，从新旧两边找出该窗口内有二、三轮成功事件的资产，取并集，每个“资产 × 时点”只计一次。这里的“有帖子”以提取出的目标资产事件为依据；没有提取出该资产事件的原帖不构成该资产的候选。

例如 16:47 对应 UTC+8 的 [15:45, 16:45)，与旧表 17:00 行对齐。新旧事件索引是 UTC 发布时间，统计时按同一窗口换算，不改原文件时间。

- 有事件但因为中性、repeat 等评分规则得到 0，仍计入分母。
- 只有旧侧有事件、或只有新侧有事件，都计入，避免遗漏新增/消失信号。
- 多个帖子或事件涉及同一资产，仍只计一个资产时点；max_abs 的分母也是如此。
- 新旧两边都没有事件的资产时点排除。不能简单用 weight != 0 代替“有事件”。

```text
差异率 = 有事件资产时点中最终 weight 不同的条数
         ÷ 新旧任一侧有事件的资产时点总数 × 100%
```

旧侧读取 DATA_DIR/reference_events.pkl；full 模式的新侧从已成功批次的 event_results.pkl 按调用顺序合并，逐时点还原，避免使用未来批次的结果。weight_only 模式新旧评分都使用旧事件，因此两侧事件范围相同。

脚本检查 RUN_DIR 与 DATA_DIR 的 dataset_id、旧事件文件校验和、已完成对账时点之前的全部成功批次。缺少旧基准或中途批次文件时明确报错，不能把文件缺失当作无事件。若两边无事件却有最终 weight 差异，会单独输出 excluded_diff_details.csv 并提示核对。

## 保留原全资产口径

设置 SCOPE="all"，仍统计全部资产时点，输出到 diff_stats_full/，不需要 DATA_DIR 或事件文件。这个分母包含新旧都没有事件的零值。程序调用 summarize(...) 的 scope 默认保持 all；直接执行脚本则使用顶部默认的 with_events。

## 统计口径

- differing_asset_values：当前口径下不一致的“时点 × 资产”记录条数。
- unique_differing_assets：这些记录涉及多少个不同资产。同一资产在 10 个时点不同，算 10 条差异值、1 个不同资产。
- differing_hours：当前口径下有差异的时点数量。
- compared_asset_values：当前口径的分母。with_events 下只包含新旧任一侧有事件的资产时点。
- diff_rate_pct：当前口径的百分比；分母为 0 时保存 null，表示无可比资产时点。
- all_compared_asset_values / all_differing_asset_values / all_diff_rate_pct：with_events 下额外保留原全资产口径，方便对照。
- excluded_without_events：新旧都没有已完成事件、因此排除的资产时点数。
- compared_hours：有成功对账文件的小时数；hours_with_events 是其中至少有一个有事件资产的小时数。

优先使用 CSV 中已经保存的 equal 列，不把浮点容差内的非零差值误判为 DIFF。少数旧文件没有 equal 时，沿用原脚本 rtol=1e-9、atol=1e-12 的判断，NaN/Inf 仍算不一致。旧表缺列补零后的对账值和 old_column_present 标记原样保留。

仅纳入有成功完成标记的批次。若文件数量少于预期小时数，或明细统计的差异小时数与原 summary.json 不符，终端和新 summary.json 会提示，不能当作整段测试全部完成。

## 输出

默认 diff_stats_full_with_events/ 下：

| 文件 | 内容 |
| --- | --- |
| summary.json | 当前口径的分母、差异条数/比例，以及全资产口径、排除数量和完整性提示。 |
| asset_diff_summary.csv | 全部资产的比较次数、差异次数、差异比例、差异值的最大/平均绝对值；按差异次数降序。 |
| compared_values.csv | 纳入分母的所有资产时点，含相同和不同的记录；有事件口径另含 old_event_rows、new_event_rows，方便核对分母。 |
| diff_details.csv | 每一条不同的记录，包括时点、资产、新旧值、差值、原批次目录，方便继续定位。 |
| excluded_diff_details.csv | 有事件口径额外保存：新旧都无事件却有最终数值差异的记录，正常应为空。 |

终端只展示差异次数最多的前 PRINT_TOP 个资产，CSV 不截断。

如需单独统计旧事件重算 weight 的差异，把顶部 COMPARISON 改为 "weight_only"。有事件口径输出到 diff_stats_weight_only_with_events/，全资产口径输出到 diff_stats_weight_only/。

原 hourly_summary.csv 的 full_different_assets 列求和对应全资产口径的差异条数。有事件口径会重新筛选分母和分子；若所有原差异都落在有事件范围内，差异条数不变，百分比会因分母缩小而提高。
