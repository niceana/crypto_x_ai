# Weight 与事件结果诊断

修改 [脚本顶部配置](../entrypoints/plot_signal_diagnostics.py)，然后在项目 Python 环境运行：

```powershell
python -X utf8 -m entrypoints.plot_signal_diagnostics
```

所有数据库访问均为 `cli.read`。不调用 AI、不写数据库、不运行策略、不读取净值、不计算 SMA/EMA。已有 weight 的时间不再加 8 小时。

## 顶部配置

```python
WEIGHT_PATH = "填写实际的完整参数权重表路径"
RESULT_PATH = "填写第二、三轮共用结果表路径"  # 留空仅分析 weight
START_TIME = "2023-01-01"
END_TIME = "2023-12-31"
ASSET_COLUMNS = None  # 可填 194 个可交易资产的完整合约列名列表
WEIGHT_EPSILON = 0.0
TOP_N = 30
```

日期以 weight 表现有时间轴为准，包含首尾日全天。统计只针对表中实际存在的小时；重复/非整点索引、NaN、无穷大报错，缺失小时单独计数，不作为无信号小时补零。首尾范围内完全未返回的数据无法仅凭现有表判断，请核对实际返回的起止时间。

需要事件诊断时，`SCORING_CONFIG` 指向生成该 weight 使用的事件配置；配置有多个评分组合时填写 `PARAM_ID`，单组合可留空。同时配置了多种聚合模式时，填写 `AGGREGATION_METHOD="sum"` 或 `"max_abs"`；相同评分参数在不同模式下共享 `PARAM_ID`。参数标识和模式可通过 `entrypoints.write_weight_data --dry-run` 查看。顶部 `WEIGHT_PATH` 是实际要读的表，事件解释使用所选参数；请保证两者一致。`EVENT_TIME_OFFSET_HOURS=8` 对应当前写库口径。事件对齐使用当前 v3 的 45 分钟窗口；旧版整点分桶表应使用对应旧版诊断代码，仅调整这个偏移不能切换窗口规则。

事件读取会向前扩展日期，按 `(发布时间 + 15分钟) 向下取整 + 1小时 + EVENT_TIME_OFFSET_HOURS` 筛选到目标 weight 日期。因此，2023 年初可能包含上一年末的必要事件。事件范围进一步限制为所选 weight 列对应的资产，去掉约定的完整合约后缀，不按点号猜测币名。

## Weight 输出

默认保存到 `reports/signal_diagnostics/`；同目录重复运行覆盖此前生成的同名报告。

- `weight_overview.png`：四行共享时间轴，分别为每小时正/负/非零资产数及有效资产数、Top 1/Top 3 集中度、绝对分数总和/净分数、Top N 资产带方向的占比热图。
- `weight_all_assets.png`：资产数超过 Top N 时，额外保存全部资产热图。按全期绝对分数总和排序，稀有资产也保留。
- `weight_hourly.csv`：每小时统计与 `dominant_asset`、`dominant_score`，可按 `date` 与现有收益曲线对齐。
- `weight_assets.csv`：每个资产的非零小时数、正/负小时数、主要资产小时数等。
- `weight_signed_share.csv`：全部资产每小时的带方向占比。
- `weight_flat_periods.csv`：连续全零时段的首小时、最后一小时和小时数；遇到缺失小时断开。
- `weight_summary.json`：实际时间范围、缺失小时数、活跃小时占比、最长连续全零时长等。

定义：

```text
非零资产数 = count(abs(weight) > WEIGHT_EPSILON)
占比 p_i = abs(weight_i) / sum(abs(weight))
有效资产数 = 1 / sum(p_i ** 2)
带方向占比 = weight_i / sum(abs(weight))
```

阈值仅影响诊断，不修改输入或交易数据。集中度也在同一个阈值筛选后计算。有效资产数能区分“十个资产均匀贡献”和“一个资产贡献九成”。全零小时的有效资产数为 0，集中度和主要资产留空；并列最大资产按表的列顺序取第一个。这里描述的是信号覆盖和分数集中度，不代表交易所可交易数量、资金仓位或风险敞口。

## 事件结果输出

- `event_distributions.png/csv`：第二轮最终 new/update/repeat 分布、AI 原始判断（保留 uncertain、区分 automatic_new），以及第三轮 direction/materiality/execution_stage/persistence 分布。第三轮同时列出全部披露和 new/update 子集，便于观察 repeat 复制响应的影响。
- `event_zero_cancellation.png`：按日展示零分事件数、完全/部分抵消的资产小时数、零分比例与抵消比例。小时原始统计另存 CSV。
- `event_event_scores.csv`：逐事件枚举、实际得分、目标小时及零分原因。
- `event_zero_causes.csv`：各零分原因，可重叠；例如同一条事件可同时因中性方向和重要性为零而得零分。
- `event_zero_combinations.csv`：互斥的原因组合，计数合计等于零分事件数。
- `event_asset_hours.csv`：同一资产同一目标小时的正分数、负分数绝对值、净分数、抵消比例和完全/部分抵消标记。
- `event_hourly.csv`、`event_summary.json`：小时汇总与评分参数说明。

零分归因依据实际参数映射。若“重要性=不确定”被映射为 2，就不会归因为零；persistence 当前不参与评分，也不归因为零。系数或其他枚举被设为零时会单列原因。

## 抵消口径与内存接口

```text
抵消比例 = 1 - abs(净分数) / (正分数 + 负分数绝对值)
```

抵消先在同一资产、同一小时内计算。不同币或不同小时不互相抵消；日汇总对小时资产净分数先取绝对值再加总。纯零分事件与多空完全抵消单独统计，分母为零时抵消比例留空。

第二轮分布只统计第二轮成功记录；第三轮分布与评分只统计两轮均成功记录。不输出“没有帖子”或“未完成被跳过”的诊断图，不读取向量或原帖。

也可直接使用已经读好的 DataFrame：

```python
from postprocessing.signal_diagnostic_plots import plot_weight_diagnostics
from postprocessing.signal_diagnostics import analyze_weight

report, figures = plot_weight_diagnostics(weight, output_dir="reports/signal_diagnostics")
hourly = report["hourly"]
# 只统计、不绘图：
report = analyze_weight(weight)
```
