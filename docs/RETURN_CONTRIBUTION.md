# 月度资产收益贡献：归一化小时持仓与 close_price_60min

## 版本备注与服务器价格口径

2026-11-10 用户确认：本版本的收益贡献审计使用整点价格，当前默认价格表为 `close_price_60min`；服务器上的收益贡献审计代码使用 VWAP 价格。

后续修改收益贡献审计、价格读取、收益时间对齐或对应测试时，须先提醒用户这一差异。迁移或对账时应核对实际价格口径及收益区间，不能仅因时间索引相同就把整点价格与 VWAP 价格视为等价；本版本保留整点价格计算口径。

## 本次确定的范围

只分析截面归一化后的持仓。先按完整资产列名取 weight 与 price 的交集，将各资产价格 NaN 用此前最近的非空价格前向填充，再按每小时起点的有效报价过滤权重，最后逐小时归一化，与随后一小时价格收益相乘。填充后仍为 NaN、0、负数或无穷的价格视为无有效报价。输出三张月度表和一张资产全期汇总，不增加原值持仓模式，不用月均仓位乘月收益，不对小时贡献进行复利链接调整。

平均持仓使用每小时截面归一化后的仓位，含全零小时。已有时间行中的 NaN 权重按空仓 0 处理，再执行报价筛选和归一化；整行全是 NaN 时也按全零仓位处理。

## 计算与时间对齐

记 `x[i,s]` 为 weight 在时刻 s 的原值，`P[i,s]` 为价格表逐资产按时间前向填充 NaN 后的时点价格：

```text
I = columns(weight) intersect columns(price)
x[i,s] = 0 when the existing weight cell is NaN
I[s] = {i in I: P[i,s] is finite and P[i,s] > 0}
g[s] = sum_{i in I[s]}(abs(x[i,s]))
w[i,s] = x[i,s] / g[s]      当 i in I[s] 且 g[s] > 0
w[i,s] = 0                  其他情况
r[i,s+1h] = P[i,s+1h] / P[i,s] - 1
c[i,s+1h] = w[i,s] * r[i,s+1h]
```

归一化是逐小时、仅在交集内有效资产之间进行，并保留 weight 原列顺序。例如 weight 为 `A=3,B=-1,C=1000`，price 只有 A、B 时，先剔除 C，再得到 `A=75%,B=-25%`；C 不占用归一化分母。若有效资产在某小时全为零，即使被剔除资产有信号，该小时仍保持空仓。

资产列存在但当前价格为 0 等无效值时，仅移除该小时对应的权重，不永久删除该资产；后续起点价格恢复有效后重新参与。例如新币上市前价格为 0，则上市前不占用归一化分母，不能计算 `正价格/0` 或 `0/0`。只有起点价格参与仓位筛选，不会利用未来一小时的结束价重新分配当前仓位。

NaN 价格执行 `prices.ffill()`，仅使用同一资产此前的非空报价；不使用未来价格回填，没有历史值的开头 NaN 仍保留。若填充后已持仓资产仍没有有效结束价，会报错并列出资产、起止时间和处理后价格。未持仓资产的贡献确定为零。

保留负号，活跃小时总绝对敞口为 100%；全零小时计入月均值。正负信号之和可能为零，因此不使用 `sum(weight)` 作为分母。共同放大或缩小所有原始值，不改变归一化结果。没有任何共同资产时停止并报错。

按**收益区间起点 s 的自然月**统计。例如 `1 月 31 日 23:00` 的持仓乘上 `2 月 1 日 00:00 / 1 月 31 日 23:00 - 1`，贡献属于 1 月。这样完整 1 月覆盖 `[1 月 1 日 00:00, 2 月 1 日 00:00)`，资产月收益也对应两个自然月边界价格。

计算对资产标签和时间戳精确匹配，不用“上一条可用记录”代替前一小时，不自动加 8 小时或转换时区。`close_price_60min` 必须保存**索引所标时刻**的价格；如果它用 K 线开盘时刻标记随后一小时的收盘价，需先在数据适配中明确重标。两表索引形式一致本身不能证明价格的实际时刻一致。

假设时刻 s 的仓位已生效，并作用于随后整小时；若回测还包含执行延迟、额外持有期限或小时内调仓，须提供执行后的小时仓位，或沿用同样的持仓转换规则。当前价格贡献不含手续费、滑点、资金费、借币成本或现金利息。

## 表 1：每月资产持仓与收益贡献

文件 `monthly_assets.csv`，每月、每资产一行，保留全部有效资产及零贡献资产；无价格列的资产不进入报告或图表。

| 字段 | 含义 |
| --- | --- |
| `month` / `asset` | YYYY-MM / weight 完整资产列名 |
| `mean_weight` | 月内归一化仓位的有符号平均值，含空仓小时 |
| `mean_absolute_weight` | 月内先取仓位绝对值、再求平均，便于识别多空抵消 |
| `asset_return` | 资产价格收益：处理后区间结束价/起点价减 1；边界价格仍无效时留空 |
| `asset_return_available` | 能否由两个有效边界报价确定区间资产收益；不代表覆盖完整自然月 |
| `return_contribution` | `sum(w * r)`，资产对组合的小时加总收益贡献 |
| `observed_hours` | 本月参与计算的完整小时数 |
| `valid_return_hours` / `missing_return_hours` | 前向填充后该资产有/没有可计算小时收益的价格对数量 |

贡献不复利链接，资产自身的月收益使用处理后的边界价格；完整小时收益连乘与该边界价比值一致。例如同一资产连续两小时各涨 10%，资产收益为 21%；每小时持有 100% 时，本报告的加总贡献为 20 个百分点。填充后边界报价仍无效时月收益留空；缺失小时数字段按填充后的报价计算，填充数量单独记录在 `method.json` 的 `nan_price_cells_filled`。

CSV 保存小数：仓位 `0.45` 显示为 45%，资产收益 `0.20` 显示为 +20%；贡献 `0.03` 表示对组合贡献 +3 个百分点。部分月份的 `asset_return` 仅对应实际审计区间，不能解释成完整月收益。

## 表 2：每月收益贡献排名

文件 `monthly_rankings.csv`。按月、按有符号贡献从高到低排列，保留全部有效资产，标记正贡献前 5 和负贡献前 5；不足 5 个时仅标记实际存在的资产，不拿零贡献补足。

| 字段 | 含义 |
| --- | --- |
| `return_contribution` | 与表 1 相同的小时加总贡献 |
| `positive_rank` / `negative_rank` | 正贡献从大到小 / 负贡献从最负开始排名；不属于该侧则留空 |
| `is_top_positive` / `is_top_negative` | 是否属于该月对应侧前 N 名，N 默认 5 |
| `share_of_net_return` | `C[i,m] / sum_j(C[j,m])`，仅供理解资产对净收益的带符号比例 |
| `share_of_positive_contribution` | 正贡献资产占全部正贡献之和的比例；其他资产留空 |
| `share_of_negative_contribution` | 负贡献绝对值占全部负贡献绝对值之和的比例；其他资产留空 |
| `absolute_contribution_share` | `abs(C[i,m]) / sum_j(abs(C[j,m]))`，观察正负收益来源的集中程度 |

正负贡献的占比分开计算。净收益为负时，盈利资产的净收益占比会为负；亏损资产占比可能为正。净收益绝对值不超过 `net_return_epsilon`（默认 `1e-8`）时，`share_of_net_return` 留空。净收益很小但仍高于阈值时，占比可能超过 100% 或非常大，不截断，也不把它当作主要集中度指标。

并列贡献按 weight 原列顺序稳定排名。全零月无正负排名，占比分母为零时留空。

## 表 3：月度组合收益来源汇总

文件 `monthly_summary.csv`，每月一行。

| 字段 | 含义 |
| --- | --- |
| `portfolio_return_arithmetic` | 所有资产贡献之和，即费用前小时组合收益的加总值 |
| `total_positive_contribution` / `total_negative_contribution` | 正贡献合计 / 带负号的负贡献合计 |
| `largest_positive_asset` / `largest_positive_contribution` | 最大正贡献资产及贡献；不存在时资产留空、贡献为零 |
| `largest_negative_asset` / `largest_negative_contribution` | 最大负贡献资产及贡献；不存在时资产留空、贡献为零 |
| `top1_positive_share` / `top3_positive_share` | 最大 / 前三大正贡献占全部正贡献的比例 |
| `top1_negative_share` | 最大亏损资产占全部亏损绝对值的比例 |
| `top3_absolute_contribution_share` | 绝对贡献最大的三个资产，占所有资产绝对贡献之和的比例 |
| `majority_positive_asset` | 最大正贡献是否超过全部正贡献的 50% |
| `period_start` / `period_end_exclusive` | 实际收益区间，右边界不含 |
| `observed_hours` / `is_full_month` | 小时数 / 是否覆盖完整自然月 |
| `net_return_share_defined` | 本月净收益分母是否超过阈值，可否解释表 2 的净收益占比 |

“前三大资产贡献占比”明确采用 `top3_absolute_contribution_share`，它衡量本月净贡献来源的集中度；`top3_positive_share` 单独回答盈利是否依赖少数币种。这里先对每个资产的小时贡献有符号加总，再取月贡献绝对值；不会把同一资产月内正负小时都当作未抵消的盈利来源。

核对恒等式：表 1 每月贡献之和 = 表 3 的 `portfolio_return_arithmetic` = `total_positive_contribution + total_negative_contribution`。该数值不是复利月净值收益。后续与独立回测对账时，应比较同样执行时点、归一化、费用前和小时加总口径；当前脚本不声称已与外部回测完成对账。

## 表 4：从资产角度看全期表现

文件 `asset_summary.csv`，每个资产一行，直接使用同一次数据库读取的小时仓位、小时贡献和首末价格统计，默认按累计组合收益贡献降序排列。数据库入口一次返回四张表，不读取已有 CSV。

| 字段 | 含义 |
| --- | --- |
| `asset` | 资产名称 |
| `first_month` / `last_month` | 该资产统计覆盖的首末月份 |
| `months` / `observed_hours` | 参与统计的月份数、小时总数 |
| `mean_weight` | 全期小时有符号仓位的平均值 |
| `mean_absolute_weight` | 全期小时绝对仓位的平均值 |
| `total_asset_return` | 资产全期价格回报：审计结束价 / 起点价减 1，使用前向填充处理后的价格 |
| `total_asset_return_available` | 审计首末端点价格是否有效，能否确定全期价格回报 |
| `total_return_contribution` | 全期小时组合收益贡献之和，不复利链接 |
| `positive_months` / `negative_months` | 该资产组合收益贡献为正 / 为负的月份数 |
| `positive_month_share` | 正贡献月数 / 全部统计月数，空仓和零贡献月也在分母中 |
| `max_weight_month` | 月均绝对仓位最大的月份，即重仓月 |
| `max_month_mean_absolute_weight` | 重仓月的平均绝对仓位 |
| `max_month_mean_weight` | 重仓月的有符号平均仓位，可据此辨别多空方向 |
| `max_weight_month_asset_return` | 重仓月资产价格回报 |
| `max_weight_month_return_contribution` | 重仓月对组合的收益贡献 |
| `best_contribution_month` / `best_month_return_contribution` | 月贡献最大的一月及其贡献值 |
| `worst_contribution_month` / `worst_month_return_contribution` | 月贡献最小的一月及其贡献值 |

全期平均仓位直接对本次审计的全部小时仓位求平均，包括空仓小时，等价于各月均值按 `observed_hours` 加权。重仓月按 `mean_absolute_weight` 选取，以同时覆盖多头和空头；它是最大**月均**仓位，不是单个小时的最大仓位。并列取最早月份，从未持仓的资产其重仓月留空。最好、最差贡献月份也按月份最早打破并列。

价格累计回报与策略贡献分开：资产先涨 10%、再跌 10%，价格累计回报是 -1%；策略在两个时段的仓位不同，组合累计贡献由小时持仓决定，仍沿用原来的加总口径。全期价格回报只要求审计首末端点有效：即使某个月边界报价无效导致该月资产回报留空，只要全期首末价格有效，仍可计算全期价格回报。首末端点仍无效时，`total_asset_return` 留空、可用标志为 false。

若起止月份只覆盖部分月份，所有“全期”字段均指本次实际审计区间，不能解释成完整自然月或自然年的表现。四张表统一使用同一次数据库读取和相同审计区间，在内存中完成计算后一起返回和导出。

## 数据完整性与边界

- 索引须唯一、递增、整点；持仓和价格必须具有相同的时区解释。
- 先取两表资产列交集，把已有时间行中的 NaN 权重置为 0，再按当前小时起点价格为有限正数过滤权重，最后归一化。可持仓资产的无穷权重仍会停止。
- 置零的 NaN 单元格数量写入 `method.json` 的 `nan_weight_cells_filled`，不修改源表。全 NaN 行按空仓计入月均持仓和小时数。
- 价格 NaN 在收益计算前按列执行 `ffill()`，使用此前最近的非空价格；不修改源表。填充数量保存在 `nan_price_cells_filled`，规则保存在 `price_nan_policy`。没有历史值的首部 NaN 不会向后取值补齐。
- 价格表可以包含其他资产，但只有与 weight 完整列名相同的列参与计算，不按币名片段猜测映射。无价格列的权重先剔除，记录在 `method.json` 的 `excluded_weight_assets`；有效列保存在 `valid_assets`。
- 共同资产的 0、负数、NaN 或无穷报价会让它在该时点失去可持仓资格；后续价格有效时可以恢复参与。无有效报价的资产小时数写入 `unavailable_entry_price_hours_by_asset`，终端打印合计。
- 指定范围内缺少整个持仓小时或价格时点仍会停止；单元格 NaN 处理不生成缺失时间行。缺少持仓行的报错会列出缺失时点、数量和已有持仓时间范围。价格前向填充仅处理 NaN，0、负数、无穷价格仍按原有无效报价规则处理；填充后已持仓资产仍无法计算结束收益时会停止。
- 脚本顶部必须填写起止日期，也可通过命令行覆盖，包含首尾日。读取 weight 到结束日，读取价格到结束日次日，再只使用次日 `00:00` 作为最后端点。
- Notebook 省略日期且两表索引完全相同时，只能计算到倒数第二条持仓对应的小时；最后一条没有后续价，不参与，记录在 `method.json`。指定完整日期却缺少末端价格时会报错。
- 不足整月的指定范围允许分析，但通过 `is_full_month=false` 明确标记。

## 使用

在已有 `weight` 与价格宽表 `close_price_60min` 的 notebook 中：

```python
from postprocessing.return_contribution import analyze_price_contributions

report = analyze_price_contributions(
    weight, close_price_60min,
    start_time="2023-01-01", end_time="2023-12-31",
    output_dir="reports/return_contribution",
)
assets = report["monthly_assets"]
rankings = report["monthly_rankings"]
top_positive = rankings.loc[rankings["is_top_positive"]]
top_negative = rankings.loc[rankings["is_top_negative"]].sort_values(["month", "negative_rank"])
summary = report["monthly_summary"]
asset_summary = report["asset_summary"]

# 可选：把归一化仓位、资产贡献和来源汇总图保存到项目根目录，无需图形界面。
from postprocessing.return_contribution_plots import save_return_contribution_plots
save_return_contribution_plots(report)
```

从数据库只读审计：先修改 `entrypoints/audit_return_contribution.py` 顶部配置，再直接运行：

```python
WEIGHT_PATH = "完整weight表路径"
PRICE_PATH = "close_price_60min"
START_TIME = "2023-01-01"
END_TIME = "2023-12-31"
```

```powershell
python -X utf8 -m entrypoints.audit_return_contribution
```

顶部还可修改 `TOP_N`、`NET_RETURN_EPSILON`、`OUTPUT_DIR` 和 `FIGURE_DIR`。全部命令行选项继续支持，传入的值优先于顶部配置。三张月度 CSV、`asset_summary.csv` 和 `method.json` 默认保存在 `reports/return_contribution/`，同名文件会覆盖。不调用 AI、不修改数据库。

三张图默认直接保存到**项目根目录**（按脚本位置确定，与启动时工作目录无关）：

| 文件 | 内容 |
| --- | --- |
| `return_contribution_positions.png` | 全部有效资产的月均归一化仓位与月均绝对仓位热图，单位为百分比 |
| `return_contribution_assets.png` | 全部有效资产的月度收益贡献热图，单位为组合收益百分点 |
| `return_contribution_summary.png` | 每月正负贡献、组合小时加总收益和 Top 1/Top 3 集中度 |

`FIGURE_DIR = Path(__file__).resolve().parent.parent` 是默认位置，可在顶部修改或用 `--figure-dir` 临时覆盖。绘图直接使用 Agg 画布，不打开窗口，适合没有桌面的服务器；需要安装 matplotlib。仅调用统计函数时不依赖 matplotlib。

`method.json` 保存原始权重资产、共同资产列、无价格列的剔除资产、NaN 权重处理口径及置零数量、各资产无效起点报价小时数、归一化方法、价格收益公式、时段归属、时区、是否复利链接、费用口径、参数和命令行来源表路径。终端同时打印共同资产数量、剔除名单、NaN 权重置零数量和无效报价合计。低层 `analyze_monthly_contributions` 接口接收已准备好的期初净值权重与完整期末标记小时收益，已有行的 NaN 仓位同样视为 0；主审计入口先取资产交集、将 NaN 权重置零，再过滤无效当前报价，最后做截面归一化。

## 方法参考

持仓归因用持仓序列解释收益来源；若要完全解释期间交易，则需交易层面的归因，见 [CFA Institute：Portfolio Performance Evaluation](https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2026/portfolio-performance-evaluation)。多期收益归因的链接需要单独定义，见 [CFA Institute：Multiperiod Arithmetic Attribution](https://rpc.cfainstitute.org/research/financial-analysts-journal/2004/multiperiod-arithmetic-attribution)。本次按约定只做小时贡献加总。
