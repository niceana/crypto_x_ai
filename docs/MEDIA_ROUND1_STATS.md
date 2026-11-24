# media_round1 月度任务量与逐日回看分布

在项目根目录、能够导入 `xdata.client` 的服务器 Python 环境运行，把路径替换成实际 media_round1 表路径：

```bash
python -X utf8 -m entrypoints.stat_media_round1 --source-path "media_round1的完整xdata路径"
```

默认从 `read_meta` 获取全表起止日，再逐月只读取 `status`、`ai_response_content` 两列。也可以指定日期范围，包含首尾日全天；首尾月只统计指定部分。指定起始日时，额外补读最长回看天数的前史（最早不早于源表起始日），供逐日窗口使用；前史不进入月度数量和 `totals`：

```bash
python -X utf8 -m entrypoints.stat_media_round1 --source-path "media_round1的完整xdata路径" --start-time 2026-01-01 --end-time 2026-10-31
```

程序在终端显示月度数量与总计，并将报告保存到 `reports/media_round1_stats_<北京时间戳>/`。可通过 `--output-dir` 指定尚不存在的目录。整个过程只读 xdata，不调用 AI。

统计按原帖 `date` 的自然月，不转换时区。媒体 JSON 初始化后的 `date` 是 UTC 无时区发布时间，因此该来源按 UTC 月份归属。仅统计 `status` 为字符串 `"1"` 且回答可解析的帖子，沿用现有事件导入的 JSON 校验与 event 正文引号修复规则。

**`round2_records` 是主要结果：每条 event × 每个影响对象计一条。** 单条事件内 ticker 去首尾空白、转大写并去重，包含 `INDUSTRY`、`MACRO`；空 ticker 数组不生成任务。例如一条事件包含 BTC、ETH，计 2 条。跨帖子或不同事件内容相同仍分别计数。

这是第一轮 AI 结果能展开的任务总量，不是 round2 归组后的独立事项数。程序不读取现有 round2 表、不按导入表尾截断、不按第二轮资产名单筛选，因此也不等于下一次导入实际新增的行数或待执行的 AI 调用数。未成功帖子不推算最终产出。

报告包含：

| 文件 | 内容 |
| --- | --- |
| `monthly_counts.csv` | 每月帖子、事件和 round2 任务数量；无数据的中间月份保留为 0 |
| `invalid_answers.csv` | 成功状态但无法解析的回答所对应的 date、id 和错误原因；scope 区分 report 和 prehistory |
| `daily_ticker_history.csv` | 每天每个 ticker、每种回看天数的历史事件记录数 |
| `ticker_history_distribution.csv` | 每个 ticker 在不同回看天数下的数量分布 |
| `history_distribution.csv` | 所有 ticker-day 的总体数量分布，以及具体资产、INDUSTRY、MACRO 分布 |
| `summary.json` | 表路径、统计范围、各列合计、前史数量、回看分布及统计口径 |

月度 CSV 的数量列：

| 列 | 含义 |
| --- | --- |
| `total_posts` / `successful_posts` | 本范围帖子总数 / 第一轮成功状态数（包含非法回答） |
| `pending_posts` / `processing_posts` / `failed_posts` | 状态 0 / 9 / -1 的帖子数 |
| `unknown_status_posts` | 其他状态值的帖子数；不计入事件和任务数 |
| `invalid_answers` / `repaired_answers` | 成功回答中无法解析 / 经现有引号修复后解析成功的帖子数 |
| `posts_without_events` | 合法成功回答中 events 为空的帖子数 |
| `extracted_events` | 合法成功回答中的原始 event 数，含空 tickers 事件 |
| `events_without_tickers` | tickers 为空、不能生成 round2 任务的 event 数 |
| `round2_records` | 可展开的 round2 任务总数 |
| `round2_asset_records` | 其中具体资产任务数 |
| `round2_industry_records` / `round2_macro_records` | 其中 INDUSTRY / MACRO 任务数 |

三类任务数之和等于 `round2_records`。存在未成功帖子或非法回答时，终端会提示统计未覆盖这些帖子的最终产出。按月读取期间若第一轮仍在写入，结果反映各月被读取时的状态。

## 逐日回看分布与选参

原命令现在默认同时统计 **30 天**窗口。比较多个窗口时运行：

```bash
python -X utf8 -m entrypoints.stat_media_round1 --source-path "media_round1的完整xdata路径" --lookback-days 7 14 30 60
```

每天 D 取日初 0 点的 `[D-N天, D)` 区间：包含下界，不包含当天。例如 2026-02-01 的 30 天窗口是 `[2026-01-02 00:00:00, 2026-02-01 00:00:00)`。沿用原 date 日期，不转换时区。事件数量为第一轮可展开的同 ticker 事件记录数，跨帖重复披露仍计数；并非第二轮归组后的独立事项数。

`daily_ticker_history.csv` 含以下字段：

| 字段 | 含义 |
| --- | --- |
| `date` / `ticker` / `lookback_days` | 采样日期、规范化 ticker、固定回看天数 |
| `window_start` / `window_end` | 包含的起始日期 / 不包含的结束日期 |
| `today_events` | 当天这个 ticker 的事件记录数，不计入历史窗口 |
| `history_events` | 过去 N 个完整日期的同 ticker 事件记录数 |
| `has_full_history` | 窗口起始日是否不早于源表最早日期；只反映时间跨度，不保证第一轮已全部成功 |

每个 ticker 从本次读取范围内首次事件日开始采样，到统计结束日与源表尾日的较早者结束；之后没有事件的日期也输出零值，不在首次出现前填入大量零值。明细逐 ticker 写出，按 ticker、回看天数、日期排列，不在内存中构造整个日期 × ticker 明细表。

两份分布 CSV 同时提供：

- `sample=all_days`：所有采样日期，包含当天没有新事件的日期。
- `sample=active_days`：仅当天有新事件的日期，更适合评估需要运行第二轮时的历史量。
- `sample_days`、`zero_days`、`mean`、`min`、`p50`、`p75`、`p90`、`p95`、`p99`、`max`。
- `partial_window_days`：因源表起始时间过晚、回看跨度不足而排除的样本数；这些行仍保留在逐日明细中。

分位数仅使用 `has_full_history=True` 的样本。P95 按排序后第 `ceil(0.95*N)` 位取整数：至少 95% 的样本不超过此事件量；样本数为零时分位数留空，不用 0 代替。其他分位数同理。

总体表按 ticker-day 等权，每个有新事件的日期只计一次，不按当天事件数加权。`scope=ASSETS` 排除 INDUSTRY/MACRO，两个大桶单列；`scope=ALL` 包含全部对象。先比较 `active_days` 下各窗口的 P95/P99 和最大值，再检查单个 ticker 与两个大桶，避免总体分位数掩盖热点对象。可以结合输入容量决定要进一步评估的窗口。

当前第二轮实际使用 `history_lookback_months`，按**日历月**回看；一个日历月不恒等于 30 天。此脚本只做固定天数窗口对比，不改第二轮参数。日初切片不包含当天此前事件，与逐条 AI 请求的精确时刻窗口有差异；第一轮记录量也不等于归组后的候选组数、实际发送的历史条数或 token 数。回看跨度还需结合归组质量选择，不能仅由数量分位数确定。
