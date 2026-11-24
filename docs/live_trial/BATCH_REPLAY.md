# 连续 240 小时回放

这组脚本复用 0.4.3 业务包，不修改三轮 AI、向量或 weight 算法。只读 xdata 并写本地测试文件，不更新 Redis/xdata，不下单。使用实盘解释器 /opt/ap/python/anaconda3/bin/python；从 0.2.0 升级后新处理行会保存调用统计及续写原因，旧业务数据仍兼容。回放固定模拟时钟，正式运行的 weight.date 会在最后重新取得。续跑只跳过已成功批次，失败批次从头重算；切换业务包版本后应新建回放输出目录，不复用旧版本进度。

## 输入路径用哪个表

优先把 export_xdata.py 顶部的 POSTS_PATH 填成旧第一轮任务表路径（旧配置 task_path）。只读取其 text、user_name 和 (date,id) 发布时间索引，忽略旧的 AI 回答与状态，回放时仍重新执行第一轮。

旧原始帖子表也可以，前提是索引同样为 (date,id)，date 为 UTC 原帖发布时间，并有字符串 text/user_name。不要直接使用 date 已是爬虫时间的新表；历史表不含真实爬虫时间时，脚本按每批截止刻度生成模拟抓取 date，并从原 date 生成 created_at。

EVENTS_PATH、OLD_WEIGHT_PATH、ASSETS_PATH、METHOD、WEIGHT_PARAM_ID 同样沿用你已经配置好的 export_xdata.py，不需要重新填写到新脚本。

## 第一步：导出 240 小时数据

修改 export_hours.py 顶部：

```python
FIRST_TIME = settings.TEST_TIME  # 第一个 47 分对账时刻，默认沿用 export_xdata 的 TEST_TIME
HOURS = 240
OUTPUT_DIR = ROOT / "data_240h"
```

也可把 FIRST_TIME 直接改为字符串，例如 "2021-03-20 00:47:00"（UTC+8）。它是第一个对账时刻，不是最后一个；脚本测试该时点起连续 240 个小时。应选择这 10 天旧事件、旧 weight 已全部完成且起点之前有足够历史的区间。

在试跑目录执行：

```bash
/opt/ap/python/anaconda3/bin/python export_hours.py
```

这条命令使用指定实盘 Python，一次从三个表和资产对象读取数据，不会请求 AI。按日读取后在内存做精确时间筛选，空帖子批次或空事件小时合法。旧 weight 必须有对应的 240 个整点，缺少资产列则按旧交集规则处理，不要求列齐全。

导出目录包含：

- history_seed.pkl：首批之前的初始 B，最多 100 天，保留原向量及二三轮结果。
- posts.pkl：回放覆盖的 240 小时原帖内容，仍按 UTC 发布时间索引存储。
- reference_events.pkl：这段时间的旧事件结果，仅用于独立的 weight 对账。
- old_weight.pkl、assets.json：旧 weight 对应行及完整指定名单。
- manifest.json：调度时间、方法、数据 ID、文件摘要和区间。

例如 FIRST_TIME 为 3 月 20 日 00:47，回放原帖覆盖本地 3 月 19 日 23:45 至 3 月 29 日 23:45，左闭右开；调用从 3 月 20 日 00:02 开始，到 3 月 29 日 23:47 结束。需要读取的旧 weight 对应本地 3 月 20 日 01:00 至 3 月 30 日 00:00，共 240 行。

## 第二步：连续完整回放

replay_hours.py 顶部默认配置：

```python
DATA_DIR = ROOT / "data_240h"
CONFIG_DIR = DEFAULT_CONFIG_DIR  # 仓库的 configs/，或解压交付包中的 configs/
MODE = "full"
MAX_ATTEMPTS = 100
RESUME_DIR = None
OUTPUT_ROOT = ROOT / "outputs"
LOG_LEVEL = "INFO"  # 改成 DEBUG 查看源模块已有的详细日志
LOG_TO_FILE = True
```

直接执行：

```bash
/opt/ap/python/anaconda3/bin/python replay_hours.py
```

这条命令依次模拟每小时 02、17、32、47 分的调用，分别处理截止 00、15、30、45 刻度的前 15 分钟原帖，共 960 批，串行 await 逐批等待完成；不按真实时钟等待 10 天。每批调用的是正式 run_live_batch，只有这个测试进程临时模拟内部系统时间，正式函数没有新增时间参数。

历史 B 只在最初从 history_seed 载入一次，随后仅用本次返回的 event_results 按索引更新，并逐批保留最近 100 天。绝不把后续旧事件结果灌进 B；reference_events 只用于对账。空批次也照常调用。全部使用同一固定资产名单和同一套配置。

每小时 47 分生成两种比较：

- weight_only：旧事件结果用新 weight 函数重算，再与对应旧整点行对比。验证评分、窗口和聚合；不受本次 AI 回答变化影响。
- full：本次连续完整流程返回的 weight 与同一旧整点行对比。若不同，可能来自新 AI 回答、归组或累计历史的差异，不能仅看 DIFF 就认定 weight 算法有错。

例如本地 16:47 对齐旧表 17:00，窗口为 [15:45,16:45)。指定名单完整保留，旧表不存在的列仅在比较时按 0 对齐；已有 NaN 不补零，非零差异仍报 DIFF。

MODE="full" 会真实调用配置中的 AI 和向量服务。可先把 HOURS 改小验证数据和服务，再导出 240 小时的数据运行；不需要修改业务代码。

## 输出和续跑

输出目录为 outputs/replay_full_实际启动时间/：

- run.log：原三轮、向量和网络执行器日志，含实际 UTC+8 时间、模块/行号、工作线程及模拟批次；终端同步显示，续跑时追加。详见 [LOGGING.md](LOGGING.md)。
- 每批子目录：成功批次的三张输出表及批次状态；失败仅有状态和日志；47 分另有 comparison_full.csv、comparison_weight_only.csv。
- hourly_summary.csv：240 个小时的两种 PASS/DIFF、差异资产数量和旧表对应时刻。
- weights.csv / .pkl：全部 960 次权重；weights_47.csv：240 个 47 分权重。
- summary.json：完成批次/小时、空批次数、原帖数、两种比较的 PASS/DIFF 数量。
- final_history.pkl：全部成功结束后的内存事件快照。

需要按资产统计已经保存的差异时，不用重新回放。运行 summarize_diffs.py 可统计差异资产值总条数、涉及的不同资产数量及每个资产的差异次数，见 [DIFF_SUMMARY.md](DIFF_SUMMARY.md)。

需要查看三轮结果变化涉及多少原帖时，运行 compare_post_results.py，忽略解释与请求元数据，见 [AI_RESULT_COMPARISON.md](AI_RESULT_COMPARISON.md)。

只比较同一帖子＋资产的聚合权重时，使用 compare_post_weights.py，不要求事件文本相同，见 [POST_WEIGHT_COMPARISON.md](POST_WEIGHT_COMPARISON.md)。

DIFF 会继续对完后续小时；AI/向量异常或超时会记录失败状态并停止，后续批次不启动。修复问题后把 replay_hours.py 顶部 RESUME_DIR 改为日志中的本次输出目录，再执行同一命令。它会从初始 B 重建已经成功的批次增量，跳过成功批次的 AI 请求，失败的同一批从头重算，不恢复 AI 中间结果。数据文件、模式和处理版本必须匹配；不要在续跑前重新导出覆盖数据。

只修复密钥、接口连接或提高 MAX_ATTEMPTS 可以续跑。修改提示词、模型、向量版本、账号映射、评分参数或数据后，应开始新回放，不能混用旧进度。若换了实际向量服务地址，旧向量摘要也可能不匹配，不能靠续跑绕过检查。

返回码 0 表示全部比较 PASS，1 表示处理完成但有 DIFF，2 表示错误停止。summary.json 会区分完成与失败，不能把尚未完成的结果当作 240 小时全部通过。

这次模拟覆盖函数的连续调用与内存状态传递，不执行实盘宿主每天 16 点的真实 Redis 清空/进程重启。

## 可选：只跑 240 次纯 weight 对账

同一份导出数据，把 replay_hours.py 顶部改为 MODE="weight"、RESUME_DIR=None，再执行同一个命令即可。它不跑三轮或向量，不加载 AI 密钥；输出到独立的 replay_weight 目录。正式完整回放仍使用 MODE="full"。
