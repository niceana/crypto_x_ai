# Linux 手动试跑说明（0.4.3）

请安装本试跑包内的 0.4.3 wheel。试跑脚本通过异步入口执行三轮、向量和 weight；失败批次重新处理，不保存业务中间检查点。

0.4.3 保留原准备/网络/协调线程分工，通过 async 接口接入与统一取消，调用须 await；试跑 CLI 已通过 asyncio.run 接入。整批超时/失败仅写 summary 和日志，不保存中间表或检查点；重新执行会从头重算失败批次。成功输出三张表及 status。默认全批计算预算 840 秒，取消关闭资源可能额外耗时。旧测试数据仍可用，调用统计列须完整保存。

事件合并只追加 date 严格晚于已有历史事件表全表最大时间的任务，所有资产共用 UTC 发布时间截止，较早及同刻任务跳过并记数量。空历史首次导入不受限制；同一批较新的同刻任务一起处理。第一轮输出保留所有回答，跳过任务不进入向量或后两轮，也不写入事件增量。

## 1. 目录和安装

```text
crypto_x_ai_linux_trial_0.4.3/
├── wheels/crypto_x_ai_live-0.4.3-py3-none-any.whl
├── configs/
│   ├── event_processing_config.yaml
│   ├── round1_ai_config.yaml
│   ├── round2_ai_config.yaml
│   ├── round3_ai_config.yaml
│   ├── embedding_config.yaml
│   └── .env.example
├── data/                    # 放你导出的测试数据
├── run_full.py              # 测完整三轮及 weight，会请求实际配置的 AI 服务
├── check_weight.py          # 只计算 weight，不请求 AI 或向量
├── _trial.py                # 两个脚本共用的辅助代码
└── README.md
```

服务器沿用实盘解释器 `/opt/ap/python/anaconda3/bin/python`。当前业务包在 Python 3.8.5 下验证；安装前核对这套环境的版本及依赖，不创建另一套虚拟环境。

```bash
mkdir -p "$HOME/crypto_x_ai_trial"
unzip crypto_x_ai_linux_trial_0.4.3.zip -d "$HOME/crypto_x_ai_trial"
cd "$HOME/crypto_x_ai_trial/crypto_x_ai_linux_trial_0.4.3"
/opt/ap/python/anaconda3/bin/python -m pip install --no-deps wheels/crypto_x_ai_live-0.4.3-py3-none-any.whl
/opt/ap/python/anaconda3/bin/python -c 'import crypto_x_ai_live; print(crypto_x_ai_live.__version__, crypto_x_ai_live.__file__)'
```

pip 会把业务代码安装到该解释器的包目录，安装后可从任何工作目录 import。`--no-deps` 只安装业务包，保留实盘现有依赖；numpy、pandas、httpx、PyYAML、python-dotenv 需已具备并核对兼容性。wheel 文件只是安装文件，不需要放进实盘源码目录。后文的 python 均指这套实盘解释器。

wheel 内含三轮业务实现、HTTP 请求与校验、向量编码/检索、内存表适配器、续跑及 weight 计算。它不包含模型权重、密钥、帖子、历史表、Redis 或 xdata 客户端，数据由调用方提供，模型通过已有 HTTP 服务调用。

## 2. 配置放哪里

放在本目录的 configs/ 中，也可以通过 --config-dir 指向服务器已经在用的配置目录。

最稳妥的做法是从旧程序复制实际使用的以下五个文件，保持文件名：

- round1_ai_config.yaml：第一轮提示词、模型、接口。
- round2_ai_config.yaml：第二轮提示词、模型、接口。
- round3_ai_config.yaml：第三轮提示词、模型、接口。
- embedding_config.yaml：向量服务地址、模型、版本及检索配置。
- event_processing_config.yaml：账号映射和 weight_grid 的评分系数、枚举映射。

模板中的接口地址/模型仍有空值，全流程前须填写实际配置。向量服务地址、模型、版本和编码配置必须与历史 B 保存的向量一致，否则按约定停止，不自动重算历史向量。event_processing_config 中旧数据库表路径不用于这两个脚本的读写，数据从 PKL 参数传入。

密钥使用环境变量，也可在 configs/.env 填写：

```bash
cp configs/.env.example configs/.env
```

```text
ROUND1_AI_API_KEY=你的第一轮密钥
ROUND2_AI_API_KEY=你的第二轮密钥
ROUND3_AI_API_KEY=你的第三轮密钥
EMBEDDING_API_KEY=你的向量密钥
```

纯 weight 测试只读取 event_processing_config 中的评分配置，不加载三轮接口或密钥。

## 3. 怎么把数据传给包

函数接收 pandas.DataFrame，并不自行读 Redis。正式接入时，直接把宿主从 Redis 读到的两个 DataFrame 传给 run_live_batch 即可。手动试跑先用 PKL 保存同样的 DataFrame，保持复合索引、datetime、字符串 ID 和 embedding，不需要经过 CSV 转换。

| 文件 | 使用场景 | 内容 |
| --- | --- | --- |
| data/posts.pkl | 全流程 | 本批新帖子，单层 date 为爬虫时间，可重复；普通列至少为 id、created_at、text、user_name，id 字符串且全批唯一。 |
| data/history_before.pkl | 全流程 | 该批开始前最近 100 天已处理的事件表 B，保留原 14 个基础列及已有的两轮 call_meta，旧 14 列表也兼容；使用 `(date, record_id)` 索引，date 是 UTC 发布时间。不要包含比该批更晚已完成的同资产事件。 |
| data/events_completed.pkl | 纯 weight | 已完成二三轮的事件结果表，可以用旧程序的完整结果，或全流程脚本输出的 events_merged.pkl。 |
| data/old_weight.pkl | 对比旧 weight | 旧算法的某一组权重宽表，date DatetimeIndex；允许只含交集资产列，评分组合和聚合模式须对应。 |
| data/assets.json | 两种测试 | 资产字符串数组，也支持原 InstrumentList JSON 对象。使用与旧比较范围相同的资产。 |

在你能读取旧数据的脚本/notebook 中导出：

```python
from pathlib import Path
import json

folder = Path("trial_data")
folder.mkdir(exist_ok=True)
new_posts.to_pickle(folder / "posts.pkl")
history_before.to_pickle(folder / "history_before.pkl")
events_completed.to_pickle(folder / "events_completed.pkl")
old_weight.to_pickle(folder / "old_weight.pkl")
(folder / "assets.json").write_text(json.dumps(assets, ensure_ascii=False), encoding="utf-8")
```

把这些文件复制到 Linux 的 data/ 下。变量名只是示例，替换为你读取出的实际 DataFrame。没有历史时，全流程用 --history none；想测正常历史归组效果时应提供真实 B，而不是空历史。

也可以直接使用新增的 export_xdata.py，从已有 xdata 环境调用 cli.read 并导出上述文件。具体选时段、表路径及命令见 [EXPORT_XDATA.md](EXPORT_XDATA.md)。

时间不要提前整体加 8 小时：posts 的 created_at 按原帖 UTC 发布时间传入，date 索引仍为爬虫时间；历史事件表 date 也保留旧程序的 UTC 发布时间。包只在筛选 weight 时按 created_at + 8 小时换算，weight 输出索引为 UTC+8 的当前时刻。新帖子 id 应在导出前就是字符串。

## 4. 测全流程

```bash
python run_full.py --method sum
```

默认读取 data/posts.pkl、data/history_before.pkl 和 data/assets.json。也可显式传绝对路径或相对于本试跑目录的路径：

```bash
python run_full.py --posts data/posts.pkl --history data/history_before.pkl --assets data/assets.json --method max_abs
```

输出保存在新建的 outputs/full_时间戳/：

- round1_results.pkl / .csv：第一轮输出。
- event_results.pkl / .csv：本批事件增量输出。
- weight.pkl / .csv：最终一行 weight。
- events_merged.pkl：B 加上本次增量，方便让新旧 weight 使用完全相同的事件输入。
- summary.json：调用参数、评分组合和结果摘要。

失败或超时仅写 summary.json 和日志，不写处理表、weight 或 resume_state.pkl。排查后用原数据重新执行命令，整个失败批次从头处理；脚本只写本地 outputs，不更新 Redis、xdata。

## 5. 用历史数据测全流程

正式函数没有 as_of 参数。为了用历史数据试跑，这个独立脚本支持模拟系统时钟，不改业务源码或正式函数接口：

```bash
python run_full.py --method sum --test-time '2025-01-01 16:47:00'
```

test-time 按 UTC+8 理解。上例需准备该测试时刻的新帖子批次，以及它开始前的历史 B，不能让输入包含测试时刻之后的帖子或事件。未填写 test-time 时使用真实系统时间。

## 6. 单独检查 weight

先复用同一份已完成的二三轮事件结果，避免重新请求 AI 的输出变化干扰比较。这个脚本调用包里与正式入口相同的评分和聚合函数，只计算 weight，不跑三轮、不请求向量、不需要密钥。

```bash
python check_weight.py --events data/events_completed.pkl --method sum --test-time '2025-01-01 16:47:00'
```

有旧 weight 表时，直接比较：

```bash
python check_weight.py --events data/events_completed.pkl --old-weight data/old_weight.pkl --method sum --test-time '2025-01-01 16:47:00'
```

按本项目保存的 main 基线 cad15ee 的旧规则，UTC+8 的 16:47 新权重对应旧表 17:00，二者都使用 `[15:45, 16:45)`。脚本会自动选择旧表这一行，按相同资产列对齐，打印 PASS 或 DIFF，并保存 outputs/weight_时间戳/comparison.csv。若服务器旧代码使用其他小时窗口规则，需先按它实际的规则对齐。

旧表只包含“事件表出现的资产”与“指定名单”的交集，允许缺少部分指定资产列。脚本保留完整指定名单，只将旧表不存在的列按 0 参与比较，并在 comparison.csv 的 old_column_present 及 summary.json 中标明；若新结果非零仍报 DIFF。旧表已有列中的 NaN 不补零。

建议选择 xx:45 到 xx:59 的测试时刻。16:02、16:17、16:32 是部分小时窗口，不能直接与旧的完整小时结果比较；脚本会提示而不会误报为算法差异。没有 old-weight 时，任何刻度都可以只计算并查看新结果。

method 必须和旧表一致：旧表是 sum 就传 sum，旧表是 max_abs 就传 max_abs。评分系数与枚举映射也要一致。若旧配置有多个不同评分参数组合，两个脚本都可添加 --weight-param-id 对应参数ID。

全流程生成新的 AI 回答时，最终 weight 不保证与旧回答生成的 weight 相同。定位差异时先比较各轮结果，再用同一事件结果验证 weight。

需要连续 240 小时回放时，参见 [BATCH_REPLAY.md](BATCH_REPLAY.md)：每 15 分钟处理一次，共 960 批，只在每小时 47 分执行 240 次对账；使用实盘解释器 `/opt/ap/python/anaconda3/bin/python`，参数直接在脚本顶部配置。

run_full.py 和 replay_hours.py 已初始化源模块日志，默认 INFO，终端与本次输出目录的 run.log 同步记录；可在脚本顶部改 LOG_LEVEL="DEBUG"。更新和续跑说明见 [LOGGING.md](LOGGING.md)。

## 7. 正式宿主如何 import

把 wheel 安装进宿主实际使用的 Python 环境，配置目录可以是任意绝对路径。下面两个输入是宿主已从 Redis 读取并还原出的 DataFrame，不是 Redis 键名：

```python
from crypto_x_ai_live import load_live_config, run_live_batch

config = load_live_config("/你的配置目录", max_attempts=100)
result = await run_live_batch(
    new_posts,               # 本批原帖 DataFrame
    history_results,         # 过去 100 天事件表 B，DataFrame
    assets=tradable_assets,  # 当前可交易资产的字符串数组
    config=config,
    aggregation_method="sum",  # 或 max_abs
)
if result["status"] == "success":
    round1_results = result["round1_results"]  # 输出：第一轮结果表
    event_results = result["event_results"]    # 输出：本次事件增量
    weight = result["weight"]                  # 输出：当前时间的一行权重
else:
    pass  # 原帖留待下一轮合并重算，不写业务表。
```

只有 result["status"] == "success" 时才读取和提交三张输出表。失败/超时仅有 status，原帖留待下一轮合并重算，不发布新 weight。event_results 是增量，不能替代整个 B。

宿主应保证下次调用函数前，旧进程已经结束，前一批未结束时不要并发启动后一批。要求重启前，Redis 里的数据已更新进 xdata，读取的时候已经是更新后的 xdata。

回放结束后，summarize_diffs.py 可以只读已有对账文件，按资产统计差异值条数和不同资产数量，无需重跑 AI，见 [DIFF_SUMMARY.md](DIFF_SUMMARY.md)。

compare_post_results.py 则按原帖统计第一轮事件、第二轮归组、第三轮四项标签的变化，排除解释和请求元数据，见 [AI_RESULT_COMPARISON.md](AI_RESULT_COMPARISON.md)。
