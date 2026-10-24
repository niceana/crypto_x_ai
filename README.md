# AI 帖子三轮处理项目

本项目从帖子提取事件，建立按代币划分的披露记录，关联历史事件，再分析经济影响。项目独立运行，不需要打包或安装本项目；三个轮次分别启动，round1事件提取正常结束后自动导入成功帖。

三个轮次统一称为 **round1事件提取**、**round2事件去重归组**、**round3事件影响分析**。两张表按用途区分：

| 表名 | 数据粒度与用途 | 对应配置 |
| --- | --- | --- |
| round1帖子事件提取表 | 一帖一行，保存原帖输入、提取结果及状态 | `post_extraction_config.yaml` 的 `task_path`；独立导入来源为 `event_processing_config.yaml` 的 `first_round_task_path` |
| 事件去重分析表 | 一条事件 × 一个代币一行，共用保存归组结果、向量及影响分析结果 | `event_processing_config.yaml` 的 `result_path` |

本次统一显示名称，现有配置键、路径值和真实数据表路径保留。配置中的旧称呼占位文本也保持原值，填写时以上表为准。“去重归组”通过 new/update/repeat 和事件组表达披露关系，仍保留每条记录。完整职责见 [项目总览](project_overview.md#统一主题与表名)。

事件去重分析表当前为 14 个普通列，第二、三轮 AI 回答分别保存在 round2_ai_response、round3_ai_response 两个 JSON 字符串列，第三轮五个影响字段保留在原始正文内。回答列还保存最后一次响应 usage、接口生成时间、实际请求次数，以及提交数据库前的北京时间 created，不保存推理文本。字段、失败和存储重试口径见 [表结构](docs/data/EVENT_RESULT_TABLE.md#两轮-ai-回答的保存口径)。

## 环境与配置

本项目使用 Python 3.8.5。当前机器的整机解释器为 `C:\Program Files\Python38\python.exe`，项目隔离环境为 `.python38\venv\Scripts\python.exe`，VS Code 默认解释器也指向该环境。本地验证使用项目隔离环境；正式读库仍需可导入已有数据客户端 `xdata` 的 Python 3.8.5 环境。

```powershell
.\.python38\venv\Scripts\python.exe -m pip install -r requirements.txt
.\.python38\venv\Scripts\python.exe -X utf8 -m unittest discover -s tests -t .
```

下文命令中的 `python` 均应使用 Python 3.8.5；本机可替换为 `.\.python38\venv\Scripts\python.exe`，不依赖系统 PATH 的默认版本。

三轮 AI 请求遇到 HTTP 429 时会在本次运行中暂停后重试，全部后续起发共同等待本地指数随机退避和有效 `Retry-After` 中的较长时间；缺少有效响应头时使用本地退避。429 与网络故障、5xx、无效输出共用 `run.max_attempts`，默认 3 次且包含首次；最后仍为 429 时保存失败并停止，避免无限重试。第二轮 Embedding 请求也在其独立批次预算内重试 429。持续限流时应降低 `concurrency` 或设置 `requests_per_second`；等待重试不会自动调整并发配置。

| 文件 | 用途 |
| --- | --- |
| `configs/post_extraction_config.yaml` | 帖子提取任务：源帖子路径、round1帖子事件提取表、日期范围、round1事件提取 AI 配置和日志 |
| `configs/event_processing_config.yaml` | 事件导入、第二三轮及小时权重处理：来源表、事件去重分析表、权重表、日期范围、代币映射、检索、round2/round3 AI 配置路径和日志 |
| `configs/round1_ai_config.yaml` | round1事件提取提示词、AI 接口、请求及运行参数 |
| `configs/round2_ai_config.yaml` | round2事件去重归组提示词、接口、请求及运行参数 |
| `configs/round3_ai_config.yaml` | round3事件影响分析提示词、接口、请求及运行参数 |
| `configs/embedding_config.yaml` | round2事件去重归组使用的 bge-m3 地址、模型、每批条数、重试及相似度阈值 |
| 进程环境或 `configs/.env` | AI_API_KEY、EMBEDDING_API_KEY，参考 `configs/.env.example` |

入口按脚本位置读取项目的 `configs`，不依赖当前工作目录。两份任务配置中的相对 AI 配置路径按配置所在目录解析，例如round1事件提取填写 `ai_config_path: "round1_ai_config.yaml"`；绝对路径继续支持。数据系统路径原样传给客户端。

`.env` 继续与 AI 配置放在同一目录，进程环境优先。因此使用本地文件时放在 `configs/.env`；不要只移动 YAML 而留下原根目录 `.env`。自定义绝对 AI 配置路径时，仍查找该文件同目录的 `.env`。

起止日期使用带引号的 `YYYY-MM-DD`，包含起止日全天。第二、三轮必须同时填写起止日期；round1事件提取和独立导入仍允许两端同时为空。第一轮设置范围时按自然月分段，第三轮始终按配置范围分段。

```yaml
start_time: "2021-01-01"
end_time: "2021-12-31"
```

## 初始化和日常运行

从项目根目录执行。第一次建立round1帖子事件提取表前，在 `configs/post_extraction_config.yaml` 填写源 `source_path` 和尚未创建的 `task_path`，两者必须不同：

```powershell
python -X utf8 -m entrypoints.initialize_tasks
```

round1事件提取初始化完整读取源帖，校验后注册目标并一次写入，不使用处理日期、不读取 AI 配置、不调用 AI。注册成功但写入失败可能留下空目标；须按真实客户端规则处理原目标或使用尚未注册的新目标，再重新初始化。仅清空数据不代表可安全重复注册。

配置好round1事件提取 AI，以及事件配置的 `result_path` 和所需账号映射后，日常依次执行：

```powershell
python -X utf8 -m entrypoints.run_round1
python -X utf8 -m entrypoints.run_round2
python -X utf8 -m entrypoints.run_round3
```

`entrypoints.run_round1` 跳过读取时已成功的帖子，处理其余任务；正常返回后，将本范围成功帖从内存导入事件去重分析表。自动导入沿用 post_extraction_config 的实际round1帖子事件提取表及范围，无需手工同步两份配置。round2事件去重归组与round3事件影响分析仍分别启动，不会自动串行执行。

当前三轮 AI 接口 URL 和 Embedding 基础地址为空，第二、三轮模型为空。第一轮已配置事件 JSON 提取及账号背景规则，第二、三轮 system 均已配置正式提示词；提取及经济影响判断的质量仍需真实样本验证。账号背景已按用户提供的 TradeCoins 50 名单填入；正式运行还须准备认证信息、核对源帖用户名并确定日期范围。event_processing_config.yaml 的日期目前留空，第二、三轮启动前必须填写，未设置会停止；仅独立导入保留日期为空时的全范围处理。第三轮提示词始终分析当前完整 event，new_information 仅辅助理解事件进展和新旧变化。阻塞项与可后置项见 [接入待办](docs/IMPLEMENTATION_ISSUES.md#运行前必须补齐)。

round1事件提取普通单帖失败不阻止其他成功帖导入；round1事件提取整体异常或中断不进入自动导入。导入失败时round1事件提取结果保留，重新运行同范围可跳过成功帖 AI 并补导。没有成功帖时不创建事件去重分析表。

历史数据独立导入使用：

```powershell
python -X utf8 -m entrypoints.import_events
```

独立导入使用 `configs/event_processing_config.yaml` 的来源和范围，默认要求范围内round1事件提取全部成功。导入自动判断目标是否存在：仅明确缺表时注册并首次写入；已有目标严格校验后只追加缺失记录，保留已有分析结果。无需配置导入模式。round2事件去重归组与round3事件影响分析按已有任务状态运行，不重新展开round1事件提取，不自动补跑范围外任务。

已有第一轮回答的 event 正文包含未转义英文双引号时，导入会批量尝试在内存中补转义，再严格校验；第一轮原始回答和状态保留，不调用 AI，也无需新参数或依赖。日志逐条记录修复与失败的帖子 ID，并汇总数量。内容截断、字段缺失或边界不明确的记录集中报错，本批在建表或写入前停止。具体支持范围和例子见 [历史回答引号修复](docs/rules/EVENT_RULES.md#历史回答中未转义引号的批量处理)。

`configs/event_processing_config.yaml` 已包含用户提供的 50 组账号关联币种，保留原名单大小写及迁移备注。第一轮随正文发送 `user_name`，按账号忽略大小写匹配后附加可选 `account_tickers`；没有背景时省略该字段。配置例如：

```yaml
user_name_to_tickers:
  ethereum: ["ETH"]
  project_account: ["ETH", "LEND"]
```

账号背景只帮助理解主体及指代，不能补造事实、不限制 AI 识别其他币种；没有背景的账号可能是公共媒体，逐条按正文判断。`tickers: []` 为合法输出，导入时直接跳过该事件，不按账号兜底，也不阻止其他事件导入。原回答和第一轮成功状态保留，不创建第二、三轮任务。缺少 tickers 字段、为 null 或类型不合法仍属输出错误，按请求预算重试。新规则不自动重跑历史成功帖或迁移旧事件行；已有表与新展开结果冲突时仍停止核对。具体例子见 [事件展开规则](docs/rules/EVENT_RULES.md#2-检查round1事件提取并展开记录)。

## 分析资产频数

在可导入 xdata 的 Python 环境中安装 matplotlib 后，可在项目根目录直接执行：

```powershell
python -X utf8 -m entrypoints.plot_asset_frequency
```

默认按 event_processing_config.yaml 的 result_path 及可选 start_time/end_time 读取 asset 列；日期留空时读取全表，不检查任务状态。输出到 reports/asset_frequency/，默认仅保存 PNG 和 CSV；`--top-n 100` 调整条形图展示数量，`--output-dir` 指定保存目录，`--show` 额外弹出图形窗口。

将第一轮已导入的第二、三轮共用结果表读取为 `df` 后，在分析环境安装 `matplotlib`，即可使用以下代码。只需 asset 列，不要求第二、三轮运行或成功：

```python
from postprocessing.asset_frequency import plot_asset_frequency

# 结果表每行计一次，不检查任务状态或事件组。
frequency, figure = plot_asset_frequency(df, top_n=30, output_dir="reports/asset_records")
print(frequency.head(100))
```

每次绘制 Top N 条形图、全部资产排名频数对数图和累计覆盖率曲线，标注覆盖 80%/90%/95% 数据需要的资产数；可选输出目录保存 PNG 与完整 CSV。累计占比的分母为全部有效资产行，不限于 Top N。`asset` 按原值统计，方便发现大小写、空白或别名造成的拆分；不读取状态或 event_id，不按事件组去重。没有出现在 df 中的资产不自动补零。只需统计时使用 `asset_frequency(df)`，不需要绘图库。

round2事件去重归组默认使用 `embedding_config_path: "embedding_config.yaml"` 指向的 bge-m3 纯向量检索。填写该文件的 base_url，例如 `https://服务地址/v1`，程序追加 `/embeddings`；把密钥填入 `configs/.env` 的 EMBEDDING_API_KEY 或进程环境。默认每批 8 条，只编码 event；生成后保存到事件去重分析表的 embedding 列，JSON 包含 model、model_version、config_hash 和 vector，重启复用。config_hash 只标识服务、模型、版本和编码规则，不含 event；同名模型更换权重时要修改 model_version，人工修改 event 时须清空该行向量并确定下游重算范围。第二轮只使用 event 和候选历史，不再读取原帖；第三轮仅使用本条记录的 asset、event、new_information，也不读取原帖。初始化已包含该列，不使用 SQLite。

用户确认模型已输出归一化向量；程序保留返回数值，直接用内积计算余弦相似度，不再归一化或转成 float32。仍校验格式、维度、有限数值、非零向量及单位范数，范数允许绝对误差 1e-5；新响应不满足约定时使用该批剩余请求预算重试，耗尽后停止。

第二、三轮共用 `round2_asset_list_path` 配置，各自在入口中用 `json.loads(cli.read_object(path).decode('utf-8'))['InstrumentList']` 读取名单，取每项第一个 `.` 前的币名，去首尾空白、转大写并去重。当前路径为 `crypto_1m/research_prod/meta/alphas/Cpt380_BA_Daily_Update_All.json`。两轮都只处理日期范围内 `asset` 命中名单的记录。第二轮在状态恢复、前史检查、向量准备和任务调度之前筛选；第三轮在检查第二轮状态、恢复第三轮状态、准备 AI 请求及 repeat 继承之前筛选，统计也只包含目标资产。每轮启动读取一次名单；配置留空字符串或未配置时处理全资产。本项不筛选权重入口。

第二轮按 event_processing_config.yaml 指定日期读取待处理任务，前史校验、候选检索及向量准备共用 history_lookback_months（默认 3 个日历月）。下面仅为范围填写示例，按实际要处理的日期修改：

```yaml
start_time: "2025-01-01"
end_time: "2025-12-31"
history_lookback_months: 3
```

历史窗口按日历月回推，目标月没有对应日期时取月末，保留原时分秒、时区和纳秒；3 个月不等同于固定 90 天。旧配置键 `history_lookback_years` 已替换为 `history_lookback_months`，须同步修改配置；已完成结果不会自动重算。

每条披露按自身发布时间 t 检索 `[t-3个月, t)`，包含下界、排除同刻与未来。普通历史不读取 3 个月下界以前的数据；较晚已完成冲突只读取资产、状态两列，全表 AI 摘要仍只读单列。组起点不在窗口内时不回查旧组，保留已保存的组 ID 和窗口内进展；内部 first_disclosure=null、history_trimmed=true，并用 first_available_disclosure 提供窗口内最早披露。第二轮 AI 只接收 `{"current":{"published_at":...,"event":...},"candidates":[{"candidate_id":"E1","history":[{"published_at":...,"event":...}]}]}`，每条 published_at 来自对应 df 的 date 索引值，以字符串保留完整精度和时区；不发送真实 event_id、资产、披露标识、增量、次数或截断标记。窗口内起点或最早可用披露与全部 update 的完整 event 按顺序保留。AI 返回 matched_candidate_id，程序校验并映射回真实 event_id 后保存；每个请求独立使用 E1、E2 等编号，重试沿用原输入和映射，数据库结构不变。第三轮不接收组历史，仅使用本条记录的事件输入。输入例子及边界见 [第二轮规则](docs/rules/EVENT_RULES.md#6-round2事件去重归组调度和输出)。

第三轮先按配置范围读任务。当前事件配置开启 `round3_skip_incomplete_round2: true`：在状态检查、恢复、AI 请求和 repeat 继承前，跳过第二轮状态为 `0/9/-1` 的记录，先处理第二轮已完成的部分；跳过行的状态、回答与错误均保留。第二轮标记成功但基本关联不合法，或状态值未知，仍会停止。配置关闭或省略时保留严格规则，目标范围第二轮有未完成就停止。汇总的 total 等字段只计入可处理记录，skipped_round2 单列跳过数量；补跑第二轮后再运行第三轮，会补上新完成的记录，已成功的第三轮不重做。

无可处理任务或可处理部分全部完成时，第三轮直接返回。其余记录检查通过后恢复第三轮状态，再按月分析 new/update。请求必含 asset、event；仅在有新增信息时附带 new_information，空值或纯空白时省略该字段。new/update 均分析当前完整 event，new_information 辅助理解进展。第二轮保存 repeat 时不改第三轮状态，初始仍为 0；第三轮不调用模型，而是复制同币种、同事件组严格早于本条披露的最近有效第三轮响应，保存后标记完成。仅在有待处理 repeat 时读取必要的历史响应列和元数据，不受第二轮 3 个月窗口限制；new/update 的 AI 输入仍不含历史或原帖，不回查组起点。新回答须为恰好五字段的合法 JSON，五字段均为非空字符串，前四字段严格使用正式提示词规定的枚举；不合规则按原请求预算重试，耗尽后标记本条失败。仍保留跨月 record_id 检查、全表第三轮配置摘要单列核对，以及原有并发、保存和停止恢复规则。详细边界见 [第三轮规则](docs/rules/EVENT_RULES.md#7-round3事件影响分析检查调度和保存)。

使用 `round2 && round3` 连跑时，第二轮单条普通失败仍允许整轮正常返回并启动第三轮，开启上述开关即可先处理成功部分；若第二轮整轮异常退出，`&&` 不会启动第三轮，第三轮开关不改变外部命令的行为。第三轮自身的配置、摘要冲突、数据库和停止型 HTTP 错误仍按原规则停止。

按事件组最高余弦分数筛选，只将分数大于等于 similarity_threshold 的最多 10 组交round2事件去重归组；无历史或所有组都低于阈值时直接 new，继续round3事件影响分析。默认阈值 0.50 是待真实样本标定的试运行起点，可在 Embedding 配置调整：

```yaml
similarity_threshold: 0.50
```

调高阈值会增加直接判新，调低会增加round2事件去重归组调用；更改只影响待处理记录，不自动重算已完成关联。Embedding 或向量保存失败停止本轮；存储重试不重复请求向量。自定义检索可用 retriever 的 `模块:函数` 覆盖，应用也可直接注入函数。完整格式与失败规则见 [表内向量与阈值方案](docs/plans/EMBEDDING_TABLE_STORAGE_PLAN.md)。

第二轮正式提示词位于 `configs/round2_ai_config.yaml`，关系选择和新增信息填写融入“判断同一具体事项”“识别新增事实”两个步骤，保留 `# ***标题***` 和四种关系的固定 JSON 输出示例，user 为 `{{task.text}}`；第三轮正式提示词已写入 `configs/round3_ai_config.yaml`，按当前完整事件分析五个经济字段；两轮真实接口、模型仍待配置。三轮共有的全表按列摘要读取 `common.store.read_stage_hashes` 使用 `cli.read(path, start_time='', end_time='', columns=[hash_column])`，从返回的 DataFrame 中取摘要列原值；首次认领前核对一次，跨月复用，读取失败时停止。调整任一轮提示词会改变该轮 config_hash；已有不同摘要仍按原规则停止，不自动覆盖或重算历史结果。

update 的 new_information 须独立说明此前已知状态、本次变化及当前状态和条件，不能只写“获批”“金额提高”。旧状态引用唯一匹配组历史，新事实引用当前 event；未知部分说明未披露，不补造，累计总值与本次新增量分清。第三轮 AI 输入不含历史，用这段对照辅助理解当前事件的进展。完整要求及例子见 [第二轮规则](docs/rules/EVENT_RULES.md#6-round2事件去重归组调度和输出)；程序的非空字符串校验不能代替真实样本的语义质量验证。

## 只读观察已完成批次的相似度

在刚才运行第二轮的 Python 环境执行 `python -X utf8 -m entrypoints.audit_similarity`，按 `event_processing_config.yaml` 的日期范围复用表内向量，导出本地 `events.csv`、`candidates.csv`、`thresholds.csv` 和完成说明。最高相似度对应的具体历史事件、记录 ID、事件组 ID 及低于阈值的候选都可查看；不调用模型、不恢复状态、不写数据库。默认每条导出阈值筛选前的前 10 个组，可用 `--top-k 20` 增加。多阈值统计固定使用已有分组，不代表人工验收准确率。命令参数、观察方法及还原限制见 [相似度只读观察](docs/SIMILARITY_AUDIT.md)。

## 第三轮结果转为小时权重表

第二、三轮完成后，使用 `configs/event_processing_config.yaml` 中的 `result_path`（共用结果表）、`weight_path`（参数表的基础路径）、`start_time`、`end_time` 和 `weight_grid`。`weight_path` 按要求留空，运行前自行填写；实际运行的日期必须同时设置为 `YYYY-MM-DD`，包含起止日全天。先预览参数、组合数量和表名，再在已安装 `xdata` 的环境运行：

```powershell
python -X utf8 -m entrypoints.write_weight_data --dry-run
python -X utf8 -m entrypoints.write_weight_data
```

固定评分为 `direction × (B × materiality) × (C × execution_stage) × (D × event_relation)`。B/C/D 和每个枚举映射均配置为候选数组，独立做笛卡尔积，每组参数输出一张表。未填参数采用默认值；显式数组须非空且只含有限数值，候选按数值去重。persistence 保留映射配置，暂不参与评分、组合展开或参数标识。即使两组结果相同也分别输出。

实际表路径默认为 `{weight_path}__B{B}_C{C}_D{D}__{param_id}`。param_id 来自评分版本、系数和完整有效映射的 SHA-256 前 16 位，日期及候选顺序不影响标识。同一参数在后续日期继续更新同一表，旧无参数后缀表不自动改写。默认上限 1000 组，超限、非法参数或冲突表名在连接数据库前停止。`--dry-run` 不连接数据库。

可用 `--config "另一个事件配置文件路径"` 指定同结构的事件配置。源表按日期范围只读取一次、回答只解析一次，供所有组合复用，不调用 AI。逐组合生成、保存并释放 DataFrame。目标不存在时注册并首次 write；已有表通过 `update_by_column` 替换重叠小时，通过 `update_by_index` 追加新小时。重复运行不累加，范围外数据保留。完整参数、目标表和状态保存到 `reports/weight_grid/<北京时间>/manifest.json`，可用 `--report-dir` 指定新目录。某表失败时停止，已成功表保留，重跑继续对齐更新。

`weight_data` 的单层索引名为 `date`，类型为 `DatetimeIndex`（`datetime64[ns]`），每条时间均为整点。列为读取范围内去重资产加 `.USDT.UFUTURE.BINANCE` 后缀。`basic_df` 保留原始完整发布时间，内存构建时将事件归入下一小时：`15:20:10 → 16:00:00`，整点也顺延。先按目标小时和资产以数值求和、补零并将普通列转为文本，写库前最后把时间索引整体加 8 小时；例如 `12-31 15:20 → 16:00 → 写入 01-01 00:00`。实际写库范围从 `start_time 08:00:00` 到 end_time 次日 `08:00:00`。源表读取日期不变，索引仍为无时区的 datetime。

当前版本 `direction_product_v2` 的参数标识与旧版不同，避免将平移后的数据混入原时间口径的参数表；旧表保留。`reports/weight_grid/` 只保存本地参数和状态清单，不存权重 DataFrame；重跑依赖数据库实际数据，不依赖旧清单。

续写时保留已有资产列：本批未出现的旧资产在新小时填 `"0.0"`，新资产的已有历史行也补文本零值。平移后的批次起点 08:00 行若已存在则保留，避免覆盖上一批最后一小时的顺延权重。重复运行不会再次平移已有表。返回的本批 df 包含保留的起点值及合并后的资产列。

默认 B/C/D 为 1；方向为利好 `1`、利空 `-1`、其他 `0`；重要性为高 `3`、中 `2`、低 `1`、不确定 `0`；执行阶段为未开始 `1`、执行中 `2`、已完成 `3`、不确定 `0`；关系为 new `2`、update `1.5`、repeat `0.5`。利好、高、已完成、new 得分为 `18`，利空为 `-18`。任一乘数为零则得分为零；事件及小时聚合结果溢出时停止。

第三轮回答先解析外层 `round3_ai_response`，再解析其 `ai_response_content`。repeat 与其他行一样只使用本条已保存的响应，关系使用本条 event_relation；响应填充已由第三轮处理，权重阶段不再查历史或继承其他行。任何行（包括 repeat）回答为空、状态未完成或 JSON 损坏时停止。

在已有客户端的应用或 notebook 中可以执行整个网格，返回逐组合摘要和参数清单路径：

```python
from common.config import load_event_config
from postprocessing.weight_grid import run_weight_grid

settings = load_event_config("configs/event_processing_config.yaml")
summary = run_weight_grid(cli, settings)
print(summary["manifest_path"])
```

单组合接口仍返回一个 DataFrame：`build_basic_df(results, parameters=WeightParameters(...))`、`build_weight_data(basic_df, start_time=..., end_time=...)` 在内存构建；`write_weight_data(..., parameters=...)` 续写指定目标。完整配置、口径和示例见 [小时权重表](docs/data/WEIGHT_DATA.md)。

已有表出现无法对应源资产的列时，运行 `python -X utf8 -m entrypoints.audit_weight_columns --param-id <参数标识>`；只有一个组合时可省略参数标识。该入口只读对照配置范围内 asset 与所选参数表列名。续写表中本批未出现的列可能来自历史批次，需结合此前范围判断。保存入口也会核对本批生成列的来源。

## 代码目录与应用内调用

`common` 统一放公共能力和共用业务约定；`initialization` 放两种初始化；`round1`、`round2`、`round3` 放各轮执行流程；`entrypoints` 负责配置和客户端接入。目录直接位于根目录，不套顶层包。

公共目录共 9 个功能模块：`config.py` 加载各类配置；`ai_protocol.py` 构造三轮大模型请求体并解析接口响应；`validation.py` 集中表字段、结构检查及三轮输出校验；`event_context.py` 组织业务输入和事件历史。其余为独立的时间规则、网络、分段执行、Store 和存储重试。三个 Store 的职责保留，具体依赖见 [代码结构](docs/CODE_ARCHITECTURE.md)，文件对应关系见 [公共模块收拢记录](docs/history/COMMON_MODULE_CONSOLIDATION.md)。

`common/time_rules.py` 继续独立保留，负责解析完整发布时间、按包含首尾日的范围判断和选择数据；原始索引及其时区不转换。它供校验、Store、事件上下文及导入共用，不随本次文字调整移动或修改。

已有应用可自行管理客户端，直接调用：

```python
from round1.runner import run_extraction

summary = run_extraction(
    cli, task_path, config_path,
    start_time="2021-01-01", end_time="2021-12-31",
)
```

该函数同步处理round1事件提取，返回 ExtractionSummary；仅主入口负责自动导入。可选 `on_successful_posts(new_posts, previous_posts)` 在分段保存完成后交出成功帖副本。初始化接入为 `entrypoints.initialize_tasks.initialize_existing_posts(cli, settings)`；直接业务函数的完整路径见 [项目总览](project_overview.md)。模块迁移见 [公共目录合并记录](docs/history/COMMON_DIRECTORY_MERGE.md)，类和函数名称对应见 [命名统一记录](docs/history/NAMING_CONSISTENCY.md)。

## 日志、停止和调试

入口使用任务配置的 logging，级别和各入口既有默认格式保留；日志模块名称随目录更新。INFO 显示进度和统计，WARNING 显示重试和单条失败，ERROR 显示整体停止和保存故障。常规项目日志不主动输出正文、提示词、认证或完整响应；外部异常说明仍按原文显示。

ReadTimeout 排查日志是例外：三轮 AI 每次读取超时都会在 WARNING 日志中直接输出任务已有的完整 `text` 输入，不反解析 HTTP 请求体，也不再次包装成 JSON 数组。第二轮即已组装好的 current 和 candidates，第一轮为原帖正文，第三轮为已组装好的当前事件输入。当前第二、三轮 user 模板为 `{{task.text}}`，与该业务输入一致；第一轮仅记录原帖，不附加模板里的固定说明。若以后修改模板，日志仍记录业务输入本身。若重试耗尽时最终仍为 ReadTimeout，同样的内容以换行及 `user=` 追加到 error_message，完整保留、不按 512 字符截断；其他错误继续沿用原限制，最终成功则清空错误。重试预算和状态规则不变。

第二轮启动时显示读表、历史校验及向量准备阶段。主线程批量认领各币当前可推进时点的任务，再将固定输入交给独立准备线程，提前检索、判断自动 new、组装并编码 AI 请求。`configs/round2_ai_config.yaml` 的 `run.ready_request_buffer_size` 默认 20，只限制已经准备好、尚未发送的备用 AI 请求。网络线程取得 HTTP 名额后从就绪队列取请求，取走即通知补货；补货不等待答复写库。自动判新及构造失败直接进入同一结果队列，不占备用请求位置。主线程按当前队列快照收取已有结果，一条就直接保存，多条就一次合并保存，不等待未来结果凑批或定时刷新；写库期间新返回的结果留到下一批。第二轮不再使用 result_buffer_size 限制准备或结果总量；数据库慢时允许结果积压，继续发送就绪请求。只准备同币当前时点，下一时点仍等前一时点全部写库后再准备；同刻使用同一份此前历史。

处理期间每保存确认 `configs/round2_ai_config.yaml` 中 `run.progress_every` 条结果（默认 100 条），或距上次汇报达到 10 秒时输出进度；开始、结束和异常停止也会汇报。日志包含已处理比例、已有完成、本次成功和失败、自动判新、剩余未处理、HTTP 请求中数量/并发上限、准备状态、待准备数量、就绪请求数量/备用上限、已认领未落库数量、待写库数量、累计 HTTP 尝试、暂停币种数、耗时及结果确认速度。直接读取各队列和计数的瞬时快照，不再用差值推算等待发送数量。成功和失败均在结果写库确认后才计入已处理；失败会单列，已处理 100% 不代表全部成功。统计不额外查库。10 秒汇报由调度循环触发，同步读写阻塞时会延后到返回后输出。

每次进度之后另报瓶颈统计：HTTP 平均并发按网络启动以来实际请求占用时长积分计算，并给出峰值，避免单看主线程取样时的瞬时并发。待写库结果拆成 AI、自动判新和其他；同时显示仍未结束的币种、可认领币种、有后续数据且前序结果待写库的币种。结果保存与认领写库分别显示平均每批耗时、平均批量及折合每条耗时，统计覆盖运行以来的调用，包含存储重试；认领计时仅覆盖状态和配置摘要写入及内存同步，不包含读表、向量计算或请求准备。待准备和就绪都为 0、待写库较多时，应结合这些指标判断是否卡在同币写库解锁；名单命中资产数不等于同时可请求 AI 的币种数。结果确认速度是整轮处理速度，不能当作单次数据库调用速度。一批结果保存确认后，共同认领已解锁的下一时点任务并立即提交准备，不再等待解锁币种达到 HTTP 并发数；认领仍按每次最多 200 条提交。

各轮专用日志使用完整主题名称，建表、读取及导入日志明确表名。公共网络与分段执行日志使用通用任务描述；内部阶段标识 `round1/round2/round3`、异常文本和错误字段继续沿用原值。

退出码继续为成功 0、普通异常 1、用户中断 130。round1事件提取初始化保留具体错误和目标核查提示；其他入口保留标准异常栈。停止程序后重启各自入口继续处理，恢复、保存和历史时间边界见 [round1事件提取规则](docs/rules/KEY_RULES.md) 与 [事件规则](docs/rules/EVENT_RULES.md)。

VS Code 打开项目根目录并选用已安装依赖和 xdata 的解释器。打开 `entrypoints/initialize_tasks.py` 等入口，使用 **Python Debugger: Debug Python File**；现有 `.vscode/launch.json` 为当前文件提供根目录导入路径和 UTF-8 设置，无需安装项目。

## 验证和文档

模拟测试按公共能力、初始化、轮次和入口分组，公共模拟客户端在 tests/support.py 与 tests/event_support.py。2026-10-16 按列摘要接口接入后，完整 267 项测试通过；包含三轮摘要列查询、跨月复用与读取失败停止，以及 23 项表内向量与阈值测试中的模型输出原值保存及复用、第二轮不读原文、共用网络重试及 1024 维相似度。测试不访问真实 AI 或数据库：

```powershell
python -X utf8 -B -m unittest discover -s tests -t . -q
```

当前本地验证可使用 `.\.python38\python.exe` 替代 python。真实接口和规模验证事项见 [待办](docs/IMPLEMENTATION_ISSUES.md)，模块职责和总体数据流见 [项目总览](project_overview.md)，数据结构见 [round1帖子事件提取表](docs/data/TABLE_DESIGN.md) 和 [事件去重分析表](docs/data/EVENT_RESULT_TABLE.md)。历史改造记录保留在 docs/history，研究计划位于 docs/plans。
