# 每个命令运行前的配置检查清单

按当前代码和配置核对，更新日期：2026-10-28。本文覆盖 `entrypoints/` 下的可执行入口，重点说明三轮 AI 处理。日常按“第一轮 → 确认自动导入 → 独立向量计算 → 第二轮 → 第三轮 → 权重”推进；初始化、独立导入和重置按需要单独运行。

## 独立阶段入口（Windows / Linux）

已初始化第一轮任务表后，按第一轮及自动导入、独立向量准备、第二轮、第三轮分别启动。Linux 单行 nohup 命令、日志查看、交接、恢复和跨年份扩展见 [独立阶段运行说明](INDEPENDENT_STAGES.md)。

- 第一轮使用 post_extraction_config.yaml 的日期；向量及第二、三轮使用 event_processing_config.yaml 的日期。确认第一轮任务表已经包含范围内的帖子，运行入口不会自动从源帖子表补任务。
- 三轮分别使用自己的密钥和 run.concurrency；向量阶段独立使用 Embedding 配置。
- 中断后重新执行对应独立命令，按数据库状态恢复并跳过成功结果；不使用 --resume 或运行清单目录。
- 向量及第二、三轮各自在启动时读取公司目标资产名单，允许名单随公司数据更新；已有成功结果保留。
- 向量及第二轮要求配置范围内第一轮全部成功；第三轮沿用 round3_skip_incomplete_round2 配置。各入口不会自动等待上游或启动下游。
- 不要对已有成功结果的任务表再次运行首次初始化入口；权重生成仍单独执行。

## 1. 先看：当前仓库还缺哪些配置

以下是核对时的文件状态，修改后以本地实际配置为准。

| 位置 | 当前值 / 状态 | 正式运行前要做什么 |
| --- | --- | --- |
| `post_extraction_config.yaml` 的 `source_path`、`task_path` | 仍为“填写……”占位文本 | 填写真实源帖子表、第一轮任务表路径 |
| `post_extraction_config.yaml` 的日期 | `2021-01-01` 至 `2021-12-31` | 确认确实要处理这一整年，首次接入可先选小范围 |
| `event_processing_config.yaml` 的 `first_round_task_path`、`result_path` | 仍为占位文本 | 填写第一轮任务表、事件去重分析表的真实路径 |
| `event_processing_config.yaml` 的日期 | 两端均为 `null` | 第二轮、第三轮及实际写权重前必须填写 |
| 三份 `round*_ai_config.yaml` 的 `api.url` | 均为空 | 分别填写完整 AI 请求地址 |
| 第二、三轮 `api.payload.model` | 均为空 | 填写对应接口实际支持的模型 |
| 第一轮 `api.payload` | 模型为 `glm-5.3-flash`，含 `provider_options: "max"`、`max_completion_tokens: 512` | 核对服务是否支持这些参数，输出额度是否足够容纳事件 JSON |
| `embedding_config.yaml` 的 `base_url` | 为空 | 使用默认向量检索时必须填写 |
| `configs/` 的密钥文件 | 只有 `.env.example`，没有 `.env` | 使用进程环境变量，或创建并填写同目录 `.env` |
| 第一轮 / 第二轮 / 第三轮并发 | `100` / `5` / `10` | 按接口额度检查，尤其注意第一轮 100 并发；三轮均未主动限制每秒请求数 |
| `round2_asset_list_path` | 已配置远程资产名单路径 | 确认名单可读、内容和本次资产范围正确；第二、三轮共用 |
| `round3_skip_incomplete_round2` | `true` | 第三轮会先处理第二轮成功部分；结束不代表整个日期范围全部完成 |
| `weight_path` / 诊断脚本的 `WEIGHT_PATH` | 均为空 | 运行对应权重、诊断命令前填写 |

核对时，项目 `.python38/venv` 使用 Python 3.8.5，但其中找不到 `xdata`。需要读写数据库的命令，应使用已接入内部 `xdata` 的 Python 3.8.5 环境；不能仅凭安装 `requirements.txt` 判断已具备读库条件。

## 2. 每次打开终端后的共同检查

### 2.1 固定解释器和工作目录

在项目根目录执行。以下命令统一使用 PowerShell 的 `& $py`，每个新终端先设置一次变量；若正式环境使用其他解释器，将路径替换为那个环境的完整路径。

```powershell
$py = (Resolve-Path ".\.python38\venv\Scripts\python.exe").Path
& $py --version
```

运行前确认：

- [ ] 当前目录是项目根目录，能够找到 `entrypoints/` 和 `configs/`。
- [ ] `$py` 指向本次实际要用的 Python 3.8.5 环境。
- [ ] 已确定本次是接着处理待完成任务，还是有意重算已完成结果。
- [ ] 已检查目标表、日期、资产范围，且没有另一个进程同时处理或重置相同表的重叠任务。
- [ ] YAML 日期写为带引号的 `"YYYY-MM-DD"`，起点不晚于终点。

读写数据库前，可用下面的命令检查依赖能否导入。它不创建客户端，也不验证实际连接和表权限：

```powershell
& $py -X utf8 -c "import httpx, pandas, numpy, yaml, dotenv; from xdata.client import get_client; print('依赖和 xdata 导入成功')"
```

安装或更新项目依赖前，先确认 `$py` 选对了环境：

```powershell
& $py -m pip install -r requirements.txt
```

`xdata` 不在 `requirements.txt` 中，需要沿用内部环境的接入方式。两个绘图入口还需要在同一环境安装 `matplotlib`：

```powershell
& $py -m pip install matplotlib
```

修改程序后如需运行模拟测试，先确认当前环境具备测试依赖；这条命令不代表真实数据库和 AI 接口联调：

```powershell
& $py -X utf8 -m unittest discover -s tests -t .
```

### 2.2 不要把不同入口的配置混在一起

| 配置文件 | 需要关注的字段 | 主要使用位置 |
| --- | --- | --- |
| [post_extraction_config.yaml](../configs/post_extraction_config.yaml) | `source_path`、`task_path`、`ai_config_path`、日期 | 初始化、第一轮 |
| [event_processing_config.yaml](../configs/event_processing_config.yaml) | `first_round_task_path`、`result_path`、日期、账号映射、资产名单、历史月数、第二三轮配置路径、权重参数 | 第一轮自动导入，以及事件表后续处理 |
| [round1_ai_config.yaml](../configs/round1_ai_config.yaml) | 提示词、接口、模型、输出额度、并发与重试 | 第一轮 |
| [round2_ai_config.yaml](../configs/round2_ai_config.yaml) | 提示词、接口、模型、并发、就绪请求缓冲 | 第二轮归组 AI |
| [round3_ai_config.yaml](../configs/round3_ai_config.yaml) | 提示词、接口、模型、并发与保存参数 | 第三轮 new/update 分析 |
| [embedding_config.yaml](../configs/embedding_config.yaml) | `base_url`、密钥、模型版本、批大小、相似度阈值 | 第二轮默认向量检索 |
| `configs/.env` 或进程环境 | `ROUND1_AI_API_KEY`、`ROUND2_AI_API_KEY`、`ROUND3_AI_API_KEY`、`EMBEDDING_API_KEY` | 三轮 AI 分别鉴权，Embedding 独立鉴权 |

路径与范围规则：

- 源帖子表、第一轮任务表、事件去重分析表按用途填写不同路径；`weight_path` 是另一个用途的基础路径。
- 通常 `post_extraction_config.yaml.task_path` 与 `event_processing_config.yaml.first_round_task_path` 指向同一张第一轮任务表。
- 第一轮自动导入会使用第一轮实际的任务表和日期；**不会把该日期写回事件配置**。第二、三轮仍须单独核对事件配置日期。
- 任务配置中相对 AI / Embedding 配置路径按配置所在目录解析，例如 `round2_ai_config_path: "round2_ai_config.yaml"`；数据系统路径直接交给 `xdata`，不是本地文件路径。
- 日期包含起止日全天。允许全范围的入口需把两端都设为 `null`，不要只清空一端，也不要写成 `""`。
- `.env` 从所加载 AI / Embedding 配置的同目录读取，进程环境优先。改了 `.env` 但仍用旧密钥时，检查终端是否已有同名环境变量。

**参数提醒：** 三轮主命令、初始化、独立导入、LINK 专用重置和信号诊断没有命令行参数解析。不要给它们追加 `--help`、`--dry-run`、`--config` 来试运行或切换配置；这些参数不会提供相应保护或覆盖，程序仍会进入实际入口。只有本文明确列出选项的命令才支持相应选项。

### 2.3 三轮 AI 都要核对的配置

- [ ] `api.url` 是完整 POST 地址；程序直接请求它，不自动追加聊天接口后缀。
- [ ] 三轮 `api.headers.Authorization` 分别引用的 `${ROUND1_AI_API_KEY}`、`${ROUND2_AI_API_KEY}`、`${ROUND3_AI_API_KEY}` 已填写，各轮 `run.concurrency` 已按对应服务配置。
- [ ] `api.payload.model`、厂商专有参数、输出额度都与服务匹配；`stream` 保持 `false`。
- [ ] 接口返回项目支持的聊天响应结构：`choices` 中有唯一的整数 `index: 0`，该项 `message.role` 为 `assistant`、`message.content` 为正文字符串；程序按编号选择，不按列表位置猜测。正文还需满足本轮 JSON 要求。
- [ ] `prompts.system`、`prompts.user` 和 `api.payload.messages` 配套。当前 user 为 `{{task.text}}`，不要随意改掉模板引用。
- [ ] 核对 `api.trust_env`：当前均为 `false`，不会使用环境代理；确实依赖环境代理时才改为 `true`。
- [ ] 检查 `run.concurrency`、`requests_per_second`、`timeout_seconds` 和 `max_attempts`，使其符合接口额度和延迟。

当前运行参数如下，修改后以各轮 YAML 为准：

| 参数 | 第一轮 | 第二轮 | 第三轮 | 提醒 |
| --- | --- | --- | --- | --- |
| `concurrency` | 100 | 5 | 10 | 第一轮接入前尤其要核对限流额度 |
| `requests_per_second` | 0 | 0 | 0 | 0 表示不主动限制起发速率，不表示不发请求 |
| `timeout_seconds` | 120 | 300 | 120 | 与服务实际响应耗时配套 |
| `max_attempts` | 10 | 10 | 10 | 包含第一次请求，网络、429、5xx、无效输出共用预算 |
| `batch_size` | 200 | 未显式配置，默认 200 | 200 | 第二轮结果即时合批保存；不要将它理解为每批 AI 请求数 |
| `result_buffer_size` | 200 | 未显式配置，默认 200 | 200 | 不得小于 `batch_size` |
| `ready_request_buffer_size` | 默认 20 | 20 | 默认 20 | 主要用于第二轮提前准备请求 |

**改提示词或模型前先检查已有结果。** 三轮的 `config_hash` 由原始提示词和整个 `api.payload` 计算；模型、temperature、输出 token 限制、厂商参数等变化都会改变摘要。要认领新任务时，会核对对应表内本轮的全表摘要，范围外的不同摘要也会阻止继续。并发、重试、URL、认证不参与该摘要；不参与摘要也不意味着更换服务后的语义质量相同。

重跑只会自动处理未完成任务，不会重算已成功结果。重置状态不会清除配置摘要；缩小日期也不能绕过全表摘要冲突。若确实要更换处理版本，应先确定独立目标表或完整的数据迁移、下游重算方案。

## 3. 初始化第一轮任务表：只在首次建表时运行

```powershell
& $py -X utf8 -m entrypoints.initialize_tasks
```

运行前检查 [post_extraction_config.yaml](../configs/post_extraction_config.yaml)：

- [ ] `source_path` 是真实原始帖子表，`task_path` 是尚未注册的新目标，两者不同。
- [ ] 源数据有唯一的 `(date, id)` 双层索引，`id` 为非空字符串且不重复，`text`、`user_name` 为字符串列。
- [ ] 当前客户端可读取源表，并能注册、写入任务表。
- [ ] 已确认要读取**全部源帖子**；这里的 `start_time/end_time` 不生效，不调用 AI，也不读取 AI 配置。

此入口不是日常增量追加入口。注册成功但写入失败，目标可能已经存在；不要直接再次运行。先按真实客户端规则处理原目标，或改用尚未注册的新目标。只清空数据不代表可以重复注册同一路径。

## 4. 第一轮：事件提取并自动导入

```powershell
& $py -X utf8 -m entrypoints.run_round1
```

### 运行前必须核对

- [ ] `post_extraction_config.yaml.task_path` 已初始化；本命令读取该任务表，不直接读取 `source_path`。
- [ ] 第一轮 `start_time/end_time` 是本次范围；均为 `null` 时会处理全表。
- [ ] `ai_config_path` 指向要使用的第一轮配置，接口、密钥、模型、模板、输出额度已核对。
- [ ] 尤其核对当前 `concurrency: 100` 和 `max_completion_tokens: 512`：前者涉及接口压力，后者可能不足以容纳长帖或多事件输出。
- [ ] `event_processing_config.yaml.result_path` 是本次自动导入目标，不能等于第一轮任务表。
- [ ] `event_processing_config.yaml.user_name_to_tickers` 与源帖 `user_name` 匹配；程序忽略用户名大小写，账号币种只是辅助背景。
- [ ] 事件配置本身能正常加载，即使第一轮会覆盖导入来源和日期，也不要留下不合法字段或半边日期。
- [ ] 没有通过改变提示词、模型或 payload 参数与表内旧摘要发生冲突。

第一轮提示词须要求正文恰好为 `{"events": [{"event": "完整事件文字", "tickers": ["BTC"]}]}` 这样的结构：顶层只有 `events` 数组，每个事件只有 `event` 和 `tickers`。不能用解释文字、缺失字段或 `tickers: null` 代替。

### 跑完要确认

第一轮会跳过启动时已成功的帖子，恢复并处理未完成任务。正常返回后，自动把**本次范围内所有成功帖**导入事件表，包括本次新成功及原先已成功的帖子；第二轮不会自动启动。

检查第一轮失败数，以及“自动导入完成”日志。普通单帖失败不阻止其他成功帖导入；整体异常或人工中断不会进入自动导入。自动导入失败时已保存的第一轮回答保留，修正问题后同范围重跑可以补导。没有成功帖时不会创建事件表。

第一轮成功不代表一定有事件行：`events: []` 或事件的 `tickers: []` 可以合法地产生零行；导入不按账号映射补造币种。更改账号映射也不会自动重算历史成功帖。

## 独立向量计算：第一轮完成后、第二轮之前

```powershell
python -X utf8 -m entrypoints.prepare_vectors
```

确认同范围第一轮已全部成功并完成导入。本步骤按事件配置的日期和资产范围计算、复用并保存向量，不执行归组。向量服务的地址、密钥、批量和重试参数在此检查；第二轮不调用向量服务。三轮 AI、向量请求和写入默认最多尝试 10 次，含首次，沿用原有退避和写入重试。

## 5. 第二轮：事件去重归组，重点检查历史和向量

```powershell
& $py -X utf8 -m entrypoints.run_round2
```

### 运行前必须核对

- [ ] `event_processing_config.yaml.result_path` 已存在，第一轮成功结果已导入。
- [ ] **事件配置的** `start_time/end_time` 都已填写；不会沿用第一轮配置日期，也不允许两端均空。
- [ ] `round2_asset_list_path` 指向本次资产名单，客户端能读取 JSON 的 `InstrumentList`。每项取第一个 `.` 前的部分、去首尾空白、转大写并去重，再与表内 `asset` 匹配；确认表内资产值也符合预期。
- [ ] 若有意处理全资产，将 `round2_asset_list_path` 设为 `""`；这会同时影响第三轮和相似度观察。
- [ ] `history_lookback_months` 符合本次口径，当前为 3 个日历月；旧键 `history_lookback_years` 不再使用。
- [ ] 所需窗口内同币较早记录已完成归组。第二轮不会自动补跑配置范围外的前史。
- [ ] 已成功前史的 event_id 直接指向同币、更早的 new 组起点；发现链式引用时先核查数据，普通续跑不会改写成功结果。
- [ ] 未在更晚同币记录已成功的情况下插入或重跑较早任务；否则会触发“历史补录或历史状态缺口”检查。
- [ ] `round2_ai_config_path` 对应的模型、接口、密钥、提示词、并发、超时和摘要都已核对。

### 默认 Embedding 检索还必须核对

当前 `retriever: ""` 且 `embedding_config_path: "embedding_config.yaml"`，使用内置向量检索：

- [ ] `base_url` 填到服务基础地址，例如 `https://服务地址/v1`；程序追加 `/embeddings`，不要把完整 `/embeddings` 再填一次。
- [ ] `api_key` 所引用的 `EMBEDDING_API_KEY` 已设置；与聊天 AI 密钥分别检查。
- [ ] `model`、`model_version` 对应真实部署。当前为 `bge-m3`、`bge-m3-v1`；同名模型更换权重时更新版本。
- [ ] 服务返回归一化单位向量、维度一致；程序直接用内积，不再替服务归一化，单位范数容差为 `1e-5`。
- [ ] `batch_size`、`timeout_seconds`、`max_attempts`、`trust_env` 适合向量服务。当前每批 8 条、超时 120 秒、最多 10 次、`trust_env: false`。
- [ ] `similarity_threshold` 是本次要用的阈值，当前为 `0.50`，需要结合真实样本确认。
- [ ] 若人工修改过 `event`，已处理对应旧向量及其下游重算范围；不能期待系统按正文变化自动识别旧向量失效。

如改用 `retriever: "模块:函数"`，先确认该模块在当前环境可导入、实现符合检索约定；自定义检索优先于内置 Embedding，后面的相似度只读复算工具也不支持它。

第二轮提示词须保持三个输出字段 `event_relation`、`matched_candidate_id`、`new_information`。关系允许 `new/update/repeat/uncertain`；`update/repeat` 的匹配编号必须来自本次请求的 `E1` 等候选，不能返回数据库真实组 ID。`update` 必须有非空新增信息，`repeat` 的新增信息为 `null`；`new/uncertain` 的匹配编号和新增信息均为 `null`。

### 跑完要确认

第二轮仅处理目标日期、目标资产，逐币按时间推进。每条事件只检索自身发布时间之前、历史窗口内的同币披露；默认按组最高分筛选达到阈值的最多 10 组。没有历史或没有候选达标时直接判 `new`，不调用归组 AI，但仍需正常的第二轮配置。

检查成功、失败、剩余未处理和暂停币种。普通单条失败会暂停该币后续归组，其他币可以继续；**进程正常退出不等于所有记录均成功**。向量缺失或损坏时先补齐独立向量阶段，第二轮不会自行生成向量。先补齐必要前史，再继续后续日期。

调高阈值通常增加直接判新，调低通常增加交给 AI 的候选和调用。更改历史月数、阈值或向量版本不会自动重算已成功的分组；这些也不包含在归组 AI 的 `config_hash` 中，仍需自己记录处理口径。

## 6. 第三轮：事件影响分析，重点检查跳过与继承

```powershell
& $py -X utf8 -m entrypoints.run_round3
```

### 运行前必须核对

- [ ] `event_processing_config.yaml.result_path` 是已完成归组的事件表。
- [ ] 事件配置 `start_time/end_time` 都已填写；确认与这次要分析的范围一致。
- [ ] `round2_asset_list_path` 是第三轮也要使用的名单。第三轮复用这个字段，没有单独的第三轮资产名单开关。
- [ ] 明确 `round3_skip_incomplete_round2` 的选择：当前 `true` 跳过第二轮 `0/9/-1`，先分析成功部分；`false` 或省略则要求目标范围第二轮全部完成。
- [ ] 第二轮标成功的记录，其 `event_relation` 和 `event_id` 仍合法。跳过开关不会放过损坏的成功关联或未知状态。
- [ ] 存在待分析的 `new/update` 时，`round3_ai_config_path` 的接口、模型、密钥、提示词、运行参数和摘要已核对。
- [ ] 存在待处理 `repeat` 时，同币、同组、严格更早的披露有可继承的有效第三轮响应；来源可能在本次日期范围外。

### 提示词和输出不能混用旧字段

第三轮 AI 只读取本条记录的 `asset`、完整 `event`，以及非空时的 `new_information`。它不读取原帖或组历史；`new_information` 只辅助理解进展，不能替代完整事件。

AI 正文必须是恰好五个字段的 JSON，值均为非空字符串：

| 字段 | 允许值 |
| --- | --- |
| `direction` | 利好、利空、中性、不确定 |
| `materiality` | 高、中、低、不确定 |
| `execution_stage` | 未开始、执行中、已完成、不确定 |
| `persistence` | 短期、中期、长期、不确定 |
| `materiality_reason` | 非空说明文字 |

不要使用旧版 `certainty`、方向“混合”或额外解释字段，否则会校验失败并消耗原请求重试预算。

### 跑完要确认

- 查看 `skipped_round2`，不能只看第三轮成功数。开启跳过后，补跑第二轮，再运行第三轮才能补上新完成的记录；原先第三轮成功的记录会跳过。
- `new/update` 请求 AI；`repeat` 复制同组严格更早的最近有效第三轮响应，不重新调用模型。只剩 repeat 时不需要加载聊天 AI 配置。
- repeat 找不到有效来源会失败；来源补齐后可重跑。第三轮不会自动为范围外来源补跑 AI。
- `history_lookback_months` 仅控制第二轮，不限制第三轮 repeat 查找历史响应的范围。
- 旧记录若第三轮已经标成功却没有响应，不会自动回填；需要单独确定该记录的修复方案。

## 7. 独立导入历史第一轮结果

```powershell
& $py -X utf8 -m entrypoints.import_events
```

运行前检查事件配置：

- [ ] `first_round_task_path` 是本次真实导入来源，`result_path` 是目标，二者不同。
- [ ] `start_time/end_time` 是导入范围；两端为 `null` 时读取全范围。
- [ ] 范围内第一轮帖子全部成功，且回答满足事件 JSON 规则。独立命令默认不会像第一轮自动导入那样只取成功部分。
- [ ] 已有目标的固定输入与本次展开结果一致，没有人工改动导致冲突。

此入口不调用 AI。明确缺表时自动注册并首次写入，已有目标校验后仅追加缺失行；已有第二、三轮结果保留。历史回答中的部分未转义引号会尝试在内存修复，截断或字段缺失等问题仍会在写入前报错。日常第一轮自动导入成功后，通常不必额外运行此命令。

## 8. 相似度只读观察

```powershell
& $py -X utf8 -m entrypoints.audit_similarity
```

- [ ] 事件配置 `result_path`、日期、资产名单和 `history_lookback_months` 是要观察的口径。
- [ ] 需要覆盖日期时，同时传 `--start-time YYYY-MM-DD --end-time YYYY-MM-DD`。
- [ ] 用于比较的成功记录已保存有效、维度和版本一致的向量；工具不会补生成或修复向量。
- [ ] 当前采用内置向量检索，未配置自定义 `retriever`。
- [ ] 默认从 Embedding YAML 只读取 `similarity_threshold`，或用 `--threshold` 显式指定；不需要服务地址、密钥或当前 AI 提示词。
- [ ] 核对 `--thresholds` 对照阈值列表、`--top-k` 导出候选数和新的 `--output-dir`；也支持 `--config`。

默认输出到新的 `reports/similarity_audit_北京时间/`。只比较第二轮成功记录，无需等待整轮完成；`--top-k` 只影响导出条数。观察阈值不会修改正式阈值、分组或数据库，也不能完全预测重跑第二轮后的结果。详情见 [相似度观察说明](SIMILARITY_AUDIT.md)。

## 9. 小时权重和诊断

### 9.1 权重参数预览

```powershell
& $py -X utf8 -m entrypoints.write_weight_data --dry-run
```

- [ ] 事件配置 `result_path`、`weight_path` 正确，`weight_path` 已填写为基础路径。
- [ ] 核对 `weight_grid.coefficients` 的 B/C/D 和各字段枚举映射候选数组。
- [ ] 核对 `weight_grid.aggregation_methods`：`[sum]` 求和，`[max_abs]` 取同资产同小时绝对值最大的原信号，`[sum, max_abs]` 分别生成独立表。
- [ ] 核对组合数、`max_combinations`、`table_name_template` 和每个完整目标表名；模板必须包含 `{aggregation_method}` 和 `{param_id}`，默认目标为 `{weight_path}/{aggregation_method}__B{B}_C{C}_D{D}__{param_id}`。

此预览不连接数据库、不创建权重表、不生成运行清单，也不要求填写日期。它不能替代实际写入前的日期、数据和权限检查。可用 `--config` 指向其他事件配置。

### 9.2 实际生成或更新权重表

```powershell
& $py -X utf8 -m entrypoints.write_weight_data
```

- [ ] 已完成上一节预览，所有目标路径和参数组合正确。
- [ ] 事件配置 `start_time/end_time` 都已填写，且按源时间轴填写；程序为首尾完整窗口补读相邻日，输出延伸到结束日次日 `01:00`（写库后 `09:00`）。
- [ ] 检查第二、三轮均成功的记录数量；未完成记录会跳过，若有意要求完整样本，应先补齐三轮。
- [ ] 成功记录都有可解析的第三轮响应，评分字段有正确映射。当前 YAML 中“重要性 / 执行阶段=不确定”映射为 2，不能想当然按程序缺省值理解。
- [ ] 明白 `round2_asset_list_path` 不筛选此入口；其他资产已有的成功结果也可能进入权重。
- [ ] 明白每个有效参数组合对应一张表；重复运行会对齐并替换重叠小时、追加新小时，可能更新已有数据。
- [ ] 可用 `--report-dir` 指定尚不存在的新运行清单目录，默认在 `reports/weight_grid/北京时间/`。

当前公式为 `direction × (B × materiality) × (C × execution_stage) × (D × event_relation)`，`persistence` 不参与计分和组合展开。整点 T 聚合 `[T-75分钟, T-15分钟)`，即 `(发布时间 + 15分钟).floor("h") + 1小时`；写库前时间索引再整体加 8 小时，不要预先把配置日期平移一遍。

权重数据写入数据库，本地目录保存参数及执行清单。检查各组合是否 `completed`，不能只看第一张表已写入。详情见 [权重说明](data/WEIGHT_DATA.md)。

### 9.3 核对已有权重表列名

```powershell
& $py -X utf8 -m entrypoints.audit_weight_columns
```

- [ ] 事件配置 `result_path`、`weight_path`、必填日期和 `weight_grid` 与待核对权重一致。
- [ ] 存在多个评分组合时追加 `--param-id "实际参数标识"`；存在多个聚合模式时追加 `--aggregation-method sum` 或 `--aggregation-method max_abs`。标识与模式从 `write_weight_data --dry-run` 中查看。
- [ ] 已理解 `unexpected_columns` 也可能是其他日期批次保留的合法历史资产列。

此入口支持 `--config`，只读数据库，不生成权重、不调用 AI。

### 9.4 信号与事件诊断图：配置在脚本顶部

```powershell
& $py -X utf8 -m entrypoints.plot_signal_diagnostics
```

先修改 [plot_signal_diagnostics.py](../entrypoints/plot_signal_diagnostics.py) 顶部变量：

- [ ] `WEIGHT_PATH` 填写**带参数后缀的完整权重表路径**，不能只填 YAML 中的基础 `weight_path`。
- [ ] `START_TIME/END_TIME` 按已有 weight 表的时间轴填写，不使用事件 YAML 日期自动覆盖。
- [ ] `ASSET_COLUMNS` 使用实际完整合约列名；`None` 表示全部列，不读取第二轮资产名单来筛选。
- [ ] 核对 `WEIGHT_EPSILON`、`TOP_N`、`OUTPUT_DIR` 和 `SHOW`，且已安装 `matplotlib`。
- [ ] 只做 weight 诊断时，`RESULT_PATH` 可留空；需要事件解释时，填写真实结果表路径。
- [ ] 做事件解释时，`SCORING_CONFIG`、`PARAM_ID` 对应生成该 weight 的实际评分参数；多个评分组合必须指定标识，多种聚合模式必须填写 `AGGREGATION_METHOD`。
- [ ] 当前表用 `EVENT_TIME_OFFSET_HOURS = 8`；当前事件对齐采用 v3 的 45 分钟窗口，旧版整点分桶表需使用对应旧版诊断代码。该参数用于事件对齐，已有 weight 本身不再平移。
- [ ] 核对 `CANCELLATION_EPSILON`；它影响完全抵消的诊断判断。

此入口只读数据库、输出图和 CSV，不调用 AI。默认 `reports/signal_diagnostics/` 同名报告会覆盖；`--config` 等参数不会覆盖顶部变量。详情见 [诊断说明](SIGNAL_DIAGNOSTICS.md)。

### 9.4.1 归一化小时持仓的月度收益贡献

```powershell
& $py -X utf8 -m entrypoints.audit_return_contribution
```

- [ ] 修改 [audit_return_contribution.py](../entrypoints/audit_return_contribution.py) 顶部 `WEIGHT_PATH`、`PRICE_PATH`、`START_TIME`、`END_TIME`；`WEIGHT_PATH` 是完整表路径，`PRICE_PATH` 默认 `close_price_60min`。
- [ ] 先取资产列交集，再按当前起点报价过滤 0、负数、NaN 和无穷价格对应的小时权重，最后逐小时归一化；核对终端或 `method.json` 的剔除名单和无效报价统计。
- [ ] 保留方向，有效资产全零的小时保持空仓；两表索引代表相同实际价格时点。
- [ ] 已有时间行中的 NaN 权重按 0 处理，全 NaN 行按空仓计入统计；数量见 `method.json` 的 `nan_weight_cells_filled`。缺少整条时间行仍会停止。
- [ ] 日期包含首尾日，按收益区间起点归属；价格会多读至结束日次日，以取得最后的 `00:00` 端点。
- [ ] 持仓应已在该时点生效；有额外执行延迟或持有规则时，先按回测规则转换。
- [ ] 价格 NaN 用同一资产此前最近的非空值前向填充，填充数量见 `nan_price_cells_filled`；没有历史值的首部 NaN 保留。缺少整个价格时点或持仓小时仍会停止，填充后已持仓资产仍缺有效结束价也会停止。
- [ ] 理解三张月度报告和 `asset_summary.csv` 中的策略贡献为费用前小时加总、不复利链接；资产全期价格回报使用审计首末端点的处理后价格。
- [ ] 同一入口读取数据库并返回四张表；`asset_summary.csv` 的全期平均仓位直接对小时仓位求平均，重仓月按最大月均绝对仓位选择。
- [ ] 核对顶部 `OUTPUT_DIR`、`TOP_N`、`NET_RETURN_EPSILON`；默认输出 `reports/return_contribution/`，同名文件会覆盖。命令行参数可临时覆盖顶部配置。
- [ ] 已安装 matplotlib；顶部 `FIGURE_DIR` 默认是项目根目录，三张 PNG 直接写入该目录，使用 Agg 画布，不弹出窗口。

只读数据库，不调用 AI；详细字段与比例定义见 [收益贡献方案](RETURN_CONTRIBUTION.md)。

### 9.5 标签贡献实验仓位表

```powershell
& $py -X utf8 -m entrypoints.write_label_experiments --dry-run
& $py -X utf8 -m entrypoints.write_label_experiments
```

- [ ] 已填写事件配置的 `result_path`、`weight_path` 和源事件起止日期；核对预览中的完整目标路径。
- [ ] `weight_path` 是 root，地址为 `root/聚合模式__labels__方案名__B…_C…_D…__param_id`；评分标识与 `write_weight_data` 共用，模式和分组由表名区分，旧表不会自动迁移。
- [ ] 已选定一套固定评分；多套候选时用 `--param-id` 指定，实验 B/C/D 固定为 1。
- [ ] 已确认回测如何将原始信号变成下单数量：直接买币，或按资金占比除以价格换算。
- [ ] `--signal-scale`（旧名 `--unit-position`）只是信号倍率，默认 1，不能把 0.01 当作 1% 资金。
- [ ] 理解等级分组固定的是每条事件的信号绝对值；直接按币数下单时，并不保证等金额。
- [ ] 源事件的分组等级（包括 persistence）完整且已配置；方向为零不交易。
- [ ] 按 `tables.csv` 的完整表路径逐组回测，确认 `manifest.json` 中全部方案均完成。

默认生成 8 组固定评分对照及 17 组等级组合，沿用现有小时归属、加 8 小时及续写口径。新入口只生成原始信号，不读取价格或计算收益。真正的等金额研究需按实际下单接口作价格换算。详情见 [标签贡献实验](LABEL_EXPERIMENTS.md)。

## 10. 每轮结束后，再决定下一步

| 看到的情况 | 下一步 |
| --- | --- |
| 第一轮结束 | 同时检查失败数与自动导入日志；没有导入事件表，第二轮就没有对应任务 |
| 第二轮正常退出，但仍有失败或待处理 | 核对暂停币种、前史和错误；不要仅凭退出码认定全部完成 |
| 第三轮结束且 `skipped_round2 > 0` | 补跑第二轮后再跑第三轮；需要完整权重样本时先补齐 |
| HTTP 429 持续出现 | 降低本轮并发或设置每秒请求上限，再按原配置续跑；等待重试不会自动降并发 |
| HTTP 401/403 或其他停止型接口错误 | 先核对地址、密钥、模型权限和代理，再重新运行 |
| JSON 校验失败 | 检查错误原因、字段、枚举和输出截断；调整 payload 也会改变配置摘要 |
| 配置摘要冲突 | 核对提示词和 payload 变化及全表旧摘要；只改日期或重置状态不能解决 |
| 写库失败或人工中断 | 查日志和数据库实际状态，再按原配置续跑；不要默认此前写入已回滚 |

任务状态保存为字符串：`"0"` 待处理、`"9"` 处理中、`"1"` 成功、`"-1"` 失败。三轮正常续跑会按各自规则恢复本轮遗留 `9/-1`，跳过成功结果。**不要为了普通续跑先执行初始化或重置命令。** 第二、三轮的状态恢复可能先于 AI 配置校验，因此它们也不能当作只读配置检查命令。

这些命令互相独立；第一轮不自动启动第二轮，第二轮不自动启动第三轮。若自行编排连跑，应检查每个命令的 `$LASTEXITCODE`，异常退出后停止；即使退出码为 0，也要结合失败数、待处理数和跳过数判断完整性。Windows PowerShell 5.1 不支持 `&&`，不要直接套用其他 Shell 的连跑写法。
