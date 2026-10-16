# AI 帖子三轮处理项目

本项目从帖子提取事件，建立按代币划分的披露记录，关联历史事件，再分析经济影响。项目独立运行，不需要打包或安装本项目；三个轮次分别启动，round1事件提取正常结束后自动导入成功帖。

三个轮次统一称为 **round1事件提取**、**round2事件去重归组**、**round3事件影响分析**。两张表按用途区分：

| 表名 | 数据粒度与用途 | 对应配置 |
| --- | --- | --- |
| round1帖子事件提取表 | 一帖一行，保存原帖输入、提取结果及状态 | `job_config.yaml` 的 `task_path`；独立导入来源为 `event_config.yaml` 的 `first_round_task_path` |
| 事件去重分析表 | 一条事件 × 一个代币一行，共用保存归组结果、向量及影响分析结果 | `event_config.yaml` 的 `result_path` |

本次统一显示名称，现有配置键、路径值和真实数据表路径保留。配置中的旧称呼占位文本也保持原值，填写时以上表为准。“去重归组”通过 new/update/repeat 和事件组表达披露关系，仍保留每条记录。完整职责见 [项目总览](project_overview.md#统一主题与表名)。

事件去重分析表当前为 14 个普通列，第二、三轮 AI 回答分别保存在 round2_ai_response、round3_ai_response 两个 JSON 字符串列，第三轮五个影响字段保留在原始正文内。回答列还保存最后一次响应 usage、接口生成时间、实际请求次数，以及提交数据库前的北京时间 created，不保存推理文本。字段、失败和存储重试口径见 [表结构](docs/data/EVENT_RESULT_TABLE.md#两轮-ai-回答的保存口径)。

## 环境与配置

使用现有 Python 环境安装 `requirements.txt` 中的依赖，并确保运行环境可导入已有数据客户端 `xdata`。仓库中的 `.python38` 用于当前本地验证；正式运行使用已有可访问数据系统的解释器。

```powershell
python -m pip install -r requirements.txt
```

| 文件 | 用途 |
| --- | --- |
| `configs/job_config.yaml` | 源帖子路径、round1帖子事件提取表、日期范围、round1事件提取 AI 配置和日志 |
| `configs/event_config.yaml` | round1帖子事件提取表的来源路径、事件去重分析表的目标路径、日期范围、代币映射、检索和 round2/round3 AI 配置路径 |
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

从项目根目录执行。第一次建立round1帖子事件提取表前，在 `configs/job_config.yaml` 填写源 `source_path` 和尚未创建的 `task_path`，两者必须不同：

```powershell
python -X utf8 -m entrypoints.initialize_tasks
```

round1事件提取初始化完整读取源帖，校验后注册目标并一次写入，不使用处理日期、不读取 AI 配置、不调用 AI。注册成功但写入失败可能留下空目标；须按真实客户端规则处理原目标或使用尚未注册的新目标，再重新初始化。仅清空数据不代表可安全重复注册。

配置好round1事件提取 AI，以及事件配置的 `result_path` 和所需账号映射后，日常依次执行：

```powershell
python -X utf8 main.py
python -X utf8 -m entrypoints.run_round2
python -X utf8 -m entrypoints.run_round3
```

`main.py` 跳过读取时已成功的帖子，处理其余任务；正常返回后，将本范围成功帖从内存导入事件去重分析表。自动导入沿用 job_config 的实际round1帖子事件提取表及范围，无需手工同步两份配置。round2事件去重归组与round3事件影响分析仍分别启动，不会自动串行执行。

当前仓库的表路径仍为占位文本，三轮 AI 接口 URL 和 Embedding 基础地址为空，第二、三轮模型和 system 提示词也为空。第一轮仍使用“简要概括”示例提示词，与程序要求的事件 JSON 不一致，实际提取前必须替换。账号映射已按用户提供的 TradeCoins 50 名单填入；正式运行还须准备认证信息、核对源帖用户名并确定日期范围。event_config.yaml 的日期目前留空，第二、三轮启动前必须填写，未设置会停止；仅独立导入保留日期为空时的全范围处理。第三轮提示词须理解五年窗口的历史截断标记。阻塞项与可后置项见 [接入待办](docs/IMPLEMENTATION_ISSUES.md#运行前必须补齐)。

round1事件提取普通单帖失败不阻止其他成功帖导入；round1事件提取整体异常或中断不进入自动导入。导入失败时round1事件提取结果保留，重新运行同范围可跳过成功帖 AI 并补导。没有成功帖时不创建事件去重分析表。

历史数据独立导入使用：

```powershell
python -X utf8 -m entrypoints.import_events
```

独立导入使用 `configs/event_config.yaml` 的来源和范围，默认要求范围内round1事件提取全部成功；initialize_mode 支持 auto/create/resume，只控制该独立入口。round2事件去重归组与round3事件影响分析按已有任务状态运行，不重新展开round1事件提取，不自动补跑范围外任务。

`configs/event_config.yaml` 已包含用户提供的 50 组账号代币映射，保留原名单大小写及迁移备注。原名单 `screen_name` 对应原帖 `user_name`，`ticker` 转为代币列表。空 tickers 使用账号映射，例如：

```yaml
user_name_to_tickers:
  ethereum: ["ETH"]
  project_account: ["ETH", "LEND"]
```

`tickers: []` 在第一轮属于合法输出，导入时才按原帖 `user_name` 精确查映射；没有映射会报错并停止本次整批导入，其他成功帖的事件本次也不会新增。第一轮已保存结果及成功状态保留，不为无代币事件创建失败行；补齐映射后重跑同一范围的 main.py 可补导。若整个 tickers 字段缺少、为 null 或类型不合法，则第一轮输出校验失败并按请求预算重试。`events: []` 表示没有事件，不查映射。具体例子见 [事件展开规则](docs/rules/EVENT_RULES.md#2-检查round1事件提取并展开记录)。

round2事件去重归组默认使用 `embedding_config_path: "embedding_config.yaml"` 指向的 bge-m3 纯向量检索。填写该文件的 base_url，例如 `https://服务地址/v1`，程序追加 `/embeddings`；把密钥填入 `configs/.env` 的 EMBEDDING_API_KEY 或进程环境。默认每批 8 条，只编码 event；生成后保存到事件去重分析表的 embedding 列，JSON 包含 model、model_version、config_hash 和 vector，重启复用。config_hash 只标识服务、模型、版本和编码规则，不含 event；同名模型更换权重时要修改 model_version，人工修改 event 时须清空该行向量并确定下游重算范围。第二轮只使用 event 和候选历史，不再读取原帖；第三轮保留正文输入。初始化已包含该列，不使用 SQLite。

用户确认模型已输出归一化向量；程序保留返回数值，直接用内积计算余弦相似度，不再归一化或转成 float32。仍校验格式、维度、有限数值、非零向量及单位范数，范数允许绝对误差 1e-5；新响应不满足约定时使用该批剩余请求预算重试，耗尽后停止。

第二轮先按 event_config.yaml 指定日期读取待处理任务，前史校验、候选检索及向量准备共用 history_lookback_years（默认 5 个日历年）。下面仅为范围填写示例，按实际要处理的日期修改：

```yaml
start_time: "2025-01-01"
end_time: "2025-12-31"
history_lookback_years: 5
```

每条披露按自身发布时间 t 检索 `[t-5年, t)`，包含下界、排除同刻与未来。普通历史不读取 5 年下界以前的数据；较晚已完成冲突只读取资产、状态两列，全表 AI 摘要仍只读单列。组起点不在窗口内时不回查旧组，保留已保存的组 ID，提供窗口内进展，first_disclosure=null、history_trimmed=true，并用 first_available_disclosure 提供窗口内最早披露。无法据此核实窗口外起点是否实际存在，第二、三轮提示词都须理解这些截断标记。

第三轮先按配置范围读任务，无任务或全部完成时直接返回。只为未完成的 update 准备同币、同组、过去 5 个日历年内的第二轮已完成历史，new 不补历史。范围内事实复用，范围外按月仅读 asset、event_id、event_relation、event、new_information、round2_status 六列；不读表元数据或遍历全表事件内容。第三轮固定 5 年，不受第二轮 history_lookback_years 调整影响；窗口内有事实但缺起点时标记截断，完全无该组历史时停止，不自动判新。仍保留全表第三轮配置摘要单列核对，已完成分析不自动重算。详细边界见 [第三轮规则](docs/rules/EVENT_RULES.md#7-round3事件影响分析检查调度和保存)。

按事件组最高余弦分数筛选，只将分数大于等于 similarity_threshold 的最多 10 组交round2事件去重归组；无历史或所有组都低于阈值时直接 new，继续round3事件影响分析。默认阈值 0.50 是待真实样本标定的试运行起点，可在 Embedding 配置调整：

```yaml
similarity_threshold: 0.50
```

调高阈值会增加直接判新，调低会增加round2事件去重归组调用；更改只影响待处理记录，不自动重算已完成关联。Embedding 或向量保存失败停止本轮；存储重试不重复请求向量。自定义检索可用 retriever 的 `模块:函数` 覆盖，应用也可直接注入函数。完整格式与失败规则见 [表内向量与阈值方案](docs/plans/EMBEDDING_TABLE_STORAGE_PLAN.md)。

round2事件去重归组与round3事件影响分析正式提示词和真实接口仍待配置。三轮共有的全表按列摘要读取 `common.store.read_stage_hashes` 使用 `cli.read(path, start_time='', end_time='', columns=[hash_column])`，从返回的 DataFrame 中取摘要列原值；首次认领前核对一次，跨月复用，读取失败时停止。

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

各轮专用日志使用完整主题名称，建表、读取及导入日志明确表名。公共网络与分段执行日志使用通用任务描述；内部阶段标识 `round1/round2/round3`、异常文本和错误字段继续沿用原值。

退出码继续为成功 0、普通异常 1、用户中断 130。round1事件提取初始化保留具体错误和目标核查提示；其他入口保留标准异常栈。停止程序后重启各自入口继续处理，恢复、保存和历史时间边界见 [round1事件提取规则](docs/rules/KEY_RULES.md) 与 [事件规则](docs/rules/EVENT_RULES.md)。

VS Code 打开项目根目录并选用已安装依赖和 xdata 的解释器。打开 `entrypoints/initialize_tasks.py` 等入口，使用 **Python Debugger: Debug Python File**；现有 `.vscode/launch.json` 为当前文件提供根目录导入路径和 UTF-8 设置，无需安装项目。

## 验证和文档

模拟测试按公共能力、初始化、轮次和入口分组，公共模拟客户端在 tests/support.py 与 tests/event_support.py。2026-10-16 按列摘要接口接入后，完整 267 项测试通过；包含三轮摘要列查询、跨月复用与读取失败停止，以及 23 项表内向量与阈值测试中的模型输出原值保存及复用、第二轮不读原文、共用网络重试及 1024 维相似度。测试不访问真实 AI 或数据库：

```powershell
python -X utf8 -B -m unittest discover -s tests -t . -q
```

当前本地验证可使用 `.\.python38\python.exe` 替代 python。真实接口和规模验证事项见 [待办](docs/IMPLEMENTATION_ISSUES.md)，模块职责和总体数据流见 [项目总览](project_overview.md)，数据结构见 [round1帖子事件提取表](docs/data/TABLE_DESIGN.md) 和 [事件去重分析表](docs/data/EVENT_RESULT_TABLE.md)。历史改造记录保留在 docs/history，研究计划位于 docs/plans。
