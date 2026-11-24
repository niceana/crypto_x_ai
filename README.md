# AI 帖子三轮处理项目

本项目从帖子提取事件，建立按代币划分的披露记录，关联历史事件，再分析经济影响。原脚本可独立运行，无需安装本项目；处理流程为第一轮提取及导入、独立向量计算、第二轮归组、第三轮分析，各环节独立启动。本分支另外提供可安装的实盘函数封装，见下方接入说明。

媒体账号使用独立的三轮提示词、配置和两张处理表，复用现有执行框架；按同对象一个月全部历史组归组，不运行向量准备，支持加密资产、`INDUSTRY`、`MACRO`。配置、初始化、独立补导及运行命令见 [媒体处理说明](docs/MEDIA_PIPELINE.md)。下文原入口和配置保持官方账号流程。

运行每条命令前，请先看 [命令运行前配置检查清单](docs/RUN_COMMAND_CHECKLIST.md)：逐项核对三轮 AI 的表路径、日期、接口、资产范围、历史与向量配置，以及权重命令的影响范围。

三个轮次统一称为 **round1事件提取**、**round2事件去重归组**、**round3事件影响分析**。两张表按用途区分：

| 表名 | 数据粒度与用途 | 对应配置 |
| --- | --- | --- |
| round1帖子事件提取表 | 一帖一行，保存原帖输入、提取结果及状态 | `post_extraction_config.yaml` 的 `task_path`；独立导入来源为 `event_processing_config.yaml` 的 `first_round_task_path` |
| 事件去重分析表 | 一条事件 × 一个代币一行，共用保存归组结果、向量及影响分析结果 | `event_processing_config.yaml` 的 `result_path` |

本次统一显示名称，现有配置键、路径值和真实数据表路径保留。配置中的旧称呼占位文本也保持原值，填写时以上表为准。“去重归组”通过 new/update/repeat 和事件组表达披露关系，仍保留每条记录。完整职责见 [项目总览](project_overview.md#统一主题与表名)。

事件去重分析表当前为 14 个普通列，第二、三轮 AI 回答分别保存在 round2_ai_response、round3_ai_response 两个 JSON 字符串列，第三轮五个影响字段保留在原始正文内。回答列还保存最后一次响应 usage、接口生成时间、实际请求次数，以及提交数据库前的北京时间 created，不保存推理文本。字段、失败和存储重试口径见 [表结构](docs/data/EVENT_RESULT_TABLE.md#两轮-ai-回答的保存口径)。

实盘包 0.3.0 的输出在旧字段上增加调用记录：第一轮增加 round1_call_meta（共 10 列），事件表增加 round2_call_meta、round3_call_meta（共 16 列），记录时间、次数及本轮重试的失败原因。旧 9/14 列输入兼容；宿主保存新增列。原独立脚本在旧表上继续使用旧结构，AI 提示词、判断与 weight 算法沿用原实现。字段见 [实盘调用记录](docs/LIVE_INTEGRATION.md#调用记录030)。

## 实盘系统接入提醒

本分支提供 `from crypto_x_ai_live import run_live_batch` 内存函数接口，异步接入、超时与安装说明见 [实盘函数接入](docs/LIVE_INTEGRATION.md)。原独立脚本仍可使用。宿主运行要求：

0.4.3 同步事件导入时间限制：第一轮结束后，只追加严格晚于已有事件表全表最大 `date` 的事件，不补较早或同刻任务。实盘以内存历史表的 UTC 发布时间为截止，所有资产共用，保留完整时间精度，跳过数量记日志；空历史首次导入不受限制。同一批符合条件的事件一起处理，第一轮输出仍保留全部本批回答。独立脚本的自动和独立导入使用相同限制。

Linux 试跑、数据导出、240 小时连续回放和日志说明见 [试跑工具说明](docs/live_trial/README.md)。调用脚本源码位于 `tools/live_trial/`，打包入口为 `tools/build_linux_trial_zip.py`，专项测试位于 `tests/live_trial/`。`reports/` 只保留运行报告、模拟输出和安装验证副本，不再存放需要维护的试跑源码。

当前交付文件及历史构建归档见 [发行文件说明](docs/DISTRIBUTIONS.md)。`dist/` 根目录保留当前版本交付物，功能补充包按需生成到 `dist/supplements/`，旧构建归档到 `reports/dist_archive/`。

实盘输入的 date 是爬虫时间，created_at 是原帖发布时间；函数内部从 created_at 构造旧三轮的发布时间索引，weight 按原帖时间加 8 小时筛选。当前时间仅自动获取。0.4.0 起必须 await 调用，成功返回 status 加三张表；失败或超时仅返回状态，不回传中间表，下一轮合并原帖从头重算。

实盘包 0.3.1 起，weight.date 在三轮和聚合完成、返回前最后填入，使用此时重新取得的 UTC+8 系统时间；筛选窗口仍按本批开始时的 15 分钟刻度确定。

0.4.2 直接复用原三轮执行器，保留准备线程、网络线程、协调线程的批次提交分工及原有就绪/完成队列；宿主通过 async 接口等待完成。统一停止信号会通知内部退出，并等所有线程结束后才返回失败状态，取消不会只停止外层等待。全批默认计算预算 840 秒，收尾与写回仍需预留时间；同进程不能强杀底层阻塞。已移除 0.4.0 的 stages.py、async_io.py 另一套调度层。向量编码器直接使用原生异步 HTTP，在协调线程的事件循环运行，不保留客户端桥接或同步兼容接口。

> 宿主应保证下次调用函数前，旧进程已经结束，前一批未结束时不要并发启动后一批。要求重启前，Redis里的数据已更新进xdata，读取的时候已经是更新后的xdata。

## 环境与配置

本项目使用 Python 3.8.5。当前机器的整机解释器为 `C:\Program Files\Python38\python.exe`，项目隔离环境为 `.python38\venv\Scripts\python.exe`，VS Code 默认解释器也指向该环境。本地验证使用项目隔离环境；正式读库仍需可导入已有数据客户端 `xdata` 的 Python 3.8.5 环境。

```powershell
.\.python38\venv\Scripts\python.exe -m pip install -r requirements.txt
.\.python38\venv\Scripts\python.exe -X utf8 -m unittest discover -s tests -t .
```

下文命令中的 `python` 均应使用 Python 3.8.5；本机可替换为 `.\.python38\venv\Scripts\python.exe`，不依赖系统 PATH 的默认版本。

三轮 AI 请求遇到 HTTP 429 时会在本次运行中暂停后重试，全部后续起发共同等待本地指数随机退避和有效 `Retry-After` 中的较长时间；缺少有效响应头时使用本地退避。429 与网络故障、5xx、无效输出共用 `run.max_attempts`，默认 10 次且包含首次；最后仍为 429 时保存失败并停止，避免无限重试。独立向量计算请求也在其独立批次预算内重试 429。持续限流时应降低 `concurrency` 或设置 `requests_per_second`；等待重试不会自动调整并发配置。

| 文件 | 用途 |
| --- | --- |
| `configs/post_extraction_config.yaml` | 帖子提取任务：源帖子路径、round1帖子事件提取表、日期范围、round1事件提取 AI 配置和日志 |
| `configs/event_processing_config.yaml` | 事件导入、第二三轮及小时权重处理：来源表、事件去重分析表、权重表、日期范围、代币映射、检索、round2/round3 AI 配置路径和日志 |
| `configs/round1_ai_config.yaml` | round1事件提取提示词、AI 接口、请求及运行参数 |
| `configs/round2_ai_config.yaml` | round2事件去重归组提示词、接口、请求及运行参数 |
| `configs/round3_ai_config.yaml` | round3事件影响分析提示词、接口、请求及运行参数 |
| `configs/embedding_config.yaml` | round2事件去重归组使用的 bge-m3 地址、模型、每批条数、重试及相似度阈值 |
| 进程环境或 `configs/.env` | ROUND1_AI_API_KEY、ROUND2_AI_API_KEY、ROUND3_AI_API_KEY、EMBEDDING_API_KEY，参考 `configs/.env.example` |

入口按脚本位置读取项目的 `configs`，不依赖当前工作目录。两份任务配置中的相对 AI 配置路径按配置所在目录解析，例如round1事件提取填写 `ai_config_path: "round1_ai_config.yaml"`；绝对路径继续支持。数据系统路径原样传给客户端。

`.env` 继续与 AI 配置放在同一目录，进程环境优先。因此使用本地文件时放在 `configs/.env`；不要只移动 YAML 而留下原根目录 `.env`。自定义绝对 AI 配置路径时，仍查找该文件同目录的 `.env`。

三轮分别引用上述三个 AI 密钥变量，各自在对应 YAML 的 `run.concurrency` 设置 HTTP 并发上限，当前分别为 100、5、10；并行启动后相互独立，不共用并发名额。已有 `AI_API_KEY` 不会自动转成三个新变量，运行前需分别填写。独立向量阶段继续使用 `EMBEDDING_API_KEY`。

三轮共用独立请求准备与网络就绪队列，`run.ready_request_buffer_size` 当前均为 20，限制已编码、尚未发送的备用请求。网络执行器启动前必须绑定就绪队列，全部请求统一从该队列取走后起发。第一、三轮由主线程按 `batch_size` 提前认领并交出固定输入，待准备不足一批时补充下一批；准备线程取走即补货，主线程保存结果或认领下一批时，已认领请求仍可继续发送。`concurrency` 不再限制未结束任务总数，退避期间也可发送其他就绪任务；429 全局暂停仍生效。备用输入耗尽后仍需等待认领写库，不能保证任何写库时长下都满并发。完成结果统一进入允许积压的无界队列，正常处理和异常收尾都从该队列收取；`result_buffer_size` 只约束当前提交缓冲，不限制全部待写库结果；批量、定时保存和按月确认规则沿用。

请求模板渲染和 UTF-8 JSON 编码由准备线程统一完成；网络线程只发送就绪队列中已编码的正文，重试复用相同字节。构造失败在准备阶段交付单条失败结果，不发送 HTTP。

起止日期使用带引号的 `YYYY-MM-DD`，包含起止日全天。第二、三轮必须同时填写起止日期；round1事件提取和独立导入仍允许两端同时为空。第一轮设置范围时按自然月分段，第三轮始终按配置范围分段。

```yaml
start_time: "2021-01-01"
end_time: "2021-12-31"
```

## 独立运行与后台启动

已初始化第一轮任务表后，分别运行第一轮、独立向量准备、第二轮、第三轮。向量准备和第二轮入口检查其配置范围内第一轮是否全部成功；还须确认成功帖已经导入事件表。第二轮只载入已保存向量，保留候选矩阵缓存，检索时间范围、组内最高分、阈值和排序不变。

第一轮日期取帖子配置，向量及第二、三轮日期取事件配置。各入口重新执行时按数据库状态复用成功结果；配置摘要和向量版本检查保留。Linux 后台命令、分阶段交接、续跑与扩展年份说明见 [独立阶段运行说明](docs/INDEPENDENT_STAGES.md)。

各轮、向量及导入入口自动使用本机进程锁：同一阶段、同一表不能重复启动，日期不同也会拦截；不同阶段仍可并行。重复启动退出码为 2，正常退出或强杀后自动释放。首次更新后须先停掉旧进程再重启；锁目录及适用范围见 [进程锁说明](docs/INDEPENDENT_STAGES.md#防止重复启动)。

## 初始化和日常运行

从本地 JSONL 向现有原始帖子表补录时，修改 `entrypoints/supplement_posts.py` 顶部的 `SOURCE_PATH`、`JSON_FILES` 和字段映射，先生成备份和预览：

```powershell
python -X utf8 -m entrypoints.supplement_posts preview
```

JSONL 放在项目根目录时，顶部填写 `JSON_FILES = ["_ALL_new.jsonl"]`；相对路径统一以脚本所在项目的根目录为基准，不受启动时工作目录影响，绝对路径也支持。文件改名时只需修改该文件名。

`preview` 先通过 `cli.read` 完整读取现有原始帖子表，把未改动的原始 df 备份到本地 `reports/supplement_posts/<北京时间戳>/original.pkl`，并读回核对索引、值、类型及时间精度；备份成功后才逐行读取全部 JSONL，校验并构造 `(date, id)` 索引，对比原表，保存固定的续表 df。省略子命令也只执行预览，不写数据库。备份或后续检查失败，不生成可执行的完整计划。

预览目录包含：

| 文件 | 用途 |
| --- | --- |
| `original.pkl` | 完整原表备份，保留原 df 的索引、类型和数据；可用 `pd.read_pickle` 读取 |
| `incoming_posts.csv` | JSONL 文件中经过校验和去重的全部候选帖子 |
| `new_posts.csv` | 原表中不存在的 `(date, id)`，供查看待补录内容 |
| `new_posts.pkl` | 与清单对应的最终续表 df，供 `apply` 使用，保留 datetime 和文本类型 |
| `overlap_comparison.csv` | 复合索引重叠的帖子，`source=original/incoming` 分别显示原表和候选记录 |
| `original_only.csv` | 完整原表有、候选 df 没有的帖子，仅供核对 |
| `id_date_conflicts.csv` | 同 ID 不同发布时间的双方记录；存在冲突时禁止 apply |
| `plan.json` | 本次表路径、JSON 来源、字段映射、时间处理规则、各类数量及两个 pkl 文件的校验值 |

CSV 按发布时间、账号、ID 排序，包含完整正文；重叠索引的两份记录相邻展示。日志和 `plan.json` 统计完整原表数量、JSON 总数、去重数量、重叠数量、重叠内容不同数量、待补录数量和 ID 日期冲突数量。内容不同的重叠行仍保留原表内容，只新增不存在的索引。`new_posts.csv` 按新复合索引列出，若其中包含同 ID 不同日期的冲突，须先修正数据并重新预览。

查看清单后，使用终端打印出的具体预览目录执行：

```powershell
python -X utf8 -m entrypoints.supplement_posts apply "reports/supplement_posts/<预览目录>"
```

`apply` 先核对备份及续表 pkl 的校验值，再完整读取原表，确认与本次备份一致、待补录 df 没有重叠索引或 ID，才调用一次 `cli.update_by_index`。原表在预览后新增、删除、修改记录或改变类型时停止，须重新预览和查看清单。执行使用本次保存的 `new_posts.pkl`，不会重新读取 JSON 或使用修改后的顶部配置；CSV 供查看，编辑 CSV 不会改变写入内容。成功后生成 `apply_result.json`，同一计划禁止重复执行。不会注册新表或生成 HTML。

输入仅使用 JSONL，每行一个帖子对象，跳过空白行；格式或字段错误会报出文件名和实际行号。例如文件中的一行：

```json
{"id": "1999999999999999999", "created_at": "Mon Sep 21 15:07:13 +0000 2026", "text": "帖子正文", "user": {"id": "867100084248469505", "screen_name": "alice"}, "retweet_count": 34, "favorite_count": 246}
```

`JSON_FILES` 支持一个或多个本地 JSONL 文件路径，默认读取项目根目录的 `_ALL_new.jsonl`。默认对应原表的 8 个字段，`date/id` 为双层索引，其余 6 个为普通列：

| 原表字段 | JSONL 来源 | 处理 |
| --- | --- | --- |
| `date` | `created_at` | 解析为无时区 datetime 索引，保留原时分秒 |
| `id` | `id` | 帖子 ID 字符串，作为索引 |
| `user_id` | `user.id` | 用户 ID 字符串 |
| `user_name` | `user.screen_name` | 保留原账号文本 |
| `text` | `text` | 保留原正文 |
| `created_at` | `created_at` | 保留 JSON 原始发布时间字符串 |
| `retweet_count` | `retweet_count` | 转发数转为文本，如 `"34"` |
| `like_count` | `favorite_count` | 点赞数转为文本，如 `"246"`，改为原表列名 |

字段名不同可修改 `JSON_FIELD_MAP`，其键为原表字段名，值为每行帖子对象中的字段路径，例如 `user.screen_name`。使用 `created_at` 发布时间，不使用 `fetched_at` 采集时间。映射中的普通列须与原表一致，否则写入前报错，不自动填空。帖子 ID 和用户 ID 的整数在创建 df 前精确转为字符串，拒绝浮点数、布尔值及空值。

调用 `cli.update_by_index` 前，最终 df 的 `date` 索引保持无时区 datetime 类型，`id` 索引及所有普通列统一为文本，包括 `retweet_count`、`like_count`。若原表也保留普通 `date` 列，该列同样保持无时区 datetime。时间精度保留。JSON 的 `created_at` 文本若带 `+0000`、`Z` 等时区标记，生成 `date` 时只移除时区，保留原时分秒，不换算为其他时区；`created_at` 普通列仍保留原始文本。原表的 `date` 应无时区，实际读取若带时区则在备份后报错，须核对源表。

JSONL 作为已经筛选好的最终输入，全部记录参与对比，不再按起止日期、账号或 `RT @` / `JUST IN` 等正文前缀筛选；原始正文及账号显示值保留。

相同 `(date, id)` 且内容一致的 JSON 记录合并；JSON 内部同索引内容冲突，或同一 ID 对应不同日期时，预览在完整备份后停止并报错。候选 df 与原表出现同 ID 不同日期时，会导出冲突清单并禁止 apply。原表已存在的索引直接跳过，不覆盖旧内容；补录后重新 preview 会把这些索引识别为重叠。

原始表补录成功后，已有第一轮任务表需要另外追加任务。在 `configs/post_extraction_config.yaml` 中，`source_path` 指向补入成功的原始帖子表，`task_path` 沿用已有第一轮任务表；使用上次 `supplement_posts apply` 的同一个目录：

```bash
python -X utf8 -m entrypoints.append_tasks preview "上次补录目录"
python -X utf8 -m entrypoints.append_tasks apply "上次补录目录"
```

第一个命令只预览，生成目录内的 `round1_tasks_to_append.csv`；查看后执行第二个命令。入口核对 `apply_result.json` 和 `new_posts.pkl`，从原表确认这批帖子的索引、正文、账号已经落库，再只向任务表追加缺失索引。新任务 `status="0"`，配置摘要及 AI 输出为空；已有状态、配置摘要和输出保留。存在同 ID 不同 date 或共同索引输入不一致时停止。实际追加前完整备份原任务表到该目录的 `round1_backups/<时间戳>/tasks.pkl`，读回核对成功后才调用 `cli.update_by_index`。重复运行跳过已存在任务，不注册新表、不调用 AI；入口与第一轮共用任务表的进程锁。

追加日志会给出本批实际 `start_time/end_time`。把这段日期填写到帖子配置和事件配置中，事件配置的 `first_round_task_path` 与帖子配置的 `task_path` 一致，`result_path` 沿用已有事件表。这里的日期控制 AI 处理范围，范围内已有成功记录会跳过，其他未完成任务也会参与处理。按下方四个命令依次执行：第一轮提取及自动导入、向量准备、第二轮、第三轮；第一轮全部成功且导入完成后再准备向量。

从项目根目录执行。第一次建立round1帖子事件提取表前，在 `configs/post_extraction_config.yaml` 填写源 `source_path` 和尚未创建的 `task_path`，两者必须不同：

```powershell
python -X utf8 -m entrypoints.initialize_tasks
```

round1事件提取初始化完整读取源帖，校验后注册目标并一次写入，不使用处理日期、不读取 AI 配置、不调用 AI。注册成功但写入失败可能留下空目标；须按真实客户端规则处理原目标或使用尚未注册的新目标，再重新初始化。仅清空数据不代表可安全重复注册。

配置好round1事件提取 AI，以及事件配置的 `result_path` 和所需账号映射后，日常依次执行：

```powershell
python -X utf8 -m entrypoints.run_round1
python -X utf8 -m entrypoints.prepare_vectors
python -X utf8 -m entrypoints.run_round2
python -X utf8 -m entrypoints.run_round3
```

`entrypoints.run_round1` 跳过读取时已成功的帖子，处理其余任务；正常返回后，将本范围成功帖从内存导入事件去重分析表。自动导入沿用 post_extraction_config 的实际round1帖子事件提取表及范围，无需手工同步两份配置。后续独立向量计算、第二轮、第三轮分别启动。

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

## 审计归一化持仓的月度收益贡献

> **版本备注（2026-11-10）**：本版本的收益贡献审计使用整点价格（当前默认价格表为 `close_price_60min`），服务器上的收益贡献审计代码使用 VWAP 价格。后续修改收益贡献审计相关代码时，须先提醒这一差异，并核对实际价格口径和收益区间对齐。详见 [收益贡献方案中的版本备注](docs/RETURN_CONTRIBUTION.md#版本备注与服务器价格口径)。

使用已有 `weight` 和 `close_price_60min`：先取资产列交集，把已有时间行中的 NaN 权重视为 0，价格 NaN 用该资产此前最近的非空价格前向填充，再过滤剩余无效起点报价并逐小时做 `weight / sum(abs(weight))` 截面归一化，乘随后一小时的价格简单收益，按收益区间起点所属月份加总。没有历史报价的首部 NaN 保留；全零或全 NaN 权重小时保持空仓。剔除名单、NaN 权重置零数量、价格填充数量和无效报价统计写入 `method.json`。仅分析归一化持仓，不提供原值模式。

```powershell
python -X utf8 -m entrypoints.audit_return_contribution
```

先修改 `entrypoints/audit_return_contribution.py` 顶部的 `WEIGHT_PATH`、`PRICE_PATH`、`START_TIME`、`END_TIME`；价格路径默认 `close_price_60min`，输出目录和排名数量也在顶部。命令行参数仍可临时覆盖。输出 `monthly_assets.csv`（归一化平均持仓、资产收益、小时实际贡献和报价覆盖）、`monthly_rankings.csv`（正负前五及完整资产贡献占比）、`monthly_summary.csv`（组合加总收益、最大正负贡献及集中度）和口径文件 `method.json`，默认目录 `reports/return_contribution/`。三张 PNG 图默认直接保存到项目根目录，可用顶部 `FIGURE_DIR` 修改；使用 Agg 画布，服务器无需图形界面，需安装 matplotlib。仅使用相同实际时点的价格；完整月份需要下月月初 `00:00` 价格，命令会多读下一日并保留该端点。缺少整个持仓或价格时点仍会停止；已持仓资产缺有效结束价也会停止，不把未知持仓收益算成零。资产区间边界价格无效时，其价格收益留空。贡献为费用前小时加总，不是复利净值月收益；详细字段、分母、边界和 notebook 用法见 [收益贡献方案](docs/RETURN_CONTRIBUTION.md)。

同一审计入口读取数据库的 weight 与价格后，一起返回并导出四张表：`monthly_assets.csv`、`monthly_rankings.csv`、`monthly_summary.csv`、`asset_summary.csv`。资产汇总每资产一行，直接使用本次小时仓位、小时贡献和价格首末端点，包含全期平均仓位、价格累计回报、累计组合收益贡献、最大月均绝对仓位及该月回报、最好/最差贡献月份。字段含义见 [收益贡献方案](docs/RETURN_CONTRIBUTION.md)。

## 向量准备与事件处理

独立向量计算由 vectorization.encoder.EmbeddingEncoder 和 entrypoints.prepare_vectors 执行，使用 embedding_config_path 指向的配置。填写 bge-m3 服务 base_url 和 EMBEDDING_API_KEY；默认每批 8 条，只编码 event。向量保存于事件表 embedding 列，JSON 含 model、model_version、config_hash、vector，重启复用。config_hash 标识服务、模型、版本和编码规则，不含 event；换权重时修改 model_version，修改 event 时清空相应向量并确定下游重算范围。第二轮只载入同版本的已保存向量和检索候选，不调用向量服务、不读取向量密钥；缺失或损坏时先补齐独立向量阶段。第二轮 AI 仅使用 event 和候选历史；第三轮仅使用 asset、event、new_information。不使用 SQLite。

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

前史第二轮已完成但组引用不合法时，普通续跑不会改写成功结果，应先核查数据和影响范围。每条 update/repeat 的 `event_id` 应直接指向同币、更早的 new 组起点。

## 只读观察第二轮成功记录的相似度

在运行第二轮的 Python 环境执行 `python -X utf8 -m entrypoints.audit_similarity`，按 `event_processing_config.yaml` 的日期范围及资产名单复用表内向量，仅统计 `round2_status="1"` 的成功记录；当前批次和前史中的非成功记录均跳过，无需等待整轮完成。导出本地 `events.csv`、`candidates.csv`、`thresholds.csv` 和完成说明。最高相似度对应的具体历史事件、记录 ID、事件组 ID 及低于阈值的候选都可查看；不调用模型、不恢复状态、不写数据库。默认每条导出阈值筛选前的前 10 个组，可用 `--top-k 20` 增加。多阈值统计固定使用已有分组，不代表人工验收准确率。命令参数、观察方法及还原限制见 [相似度只读观察](docs/SIMILARITY_AUDIT.md)。

## 第三轮结果转为小时权重表

第二、三轮完成后，使用 `configs/event_processing_config.yaml` 中的 `result_path`（共用结果表）、`weight_path`（参数表的基础路径）、`start_time`、`end_time` 和 `weight_grid`。`weight_path` 按要求留空，运行前自行填写；实际运行的日期必须同时设置为 `YYYY-MM-DD`，包含起止日全天。先预览参数、组合数量和表名，再在已安装 `xdata` 的环境运行：

```powershell
python -X utf8 -m entrypoints.write_weight_data --dry-run
python -X utf8 -m entrypoints.write_weight_data
```

固定评分为 `direction × (B × materiality) × (C × execution_stage) × (D × event_relation)`。B/C/D 和每个枚举映射均配置为候选数组，独立做笛卡尔积，每组参数输出一张表。未填参数采用默认值；显式数组须非空且只含有限数值，候选按数值去重。persistence 保留映射配置，暂不参与评分、组合展开或参数标识。即使两组结果相同也分别输出。

`weight_grid.aggregation_methods` 选择同资产同小时的聚合算法，默认 `[sum]` 求和；填写 `[max_abs]` 只保留绝对值最大的原信号，保留正负号，例如 `3、-8、7 → -8`。并列时取最早发布时间，同一时刻取源记录中的第一条。填写 `[sum, max_abs]` 可对每组评分同时生成两张独立表，组合数翻倍。相同评分参数在两种模式下共享 `param_id`，聚合模式由表名单独区分，预览和清单中列出所选算法。

实际表路径默认为 `{weight_path}/{aggregation_method}__B{B}_C{C}_D{D}__{param_id}`，自定义模板必须同时包含 `{aggregation_method}` 和 `{param_id}`。param_id 来自评分版本、系数和完整有效映射的 SHA-256 前 16 位，聚合模式、日期及候选顺序不影响标识。同一参数和模式在后续日期继续更新同一表；旧路径表保留，不自动迁移。默认上限 1000 组，超限、非法参数或冲突表名在连接数据库前停止。`--dry-run` 不连接数据库。

可用 `--config "另一个事件配置文件路径"` 指定同结构的事件配置。源表按日期范围只读取一次、回答只解析一次，供所有组合复用，不调用 AI。逐组合生成、保存并释放 DataFrame。目标不存在时注册并首次 write；已有表通过 `update_by_column` 替换重叠小时，通过 `update_by_index` 追加新小时。重复运行不累加，范围外数据保留。完整参数、目标表和状态保存到 `reports/weight_grid/<北京时间>/manifest.json`，可用 `--report-dir` 指定新目录。某表失败时停止，已成功表保留，重跑继续对齐更新。

`weight_data` 的单层索引名为 `date`，类型为 `DatetimeIndex`（`datetime64[ns]`），每条时间均为整点。列为窗口范围内去重资产加 `.USDT.UFUTURE.BINANCE` 后缀。整点 T 聚合 `[T-75分钟, T-15分钟)` 内的事件，映射公式为 `(发布时间 + 15分钟).floor("h") + 1小时`；例如 `[16:45, 17:45)` 计入 `18:00`，`17:45` 计入 `19:00`。按目标小时和资产以 float64 使用所选算法聚合并补零，写库前索引整体加 8 小时，`18:00` 最终写入次日 `02:00`。实际写库范围从 `start_time 08:00:00` 到 end_time 次日 `09:00:00`。程序补读首日前一天和末日次日，再精确保留首尾完整窗口所需事件；索引仍为无时区 datetime。

当前版本 `direction_product_v3` 的参数标识与旧版不同，避免将 45 分钟窗口的数据混入旧版整点分桶的参数表；旧表保留。`reports/weight_grid/` 只保存本地参数和状态清单，不存权重 DataFrame；重跑依赖数据库实际数据，不依赖旧清单。

续写时保留已有资产列：本批未出现的旧资产在新小时填浮点数 `0.0`，新资产的已有历史行也补浮点零值。首尾完整窗口补读后，所有重叠小时（包括批次起点）均按本次重新计算值替换，跨日分批与一次性计算一致。重复运行不会再次平移已有表。返回的本批 df 包含重新计算的边界值及合并后的资产列。

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

## 标签贡献实验仓位表

标签实验的 `weight_path` 同样作为 root，输出地址为 `{weight_path}/{aggregation_method}__labels__{方案名}__B{B}_C{C}_D{D}__{param_id}`。`param_id` 直接复用 `write_weight_data` 的评分参数哈希，聚合模式和等级分组由表名独立区分；公共信号倍率折入实际评分参数 B，旧路径表保留。

已有回测只接收仓位分布时，运行 `python -X utf8 -m entrypoints.write_label_experiments --dry-run` 预览，再去掉 `--dry-run` 生成表。默认包括 8 组标签组合对照和 17 组等级分组，共 25 张独立小时原始信号表。沿用事件配置的来源、日期和固定评分，B/C/D 统一为 1；每条分组事件只贡献方向符号，不再乘等级分数。`--signal-scale`（旧名 `--unit-position`）只是共同信号倍率，默认 1，不代表资金比例。若回测直接按数值买币，这些表是等信号/等数量分组，并非等金额分组；金额口径须核对回测的价格换算。完整输出路径和事件数见本次 `reports/label_experiments/北京时间/tables.csv` 与 `manifest.json`。口径、配对关系和 notebook 用法见 [标签贡献实验说明](docs/LABEL_EXPERIMENTS.md)。

## Weight 信号覆盖与事件分布诊断

修改 `entrypoints/plot_signal_diagnostics.py` 顶部的表路径和日期，运行 `python -X utf8 -m entrypoints.plot_signal_diagnostics`。统一用 `cli.read` 读取，输出非零资产数、集中度、资产占比热图和连续全零时段；可选分析第二、三轮分布、实际映射的零分来源及同资产小时多空抵消。不接入策略或净值，详细配置和 CSV 字段见 [信号诊断](docs/SIGNAL_DIAGNOSTICS.md)。

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

第一轮和第三轮也通过共用调度器输出详细进度与瓶颈统计，复用第二轮的汇报节流、批次耗时计算及网络层实际 HTTP 并发快照。各自遵守 `run.progress_every`，或距上次汇报达到 10 秒时输出；分段开始、结束及异常停止强制汇报。进度按当前分段启动时待执行的 AI 任务计算，显示成功、失败、剩余、待认领、HTTP 请求中/上限、准备状态、待准备、就绪请求/备用上限、已认领未落库、待写库结果及累计 HTTP 尝试；已有成功不重复计入，第三轮 repeat 继承仍由月末汇总统计。成功与失败只在结果保存确认后累计，未保存的结果保留为待写库。HTTP 平均并发、峰值以及认领和保存的平均每批耗时、平均批量、折合每条耗时按本次运行跨月累计，不额外查库。第一轮和第三轮认领计时覆盖选择与认领回调，第一批可能包含配置摘要核对；保存计时包含结果转换、存储重试和内存同步。10 秒汇报也由调用线程触发，同步数据库调用阻塞时会延后到返回后输出。

退出码继续为成功 0、普通异常 1、用户中断 130。round1事件提取初始化保留具体错误和目标核查提示；其他入口保留标准异常栈。停止程序后重启各自入口继续处理，恢复、保存和历史时间边界见 [round1事件提取规则](docs/rules/KEY_RULES.md) 与 [事件规则](docs/rules/EVENT_RULES.md)。

VS Code 打开项目根目录并选用已安装依赖和 xdata 的解释器。打开 `entrypoints/initialize_tasks.py` 等入口，使用 **Python Debugger: Debug Python File**；现有 `.vscode/launch.json` 为当前文件提供根目录导入路径和 UTF-8 设置，无需安装项目。

## 验证和文档

模拟测试按公共能力、初始化、轮次和入口分组，公共模拟客户端在 tests/support.py 与 tests/event_support.py。2026-10-16 按列摘要接口接入后，完整 267 项测试通过；包含三轮摘要列查询、跨月复用与读取失败停止，以及 23 项表内向量与阈值测试中的模型输出原值保存及复用、第二轮不读原文、共用网络重试及 1024 维相似度。测试不访问真实 AI 或数据库：

```powershell
python -X utf8 -B -m unittest discover -s tests -t . -q
```

当前本地验证可使用 `.\.python38\python.exe` 替代 python。真实接口和规模验证事项见 [待办](docs/IMPLEMENTATION_ISSUES.md)，模块职责和总体数据流见 [项目总览](project_overview.md)，数据结构见 [round1帖子事件提取表](docs/data/TABLE_DESIGN.md) 和 [事件去重分析表](docs/data/EVENT_RESULT_TABLE.md)。历史改造记录保留在 docs/history，研究计划位于 docs/plans。
