# AI 帖子三轮处理项目

## 统一主题与表名

| 主题 | 处理内容 | 使用的表 |
| --- | --- | --- |
| round1事件提取 | 从每条帖子提取事件事实和代币 | round1帖子事件提取表，一帖一行 |
| round2事件去重归组 | 判断 new/update/repeat，将同一事件的披露归组 | 事件去重分析表，一条事件 × 一个代币一行 |
| round3事件影响分析 | 分析 new/update 的经济影响；repeat 沿用既有跳过规则 | 与 round2 共用事件去重分析表 |

“去重归组”保留每条披露记录，通过关联类型和事件组表达重复或新增信息，不删除重复披露行。本文档及运行日志采用上述名称；配置键、代码符号、表字段和实际数据系统路径保持不变。两张表的配置对应关系见 [运行说明](README.md)。历史改造记录保留当时称呼，现行命名以本节为准。

本次仅统一文档、日志和配置说明，同步调整日志测试；`common/time_rules.py` 继续独立保留。时间解析、日期边界、筛选排序、状态恢复、请求预算、保存与停止规则均不调整。整理与验证范围见 [主题和表名统一记录](docs/history/TERMINOLOGY_STANDARDIZATION.md)。

2026-10-16 已将公共代码收拢为 9 个功能模块，按请求与响应、校验、事件上下文合并，并完成导入、测试及文档复查。处理规则、数据字段和配置保持不变，详见 [公共模块收拢记录](docs/history/COMMON_MODULE_CONSOLIDATION.md)。

2026-10-16 已按用户追加要求将向量存入事件去重分析表新增的 embedding 列，取消 SQLite。新增列初始化为空字符串，生成后保存含模型、版本、配置标识及向量的 JSON；config_hash 不含 event，相同向量配置的各行一致。第二轮只使用固定 event 和候选历史，不读原帖，第三轮保留正文输入；Embedding 复用公共网络异常分类、随机退避和 Retry-After 解析。只提交组分数达到阈值的最多 10 组，不补足，全部低于时直接 new。默认阈值 0.50 是用户授权 AI 建议的试运行值，尚未按实际样本标定。实现及例子见 [表内向量与阈值方案](docs/plans/EMBEDDING_TABLE_STORAGE_PLAN.md)。用户确认事件去重分析表尚未创建，本次不增加旧表迁移。

2026-10-16 已按命名检查清单统一代码符号及测试类，并完成导入、测试和文档复查；目录、表字段、配置键和处理规则保持不变。对应关系与验证结果见 [命名统一记录](docs/history/NAMING_CONSISTENCY.md)。

2026-10-16 用户确定round2事件去重归组采用已部署的 `bge-m3` 兼容接口，只编码 event 做纯向量检索，不使用关键词或排名融合。向量持久保存、阈值放行及重启行为以本次表内向量方案为准。

用户进一步确认服务已经输出归一化向量：程序仅校验单位范数，按模型返回值保存并计算内积，不再次归一化或转成 float32；内存计算使用 float64，配置摘要中的编码规则标识同步更新。详见 [表内向量方案](docs/plans/EMBEDDING_TABLE_STORAGE_PLAN.md)。

2026-10-16 按已确认方案将公共代码统一放入 common，集中表结构、三轮输出校验及 Store，分别组织配置加载、业务输入与请求构造。处理规则保持不变，详见 [公共目录合并记录](docs/history/COMMON_DIRECTORY_MERGE.md)。

本项目独立运行，目标是从帖子提取事件事实，按代币生成披露记录，关联已有事件组，再分析有效新增信息的经济影响。原始帖子采集和后续新增round1事件提取任务由现有数据流程负责。

2026-10-16 用户确认两轮 AI 回答分别存入 round2_ai_response、round3_ai_response 两列 JSON，第三轮不再拆成五个影响列。事件去重分析表为 14 个普通列；两轮回答内保存原始正文、最后一次响应 usage、接口响应时间、实际请求次数及提交数据库前的北京时间 created，不保存推理文本。存储重试仅刷新 created，不重新调用 AI。第三轮五字段校验、归组与状态规则保留，详见 [回答保存口径](docs/data/EVENT_RESULT_TABLE.md#两轮-ai-回答的保存口径)。

2026-10-15 用户确认目录直接按职责划分，不套顶层包，不增加安装或发布流程。本次只调整文件归属、导入、配置定位、测试和文档；处理规则保持不变。迁移记录见 [目录整理方案](docs/history/DIRECTORY_REORGANIZATION_PLAN.md)。

## 模块职责与依赖

第二轮在内存位置字典、按币分组和有序候选索引的基础上，默认采用 5 个日历年的历史期限。启动必须配置处理起止日期，先按日期读取本次任务，有待处理任务时才补读所需前史；较晚已完成冲突只按日期读取资产、状态两列。前史校验、候选检索及向量准备共用 history_lookback_years，不再读取全表事件内容或回查窗口外组起点。截断组保留 event_id 和窗口内事实，缺起点时 first_disclosure=null、history_trimmed=true。第三轮现也按任务准备固定 5 年组历史，具体流程见下文；第二轮向量临时矩阵和评分方式保留。读取顺序和截断例子见 [事件规则](docs/rules/EVENT_RULES.md#4-round2事件去重归组前置检查与恢复)。

| 目录或文件 | 职责 | 主要依赖 |
| --- | --- | --- |
| `common/config.py` | 分别加载任务、事件、AI 及 Embedding 配置，维护配置对象、模板检查和摘要 | PyYAML、python-dotenv、标准库 |
| `common/ai_protocol.py` | 三轮大模型请求体构造、接口响应解析及答复类型 | 配置对象、标准库 |
| `common/validation.py` | 两张表字段及结构、严格 JSON 解析、三轮各自的输出校验 | 时间规则、公共异常、pandas |
| `common/event_context.py` | 原帖正文和业务输入、候选检索、批量建立历史索引及两轮窗口内的组事实与截断标记 | 数据约定、时间规则 |
| `common/time_rules.py` | 完整发布时间解析、包含首尾日的范围判断和选择，原索引及时区不变 | pandas、标准库 |
| `common/store.py` | TableStore 共用保存；PostTaskStore 管理 round1帖子事件提取表，EventRecordStore 管理事件去重分析表 | 配置摘要、数据约定、存储重试 |
| `common/network.py` | 网络线程、请求预算、限速、完成消息和关闭 | 配置、AI 请求与响应约定、HTTPX |
| `common/execution.py`、`persistence.py` | 共用分段执行、指定列写入、排序及整批存储重试 | 网络作业、pandas |
| `initialization/post_tasks.py` | 根据完整源帖子建立round1帖子事件提取表，返回 InitializationSummary | 事件数据约定、pandas |
| `initialization/event_records.py` | 固定round1事件提取校验、事件与代币展开、确定性 ID、去重、自动导入和独立续建 | 事件数据约定、共用存储重试 |
| `round1/` | round1事件提取按月加载、任务协调和成功帖回调 | common |
| `round2/` | round2事件去重归组：bge-m3 接口、表内向量复用、按组评分及阈值筛选、候选校验和同币时间推进 | common、HTTPX、NumPy |
| `round3/` | round3事件影响分析：全范围前提检查、必要历史准备和分段任务 | common |
| `entrypoints/` | 配置定位、日志、客户端接入、独立启动；round1事件提取入口协调自动导入 | 对应业务入口、运行环境提供的 xdata |
| `main.py` | 根目录round1事件提取及自动导入启动入口 | entrypoints.run_round1 |

目录之间直接导入，例如 `from round1.runner import run_extraction`。`common` 包含基础能力及共用业务约定，不导入初始化、轮次或入口；三个轮次不互相导入。round1事件提取与round3事件影响分析共用分段执行器，round2事件去重归组保留按币和时间推进的独立调度。两种业务 Store 在同一模块内继承 TableStore，round2事件去重归组与round3事件影响分析继续写同一张事件去重分析表。

## 配置与运行环境

非敏感配置集中在 `configs/`。`job_config.yaml` 指定源帖子、round1帖子事件提取表、处理范围、round1事件提取 AI 配置和日志；`event_config.yaml` 指定事件去重分析表、日期范围、映射、检索及两份 AI 配置路径。三份 AI 配置分别为 `round1_ai_config.yaml`、`round2_ai_config.yaml`、`round3_ai_config.yaml`；`embedding_config.yaml` 独立提供向量服务、批量及相似度阈值参数。各接口参数独立，不增加配置继承。

第二、三轮必须在 event_config.yaml 设置 start_time/end_time，两端包含全天，仅处理该范围内的任务。history_lookback_years 默认 5，须为正整数，供第二轮使用；第三轮固定回看 5 个日历年。两轮均按每条披露发布时间回推，不按程序运行日计算；闰日回推平年取 2 月 28 日，保留时区和纳秒。缺失范围时不读数据库，也不默认处理全表；仅独立导入继续允许日期为空。

第三轮先按配置范围读取任务，确定未完成 new/update 后，仅为 update 准备同币、同组且位于其 `[发布时间-5年, 发布时间)` 的第二轮已完成事实。先复用范围内事实，再按月和六个必要列补读范围外历史；不读取表元数据或遍历全表事件内容。窗口外起点不回查，缺起点时复用截断标记，窗口内完全无可用组历史时停止。范围及历史检查通过后才恢复、认领和分析，配置摘要仍保留全表单列核对。模块接口与边界见 [第三轮规则](docs/rules/EVENT_RULES.md#7-round3事件影响分析检查调度和保存)。

所有 YAML 配置统一使用现有的安全重复键检查加载器。字典键须为字符串，同一个字典内同名键重复时直接报错，即使两次的值相同也不允许；不同字典可以使用相同键名。例如账号映射中两次填写 BitcoinMagazine 会停止加载，不会用后一个代币覆盖前一个；任务或事件配置错误会在创建数据客户端前停止。

2026-10-16 已按用户提供的 TradeCoins 50 名单（生成于 2026-09-18）填入 `event_config.yaml` 的 `user_name_to_tickers`：原 `screen_name` 作为用户名，原 `ticker` 转为单元素列表，共 50 组，保留大小写及原名单迁移备注。映射沿用现有规则，仅在导入事件的 tickers 为空时按原帖 user_name 精确匹配；实际源帖的用户名仍待核对。

入口按自身文件位置定位项目根目录，读取 `configs`，不依赖当前工作目录。任务配置中的相对 AI 配置路径按 `configs` 目录解析；绝对路径继续支持。数据系统路径原样交给客户端。认证信息来自进程环境或与 AI 配置同目录的 `.env`，进程环境优先；默认即 `configs/.env`，示例在 `configs/.env.example`。

## 总体数据流

```text
源帖子 → 初始化round1帖子事件提取表
                  ↓
round1事件提取任务 → 按日期范围读取 → 跳过已成功帖 → AI 提取并保存
                  ↓ 正常返回后交出成功帖的内存副本
              事件导入 → 按事件 × 代币展开、去重 → 事件去重分析表
                                                       ↓
round2事件去重归组 → 阶段恢复和前史检查 → 同币时间队列 → 检索候选 → 关联 AI
                                                       ↓
                                  保存确认 → 更新历史 → 推进下一时点
                                                       ↓
round3事件影响分析 → 本范围关联检查 → 未完成 update 的 5 年组历史 → new/update 并发分析
                                                       ↓
                                      第三轮回答 JSON 和状态共同批量保存
```

日常由 `main.py` 在round1事件提取正常返回后自动导入本次范围的成功帖，round2事件去重归组与round3事件影响分析分别启动。round1事件提取处理函数只通过回调交出数据，不依赖导入业务。round1事件提取异常或中断时不自动导入；普通单帖失败不阻止其他成功帖导入。导入失败保留已保存的round1事件提取结果，重跑同范围时跳过成功帖 AI 并补导。

初始化有两个独立职责。round1事件提取初始化完整读取源帖，校验后注册并一次完整写入，不读取 AI 配置、不重试。事件导入负责映射、展开、固定输入核对和追加；主入口自动导入与独立 auto/create/resume 入口保留各自规则。

## 跨模块接口

- `initialization.post_tasks.initialize_tasks(source_df, write_tasks)`：建立完整round1帖子事件提取表；接入函数为 `entrypoints.initialize_tasks.initialize_existing_posts(cli, settings)`。
- `round1.runner.run_extraction(cli, task_path, config_path, start_time=None, end_time=None, *, on_successful_posts=None)`：返回原 ExtractionSummary；分段保存并检查结束后，可通过回调交出本次新成功及读取时已成功的帖子副本。
- `initialization.event_records.import_event_records_from_posts(cli, settings, new_posts=None, previous_posts=None)`：从内存成功帖导入事件，返回本次新增行数，不重读round1事件提取。
- `initialization.event_records.import_event_records_from_table(cli, settings, *, successful_only=False)`：独立导入及续建，返回 InitializationSummary。
- `round2.runner.run_relation(cli, settings, ai_config=None, retriever=None, network_factory=NetworkWorker)`：返回 StageSummary；同币当前时点全部保存后才推进历史。
- `round3.runner.run_impact(cli, settings, ai_config=None, network_factory=NetworkWorker, validator=validate_impact_output)`：返回 StageSummary；只分析已关联的待处理 new/update。
- `common.store.TableStore` 提供快照、阶段恢复、选择、认领、写入和结束状态统计；PostTaskStore、EventRecordStore 保留各自转换和表约定。
- `common.store.read_stage_hashes(cli, task_path, hash_column)` 调用 `cli.read(task_path, start_time='', end_time='', columns=[hash_column])` 读取全表单列，返回 DataFrame 中该列的原值；三轮在本次首次认领前核对一次，跨月复用。
- `common.config.load_job_config`、`load_event_config`、`load_ai_config` 分别加载任务、事件及 AI 配置；不会因为共用模块而提前读取 AI 配置。
- `common.config.load_embedding_config` 加载向量服务及阈值配置；`round2.embedding.EmbeddingRetriever.prepare(records, save_embeddings)` 读取原键、event 和表内向量，按批生成缺失或标识不匹配的向量，调用 `EventRecordStore.save_embeddings` 只更新 embedding 列。保存完成后才认领关联，评分对象接收 current/searchable 并返回 RetrievalSelection。生成向量不代表关联完成，业务阶段只做内存比较，round3事件影响分析不修改或发送向量。
- `common.event_context.build_relation_input`、`build_impact_input` 在原调用时机组织业务文本；`common.ai_protocol.render_payload` 只把文本填入请求模板。
- `common.validation.validate_extraction_output`、`validate_relation_output`、`validate_impact_output` 分别检查三轮输出；round2事件去重归组使用本次候选 ID，round3事件影响分析保留五字段非空文本检查及 validator 注入接口。

调用方管理数据客户端生命周期及同表写入协调。数据操作在调用线程，AI 请求在独立网络线程；同一张处理表由一个处理程序写入，不支持多实例抢占。

## 保持不变的关键约定

round1帖子事件提取表按原 `(date, id)` 定位，事件去重分析表按原 `(date, record_id)` 定位；事件 ID 由 `source_post_id:event_index:asset` 确定性生成，日期不做时区转换。round1事件提取请求和事件导入共用 events/event/tickers 格式校验，空事件及空代币数组仍按原规则处理。

空 events 不生成事件任务，也不要求账号映射。空 tickers 在第一轮格式校验中合法；导入时若账号也无映射，本次导入调用的全部事件在写入前停止，第一轮已保存结果保留，不生成失败事件行。补齐映射后可重跑同范围补导。缺少 tickers 字段则属于第一轮输出错误。完整边界和例子见 [事件展开规则](docs/rules/EVENT_RULES.md#2-检查round1事件提取并展开记录)。

三轮状态继续使用字符串 `0`、`9`、`1`、`-1`。重启只恢复所运行阶段遗留的 `9/-1`，不根据残留结果推断成功，不质检或补写历史普通列。认领时按全表本阶段摘要核对配置；读回普通列原值使用。

客户端 write、update_by_column、update_by_index 正常返回即本次整批写入，抛异常即本次未写入；存储整批重试与 AI 请求预算独立。运行结果仅按原索引更新目标列，数据库写入成功后才同步内存；预算耗尽停止，不在收尾额外开启保存预算。

round2事件去重归组同刻记录不互用本时刻结果；普通最终失败暂停同币后续关联，其他币继续。合法 uncertain 的业务关系保存为 new，AI 原文保留；repeat 同时完成round3事件影响分析状态且第三轮回答列留空。round3事件影响分析失败只影响本条，不重做round2事件去重归组。具体边界和例子由以下规则文档维护。

## 文档与验证入口

- [运行和配置方法](README.md)
- [代码结构与接口](docs/CODE_ARCHITECTURE.md)
- [round1事件提取关键规则](docs/rules/KEY_RULES.md)
- [事件导入、round2事件去重归组及round3事件影响分析规则](docs/rules/EVENT_RULES.md)
- [round1帖子事件提取表](docs/data/TABLE_DESIGN.md)、[事件去重分析表](docs/data/EVENT_RESULT_TABLE.md)
- [真实接入和规模验证待办](docs/IMPLEMENTATION_ISSUES.md)
- [研究总计划](docs/plans/项目总计划书.md)、[三轮业务计划](docs/plans/第一轮+第二轮+第三轮计划.md)

全表按列摘要读取使用用户确认的 `cli.read(..., start_time='', end_time='', columns=[hash_column])`，返回 DataFrame，空日期表示全表；读取失败时停止，不回退整表读取。纯向量评分器已实现，真实 Embedding 接口地址与认证、round2事件去重归组与round3事件影响分析提示词及具体经济评分枚举仍待配置或接入；召回质量须用真实标注样本验证。
