# 代码结构与接口

2026-10-16 按用户确认方案将公共目录收拢为 9 个功能模块。项目独立运行，代码目录直接位于根目录，不增加顶层包、src 布局或安装步骤。目标与数据流见 [项目总览](../project_overview.md)，实际规则见 [round1事件提取规则](rules/KEY_RULES.md) 和 [事件规则](rules/EVENT_RULES.md)。

## 1. 目录及依赖方向

```text
项目根目录/
├─ main.py                       round1事件提取及自动导入启动入口
├─ configs/                      两份任务配置、三份 AI 配置、Embedding 配置及 .env.example
├─ common/                       9 个功能模块，另有 __init__.py
│  ├─ config.py                  配置加载、对象、模板检查和摘要
│  ├─ ai_protocol.py             大模型请求体构造、响应解析及答复类型
│  ├─ validation.py              表字段、结构检查和三轮输出校验
│  ├─ event_context.py           原帖正文、业务输入、候选及组内时间线
│  ├─ time_rules.py              事件时间解析和范围选择
│  ├─ store.py                   TableStore、PostTaskStore、EventRecordStore 及汇总
│  ├─ network.py                 作业、限速、重试、完成消息和线程关闭
│  ├─ execution.py               round1事件提取与round3事件影响分析共用分段并发执行
│  └─ persistence.py             排序、按列更新和存储重试
├─ initialization/
│  ├─ post_tasks.py              round1事件提取任务初始化及 InitializationSummary
│  └─ event_records.py           事件展开、自动导入和独立续建
├─ round1/runner.py              round1事件提取执行和成功帖回调
├─ round2/                       round2事件去重归组：调度、候选校验及向量检索
├─ round3/runner.py              round3事件影响分析前提检查、历史准备和执行
├─ entrypoints/                  五种独立任务入口及 runtime.py
├─ tests/                        common、initialization、各轮及 entrypoints
└─ docs/                         规则、字段、研究计划及历史方案
```

common 包含基础能力及共用业务约定，只依赖本目录、标准库和已有第三方库，不导入初始化、轮次或入口。各轮依赖 common，不互相导入；初始化依赖公共数据约定和保存能力，不依赖轮次运行器。入口协调客户端、配置及业务函数。

## 2. 配置、业务输入与请求构造

common.config 按配置类别提供加载入口：load_job_config 读取round1事件提取任务 YAML 并检查调用方指定的必填字段；load_event_config 读取事件任务配置；load_ai_config 读取 AI 配置、加载同目录可选 .env，并检查模板及参数。向量配置另由下文的 load_embedding_config 加载。各入口原有的日志、路径冲突等检查继续留在调用方，顺序不变。初始化不加载 AI 配置；第二轮全部完成或无待处理任务时直接返回，第三轮只在需要分析 new/update 时加载 AI 配置。第一轮仍在读取任务表前加载并校验其 AI 配置，即使范围内任务全部成功也需要配置可加载。

配置仍位于 configs，相对 AI 配置按任务配置所在目录解析，绝对路径继续支持；环境变量优先于 .env。ai_config_hash 仍只基于原始提示词及 api.payload，URL、认证、并发和重试参数不参与摘要。

四类配置加载入口统一使用 common.config 已有的 _UniqueLoader，沿用安全 YAML 解析和字符串键要求；每个字典独立检查重复键，根层及嵌套层重复均抛 ConfigError，不静默覆盖，不新增加载封装或配置项。任务和事件配置在创建数据客户端前完成检查。

common.event_context 的 build_current_record_input 供第二、三轮组织固定 event 和定位字段；第二轮不读取原帖。load_source_texts 仅供第三轮，build_impact_input 在认领后补入原帖正文及必要历史；build_relation_input 在检索后序列化当前事件及候选。round1事件提取仍直接使用原帖正文。

EventHistory 在批量加入已完成记录后，按币将候选列表按发布时间和 record_id 排序；add_frame 和 add_records 共用批量登记，后者接收筛选后的原键和必要字段，避免第三轮分段补入旧历史时逐条移动有序列表。运行中新确认的记录仍通过追加或二分定位插入。get_searchable_records 二分定位按 history_lookback_years 回推的左闭右开窗口，默认 5 个日历年。第二、三轮均启用 truncate_group_history，组输入只取各条披露自己的窗口；缺起点时 first_disclosure=null、history_trimmed=true，并提供 first_available_disclosure。第三轮固定 5 年。common.store.validate_relation_history 保持第二轮按配置期限检查前史的规则，向量临时矩阵和评分方式保留。

第二轮必须配置 start_time/end_time，EventRecordStore 先按日期读取本次任务；有待处理任务时，load_relation_history 按日期补入所需较早历史并重建原行位置。round2.runner._load_relation_history 通过 read_meta 获取表尾日期，范围之后只按日期读取 asset、round2_status 两列，供补录冲突检查，不参与候选。common.time_rules.history_window_start 统一日历年边界，table_date_bounds 供第二轮解析元数据。

第三轮也必须配置日期。round3.runner._prepare_impact 先只读本范围任务，检查第二轮状态和基本关系，并暂存历史必要字段；无记录或全部完成时直接返回。根据未完成 update 确定组与每条披露的 5 年窗口，复用范围内事实，再按月和六个必要列补读范围之前的历史，不读取元数据或范围之后的事件内容。只保留至少能进入一条任务窗口的同组完成事实，批量建立 EventHistory；窗口内已知起点仍检查合法性，缺失或窗口外起点按截断处理，完全没有组历史时停止。EventRecordStore.require_relation 可接收期限并复用既有 origin_start 检查，不改变未传期限调用的完整起点要求。所有任务和所需历史通过后才恢复状态，之后沿用原有正文读取、并发、批量保存和全表摘要单列核对。

common.config.load_embedding_config 独立加载向量服务地址、模型版本、认证、批量及阈值。round2.embedding.EmbeddingRetriever.prepare(records, save_embeddings) 接收原键、event 与表内 embedding，复用模型及配置标识匹配的 JSON；config_hash 不含 event。模型已归一化，_validate_unit_vector 只校验原向量及单位范数，不修改分量；保存响应数值，内存使用 float64 数组直接计算内积，编码规则标识同步更新。使用 HTTPX 批量生成缺失向量，复用 common.network 的 RETRYABLE_NETWORK_ERRORS、retry_backoff 和 parse_retry_after，传输、5xx 和无效输出共用批次预算。再调用 EventRecordStore.save_embeddings 仅按原索引更新 embedding 列；Store 使用独立存储重试，失败不重新请求 Embedding。全部向量保存后才认领关联。HTTP 客户端在准备结束或异常时关闭，业务评分只做内存点积，按组取最高分后过滤阈值，再取最多 10 组，不补足；全低分或无历史返回自动 new。第二轮按配置日期读取任务和必要历史，窗口筛选及按币时间队列推进均在内存完成。向量准备不改变记录的历史资格，round3事件影响分析保留向量列但不解析或发送给 AI。不使用 SQLite，未新增安装依赖，详细流程见 [方案](plans/EMBEDDING_TABLE_STORAGE_PLAN.md)。

common.ai_protocol.render_payload 接收已准备的文本，完成一次模板替换和请求体编码检查，不读取数据库、不重新查询历史。TaskRenderError 也由 common.ai_protocol 定义。配置加载不依赖 ai_protocol；ai_protocol 使用 AIConfig 和同一模板变量语法。

## 3. 表结构与三轮输出校验

common.validation 的 POST_RESULT_COLUMNS 和 TASK_COLUMNS 定义round1帖子事件提取表，RESULT_COLUMNS 和 RELATION_COLUMNS 定义事件去重分析表及关联业务列。IMPACT_FIELDS 只定义第三轮正文内的五个字段，不再对应数据库独立列。事件去重分析表为 14 个普通列，第二、三轮分别使用 round2_ai_response、round3_ai_response 保存原始回答和调用信息。共用 validate_index 后仍保留各自的索引、日期和字段检查，不合并不同输入的约束。

common.validation 集中 parse_strict_json 和三轮独立校验：

- validate_extraction_output 检查 events/event/tickers，供round1事件提取请求及事件导入共用；空事件和空代币数组在格式层面合法。导入另对空 tickers 查账号映射，无映射时停止本次全部导入，尚未写入事件表；缺少 tickers 字段则不通过格式校验。
- validate_relation_output 检查三字段、关系枚举及本次候选 ID，候选快照由调用方传入。
- validate_impact_output 保留五字段非空字符串检查，round3事件影响分析仍支持注入 validator；具体评分枚举没有新增。

OutputValidationError 由网络层计入同一任务的原请求预算。HTTP 外层响应及生成时间解析继续位于 common.ai_protocol，不与正文校验混合。common.time_rules 只解析业务时间用于校验、排序和范围选择，保存仍用原索引。

## 4. Store 与保存

三个 Store 集中在 common.store，各自职责保留。TableStore 负责工作快照、原索引定位、阶段恢复、认领、字符串更新检查、提交和统计，不解释 new/update/repeat。PostTaskStore 负责round1事件提取任务选择及答复转换；EventRecordStore 负责事件去重分析表的round2事件去重归组与round3事件影响分析保存及跨阶段约定，不拆成重复的两个 Store。

TableStore.update_stage_columns 更新当前阶段指定列，数据库调用返回后由 _apply_saved_patch 同步内存；count_completed 返回完成数量。SegmentCoordinator.should_flush 只判断是否满足保存条件，实际保存仍由 flush 执行。名称直接区分数据更新、内存同步、计数及布尔判断，处理顺序不变。

业务转换生成本次更新列后，由 common.persistence.write_columns 按原索引整行排序，再以同一批次调用 update_by_column。EventRecordStore 将最后一次响应和实际请求次数交给保存层，不再丢弃 usage、响应时间或原始 uncertain。第三轮正文已在网络入口校验，保存时不重复校验或拆列。回答 JSON 不含推理文本，结构见 [表约定](data/EVENT_RESULT_TABLE.md#两轮-ai-回答的保存口径)。

write_columns 的可选 prepare_write 回调只用于两轮回答保存，在每次提交前补当前北京时间 created，解决存储重试等待后仍记录首次时间的问题；其余写入不使用回调。每次提交前保留实际提交值的副本，防止客户端修改入参；正常返回后同步该副本。write_with_retry 不重新调用 AI，重试保持原逻辑批次，仅 created 随提交时间更新。内存同步失败停止并保留答复，不重新进入数据库写入预算。新表 write 和新索引 update_by_index 继续共用同一存储重试函数。

全表阶段摘要由 common.store.read_stage_hashes 调用 `cli.read(task_path, start_time='', end_time='', columns=[hash_column])` 取得，从返回的 DataFrame 中取摘要列原值。只有本次确实需认领时检查，跨月复用核对后的摘要；读取失败或返回缺列时停止，不回退整表读取。普通列原值使用，不增加读取质检或自动修复。

## 5. 执行和业务差异

common.network.NetworkWorker 只处理 RequestJob，在网络线程内完成该作业的请求尝试并返回 CompletionMessage。任务键只用于关联答复，业务校验回调使用同一请求预算。数据操作仍在调用线程。

round1事件提取与round3事件影响分析使用 common.execution.run_segments 和 SegmentCoordinator，共用分段加载、网络名额、结果缓冲、批量保存和停止收尾。各轮提供任务选择、输入、校验及汇总。

round2事件去重归组保留独立调度：同币按发布时间推进，同一时点共享此前历史，当前时点全部保存后才加入历史；普通最终失败暂停该币，其他币继续。

两种初始化保留不同流程。post_tasks 对完整源帖一次建表，不重试、不读 AI 配置；event_records 负责事件展开、映射和去重，并保留自动导入及独立 auto/create/resume 的不同检查。主入口自动导入不重读round1事件提取，只在存在读取时已成功帖时读一次已有事件去重分析表去重。

## 6. 入口、测试与后续接入

根目录 main.py 保留可调用的 main，并转调 entrypoints.run_round1.execute_round1。根入口和模块入口共用round1事件提取退出处理，保留调用脚本的日志来源、标准异常栈及退出码 0、1、130。其他入口为 entrypoints.initialize_tasks、entrypoints.import_events、entrypoints.run_round2 和 entrypoints.run_round3。

entrypoints.runtime 维护共用默认日志配置和后续阶段的配置、客户端接入及退出处理。round1事件提取初始化保留具体错误提示，round1事件提取处理保留原异常处理流程。日志来源随迁移后的模块名称更新，正文、提示词和认证仍不主动写入日志。

测试与源码职责对应，事件配置、三轮输出和公共 Store 测试统一归入 tests/common；各轮执行、初始化和入口测试仍独立。基础夹具位于 tests/support.py，事件模拟表及 EventFixture 位于 tests/event_support.py。

本次公共模块归并完整保留当前 255 项测试及原断言，包含已确认的表内向量与阈值规则。迁移后 54 个源码及测试模块对照实际工作区语法树，处理代码和数据字面值一致，9 份配置文件指纹保持原样；验证结果见 [公共模块收拢记录](history/COMMON_MODULE_CONSOLIDATION.md)。此前符号变化见 [命名统一记录](history/NAMING_CONSISTENCY.md)，目录迁移背景保留在 [合并记录](history/COMMON_DIRECTORY_MERGE.md)。

按列摘要 CLI 已按用户确认的调用方式接入；真实 Embedding 服务配置、提示词和经济评分枚举仍待接入，检索召回质量仍待真实样本验证，详见 [待办](IMPLEMENTATION_ISSUES.md)。历史方案保留于 history，现行结构和规则以本文件及规则文档为准。

上述 255 项及模块迁移对照属于公共目录收拢时的历史验证。此后已完成两轮回答合列、服务单位向量原值保存和按列摘要接口接入，当时完整 267 项模拟测试通过；内存索引优化新增 7 项，当时完整 274 项测试通过，覆盖时间边界、按币隔离、乱序载入与加入、返回副本、完整组前史及起点合法性。事件去重分析表仍为 14 个普通列，向量保存在 embedding 列，不使用 SQLite；实施依据见 [表内向量与阈值方案](plans/EMBEDDING_TABLE_STORAGE_PLAN.md)。测试未调用真实 AI 或数据库，不代表真实服务与数据规模已验收。

此前内存索引优化使用上一版代码和相同模拟数据对比：4,000 条同币历史的前史校验从约 8.12 秒降至 0.31 秒；2 万条历史、180 条窗口记录连续检索 100 次，结果一致，耗时从约 0.089 秒降至 0.004 秒。这是本机单次模拟观测，只衡量内存校验和窗口查询，不含数据库、Embedding 或 AI 请求；向量临时矩阵保持原实现。

任务和事件配置的重复键修复新增 4 项测试，覆盖根层、嵌套账号与日志键、重复值相同、不同字典同名键、单账号多代币，以及配置出错时不创建数据客户端。当时完整 278 项测试通过；仓库 6 份 YAML 在新旧加载器下解析值一致，50 组账号映射保留。
