# 命名统一实施记录

> 本文保留当时的称呼和实施记录。现行主题与表名以 [主题和表名统一记录](TERMINOLOGY_STANDARDIZATION.md) 为准；实际表路径未调整。

后续结构说明：本文件保留当时的模块名称，当前已进一步收拢为 9 个公共模块。现行位置和验证结果见 [公共模块收拢记录](COMMON_MODULE_CONSOLIDATION.md)。

日期：2026-10-16。状态：已完成并通过整体复查。

## 范围与约束

用户确认按命名检查中的两张表统一类、函数、方法和测试类名称，同步调用、导入、测试和文档。目录及文件路径沿用当前结构，第一轮仍由 main.py 或原模块入口启动。

本次只调整代码符号与说明，不改变参数、返回数据、表字段、配置键、日志规则或处理行为。当前向量检索实现完整保留；三轮读取、筛选、排序、时间边界、输入构造、校验范围、状态、摘要算法、请求与保存预算、停止及恢复规则保持不变。

## 已确认名称对应

| 位置 | 原名称 | 新名称 |
| --- | --- | --- |
| common.config | config_hash | ai_config_hash |
| common.store | check_stage_config | validate_stage_config_hash |
| common.store | TaskStore / ResultStore | PostTaskStore / EventRecordStore |
| common.table_schema | validate_table | validate_event_table_structure |
| common.table_schema | validate_source_index | validate_event_source_index |
| common.table_schema | validate_post_tasks | validate_post_task_structure |
| round1.runner | run_worker / RunSummary | run_extraction / ExtractionSummary |
| common.store | check_history_coverage | validate_relation_history |
| TableStore | completed | count_completed |
| SegmentCoordinator | flush_due | should_flush |
| TableStore | write / _confirm | update_stage_columns / _apply_saved_patch |
| initialization.event_records | initialize_event_records | import_event_records_from_table |
| initialization.event_records | import_saved_event_records | import_event_records_from_posts |
| common.output_validation | validate_first_round | validate_extraction_output |
| common.output_validation | validate_relation / validate_impact | validate_relation_output / validate_impact_output |
| common.inputs | current_input | build_current_record_input |
| EventHistory | searchable / build_group | get_searchable_records / build_group_history |
| common.store | valid_relation | is_stored_relation_valid |
| round2.retrieval | check_selection | validate_retrieval_selection |
| common.output_validation | strict_json | parse_strict_json |
| entrypoints.run_round1 | execute | execute_round1 |

六处 EventTests 根据测试职责分别命名为 EventConfigTests、EventStageTests、RelationRunnerTests、ImpactRunnerTests、EventImportTests 和 EventEntryPointTests。原有测试方法、断言及模拟数据保留。

config_hash 数据列、round2_config_hash 和 round3_config_hash 数据列保留原名；客户端 write、SQLite execute、公共入口 execute(main)、局部 completed 列表和 searchable 参数也保持原义。只调整清单中对应的符号，不按单词批量改写这些内容。

## 验证安排

先保存实际工作区源码、文档及配置指纹，运行整理前完整测试。迁移后对照基线检查整个模块的语法树，归一化清单中的符号、测试引用及说明后应一致；普通字符串、表字段、参数默认值和测试数据保持原样。

运行完整模拟测试，核对内部导入、旧名称、文档链接、目录与配置路径，清理临时脚本和日志。测试不访问真实 AI、Embedding 或数据库接口，现有真实接入和规模验证待办保留。

## 完成结果

- 完成 26 个生产类、函数及方法，以及 6 个测试类的改名，涉及 29 个源码和测试文件；目录和模块文件路径保持原样。
- 整理前完整 249 项测试通过；整理后同一套 249 项测试全部通过，Python 3.8.10 下耗时 28.048 秒。没有新增、删除或放宽测试方法和断言。
- 对照实际工作区基线，57 个源码及测试模块的语法树只包含清单内的命名、测试引用及说明变化；参数、返回结构、普通数据字符串和原有测试数据保留。
- 六份 YAML、.env.example、requirements.txt 和调试配置共 9 份文件的指纹均与基线一致；没有读取认证 .env，摘要算法、缓存标识与数据系统路径未改动。
- 模块编译、内部导入及符号检查通过；生产模块仍由原有入口使用，没有循环或反向依赖，没有重复顶层实现、重复或无用导入。
- 现行代码和文档无失效的旧符号引用；config_hash 数据列及外部客户端接口保留原名。历史方案中的旧名称有明确说明并保留供对照，本地文档链接已逐一检查。
- 本次临时迁移脚本、基线快照和测试日志已清理；现有未提交工作、向量检索实现及有用历史记录保留。

本次验证使用模拟客户端，未调用真实 AI、Embedding 或数据系统。真实接口接入、召回质量和规模联调仍按既有待办执行。
