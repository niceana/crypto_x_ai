# 媒体账号三轮处理运行说明

媒体与官方账号分别配置、分别存表，三轮复用同一套请求、执行、存储和续跑机制。媒体流程为：初始化第一轮表 → 第一轮提取并导入 → 第二轮归组 → 第三轮影响分析，不执行向量准备。

## 配置与两张表

| 文件 | 需要填写的内容 |
| --- | --- |
| `configs/media_post_extraction_config.yaml` | 原始媒体表 `source_path` 或本地 `json_files` 二选一、新第一轮表 `round1_result_path`、本轮日期 |
| `configs/media_event_processing_config.yaml` | 同一 `round1_result_path`、相同物理路径的 `round2_result_path` 与 `round3_result_path`、第二三轮日期、可选资产名单和输入字符上限 |
| `configs/media_round1_ai_config.yaml` | 第一轮接口 URL、模型、请求及运行参数 |
| `configs/media_round2_ai_config.yaml` | 第二轮接口 URL、模型、请求及运行参数 |
| `configs/media_round3_ai_config.yaml` | 第三轮接口 URL、模型、请求及运行参数 |

出厂配置中的数据路径、接口 URL 和模型留空；填写前会在启动检查时报错，不回退到官方配置。来源表与两张目标表必须不同，两份配置里的第一轮路径必须一致。目标使用独立媒体新路径。

第一轮路径映射为公共函数内部的 task_path/first_round_task_path；第二三轮的相同路径映射为 result_path。媒体公开配置保持 round1/2/3_result_path 命名，官方原配置键不变。

三轮密钥分别使用 MEDIA_ROUND1_AI_API_KEY、MEDIA_ROUND2_AI_API_KEY、MEDIA_ROUND3_AI_API_KEY，放入运行环境或同目录 `configs/.env`。进程环境优先，媒体不修改官方三轮 AI 配置或密钥。根据实际公司接口调整 payload 中的模型及参数。

使用 Python 3.8.5 和能够导入 `xdata.client` 的正式环境。本机隔离环境中的离线测试使用模拟客户端，不代表已经连通真实 SDK、AI 或数据库。

## 原帖输入和初始化

初始化支持数据库源表和本地 JSON 文件，在 `configs/media_post_extraction_config.yaml` 中二选一填写来源。

**本地 JSON 文件：** 无需先建立原始帖子表。示例配置如下，文件相对路径从本配置所在目录计算，也可使用绝对路径：

```yaml
source_path: ""
json_files:
  - "../media_posts.jsonl"
round1_result_path: "填写尚未创建的媒体第一轮任务表路径"
ai_config_path: "media_round1_ai_config.yaml"
start_time: null
end_time: null
```

支持每行一条帖子的 JSONL/NDJSON、JSON 数组和单条 JSON 对象，可同时填写多个文件；`.json` 文件也兼容每行一条帖子的格式。输入读取 `id`、`created_at`、`text`、`user.screen_name`：

- `id` 接受非空字符串或整数，大整数精确转为字符串，不经过浮点数；拒绝浮点数、布尔值和空 ID。
- `created_at` 是发布时间，不使用 `fetched_at`。带时区的时间转 UTC 后移除时区，无时区时间按 UTC 理解，保留时间精度。例如 `Tue Feb 17 00:25:36 +0000 2026` 对应任务索引 `2026-02-17 00:25:36`。
- `text` 原样保留，`user.screen_name` 写入 `user_name`。点赞、转发、引用等其他字段不进入任务表。
- JSON 导入时过滤正文以 `RT @` 或 `JUST IN` 开头的帖子（区分大小写，不去除前导空格），不按账号或配置日期过滤，包括示例中的 WuBlockchain。过滤后所有文件内相同 ID、发布时间、正文和账号的记录合并；同 ID 任一任务输入不同则在连接 SDK 和注册目标之前报错，并给出冲突字段及来源位置。所有文件均无可导入帖子时停止，不创建空任务表。此规则仅在初始化导入时生效，不会自动清理已导入的任务。

**数据库源表：** `json_files: []`，`source_path` 填已筛选好的媒体原帖表。原始表需要 `(date, id)` 索引，date 为完整发布时间、id 唯一，普通列至少包含字符串 text、user_name。

两种模式均完整读取来源，不调用 AI、不按处理日期截取；`start_time/end_time` 只控制之后第一轮 AI 的处理范围。初始化不读取 AI 配置，不需要提前填写接口、模型或密钥。使用相同入口运行：

```powershell
python -X utf8 -m entrypoints.initialize_media_tasks
```

本命令只用于初始化新的第一轮目标：先验证数据，然后 `cli.register(path, "timeseries", "")` 和 `cli.write(path, df)`。不把它当作重置已有结果的命令。若注册成功而写入失败，先核对实际目标状态，再按现有客户端恢复规则处理。

## 独立运行三轮

```powershell
python -X utf8 -m entrypoints.run_media_round1
python -X utf8 -m entrypoints.run_media_round2
python -X utf8 -m entrypoints.run_media_round3
```

这是三个分别启动的阶段。第一轮正常结束后自动把本次范围成功帖展开、补入媒体事件表；普通单帖失败的成功部分仍可导入，整体异常或中断不开始导入。

事件表首次缺表时 register/write；已有表仅用 update_by_index 追加缺少的记录。各轮状态和回答通过 update_by_column 更新，已成功的 AI 任务按状态跳过。

第二轮开始前，指定范围第一轮必须全成功，并确认事件导入已经正常完成。第一轮日期取媒体帖子配置；第二三轮日期取媒体事件配置，必须同时填写起止日，并包含首尾日全天。

第三轮默认跳过第二轮未完成记录，仅处理目标范围内已经归组成功的记录。第二轮补跑后，重新运行第三轮补齐。可通过 round3_skip_incomplete_round2 关闭跳过、要求整段归组完成。

## 对象与候选历史

第一轮输入只包含账号和正文，不发送 account_tickers。输出仍是 events 数组，每项 event 和 tickers；主体、报道来源、消息状态和关键条件保留在描述中。

所有对象去首尾空白、转大写、单事件内去重。允许具体加密资产、INDUSTRY、MACRO；不会把项目名或交易所名自动映射为代币。

空 events 是正常无事件结果。tickers 为空的事件保留在第一轮回答中，不强造目标，也不生成无目标的后续任务。

第二轮按事件 × 对象生成独立记录，稳定 ID 为 source_post_id:event_index:asset。相同对象按时间推进；不同对象可并发；同刻事件只看到此前历史。

默认历史为 `[当前披露时刻向前一个日历月, 当前披露时刻)`，不是固定 30 天；包含下界，排除同刻及未来。对窗口内同对象已完成记录按事件组组织，发送全部组，不使用相似度、Top N 或向量服务。候选为空仍向 AI 发送空 candidates。

行业、宏观两个桶可能较大。可设置 round2_max_input_characters 为正整数，限制每次业务 JSON 的字符数，超限时不发送 AI、不截断，保存准备失败并暂停该对象。这个数不包含 system 提示词，不等同模型 token 容量；默认 null 不限制本地字符数，仍受实际接口上下文限制。DEBUG 日志记录候选组数和输入字符数。

如果设置 round2_asset_list_path，沿用公司 InstrumentList 解析方式筛选具体资产，同时保留 INDUSTRY/MACRO；留空处理所有对象。名单筛选不删除提取结果或事件行。

## 归组和影响结果

第二轮 AI 返回 new/update/repeat/uncertain。uncertain 沿用现有逻辑按 new 保存，使用自己的 record_id 建组；round2_ai_response 中仍保留原始 uncertain 正文。

第三轮只为业务关系 new/update 请求 AI，update 发送完整变化说明。输出 direction、materiality、execution_stage、persistence、materiality_reason 五字段，继续使用现有枚举校验。

repeat 复制同对象、同组、严格此前最近的有效第三轮回答，找不到来源则明确失败。复制的 usage 和 attempts 属于来源，不计为新增调用。

事件表保留现有 14 列以复用 Store 和校验，embedding 列始终留空，不代表执行或加载向量。第二、三轮只更新自己允许的字段；两轮的业务状态和原始回答分别存储。

## 独立补导、审计与续跑

需要单独补导或严格核对第一轮展开结果时：

```powershell
python -X utf8 -m entrypoints.import_media_events
```

独立导入只对本次配置范围展开、核对固定输入。共同索引的 event/asset/source_post_id 冲突、目标在本范围多出源记录时停止；仅将索引缺失且 date 严格晚于目标表全表最大时间的记录用 update_by_index 追加，不覆盖已有分析。

自动导入和独立导入均不补较早或同刻缺失任务，只追加严格晚于目标表全表最大 date 的事件，保留完整时间精度，所有对象共用截止时间；跳过数量记日志，空表首次导入不受限制。自动导入依赖第一轮成功内容固定，不额外比较已有事件正文。纯新成功批次按元数据只读表尾日的 asset 一列及索引，取得准确截止时刻；包含历史成功帖时仍读取目标表核对已有索引。两种路径均不重写已有成功业务结果。

每阶段复用现有进程锁，同阶段同表不能并行跑不同日期。AI 配置摘要按本轮全表核对，提示词或模型参数冲突不会自动重算成功任务。历史窗口或候选规则不在 AI 摘要中；改处理口径时使用新结果表或明确重算范围，第一轮结果可复用。

存储错误在同一进程内重试缓冲的同一份 AI 结果，不重调模型。写入确认后才更新内存工作表和第二轮历史。未落库缓冲不是持久队列；进程退出后重新运行可能再次请求未落库的 AI 任务，已落库成功任务继续跳过。

INDUSTRY/MACRO 的分析是风控研究输入，不直接使用原币种权重入口追加期货合约后缀。正式风控组合和做空映射另行设计。

## 验证记录

2026-11-09 在 Python 3.8 环境运行完整 `unittest discover -s tests -t .`，658 项全部通过，其中新增媒体相关测试 15 项。覆盖两表 register/write、增量补导与严格冲突检查、跨配置路径一致性、超过 10 组的全量候选、月末与同刻边界、超限不请求 AI、对象范围、uncertain 转 new、repeat 继承和成功任务续跑。

完整回归包含原有绘图测试，项目隔离环境已补装 Matplotlib 3.7.5；核心 requirements.txt 未改动。测试中的 AI 和数据库均为模拟对象，尚未进行真实媒体数据的语义质量和接口验收。
