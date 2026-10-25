# 第二轮成功记录的相似度只读观察

`entrypoints.audit_similarity` 复用事件去重分析表内的 `embedding`，在本地重算候选并导出 CSV。无论第二轮是否全部完成，都只统计 `round2_status="1"` 的成功记录，跳过待处理、处理中、失败及其他非成功状态的记录。数据库仅用于读取；配置资产名单时另用 `read_object` 读取名单。不调用 bge-m3 或归组 AI，不恢复状态，不更新表结构或业务结果。读取数据库需要已有的 xdata 环境；计算和导出在本地进行。

## 运行

在项目根目录、运行第二轮的同一 Python 环境中执行，默认使用 `configs/event_processing_config.yaml` 的结果表、日期和历史月数：

```powershell
python -X utf8 -m entrypoints.audit_similarity
```

日期指定需要观察的范围，无需等待整轮完成。也可以只在命令中覆盖观察日期，无须改配置。以下日期只是格式示例，须替换为实际批次：

```powershell
python -X utf8 -m entrypoints.audit_similarity --start-time 2021-01-01 --end-time 2021-01-31 --output-dir reports/similarity_january
```

默认只从 `embedding_config.yaml` 读取 `similarity_threshold`，不要求模型地址或密钥。用参数覆盖阈值、同时比较多个阈值或增加导出候选组数：

```powershell
python -X utf8 -m entrypoints.audit_similarity --threshold 0.5 --thresholds 0.4 0.45 0.5 0.55 0.6 --top-k 20
```

`--threshold` 决定 `events.csv`、`candidates.csv` 中的筛选标记；`--thresholds` 是 `thresholds.csv` 要对比的阈值列表。例如 `--threshold 0.6 --thresholds 0.5 0.6 0.65` 中，0.65 只是额外的对照值，可删除。这些参数均不修改正式配置或数据库，也不会重新调用归组 AI。日期范围内只有第二轮成功记录参与统计；整轮可以尚未完成，未完成记录不会计入。用于比较的前史也只取第二轮成功记录，可以位于观察日期范围之前，但仍受每条记录的历史窗口限制。

`--config` 可指定其他事件配置。指定的输出目录必须尚不存在，默认新建 `reports/similarity_audit_北京时间/`。`reports/` 已加入 Git 忽略规则。无需重跑 `entrypoints.run_round2`。

## 输出和观察方法

CSV 使用带 BOM 的 UTF-8，保留完整事件文本和相似度数值，可以直接用 Excel 打开。每条第二轮成功的当前事件都导出，默认每条导出阈值筛选前分数最高的 10 个事件组，包含低于阈值的组。范围内没有成功记录时，仍生成带表头的 CSV、零值阈值统计和完成说明，不再读取前史。

**只想看“每条事件最像哪条历史事件、分数多少”，直接打开 `events.csv`。** 新导出文件的前三列并排展示以下内容，每行对应一条当前记录：

| 列名 | 含义 |
| --- | --- |
| `event` | 当前这条数据的完整事件正文。 |
| `max_similarity_event` | 与它相似度最高的那一条历史记录的完整事件正文。 |
| `max_similarity` | 这两条事件向量计算出的相似度数值。 |
| `tenth_candidate_similarity` | 达到观察阈值、最终入选的候选组中第 10 名的相似度。不足 10 组时留空；不受 `--top-k` 导出数量影响。 |

`max_similarity_record_id` 可定位这条最相似历史记录，`max_similarity_event_id` 是它所属的事件组。这里的“最相似”按第二轮原有候选范围计算：同币、当前时刻之前、配置的历史窗口内，排除自己、同刻和未来。最高分低于 0.5 也会显示，便于观察被阈值拦下的关联。没有可比较历史时，历史正文和分数都留空，`recomputed_decision` 为 `no_history_auto_new`。

| 文件 | 内容及用法 |
| --- | --- |
| `events.csv` | 每条当前事件一行。查看 `max_similarity`、`max_similarity_event`、`max_similarity_record_id` 和 `max_similarity_event_id`，即可对照最高分的具体历史事件及所属组。包含表内实际关系、历史数量和本次复算的筛选结果。 |
| `candidates.csv` | 每条当前事件与各候选组的对照。`current_event` 对照 `best_record_event`；`similarity` 为组内最高分；`best_record_id` 是贡献该分数的具体历史记录；`candidate_event_id` 是事件组 ID。 |
| `thresholds.csv` | 固定既有历史分组，在不同阈值下估算多少事件会交给 AI、多少会自动判新及候选组总数。另统计现有 update/repeat 的匹配组是否被该阈值或前 10 限制排除。 |
| `manifest.json` | 完成标记、日期、阈值、模型/版本/向量摘要、统计及复算限制。完整导出后才生成；缺少该文件表示执行未完整完成。 |

建议从 `events.csv` 筛选 `recomputed_decision=below_threshold_auto_new`，优先看最高分接近 0.5 的记录；再在 `candidates.csv` 中按当前 `record_id` 查看其他候选，判断是否属于同一具体事项。`manual_has_historical_match`、`manual_same_matter` 和 `manual_note` 留给人工填写，本工具不自动生成真值。

同时抽查更低分及更高分样本。只观察 0.5 附近或最高分的一对，不能估计全批次漏检率；真实匹配也可能是排名第二到第十的组。低分的同一事项提示阈值可能漏检；高分的不同事项说明语义相似不等于同一事件，仍须归组 AI 判断。

`passes_threshold` 表示分数大于等于观察阈值；`would_send_to_ai` 表示在固定既有分组下，按该阈值及前 10 规则会提交 AI，**不是历史调用审计**。`is_stored_match` 对应表内 update/repeat 的最终匹配组，未必是最高分组。`stored_matches_excluded` 是现有匹配组被新筛选规则排除的数量，包含候选历史中已经找不到的匹配组；后者另列 `stored_matches_missing_from_history`。现有 AI 判断不是人工真值，这些数字不能当作准确率或召回率。`send_to_ai_events` 不含重试，不等于日志中的 HTTP 次数。

## 复算口径与限制

- 当前记录按配置起止日全天选择，再按 `round2_asset_list_path` 配置的资产名单（未配置或为空时不限资产）和 `round2_status="1"` 筛选。只统计第二轮成功记录，无需等待整个日期范围或整轮完成；第三轮状态不影响统计。没有运行 ID 时，不能区分同一日期范围内“这次新完成”和“以前已完成”的记录。
- 先读本批记录，再补读最早所需窗口下界到本批起点前一天的历史；只读所需列，不读全表正文、模型回答或向量服务配置密钥。
- 候选仅为同币、当前时刻之前且在 `history_lookback_months` 窗口内的第二轮成功记录。包含窗口下界，排除同刻与未来。前史中非成功记录直接跳过，不阻止导出，也不读取其向量参与计算。
- 保留原向量数值，以 float64 内积并截断到 [-1, 1]，按组内最高分排序，同分组按 `event_id` 排序。最高分来源有多条同分记录时，选时间和 `record_id` 最早的一条。正式提交最多 10 组；`--top-k` 只控制导出数量，不改变这条规则。
- 只使用参与比较的已保存向量。缺失、格式损坏、非单位向量、维度不一致或混用模型/版本/配置摘要时停止，不生成或覆盖向量。没有历史的记录相似度留空，另标 `no_history_auto_new`，不记为 0。
- 表内向量、历史记录、已完成分组或历史窗口后来有变时，结果可能不等于原运行时的候选。本工具不是不可变的历史审计快照。
- 多阈值统计固定使用目前已保存的分组；实际重跑整轮可能改变此前分组及后续候选，因此它不预测重跑后的全部 AI 结果，也不改动现有结果。
- 当前仅支持内置纯向量检索，配置自定义 `retriever` 时拒绝复算。

## 本地验证记录

2026-10-25 调整为仅统计第二轮成功记录后，`tests.round2.test_similarity_audit` 的 18 项模拟测试全部通过。覆盖当前批次和前史混有待处理、处理中、失败及其他非成功状态、配置资产名单、没有成功记录时的空报表，以及命令入口无需等待整轮完成；未连接真实数据库或模型。

2026-10-19 在 Python 3.8.5、NumPy 1.24.4、Pandas 2.0.3 下，`tests.round2.test_similarity_audit` 的 13 项模拟测试全部通过；命令帮助及依赖一致性检查通过。覆盖只读调用、组内最高分来源、与正式检索结果一致、低分候选保留、阈值相等、前 10 限制、同刻排除及五年闰日窗口、缺失或混用向量停止、完整 CSV 文本保存。未连接真实数据库或模型；本地环境尚未接入内部 `xdata`。

完整项目测试发现 340 项，但在 Windows 异步事件循环创建本机 `socketpair` 时等待未结束，已停止，不能记为完整通过。该阻塞与已经通过的只读导出专项测试分别记录；底层原因尚未通过沙箱外对照确认。
