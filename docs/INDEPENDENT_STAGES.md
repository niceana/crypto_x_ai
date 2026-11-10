# 独立阶段运行说明

处理顺序为：第一轮提取并导入 → 独立向量准备 → 第二轮归组 → 第三轮分析。各入口独立运行，使用自己的重试与数据库状态恢复，不自动等待上游或启动下游。权重生成仍单独执行。

## 配置与交接

- 第一轮读取 `configs/post_extraction_config.yaml` 的任务表和日期；正常结束后，才将本次范围内成功帖自动导入事件表。
- 向量、第二轮、第三轮读取 `configs/event_processing_config.yaml` 的表路径和日期。`first_round_task_path` 须指向实际第一轮任务表，`result_path` 须与第一轮自动导入目标相同。日期不会自动沿用第一轮配置。
- 向量和第二轮入口只读检查配置范围内的第一轮状态，存在未成功帖子时停止。第一轮全成功后仍须确认导入完成，再进入下游。
- 第三轮保留 `round3_skip_incomplete_round2` 的原有行为：当前配置为 `true`，可以先处理第二轮成功部分，跳过未完成记录；关闭时要求整个目标范围第二轮完成。补跑第二轮后，再运行第三轮补齐。
- 需要分月交接时，分别设置已就绪的月份范围并手动启动。第一轮独立入口按本次整个配置范围处理结束后自动导入；处理中完成某个月，不代表该月已经导入。

三轮分别使用 `ROUND1_AI_API_KEY`、`ROUND2_AI_API_KEY`、`ROUND3_AI_API_KEY` 和对应 YAML 的 `run.concurrency`。向量使用独立的 `EMBEDDING_API_KEY` 及 Embedding 配置。请求准备队列、HTTP 并发、批量读写、矩阵缓存和既有重试规则保留；第二轮只读向量，不调用向量服务。候选窗口、组内最高分、阈值和排序规则不变。

向量及第二、三轮各自在启动时重新读取公司资产名单，名单允许变化；已有成功记录保留。不同入口启动时间不同，读到的名单可能不同。

## 按顺序运行

在项目根目录、已安装依赖并可导入 `xdata` 的 Python 环境执行。Windows 可将 `python` 替换为 `.\.python38\venv\Scripts\python.exe`。

```bash
python -u -X utf8 -m entrypoints.run_round1
python -u -X utf8 -m entrypoints.prepare_vectors
python -u -X utf8 -m entrypoints.run_round2
python -u -X utf8 -m entrypoints.run_round3
```

这些是四个分别执行的命令。每步结束后核对日志和统计，再启动下一步；普通单条失败可能仍以退出码 0 结束，不能仅凭正常退出判断全部成功。

## Linux 后台运行

第一轮：

```bash
nohup python -u -X utf8 -m entrypoints.run_round1 >> round1.log 2>&1 &
```

确认对应范围第一轮全部成功且导入完成后，可将向量准备和第二轮串联为一个后台任务：

```bash
nohup sh -c 'python -u -X utf8 -m entrypoints.prepare_vectors && python -u -X utf8 -m entrypoints.run_round2' >> round2.log 2>&1 &
```

向量准备异常退出时，`&&` 不会启动第二轮。两步分别在各自启动时加载事件配置，等待第二轮启动期间不要修改这份配置，否则两步可能使用不同日期或表路径。

第三轮，在其配置范围已具备可处理的第二轮结果后启动：

```bash
nohup python -u -X utf8 -m entrypoints.run_round3 >> round3.log 2>&1 &
```

三条后台命令分别管理自己的任务；启动下游前仍需满足上述交接条件。入口会阻止同一阶段、同一表的重复启动，日期不同也不能双开。各入口内部的请求准备与网络线程继续保留。

查看对应日志，例如：

```bash
tail -f round2.log
```

`-u` 及时输出日志，`>>` 追加保留原日志，`2>&1` 合并标准错误，末尾 `&` 后台运行。退出 `tail` 不停止任务。

## 防止重复启动

启动命令不变，入口自动加本机操作系统进程锁，不需要额外参数或调度程序：

| 入口 | 互斥依据 | 持锁范围 |
| --- | --- | --- |
| `append_tasks` | round1 + 实际 task_path，与 `run_round1` 共用锁 | 原表核对、任务预览或备份及追加 |
| `run_round1` | round1 + 实际 task_path | 创建客户端之前，直到提取及自动导入全部结束 |
| `prepare_vectors` | vectors + result_path | 整次向量准备 |
| `run_round2` | round2 + result_path | 包括上游检查、状态恢复、AI 和写入 |
| `run_round3` | round3 + result_path | 包括状态恢复、AI 和写入 |
| `import_events` | import_events + result_path，并占用 round1 + first_round_task_path | 整次独立导入；不能与同来源第一轮同时运行 |
| `write_weight_data` | weights + weight_path | 读取事件及生成、写入权重；`--dry-run` 不加锁 |

日期、资产名单、并发、AI 配置及代码所在目录不参与锁身份。因此同一表不能开两个第二轮来分别处理不同月份；不同表可以运行同一阶段。同一事件表的第一轮、向量、第二轮和第三轮可以同时持锁，仍须自行满足数据交接条件。第一轮自动导入时还会短暂占用该事件表的 import_events 锁，避免与其他导入互相覆盖。

重复启动的新进程立即退出，退出码为 **2**，日志提示目标表及可读取的占用阶段、PID、启动时间和日期；原进程继续运行。锁在数据库读取、状态恢复之前取得，避免第二个进程将正在处理的状态 9 恢复为 0。加锁目录不可用或加锁异常时也会停止，不会跳过保护继续处理。


Linux 使用 `fcntl.flock`，默认锁目录为 `/tmp/crypto_x_ai_locks`；Windows 使用系统文件锁，默认为 `%LOCALAPPDATA%/crypto_x_ai/locks`。目录独立于当前工作目录、代码副本和报告目录。同一服务器所有入口必须使用同一个锁目录；需要覆盖时统一设置绝对路径，例如：

```bash
export CRYPTO_X_AI_LOCK_DIR=/var/tmp/crypto_x_ai_locks
```

该目录须可读写，并保留在本机磁盘。运行期间不要删除、替换锁文件或清理锁目录。锁文件长期保留是正常现象；判断占用依靠内核锁，文件内旧 PID 只是诊断信息。正常退出、中断、进程被强杀或服务器重启后，系统自动释放锁，再执行原启动命令即可，无须删除文件或使用 `--resume`。

**首次部署本次更新时，先停止旧版本进程，再用原命令启动。** 已运行的旧进程尚未加锁，更新文件不会自动让它参与保护。保护范围是上述入口在同一台服务器上的运行；直接调用底层 runner、自行写库、其他服务器上的进程不受本机锁约束。多服务器处理同一张表时需要数据库或分布式锁。

## 中断后续跑与扩展年份

中断后重新执行对应阶段的原命令即可。各轮按数据库状态恢复未完成任务，跳过成功记录；向量阶段复用同版本的已保存向量。既有请求、存储重试预算及配置摘要检查保留，不增加自动循环补跑。

统一调度入口 `entrypoints.run_pipeline` 已移除，上述阶段不再使用原调度入口的 `--resume` 或 `--report-dir`。旧的运行清单、报告和日志可保留，但不参与独立入口的续跑；已落库结果继续使用。

从 2023 年扩展到 2025 年时，保留实际任务表、事件表及核心处理配置，分别调整帖子配置和事件配置的日期。先确认第一轮任务表已经包含新增年份帖子；仅原始帖子表有数据不够。处理入口不会自动补入源帖，首次初始化入口也不是增量追加命令，不要在已有任务表上重新初始化。

对于通过 `supplement_posts apply` 补入的批次，使用同一个补录目录追加第一轮任务：

```bash
python -X utf8 -m entrypoints.append_tasks preview "上次补录目录"
python -X utf8 -m entrypoints.append_tasks apply "上次补录目录"
```

`post_extraction_config.yaml` 的 `source_path` 须与补录计划原表一致，`task_path` 使用已有第一轮任务表。先查看 `round1_tasks_to_append.csv`，再 apply；追加前会完整备份任务表，只提交缺失索引，旧任务及 AI 输出保留。日志给出本批日期范围，分别填写到帖子和事件配置后按前述顺序运行。若补入日期早于同币种已成功的第二轮记录，第二轮会按既有规则报“疑似历史补录或历史状态缺口”，须核查受影响历史，不能仅靠缩小日期或跳过检查继续处理。

第一轮完成并导入新增范围后，准备向量，再运行第二、三轮。可保留含 2023 年的范围以复用已成功结果；只跑新增范围时，仍保留第二轮所需前史及向量。调整密钥、并发、重试不改变 AI 配置摘要；修改提示词或模型参数、向量版本仍按原规则检查，不会自动重算成功结果。

## 导入冲突的只读核对

导入提示“已有结果包含固定第一轮输入之外的记录”时，先停止相关范围的写入进程，再执行：

```bash
python -u -X utf8 -m entrypoints.audit_event_import --output-dir reports/event_import_audit_01
```

日期和表路径沿用事件配置，目录必须尚不存在。核对不调用 AI、不写数据库，也不按公司资产名单过滤。它读取并保存两张完整表的本地快照 `first_round_snapshot.pkl`、`event_snapshot.pkl`；核对本身仍限定配置日期范围。快照来自两次独立读取，需事先停止相关写入以免采集到变化中的数据。

`summary.json` 汇总多余、缺失及共同索引下的固定输入冲突；`extra_records.csv` 中 `post_now_has_no_asset_events` 表示帖子仍存在但当前展开为零条有币种事件，`event_id_changed` 表示当前仍有事件但旧 ID 已不存在，`date_mismatch` 表示同 ID 的时间索引不同。`input_conflicts.csv` 列出共同索引下新旧正文、币种或来源帖子变化；`missing_records.csv` 额外标记同币种是否已有更晚的第二轮成功记录。

`potential_recompute.csv` 按资产给出保守的可能重算起点与已有完成数量，包含配置结束日之后的记录。这是评估范围，工具不执行重算或修复。若此次核对需要与 `successful_only=True` 导入一致，可显式加 `--successful-only`；两张快照仍保留读取到的完整表。
