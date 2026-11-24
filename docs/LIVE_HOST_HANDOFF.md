# 实盘接入说明（0.4.3）

每次爬虫完成后，宿主将本次新帖与所有未成功原帖合并、按 id 去重，再 await 调用一次函数。包内仅通过内存完成三轮、向量化和 weight；宿主负责 Redis/xdata 读写。

0.4.3 的事件导入仅追加严格晚于历史事件表全表最大 `date`（UTC 原帖发布时间）的任务，所有资产共用完整时间截止。较早和同刻任务跳过并记日志，空历史不受限制；跳过的任务仍保留第一轮输出，但不返回新增事件行。晚到的旧发布时间帖子也会跳过，已经存在的历史缺口不自动修复。

线程流程图、各线程职责及超时停止过程见 [详细说明](LIVE_INTEGRATION.md#线程分工与流程图)。

**安装及启动**

```bash
python -m pip install wheels/crypto_x_ai_live-0.4.3-py3-none-any.whl
```

使用 Python 3.8 环境。将业务方确认的接口、模型、提示词、评分和向量配置放入 configs/ 的五份 YAML；密钥变量见 configs/.env.example，可复制为同目录 .env 后填写。启动时初始化日志、加载配置一次。

```python
from crypto_x_ai_live import load_live_config
from call_example import init_logging, process_batch

init_logging()  # 默认终端及 logs/crypto_x_ai_live.log，按天轮转。
config = load_live_config("/实际绝对路径/configs", max_attempts=100)

# 以下代码放在宿主 async 函数中，每批串行调用：
result = await process_batch(new_posts, history_results, assets, config=config,
                             timeout_seconds=840)
if result["status"] == "success":
    round1_results = result["round1_results"]
    event_results = result["event_results"]
    weight = result["weight"]
    # 宿主统一提交三张表，再清除本批积压/推进成功批次标记。
else:
    # 只收到 failed 或 timeout 状态，不写业务表；原帖留到下轮合并重算。
    pass
```

0.4.3 沿用异步接口，不兼容旧同步调用，不再使用 resume_state、LiveBatchError 或检查点。异步宿主直接 await；独立命令行脚本可在顶层用 asyncio.run(main())，不要在已运行的事件循环里再调用 asyncio.run。

**输入**

| 参数 | 格式 |
| --- | --- |
| new_posts | 本次新帖加所有未成功原帖，按 id 去重。DataFrame 的单层索引名 date，表示爬虫时间，可重复，可为有效时间字符串。普通列至少有 id、created_at、text、user_name；id 为非空唯一字符串，text/user_name 为字符串，created_at 为原帖 UTC 发布时间。 |
| history_results | 第二、三轮共用结果表中过去 100 天的成功数据 B，使用唯一的 (date, record_id) 复合索引。保留原 14 列及已有两列调用统计；首次确无历史可传 None。目标资产历史及向量须完整、与配置匹配。 |
| assets | 宿主从 xdata 原 round2_asset_list_path 对应 JSON 中取出的 InstrumentList 字符串数组，不传路径或整个 JSON。同币名不重复。 |
| timeout_seconds | 整轮计算预算，默认 840 秒。全轮共用预算，进入下一轮或重试不会重置。 |

B 的 14 个基础列：

```text
event_id, event, asset, source_post_id, event_relation, new_information,
round2_ai_response, round3_ai_response, round2_status, round2_config_hash,
round3_status, round3_config_hash, error_message, embedding
```

保留 round2_call_meta、round3_call_meta；旧 B 无这两列也接受。示例会过滤正文以 RT @ 或 JUST IN 开头的帖子，区分大小写。没有新帖也应调用，可传空 DataFrame。不把 created_at 或 B.date 预先加 8 小时。

**输出及失败**

| 返回 | 宿主操作 |
| --- | --- |
| status=success，附 round1_results、event_results、weight | 第一轮表按 (date,id) 更新；事件增量按 (date,record_id) 更新，不能替换整个 B；weight 追加一行。 |
| 仅 {"status": "failed"} | 不写业务表，保留原帖，下轮从头重算；原因见日志。 |
| 仅 {"status": "timeout"} | 本轮网络、准备和协调线程已退出，客户端已关闭；不写业务表，保留原帖，下轮重算。 |

两张处理表的 date 都是原帖 UTC 发布时间；weight.date 是最终返回前的 UTC+8 系统时间。weight 窗口使用本轮开始时刻对应的 15 分钟刻度。示例用 max_abs，需要 sum 时改示例参数；宿主不再聚合整点 weight。

成功输出第一轮为 10 列，事件表为 16 列，含 round1_call_meta、round2_call_meta、round3_call_meta 三个 JSON 字符串统计列，必须完整保存。失败轮不保存、不累计到下一次重算。

**时限及调度**

- 840 秒为计算预算，取消关闭资源还可能额外耗时；宿主须将读写 Redis 和收尾计入 15 分钟总预算，根据实测调整预留时间。
- 内部复用原执行器的准备线程、网络线程和协调提交线程，宿主异步等待完成；取消会通知内部停止并等待线程真正退出。同进程不能强杀底层库卡死或长时间阻塞的线程，不是进程级硬时限。宿主主动取消 await 任务时，包收尾后传播 CancelledError；使用内置 timeout_seconds 则返回 timeout 状态。
- 宿主应保证下次调用函数前，旧进程已经结束，前一批未结束时不要并发启动后一批。同进程部署时，对应要求是前一调用及其所有任务已结束后再启动下一轮。
- 连续失败时累计保留所有未成功原帖。若爬虫已有原帖持久化表，可按上次成功批次位置重读，不必保存 AI 中间进度。
- 要求重启前，Redis里的数据已更新进xdata，读取的时候已经是更新后的xdata。清空 Redis 后恢复成功历史 B，并确保未成功原帖及成功批次标记可恢复。
