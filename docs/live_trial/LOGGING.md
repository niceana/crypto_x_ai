# 查看原业务模块日志

调用脚本现在会显式初始化 Python logging，让已安装包中三轮、向量、请求执行器的原有日志同时写到终端和 UTF-8 文件。业务 wheel、提示词、处理和 weight 算法没有改动，不需要重新安装业务包。

日志补充包有 _trial.py、run_full.py、replay_hours.py 和本说明。放到原试跑目录替换同名文件；替换 replay_hours.py 时保留自己已经填写的 DATA_DIR、CONFIG_DIR、MODE、MAX_ATTEMPTS、RESUME_DIR、OUTPUT_ROOT。export_xdata.py 的表路径不会被覆盖。

两个调用脚本顶部均可设置：

```python
LOG_LEVEL = "INFO"
LOG_TO_FILE = True
```

INFO 显示旧代码中的启动、处理进度、向量复用、重试、异常等 INFO/WARNING/ERROR 日志；改成 DEBUG 可查看旧代码已有的请求起发、准备等更细日志。httpx/httpcore 的第三方 INFO/DEBUG 日志保持关闭，源业务网络执行器自己的日志仍可见。

日志示例：

```text
2026-11-11 16:00:00 UTC+8 | INFO | crypto_x_ai_live._engine.round1.runner:55 | MainThread | batch=2021-03-20 00:02:00 | round1事件提取开始启动
```

前面的日期是实际运行时刻（固定 UTC+8），batch 是当前模拟批次时间，模块名及行号标出日志来源。工作线程同样显示所属批次。crypto_x_ai_live._engine 是 wheel 内原业务源码的私有命名空间。

日志位置：

- 单批 run_full.py：本次 outputs/full_.../run.log。
- 连续 replay_hours.py：本次 outputs/replay_full_.../run.log；复用 RESUME_DIR 续跑会追加到同一文件。

运行命令不变：

```bash
/opt/ap/python/anaconda3/bin/python replay_hours.py
```

另一终端实时查看文件：

```bash
tail -f /你的试跑目录/outputs/replay_full_本次时间戳/run.log
```

tail -f 会持续显示新追加的日志。此终端按 Ctrl+C 只会停止查看；回放在原终端继续运行。

已经运行中的 Python 进程不会自动加载新脚本。如果当前回放仍在跑，先在回放终端按一次 Ctrl+C，等待资源关闭并结束，再替换文件；把 RESUME_DIR 填为原输出目录后重新执行回放命令。成功批次会恢复增量，不重复请求 AI；中断批次从头重算，不保存 AI 中间结果。原本未启用的 INFO/DEBUG 日志无法补回，只能记录重新启动后的处理过程。
