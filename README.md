# AI 帖子批量处理

本工具先把现有帖子建立为任务，再调用 AI 处理指定日期范围的任务，保存每帖最新结果。项目结构见 [项目总览](project_overview.md)，重要处理行为见 [关键规则](docs/KEY_RULES.md)，字段见 [任务表](docs/data/TABLE_DESIGN.md)。

## 准备环境

使用 Python 3.8，并安装项目锁定的公共依赖：

```powershell
python -X utf8 -m pip install -r requirements.txt
```

运行还需要原有环境提供的 `xdata` 包及其连接、认证配置，使 `from xdata.client import get_client` 可用。该包的安装来源和版本尚未核实，请沿用原有安装方式。

处理任务时，客户端须提供 `read` 和 `update_by_column(path, df)`；后者按原索引只更新提交列，保留其他字段。原实现误用 `update_by_index` 已导致任务输入和配置标识缺失，已有受影响数据的恢复方法见 [关键规则第 7 节](docs/KEY_RULES.md#7-保存时只改哪些内容)。

| 文件 | 需要填写的内容 |
| --- | --- |
| `job_config.yaml` | 源帖子路径 `source_path`、任务表路径 `task_path`、AI 配置路径 `ai_config_path`、处理起止日期和日志设置。 |
| `ai_config.yaml` | 提示词、真实 AI 接口 URL、请求参数及运行参数。 |
| 环境变量或 `.env` | AI 认证信息；默认认证配置读取 `AI_API_KEY`。可参考 `.env.example`。 |

`.env` 放在 AI 配置文件所在目录，同名进程环境变量优先。公司接口请求体使用 `messages`，模型及其他参数由 `api.payload` 提供。默认空 URL、缺失或为空的认证变量会使批量处理启动失败。

## 第一次建立任务表

在 `job_config.yaml` 中填写实际 `source_path` 和尚未创建的新 `task_path`，两个路径必须不同。从项目根目录执行：

```powershell
python -X utf8 -m integration.initialize_tasks
```

初始化完整读取现有帖子，校验后注册目标，再一次写入完整任务表。它不使用处理起止日期，也不读取 AI 配置或调用 AI。源数据要求见 [关键规则](docs/KEY_RULES.md)。

成功建立任务表后运行 `main.py`。初始化失败时先核查目标是否已注册、是否已有部分数据；使用尚未注册的新目标，或按真实客户端规则处理原目标后再重跑。仅清空数据不代表重复注册一定安全。

## 批量处理已有任务

设置好任务表路径、AI 配置及认证信息，从项目根目录执行：

```powershell
python -X utf8 main.py
```

起止日期使用带引号的 `YYYY-MM-DD` 字符串，包含起止日期全天。例如处理 2021 年全年：

```yaml
start_time: "2021-01-01"
end_time: "2021-12-31"
```

两端必须同时设置或同时不设置，起点不能晚于终点；起止同日表示处理当天。有日期范围时按自然月依次处理；没有两端日期时读取全表一次。

入口配置固定从项目根目录读取；相对的 `ai_config_path` 按项目根目录解析，数据路径原样交给客户端。已有应用也可自行管理客户端，直接调用：

```python
from ai_batch import run_worker

summary = run_worker(
    cli,
    settings["task_path"],
    settings["ai_config_path"],
    start_time=settings.get("start_time"),
    end_time=settings.get("end_time"),
)
```

该入口会阻塞调用线程。应用内初始化使用 `integration.initialize_tasks.initialize_existing_posts(cli, settings)`；仅导入初始化接入模块不会创建客户端。

## 日志与调试

两个独立入口通过 `job_config.yaml` 的 `logging` 设置日志，默认显示 INFO；应用内调用包接口时由应用配置日志。INFO 显示月份、进度及完成统计，WARNING 显示重试和单帖失败，ERROR 显示整体停止及保存故障，DEBUG 可查看逐帖和保存细节。

批量处理异常使用标准日志显示类型、消息、调用栈和异常链；初始化入口显示异常类型和具体原因。常规日志不主动打印正文、提示词、认证信息或完整响应，外部异常消息仍按原文显示。“worker 收尾结束”只表示清理流程结束，不代表任务全部成功或结果全部保存。

正常退出码为 0，普通异常为 1，用户中断为 130。停止整个程序是当前的停止方式；已保存状态和重启行为见 [关键规则](docs/KEY_RULES.md)。

VS Code 中打开项目根目录，选择已安装依赖和 `xdata` 的解释器，打开 `integration/initialize_tasks.py`，使用右上角的 **Python Debugger: Debug Python File**。现有 `.vscode/launch.json` 为这个按钮提供项目导入路径和 UTF-8 设置；终端初始化使用上面的模块命令。

## 本地验证与部署

可运行模拟测试：

```powershell
python -X utf8 -B -m unittest discover -s tests -v
```

工作区已有 `.python38` 验证环境时，可将 `python` 换成 `.python38/python.exe`。现有记录为 Python 3.8.10 下 102 项模拟测试通过，包含并发乱序答复的对应关系、每批索引严格递增和部分写入重试；真实客户端、AI 接口和大规模容量仍需接入验证，具体限制见 [关键规则](docs/KEY_RULES.md)。

部署时携带 `main.py`、完整的 `ai_batch` 和 `integration`、两份 YAML 配置、`requirements.txt`，以及按需配置的 `.env`；在新机器安装 Python、公共依赖和原有 `xdata` 环境。
