# AI 帖子批量处理

目标环境为 Python 3.8，安装规格锁定的依赖：

```powershell
python -X utf8 -m pip install -r requirements.txt
python -X utf8 -m unittest discover -s tests -v
```

验证环境为 Python 3.8.10，模拟测试结果、规格自查和未验证项见 [实现自查](docs/IMPLEMENTATION_REVIEW.md)，本次未改依赖。工作区保留了 `.python38` 验证环境，可将上述 `python` 换成 `.\.python38\python.exe`。Windows 下建议使用 `-X utf8`，在 Python 侧启用 UTF-8，避免默认编码与终端或重定向工具不一致造成中文乱码。

运行环境还需要你们提供的 `xdata` 包，使 `from xdata.client import get_client` 可用，并按已有数据环境配置连接和认证。它的安装来源及版本未知，未向 `requirements.txt` 添加猜测的包名；按你们原有方式安装。

项目运行文件的职责如下：

```text
main.py                  启动已有任务表的 worker
ai_batch/                核心处理包
integration/             初始化接入与启动包
  initialize_tasks.py    一次性初始化已有帖子，含独立启动入口
job_config.yaml          任务路径、总日期范围和日志设置
ai_config.yaml           提示词、AI 接口和运行参数
.env                     可选的 AI 认证环境变量
requirements.txt         已锁定的公共依赖
```

首次初始化只需填写 `job_config.yaml` 的实际 `source_path` 和 `task_path`，启动命令见下文。处理任务前，再填写实际任务表路径和总起止日期；`ai_config_path` 默认指向根目录的 `ai_config.yaml`，`source_path` 只在初始化时使用。随后填写 `ai_config.yaml` 的公司接口 URL、提示词和 payload。请求体按用户给定的公司接口契约使用 `messages`。认证头已配置为 `Authorization: "Bearer ${AI_API_KEY}"`；密钥放进进程环境或配置文件同目录的 `.env`，可参考 `.env.example`，同名进程环境变量优先。该 YAML 写法等价于 Python 的 `{"Authorization": f"Bearer {os.environ['AI_API_KEY']}"}`，其中环境变量名不能留空，YAML 不执行 Python 表达式。真实接口其余字段仍需验证，默认空 URL 或缺失、为空的认证变量会在读取或更新任务前报配置错误。

任务表已初始化且真实 cli 契约已验证时，从项目根目录执行：

```powershell
python -X utf8 main.py
```

入口读取 `job_config.yaml`，在调用线程执行一次 `cli = get_client()`，直接调用 `run_worker(cli, task_path, config_path, start_time=None, end_time=None)`。worker 将同一客户端、任务路径和当前月范围交给 `TaskStore`，由它直接调用 `cli.read` 和 `cli.update_by_index`。配置定位以 `main.py` 所在目录为准，因此从其他工作目录使用脚本的绝对路径启动也会读取同一份配置。任务表标识原样交给 cli，只有相对的 AI 配置文件路径按项目根目录解析。正常结束退出码为 0，普通异常为 1，用户中断为 130；普通异常使用标准日志输出类型、消息、调用栈和异常链。

`main.py` 处理已有任务表，不自动初始化或清空目标。一次性初始化使用下文的独立启动命令。已有应用可自行创建和管理 cli，准备日志及 AI 配置文件路径后，直接调用包接口：

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

任务配置的起止范围使用带引号的 `YYYY-MM-DD` 字符串，按日左闭右闭，包含起止日期全天。worker 在启动处将两端解析为只含年月日的 `date`，检查两端同时设置及起点不晚于终点，并按自然月生成分段。完整月份从月初到月末，首尾不足整月按总范围截断；例如全年配置为 `start_time: "2021-01-01"`、`end_time: "2021-12-31"`，正常读取 12 段。起止同日表示处理该日；若截止日期设为 `2022-01-01`，则还会处理这一天。

分段生成时直接输出 `YYYY-MM-DD` 字符串；断点处的 `segment_start/segment_end`、`store.start_time/end_time`、分段日志和实际 `cli.read` 参数使用同一组字符串，例如 `start_time="2021-01-01"`、`end_time="2021-01-31"`。读取范围不扩大，也不对返回结果做时间筛选；本地范围校验用截止日的下一日日界线覆盖末日全天，该辅助日期不传给客户端。无界读取只传任务路径；异常读回原样使用同一路径和边界字符串。原任务表、分段范围和保存核对继续校验，任务索引的时间、时区与精度保持原值。真实 cli 的日历时区、截止日全天覆盖与稳定读回仍须验证。初始化沿用既有接入函数及完整写入回调。

`job_config.yaml` 已提供可选的 `logging` 配置，两个独立启动入口会据此初始化日志，应用已有处理器时保持它们。独立入口在未配置 logging 时默认显示 INFO；核心包和初始化接入函数不重复配置日志，在已有应用直接调用它们时，由应用启动代码统一配置标准日志。例如：

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# 避免第三方库的逐请求日志输出接口 URL；本库的日志级别不受影响。
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
```

日志使用 `ai_batch.*` 模块名称和中文消息。INFO 显示启动、月份、进度和完成统计，WARNING 显示重试及单帖失败，ERROR 显示全局停止和存储故障；需要逐帖起发、缓冲提交或解析细节时将级别设为 DEBUG。正常完成提示已使用 INFO 日志，请配置显示 INFO。库不覆盖应用已有处理器，也不自动创建日志文件；应用已有日志系统时直接接收这些模块记录即可。

worker 启动校验失败、异常停止和收尾保存失败，以及 `main.py` 的独立异常出口，直接用标准 `logger.exception` 输出异常类型、原始消息、调用栈和异常链，不自行提取原因或分类。空异常消息沿用 Python 的类型显示。worker 的单帖失败说明仍按既有规则写入任务表的 `error_message`；初始化入口沿用其现有具体原因输出。常规日志不主动打印帖子正文、用户名、提示词、payload、认证信息、URL、原始响应或 DataFrame；异常消息及调用栈由标准日志原样格式化。

首次初始化覆盖全部现有帖子，入口用于尚未创建任务表的一次性建表。先填写 `job_config.yaml` 中实际的 `source_path` 和新的 `task_path`，两个路径必须不同；在项目根目录的终端执行，无需操作参数：

```powershell
python -X utf8 -m integration.initialize_tasks
```

启动入口读取配置，在当前线程执行一次 `cli = get_client()`。接入函数完整读取一次源帖子，将原源对象直接交给 `ai_batch` 校验并构建完整任务表，再由写入回调执行一次 `cli.register(task_path, "timeseries", "")`，正常返回后执行一次 `cli.write(task_path, df)`。三项数据操作串行使用同一个客户端；源、核心输入和任务表统一使用 `user_name`，不做字段重命名。源校验或构造失败时不注册、不写入；注册失败时不写入，不自动重试。空源也先注册目标，再写入合法空表。正常完成日志为“任务初始化完成：已确认=N”。整个初始化过程不读取目标或准备占位空表，不检查实际目标是否已有数据；成功后不要再次建表，重复注册及写入已有目标的效果由真实客户端决定。初始化期间应避免其他程序写入同一目标。

命令行通过 `python -m` 启动，须从项目根目录执行；普通 Python 环境直接运行 `integration/initialize_tasks.py` 时，导入搜索路径只加入 `integration`，可能找不到同级的 `ai_batch`。配置定位以模块所在目录为准，源路径和任务路径原样交给 cli。已有应用可调用 `initialize_existing_posts(cli, settings)`，仅导入它不会导入 xdata 或创建客户端。核心初始化接口为 `initialize_tasks(source_df, write_tasks)`，已删除旧目标读取参数；外部调用须同步更新，复制到其他机器时同时更新 `ai_batch` 和 `integration`。

VS Code 调试时，用“打开文件夹”打开本项目根目录，选择已安装项目依赖和 `xdata` 的 Python 解释器，再打开 `integration/initialize_tasks.py`，点击右上角下拉菜单中的 **Python Debugger: Debug Python File**。项目提供的 `.vscode/launch.json` 使用 `purpose: ["debug-in-terminal"]`，让这个按钮采用当前文件调试配置；配置把项目根目录加入 `PYTHONPATH` 并保留已有搜索路径，开启 UTF-8，不修改脚本导入或业务逻辑。该配置专供右上角按钮，F5 不使用它，终端运行仍使用上面的模块命令；配置字段说明见 [VS Code 官方调试文档](https://code.visualstudio.com/docs/python/debugging#_purpose)。

cli 返回的源帖子必须保留名称、顺序为 `date/id` 的原双层 MultiIndex，id 为唯一字符串，普通列具有字符串 `text`、`user_name`，无需提供 id 普通列；不接受单层 date 索引加 id 列。接入层直接传递原源对象，保留日期、顺序和值；直接调用核心 `initialize_tasks` 时也使用 `user_name`，调用方提供的回调自行管理目标准备与完整写入。缺少源 `user_name`（即使存在旧 `username`）时由核心报“任务缺少必要列”，重复源列名或非字符串用户名同样在写入前失败。源具有额外 `username` 时只读取 `user_name`，不保存额外列。任务表仍恰好包含约定的九列，用户名列为 `user_name`；旧 `username` 任务表、外部核心调用及后续新任务创建流程需同步字段，本工具不自动迁移业务表。初始化不使用 `start_time/end_time` 过滤源帖子，不加载 AI 配置或调用 AI；这些日期范围只用于后续 worker 处理。注册和一次完整 `cli.write` 均正常返回才报告成功，不分批追加或自动重试。注册与写入不是已验证的同一事务，异常或中断可能留下已注册资源及部分数据；须等待旧操作结束并核查目标状态，使用尚未注册的新 `task_path`，或按真实客户端规则处理原目标的注册状态与内容后完整重跑。不能仅清空数据就假设重复注册安全；本工具不自动删除或注销目标。正常退出码为 0，普通异常为 1，中断为 130。

初始化成功后填写 AI 配置和认证，再执行 `python -X utf8 main.py`。以后新增任务由现有流程负责创建。

worker 按自然月完成读取、遗留 9/-1 恢复、认领、并发请求和最终保存，再进入下月。AI 消息只使用提示词和 text，date/id 留在程序内定位任务，user_name 仅为表内记录和保存核对。改提示词或 payload 后使用新任务表；只调整重试、并发或 URL 不改变配置摘要。

网络传输故障和全部 5xx 共用每帖最多三次尝试，包含首次。其他非 2xx 立即停止整个 worker；最后仍为 5xx 也停止，供人工排查。2xx 中的内容或结构失败仅终结该帖。停止整个程序时尽力保存已有最终结果，没有最终响应的已认领任务保留 9，下次读取到时恢复；不提供远端取消。

异常收尾结束只记录“worker 收尾结束”，不统计或分类任务。该句只说明收尾流程结束，不能据此认定全部任务处理完毕或所有结果已保存；失败原因查看前面的标准异常日志。原有停止请求、保存已完成结果和状态 9 恢复行为不变，未确认保存的内存结果仍会随进程退出丢失。

cli 始终在调用线程串行运行。它须按原 date/id 更新、保留未提交列，并支持异常后的同范围稳定读回；单行多列的一致写入须由真实 cli 验证。存储失败时不会重新调用 AI；`BatchPersistenceError.pending_patch` 保留尚未确认的更新，异常的 `unsaved_results` 和 `completed_results` 保留程序持有的未保存响应，均仅存在于内存。

只支持一个 worker 写同一任务表。请求已执行但响应丢失或未保存时，重试或重启可能重复调用。真实公司接口、cli 及服务端容量须在接入时验证。

复制到其他机器时，携带 `main.py`、完整的 `ai_batch` 和 `integration`、`job_config.yaml`、`ai_config.yaml`、`requirements.txt`，以及按需提供的 `.env`。新机器安装 Python 3.8、公共依赖和已有环境的 xdata 包。`__pycache__`、`.pyc`、`.python38`、`tests` 和 `docs` 不属于运行必需文件；当前目录只有 `.env.example`，需要使用 `.env` 时自行创建并填写。
