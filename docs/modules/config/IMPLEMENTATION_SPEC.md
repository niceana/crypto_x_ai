# 配置与请求构造模块 IMPLEMENTATION_SPEC

本次闭区间契约同步（2026-10-13）：用户已确认项目方案及本次相关规格，第 2 节的 worker 日期配置描述同步为按日左闭右闭。本模块源码与 ai_config.yaml 的 SHA-256 均与修改前一致，配置加载、请求构造、摘要不新增正式逻辑，未发现规格外逻辑或遗漏；全部既有配置回归随 Python 3.8.10 完整 86 项测试通过。worker 日期范围由运行、任务模块实现，真实 cli 的末日全天和日历时区尚未验证。

本次接口配置修订（2026-10-13）：按用户明确的公司接口契约，请求体消息字段使用 `messages`，认证头使用从环境变量读取密钥的 Bearer 格式。只同步 YAML 配置、使用说明和现有测试中的请求样例；既有配置加载、环境展开、payload 透传、模板渲染和摘要逻辑无需修改，不新增字段重命名或兼容分支。未改变模块职责、依赖关系或总体数据流。

本次文档同步（2026-10-13）：随已确认的项目字段契约，将用户名记录字段描述与代码注释同步为 `user_name`。配置加载、模板白名单、请求构造和摘要算法均沿用已确认规格；不增加用户名模板变量。现有模板校验回归已覆盖 {{task.user_name}} 与旧 {{task.username}} 均被拒绝，全部 70 项模拟测试通过；本模块没有新增正式逻辑，未发现规格外逻辑或遗漏。

状态：用户已确认，已实现并在 Python 3.8.10 完成模拟验证。AI 输入仅含提示词与 text，启动只加载固定配置快照，认领时才计算摘要。对应 ai_batch/config.py，项目边界见 [项目总览](../../../project_overview.md)，验证记录见 [实现自查](../../IMPLEMENTATION_REVIEW.md)。

## 1. 目的和范围

读取 YAML 与可选 .env，验证启动参数，构造单条 HTTP JSON 请求，提供处理配置的稳定摘要。本模块不调用 cli、网络或处理任务状态，不解析模型响应。

## 2. 输入、前提与接口

配置加载输入为 UTF-8 YAML 路径；单帖请求构造输入为固定配置快照和一个 text 字符串。返回独立配置对象与独立 payload，不修改原始 YAML 对象。任务键和 user_name 不传入本模块。

正式接口为 load_config(path)、render_payload(config, text)、config_hash(config)。异常为 ConfigError 或 TaskRenderError，只描述字段和原因，不输出密钥或帖子正文。

YAML 仅包含 prompts、api、run 三个顶层部分。任务路径、数据库、源文件、结果文件、JSONL、resume 开关和业务 CLI 配置不属于本模块；续跑始终依据任务表进行。

worker 总日期范围属于调用方的读数配置，start_time/end_time 与任务路径放调用方配置文件，将两端值显式交给 run_worker；运行模块按自然月生成当前分段，TaskStore 将该分段范围原样传给 cli.read。不新增本模块的月份参数或命令行开关。改变读取日期只改变本次任务范围，不改变 config_hash，不要求新建任务表。日期参数按任务索引中的 date 限定读取，date 与源帖 created_at 一致；总范围与分段均为按日左闭右闭，完整月份截止到月末，起止同日有效。日期分段和末日全天核对分别由运行、任务模块负责，本模块不解析 worker 日期；真实日历时区与末日全天覆盖须接入验证。

## 3. 总体流程

worker 启动顺序：安全解析 YAML → 加载同目录可选 .env → 展开 URL 与请求头环境变量 → 完整校验并填充默认参数 → 校验模板 → 返回本次运行固定的配置快照。此阶段不调用 config_hash，不提前计算或保存摘要；分月处理期间不重新加载 YAML。

认领批次：任务模块准备认领时调用 config_hash(config) → 用固定快照计算提示词模板和 api.payload 的摘要 → 与本批 status=9 同一次更新确认 → 运行模块使用同一快照构造与发送请求。该纯函数不做 cli 更新。

单任务请求构造顺序：验证 text 为字符串 → 使用 text 渲染 system/user prompt → 递归渲染 payload 值 → 检查标准 JSON 与 UTF-8 可编码性 → 返回独立请求 payload。运行模块在状态 9 和摘要保存确认后执行本流程，单条构造失败回填最终失败，不发 HTTP。初始化和现有数据流程创建新任务均不加载本模块配置、不计算摘要，config_hash 初始为空。

## 4. 启动配置

使用 PyYAML 安全加载，拒绝重复键、未知顶层及固定配置键；api.payload 的厂商键不做白名单限制。不执行 YAML 对象标签或自定义代码，不接受日期、集合、非字符串字典键和非有限浮点数作为请求内容。

可选 .env 在 YAML 目录，缺失不报错，进程环境变量优先。仅 api.url 和 api.headers 值支持 ${ENV_NAME}；变量不存在或为空时报字段和变量名。prompt 与任务文本不展开环境变量。

URL 必须为非空绝对 HTTP/HTTPS 地址，不补路由；拒绝 URL 中的用户名密码。敏感认证通过 headers 配置。headers 是字符串字典，允许空字典。初版假设 headers 用于认证和传输，不以它们决定业务处理版本；需要业务路由或模型版本请求头时须另行确认版本规则。

prompts.system/user 必须为字符串，允许空字符串。payload 是标准 JSON 对象，若有顶层 stream，只允许布尔 false。公司接口请求体消息字段使用 messages；配置模块沿用 payload 的原字段名，不自动将 message 重命名为 messages。响应中的 choices[].message 由响应模块处理，不属于此次请求配置修订。

整数参数拒绝 bool，计数和容量必须大于 0。时间参数须为有限正数；requests_per_second 可为 0。退避上限不小于基数，result_buffer_size 不小于 batch_size。api.trust_env 是布尔值，默认 false。

worker 配置错误在状态更新和 HTTP 发送前终止；初始化不读取 AI 配置，不因 URL、认证或模型配置尚未准备而拒绝创建任务。

## 5. 模板与请求构造

唯一的任务模板变量为 {{task.text}}，直接取本帖 text 字符串，不生成整帖 JSON，也不提供 date、id 或 user_name 变量。{{task.user_name}} 和旧 {{task.username}} 均沿用既有未知变量校验而被拒绝。date/id 复合索引由任务与运行模块管理；user_name 只保留在任务表，不参与请求构造。payload 另支持 {{prompts.system}}、{{prompts.user}} 引用已渲染 prompt；prompt 本身不引用 prompts。AI 消息只由提示词和 text 组成，model、输出长度等接口参数仍由 api.payload 提供。

只处理配置中的字符串值，不渲染字典键；不支持表达式、循环、过滤器、属性访问或列表下标，不使用 eval。模板双大括号片段必须匹配上述变量，首尾空格允许。普通 JSON 单大括号不受影响。

插入 text 或已渲染 prompt 后不再次解析它们；正文中的 {{...}} 或 ${...} 只是文本。字符串恰好是一个变量时保留变量类型；text 和已渲染 prompt 均是字符串。嵌入字符串时按文本插入。

text 必须为字符串，允许空字符串；其他类型抛 TaskRenderError，不自动转为字符串或补默认空值。system 和 user 分别使用本帖 text 渲染，之后递归生成独立 payload。用 allow_nan=false 的标准 JSON 序列化验证，再验证 UTF-8 编码。不能构造合法请求时抛 TaskRenderError。

例如 text="@arc 👀"，用户 prompt 为“请概括以下帖子：{{task.text}}”，得到“请概括以下帖子：@arc 👀”。正文若含 {{task.text}} 字样，该字样作为原帖内容保留，不进行第二轮替换。配置中出现任何其他任务变量则在启动模板校验时报 ConfigError，不能默默替换为空。

## 6. 身份与版本

任务模块仅在准备认领 0 任务时调用 config_hash。统一规范化 JSON：sort_keys=true、紧凑逗号/冒号分隔、ensure_ascii=false、allow_nan=false，UTF-8 编码后计算 SHA-256 十六进制摘要；不在初始化、worker 配置加载、任务读取或启动恢复时计算。

config_hash 只对原始 prompts（system/user 提示词内容）和 api.payload（AI 调用参数及消息模板）进行上述规范化与 SHA-256 计算。包含模型、消息模板、输出长度等实际 payload 参数；不包含 URL、headers、.env、帖子数据、日期索引、响应解析版本、并发、速率、重试、批次或超时。输入使用渲染前配置，不把每帖正文算成不同配置。

摘要是 64 位小写十六进制字符串，实际提示词和调用参数仍保存在 YAML，不逐帖重复保存全文。它表示本轮认领准备使用的处理配置，不证明请求已经发送或成功；同一配置处理不同帖子时摘要相同。它仅检测提示词及 AI 参数变更，不为接口实现或响应解析逻辑提供版本保证。例如只改重试次数或 URL，摘要不变；改提示词、模型或 max_completion_tokens，摘要改变。

提示词模板或 api.payload 改变时仍不能复用同一任务表，由调用方选择新任务表并重新初始化；不能借空摘要绕过配置版本规则，状态 1 不自动重跑。任务模块在实际认领时才核对当前分段已有非空摘要，不在无 0 的分段计算摘要，也不先读取全年或范围外任务。故工具不承诺仅凭启动就检测全表配置错误；固定配置表的使用由调用方保证。本模块不自动创建新路径，不提供 input_hash。任务模块校验初始化输入及当前分段数据的唯一性，后续新任务的输入快照与全表唯一性由现有数据新增流程负责。

run.max_attempts 和退避参数仅控制一个帖子在本次 worker 中因网络传输故障或 5xx 进行的内存重试，不生成数据库重试字段。两种错误共用一个预算，max_attempts 默认 3，包含首次请求；进程重启从头计数。其他非 2xx 响应立即停止 worker，不因尚有预算而继续请求；具体范围和停止条件见运行规格。没有任务状态恢复时间参数。

## 7. YAML 示例与参数汇总

以下示例已提供为 ai_config.yaml，prompt 只用于演示，最终由用户修改。

~~~yaml
prompts:
  system: |
    你是文本分析助手。根据输入回答，信息不足时明确说明。
  user: |
    请用中文简要概括以下帖子：
    {{task.text}}

api:
  url: ""  # 填公司接口完整 URL；空值启动报错。
  headers:
    Authorization: "Bearer ${AI_API_KEY}"
  trust_env: false
  payload:
    model: glm-5.3-flash
    messages:
      - role: system
        content: "{{prompts.system}}"
      - role: user
        content: "{{prompts.user}}"
    max_completion_tokens: 512
    stream: false

run:
  concurrency: 100
  batch_size: 200
  result_buffer_size: 200
  flush_interval_seconds: 1
  timeout_seconds: 120
  requests_per_second: 0
  max_attempts: 3
  backoff_base_seconds: 1
  backoff_max_seconds: 30
  storage_max_attempts: 3
  storage_retry_seconds: 1
  progress_every: 100
~~~

| 参数 | 默认与口径 |
| --- | --- |
| concurrency | 100；已提交但未收取完成消息的网络作业上限，实际 HTTP 在途不超过此值。 |
| batch_size | 200；结果保存阈值，与发请求批次独立。 |
| result_buffer_size | 200；主线程未确认结果快照容量，不小于 batch_size。 |
| flush_interval_seconds | 1；缓冲首条结果进入后达到该时长即提交，不是单条响应完成立即写入。 |
| timeout_seconds | 120；HTTPX 连接、读取、发送、连接池等待的基础阶段超时，与数据库状态计时无关；公司接口的失败反馈由响应模块解释。 |
| requests_per_second | 0 不额外限速，大于 0 限制全局实际 HTTP 起发速率，重试亦计入。 |
| max_attempts | 3；网络传输故障与全部 5xx 共用的单帖请求尝试上限，包含首次，仅在内存计数，重启从头开始。 |
| backoff_base_seconds / backoff_max_seconds | 1 / 30；网络传输故障或 5xx 后的指数随机等待，不截短 5xx 的有效 Retry-After，不写数据库。 |
| storage_max_attempts | 3；仅限 worker 同一索引上同一组列更新的回调尝试上限，不增加 HTTP 次数；初始化写入不自动重试。 |
| storage_retry_seconds | 1；核对后重新更新同一快照的等待。 |
| progress_every | 100；每增加这些已确认终态任务记录一次统计，结束时再输出。 |

鉴权使用 Authorization: "Bearer ${AI_API_KEY}"，密钥在环境或 .env。加载配置时先加载同目录 .env，再展开 headers；进程环境中的同名值优先。此 YAML 设置等价于 Python 中 {"Authorization": f"Bearer {os.environ['AI_API_KEY']}"}，YAML 不执行 Python 表达式。环境变量名必须非空，示例统一使用已有的 AI_API_KEY；引用变量缺失或为空时在请求前报配置错误。空 headers 仍由原有配置契约允许，不要求密钥。配置文件不包含任务目标路径或 CLI 初始化。

### 中文日志与注释（工程约定）

使用 ai_batch.config 模块记录器，以 DEBUG 记录配置加载和校验完成，不输出配置内容、URL、认证头、提示词或环境变量值。中文文档字符串及注释解释安全加载、环境优先级、一次模板替换和摘要口径。日志格式由调用方配置，本模块不修改根记录器、配置参数或接口。

## 8. 验证与 Python 3.8

需要验证安全 YAML、重复与未知键、数值/布尔边界、环境优先级、模板一次替换、原对象不变、实际请求中的 messages 字段，唯一任务变量为 task.text、正文以外的任务变量在启动时拒绝、正文中的模板片段不二次展开、请求构造只接收 text 且不含用户名或任务键，load_config 不调用摘要函数，认领才计算且计算与请求使用同一固定快照，分月处理不热加载配置，摘要口径稳定：URL、密钥、日期范围、并发或重试参数变化不改摘要，prompt、模型或输出长度参数变化改变摘要，text 变化不会产生另一配置摘要。max_attempts 默认 3 且拒绝布尔、零和负数；旧任务恢复时间参数作为未知键拒绝。

直接依赖 PyYAML==6.0.2、python-dotenv==1.0.1，其发布元数据支持 Python 3.8：[PyYAML](https://pypi.org/project/PyYAML/6.0.2/)、[python-dotenv](https://pypi.org/project/python-dotenv/1.0.1/)。类型注解使用 typing，不用新版 union、容器泛型或新版 dataclass 参数。

Python 3.8.10 已安装上述锁定依赖并通过配置与请求构造测试。公司 URL 和认证方式尚未验证；假设鉴权头不改变业务版本须在真实接入确认。

### 本次接口配置自查（2026-10-13）

ai_config.yaml 已按第 4、7 节使用 messages 与 Authorization: "Bearer ${AI_API_KEY}"；README、测试配置、请求解析辅助函数及现有请求断言同步。ai_batch/config.py 及其余模块代码无需修改，已有 payload 原字段透传与环境展开即可实现用户给定的接口配置。未新增超出规格的正式逻辑，未发现必要逻辑遗漏；模块职责和项目级数据流未变，未更新项目总览。

Python 3.8.10 执行 `.python38/python.exe -X utf8 -B -m unittest discover -s tests -v`，全部 71 项测试通过，耗时 15.794 秒。现有验证覆盖 messages 请求构造、正文一次渲染、进程环境优先于 .env、空认证变量拒绝，以及 worker 月间顺序、状态保存、重试、停止和资源收尾。响应样例继续使用 choices[].message。

未调用真实公司接口，未读取或写入业务任务表。实际 URL、密钥、模型及服务端响应契约仍待试跑验证。message 改为 messages 会改变 payload 摘要；仅已初始化且从未被 worker 认领的任务表可以直接使用修正后的配置，已写入旧配置摘要的表遵守第 6 节的新任务表规则。
