# 响应解析模块 IMPLEMENTATION_SPEC

状态：用户已确认，已实现并在 Python 3.8.10 完成模拟验证。响应只返回成功与否、必要结果和失败原因，不建立内部错误码或细分类。对应 ai_batch/response.py，项目边界见 [项目总览](../../../project_overview.md)，验证记录见 [实现自查](../../IMPLEMENTATION_REVIEW.md)。

## 1. 目的与范围

解析公司接口响应，提取 AI 正文、推理、token 用量和接口生成时间，判断是否成功；失败时保留接口提供的具体说明或实际解析失败原因。本模块不调用网络或 cli，不执行重试，不修改任务状态，不识别厂商错误码或公共配置错误。

正式接口为 parse_response(http_status, raw_response) 和 request_failure(error_message)，均返回 ParsedResponse。输出只含 success 布尔值、HTTP 状态、必要结果及 error_message；任务模块将 success 映射为最终状态 1/-1。运行模块直接依据 HTTP 状态和实际捕获的异常类型决定重试或停止：网络传输故障与全部 5xx 有限重试，其他非 2xx 和预算耗尽后仍返回 5xx 时停止整个 worker；本模块不执行这些调度决策。

## 2. 输入与前提

输入为 HTTP 状态码和本次完整响应文本；网络或请求构造失败没有 HTTP 响应，调用方提供实际异常的简短原因。

已知成功响应含 choices，所选 message 含 content、role、reasoning_content 和可选 provider_specific_fields.refusal。额外提取顶层 usage 和 created；created 不替换帖子 date，不自行解释时间单位。其他接口元数据不提取，不保存原始响应。

正文与推理不强制再解析业务 JSON，不验证答案业务语义。date/id 任务键由运行模块保管，本模块不生成或替换任务索引。真实接口的成功响应、错误封装及完成标记尚未实测。

## 3. 总体处理顺序

接收 HTTP 状态与文本 → 尝试解析标准 JSON 根对象 → 提取可用 usage/created → 非 2xx 或存在明确 error 时提取失败原因并返回 success=false → 无明确错误的 2xx 校验 choices 和所选 message → 判断完成标记、拒答及正文是否有效 → 校验需要保存的结果可编码 → 返回 success、必要结果和 error_message。

不再先生成错误码或响应分类，再据其生成失败文案。接口错误直接使用错误消息；结构或编码检查失败直接说明失败字段或使用解析异常原因。预期响应错误作为 success=false 返回，未预期程序异常继续传播给运行模块。

完整响应文本只作为当次解析输入，不放入 ParsedResponse、结果缓冲或日志。每次收到 HTTP 响应都按同一流程解析，不因可能重试而隐藏实际 HTTP 状态；5xx 即使错误 body 无法解析，也保留原 http_status 供运行模块判断。是否继续请求由运行模块决定；只有本次成功或最后失败交给任务模块保存。

## 4. HTTP 错误与失败原因

非 2xx 始终返回 success=false，AI 正文和推理为空，不能因为 body 有正常 choices 就判成功。错误 body 能解析为标准 JSON 对象时仍提取可用 usage/created，否则两项为 null。

接口失败原因只取一个文本字段，优先顺序为 error 对象中的非空白字符串 message → error 本身的非空白字符串 → 非 2xx 根对象中的非空白字符串 message。非 2xx 可在该原因前附实际 HTTP 状态；都没有时使用“HTTP N”。不读取或映射厂商 code/type，不序列化整个 error 对象或错误 body 作为原因。

2xx 中 error 缺失、null 或空字符串表示没有明确接口错误；其余值优先视为失败，不再继续将 choices 判成成功。有可用消息则保留消息，没有则写“接口返回错误，但未提供失败说明”。仅有未知顶层 code/message 而没有 error 的 2xx 不自行猜测业务错误，仍按成功响应结构检查。

2xx 使用 json.loads 解析标准 JSON，拒绝 NaN、Infinity、-Infinity；无法解析时返回 success=false，error_message 使用实际解析异常的简短原因。根值非对象时直接说明“响应 JSON 根值不是对象”。

例如 HTTP 400、error.message="模型不存在"，返回“HTTP 400：模型不存在”；HTTP 400、error.message="输入超过长度限制"，返回该具体说明。两者均为 success=false、http_status=400，运行模块据 HTTP 400 保存触发任务的失败并停止整个 worker，本模块不通过消息内容区分它们。HTTP 503 则返回 success=false、http_status=503 和具体原因；运行模块尚有预算时重试，预算耗尽时保存最后失败并停止。HTTP 200 的解析或内容失败仍返回 success=false、http_status=200，不伪造 5xx，不因此自动重试。

## 5. 成功响应检查与结果保留

choices 必须是非空对象列表，按实际整数 index=0 选择唯一 choice；bool 不作为整数，没有或重复 index=0 均失败，并直接说明该结构问题。选择编号只用于解析，不输出或落库。

所选 message 必须是对象且 role=assistant。content 必须是字符串，null 转为空；reasoning_content 缺失或 null 转为空，其他非字符串类型失败。provider_specific_fields 缺失或 null 可接受，否则必须为对象。这些检查直接报告出错字段，不产生内部错误分类。

refusal 缺失、null 或空字符串表示未明确拒答；其他值，或 finish_reason=content_filter，均判失败，优先使用可用的字符串 refusal 作为原因，没有具体说明时写“AI 拒绝生成内容”。finish_reason=length 判失败，写“回答被截断”。只有 finish_reason=stop、无明确拒答且正文包含非空白字符时 success=true；其他完成原因（包括 tool_calls）或空正文直接说明实际原因，不执行工具。

正文与推理保留原文，不 strip；结构检查失败也可保留已经安全提取的部分文本。无法 UTF-8 编码的结果字段置空，并以实际编码失败原因返回 success=false，不能把不可保存值交给任务模块。

usage 保留顶层原 JSON 值，created 原值映射为 response_created；缺失或 null 均为 null，不补 token 用量、不转换时间单位。根对象解析成功后即提取，两项在失败响应、拒答或截断时也可保留。它们只属于最终一次响应，不累计中间重试；无法标准 JSON/UTF-8 编码的项清空，并直接说明编码失败。

request_failure(error_message) 仅将调用方提供的失败原因封装为 success=false、空正文/推理、http_status=null、usage=null、response_created=null，不接收机器代码或异常对象，也不推断失败类型。异常原因由运行模块直接取自实际异常。

所有失败原因只保留一个字符串，须可 UTF-8 保存，最长 512 个字符；超出时取前 511 个字符加省略号。没有具体原因时使用上述简单兜底，不拼接请求头、完整任务、完整异常堆栈或原始响应。成功时 error_message 为空。不维护专门的错误文案表、厂商错误码映射或按消息关键词识别配置错误。

## 6. 输出字段汇总

ParsedResponse 不包含任务四值状态、响应细分类、内部错误码、原文或重试计划。任务模块按 [任务表设计](../../data/TABLE_DESIGN.md) 将五个结果列与 1/-1 在同一次更新保存。

| 内存字段 | 含义 |
| --- | --- |
| success | 布尔值，完整有效成功响应为 true，其余预期失败为 false。 |
| http_status | 实际 HTTP 状态码整数或 null；失败解析仍保留实际状态，运行模块据此区分 5xx 重试与其他 HTTP 失败停止。 |
| ai_response_content | 所选 message.content，或可安全提取的失败正文，缺失为空。 |
| reasoning_content | 最终推理或可安全提取的失败推理，缺失为空。 |
| usage | 顶层 usage 原 JSON 值或 null，任务模块编码为 usage_json。 |
| response_created | 顶层 created 原 JSON 值或 null，任务模块转为字符串。 |
| error_message | 接口的具体错误消息或实际失败原因，成功为空，最多 512 字符。 |

### 中文日志与注释（工程约定）

使用 ai_batch.response 模块记录器，以 DEBUG 记录解析结果、HTTP 状态及解析或编码异常的类名，不输出响应原文、AI 内容、拒答文本、usage/created 值或具体外部错误消息。中文注释解释错误优先级、index=0 选择、最终字段保留和编码检查。日志格式由调用方配置，不增加响应字段或失败分类，不改变解析判断。

## 7. 验证与未验证项

验证要求包括 index=0 选择及重复/缺失索引、错误结构、Unicode 与换行、空正文、拒答、截断、非 JSON/非有限值，只有完整有效响应才返回 success=true。验证非 2xx 和明确 error 不会误成功，400 不按失败消息区分类型、5xx 非 JSON body 仍保留实际 HTTP 状态、200 的解析失败不改写 HTTP 状态，接口错误消息提取优先级及无消息兜底，原因长度限制，request_failure 直接保留调用方原因，输出不存在错误码或细分类；usage/created 在成功失败时按最终一次原值保留，不累计重试。重试预算、全局停止及最终保存的联动由运行模块验证，本模块不引入对应状态。

已使用 Python 3.8.10 标准库 unittest 完成模拟响应验证。真实接口的错误消息位置、多 choices、拒答、finish_reason 及空正文约定尚未验证；真实契约若不同，先更新规格再实现。未调用真实接口。
