# 参数网格与小时权重表

该表从第二、三轮共用的事件去重分析表生成。网格业务入口为 `postprocessing.weight_grid.run_weight_grid`，单组合入口为 `postprocessing.weight_data.write_weight_data`，命令入口为 `entrypoints.write_weight_data`。每个参数组合对应一张独立表，同一组合在后续日期运行时继续更新同一张表。

## 评分参数与表路径

固定评分公式为：

```text
data = direction映射值
     × (B × materiality映射值)
     × (C × execution_stage映射值)
     × (D × event_relation映射值)
```

没有 A 系数，任意乘数为零时事件得分为零。B/C/D 和每个枚举各自的候选数组独立做笛卡尔积。例如在 `configs/event_processing_config.yaml` 中配置：

```yaml
weight_grid:
  coefficients:
    B: [1, 2]
  mappings:
    materiality:
      高: [3, 5]
  table_name_template: "{weight_path}__B{B}_C{C}_D{D}__{param_id}"
  max_combinations: 1000
```

此配置产生 4 张表，未列出的系数和枚举沿用默认值。若再将 `materiality.中` 配为 `[2, 4]`，则产生 8 张表；即使本批没有“中”，也展开这一参数。即使两组参数的 `B×C×D` 相同或本批结果相同，也分别输出。persistence 候选配置保留在清单中，暂不参与评分、组合展开和参数标识。

显式数组必须非空，元素必须为有限数值；布尔值、字符串、NaN、无穷大均不接受。候选按数值去重，例如 `[1, 1.0, 2]` 等同 `[1, 2]`。允许零和负数。事件评分和小时求和均检查有限性，溢出时停止，避免写入 `"inf"` 等文本。max_combinations 必须为正整数，默认上限 1000，超限在连接数据库前停止。

weight_path 是基础路径，实际目标由模板生成。模板支持 `{weight_path}`、`{B}`、`{C}`、`{D}`、`{param_id}` 简单占位符，必须包含 `{param_id}`。参数标识是评分版本、B/C/D 及四个评分字段完整有效标量映射的规范化 JSON 的 SHA-256 前 16 位。日期、候选数组顺序、字典顺序和 persistence 都不影响标识。改变任何有效映射会得到新标识，即使该枚举未在本批出现。改变模板或基础路径会改变实际目标路径。

连接数据库前校验所有目标，遇到重复、与源表相同、与无参数基础表相同的路径会停止。旧的无参数后缀表不会自动改写。

当前版本为 `direction_product_v2`，写库索引在小时计算完成后整体加 8 小时；参数标识随版本更新，避免与旧版未平移时间的参数表混写。旧参数表保留，不自动迁移。

## 数据口径

`build_basic_df(results, parameters=WeightParameters(...))` 仅为第二、三轮状态均为字符串 `"1"` 的源记录生成一行，恰好包含 `date`、`asset`、`data` 三列。省略参数时使用默认标量参数。源表的 `(date, record_id)` 索引须唯一。任一轮状态不为 `"1"` 的记录直接跳过，不解析回答，也不参与评分或本批资产列生成；日志记录参与和跳过的数量。读取数据库时只选择 asset、event_relation、round3_ai_response、round2_status、round3_status 五列，不再读取 event_id，不读取向量、原文或用量等其他列。整个网格只读取一次源数据、解析一次成功记录的回答，保留原始枚举特征供所有组合复用，逐组合生成、保存并释放 DataFrame。

- `date`：保留源记录完整发布时间，例如 `2017-08-17 15:20:10`，不在 basic_df 中截断时间。若原索引带时区，仅去掉时区信息，不转换年月日时分秒。
- `asset`：使用源表原值，不按事件组去重，不合并币种别名。
- `data`：先解析 `round3_ai_response`，再解析其中的 `ai_response_content`，取 direction、materiality、execution_stage，结合本行 event_relation 和组合参数套用上面的乘法公式。

| 字段 | 默认映射（在配置中均写为数组） |
| --- | --- |
| direction | 利好 1；利空 -1；中性、不确定、其他 0；未知字符串使用“其他”映射 |
| materiality | 高 3；中 2；低 1；不确定 0 |
| execution_stage | 未开始 1；执行中 2；已完成 3；不确定 0 |
| event_relation | new 2；update 1.5；repeat 0.5 |
| persistence | 短期 1；中期 100；长期 10000；不确定 0；暂不参与评分 |

默认 B/C/D 均为 1。利好、高、已完成、new 得分为 `1 × 3 × 3 × 2 = 18`，利空对应 `-18`。

repeat 的响应已由第三轮填充保存。权重阶段对 new/update/repeat 一视同仁，只解析本行已保存的响应，再使用本行 event_relation 的映射值。这里不建立历史、不查找同事件组、不复制或继承其他行的响应，basic_df 保留源表行顺序。

两轮状态均成功的记录（包括 repeat）没有回答、必要分析字段缺失、评分字段没有配置映射或 JSON 解析失败时，计算直接失败，不静默排除或填零。可在相应评分字段下增加枚举映射。如果范围内没有两轮均成功的记录，停止计算，不创建权重表。

`build_weight_data(basic_df, start_time=..., end_time=...)` 先将原始 date 向下取整到小时再加 1 小时，然后按这个目标小时和 asset 求和，再生成宽表。例如 `[15:00:00, 16:00:00)` 的事件统一计入 `16:00:00` 行。`15:20:10` 计入 `16:00:00`，整点 `15:00:00` 也计入 `16:00:00`。

上述内存构建仍使用源时间轴。在评分、聚合、补零和文本转换完成后，写库入口最后执行 `weight_data.index = weight_data.index + pd.Timedelta(hours=8)`，再按平移后的索引与数据库对齐。所有值和列名不变，源表读取日期不变，也不进行时区本地化。例如 `2017-12-31 15:20 → 16:00 → 实际写入 2018-01-01 00:00`。`write_weight_data` 返回值及网格清单中的输出范围均使用平移后的索引。

- 单层索引名为 `date`，类型为 `DatetimeIndex`，dtype 为 `datetime64[ns]`，每条时间均为整点，排序且唯一。时间本身为 datetime，不转换成日期字符串写库。
- 内存时间轴从 `start_time 00:00:00` 连续到 end_time 次日 `00:00:00`，写库后对应从 `start_time 08:00:00` 连续到 end_time 次日 `08:00:00`。无事件小时补零。最后一天 `23:59:59` 的事件最终写入次日 `08:00:00`。单独在内存调用时可省略 end_time，此时结束在最后披露所归属的下一小时整点。范围内没有事件时停止，不创建无资产列的表。
- 列名为读取范围内去重资产加 `.USDT.UFUTURE.BINANCE`，按资产名称排序。入库续写时再与已有表资产列取并集，保留此前批次的列。
- 先以 float64 计算分数、聚合和补零，保留 0.5、1.5 等小数，再把所有资产列转为字符串，例如 `"4.5"`、`"-2.5"` 和 `"0.0"`。普通列 dtype 为 object，每个单元格为 Python str；date 索引保留 datetime 类型。无事件的小时和资产组合补零，不向前延续权重，不累计前面小时。

例如默认参数下，BTC 在 `00:20` 有利好、高、已完成、new，贡献 18；`00:40` 有利空、中、执行中、update，贡献 -6；`00:50` 的 repeat 本行已保存利空、中、执行中，贡献 `-1 × 2 × 2 × 0.5 = -2`。这三条事件在内存中聚合到 `01:00:00`，最终写入 `09:00:00` 行，该行 BTC 权重为文本 `"10.0"`。

## 运行与写入

先预览组合数量、完整参数和表名，不连接数据库、不创建本地运行清单：

```powershell
python -X utf8 -m entrypoints.write_weight_data --dry-run
```

实际运行需在已安装 xdata 的环境中执行：

```powershell
python -X utf8 -m entrypoints.write_weight_data
```

统一读取 `configs/event_processing_config.yaml` 中的 result_path、weight_path、start_time、end_time 和 weight_grid。weight_path 留空供运行前填写，作为参数表的基础路径。实际运行的 start_time/end_time 必须同时配置为 `YYYY-MM-DD`，包含起止日全天，不允许留空后读取全表。预览模式不要求填写日期。输出时间轴也使用本次读取范围，不再有固定历史起点。可用 `--config` 指定另一份同结构的事件配置。

实际运行在 `reports/weight_grid/<北京时间>/manifest.json` 保存清单，也可用 `--report-dir "一个尚不存在的新目录"` 指定位置。清单包含公式、评分版本、完整候选配置、每个组合的标量参数和完整哈希、目标路径、处理状态、完成数量及输出范围。状态为 pending、running、completed、failed 或 interrupted。某张表失败时停止后续组合，记录错误，已成功表保留。重跑会生成新清单，按相同参数定位原表并重新对齐更新；没有变化的已完成行不再提交。

这个目录保存本地运行记录，不保存权重 DataFrame，权重数据仍在数据库中。它用于核对参数和表名、查看失败位置；重跑根据数据库实际索引和数据更新，不依赖旧清单。删除旧清单不影响数据库表。清单的 output_time_offset_hours 为 8，逐表输出范围记录平移后的时间。

调用 `cli.read(result_path, start_time=settings.start_time, end_time=settings.end_time, columns=SOURCE_COLUMNS)`。仅所读范围的事件参与资产列去重和权重计算；若客户端返回范围外数据，计算会在注册前停止。最终索引加 8 小时不改变这次源表读取范围。

本批数据计算和校验完成后，读取已有权重表的索引、列及数据。只将已确认的 `DataNotFound` 识别为缺表，其他读取错误直接传播：

- 首次缺表时注册 timeseries，再用 cli.write 初始化；已注册但尚无行列的空目标只初始化，不重复注册。
- 已有表以 date 整点 DatetimeIndex 对齐；重叠小时由 update_by_column 替换为本批重新计算值，缺少的小时通过 update_by_index 追加，追加批次只包含新索引。
- 本批起点（平移后为起始日 08:00）若已经存在，保留其已有值。由于本次没有读取前一天最后一小时，该边界值属于上一批的顺延结果，不能用起始补零覆盖。
- 资产列取本批与已有表的并集。本批没有事件的旧资产在新小时补文本零；新资产先用 update_by_column 将已有历史行补为文本零，再保存当前计算值。已有列在本批范围外的值保持原样。
- 同范围重复运行以计算结果替换旧值，不相加；无变化行不提交。各阶段沿用存储重试，最多三次，同一阶段重试同一批，不重新计算或注册。

例如先读取 12 月 31 日再读取 1 月 1 日，上一批 `12-31 23:20` 产生的权重最终位于 `01-01 08:00`，后一批会保留这个值，再追加 `01-01 09:00` 之后的小时。重复运行不会再次平移已有表索引，只对本次新计算的 DataFrame 加 8 小时。返回的本批 df 包含保留的起点值，以及合并后的资产列，不返回整张历史表。

保存前从源表两轮状态均成功记录的 asset 原值去重生成本批预期列集合，与本批生成 DataFrame 的列名比较。列名重复、多出或缺少时停止并列出差异；核对通过后日志打印本次实际使用的 result_path、weight_path 和去重 asset 原值。列重命名按资产标签映射，不使用 record_id、event_id 或配置中的账号币种名单。核对后才合并已有表的历史资产列。

保存失败会立即停止后续阶段；此前已成功的更新保留。再次运行时重新读取目标实际索引和数据，重算并对齐后续写，支持注册后初始化失败、更新成功但追加失败等情况。各阶段不是共同事务。

已有表的列名来源核对命令为：

```powershell
python -X utf8 -m entrypoints.audit_weight_columns --param-id <预览中的参数标识>
```

配置只有一个组合时可省略 --param-id。此命令使用同一个 event_processing_config.yaml，仅读取配置范围内源表的 asset 和所选参数权重表，不注册、不写入、不重新请求 AI。输出实际表路径、日期范围、source_assets（源表去重值）、expected_columns（本批应有列）、actual_columns（已有列）、unexpected_columns（本批来源中没有的列）和 missing_columns（缺少的列）。续写表保留历史批次资产，因此 unexpected_columns 也可能是其他批次的合法资产，需结合此前范围核对。

执行整个网格，返回各组合摘要和参数清单路径，不返回所有 DataFrame：

```python
from common.config import load_event_config
from postprocessing.weight_grid import run_weight_grid

settings = load_event_config("configs/event_processing_config.yaml")
summary = run_weight_grid(cli, settings)
print(summary["manifest_path"], summary["tables"])
```

只计算一个组合的内存数据，继续返回单个 DataFrame；显式参数对象采用标量：

```python
from postprocessing.weight_data import SOURCE_COLUMNS, build_basic_df, build_weight_data
from common.config import load_event_config
from postprocessing.weight_parameters import WeightParameters

settings = load_event_config("configs/event_processing_config.yaml")
results = cli.read(settings.result_path, start_time=settings.start_time,
                   end_time=settings.end_time, columns=SOURCE_COLUMNS)
parameters = WeightParameters(B=2, mappings={"materiality": {"高": 5}})
basic_df = build_basic_df(results, parameters=parameters)
weight_data = build_weight_data(basic_df, start_time=settings.start_time, end_time=settings.end_time)
```

若已用 `parse_weight_features(results)` 解析特征，可重复调用 `score_weight_features(features, parameters)` 生成不同组合的 basic_df。单组合写库接口为 `write_weight_data(..., parameters=parameters)`，返回本批实际保存的一个 DataFrame，目标路径由调用方明确提供；可用 `prepare_weight_grid(settings.weight_path, settings.weight_grid).variants` 中的 table_path 和 parameters 保持路径与参数一致。
