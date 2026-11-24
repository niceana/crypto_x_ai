# 用 cli.read 导出试跑数据

把 export_xdata.py 放到已经配置好的 Linux 试跑目录，与 run_full.py、check_weight.py 同级。它只使用 cli.read / cli.read_object 和本地 to_pickle，不更新 xdata、Redis，不调用 AI。业务 wheel 不需要重新安装。

本次补充包同时包含更新后的 check_weight.py，请一并替换。更新 export_xdata.py 时，保留你已填写的顶部时间、表路径和其他配置。

## 建议选哪一批

先选一个旧程序已完成三轮、向量和旧 weight 的历史日期，再选有帖的某个小时的 xx:47；最好该日期之前已有至少 100 天数据，能覆盖历史归组场景。不要直接用正在处理的最新时段。

以某天 16:47（UTC+8）模拟调用为例：

| 文件 | 精确内容 |
| --- | --- |
| posts.pkl | 原帖发布时间在本地 `[16:30,16:45)`，即 UTC `[08:30,08:45)`；从旧原帖表或旧第一轮任务表读取 text/user_name。 |
| history_before.pkl | 从模拟调用时刻往前 100 天开始，到本批开始（16:30）之前的目标资产事件 B；保留原 14 个基础列、已有的 round2_call_meta/round3_call_meta、向量、状态、响应和 `(date,record_id)` 索引。旧 14 列表仍兼容。 |
| events_completed.pkl | 本地 `[15:45,16:45)` 的旧二三轮事件结果，供纯 weight 测试；与全流程原帖是两个独立输入。 |
| old_weight.pkl | 旧算法写库后的本地 17:00 行；必须是相同聚合方式及评分参数组合的实际宽表。 |
| assets.json | cli.read_object 读取的 InstrumentList 数组。B 和 weight 事件按该名单筛选。 |
| export_manifest.json | 数据路径、模拟时刻、UTC 区间、行数及时间转换说明。 |

所有事件区间都是左闭右开。脚本先按日期调用 cli.read，再在内存中筛选精确分钟，所以不会依赖数据库是否支持分秒参数。

旧原帖表和旧第一轮任务表的 `(date,id)` 中 date 是 UTC 发布时间，已经不含真实爬虫时间。脚本仅用于历史模拟：把该时间保留为 created_at，输出 posts 的单层 date 全部设为模拟抓取刻度 16:45（UTC+8），允许重复；id 转为字符串列。不会把旧的发布时间误称为真实爬虫时间，也不读取旧的第一轮 AI 回答来跳过本次处理。

不要把当前 date 已经表示爬虫时间的新原帖表填入 POSTS_PATH。本脚本针对旧程序的发布时间索引历史表；如果要测真实爬虫输入，应直接保存宿主产生的新帖 DataFrame。

## 执行导出

直接修改 export_xdata.py 顶部的配置区，无需在命令里传参数：

```python
TEST_TIME = "2021-03-20 16:47:00"  # 替换为已完成处理的历史时刻，UTC+8
POSTS_PATH = "你的旧第一轮任务表路径"
EVENTS_PATH = "你的旧第二三轮结果表路径"
OLD_WEIGHT_PATH = "你的旧weight具体表路径"
ASSETS_PATH = "crypto_1m/research_prod/meta/alphas/Cpt380_BA_Daily_Update_All.json"
METHOD = "sum"  # 或 "max_abs"，须与旧表一致
MODE = "all"  # 同时准备全流程与 weight 测试
WEIGHT_PARAM_ID = None  # 多组评分参数时填写对应 param_id
OUTPUT_DIR = ROOT / "data"
```

POSTS_PATH 可以用旧 post_extraction_config.yaml 的 source_path 或 task_path；EVENTS_PATH 用旧 event_processing_config.yaml 的 result_path。OLD_WEIGHT_PATH 要填某一聚合/评分组合最终写入的完整表路径，不能只填网格的基础目录 weight_path；可从旧 weight 网格的 manifest.json / tables 输出查找。

改好后运行：

```bash
python export_xdata.py
```

ASSETS_PATH 默认沿用原配置的资产对象路径，如不一致直接修改顶部配置。仍传完整指定资产名单，不需要为了适应旧表缩减名单。旧 weight 的列是“事件表出现的资产”与“指定资产名单”的交集，因此允许它缺少名单中的资产列。

导出时 old_weight.pkl 保留旧表原有的列，缺列写入 export_manifest.json 的 old_weight_missing_assets。check_weight.py 对完整指定名单计算新权重，再仅将旧表不存在的列按 0 对齐；这些列会在 comparison.csv 中标记 old_column_present=False，并在 summary.json 中列出。如果新结果也是 0，就视为一致；如果新结果非零，仍会报 DIFF，不会跳过。旧表已有列中的 NaN 不会被补成 0。

默认输出到脚本旁的 data/，会覆盖同名导出文件。如需保留多批，把 OUTPUT_DIR 改成另一批数据的绝对路径。成功后脚本会打印对应的完整测试命令。未替换的表路径占位符会在连接 xdata 前报错。

导出与试跑都使用实盘解释器 `/opt/ap/python/anaconda3/bin/python`，确保其中已接入 xdata 并安装业务 wheel。无需把 xdata 加进业务 wheel。日志打印的后续命令会使用当前解释器和调用脚本的绝对路径，便于在仓库和解压包两种目录下运行。

只导出纯 weight 对账数据时，顶部改为下面这一行，再直接运行同一个脚本；此模式不需要填写 POSTS_PATH，不读取原帖或 100 天历史：

```python
MODE = "weight"
```

已有 cli 实例也可在 notebook 里复用顶部配置，不会再次 get_client：

```python
from export_xdata import main

main([], cli=cli)
```

## 接着测试

在配置已完成的试跑目录，用实盘 Python 解释器执行（日期与导出保持一致）：

```bash
python check_weight.py --method sum --old-weight data/old_weight.pkl --test-time '2021-03-20 16:47:00'
python run_full.py --method sum --test-time '2021-03-20 16:47:00'
```

先用旧的 events_completed.pkl 对比旧 weight，验证评分及窗口；再跑完整流程。全流程会重新调用 AI，结果可能与旧回答不同，因此不能只根据最终 weight 不同判断实现有错。

纯 weight 测试可选其他刻度来检查部分窗口；这里要求导出 xx:45~xx:59 是为了直接与旧完整小时表对账。正式函数仍只自动读取系统时间，TEST_TIME 只用于试跑脚本。

如果原帖为空、窗口内无目标事件、目标历史 B 的二三轮未成功或缺失向量、旧 weight 缺少对应时刻行，脚本会报错，不会删掉问题记录来制造一致结果。旧表缺少资产列按上述交集规则对齐，不再当作结构错误。历史向量内容保持原样；模型版本、配置及维度仍由正式入口检查，不能通过重新生成历史向量来绕过错误。
