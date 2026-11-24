# 按原帖统计三轮结果变化，不比较解释

compare_post_results.py 基于已经成功保存的回放输出与旧基准按原帖 ID 比较，不调用 AI、向量服务或重跑任何批次。只统计结果：

| 环节 | 比较结果 | 忽略内容 |
| --- | --- | --- |
| 第一轮 | 事件正文及资产集合 | JSON 排版、事件顺序、ticker 顺序/大小写、推理与请求元数据 |
| 第二轮 | new/update/repeat 和匹配组 | new_information、候选短编号、推理与请求元数据 |
| 第三轮 | direction、materiality、execution_stage、persistence | materiality_reason、推理与请求元数据 |

这统计的是各轮保存后的业务结果，包括自动判 new 和 repeat 继承的结果，不等同于每条记录都实际请求过该轮 AI。第一轮事件文字采用规范化后的文本比较，不额外请求 AI 判断不同表述是否语义相同。

## 配置与执行

把脚本放到现有试跑目录，顶部填写：

```python
RUN_DIR = "/你的试跑目录/outputs/replay_full_本次启动时间"
DATA_DIR = ROOT / "data_240h"  # 当时导出的那份数据，必要时填写绝对路径
COMPARE_ROUND1 = True
OLD_ROUND1_PKL = None
OLD_ROUND1_PATH = None
OUTPUT_DIR = None
```

第一轮旧回答未保存在原 data_240h 中。默认从当时 manifest.json 的 POSTS_PATH 只读一次 status/text/user_name/ai_response_content，不读取原表全部列。如果之前 POSTS_PATH 填的就是旧第一轮任务表，OLD_ROUND1_PATH 保持 None 即可；如果填的是旧原始帖子表，则 OLD_ROUND1_PATH 必须填实际的旧第一轮任务表路径。

如已有旧第一轮结果 PKL，可填写 OLD_ROUND1_PKL，此时不读 xdata。该文件需含 (date,id) 索引及上述四个字段。旧第一轮基准应与当时旧二三轮结果属于同一版本；表后来重跑过时，应使用当时备份。

执行：

```bash
/opt/ap/python/anaconda3/bin/python compare_post_results.py
```

这条命令使用实盘 Python 环境，读取新输出和旧基准、解析结果 JSON 并统计。不重新导出回放数据，不更新数据库，不重跑 AI。第一轮回答沿用已安装业务包的历史 JSON 解析函数。

如果仅比较已有旧二三轮结果，顶部设置 COMPARE_ROUND1=False 即可。此时完全不读取 xdata，但第一轮变化数不属于本次统计，不能解读为第一轮都相同。

## 输出

默认写入本次回放目录下的 ai_result_diff/，不改原文件：

- summary.json：可比帖子数、结果变化的去重帖子数/比例，各轮变化帖子数、未完整比较数。
- post_comparison.csv：每条已处理原帖的逐轮变化标记、可比标记及新旧事件行数。
- changed_posts.csv：有结果变化的原帖，附原文和新旧结构化结果；不包含 AI 的解释或推理。
- comparison_issues.csv：旧结果缺失、输入不同、解析失败等，不会把这些问题直接算成“结果相同”。
- old_round1_snapshot.pkl：本次使用的旧第一轮快照。后续可把 OLD_ROUND1_PKL 指向这个文件，避免再次读库。

changed_posts 是按原帖 ID 去重的数量。各轮变化数有重叠，不能相加。例如同一帖子第一、三轮都变，最终只计 1 条变化帖子。

第二、三轮仅比较这次指定资产的结果。若第一轮事件集合变化，脚本会单列 event_set_changed_posts，不强行把不同事件的后续记录逐条对齐来计入第二、三轮。第一轮本身仍比较完整提取结果，包括指定名单之外的资产。

匹配组用组起点的原帖/资产/事件锚点对齐，避免仅因事件序号变化误报；组内容和历史轨迹变化仍可能传播到后续结果。因此本统计描述本次连续回放与旧基准的结果变化，不能据此单独归因于某轮 AI 的随机性。

缺失基准、失败状态或输入原文变化会标记为未能完整比较，终端会提示。若回放只完成部分批次，也只统计已完成的部分，不把未处理帖子当作相同。

只关心同一帖子＋资产最后聚合成的权重是否变化时，请用 compare_post_weights.py；它不要求事件正文相同，见 [POST_WEIGHT_COMPARISON.md](POST_WEIGHT_COMPARISON.md)。
