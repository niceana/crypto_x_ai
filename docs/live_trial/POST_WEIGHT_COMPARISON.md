# 按同一帖子＋资产比较权重

compare_post_weights.py 读取本次回放保存的事件结果和 data_240h/reference_events.pkl，复用正式评分函数，比较同一 source_post_id＋asset 的聚合数值。不要求事件正文、事件条数或事件编号相同，不读旧第一轮表，不请求 AI 或数据库，不重跑回放。

## 比较的数值

对每个帖子＋资产，先按原规则计算本帖每条事件的分数，再使用本次回放 manifest 中的聚合方式：

- sum：该帖子该资产所有事件分数求和。
- max_abs：取绝对值最大的分数，保留正负号；并列时沿用按发布时间及原记录顺序的稳定规则。

这可以回答“同一帖子对同一资产生成的最终信号值有没有变”。事件文本改写或解释文字变化本身不影响比较；即使三轮标签有变化，只要最终聚合数值相同，仍判为相同。

它是按帖计算的权重值，不是交易时点中多个帖子按时间窗口聚合后的最终持仓，也不是 max_abs 下每帖对持仓的独立贡献。后者继续看原来的 comparison_full.csv / summarize_diffs.py。

## 使用

把脚本放到原试跑目录，与 _trial.py 同级，修改顶部：

```python
RUN_DIR = "/你的试跑目录/outputs/replay_full_本次启动时间"
DATA_DIR = ROOT / "data_240h"  # 当时导出的数据，必要时填绝对路径
CONFIG_DIR = ...              # 保留默认，或指向当次回放使用的配置目录
OUTPUT_DIR = None
```

CONFIG_DIR 内评分参数必须与当次回放一致；sum/max_abs 和 param_id 自动从 data_240h/manifest.json 读取。本次实际使用的完整评分参数会保存到新的 summary.json。

执行：

```bash
/opt/ap/python/anaconda3/bin/python compare_post_weights.py
```

这条命令只读取结果文件、重新执行数学评分和汇总，忽略事件正文及 AI 解释，不重跑任何模型。仍使用原来的数值容差 rtol=1e-9、atol=1e-12。

## 范围与数量

仅比较本次已经成功处理的原帖和指定资产。新旧两边都存在的帖子＋资产组合才进入共同集合；有失败或无效响应的组合另列问题，不当作零信号。

新有旧无、旧有新无的组合单独列入 unmatched_pairs.csv，不混入共同组合的差异率。若要评估完整信号变化，应同时查看这些新增/消失组合及原按时点的最终持仓对账。

同一帖子涉及多个资产时，分别算多个组合；changed_posts 再按原帖去重。帖子没产生任何指定资产事件时，不会凭空生成一行零权重参与比较。

## 输出

默认保存到 RUN_DIR/post_weight_diff/：

| 文件 | 内容 |
| --- | --- |
| summary.json | 共同/可比组合数、相同/不同数量、差异率、涉及的帖子数/资产数、新增/消失组合数。 |
| post_asset_weights.csv | 所有组合的新旧 weight、差值、新旧事件数、是否共同及可比。 |
| diff_pairs.csv | 共同组合中权重不同的逐条记录。 |
| unmatched_pairs.csv | 仅一边存在的帖子＋资产组合，保留该边的实际权重。 |
| asset_summary.csv | 按资产汇总可比组合数量和差异数量。 |
| comparison_issues.csv | 结果失败、JSON 无效等未能参与比较的原因。 |

新旧三轮结果、导出数据、回放进度和原始配置均不被修改。
