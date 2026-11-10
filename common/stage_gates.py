"""独立阶段入口的第一轮完成检查。"""

from datetime import date

from common.time_rules import published_time
from common.validation import validate_post_index


def require_round1_complete(cli, settings):
    """只读当前范围的第一轮状态，禁止部分成功数据经独立入口进入下游。"""
    if not settings.start_time or not settings.end_time:
        raise ValueError("阶段交接必须配置 start_time/end_time")
    begin, end = date.fromisoformat(settings.start_time), date.fromisoformat(settings.end_time)
    if begin > end:
        raise ValueError("start_time 不能晚于 end_time")
    frame = cli.read(settings.first_round_task_path, start_time=settings.start_time,
                     end_time=settings.end_time, columns=["status"])
    validate_post_index(frame)
    if any(not begin <= published_time(key[0]).date() <= end for key in frame.index):
        raise ValueError("第一轮状态读取返回范围外的记录")
    incomplete = int(frame["status"].ne("1").fillna(True).sum())
    if incomplete:
        raise ValueError("第一轮本范围尚有 {} 条未成功，禁止进入向量计算或第二轮".format(incomplete))
