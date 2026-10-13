"""提供任务初始化和自然月批量处理入口，AI 输入仅为提示词与正文。"""

import logging

from .runner import BatchRunError, RunSummary, run_worker
from .tasks import BatchPersistenceError, InitializationSummary, initialize_tasks

# 库只发布日志；处理器和格式由宿主应用统一配置。
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = ["run_worker", "initialize_tasks", "RunSummary",
           "InitializationSummary", "BatchRunError", "BatchPersistenceError"]
