"""从根目录启动round1事件提取；保留 main 调用入口，共用原有退出处理。"""

from entrypoints.run_round1 import execute_round1, main as main


if __name__ == "__main__":
    execute_round1(__name__)
