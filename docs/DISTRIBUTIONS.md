# 发行文件与归档

`dist/` 是被 Git 忽略的生成目录，当前根目录保留同一最新版本的四种交付物及自动生成的 README.md：

| 文件 | 用途 |
| --- | --- |
| crypto_x_ai_live-0.4.3-py3-none-any.whl | 安装业务包，宿主或试跑脚本通过 import 调用。 |
| crypto_x_ai_live-0.4.3.tar.gz | 当前版本业务源码发行包。 |
| crypto_x_ai_linux_trial_0.4.3.zip | 完整试跑包，包含导出、单批、连续回放、日志、配置模板和 wheel。 |
| crypto_x_ai_live_handoff_0.4.3.zip | 宿主接入交付包，包含调用示例、配置、wheel 和接入说明。 |

各文件用途不同；完整试跑包已包含导出、回放、日志及已有结果的资产差异统计功能，一般不再分发功能补充包。维护源码在 tools/、tests/ 和 docs/，不放进 dist/。

## 构建

已有当前版本 wheel 时，在仓库根目录运行：

```bash
python tools/build_linux_trial_zip.py
```

默认只生成完整试跑 ZIP，并更新 dist/README.md，不重复生成补充包。wheel 和源码发行包须提前构建。宿主交付包单独运行 python tools/build_live_handoff.py；构建器从 docs/、examples/、configs/ 和当前 wheel 取文件，不维护另一份业务源码。

确有只更新少量脚本的需要时，显式运行：

```bash
python tools/build_linux_trial_zip.py --with-supplements
```

功能补充包会生成在 dist/supplements/，其中 diff_summary 包可单独更新资产差异统计脚本，不需要替换服务器配置或重跑。完整试跑包仍在 dist/ 根目录。可用 --wheel 指定业务 wheel、--output-dir 指定其他输出目录。

## 历史归档

旧版本构建和被完整包覆盖的功能补充包放在 reports/dist_archive/带日期时间的子目录，保留原文件名。每次整理记录 archive_manifest.json，包含原位置、归档位置、大小、SHA-256 和归档原因，便于找回及核对。

归档只整理本地交付产物，不修改服务器已经安装的包、配置、测试数据和运行进度。未来发布新版本时，把不再作为当前交付的版本归档；不要直接递归删除整个 dist/。
