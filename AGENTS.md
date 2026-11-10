# 收益贡献审计的价格口径提醒

用户已确认：本版本的收益贡献审计使用整点价格（当前默认价格表为 `close_price_60min`），服务器上的收益贡献审计代码使用 VWAP 价格。

后续修改收益贡献审计相关代码、价格读取、收益时间对齐或对应测试前，先在进度说明中提醒用户上述差异，并在修改及对账时核对实际价格口径和收益区间。相关文件包括：

- `entrypoints/audit_return_contribution.py`
- `postprocessing/return_contribution.py`
- `postprocessing/return_contribution_plots.py`
- `tests/test_return_contribution.py`

该提醒不要求额外确认；继续执行用户已授权的任务。
