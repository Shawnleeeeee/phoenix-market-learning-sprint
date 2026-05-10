# 雷霆研究/训练栈

这一层把雷霆从“规则执行引擎”扩展到“可研究、可回测、可训练”的第一版。

包含 5 个入口：

- `dataset.py`
  - 把 `btc_data/raw/BTCUSDT/5m` 的原始 CSV 合并成研究数据集
  - 生成特征、入场质量标签、baseline 分数
- `features.py`
  - 第一版特征工程
  - 动量、波动、OI、taker flow、funding、premium、EMA、ATR、RSI、布林带、VWAP、MACD
- `labels.py`
  - 第一版多 horizon / regime-aware 入场质量标签
  - 同时生成 `6 / 12 / 24` 根 horizon 标签
  - 再根据当前 regime 选择主标签与主 edge
- `backtest.py`
  - 第一版回测框架
  - 支持 baseline 分数或模型分数
  - 带初始止损、保本、追踪、手续费、滑点、冷却
- `train.py`
  - 第一版训练脚手架
  - 先做一个纯 Python 的线性打分模型
  - 不依赖 `numpy / pandas / sklearn`
- `walk_forward.py`
  - 滚动时间窗验证
  - 避免只看一次固定切分的偶然结果
- `sensitivity.py`
  - 阈值 / 手续费 / 滑点 / 止损敏感性扫描
  - 用来识别策略是否只在“理想成本”下成立
- `export_research_review.py`
  - 把研究结果导出成 Hermes 可读摘要
  - 用于后续研究闭环

## 运行顺序

1. 回填原始数据
   - `scripts/btc/run_backfill.sh`
2. 生成训练数据集
   - `scripts/btc/run_dataset.sh`
3. 跑 baseline 回测
   - `scripts/btc/run_backtest.sh`
4. 跑第一版训练
   - `scripts/btc/run_train.sh`
5. 跑 walk-forward
   - `scripts/btc/run_walk_forward.sh`
6. 跑敏感性扫描
   - `scripts/btc/run_sensitivity.sh`
7. 导出 Hermes 研究摘要
   - `scripts/btc/run_research_review.sh`

## 落盘目录

- `btc_data/raw/`
  - 原始市场数据
- `btc_data/research/datasets/`
  - 训练数据集
- `btc_data/research/backtests/`
  - 回测结果与敏感性扫描
- `btc_data/research/models/`
  - 模型产物与训练报告
- `btc_data/research/research_reviews/`
  - 面向 Hermes 的研究摘要

## 当前边界

- 这不是深度学习/HFT 引擎
- 这是“第一版研究栈”
- 目标是：
  - 先把数据、特征、标签、回测、训练流程规范化
  - 再决定后面是否引入更强模型或额外外部数据

## 当前技术指标层

当前已经纳入训练特征：

- `EMA 12/24/48` 及均线间距
- `ATR 14`
- `RSI 14`
- `布林带宽度 / z-score (20)`
- `会话 VWAP 偏离`
- `MACD line / signal / histogram`
- `OI / taker` 覆盖标记与年龄特征
- 最近 30 天 `1m` 微观结构：
  - 最后一根 1m 收益
  - 1m 平均振幅
  - 1m 主动成交失衡
  - 5m 窗口主动成交失衡
- 最近 30 天 crowding 代理：
  - `globalLongShortAccountRatio`
  - `long/short account skew`

## 当前执行数据边界

- 历史 `1m taker flow`：已接入最近 30 天
- 历史 crowding ratio：已接入最近 30 天
- 历史盘口快照：Binance 不提供可用的长历史 REST 回溯，所以改成由运行中的引擎前瞻落盘到：
  - `btc_data/research/runtime/market_micro_snapshots.jsonl`

## 新增研究分层

### `flow_features/`

这一层预留给 `flowpylib` 风格的订单流、盘口失衡、TCA、变点检测特征。

边界：

- 只读研究数据
- 只产出特征
- 不允许触达 live 执行路径

### `shadow/`

这一层预留给 `FreqAI` 或其他影子模型。

边界：

- 只做影子验证
- 只导出研究输入和对照结果
- 不允许直接接管雷霆执行
