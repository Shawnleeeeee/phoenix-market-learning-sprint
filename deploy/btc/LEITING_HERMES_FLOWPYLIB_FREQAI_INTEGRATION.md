# 雷霆 × Hermes × flowpylib × FreqAI 整合方案

这份文档定义 BTC 新系统的长期分工边界，避免后续把研究、控制面、执行权混在一起。

目标分工：

- `雷霆`：唯一执行内核，负责下单和风控
- `Hermes`：唯一控制面，负责监控、复盘、建议、通知
- `flowpylib`：只进入研究层，增强订单流和微观结构特征
- `FreqAI`：只进入影子验证层，做对照实验，不直接接管执行

## 一、核心边界

### 1. 雷霆是唯一执行者

只有雷霆可以：

- 读取交易账户签名接口
- 下单、撤单、改杠杆
- 放置初始止损、保本止损、追踪止损
- 处理孤儿仓位和残留保护单
- 持有任何实盘或 testnet 交易 API 凭据

任何其他组件都不能：

- 直接调用 Binance 交易接口
- 绕过雷霆风控直接下单
- 热修改雷霆 live 配置后立即生效

### 2. Hermes 只做控制面

Hermes 只做：

- Telegram 交互
- 状态摘要
- 每 10 单正式复盘
- 研究摘要同步
- 中文建议摘要
- 唤醒 Codex 做优化

Hermes 不做：

- 直接下单
- 直接调杠杆
- 直接调整止损参数
- 直接执行模型信号

### 3. flowpylib 只进研究层

flowpylib 的职责：

- 订单流失衡特征
- 订单簿失衡特征
- 微观结构变点检测
- TCA 风格特征

flowpylib 不做：

- 实时执行
- 实盘风控
- live 下单触发

### 4. FreqAI 只做影子验证

FreqAI 的职责：

- 用同一份 BTC 数据跑影子策略
- 输出预测概率、方向偏置、特征重要性、影子回测结果
- 和雷霆规则策略做对照

FreqAI 不做：

- 直接连接雷霆执行入口
- 直接给 Binance 发单
- 自动替换雷霆 live 策略

## 二、落地到当前仓库的目录改造

当前仓库建议分成 4 条主线：

```text
btc_engine/
  execution/        # 雷霆唯一执行权
  risk/             # 雷霆唯一风控权
  runtime/          # 雷霆运行状态与 Hermes 同步
  market_data/      # 执行侧实时数据
  review/           # 实盘复盘
  research/         # 研究层总入口
    core/           # 现有 dataset/features/labels/train/backtest/walk_forward/sensitivity
    flow_features/  # 新增：flowpylib 适配与订单流特征
    shadow/         # 新增：FreqAI shadow 对照输出

deploy/
  btc/
    ...             # 雷霆部署、systemd、环境模板
  hermes/
    ...             # Hermes 控制面、摘要、digest、长期记忆

btc_config/
  live.env.example
  strategy.yaml.example
  risk.yaml.example
  research.yaml.example
  candidate_strategy_v1_trend_only.json
  shadow_freqai.yaml.example   # 新增
  flow_feature_flags.yaml      # 新增

scripts/
  btc/
    run_engine.sh
    run_post_fill_worker.sh
    run_review.sh
    run_research_refresh.sh
    run_freqai_shadow.sh       # 新增
    run_flow_feature_refresh.sh# 新增
```

## 三、按层放置组件

### A. 执行层：只放雷霆

目录：

- `btc_engine/execution/`
- `btc_engine/risk/`
- `btc_engine/runtime/engine_service.py`
- `btc_engine/runtime/state_store.py`
- `btc_engine/market_data/` 中和 live 决策直接相关的实时数据

当前保持唯一执行权的接口：

- `btc_engine/execution/binance_signed.py`
- `btc_engine/execution/router.py`
- `btc_engine/execution/order_manager.py`
- `btc_engine/execution/protective_orders.py`
- `btc_engine/execution/post_fill_worker.py`
- `btc_engine/execution/orphan_recovery.py`
- `btc_engine/risk/gating.py`
- `btc_engine/risk/sizing.py`
- `btc_engine/risk/cooldown.py`
- `btc_engine/risk/kill_switch.py`

这部分的规则：

- 只有这里能持有交易 API key
- 只有这里能判断 `preflight -> entry -> initial stop -> breakeven -> trailing`
- 只有这里能做 orphan recovery

### B. 研究层：接 flowpylib

建议新增目录：

```text
btc_engine/research/flow_features/
  __init__.py
  adapter.py
  orderbook_imbalance.py
  metaorder_features.py
  tca_features.py
  changepoint_features.py
```

职责：

- 从雷霆已经采集和落盘的深度/盘口/逐步快照中提取更强特征
- 产出 research dataset 可消费的结构化列
- 不直接参与 live 执行代码路径

接入点：

- `btc_engine/research/dataset.py`
- `btc_engine/research/features.py`

具体方式：

1. `flow_features/adapter.py`
   - 负责把雷霆快照格式映射成 flowpylib 可处理的输入
2. `orderbook_imbalance.py`
   - 产出多窗口 OBI、depth imbalance、pressure shift
3. `tca_features.py`
   - 产出冲击成本、滑点稳定性、局部流动性退化信号
4. `changepoint_features.py`
   - 产出结构断点、异常失衡切换信号

这些输出进入：

- `dataset.csv`
- `train.py`
- `walk_forward.py`
- `sensitivity.py`

### C. 影子验证层：接 FreqAI

建议新增目录：

```text
btc_engine/research/shadow/
  __init__.py
  freqai_export.py
  freqai_ingest.py
  compare_with_leiting.py
  promote_guardrails.py
```

职责：

- 把雷霆研究数据导出成 FreqAI/影子模型可消费格式
- 接收 FreqAI 的预测结果、回测结果、特征重要性
- 与雷霆规则策略做对照

不做：

- 不生成 live 下单动作
- 不直接写雷霆 live 策略配置

影子输出建议文件：

```text
btc_data/research/shadow/
  freqai_input/
  freqai_predictions/
  freqai_backtests/
  latest_shadow_summary.json
  latest_shadow_summary.md
```

## 四、Hermes 应该读取哪些文件

Hermes 不直接读原始盘口流，也不直接碰 Binance。Hermes 只读“已结构化的结果文件”。

### 1. 执行状态

来自雷霆：

- `btc_data/state/engine_snapshot.json`
- `btc_data/state/engine_snapshot.md`
- `btc_data/state/engine_heartbeat.json`

Hermes 用途：

- 当前信号状态
- 当前 regime
- 当前方向判断
- 当前 gates 是否通过
- 当前是否有活跃仓位

### 2. 实盘/模拟 journal

来自雷霆：

- `btc_data/journals/*.json`

Hermes 用途：

- 每 10 单正式复盘
- 净收益、滑点、持仓时长、止损替换统计

### 3. 正式 review

来自雷霆：

- `btc_data/reviews/formal_latest.json`
- `btc_data/reviews/formal_latest.md`

同步到 Hermes：

- `/root/.hermes/memories/btc_reviews/formal_latest.json`
- `/root/.hermes/memories/btc_reviews/formal_latest.md`

Hermes 用途：

- 中文 digest
- TG 正式复盘通知
- 给 Codex 的参数建议输入

### 4. 研究摘要

来自雷霆：

- `btc_data/research/backtests/research_reviews/latest.json`
- `btc_data/research/backtests/research_reviews/latest.md`

同步到 Hermes：

- `/root/.hermes/memories/btc_research/latest.json`
- `/root/.hermes/memories/btc_research/latest.md`

Hermes 用途：

- 展示当前研究主线
- 展示当前策略是否可上线
- 对比不同 regime、阈值、成本假设

### 5. Shadow 摘要

未来来自 FreqAI：

- `btc_data/research/shadow/latest_shadow_summary.json`
- `btc_data/research/shadow/latest_shadow_summary.md`

同步到 Hermes：

- `/root/.hermes/memories/btc_shadow/latest.json`
- `/root/.hermes/memories/btc_shadow/latest.md`

Hermes 用途：

- 对照规则策略 vs 影子模型
- 监控 FreqAI 是否连续优于雷霆规则策略

## 五、雷霆哪些接口保持唯一执行权

这是最重要的红线。

### 唯一执行权接口

以下接口和模块保持“只有雷霆能调用”的原则：

- Binance 签名账户接口
- 下单接口
- 撤单接口
- Algo 保护单接口
- 杠杆设置接口
- 孤儿仓位恢复接口

对应代码：

- `btc_engine/execution/binance_signed.py`
- `btc_engine/execution/router.py`
- `btc_engine/execution/order_manager.py`
- `btc_engine/execution/protective_orders.py`
- `btc_engine/execution/post_fill_worker.py`
- `btc_engine/execution/orphan_recovery.py`

### 只允许 Hermes 触发，不允许 Hermes 代替执行

Hermes 可以：

- 请求状态
- 请求 review
- 请求 digest
- 请求研究刷新
- 请求建议生成

Hermes 不可以：

- 直接持有 API key
- 直接 import 并调用下单函数去发单
- 直接写 live order

### FreqAI / flowpylib 绝不允许触达的路径

- `btc_engine/execution/*`
- `btc_engine/risk/gating.py`
- `btc_engine/risk/sizing.py`
- `btc_engine/execution/protective_orders.py`

它们只能通过：

- 生成研究结果
- 生成 shadow 结果
- 再由 Hermes 汇总
- 最后由 Codex 修改参数或代码

## 六、建议的最小增量改造顺序

### 第一阶段

只做目录和文件边界，不改 live 逻辑：

1. 新增 `btc_engine/research/flow_features/`
2. 新增 `btc_engine/research/shadow/`
3. 新增 `btc_config/shadow_freqai.yaml.example`
4. 新增 `btc_config/flow_feature_flags.yaml`
5. 新增 `scripts/btc/run_freqai_shadow.sh`
6. 新增 `scripts/btc/run_flow_feature_refresh.sh`

### 第二阶段

只接研究，不接执行：

1. 把 flowpylib 特征接入 `dataset.py`
2. 把 FreqAI 输入导出接到 `shadow/`
3. 把 shadow 摘要同步给 Hermes

### 第三阶段

做对照，不做接管：

1. Hermes 展示：
   - 雷霆规则结果
   - FreqAI 影子结果
   - 二者差异
2. Hermes 每 10 单 review 加入：
   - 规则 vs shadow 的偏差
   - 建议是否需要调 threshold/regime filter

### 第四阶段

只有在 shadow 足够稳定优于规则后，才考虑：

- 把 FreqAI 分数作为雷霆 `research-only filter`
- 而不是直接变成执行主脑

## 七、最终落地结论

这套仓库后续的长期边界应该是：

- `雷霆`：唯一执行者
- `Hermes`：唯一控制面和复盘官
- `flowpylib`：研究特征增强器
- `FreqAI`：影子对照实验器

顺序不能反。

如果顺序反了，最后一定会出现：

- 模型直接接管执行
- 控制面越权下单
- 研究代码污染 live 路径
- 复盘建议直接热更新实盘参数

这些都不应该发生。
