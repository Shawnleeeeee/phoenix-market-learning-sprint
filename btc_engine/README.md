# BTC 执行目录骨架

这套骨架用于新服务器上的 `BTCUSDT` 高频/短线执行，不和现有 Phoenix altcoin 执行目录混用。

模块分层：

- `market_data/`：原始市场、用户流、回填
- `signals/`：regime、动量、微观结构、方向判断
- `execution/`：下单、保护单、孤儿仓位恢复
- `risk/`：风控闸门、仓位、冷却、熔断
- `review/`：metrics、journal、10 单 review 导出
- `runtime/`：状态、心跳、健康检查

当前文件是骨架，不是完整策略实现。

研究/训练栈：

- [btc_engine/research/README.md](/Users/yanshisan/Desktop/币安交易/btc_engine/research/README.md)
- `research/`：数据集生成、特征工程、方向标签、回测、训练脚手架
- `research/flow_features/`：订单流与微观结构特征增强，供 flowpylib 风格研究使用
- `research/shadow/`：FreqAI 等影子模型对照实验，不进入执行链
