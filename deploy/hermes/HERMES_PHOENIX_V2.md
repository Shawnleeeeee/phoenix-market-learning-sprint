# Hermes × Phoenix V2

这份文档定义 Phoenix 数据架构 v2 如何接入 Hermes，以及 Hermes 应该学习什么资料、掌握什么技能、遵守什么边界。

## 核心原则

- Phoenix 继续做唯一交易内核：
  - 数据采集
  - 评分
  - 方向判断
  - 风控
  - 下单与管仓
- Hermes 只做控制面：
  - Telegram 交互
  - 状态页
  - 自动循环编排
  - 复盘
  - 解释“为什么做 / 为什么不做”

Hermes 不应该：
- 自己拼 Binance 请求
- 自己发明仓位、止损、杠杆
- 在没有 Phoenix 结构化结论的情况下临时决定方向

## V2 数据字段对接

Phoenix v2 需要把新增字段作为结构化结果写进候选文件，Hermes 只消费这些结果。

### 一、发现层

用途：
- 解释“为什么这个币进入候选池”

字段：
- `oi_delta_1m_pct`
- `oi_delta_5m_pct`
- `oi_delta_15m_pct`
- `price_change_1m_pct`
- `price_change_5m_pct`
- `price_change_15m_pct`
- `volume_5m_ratio_short`
- `volume_5m_ratio_medium`
- `volume_5m_ratio_long`
- `social_rank`
- `social_hype_change_pct`

Hermes 展示位置：
- TG 候选解释
- `phoenix_state.md`
- 2 小时复盘中的候选变化说明

### 二、确认层

用途：
- 解释“这波异动有没有真实资金和市场结构确认”

字段：
- `market_confirmation_score`
- `smart_money_weighted_buy_strength`
- `smart_money_latest_age_minutes`
- `topic_net_inflow_usd`
- `topic_latest_age_minutes`
- `funding_rate`
- `mark_index_basis_pct`
- `taker_buy_ratio_1m`
- `taker_buy_ratio_5m`
- `aggressive_flow_delta`

Hermes 展示位置：
- TG 候选解释里的“市场确认/聪明钱/题材”
- `phoenix_state.md` 的可执行候选段
- 2 小时复盘里的“为什么高分但未放行”

### 三、执行质量层

用途：
- 解释“现在下这笔单会不会吃大滑点，是否值得执行”

字段：
- `execution_quality_score`
- `spread_bps`
- `depth_bid_5`
- `depth_ask_5`
- `depth_imbalance`
- `estimated_slippage_bps`
- `estimated_slippage_for_order_usdt`

Hermes 展示位置：
- TG 候选解释里的“执行质量”
- `phoenix_state.md`
- `market SYMBOL` 诊断命令（后续）

### 四、事件与风险层

用途：
- 解释“为什么这笔单虽然分高，但仍然应该跳过”

字段：
- `event_risk_score`
- `event_flags`
- `audit_risk_level`
- `audit_risk_label`
- `audit_flags`
- `blocked_reasons`
- `directional_conflicts`

Hermes 展示位置：
- TG 候选解释里的“事件与冲突/风险项”
- `phoenix_state.md`
- 2 小时复盘中的拦截原因汇总
- `why-not SYMBOL` 诊断命令（后续）

## Hermes 需要新增的能力

Hermes 不需要新增交易权，而需要新增 5 个控制面能力。

### 1. 候选解释器

输入：
- Phoenix 候选字段

输出：
- 发现信号
- 市场确认
- 方向判断
- 执行质量
- 事件与风险

目标：
- 让 Telegram 消息不只告诉你“开仓了”，还告诉你“为什么此时能开”

### 2. 状态页观察器

Hermes 要把 `phoenix_state.md` 当成控制台，而不是流水账。

最少应持续展示：
- 原始榜首候选
- 可执行榜首候选
- 可执行方向
- 市场确认分
- 执行质量分
- 事件风险分
- 当前跳过原因
- 当前活跃仓位
- 当前保护链状态
- 上一笔实际净利润

### 3. 复盘官

2 小时复盘不应该只看：
- 跑了几轮
- 平了几笔

还要看：
- 哪些候选因为滑点被拒
- 哪些候选因为事件风险被拒
- 哪些高分候选一直落到 `directional_bias=NONE`
- 最常见的阻断原因是什么

### 4. 诊断问答器

后续建议为 Hermes 增加这些 Phoenix 专用命令：
- `status`
- `why SYMBOL`
- `why-not SYMBOL`
- `market SYMBOL`
- `risk SYMBOL`

其中：
- `why`：解释为什么上榜
- `why-not`：解释为什么被阻断
- `market`：展示盘口、滑点、funding、basis、主动买卖方向
- `risk`：展示审计、事件、blocked 原因

### 5. 学习资料管理器

Hermes 应该从结构化知识里学习，而不是从长聊天历史里“悟”。

建议知识来源：
- Phoenix 运行文档
- Phoenix 字段字典
- 交易状态机文档
- 风控闸门规则
- journal / recap / review 历史

不建议直接喂给 Hermes 的东西：
- 原始长聊天记录
- 杂乱网页摘抄
- 不成体系的主观判断

## Hermes 该学习什么资料

建议分成 4 套资料。

### A. Phoenix 运行资料

Hermes 必须知道：
- 账户模式：`portfolio_margin / HEDGE / CROSSED`
- 当前执行模式：`DRY_RUN_ONLY / MANUAL_CONFIRM / AUTO_CONFIRM_WHEN_RULES_PASS`
- 风险预算模式
- 最大持仓数
- 冷却规则
- Guardian 状态机

来源：
- `deploy/hermes/README.md`
- `phoenix_state.json/.md`

### B. Phoenix 字段字典

Hermes 必须知道每个字段是什么意思，而不是只会原样朗读。

至少要懂：
- `directional_bias`
- `blocked_reasons`
- `market_confirmation_score`
- `execution_quality_score`
- `event_risk_score`
- `funding_rate`
- `spread_bps`
- `estimated_slippage_bps`
- `directional_conflicts`

建议后续单独维护一份：
- `deploy/hermes/PHOENIX_FIELD_DICTIONARY.md`

### C. 市场结构基础资料

Hermes 不需要会交易，但必须会解释：
- 点差
- 深度
- 滑点
- funding
- basis
- taker buy ratio
- OI 与价格的联动

否则它虽然能报数字，但不会解释意义。

### D. 复盘资料

Hermes 应该学习过去的：
- trade journals
- last exit
- 2 小时 review
- skip reason 统计

这些资料让 Hermes 逐步变成：
- 更会解释
- 更会提醒
- 更会指出控制面问题

## Hermes 该掌握什么技能

### 必须掌握
- 候选解释
- 风险解释
- 状态总结
- 复盘总结
- 拦截原因解释
- 方向冲突解释

### 可以逐步增强
- 候选对比（为什么 A 强于 B）
- 市场质量对比（为什么这笔滑点更危险）
- 复盘建议生成（建议层，不自动改参数）

### 不应该掌握
- 直接下单
- 自主调整 live 风控参数
- 绕过 Phoenix 闸门
- 用聊天临时生成交易规则

## 推荐落地顺序

1. 让 Phoenix 先产出 v2 字段
2. 让 Hermes 状态页和 TG 先展示这些字段
3. 让 2 小时复盘开始汇总这些字段
4. 再增加 `why / why-not / market / risk` 命令
5. 最后才考虑更复杂的长期记忆

## 当前状态

现在已经完成的：
- `phoenix_state.md` 已可展示可执行候选和关键运行状态
- TG 候选解释已支持方向、市场确认、风险项
- V2 字段透传骨架已经可以接新字段

还没完成的：
- Phoenix 侧尚未真正生产全部 v2 字段
- Hermes 诊断命令 `why / why-not / market / risk` 仍未落地
- 字段字典还未单独成文
