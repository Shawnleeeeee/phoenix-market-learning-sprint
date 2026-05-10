# 凤凰协议长期维护文档：交易

更新日期：2026-04-20

## 1. 目标

这份文档只记录 Phoenix 交易内核本身的长期维护信息，不掺 Hermes 网关和本机网络细节。

适用范围：
- Binance 统一账户（Portfolio Margin / PAPI）
- Phoenix 扫描、评分、方向判断、预检、下单、管仓、平仓、journal

---

## 2. 当前架构

Phoenix 当前交易链路分为：

1. `Scout`
   - 市场与 Alpha 发现层
   - 产出 snapshot / overlap universe / v2 市场字段

2. `Judge`
   - 候选评分
   - blocked reasons
   - directional bias
   - market / execution / risk score

3. `Executor`
   - `probe`
   - `preview`
   - `preflight`
   - `arm`
   - `dispatch`
   - `confirm`

4. `Guardian worker`
   - 成交后保护管理
   - 初始止损
   - 保本替换
   - trailing
   - 平仓后 journal / recap

---

## 3. 当前账户与执行约束

当前已验证环境：

- 账户模式：`portfolio_margin`
- 持仓模式：`HEDGE`
- 保证金表现：`CROSSED`
- 默认杠杆：`5x`

重要约束：

- Hedge Mode 下必须带 `positionSide`
- PM 条件单与普通单接口不同
- journal / recap 的最终盈亏必须以 Binance 真实结算口径为准

---

## 4. 当前关键文件

核心交易文件：

- [phoenix_data_scout.py](/Users/yanshisan/Desktop/币安交易/phoenix_data_scout.py)
- [phoenix/judge.py](/Users/yanshisan/Desktop/币安交易/phoenix/judge.py)
- [phoenix/executor.py](/Users/yanshisan/Desktop/币安交易/phoenix/executor.py)
- [phoenix_executor.py](/Users/yanshisan/Desktop/币安交易/phoenix_executor.py)
- [phoenix_live_execute.py](/Users/yanshisan/Desktop/币安交易/phoenix_live_execute.py)
- [phoenix_post_fill_worker.py](/Users/yanshisan/Desktop/币安交易/phoenix_post_fill_worker.py)
- [phoenix/guardian_workers.py](/Users/yanshisan/Desktop/币安交易/phoenix/guardian_workers.py)
- [phoenix/binance_futures.py](/Users/yanshisan/Desktop/币安交易/phoenix/binance_futures.py)
- [phoenix/binance_web3.py](/Users/yanshisan/Desktop/币安交易/phoenix/binance_web3.py)
- [phoenix/models.py](/Users/yanshisan/Desktop/币安交易/phoenix/models.py)
- [phoenix/runtime_state.py](/Users/yanshisan/Desktop/币安交易/phoenix/runtime_state.py)

---

## 5. 已完成的重要能力

### 5.1 统一账户实盘闭环

已经真实跑通过：

- `cycle`
- `probe`
- `arm`
- `confirm`
- `live entry`
- `initial stop`
- `breakeven replacement`
- `trailing`
- `close`
- `journal / recap`

### 5.2 多头保护链

多头完整保护链已真实闭环验证。

### 5.3 空头保护链

空头逻辑已经实现：

- 初始止损
- 保本触发
- 保本止损
- trailing

但仍需要等待下一次真实 auto short 候选，完成“修复后的真实闭环验证”。

### 5.4 风险预算

已启用：

- `RISK_BUDGET`
- 按账户余额动态缩放
- 不再默认固定满打 200U

### 5.5 V2 数据字段

第一批和第二批已接入：

- `spread_bps`
- `depth_bid_5 / depth_ask_5 / depth_imbalance`
- `estimated_slippage_bps`
- `estimated_slippage_for_order_usdt`
- `funding_rate`
- `premium_index`
- `mark_index_basis_pct`
- `market_confirmation_score`
- `execution_quality_score`
- `event_risk_score`
- `taker_buy_ratio_5m`
- `aggressive_flow_delta`
- `liquidation_long_usd / liquidation_short_usd`
- `liquidation_event_count`
- `listing_age_hours`
- `directional_conflicts`
- `event_flags`

---

## 6. 当前默认策略原则

1. 发现层只负责找到值得研究的标的
2. 确认层负责资金、盘口、拥挤度、风险拦截
3. 方向层输出 `LONG / SHORT / NONE`
4. `NONE` 不允许自动实盘放行
5. execution quality 不达标禁止放行
6. event risk 过高禁止放行
7. 当前默认同时只允许 1 笔持仓

---

## 7. 当前需要继续维护的重点

### 高优先级

1. 继续观察并验证下一笔真实 auto short 完整闭环
2. 继续调优 directional bias，减少高分候选落到 `NONE`
3. 继续调优 execution quality 硬闸门阈值

### 中优先级

4. 继续增强 orphan position 自动重挂接
5. 增强启动后自愈恢复
6. 继续补事件流数据

### 低优先级

7. 继续细化 confidence 分档
8. 继续压缩 probe/debug 输出噪音

---

## 8. 当前不建议做的事

1. 不要把本机直接变成 Binance 实盘执行节点
2. 不要让 LLM 直接拼 Binance 请求
3. 不要让 Hermes 自己决定止损、仓位、杠杆
4. 不要因为测试空头链而故意违背策略强开反向单

---

## 9. 恢复 / 排查常用命令

### 查看统一账户状态

```bash
cd /Users/yanshisan/Desktop/币安交易
python3 phoenix_probe.py --symbol BTCUSDT
```

### 查看候选与方向

```bash
cd /Users/yanshisan/Desktop/币安交易
python3 phoenix_judge.py --snapshot phoenix_snapshot.openclaw.json --output phoenix_candidates.openclaw.json
```

### 查看当前保护链 / worker

```bash
cd /Users/yanshisan/Desktop/币安交易
python3 -m json.tool /root/.hermes/memories/phoenix_guardian_workers/<TOKEN>.json
```

