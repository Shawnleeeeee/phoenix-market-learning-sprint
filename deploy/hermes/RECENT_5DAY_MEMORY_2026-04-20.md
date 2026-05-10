# 凤凰协议 / Hermes 最近 5 天记忆文档

更新日期：2026-04-20  
覆盖时间：2026-04-16 ～ 2026-04-20  
用途：作为 Phoenix / Hermes / 本机代理的最近阶段性上下文、决策记录、已完成事项与未完成事项摘要。

## 1. 这 5 天的主线

这 5 天主要完成了 4 条主线：

1. 打通 Binance 统一账户（Portfolio Margin, PAPI）真实交易链路  
2. 搭起 Hermes × Phoenix 控制面与交易面分层架构  
3. 把 Phoenix 从“人工验证脚本”推进到“可自动循环运行的实盘系统”  
4. 把本机 Clash / Telegram / Codex 网络环境整理成可用状态

当前总判断：

- Phoenix：技术上已能真实跑单、挂保护、保本、追踪、平仓、落 journal
- Hermes：控制面、TG、状态摘要、复盘、自动循环编排已打通
- 服务器：目前 **Hermes 常驻运行，Phoenix 自动交易已暂停**
- 本机：Clash Verge Rev 已可用，Telegram 需显式走 `SOCKS5 127.0.0.1:7897`

---

## 2. 最近 5 天做过的事情

## 2026-04-16：统一账户 / PAPI 路线确认与 Phoenix 预检修正

### 关键结论

- 交易账户实际走的是 **Portfolio Margin / PAPI**，不是经典 `fapi`
- 统一账户余额应该读 `/papi/v1/account`
- `BTCUSDT` 在账户里实际是：
  - `leverage = 5`
  - `marginType = CROSSED`
  - `dualSidePosition = true`（Hedge Mode）

### 做过的事

- 查 Binance 官方文档和官方代码库，确认 PM / PAPI 的正确接口路径
- 修正 Phoenix 统一账户预检逻辑：
  - 余额改读 `papi`
  - 增加 `um_account_config / um_symbol_config`
  - 不再误把统一账户当成经典合约账户

### 结果

- `probe` / `preflight` 能正确读取统一账户
- 统一账户余额、杠杆、持仓模式都能识别

---

## 2026-04-17：Hermes 安装、模型代理、Telegram 打通

### 做过的事

- 在服务器 `/opt/hermes` 独立安装 Hermes
- 安装 Python 3.11 虚拟环境
- 配置：
  - Gemini 主模型
  - OpenAI `gpt-5.4` 作为 fallback
  - Telegram bot
  - Telegram allowed user
- 修复模型调用代理问题：
  - 给 Hermes 增加 `HTTP_PROXY / HTTPS_PROXY / ALL_PROXY`

### 关键问题与修复

1. `google/gemini-3.1-pro-preview` 在 Hermes 原生 `gemini` provider 下写法不对  
   修成：
   - `provider: gemini`
   - `model: gemini-3.1-pro-preview`

2. OpenAI fallback 初期未被正确识别  
   修成：
   - `openai-direct`
   - `transport: codex_responses`

3. Telegram 连接要显式走服务器代理  
   统一使用：
   - `http://127.0.0.1:7890`

### 结果

- Hermes 可通过 Telegram 工作
- Gemini 主链和 OpenAI fallback 都能正常配置

---

## 2026-04-17 ～ 2026-04-18：Hermes × Phoenix 架构、状态摘要、风控链落地

### 架构定型

- `Hermes = 控制面 / TG / 状态摘要 / 自动循环 / 复盘`
- `Phoenix = 数据 / 评分 / 方向 / 风控 / Binance 执行`
- 明确拒绝“让聊天模型直接拼 Binance 请求”

### 新增能力

- `phoenix_state.json / .md`
- `pending_confirmation`
- `Guardian worker` 状态文件
- `trade journals`
- `last_exit recap`
- 2 小时 review

### 风控与执行能力

- `arm -> confirm` 短时效 token 闸门
- 成交后 detached Guardian worker
- 自动：
  - 初始止损
  - 保本触发
  - trailing stop
  - 平仓 journal / recap

### 资金管理

- 加入 `RISK_BUDGET` 模式
- 总资金按账户余额动态缩放
- 不再默认每次固定满打 `200U`

---

## 2026-04-17 ～ 2026-04-18：真实实盘验证与首轮问题暴露

### 真实单验证

完成了首笔 `BTCUSDT` 实盘验证单，走通：

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

### 重要修复

1. **Hedge Mode 必填 `positionSide`**
- 统一账户 `dualSidePosition=true`
- 若不带 `positionSide=LONG/SHORT`，实盘写单会失败

2. **PM 条件单不能乱带 `reduceOnly`**
- 特别是 Hedge 模式下，条件单参数要按交易所规则调整

3. **PnL 摘要改成 Binance 实际净利润口径**
- 改成读取：
  - 实际成交
  - 手续费
  - 资金费
- 之前 journal 里的“按最后 mark 估算盈亏”保留为辅助项，不再当最终结果

### 首笔实盘闭环结果

- `BTCUSDT`
- 最终净利润与 Binance 后台对齐：
  - `11.38236687 USDT`

---

## 2026-04-18：自动循环、候选门槛、方向判断、自动开多/开空

### 自动循环链路

加入并反复修正：

- `phoenix-autocycle.timer`
- 候选显著变化检测
- `actionable top candidate`
- 有持仓时自动跳过
- stale worker 清理

### 候选门槛

- `PHOENIX_STRATEGY_MIN_SCORE`
- `PHOENIX_REJECT_BLOCKED_CANDIDATES`
- `PHOENIX_MAX_OPEN_POSITIONS = 1`

### 方向层

新增：

- `directional_bias = LONG / SHORT / NONE`
- `directional_score`
- `directional_breakdown`

并完成：

- 空头版初始止损
- 空头版保本触发
- 空头版保本止损
- 空头版 trailing

### 当前结论

- 自动开空能力代码上已具备
- 但“修复后的第一笔真实 auto short 完整闭环”仍待市场触发验证

---

## 2026-04-18 ～ 2026-04-19：V2 数据架构和 Hermes 诊断命令

### V2 目标

把数据从“发现信号”推进到“确认信号 + 执行质量信号”。

### 已接入 / 产出的第一批 V2 字段

- `spread_bps`
- `depth_bid_5`
- `depth_ask_5`
- `depth_imbalance`
- `estimated_slippage_bps`
- `estimated_slippage_for_order_usdt`
- `funding_rate`
- `premium_index`
- `mark_index_basis_pct`
- `market_confirmation_score`
- `execution_quality_score`
- `event_risk_score`
- `directional_conflicts`
- `event_flags`

### 第二批 V2 字段

- `taker_buy_ratio_5m`
- `aggressive_flow_delta`
- `liquidation_long_usd`
- `liquidation_short_usd`
- `liquidation_event_count`
- `next_funding_time_ms`
- `listing_age_hours`

### Hermes 新诊断命令

已加并验证：

- `why SYMBOL`
- `why-not SYMBOL`
- `market SYMBOL`
- `risk SYMBOL`

### 结果

Hermes 现在不仅能告诉你“开没开单”，还能解释：

- 为什么上榜
- 为什么没做
- 当前市场确认度如何
- 当前执行质量如何
- 当前有哪些风险拦截项

---

## 2026-04-19：Chinese 化、review、可观测性增强

### 已汉化

- Telegram 通知
- `phoenix_state.md`
- `trade_journals/*.md`
- `phoenix_last_exit.md`
- 2 小时 review 摘要

### Review 机制

每 2 小时生成一次：

- 最近 2 小时 auto-cycle 统计
- trade journals
- worker 状态
- 当前 compact state

目前 review 是：

- `recommendations-only`
- 不会自动改 live 参数或代码

---

## 2026-04-19 ～ 2026-04-20：worker 生命周期、stale 清理、systemd 风险、服务器恢复

### 暴露的问题

1. `FARTCOIN` / `ICNT` 一类 worker 状态可能陈旧
2. `systemd` 下直接 `Popen(start_new_session=True)` 并不稳
3. Review 曾经把陈旧仓位误判成活跃仓位

### 做过的修复

- stale worker 会按 Binance 实际持仓自动清理
- `autocycle` 以真实持仓为准，而不再只看本地 JSON
- Guardian worker 改成更独立的启动/守护思路
- Phoenix 自动交易已可按实际状态跳过或恢复

### 当前服务器最新状态（截至 2026-04-20）

- **Hermes：已恢复常驻运行**
  - `hermes-gateway.service`（user-level）当前 `active (running)`
  - Telegram 当前 `connected`
- **Phoenix 自动交易：已暂停**
  - `phoenix-autocycle.timer`：`disabled / inactive`
  - `phoenix-review.timer`：`disabled / inactive`

### Hermes 停止原因

不是自己崩掉，而是：

- 2026-04-18 21:09 有人手动执行了：
  - `systemctl --user stop hermes-gateway.service`

现已恢复。

---

## 3. 本机网络 / Clash / Telegram 侧最近做过的事

## Clash Verge Rev

### 当前本机结论

- 已安装并可正常运行
- 本地有效代理端口：
  - `127.0.0.1:7897`
- `7898` 当前不是稳定监听端口，不应再作为 Telegram SOCKS 端口使用

### 当前推荐配置

- 模式：`Rule`
- 系统代理：开启
- TUN：关闭
- 当前节点策略：
  - `GLOBAL -> 选择终端节点`
  - `选择终端节点 -> PSG自动切换`
  - `PSG自动切换 = fallback(🇸🇬PSG1, 🇸🇬PSG2, 🇸🇬PSG3)`

### 当前本机 Telegram 正确代理方式

- 类型：`SOCKS5`
- 主机：`127.0.0.1`
- 端口：`7897`

不要再使用 `7898`。

### 说明

- 本机 Clash 用于：
  - 浏览资料
  - Telegram
  - Claude/Codex 联网
- **不要拿本机直接跑 Binance 签名交易**
  - 交易执行仍应只留在服务器

---

## 4. 当前稳定结论

截至 2026-04-20：

### 服务器

- Hermes：运行中
- Telegram：连接中
- Phoenix 自动交易：暂停
- 统一账户：空仓
- 账户余额：最近一次查询约 `194.76 USDT`

### 本机

- Clash Verge Rev：可用
- PSG 自动故障切换：已配置并验证
- Telegram：只要指向 `SOCKS5 127.0.0.1:7897`，即可稳定使用

---

## 5. 尚未完成 / 后续优先事项

## A. 交易系统

1. **等待下一次真实 auto short 候选**
   - 验证空头链修复后的完整闭环

2. **继续增强方向判断质量**
   - 减少高分候选落到 `NONE`

3. **继续做执行质量硬闸门调优**
   - 滑点 / spread / funding / basis / event risk 阈值继续迭代

## B. Hermes × Phoenix

4. 如未来恢复自动交易，需要先确认：
   - Hermes 常驻正常
   - Telegram 正常
   - Phoenix timers 恢复
   - worker stale / orphan 逻辑正常

5. review 目前仍建议保持：
   - recommendations-only
   - 不自动改 live 参数

## C. 本机网络

6. 如后续仍觉本机偶发不稳：
   - 继续调 `PSG自动切换` 的健康检查参数
   - 不优先开 TUN，除非出现更多不服从系统代理的 GUI App

---

## 6. 对后续工作的默认约束

1. Binance 实盘执行默认留在服务器，不挪回本机  
2. Hermes 必须保持常驻运行  
3. Phoenix 自动交易现在默认保持暂停，除非明确恢复  
4. 所有 secret 不再发聊天，统一走服务器环境文件  
5. 本机 Telegram 代理默认记住：
   - `SOCKS5 127.0.0.1:7897`

---

## 7. 快速恢复提示

### 查 Hermes

```bash
ssh -i /Users/yanshisan/Downloads/LUMIEAI.pem root@39.107.110.72 \
  'export HOME=/root; export XDG_RUNTIME_DIR=/run/user/0; /opt/hermes/venv/bin/python -m hermes_cli.main gateway status'
```

### 启动 Hermes

```bash
ssh -i /Users/yanshisan/Downloads/LUMIEAI.pem root@39.107.110.72 \
  'export HOME=/root; export XDG_RUNTIME_DIR=/run/user/0; /opt/hermes/venv/bin/python -m hermes_cli.main gateway start'
```

### 恢复 Phoenix 自动交易（谨慎）

```bash
ssh -i /Users/yanshisan/Downloads/LUMIEAI.pem root@39.107.110.72 \
  'systemctl enable --now phoenix-autocycle.timer phoenix-review.timer'
```

### 检查本机 Clash 运行时代理组

```bash
curl --silent --show-error --unix-socket /tmp/verge/verge-mihomo.sock http://localhost/proxies
```

