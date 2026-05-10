# 凤凰协议长期维护文档：Hermes

更新日期：2026-04-20

## 1. 目标

这份文档只记录 Hermes 作为 Phoenix 控制面的长期维护信息。

适用范围：
- Hermes gateway
- Telegram
- 状态摘要
- auto-cycle 编排
- 2 小时 review
- Hermes 与 Phoenix 的字段交接

---

## 2. Hermes 的角色

Hermes 不是交易引擎，Hermes 的职责是：

1. Telegram 收件箱
2. 控制面协调
3. 状态摘要
4. 自动循环编排
5. review / recap / 解释层

Hermes 不应该做：

- 直接调用 Binance
- 自己拼交易参数
- 绕过 Phoenix 风控

---

## 3. 当前部署形态

服务器路径：

- 安装目录：`/opt/hermes`
- Hermes home：`/root/.hermes`

当前运行方式：

- **user-level systemd service**
- 服务名：`hermes-gateway.service`

说明：
- 之前 Hermes 不是自己挂掉，而是在 2026-04-18 被 `systemctl --user stop hermes-gateway.service` 手动停掉
- 现在已恢复常驻运行

---

## 4. 当前配置要点

主要配置文件：

- `/root/.hermes/config.yaml`
- `/root/.hermes/.env`

当前原则：

- 主模型：`gemini-3.1-pro-preview`
- `model.provider: gemini`
- fallback：`openai-direct / gpt-5.4`
- Telegram 走服务器代理：
  - `http://127.0.0.1:7890`

重要提醒：

- 原生 Gemini provider 下不要写 `google/...`
- 如果服务器不能直连 Google，必须有：
  - `HTTP_PROXY`
  - `HTTPS_PROXY`
  - `ALL_PROXY`

---

## 5. 当前关键文件

- [deploy/hermes/README.md](/Users/yanshisan/Desktop/币安交易/deploy/hermes/README.md)
- [deploy/hermes/HERMES_PHOENIX_V2.md](/Users/yanshisan/Desktop/币安交易/deploy/hermes/HERMES_PHOENIX_V2.md)
- [deploy/hermes/update_state_summary.py](/Users/yanshisan/Desktop/币安交易/deploy/hermes/update_state_summary.py)
- [deploy/hermes/phoenix_bi_hourly_review.py](/Users/yanshisan/Desktop/币安交易/deploy/hermes/phoenix_bi_hourly_review.py)
- [deploy/hermes/phoenix_hermes_runner.sh](/Users/yanshisan/Desktop/币安交易/deploy/hermes/phoenix_hermes_runner.sh)
- [skills/phoenix-operator/phoenix_openclaw.py](/Users/yanshisan/Desktop/币安交易/skills/phoenix-operator/phoenix_openclaw.py)
- [phoenix/hermes_notify.py](/Users/yanshisan/Desktop/币安交易/phoenix/hermes_notify.py)

---

## 6. 已完成的重要能力

### 6.1 状态摘要

已统一成中文并可反映：

- raw top candidate
- actionable top candidate
- directional bias
- market / execution / event scores
- 当前活跃仓位
- 当前保护链状态
- 上一笔净利润
- 当前 skip reason

### 6.2 review

每 2 小时 review 已实现，但当前交易侧已暂停。

当前 review 原则：

- recommendations-only
- 不自动改 live 配置

### 6.3 Telegram 通知

已统一中文：

- arm ready
- auto entry
- breakeven / trailing
- close
- worker failure
- 2 小时 review

### 6.4 诊断命令

已支持：

- `why`
- `why-not`
- `market`
- `risk`
- `status`

---

## 7. 记忆与上下文策略

当前长期原则：

1. Hermes 维持短上下文
2. 结构化状态优先，不上向量记忆
3. 交易线程和研究线程分开
4. `phoenix_state.*` 是控制面主摘要
5. review / journals / worker files 是复盘依据

当前明确不做：

- 不上 Open Viking
- 不上运行时向量记忆
- Obsidian 不进执行链

---

## 8. 当前需要继续维护的重点

### 高优先级

1. 保持 Hermes 常驻运行
2. 保持 Telegram 稳定连接
3. 保持状态摘要与交易所实况一致

### 中优先级

4. 持续优化状态页可读性
5. 继续优化 TG 指令面
6. 若未来恢复自动交易，再恢复 `phoenix-review.timer`

### 低优先级

7. 如果以后 trade journals 足够多，再考虑更高级记忆方案

---

## 9. 当前默认约束

1. Hermes 不能绕过 Phoenix 下单
2. Hermes 不能直接决定仓位和止损
3. Hermes 只做：
   - orchestrator
   - explainer
   - monitor
   - reviewer

---

## 10. 常用维护命令

### 查状态

```bash
ssh -i /Users/yanshisan/Downloads/LUMIEAI.pem root@39.107.110.72 \
  'export HOME=/root; export XDG_RUNTIME_DIR=/run/user/0; /opt/hermes/venv/bin/python -m hermes_cli.main gateway status'
```

### 启动

```bash
ssh -i /Users/yanshisan/Downloads/LUMIEAI.pem root@39.107.110.72 \
  'export HOME=/root; export XDG_RUNTIME_DIR=/run/user/0; /opt/hermes/venv/bin/python -m hermes_cli.main gateway start'
```

### 停止

```bash
ssh -i /Users/yanshisan/Downloads/LUMIEAI.pem root@39.107.110.72 \
  'export HOME=/root; export XDG_RUNTIME_DIR=/run/user/0; /opt/hermes/venv/bin/python -m hermes_cli.main gateway stop'
```

