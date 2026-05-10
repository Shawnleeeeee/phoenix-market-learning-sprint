# 凤凰协议长期维护文档：本机网络

更新日期：2026-04-20

## 1. 目标

这份文档只记录本机（M2 Mac）上的 Clash Verge Rev、Telegram、终端代理环境和使用原则。

适用范围：
- 本机浏览器
- Telegram Desktop
- Claude / Codex / 终端代理
- 本机研究环境

不适用范围：
- Binance 实盘执行

---

## 2. 总原则

### 本机代理的用途

允许：

- 研究资料
- Telegram
- Claude / Codex 联网
- 普通网页访问

不建议：

- 直接拿本机跑 Binance 签名交易

原因：

- 本机出口 IP 与服务器交易白名单 IP 不一致
- 真实交易执行仍应只留在服务器

---

## 3. 当前本机 Clash 结论

客户端：

- `Clash Verge Rev`

当前有效代理端口：

- `127.0.0.1:7897`

重要说明：

- `7897` 是当前稳定工作的 `mixed-port`
- **Telegram 应直接用 `7897`**
- `7898` 目前不是稳定监听端口，不要再依赖它

---

## 4. 当前推荐配置

### Clash 模式

- 模式：`Rule`
- 系统代理：开启
- TUN：关闭

当前默认原因：

- 对日常最稳
- 对本机开发干扰最小
- 已够 Telegram / 浏览器 / Claude / Codex 使用

---

## 5. 当前节点策略

当前已配置：

- `GLOBAL -> 选择终端节点`
- `选择终端节点 -> PSG自动切换`
- `PSG自动切换 = fallback`
  - `🇸🇬PSG1`
  - `🇸🇬PSG2`
  - `🇸🇬PSG3`

说明：

- 默认优先 `PSG1`
- 若不通则自动切 `PSG2 / PSG3`

最近一次验证：

- 当前落点：`🇸🇬PSG1`
- 最近一次出口 IP：`64.118.137.55`

---

## 6. Telegram Desktop 正确配置

Telegram 不要只赌系统代理，推荐显式配置：

- 类型：`SOCKS5`
- 服务器：`127.0.0.1`
- 端口：`7897`

说明：

- Telegram Desktop 在 macOS 上不完全稳定服从系统代理
- 显式 SOCKS5 更稳

不要使用：

- `127.0.0.1:7898`

---

## 7. 终端 / Claude / Codex

当前终端环境变量应指向：

- `HTTP_PROXY=http://127.0.0.1:7897`
- `HTTPS_PROXY=http://127.0.0.1:7897`
- `ALL_PROXY=http://127.0.0.1:7897`
- `NO_PROXY=127.0.0.1,localhost`

注意：

- `~/.zshrc` 的代理变量只对终端生效
- GUI App 不一定会自动继承 `~/.zshrc`

---

## 8. 当前已知问题与建议

### 问题 1：有些 GUI App 不完全服从系统代理

典型例子：

- Telegram Desktop

解决方案：

- 优先用 App 自己的代理设置（如 Telegram SOCKS5）

### 问题 2：上传速度偏弱

最近一次 `PSG自动切换` 测试结果：

- 下载约 `13 Mbps`
- 上传约 `1.6 Mbps`

结论：

- 浏览、消息、模型请求够用
- 大文件上传体验一般

---

## 9. 什么时候再考虑开 TUN

只有在这些情况下再考虑：

1. 还有更多 GUI App 不服从系统代理
2. Telegram 显式 SOCKS5 后仍不稳
3. 需要更彻底的透明代理

当前默认仍不建议：

- `系统代理 ON + TUN ON` 双开长期使用

---

## 10. 常用检查方法

### 查当前运行组

```bash
curl --silent --show-error --unix-socket /tmp/verge/verge-mihomo.sock http://localhost/proxies
```

### 查出口 IP

```bash
curl --proxy http://127.0.0.1:7897 https://api.ipify.org
```

### 测 Telegram 代理可用性

```bash
curl --socks5-hostname 127.0.0.1:7897 https://web.telegram.org
```

