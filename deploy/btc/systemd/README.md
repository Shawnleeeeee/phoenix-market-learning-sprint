# 雷霆 systemd 模板

这些模板用于新服务器上的雷霆执行节点：

- `leiting-btc-backfill.service`
- `leiting-btc-engine.service`
- `leiting-btc-demo-auto.service`
- `leiting-btc-paper.service`
- `leiting-btc-review.service`
- `leiting-btc-review.timer`

默认工作目录：

- `/opt/leiting-btc`

说明：

- 这些模板不依赖现有服务器的 Hermes
- 不会触碰服务器已存在的代理服务
