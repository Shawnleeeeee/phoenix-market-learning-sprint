# 雷霆（BTC 执行节点）文档

本目录存放新 BTC 高频/短线执行节点“雷霆”的部署与接口约定。

入口文件：

- [DEPLOYMENT_BLUEPRINT.md](/Users/yanshisan/Desktop/币安交易/deploy/btc/DEPLOYMENT_BLUEPRINT.md)
- [LEITING_HERMES_FLOWPYLIB_FREQAI_INTEGRATION.md](/Users/yanshisan/Desktop/币安交易/deploy/btc/LEITING_HERMES_FLOWPYLIB_FREQAI_INTEGRATION.md)
- [main_account.env.example](/Users/yanshisan/Desktop/币安交易/deploy/btc/main_account.env.example)
- [review_10_trades.schema.json](/Users/yanshisan/Desktop/币安交易/deploy/btc/review_10_trades.schema.json)
- [review_10_trades.example.json](/Users/yanshisan/Desktop/币安交易/deploy/btc/review_10_trades.example.json)
- [deploy/hermes/leiting_skill_digest.py](/Users/yanshisan/Desktop/币安交易/deploy/hermes/leiting_skill_digest.py)
- `systemd/`：雷霆服务模板
- `bootstrap_remote.sh`：新服务器一键部署脚本

配套代码骨架：

- [btc_engine/README.md](/Users/yanshisan/Desktop/币安交易/btc_engine/README.md)
- [btc_config/live.env.example](/Users/yanshisan/Desktop/币安交易/btc_config/live.env.example)
- [btc_config/strategy.yaml.example](/Users/yanshisan/Desktop/币安交易/btc_config/strategy.yaml.example)
- [btc_config/risk.yaml.example](/Users/yanshisan/Desktop/币安交易/btc_config/risk.yaml.example)
- [btc_config/research.yaml.example](/Users/yanshisan/Desktop/币安交易/btc_config/research.yaml.example)
- [btc_config/shadow_freqai.yaml.example](/Users/yanshisan/Desktop/币安交易/btc_config/shadow_freqai.yaml.example)
- [btc_config/flow_feature_flags.yaml](/Users/yanshisan/Desktop/币安交易/btc_config/flow_feature_flags.yaml)
- [btc_config/candidate_strategy_v1_trend_only.json](/Users/yanshisan/Desktop/币安交易/btc_config/candidate_strategy_v1_trend_only.json)
- [btc_data/README.md](/Users/yanshisan/Desktop/币安交易/btc_data/README.md)
- [scripts/btc/run_engine.sh](/Users/yanshisan/Desktop/币安交易/scripts/btc/run_engine.sh)
- [scripts/btc/run_post_fill_worker.sh](/Users/yanshisan/Desktop/币安交易/scripts/btc/run_post_fill_worker.sh)
- [scripts/btc/run_backfill.sh](/Users/yanshisan/Desktop/币安交易/scripts/btc/run_backfill.sh)
- [scripts/btc/run_dataset.sh](/Users/yanshisan/Desktop/币安交易/scripts/btc/run_dataset.sh)
- [scripts/btc/run_backtest.sh](/Users/yanshisan/Desktop/币安交易/scripts/btc/run_backtest.sh)
- [scripts/btc/run_train.sh](/Users/yanshisan/Desktop/币安交易/scripts/btc/run_train.sh)
- [scripts/btc/run_review.sh](/Users/yanshisan/Desktop/币安交易/scripts/btc/run_review.sh)
- [scripts/btc/run_flow_feature_refresh.sh](/Users/yanshisan/Desktop/币安交易/scripts/btc/run_flow_feature_refresh.sh)
- [scripts/btc/run_freqai_shadow.sh](/Users/yanshisan/Desktop/币安交易/scripts/btc/run_freqai_shadow.sh)

推荐新服务器目录：

- `/opt/leiting-btc`

命名约定：

- 项目名称：`雷霆`
- 英文目录：`leiting-btc`
