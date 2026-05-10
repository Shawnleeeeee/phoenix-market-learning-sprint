# OI Family Forward Shadow After-Cost Evidence

只使用 forward shadow active outcomes；不使用 backtest PnL，不申请 testnet，不涉及 mainnet。

## shadow_oi_build_breakout_balanced
- unique signal 去重后样本数: 51 (outcome rows: 142)
- top symbol concentration: JTOUSDT 11.76%
- avg after-cost net return: outcome 28.2438 bps, unique-signal 26.8217 bps
- cost split avg: fee 8.0, spread(entry only) 5.527856338028169, slippage 42.89690492957747, funding proxy 0.0
- expected_net_edge_bps proxy: 26.821660843121236
- fakeout rate proxy: 52.94117647058823
- bidwall available/present: 0 / None
- bidwall removed failure rate: None (removed signals 0)
- BTC regime top: [('unavailable', 44), ('record', 7)]
- ETH regime top: [('unavailable', 51)]
- HMM regime top: [('unavailable', 51)]

## shadow_oi_build_breakout_quality
- unique signal 去重后样本数: 21 (outcome rows: 63)
- top symbol concentration: JTOUSDT 23.81%
- avg after-cost net return: outcome 12.5052 bps, unique-signal 12.5052 bps
- cost split avg: fee 8.0, spread(entry only) 7.158690476190476, slippage 56.33841904761905, funding proxy 0.0
- expected_net_edge_bps proxy: 12.50515539407223
- fakeout rate proxy: 57.142857142857146
- bidwall available/present: 0 / None
- bidwall removed failure rate: None (removed signals 0)
- BTC regime top: [('unavailable', 17), ('record', 4)]
- ETH regime top: [('unavailable', 21)]
- HMM regime top: [('unavailable', 21)]

## discovery_bidwall_oi_build_continuation
- unique signal 去重后样本数: 7 (outcome rows: 22)
- top symbol concentration: TSTUSDT 42.86%
- avg after-cost net return: outcome 81.3765 bps, unique-signal 64.1681 bps
- cost split avg: fee 8.0, spread(entry only) 4.4771363636363635, slippage 25.94589090909091, funding proxy 0.0
- expected_net_edge_bps proxy: 64.16808581876774
- fakeout rate proxy: 14.285714285714286
- bidwall available/present: 0 / None
- bidwall removed failure rate: None (removed signals 0)
- BTC regime top: [('unavailable', 5), ('record', 2)]
- ETH regime top: [('unavailable', 7)]
- HMM regime top: [('unavailable', 7)]

## Verdict
证据不足：仍不允许 limited testnet，不允许 mainnet。
主要缺口：bidwall persistence / bidwall removed 字段不足，ETH/HMM regime 未稳定 join，30s horizon 缺失或不完整；必须先由 Builder/Auditor 确认字段和 gate。
