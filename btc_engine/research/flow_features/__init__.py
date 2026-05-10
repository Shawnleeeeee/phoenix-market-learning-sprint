"""flowpylib research integration layer for 雷霆.

This package is intentionally research-only.
Nothing here is allowed to place orders or mutate live trading state.
"""

from .adapter import normalize_depth_snapshot, snapshot_to_feature_input, load_snapshot_jsonl
from .changepoint_features import compute_changepoint_features
from .metaorder_features import compute_metaorder_features
from .orderbook_imbalance import compute_orderbook_imbalance_features
from .tca_features import compute_tca_features

__all__ = [
    "compute_changepoint_features",
    "compute_metaorder_features",
    "compute_orderbook_imbalance_features",
    "compute_tca_features",
    "load_snapshot_jsonl",
    "normalize_depth_snapshot",
    "snapshot_to_feature_input",
]
