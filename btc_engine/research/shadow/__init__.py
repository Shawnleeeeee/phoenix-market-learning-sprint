"""Shadow-model integration layer.

This package is reserved for non-executing experiments such as FreqAI and
other external model comparisons. It must never obtain trading privileges.
"""

from .compare_with_leiting import compare_shadow_with_leiting
from .freqai_export import build_freqai_shadow_payload
from .freqai_ingest import load_shadow_predictions
from .promote_guardrails import evaluate_shadow_promotion_guardrails

__all__ = [
    "build_freqai_shadow_payload",
    "compare_shadow_with_leiting",
    "evaluate_shadow_promotion_guardrails",
    "load_shadow_predictions",
]
