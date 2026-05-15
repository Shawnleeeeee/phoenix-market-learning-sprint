from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


STRATEGY_REGISTRY_VERSION = "v1.0"

DEFAULT_STRATEGY_REGISTRY = {
    "registry_version": STRATEGY_REGISTRY_VERSION,
    "mode": "shadow",
    "live_trading_enabled": False,
    "promotion_allowed": False,
    "direct_config_change_allowed": False,
    "strategies": {},
    "experiments": {},
}

OLD_STRATEGY_IDS = {
    "volatility_long",
    "volatility_short",
    "trend_pullback_long",
    "trend_breakout_long",
    "impulse_pullback_long",
    "impulse_bounce_short",
    "flush_reversion_long",
    "pressure_breakdown_short",
    "explore_reclaim_long",
    "explore_reject_short",
    "major_reclaim_long",
    "micro_reclaim_long",
    "micro_reject_short",
    "range_reversion_long",
    "range_reversion_short",
    "swing_pullback_long",
    "swing_bounce_short",
    "failed_bounce_short",
}

VALID_MANIFEST_STATUSES = {
    "disabled",
    "shadow_only",
    "candidate_shadow",
    "limited_testnet",
    "promoted_testnet",
    "mainnet_blocked",
}
TESTNET_MANIFEST_STATUSES = {"limited_testnet", "promoted_testnet"}
SHADOW_MANIFEST_STATUSES = {"shadow_only", "candidate_shadow", "limited_testnet", "promoted_testnet", "mainnet_blocked"}
OLD_STRATEGY_TWEAK_TYPES = {
    "threshold_tweak",
    "score_tweak",
    "parameter_tweak",
    "filter_only",
    "rename_only",
    "gate_only",
    "quarantine_only",
}

REQUIRED_MANIFEST_FIELDS = (
    "strategy_id",
    "version",
    "family",
    "status",
    "market_hypothesis",
    "edge_source",
    "who_loses_money",
    "trigger_event",
    "entry_conditions",
    "invalidation_conditions",
    "exit_logic",
    "allowed_regimes",
    "blocked_regimes",
    "allowed_liquidity_buckets",
    "blocked_symbols",
    "expected_holding_horizon_sec",
    "evidence_level",
    "shadow_evidence",
    "replay_evidence",
    "testnet_evidence",
    "old_strategy_similarity",
    "created_at",
    "updated_at",
)
TESTNET_REQUIRED_CANDIDATE_FIELDS = (
    "strategy_manifest_id",
    "strategy_family",
    "evidence_level",
    "experiment_id",
    "expected_net_edge_bps",
    "expected_total_cost_bps",
    "expected_gross_move_bps",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_old_strategy_freeze_config() -> dict[str, Any]:
    return {
        "version": "old_strategy_freeze_v1",
        "enabled": True,
        "default_status": "shadow_only",
        "require_explicit_allowlist": True,
        "frozen_strategy_ids": sorted(OLD_STRATEGY_IDS),
        "allowlist": [],
        "report_outputs": {
            "gate_report": "old_strategy_freeze_gate_report.json",
            "blocked_candidate_report": "blocked_candidates.jsonl",
        },
    }


def load_old_strategy_freeze_config(path: Path | None = None) -> dict[str, Any]:
    if path is None or not path.exists():
        return default_old_strategy_freeze_config()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default_old_strategy_freeze_config()
    if not isinstance(payload, dict):
        return default_old_strategy_freeze_config()
    merged = default_old_strategy_freeze_config()
    merged.update(payload)
    return merged


def write_default_old_strategy_freeze_config(path: Path) -> dict[str, Any]:
    payload = default_old_strategy_freeze_config()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _candidate_value(candidate: Any, key: str) -> Any:
    if isinstance(candidate, dict):
        return candidate.get(key)
    return getattr(candidate, key, None)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def _candidate_to_dict(candidate: Any) -> dict[str, Any]:
    if isinstance(candidate, dict):
        return dict(candidate)
    keys = (
        "symbol",
        "setup",
        "strategy_id",
        "strategy_manifest_id",
        "manifest_id",
        "strategy_family",
        "evidence_level",
        "experiment_id",
        "side",
        "score",
        "quote_volume_24h",
        "avg_quote_turnover_1m",
        "current_quote_turnover_1m",
        "liquidity_bucket",
        "expected_gross_move_bps",
        "expected_fee_bps",
        "expected_spread_bps",
        "expected_slippage_bps",
        "expected_funding_bps",
        "expected_total_cost_bps",
        "expected_net_edge_bps",
    )
    return {key: getattr(candidate, key) for key in keys if hasattr(candidate, key)}


def _manifest_exists(manifest_dir: Path, strategy_id: str) -> bool:
    if not strategy_id:
        return False
    return (manifest_dir / f"{strategy_id}.json").exists()


def _candidate_source_label(candidate: Any, key: str) -> str:
    return f"payload.{key}" if isinstance(candidate, dict) else f"candidate.{key}"


def _strategy_manifest_resolution(
    candidate: Any,
    experiment: dict[str, Any] | None,
    *,
    manifest_dir: Path,
    target_stage: str,
) -> dict[str, str | None]:
    experiment = _as_dict(experiment)
    for key in ("strategy_manifest_id", "manifest_id"):
        explicit = _clean_text(_candidate_value(candidate, key))
        if explicit:
            return {"strategy_manifest_id": explicit, "manifest_id_source": _candidate_source_label(candidate, key)}
    candidate_strategy_id = _clean_text(_candidate_value(candidate, "strategy_id"))
    if candidate_strategy_id:
        return {
            "strategy_manifest_id": candidate_strategy_id,
            "manifest_id_source": _candidate_source_label(candidate, "strategy_id"),
        }
    setup = _clean_text(_candidate_value(candidate, "setup"))
    if setup and _manifest_exists(manifest_dir, setup):
        return {"strategy_manifest_id": setup, "manifest_id_source": "candidate.setup_manifest_mapping"}
    for key in ("strategy_manifest_id", "manifest_id"):
        explicit = _clean_text(experiment.get(key))
        if explicit:
            return {"strategy_manifest_id": explicit, "manifest_id_source": f"experiment.{key}"}
    fallback = _clean_text(experiment.get("strategy_id"))
    if fallback:
        return {"strategy_manifest_id": fallback, "manifest_id_source": "experiment.strategy_id"}
    for key in ("setup",):
        fallback = _clean_text(experiment.get(key))
        if fallback:
            return {"strategy_manifest_id": fallback, "manifest_id_source": f"experiment.{key}"}
    if target_stage == "shadow" and setup:
        return {"strategy_manifest_id": setup, "manifest_id_source": "shadow_candidate.setup"}
    return {"strategy_manifest_id": "", "manifest_id_source": None}


def _strategy_manifest_id(
    candidate: Any,
    experiment: dict[str, Any] | None,
    *,
    manifest_dir: Path,
    target_stage: str,
) -> str:
    return str(
        _strategy_manifest_resolution(
            candidate,
            experiment,
            manifest_dir=manifest_dir,
            target_stage=target_stage,
        ).get("strategy_manifest_id")
        or ""
    )


def _candidate_strategy_id(candidate: Any, experiment: dict[str, Any] | None) -> str:
    experiment = _as_dict(experiment)
    return str(
        _candidate_value(candidate, "strategy_id")
        or experiment.get("strategy_id")
        or experiment.get("strategy_manifest_id")
        or _candidate_value(candidate, "setup")
        or ""
    ).strip()


def _freeze_allowlisted(strategy_id: str, experiment: dict[str, Any] | None, config: dict[str, Any]) -> tuple[bool, dict[str, Any] | None]:
    experiment = _as_dict(experiment)
    experiment_id = str(experiment.get("experiment_id") or "").strip()
    for row in config.get("allowlist") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("strategy_id") or "") != strategy_id:
            continue
        if str(row.get("experiment_id") or "") != experiment_id:
            continue
        if not str(row.get("reason") or "").strip():
            continue
        return True, row
    return False, None


def load_strategy_manifest(manifest_dir: Path, strategy_id: str) -> dict[str, Any] | None:
    path = manifest_dir / f"{strategy_id}.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def load_strategy_manifests(manifest_dir: Path) -> dict[str, dict[str, Any]]:
    manifests: dict[str, dict[str, Any]] = {}
    if not manifest_dir.exists():
        return manifests
    for path in sorted(manifest_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        strategy_id = str(payload.get("strategy_id") or path.stem).strip()
        if strategy_id:
            manifests[strategy_id] = payload
    return manifests


def validate_strategy_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    strategy_id = str(payload.get("strategy_id") or "").strip()
    for field in REQUIRED_MANIFEST_FIELDS:
        if field not in payload:
            errors.append(f"missing_{field}")
    if not strategy_id:
        errors.append("missing_strategy_id")
    status = str(payload.get("status") or "")
    if status not in VALID_MANIFEST_STATUSES:
        errors.append("invalid_status")
    for list_field in (
        "entry_conditions",
        "invalidation_conditions",
        "exit_logic",
        "allowed_regimes",
        "blocked_regimes",
        "allowed_liquidity_buckets",
        "blocked_symbols",
        "expected_holding_horizon_sec",
    ):
        if list_field in payload and not isinstance(payload.get(list_field), list):
            errors.append(f"{list_field}_must_be_list")
    for dict_field in ("shadow_evidence", "replay_evidence", "testnet_evidence", "old_strategy_similarity"):
        if dict_field in payload and not isinstance(payload.get(dict_field), dict):
            errors.append(f"{dict_field}_must_be_object")
    hypothesis = str(payload.get("market_hypothesis") or "").strip()
    edge_source = str(payload.get("edge_source") or "").strip()
    who_loses = str(payload.get("who_loses_money") or "").strip()
    if not hypothesis:
        errors.append("missing_market_hypothesis")
    if not edge_source:
        errors.append("missing_edge_source")
    if not who_loses:
        errors.append("missing_who_loses_money")
    try:
        evidence_level = int(payload.get("evidence_level"))
    except (TypeError, ValueError):
        evidence_level = -1
        errors.append("invalid_evidence_level")
    if evidence_level < 0 or evidence_level > 5:
        errors.append("evidence_level_out_of_range")
    similarity = _as_dict(payload.get("old_strategy_similarity"))
    if "is_similar" not in similarity:
        errors.append("old_strategy_similarity_missing_is_similar")
    if bool(similarity.get("is_similar")) and str(similarity.get("change_type") or "") in OLD_STRATEGY_TWEAK_TYPES:
        errors.append("old_strategy_similarity_not_new_strategy")
    if strategy_id in OLD_STRATEGY_IDS and status in TESTNET_MANIFEST_STATUSES:
        warnings.append("old_strategy_manifest_status_will_be_frozen")
    if status in TESTNET_MANIFEST_STATUSES:
        if payload.get("expected_gross_move_bps") is None:
            errors.append("missing_expected_gross_move_bps_for_testnet")
        if payload.get("max_expected_total_cost_bps") is None and payload.get("expected_total_cost_bps") is None:
            errors.append("missing_expected_total_cost_bps_for_testnet")
    return {
        "strategy_id": strategy_id,
        "valid": not errors,
        "errors": list(dict.fromkeys(errors)),
        "warnings": list(dict.fromkeys(warnings)),
    }


def build_strategy_gate_decision(
    candidate: Any,
    *,
    experiment: dict[str, Any] | None = None,
    manifest_dir: Path = Path("strategy_manifests"),
    old_strategy_freeze_path: Path | None = Path("old_strategy_freeze.json"),
    target_stage: str = "testnet",
    cost_model: dict[str, Any] | None = None,
    parity_report: dict[str, Any] | None = None,
    evidence_thresholds: dict[str, Any] | None = None,
    cost_thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from phoenix_cost_gate import evaluate_cost_gate
    from phoenix_evidence_gate import evaluate_evidence_gate

    experiment = _as_dict(experiment)
    candidate_dict = _candidate_to_dict(candidate)
    strategy_id = _candidate_strategy_id(candidate, experiment)
    setup = str(_candidate_value(candidate, "setup") or strategy_id).strip()
    manifest_resolution = _strategy_manifest_resolution(
        candidate,
        experiment,
        manifest_dir=manifest_dir,
        target_stage=target_stage,
    )
    manifest_id = str(manifest_resolution.get("strategy_manifest_id") or "")
    manifest_id_source = manifest_resolution.get("manifest_id_source")
    freeze_config = load_old_strategy_freeze_config(old_strategy_freeze_path)
    frozen_ids = {str(item) for item in (freeze_config.get("frozen_strategy_ids") or sorted(OLD_STRATEGY_IDS))}
    block_reasons: list[str] = []
    reports: dict[str, Any] = {}
    manifest: dict[str, Any] | None = None

    if target_stage == "mainnet":
        block_reasons.append("blocked_mainnet_live_manual_approval_required")

    freeze_applies = bool(freeze_config.get("enabled", True)) and (setup in frozen_ids or strategy_id in frozen_ids or manifest_id in frozen_ids)
    if target_stage == "testnet" and freeze_applies:
        allowed, allow_row = _freeze_allowlisted(strategy_id if strategy_id in frozen_ids else setup, experiment, freeze_config)
        if not allowed:
            block_reasons.append("blocked_by_old_strategy_freeze")
        else:
            reports["old_strategy_freeze_allowlist"] = allow_row

    if target_stage == "testnet" and not manifest_id:
        block_reasons.append("blocked_missing_strategy_manifest_id")
    if target_stage == "testnet":
        candidate_required_values = {
            "strategy_manifest_id": manifest_id,
            "strategy_family": candidate_dict.get("strategy_family"),
            "evidence_level": candidate_dict.get("evidence_level"),
            "experiment_id": candidate_dict.get("experiment_id"),
            "expected_net_edge_bps": candidate_dict.get("expected_net_edge_bps"),
            "expected_total_cost_bps": candidate_dict.get("expected_total_cost_bps"),
            "expected_gross_move_bps": candidate_dict.get("expected_gross_move_bps"),
        }
        missing_candidate_fields = [
            field for field in TESTNET_REQUIRED_CANDIDATE_FIELDS if not _present(candidate_required_values.get(field))
        ]
        if missing_candidate_fields:
            reports["missing_candidate_fields"] = missing_candidate_fields
            block_reasons.extend(f"blocked_missing_candidate_{field}" for field in missing_candidate_fields)
    if manifest_id:
        manifest = load_strategy_manifest(manifest_dir, manifest_id)
        if manifest is None:
            block_reasons.append("blocked_missing_strategy_manifest")
    else:
        manifest = None

    if manifest is not None:
        validation = validate_strategy_manifest(manifest)
        reports["manifest_validation"] = validation
        if not validation["valid"]:
            block_reasons.append("blocked_invalid_strategy_manifest")
        status = str(manifest.get("status") or "")
        if target_stage == "shadow":
            if status not in SHADOW_MANIFEST_STATUSES:
                block_reasons.append("blocked_manifest_status_not_shadow")
        elif target_stage == "testnet":
            if status not in TESTNET_MANIFEST_STATUSES:
                block_reasons.append("blocked_manifest_status_not_testnet")
        evidence = evaluate_evidence_gate(
            manifest,
            target_stage=target_stage,
            parity_report=parity_report,
            thresholds=evidence_thresholds,
        )
        reports["evidence_gate"] = evidence
        block_reasons.extend(evidence.get("block_reasons") or [])
        if target_stage == "testnet":
            cost = evaluate_cost_gate(
                candidate=candidate_dict,
                manifest=manifest,
                experiment=experiment,
                cost_model=cost_model,
                thresholds=cost_thresholds,
            )
            reports["cost_gate"] = cost
            block_reasons.extend(cost.get("block_reasons") or [])

    unique_reasons = list(dict.fromkeys(block_reasons))
    return {
        "report_type": "strategy_candidate_gate_decision",
        "version": "strategy_gate_v1",
        "generated_at": utc_now_iso(),
        "target_stage": target_stage,
        "decision": "block" if unique_reasons else "allow",
        "block_reasons": unique_reasons,
        "strategy_id": strategy_id,
        "strategy_manifest_id": manifest_id or None,
        "resolved_manifest_id": manifest_id or None,
        "manifest_id_source": manifest_id_source,
        "strategy_family": candidate_dict.get("strategy_family") or (manifest.get("family") if isinstance(manifest, dict) else None),
        "setup": setup,
        "symbol": _candidate_value(candidate, "symbol"),
        "experiment_id": candidate_dict.get("experiment_id") or experiment.get("experiment_id"),
        "manifest_status": manifest.get("status") if isinstance(manifest, dict) else None,
        "evidence_level": manifest.get("evidence_level") if isinstance(manifest, dict) else None,
        "candidate_evidence_level": candidate_dict.get("evidence_level"),
        "expected_gross_move_bps": candidate_dict.get("expected_gross_move_bps"),
        "expected_total_cost_bps": candidate_dict.get("expected_total_cost_bps"),
        "expected_net_edge_bps": candidate_dict.get("expected_net_edge_bps"),
        "reports": reports,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "mainnet_live_promotion_allowed": False,
    }


def shadow_safe_defaults() -> dict[str, Any]:
    payload = dict(DEFAULT_STRATEGY_REGISTRY)
    payload["generated_at"] = utc_now_iso()
    payload["strategies"] = {}
    payload["experiments"] = {}
    return payload


def register_strategy(
    registry: dict[str, Any] | None,
    *,
    strategy_id: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not strategy_id or not strategy_id.strip():
        raise ValueError("strategy_id is required")
    next_registry = shadow_safe_defaults()
    if registry:
        next_registry.update(registry)
    next_registry["live_trading_enabled"] = False
    next_registry["promotion_allowed"] = False
    next_registry["direct_config_change_allowed"] = False
    strategies = next_registry.get("strategies") if isinstance(next_registry.get("strategies"), dict) else {}
    existing = strategies.get(strategy_id.strip()) if isinstance(strategies.get(strategy_id.strip()), dict) else {}
    item = dict(existing)
    item.update(metadata or {})
    item["strategy_id"] = strategy_id.strip()
    item["mode"] = "shadow"
    item["live_trading_enabled"] = False
    item["promotion_allowed"] = False
    item["direct_config_change_allowed"] = False
    item["registered_at"] = item.get("registered_at") or utc_now_iso()
    item["versions"] = existing.get("versions", {}) if isinstance(existing.get("versions"), dict) else item.get("versions", {})
    strategies[strategy_id.strip()] = item
    next_registry["strategies"] = strategies
    if not isinstance(next_registry.get("experiments"), dict):
        next_registry["experiments"] = {}
    return next_registry


def read_strategy_registry(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return shadow_safe_defaults()
    return payload if isinstance(payload, dict) else shadow_safe_defaults()


def write_strategy_registry(path: Path, registry: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe = dict(registry)
    safe["live_trading_enabled"] = False
    safe["promotion_allowed"] = False
    safe["direct_config_change_allowed"] = False
    path.write_text(json.dumps(safe, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def register_experiment_candidate(
    registry: dict[str, Any] | None,
    experiment: dict[str, Any],
) -> dict[str, Any]:
    strategy_id = str(experiment.get("strategy_id") or experiment.get("candidate_version") or "").strip()
    if not strategy_id:
        raise ValueError("experiment.strategy_id is required")
    metadata = {
        "strategy_id": strategy_id,
        "version": experiment.get("version"),
        "parent_version": experiment.get("parent_version"),
        "status": experiment.get("status") or "running_experiment",
        "mode": experiment.get("mode") or "testnet_candidate",
        "created_from_proposal": experiment.get("created_from_proposal"),
        "validation_requirements": experiment.get("validation_requirements") or {},
        "rollback_target": experiment.get("rollback_target"),
        "experiment_id": experiment.get("experiment_id"),
        "experiment_type": experiment.get("experiment_type"),
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "direct_config_change_allowed": False,
        "registered_at": utc_now_iso(),
    }
    next_registry = register_strategy(registry, strategy_id=strategy_id, metadata=metadata)
    strategies = next_registry.get("strategies") if isinstance(next_registry.get("strategies"), dict) else {}
    strategy_item = strategies.get(strategy_id) if isinstance(strategies.get(strategy_id), dict) else metadata
    versions = strategy_item.get("versions") if isinstance(strategy_item.get("versions"), dict) else {}
    version_key = str(experiment.get("version") or experiment.get("candidate_version") or "unknown")
    versions[version_key] = metadata
    strategy_item["versions"] = versions
    strategy_item["latest_candidate_version"] = version_key
    strategies[strategy_id] = strategy_item
    experiments = next_registry.get("experiments") if isinstance(next_registry.get("experiments"), dict) else {}
    experiment_id = str(experiment.get("experiment_id") or "")
    if experiment_id:
        experiments[experiment_id] = metadata
    next_registry["strategies"] = strategies
    next_registry["experiments"] = experiments
    next_registry["live_trading_enabled"] = False
    next_registry["promotion_allowed"] = False
    next_registry["direct_config_change_allowed"] = False
    return next_registry


def build_manifest_validation_report(manifest_dir: Path) -> dict[str, Any]:
    manifests = load_strategy_manifests(manifest_dir)
    validations = [validate_strategy_manifest(payload) for payload in manifests.values()]
    return {
        "report_type": "strategy_manifest_validation_report",
        "version": STRATEGY_REGISTRY_VERSION,
        "generated_at": utc_now_iso(),
        "manifest_dir": str(manifest_dir),
        "manifest_count": len(manifests),
        "valid_count": sum(1 for item in validations if item.get("valid")),
        "invalid_count": sum(1 for item in validations if not item.get("valid")),
        "validations": validations,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "mainnet_live_promotion_allowed": False,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phoenix strategy manifest registry.")
    sub = parser.add_subparsers(dest="command", required=True)
    validate = sub.add_parser("validate")
    validate.add_argument("--manifest-dir", type=Path, default=Path("strategy_manifests"))
    validate.add_argument("--output-json", type=Path, default=None)
    listing = sub.add_parser("list")
    listing.add_argument("--manifest-dir", type=Path, default=Path("strategy_manifests"))
    explain = sub.add_parser("explain")
    explain.add_argument("strategy_id")
    explain.add_argument("--manifest-dir", type=Path, default=Path("strategy_manifests"))
    freeze = sub.add_parser("write-freeze-config")
    freeze.add_argument("--output", type=Path, default=Path("old_strategy_freeze.json"))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "validate":
        report = build_manifest_validation_report(args.manifest_dir)
        if args.output_json is not None:
            args.output_json.parent.mkdir(parents=True, exist_ok=True)
            args.output_json.write_text(
                json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        print(
            f"strategy_manifest validate count={report['manifest_count']} "
            f"valid={report['valid_count']} invalid={report['invalid_count']}"
        )
        return 0 if report["invalid_count"] == 0 else 2
    if args.command == "list":
        manifests = load_strategy_manifests(args.manifest_dir)
        for strategy_id, payload in sorted(manifests.items()):
            print(
                f"{strategy_id}\tstatus={payload.get('status')}\t"
                f"evidence_level={payload.get('evidence_level')}\tfamily={payload.get('family')}"
            )
        return 0
    if args.command == "explain":
        manifest = load_strategy_manifest(args.manifest_dir, args.strategy_id)
        if manifest is None:
            print(f"strategy_manifest not_found strategy_id={args.strategy_id}")
            return 2
        validation = validate_strategy_manifest(manifest)
        print(json.dumps({"manifest": manifest, "validation": validation}, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if validation["valid"] else 2
    if args.command == "write-freeze-config":
        write_default_old_strategy_freeze_config(args.output)
        print(f"old_strategy_freeze written {args.output}")
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
