from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EXPERIMENT_TRACKING_VERSION = "experiment_tracking_v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def current_git_sha(root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    sha = result.stdout.strip()
    return sha or None


def build_experiment_manifest(
    *,
    experiment_id: str,
    strategy_manifest_id: str,
    strategy_family: str,
    evidence_level: int,
    cost_model_version: str,
    regime_model_version: str,
    data_snapshot_id: str,
    config: dict[str, Any] | None = None,
    code_git_sha: str | None = None,
) -> dict[str, Any]:
    if not experiment_id:
        raise ValueError("experiment_id is required")
    if not strategy_manifest_id:
        raise ValueError("strategy_manifest_id is required")
    config = dict(config or {})
    manifest = {
        "report_type": "experiment_manifest",
        "version": EXPERIMENT_TRACKING_VERSION,
        "generated_at": utc_now_iso(),
        "experiment_id": experiment_id,
        "strategy_manifest_id": strategy_manifest_id,
        "strategy_family": strategy_family,
        "evidence_level": int(evidence_level),
        "cost_model_version": cost_model_version,
        "regime_model_version": regime_model_version,
        "data_snapshot_id": data_snapshot_id,
        "code_git_sha": code_git_sha,
        "config_hash": stable_hash(config),
        "config": config,
        "live_trading_enabled": False,
        "promotion_allowed": False,
        "mainnet_live_promotion_allowed": False,
    }
    return manifest


def write_experiment_scaffold(
    *,
    experiments_root: Path,
    manifest: dict[str, Any],
    shadow_results: dict[str, Any] | None = None,
    testnet_results: dict[str, Any] | None = None,
    cost_report: dict[str, Any] | None = None,
    gate_report: dict[str, Any] | None = None,
) -> dict[str, str]:
    experiment_id = str(manifest.get("experiment_id") or "")
    if not experiment_id:
        raise ValueError("manifest.experiment_id is required")
    experiment_dir = experiments_root / experiment_id
    experiment_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "manifest": experiment_dir / "manifest.json",
        "shadow_results": experiment_dir / "shadow_results.json",
        "testnet_results": experiment_dir / "testnet_results.json",
        "cost_report": experiment_dir / "cost_report.json",
        "gate_report": experiment_dir / "gate_report.json",
        "summary": experiment_dir / "summary.md",
    }
    payloads = {
        "manifest": manifest,
        "shadow_results": shadow_results or {"status": "not_collected", "live_trading_enabled": False},
        "testnet_results": testnet_results or {"status": "not_collected", "live_trading_enabled": False},
        "cost_report": cost_report or {"status": "not_collected", "live_trading_enabled": False},
        "gate_report": gate_report or {"status": "not_collected", "live_trading_enabled": False},
    }
    for key, payload in payloads.items():
        outputs[key].write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary = [
        "# Phoenix Experiment Summary",
        "",
        f"- experiment_id: {experiment_id}",
        f"- strategy_manifest_id: {manifest.get('strategy_manifest_id')}",
        f"- strategy_family: {manifest.get('strategy_family')}",
        f"- evidence_level: {manifest.get('evidence_level')}",
        f"- cost_model_version: {manifest.get('cost_model_version')}",
        f"- regime_model_version: {manifest.get('regime_model_version')}",
        f"- data_snapshot_id: {manifest.get('data_snapshot_id')}",
        "- mainnet_live: false",
        "",
    ]
    outputs["summary"].write_text("\n".join(summary), encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}

