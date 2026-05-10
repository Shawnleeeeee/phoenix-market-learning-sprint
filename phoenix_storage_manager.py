from __future__ import annotations

import argparse
import fnmatch
import gzip
import hashlib
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


STORAGE_MANAGER_VERSION = "storage_manager_v1"
DEFAULT_CLEANUP_APPROVAL_FILE = "SHAWN_APPROVED_CLEANUP.txt"
DEFAULT_MAX_PLAN_AGE_HOURS = 24.0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_iso_timestamp(value: Any) -> float | None:
    if not value:
        return None
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text).timestamp()
    except (TypeError, ValueError):
        return None


def default_retention_policy() -> dict[str, Any]:
    return {
        "version": STORAGE_MANAGER_VERSION,
        "real_cleanup_apply_enabled_by_default": False,
        "keep_recent_days": 30,
        "compress_after_days": 30,
        "archive_after_days": 60,
        "delete_tmp_after_days": 7,
        "never_delete_patterns": [
            ".env",
            ".env.*",
            "**/*.env",
            "**/*secret*",
            "**/*credential*",
            "strategy_manifests/*.json",
            "strategy_manifest_schema.json",
            "experiment_manifest.json",
            "experiments/*/manifest.json",
            "strategy_experiments/*.json",
            "**/round_*_trades.jsonl",
            "**/signal_bridge_shadow_outcomes.jsonl",
            "**/promotion_gate*.json",
            "**/evidence_gate*.json",
            "**/cost_gate*.json",
            "old_strategy_freeze.json",
            "storage_retention_policy.json",
        ],
        "safe_delete_patterns": [
            ".tmp/**",
            "**/*.tmp",
            "**/__pycache__/**",
            "**/.pytest_cache/**",
            "**/.ruff_cache/**",
            "**/*.pyc",
            "**/empty_*",
        ],
        "compress_patterns": [
            "**/*.jsonl",
            "**/*.log",
            "**/*.csv",
        ],
        "archive_patterns": [
            "round_runner_reports*/**",
            "signal_lab_replay/**",
            "phoenix_reports/**",
            "audit_artifacts/**",
        ],
        "max_report_versions": 10,
        "min_free_disk_gb": 5,
        "alert_disk_usage_pct": 85,
        "red_disk_usage_pct": 90,
        "emergency_disk_usage_pct": 95,
    }


def load_retention_policy(path: Path | None = None) -> dict[str, Any]:
    if path is None or not path.exists():
        return default_retention_policy()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default_retention_policy()
    if not isinstance(payload, dict):
        return default_retention_policy()
    merged = default_retention_policy()
    merged.update(payload)
    return merged


def _rel(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _matches(path: Path, root: Path, patterns: Iterable[str]) -> bool:
    rel = _rel(path, root)
    name = path.name
    return any(fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(name, pattern) for pattern in patterns)


def _file_age_days(path: Path, now: float | None = None) -> float:
    now = time.time() if now is None else now
    try:
        return max(0.0, (now - path.stat().st_mtime) / 86400.0)
    except OSError:
        return 0.0


def _safe_stat_size(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except OSError:
        return 0


def _iter_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    return (path for path in root.rglob("*") if path.is_file())


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for file_path in path.rglob("*"):
        if file_path.is_file():
            total += _safe_stat_size(file_path)
    return total


def _category_size(root: Path, patterns: Iterable[str]) -> int:
    return sum(_safe_stat_size(path) for path in _iter_files(root) if _matches(path, root, patterns))


def _top_files(root: Path, limit: int) -> list[dict[str, Any]]:
    rows = [{"path": str(path), "size_bytes": _safe_stat_size(path)} for path in _iter_files(root)]
    return sorted(rows, key=lambda row: int(row["size_bytes"]), reverse=True)[:limit]


def _top_files_from_rows(file_rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return sorted(
        [{"path": str(row["path"]), "size_bytes": int(row["size_bytes"])} for row in file_rows],
        key=lambda row: int(row["size_bytes"]),
        reverse=True,
    )[:limit]


def _top_dirs(root: Path, limit: int) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    sizes: dict[Path, int] = {root: 0}
    for file_path in _iter_files(root):
        size = _safe_stat_size(file_path)
        current = file_path.parent
        while True:
            sizes[current] = sizes.get(current, 0) + size
            if current == root:
                break
            if root not in current.parents:
                break
            current = current.parent
    rows = [{"path": str(path), "size_bytes": size} for path, size in sizes.items()]
    return sorted(rows, key=lambda row: int(row["size_bytes"]), reverse=True)[:limit]


def _top_dirs_from_rows(root: Path, file_rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    sizes: dict[Path, int] = {root: 0}
    for row in file_rows:
        file_path = Path(str(row["path"]))
        size = int(row["size_bytes"])
        current = file_path.parent
        while True:
            sizes[current] = sizes.get(current, 0) + size
            if current == root:
                break
            if root not in current.parents:
                break
            current = current.parent
    rows = [{"path": str(path), "size_bytes": size} for path, size in sizes.items()]
    return sorted(rows, key=lambda item: int(item["size_bytes"]), reverse=True)[:limit]


def _recent_growth(root: Path, days: int = 7, limit: int = 20) -> list[dict[str, Any]]:
    now = time.time()
    grouped: dict[str, dict[str, Any]] = {}
    for path in _iter_files(root):
        if _file_age_days(path, now) > days:
            continue
        rel_parts = Path(_rel(path, root)).parts
        top = rel_parts[0] if rel_parts else "."
        row = grouped.setdefault(top, {"path": str(root / top), "size_bytes": 0, "file_count": 0})
        row["size_bytes"] += _safe_stat_size(path)
        row["file_count"] += 1
    return sorted(grouped.values(), key=lambda row: int(row["size_bytes"]), reverse=True)[:limit]


def _recent_growth_from_rows(root: Path, file_rows: list[dict[str, Any]], days: int = 7, limit: int = 20) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in file_rows:
        if float(row["age_days"]) > days:
            continue
        rel_parts = Path(str(row["relative_path"])).parts
        top = rel_parts[0] if rel_parts else "."
        grouped_row = grouped.setdefault(top, {"path": str(root / top), "size_bytes": 0, "file_count": 0})
        grouped_row["size_bytes"] += int(row["size_bytes"])
        grouped_row["file_count"] += 1
    return sorted(grouped.values(), key=lambda item: int(item["size_bytes"]), reverse=True)[:limit]


def _disk_usage(root: Path) -> dict[str, Any]:
    usage = shutil.disk_usage(root if root.exists() else root.anchor or ".")
    used_pct = round((usage.used / usage.total) * 100.0, 4) if usage.total else 0.0
    return {
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "used_pct": used_pct,
        "free_gb": round(usage.free / 1024 / 1024 / 1024, 4),
    }


def build_storage_inventory(root: Path) -> dict[str, Any]:
    root = root.resolve()
    now = time.time()
    file_rows = [
        {
            "path": str(path),
            "relative_path": _rel(path, root),
            "size_bytes": _safe_stat_size(path),
            "age_days": _file_age_days(path, now),
        }
        for path in _iter_files(root)
    ]
    def category(patterns: Iterable[str]) -> int:
        return sum(
            int(row["size_bytes"])
            for row in file_rows
            if _matches(Path(str(row["path"])), root, patterns)
        )

    category_sizes = {
        "collector_bytes": category(["signal_lab_runs/event_collect*/**", "**/event_*", "**/collector*.log"]),
        "shadow_bytes": category(["signal_lab_runs/*shadow*/**", "**/signal_bridge_shadow_*.jsonl", "**/mainnet_shadow/**"]),
        "testnet_bytes": category(["round_runner_reports*/**", "**/round_*_trades.jsonl"]),
        "replay_backtest_bytes": category(["signal_lab_replay/**", "**/*backtest*/**", "**/*replay*/**"]),
        "reports_bytes": category(["phoenix_reports/**", "**/*report*.json", "**/*report*.md"]),
        "logs_bytes": category(["logs/**", "**/*.log"]),
        "cache_tmp_bytes": category([".tmp/**", "**/__pycache__/**", "**/.pytest_cache/**", "**/*.pyc"]),
    }
    return {
        "report_type": "storage_inventory",
        "version": STORAGE_MANAGER_VERSION,
        "generated_at": utc_now_iso(),
        "project_root": str(root),
        "disk": _disk_usage(root),
        "project_size_bytes": sum(int(row["size_bytes"]) for row in file_rows),
        "category_sizes": category_sizes,
        "top_dirs": _top_dirs_from_rows(root, file_rows, 20),
        "top_files": _top_files_from_rows(file_rows, 50),
        "recent_7d_growth": _recent_growth_from_rows(root, file_rows, days=7, limit=20),
        "live_trading_enabled": False,
        "real_delete_executed": False,
    }


def _risk_level(disk: dict[str, Any], policy: dict[str, Any]) -> str:
    pct = float(disk.get("used_pct") or 0.0)
    if pct >= float(policy.get("emergency_disk_usage_pct", 95)):
        return "red"
    if pct >= float(policy.get("red_disk_usage_pct", 90)):
        return "red"
    if pct >= float(policy.get("alert_disk_usage_pct", 85)):
        return "yellow"
    if float(disk.get("free_gb") or 0.0) < float(policy.get("min_free_disk_gb", 5)):
        return "yellow"
    return "green"


def build_cleanup_plan(root: Path, *, policy: dict[str, Any] | None = None, now: float | None = None) -> dict[str, Any]:
    root = root.resolve()
    policy = default_retention_policy() if policy is None else {**default_retention_policy(), **policy}
    now = time.time() if now is None else now
    keep_recent_days = int(policy.get("keep_recent_days", 30))
    compress_after_days = int(policy.get("compress_after_days", 30))
    archive_after_days = int(policy.get("archive_after_days", 60))
    delete_tmp_after_days = int(policy.get("delete_tmp_after_days", 7))
    never_patterns = list(policy.get("never_delete_patterns") or [])
    safe_delete_patterns = list(policy.get("safe_delete_patterns") or [])
    compress_patterns = list(policy.get("compress_patterns") or [])
    archive_patterns = list(policy.get("archive_patterns") or [])
    max_items = int(policy.get("max_plan_items_per_category", 1000))

    compress_candidates: list[dict[str, Any]] = []
    archive_candidates: list[dict[str, Any]] = []
    delete_candidates: list[dict[str, Any]] = []
    never_delete: list[dict[str, Any]] = []
    file_rows: list[dict[str, Any]] = []
    summary_counts = {
        "compress_candidates": 0,
        "archive_candidates": 0,
        "delete_candidates": 0,
        "never_delete": 0,
    }
    summary_bytes = {
        "compress_candidates": 0,
        "archive_candidates": 0,
        "delete_candidates": 0,
        "never_delete": 0,
    }
    protected_recent_count = 0
    protected_recent_bytes = 0
    protected_recent_sample: list[dict[str, Any]] = []

    def add_capped(bucket: list[dict[str, Any]], name: str, row: dict[str, Any]) -> None:
        summary_counts[name] += 1
        summary_bytes[name] += int(row["size_bytes"])
        if len(bucket) < max_items:
            bucket.append(row)

    for path in _iter_files(root):
        rel = _rel(path, root)
        age = _file_age_days(path, now)
        size = _safe_stat_size(path)
        row = {"path": str(path), "relative_path": rel, "size_bytes": size, "age_days": round(age, 4)}
        file_rows.append(row)
        if _matches(path, root, never_patterns):
            add_capped(never_delete, "never_delete", {**row, "reason": "never_delete_pattern"})
            continue
        if age < keep_recent_days:
            protected_recent_count += 1
            protected_recent_bytes += size
            if len(protected_recent_sample) < 100:
                protected_recent_sample.append({**row, "reason": "recent_data_protected"})
            continue
        if age >= compress_after_days and _matches(path, root, compress_patterns):
            add_capped(compress_candidates, "compress_candidates", {**row, "action": "compress"})
        if age >= archive_after_days and _matches(path, root, archive_patterns):
            add_capped(archive_candidates, "archive_candidates", {**row, "action": "archive"})
        if age >= delete_tmp_after_days and _matches(path, root, safe_delete_patterns):
            add_capped(delete_candidates, "delete_candidates", {**row, "action": "delete"})

    disk = _disk_usage(root)
    estimated_release = summary_bytes["delete_candidates"]
    estimated_release += max(0, summary_bytes["compress_candidates"] // 2)
    return {
        "report_type": "storage_cleanup_plan",
        "version": STORAGE_MANAGER_VERSION,
        "generated_at": utc_now_iso(),
        "project_root": str(root),
        "policy": policy,
        "disk": disk,
        "top_dirs": _top_dirs_from_rows(root, file_rows, 20),
        "top_files": _top_files_from_rows(file_rows, 50),
        "compress_candidates": compress_candidates,
        "archive_candidates": archive_candidates,
        "delete_candidates": delete_candidates,
        "never_delete": never_delete,
        "candidate_counts": summary_counts,
        "candidate_bytes": summary_bytes,
        "candidate_lists_truncated": {
            "compress_candidates": summary_counts["compress_candidates"] > len(compress_candidates),
            "archive_candidates": summary_counts["archive_candidates"] > len(archive_candidates),
            "delete_candidates": summary_counts["delete_candidates"] > len(delete_candidates),
            "never_delete": summary_counts["never_delete"] > len(never_delete),
        },
        "protected_recent_count": protected_recent_count,
        "protected_recent_bytes": protected_recent_bytes,
        "protected_recent_sample": protected_recent_sample,
        "estimated_release_bytes": estimated_release,
        "risk_level": _risk_level(disk, policy),
        "will_affect_collector": False,
        "will_affect_shadow": False,
        "will_affect_testnet": False,
        "will_affect_dashboard": False,
        "rollback_plan": "Compressed files are recorded in archives/archive_manifest.json; restore by decompressing archive_path to original_path. Deleted safe tmp/cache files are intentionally regenerable.",
        "dry_run": True,
        "apply_requires": {
            "approval_file": DEFAULT_CLEANUP_APPROVAL_FILE,
            "env": "PHOENIX_ALLOW_REAL_CLEANUP=true",
            "plan_sha256": "must match the exact dry-run plan file bytes",
            "max_plan_age_hours": DEFAULT_MAX_PLAN_AGE_HOURS,
            "never_delete_patterns": "must be non-empty and checked against delete candidates",
        },
        "real_delete_executed": False,
    }


def write_inventory_outputs(inventory: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "storage_inventory.json").write_text(
        json.dumps(inventory, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Phoenix Storage Inventory",
        "",
        f"- generated_at: {inventory.get('generated_at')}",
        f"- project_root: {inventory.get('project_root')}",
        f"- disk_used_pct: {inventory.get('disk', {}).get('used_pct')}",
        f"- disk_free_gb: {inventory.get('disk', {}).get('free_gb')}",
        f"- project_size_bytes: {inventory.get('project_size_bytes')}",
        "",
        "## Top Directories",
    ]
    for row in inventory.get("top_dirs") or []:
        lines.append(f"- {row['size_bytes']} {row['path']}")
    lines.extend(["", "## Top Files"])
    for row in inventory.get("top_files") or []:
        lines.append(f"- {row['size_bytes']} {row['path']}")
    lines.append("")
    (output_dir / "storage_inventory.md").write_text("\n".join(lines), encoding="utf-8")


def write_cleanup_plan_outputs(plan: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "storage_cleanup_plan.json").write_text(
        json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Phoenix Storage Cleanup Plan",
        "",
        f"- generated_at: {plan.get('generated_at')}",
        f"- project_root: {plan.get('project_root')}",
        f"- risk_level: {plan.get('risk_level')}",
        f"- estimated_release_bytes: {plan.get('estimated_release_bytes')}",
        f"- dry_run: {str(plan.get('dry_run')).lower()}",
        f"- real_delete_executed: {str(plan.get('real_delete_executed')).lower()}",
        "",
        "## Compress",
    ]
    for row in plan.get("compress_candidates") or []:
        lines.append(f"- {row['size_bytes']} {row['path']}")
    lines.extend(["", "## Archive"])
    for row in plan.get("archive_candidates") or []:
        lines.append(f"- {row['size_bytes']} {row['path']}")
    lines.extend(["", "## Delete"])
    for row in plan.get("delete_candidates") or []:
        lines.append(f"- {row['size_bytes']} {row['path']}")
    lines.extend(["", "## Never Delete"])
    for row in plan.get("never_delete") or []:
        lines.append(f"- {row['reason']} {row['path']}")
    lines.append("")
    (output_dir / "storage_cleanup_plan.md").write_text("\n".join(lines), encoding="utf-8")


def validate_cleanup_apply_authorization(
    plan: dict[str, Any],
    *,
    approval_file: Path | None = None,
    env: dict[str, str] | None = None,
    plan_path: Path | None = None,
    plan_sha256: str | None = None,
    now: float | None = None,
    max_plan_age_hours: float = DEFAULT_MAX_PLAN_AGE_HOURS,
) -> dict[str, Any]:
    root = Path(str(plan.get("project_root") or ".")).resolve()
    env = os.environ if env is None else env
    approval_file = approval_file or root / DEFAULT_CLEANUP_APPROVAL_FILE
    now = time.time() if now is None else now
    blockers: list[str] = []

    if env.get("PHOENIX_ALLOW_REAL_CLEANUP") != "true":
        blockers.append("missing_env_PHOENIX_ALLOW_REAL_CLEANUP_true")
    if not approval_file.exists() or not approval_file.is_file():
        blockers.append("missing_SHAWN_APPROVED_CLEANUP_file")
    if plan_path is None:
        blockers.append("missing_plan_file_for_hash_check")
    elif not plan_path.exists() or not plan_path.is_file():
        blockers.append("plan_file_not_found")
    elif not plan_sha256:
        blockers.append("missing_expected_plan_sha256")
    else:
        actual_hash = file_sha256(plan_path)
        if actual_hash.lower() != str(plan_sha256).lower():
            blockers.append("plan_hash_mismatch")

    generated_at_ts = _parse_iso_timestamp(plan.get("generated_at"))
    if generated_at_ts is None:
        blockers.append("plan_generated_at_missing_or_invalid")
    elif now - generated_at_ts > float(max_plan_age_hours) * 3600.0:
        blockers.append("dry_run_plan_expired")

    if not bool(plan.get("dry_run")):
        blockers.append("plan_is_not_marked_dry_run")
    if bool(plan.get("real_delete_executed")):
        blockers.append("plan_already_marked_real_delete_executed")

    policy = plan.get("policy") if isinstance(plan.get("policy"), dict) else {}
    never_patterns = list(policy.get("never_delete_patterns") or [])
    if not never_patterns:
        blockers.append("never_delete_patterns_missing")
    for item in plan.get("delete_candidates") or []:
        if not isinstance(item, dict):
            continue
        source = Path(str(item.get("path") or ""))
        if _matches(source, root, never_patterns):
            blockers.append("never_delete_candidate_in_delete_list")
            break

    return {
        "authorized": not blockers,
        "blockers": list(dict.fromkeys(blockers)),
        "approval_file": str(approval_file),
        "plan_path": str(plan_path) if plan_path is not None else None,
        "max_plan_age_hours": max_plan_age_hours,
    }


def apply_cleanup_plan(
    plan: dict[str, Any],
    *,
    apply: bool = False,
    approval_file: Path | None = None,
    env: dict[str, str] | None = None,
    plan_path: Path | None = None,
    plan_sha256: str | None = None,
    now: float | None = None,
    max_plan_age_hours: float = DEFAULT_MAX_PLAN_AGE_HOURS,
) -> dict[str, Any]:
    if not apply:
        raise ValueError("--apply is required for real cleanup")
    root = Path(str(plan.get("project_root") or ".")).resolve()
    authorization = validate_cleanup_apply_authorization(
        plan,
        approval_file=approval_file,
        env=env,
        plan_path=plan_path,
        plan_sha256=plan_sha256,
        now=now,
        max_plan_age_hours=max_plan_age_hours,
    )
    if not authorization["authorized"]:
        raise PermissionError(f"real cleanup blocked: {', '.join(authorization['blockers'])}")

    archive_root = root / "archives"
    archive_root.mkdir(parents=True, exist_ok=True)
    compressed: list[dict[str, Any]] = []
    deleted: list[dict[str, Any]] = []

    for item in plan.get("compress_candidates") or []:
        source = Path(str(item.get("path") or ""))
        if not source.exists() or not source.is_file():
            continue
        rel = Path(str(item.get("relative_path") or source.name))
        archive_path = archive_root / rel.with_suffix(rel.suffix + ".gz")
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as src, gzip.open(archive_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        if archive_path.exists() and archive_path.stat().st_size > 0:
            compressed.append(
                {
                    "original_path": str(source),
                    "archive_path": str(archive_path),
                    "original_size_bytes": source.stat().st_size,
                    "archive_size_bytes": archive_path.stat().st_size,
                    "compressed_at": utc_now_iso(),
                }
            )

    for item in plan.get("delete_candidates") or []:
        source = Path(str(item.get("path") or ""))
        if not source.exists() or not source.is_file():
            continue
        source.unlink()
        deleted.append({"path": str(source), "deleted_at": utc_now_iso(), "reason": "safe_delete_pattern"})

    manifest = {
        "report_type": "archive_manifest",
        "version": STORAGE_MANAGER_VERSION,
        "generated_at": utc_now_iso(),
        "project_root": str(root),
        "compressed": compressed,
        "deleted": deleted,
        "authorization": authorization,
        "live_trading_enabled": False,
    }
    (archive_root / "archive_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "compressed_count": len(compressed),
        "deleted_count": len(deleted),
        "archive_manifest": str(archive_root / "archive_manifest.json"),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phoenix VPS storage lifecycle manager.")
    sub = parser.add_subparsers(dest="command", required=True)
    inventory = sub.add_parser("inventory")
    inventory.add_argument("--root", type=Path, default=Path("."))
    inventory.add_argument("--output-dir", type=Path, default=Path("."))
    cleanup = sub.add_parser("cleanup")
    cleanup.add_argument("--root", type=Path, default=Path("."))
    cleanup.add_argument("--policy", type=Path, default=Path("storage_retention_policy.json"))
    cleanup.add_argument("--output-dir", type=Path, default=Path("."))
    cleanup.add_argument("--dry-run", action="store_true")
    cleanup.add_argument("--apply", action="store_true")
    cleanup.add_argument("--plan", type=Path, default=None)
    cleanup.add_argument("--approval-file", type=Path, default=None)
    cleanup.add_argument("--plan-sha256", default=None)
    cleanup.add_argument("--max-plan-age-hours", type=float, default=DEFAULT_MAX_PLAN_AGE_HOURS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "inventory":
        inventory = build_storage_inventory(args.root)
        write_inventory_outputs(inventory, args.output_dir)
        print(f"storage_inventory output_dir={args.output_dir}")
        return 0
    if args.command == "cleanup":
        if args.apply:
            if args.plan is None:
                raise SystemExit("--plan is required with --apply")
            plan = json.loads(args.plan.read_text(encoding="utf-8"))
            try:
                result = apply_cleanup_plan(
                    plan,
                    apply=True,
                    approval_file=args.approval_file,
                    plan_path=args.plan,
                    plan_sha256=args.plan_sha256,
                    max_plan_age_hours=args.max_plan_age_hours,
                )
            except PermissionError as exc:
                print(f"storage_cleanup apply_blocked reason={exc}")
                return 2
            print(f"storage_cleanup applied compressed={result['compressed_count']} deleted={result['deleted_count']}")
            return 0
        policy = load_retention_policy(args.policy)
        plan = build_cleanup_plan(args.root, policy=policy)
        write_cleanup_plan_outputs(plan, args.output_dir)
        print(f"storage_cleanup dry_run output_dir={args.output_dir}")
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
