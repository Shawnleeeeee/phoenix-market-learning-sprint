from __future__ import annotations

import json
import os
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from phoenix_storage_manager import (
    apply_cleanup_plan,
    build_cleanup_plan,
    build_storage_inventory,
    default_retention_policy,
    file_sha256,
)


def touch_with_age(path: Path, *, age_days: int, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    stamp = time.time() - age_days * 86400
    os.utime(path, (stamp, stamp))


class StorageManagerTests(unittest.TestCase):
    def test_inventory_reports_top_files_and_categories(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir).resolve()
            touch_with_age(root / "signal_lab_runs" / "shadow.jsonl", age_days=1, content="shadow")
            touch_with_age(root / "round_runner_reports" / "round_001_trades.jsonl", age_days=1, content="trade")
            touch_with_age(root / "logs" / "collector.log", age_days=40, content="log")

            inventory = build_storage_inventory(root)

        self.assertEqual(inventory["project_root"], str(root))
        self.assertGreater(inventory["project_size_bytes"], 0)
        self.assertTrue(inventory["top_files"])
        self.assertIn("shadow_bytes", inventory["category_sizes"])
        self.assertIn("testnet_bytes", inventory["category_sizes"])

    def test_dry_run_cleanup_does_not_delete_files_and_respects_never_delete_patterns(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir).resolve()
            old_tmp = root / ".tmp" / "old.tmp"
            recent_tmp = root / ".tmp" / "recent.tmp"
            env_file = root / ".env"
            trades = root / "round_runner_reports" / "round_001_trades.jsonl"
            manifest = root / "strategy_manifests" / "new_alpha.json"
            touch_with_age(old_tmp, age_days=45)
            touch_with_age(recent_tmp, age_days=2)
            touch_with_age(env_file, age_days=90, content="SECRET=redacted")
            touch_with_age(trades, age_days=90, content="{}")
            touch_with_age(manifest, age_days=90, content="{}")
            policy = default_retention_policy()

            plan = build_cleanup_plan(root, policy=policy)

            self.assertTrue(old_tmp.exists())
            self.assertTrue(recent_tmp.exists())
            self.assertTrue(env_file.exists())
            self.assertTrue(trades.exists())
            self.assertTrue(manifest.exists())
            delete_paths = {item["path"] for item in plan["delete_candidates"]}
            never_paths = {item["path"] for item in plan["never_delete"]}
            self.assertIn(str(old_tmp), delete_paths)
            self.assertNotIn(str(recent_tmp), delete_paths)
            self.assertIn(str(env_file), never_paths)
            self.assertIn(str(trades), never_paths)
            self.assertIn(str(manifest), never_paths)

    def test_apply_is_blocked_by_default_and_does_not_delete_or_compress(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir).resolve()
            old_log = root / "logs" / "old.log"
            old_tmp = root / ".tmp" / "old.tmp"
            touch_with_age(old_log, age_days=45, content="line\n" * 10)
            touch_with_age(old_tmp, age_days=45, content="tmp")
            policy = default_retention_policy()
            plan = build_cleanup_plan(root, policy=policy)

            with self.assertRaisesRegex(ValueError, "--apply"):
                apply_cleanup_plan(plan, apply=False)

            with self.assertRaisesRegex(PermissionError, "real cleanup blocked"):
                apply_cleanup_plan(plan, apply=True)

            self.assertTrue(old_log.exists())
            self.assertTrue(old_tmp.exists())
            self.assertFalse((root / "archives").exists())

    def test_apply_requires_shawn_file_env_hash_and_never_delete_check(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir).resolve()
            env_file = root / ".env"
            touch_with_age(env_file, age_days=90, content="SECRET=redacted")
            plan = build_cleanup_plan(root, policy=default_retention_policy())
            plan["delete_candidates"] = [
                {
                    "path": str(env_file),
                    "relative_path": ".env",
                    "size_bytes": env_file.stat().st_size,
                    "age_days": 90,
                    "action": "delete",
                }
            ]
            plan_path = root / "storage_cleanup_plan.json"
            plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
            approval_file = root / "SHAWN_APPROVED_CLEANUP.txt"
            approval_file.write_text("approved by Shawn for test fixture only\n", encoding="utf-8")

            with self.assertRaisesRegex(PermissionError, "never_delete"):
                apply_cleanup_plan(
                    plan,
                    apply=True,
                    approval_file=approval_file,
                    env={"PHOENIX_ALLOW_REAL_CLEANUP": "true"},
                    plan_path=plan_path,
                    plan_sha256=file_sha256(plan_path),
                )

            self.assertTrue(env_file.exists())


if __name__ == "__main__":
    unittest.main()
