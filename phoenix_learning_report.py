from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from phoenix_learning_analyzer import run_learning_analysis
from phoenix_promotion_gate import write_promotion_gate_report
from phoenix_strategy_experiments import load_experiment_records, write_experiment_results, write_strategy_experiments
from phoenix_strategy_proposals import write_strategy_proposals


LEARNING_REPORT_VERSION = "v1.0"


def run_learning_evolution_pipeline(
    *,
    input_dir: Path,
    shadow_dir: Path | None = None,
    output_dir: Path | None = None,
    learning_store_file: Path | None = None,
    default_allocation_pct: float = 0.10,
) -> dict[str, Any]:
    root = output_dir or input_dir
    root.mkdir(parents=True, exist_ok=True)
    analysis_report = run_learning_analysis(
        input_dir=input_dir,
        shadow_dir=shadow_dir,
        learning_store_file=learning_store_file,
        output_json=root / "learning_analysis_report.json",
        output_md=root / "learning_analysis_report.md",
    )
    proposal_dir = root / "strategy_proposals"
    proposals = write_strategy_proposals(report=analysis_report, output_dir=proposal_dir)
    experiment_dir = root / "strategy_experiments"
    registry_path = root / "strategy_registry.json"
    experiments = write_strategy_experiments(
        proposals=proposals,
        output_dir=experiment_dir,
        registry_path=registry_path,
        default_allocation_pct=default_allocation_pct,
    )
    records = load_experiment_records(input_dir, learning_store_file=learning_store_file)
    experiment_results_dir = root / "experiment_results"
    experiment_results = write_experiment_results(
        experiments=experiments,
        records=records,
        output_dir=experiment_results_dir,
    )
    promotion_report = write_promotion_gate_report(
        results=experiment_results,
        output_json=root / "promotion_gate_learning_report.json",
        output_md=root / "promotion_gate_learning_report.md",
    )
    summary = {
        "report_type": "learning_evolution_pipeline_summary",
        "version": LEARNING_REPORT_VERSION,
        "input_dir": str(input_dir),
        "output_dir": str(root),
        "analysis_record_count": analysis_report.get("input_counts", {}).get("records", 0),
        "proposal_count": len(proposals),
        "experiment_count": len(experiments),
        "experiment_result_count": len(experiment_results),
        "promotion_decision_count": promotion_report.get("decision_count", 0),
        "agent_enabled": False,
        "live_trading_enabled": False,
        "mainnet_live_promotion_allowed": False,
        "direct_live_change_allowed": False,
        "paths": {
            "learning_analysis_report_json": str(root / "learning_analysis_report.json"),
            "learning_analysis_report_md": str(root / "learning_analysis_report.md"),
            "strategy_proposals_dir": str(proposal_dir),
            "strategy_experiments_dir": str(experiment_dir),
            "experiment_results_dir": str(experiment_results_dir),
            "strategy_registry": str(registry_path),
            "promotion_gate_report_json": str(root / "promotion_gate_learning_report.json"),
            "promotion_gate_report_md": str(root / "promotion_gate_learning_report.md"),
        },
    }
    (root / "learning_evolution_pipeline_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phoenix mechanical learning evolution reports.")
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--shadow-dir", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--learning-store-file", type=Path, default=None)
    parser.add_argument("--default-allocation-pct", type=float, default=0.10)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_learning_evolution_pipeline(
        input_dir=args.input_dir,
        shadow_dir=args.shadow_dir,
        output_dir=args.output_dir,
        learning_store_file=args.learning_store_file,
        default_allocation_pct=args.default_allocation_pct,
    )
    print(
        "learning_evolution proposals=%s experiments=%s output_dir=%s"
        % (summary["proposal_count"], summary["experiment_count"], summary["output_dir"])
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
