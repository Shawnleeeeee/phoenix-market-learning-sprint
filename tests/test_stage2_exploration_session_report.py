import importlib.util
from pathlib import Path


def _load_runner_module(version: str = "v02"):
    script = Path(__file__).resolve().parents[1] / f".tmp_stage2_exploration_{version}.py"
    spec = importlib.util.spec_from_file_location(f"stage2_exploration_{version}_runner", script)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _snapshot() -> dict:
    return {
        "system_status": {
            "candidate_latest_age_sec": 3.0,
            "candidate_latest_signal_at": "2026-05-15T10:56:16+00:00",
            "snapshot_generated_at": "2026-05-15T10:56:29+00:00",
        },
        "market_regime": {
            "regime": "TREND_UP",
            "source": "signal_lab.event_snapshots.enrichments",
            "confidence": 0.801,
            "reason": "trend up test fixture",
        },
        "top_candidates": [
            {
                "symbol": "PAXGUSDT",
                "bias": "LONG",
                "exchange_filter_checked": True,
                "symbol_tradeable": True,
                "micro_notional_feasible": True,
                "required_quote_allocation_usdt": 4.56875,
                "configured_quote_allocation_usdt": 5.0,
                "configured_leverage": 2,
                "max_quote_allocation_usdt": 10.0,
                "rounded_qty": 0.002,
                "min_qty": 0.001,
                "step_size": 0.001,
                "min_notional": 5.0,
            }
        ],
    }


def _cycle_result() -> dict:
    decision = {
        "action": "ENTER_LONG",
        "symbol": "PAXGUSDT",
        "source": "HERMES",
        "writer": "Hermes Trader Brain",
        "reason": "Stage 2 testnet exploration: candidate ready.",
        "stop_loss_pct": 0.25,
        "take_profit_pct": 0.45,
        "max_holding_time_sec": 300,
        "invalidation_condition": "fixture invalidation",
    }


    intent = {
        "symbol": "PAXGUSDT",
        "quantity": 0.002,
        "notional_usdt": 9.1375,
        "leverage": 2,
    }
    lifecycle = {
        "entry_fill_price": 4565.84,
        "position_size": 0.002,
        "timeout_due_at": "2026-05-15T11:01:33+00:00",
        "final_close_reason": "CLOSED_BY_TIMEOUT",
        "timeout_close_response": {"orderId": 187497420},
        "close_fill_price": 4568.51,
        "realized_pnl_usdt": 0.00534,
        "roi_pct": 0.116955,
        "fees": 0.00913434,
        "holding_time_sec": 317.392,
    }
    return {
        "trace_id": "htm_fixture",
        "snapshot_path": "/tmp/snapshot_htm_fixture.json",
        "hermes_provider_result": {
            "provider": "file",
            "decision_origin": "outbox_file",
            "fallback_used": False,
            "created_at": "2026-05-15T10:56:30+00:00",
            "decision": decision,
            "raw_response": {"_decision_path": "/tmp/decision_htm_fixture.json"},
        },
        "risk_governor_result": {"created_at": "2026-05-15T10:56:31+00:00", "approved": True},
        "safe_order_gateway_result": {
            "created_at": "2026-05-15T10:56:32+00:00",
            "execution_intent": intent,
            "normalized_decision": decision,
        },
        "execution_result": {
            "created_at": "2026-05-15T11:01:51+00:00",
            "executor_called": True,
            "order_submitted": True,
            "testnet_order_submitted": True,
            "mainnet_order_submitted": False,
            "payload": {
                "intent": intent,
                "entry_response": {"orderId": 187494710, "avgPrice": "4565.8400000", "cumQuote": "9.1316800"},
                "protective_stop_response": {"algoId": 1000000075987618, "reduceOnly": True, "closePosition": True},
                "take_profit_response": {"algoId": 1000000075987620, "reduceOnly": True, "closePosition": True},
                "lifecycle_result": lifecycle,
            },
        },
    }


def _observation_result(decision_path: Path, *, action: str = "ENTER_LONG") -> dict:
    result = _cycle_result()
    result["trace_id"] = "obs_fixture"
    decision = dict(result["hermes_provider_result"]["decision"])
    decision.update(
        {
            "trace_id": "obs_fixture",
            "action": action,
            "decision": action,
            "normalized_decision": action,
            "source": "HERMES",
            "writer": "Hermes Trader Brain",
        }
    )
    if action == "NO_TRADE":
        decision["reason"] = "Hermes observation says no trade."
        decision["no_trade_reason"] = "Hermes observation says no trade."
    result["hermes_provider_result"].update(
        {
            "created_at": "2026-05-15T10:56:30+00:00",
            "decision": decision,
            "normalized_decision": decision,
            "raw_response": {
                "_decision_path": str(decision_path),
                "trace_id": "obs_fixture",
                "decision": decision,
                "source": "HERMES",
                "writer": "Hermes Trader Brain",
            },
        }
    )
    result["safe_order_gateway_result"]["normalized_decision"] = decision
    result["safe_order_gateway_result"]["execution_intent"] = {
        "symbol": "PAXGUSDT",
        "dry_run": True,
        "approved_for_execution": action != "NO_TRADE",
    }
    result["execution_result"] = {
        "created_at": "2026-05-15T10:56:33+00:00",
        "executor_called": False,
        "order_submitted": False,
        "testnet_order_submitted": False,
        "mainnet_order_submitted": False,
        "dry_run": True,
        "status": "dry_run_intent_only",
        "result_type": "approved_dry_run" if action != "NO_TRADE" else "soft_reject",
    }
    result["bridge_archive"] = {}
    return result


def test_cycle_report_generation_after_close_does_not_reference_session_summary_state(tmp_path):
    runner = _load_runner_module()
    session_dir = tmp_path / "session"
    cycle_dir = session_dir / "cycles" / "cycle_01"
    ending_state = {"positions_count": 0, "open_orders_count": 0, "conditional_orders_count": 0}

    report = runner._cycle_report(
        session_dir=session_dir,
        cycle_dir=cycle_dir,
        result=_cycle_result(),
        snapshot=_snapshot(),
        gate_samples=[{"passed": True}],
        ending_state=ending_state,
    )

    assert report["final_close_reason"] == "CLOSED_BY_TIMEOUT"
    assert report["entry_order_id"] == 187494710
    assert report["stop_loss_order_id"] == 1000000075987618
    assert report["take_profit_order_id"] == 1000000075987620
    assert report["ending_positions"] == 0
    assert (cycle_dir / "cycle_report.json").exists()
    assert (session_dir / "cycle_reports.jsonl").exists()


def test_report_generation_error_writes_minimal_cycle_report_without_losing_lifecycle_record(tmp_path):
    runner = _load_runner_module()
    session_dir = tmp_path / "session"
    cycle_dir = session_dir / "cycles" / "cycle_01"
    ending_state = {"positions_count": 0, "open_orders_count": 0, "conditional_orders_count": 0}

    report = runner._minimal_cycle_report_after_generation_error(
        session_dir=session_dir,
        cycle_dir=cycle_dir,
        result=_cycle_result(),
        report_error=NameError("cycle_reports is not defined"),
        ending_state=ending_state,
    )

    assert report["report_reconstructed_from_execution_result"] is True
    assert "cycle_reports is not defined" in report["report_generation_error"]
    assert report["final_close_reason"] == "CLOSED_BY_TIMEOUT"
    assert report["final_close_order_id"] == 187497420
    assert report["testnet_order_submitted"] is True
    assert report["mainnet_order_submitted"] is False
    assert report["ending_positions"] == 0
    assert (cycle_dir / "cycle_report_generation_error.json").exists()


def test_session_summary_keeps_enter_reason_in_good_enter_examples(tmp_path):
    runner = _load_runner_module()
    cycle = {
        "cycle_dir": str(tmp_path / "cycle_01"),
        "trace_id": "htm_fixture",
        "symbol": "PAXGUSDT",
        "hermes_decision": "ENTER_LONG",
        "testnet_order_submitted": True,
        "hard_freeze": False,
        "final_close_reason": "CLOSED_BY_TIMEOUT",
        "realized_pnl_usdt": 0.00534,
        "roi_pct": 0.116955,
        "fees": 0.00913434,
        "holding_time_sec": 317.392,
        "market_regime": "TREND_UP",
        "enter_reason": "Stage 2 testnet exploration: candidate ready.",
    }

    report = runner._summarize_session(
        session_dir=tmp_path,
        started_at="2026-05-15T10:20:11+00:00",
        ended_at="2026-05-15T11:01:53+00:00",
        stop_reason="cycle_report_generation_failed",
        readiness_samples=3,
        gate_attempts=1,
        cancelled_by_gate=0,
        cycle_reports=[cycle],
        ending_state={"positions_count": 0, "open_orders_count": 0, "conditional_orders_count": 0},
    )

    assert report["good_enter_examples"][0]["enter_reason"] == "Stage 2 testnet exploration: candidate ready."
    assert "dry-run" not in report["good_enter_examples"][0]["enter_reason"]
    assert report["ending_positions"] == 0


def test_v03_observation_report_is_dry_run_no_order_even_for_enter(tmp_path):
    runner = _load_runner_module("v03")
    decision_path = tmp_path / "decision_obs_fixture.json"
    decision_path.write_text('{"trace_id":"obs_fixture"}\n', encoding="utf-8")
    session_dir = tmp_path / "session"
    observation_dir = session_dir / "observations" / "obs_0001"
    gate_sample = {
        "passed": True,
        "errors": [],
        "market_regime": "TREND_UP",
        "estimated_slippage_bps": 1.0,
    }

    report = runner._observation_report(
        session_dir=session_dir,
        observation_dir=observation_dir,
        observation_index=1,
        result=_observation_result(decision_path, action="ENTER_LONG"),
        snapshot=_snapshot(),
        gate_sample=gate_sample,
    )

    assert report["lane"] == "observation"
    assert report["hermes_decision"] == "ENTER_LONG"
    assert report["observation_label"] == "POTENTIAL_ENTER"
    assert report["decision_origin"] == "outbox_file"
    assert report["fallback_used"] is False
    assert report["source"] == "HERMES"
    assert report["writer"] == "Hermes Trader Brain"
    assert report["decision_file_exists"] is True
    assert report["executor_called"] is False
    assert report["order_submitted"] is False
    assert report["testnet_order_submitted"] is False
    assert report["mainnet_order_submitted"] is False
    assert runner._runtime_participation_failures(report) == []
    assert runner._observation_no_order_failures(report) == []
    assert (observation_dir / "observation_report.json").exists()
    assert (session_dir / "observation_samples.jsonl").exists()


def test_v03_cycle_report_marks_negative_candidate_latencies_unavailable_but_keeps_boundary_latencies(tmp_path):
    runner = _load_runner_module("v03")
    session_dir = tmp_path / "session"
    cycle_dir = session_dir / "cycles" / "cycle_01"
    snapshot = _snapshot()
    snapshot["system_status"]["snapshot_time"] = "2026-05-15T10:56:29+00:00"
    snapshot["top_candidates"][0]["candidate_generated_at"] = "2026-05-15T10:56:40+00:00"
    result = _cycle_result()
    result["hermes_provider_result"]["decision"]["created_at"] = "2026-05-15T10:56:30+00:00"
    result["execution_result"]["created_at"] = "2026-05-15T10:56:33+00:00"

    report = runner._cycle_report(
        session_dir=session_dir,
        cycle_dir=cycle_dir,
        result=result,
        snapshot=snapshot,
        gate_samples=[{"passed": True}],
        ending_state={"positions_count": 0, "open_orders_count": 0, "conditional_orders_count": 0},
    )

    assert report["candidate_to_decision_latency_ms"] is None
    assert report["candidate_to_order_latency_ms"] is None
    assert report["snapshot_to_decision_latency_ms"] == 1000.0
    assert report["decision_to_gateway_latency_ms"] == 2000.0
    assert "candidate_to_decision_latency_ms" in report["latency_reliability"]["candidate_latency_unavailable_fields"]
    assert "candidate_to_order_latency_ms" in report["latency_reliability"]["candidate_latency_unavailable_fields"]


def test_v03_bridge_latency_fields_marks_negative_candidate_latencies_unavailable_but_keeps_boundary_latencies(tmp_path):
    runner = _load_runner_module("v03")
    snapshot = _snapshot()
    snapshot["system_status"]["snapshot_time"] = "2026-05-15T10:56:29+00:00"
    snapshot["top_candidates"][0]["candidate_generated_at"] = "2026-05-15T10:56:40+00:00"
    decision_path = tmp_path / "decision_obs_fixture.json"
    decision_path.write_text('{"trace_id":"obs_fixture"}\n', encoding="utf-8")
    result = _observation_result(decision_path, action="ENTER_LONG")
    provider = result["hermes_provider_result"]
    provider["decision"]["created_at"] = "2026-05-15T10:56:30+00:00"
    (tmp_path / "safe_order_gateway.jsonl").write_text(
        '{"created_at":"2026-05-15T10:56:32+00:00"}\n',
        encoding="utf-8",
    )
    (tmp_path / "execution_result.jsonl").write_text(
        '{"created_at":"2026-05-15T10:56:33+00:00","executor_called":false}\n',
        encoding="utf-8",
    )

    latency = runner._bridge_latency_fields(
        trace_id="obs_fixture",
        snapshot=snapshot,
        provider_result=provider,
        output_dir=tmp_path,
    )

    assert latency["candidate_to_decision_latency_ms"] is None
    assert latency["candidate_to_order_latency_ms"] is None
    assert latency["snapshot_to_decision_latency_ms"] == 1000.0
    assert latency["decision_to_gateway_latency_ms"] == 2000.0
    assert "candidate_to_decision_latency_ms" in latency["latency_reliability"]["candidate_latency_unavailable_fields"]
    assert "candidate_to_order_latency_ms" in latency["latency_reliability"]["candidate_latency_unavailable_fields"]


def test_v03_runtime_participation_requires_real_decision_file(tmp_path):
    runner = _load_runner_module("v03")
    report = {
        "trace_id": "obs_fixture",
        "decision_origin": "outbox_file",
        "fallback_used": False,
        "source": "HERMES",
        "writer": "Hermes Trader Brain",
        "decision_path": str(tmp_path / "missing_decision.json"),
    }

    assert runner._runtime_participation_failures(report) == ["decision_file_not_found"]


def test_v03_session_summary_writes_analyst_package_and_keeps_lanes_separate(tmp_path):
    runner = _load_runner_module("v03")
    decision_path = tmp_path / "decision_obs_fixture.json"
    decision_path.write_text('{"trace_id":"obs_fixture"}\n', encoding="utf-8")
    observation = runner._observation_report(
        session_dir=tmp_path,
        observation_dir=tmp_path / "observations" / "obs_0001",
        observation_index=1,
        result=_observation_result(decision_path, action="ENTER_LONG"),
        snapshot=_snapshot(),
        gate_sample={"passed": True, "errors": [], "market_regime": "TREND_UP"},
    )
    cycle = {
        "cycle_dir": str(tmp_path / "cycles" / "cycle_01"),
        "trace_id": "exec_fixture",
        "symbol": "PAXGUSDT",
        "market_regime": "TREND_UP",
        "hermes_decision": "ENTER_LONG",
        "decision_origin": "outbox_file",
        "fallback_used": False,
        "source": "HERMES",
        "writer": "Hermes Trader Brain",
        "testnet_order_submitted": True,
        "mainnet_order_submitted": False,
        "hard_freeze": False,
        "final_close_reason": "CLOSED_BY_TIMEOUT",
        "enter_reason": "Stage 2 testnet exploration: candidate ready.",
        "realized_pnl_usdt": 0.00534,
        "roi_pct": 0.116955,
        "fees": 0.00913434,
        "holding_time_sec": 317.392,
    }

    report = runner._summarize_session_v03(
        session_dir=tmp_path,
        started_at="2026-05-15T10:20:11+00:00",
        ended_at="2026-05-15T11:01:53+00:00",
        stop_reason="sample_targets_reached",
        readiness_samples=3,
        gate_attempts=1,
        cancelled_by_gate=0,
        observation_reports=[observation],
        cycle_reports=[cycle],
        ending_state={"positions_count": 0, "open_orders_count": 0, "conditional_orders_count": 0},
    )

    package_path = Path(report["analyst_review_package_path"])
    assert report["total_observation_samples"] == 1
    assert report["total_hermes_decision_samples"] == 2
    assert report["ENTER_LONG_observation"] == 1
    assert report["real_testnet_trades"] == 1
    assert report["mainnet_order_submitted"] is False
    assert report["cleanup"] is False
    assert report["auto_promotion"] is False
    assert package_path.exists()
    assert (package_path / "all_observation_samples.jsonl").exists()
    assert (package_path / "all_hermes_decisions.jsonl").exists()
    assert (package_path / "all_executed_trades.jsonl").exists()
    assert (package_path / "package_index.json").exists()
