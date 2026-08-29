"""
Comprehensive Test Suite for Modular Swarm Sidecars & Unified CLI (Milestone 4).

Tests:
1. IntakeSidecar: GraphQL query parsing, Sniper Filter, escrow extraction, deduplication, and Firestore writing.
2. ExecutorSidecar: on_snapshot listener, atomic transactional claiming, PathGuard sandbox validation,
   ephemeral OrbStack execution, swarm_operations recording, status transitions, and instant cleanup.
3. EscortSidecar: PR telemetry, CI failure detection, 14-day staleness flagging, and memory updates.
4. SyncSidecar: Merged PR settlement processing, revenue aggregation, and coordinator state updates.
5. Unified CLI: Subcommand parsing, execution, status reporting, and swarm runner.
6. End-to-End Swarm Integration: Full multi-sidecar reactive lifecycle.
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from src.cli import build_parser, cmd_escort, cmd_executor, cmd_intake, cmd_status, cmd_sync, main
from src.core.config import (
    COLLECTION_BOUNTY_LEADS,
    COLLECTION_BOUNTY_MEMORY,
    COLLECTION_SWARM_COORDINATOR,
    COLLECTION_SWARM_OPERATIONS,
    SwarmConfig,
    get_config,
)
from src.core.exceptions import ProtectedPathViolationError
from src.core.path_guard import PathGuard
from src.core.safe_io import SafeIO
from src.sidecars.escort_sidecar import EscortSidecar, parse_iso_datetime
from src.sidecars.executor_sidecar import ExecutorSidecar
from src.sidecars.intake_sidecar import (
    BANNED_PLATFORMS,
    DEFAULT_SEARCH_CATEGORIES,
    DISQUALIFY_KEYWORDS,
    IntakeSidecar,
    clean_text_for_financials,
    extract_financials,
    verify_escrow,
)
from src.sidecars.sync_sidecar import COLLECTION_BOUNTY_SETTLEMENTS, SyncSidecar, extract_payout_numeric
from tests.conftest import MockFirestoreClient


# ==============================================================================
# 1. INTAKE SIDECAR TESTS
# ==============================================================================

class TestIntakeSidecarSniperFilter:
    """Tests the Sniper Filter and financial extraction in IntakeSidecar."""

    def test_extract_financials_dollars(self):
        txt1 = "We have allocated $500 for this issue."
        formatted, val = extract_financials(txt1)
        assert formatted == "$500.00"
        assert val == 500.0

        txt2 = "Bounty reward: $ 1,250.50 (USD)"
        formatted, val = extract_financials(txt2)
        assert formatted == "$1250.50"
        assert val == 1250.50

        txt3 = "Payment: 750$"
        formatted, val = extract_financials(txt3)
        assert formatted == "$750.00"
        assert val == 750.0

    def test_extract_financials_crypto_tokens(self):
        txt1 = "Reward for completion: 1,000 XLM upon merge."
        formatted, val = extract_financials(txt1)
        assert formatted == "1000.0 XLM"
        assert val == 1000.0

        txt2 = "500 USDC locked in smart contract."
        formatted, val = extract_financials(txt2)
        assert formatted == "500.0 USDC"
        assert val == 500.0

        txt3 = "Payout is 0.5 ETH on Base L2."
        formatted, val = extract_financials(txt3)
        assert formatted == "0.5 ETH"
        assert val == 0.5

    def test_clean_text_strips_gitcoin_promo_noise(self):
        dirty = "Task description\nMore funded OSS work available on gitcoin.co/explorer $10,000,000\nActual bounty is $100"
        formatted, val = extract_financials(dirty)
        assert val == 100.0
        assert formatted == "$100.00"

    def test_verify_escrow_discards_banned_platforms(self):
        for banned in BANNED_PLATFORMS:
            node = {
                "id": "test_id",
                "number": 1,
                "title": f"Bounty on {banned}",
                "body": f"Please visit https://{banned}.io/issue/1 to claim",
                "repository": {"nameWithOwner": "some/repo", "isArchived": False},
                "labels": [{"name": "bounty"}],
            }
            valid, reason, _, _, _, eco = verify_escrow(node)
            assert valid is False
            assert "BANNED_PLATFORM" in reason
            assert eco == "banned"

    def test_verify_escrow_discards_subjective_kyc(self):
        for keyword in ["video pitch", "loom.com", "zoom interview", "manual kyc", "figma only"]:
            node = {
                "id": "test_id",
                "number": 2,
                "title": "Build a widget",
                "body": f"Deliverable requirements: submit a {keyword} explaining architecture.",
                "repository": {"nameWithOwner": "good/repo", "isArchived": False},
                "labels": [{"name": "bounty"}],
            }
            valid, reason, _, _, _, eco = verify_escrow(node)
            assert valid is False
            assert "SUBJECTIVE" in reason

    def test_verify_escrow_discards_archived_repo(self):
        node = {
            "id": "test_id",
            "number": 3,
            "title": "Fix bug",
            "body": "$500 bounty",
            "repository": {"nameWithOwner": "old/repo", "isArchived": True},
            "labels": [{"name": "bounty"}],
        }
        valid, reason, _, _, _, _ = verify_escrow(node)
        assert valid is False
        assert reason == "REJECT_ARCHIVED_REPO"

    def test_verify_escrow_discards_cancelled_bounty(self):
        node = {
            "id": "test_id",
            "number": 4,
            "title": "Feature bounty",
            "body": "$500 bounty",
            "repository": {"nameWithOwner": "active/repo", "isArchived": False},
            "comments": {"nodes": [{"body": "Notice: This bounty has been cancelled by author."}]},
        }
        valid, reason, _, _, _, _ = verify_escrow(node)
        assert valid is False
        assert reason == "REJECT_ESCROW_CANCELLED"

    def test_verify_escrow_qualifies_high_priority_stellar(self):
        node = {
            "id": "I_stellar_42",
            "number": 42,
            "title": "Implement Soroban smart contract oracle",
            "body": "Reward of 2,500 XLM locked in escrow for verified PR.",
            "repository": {"nameWithOwner": "stellar/soroban-tools", "isArchived": False},
            "labels": [{"name": "bounty"}, {"name": "stellar"}],
        }
        valid, reason, payout_str, payout_val, is_high_priority, eco = verify_escrow(node)
        assert valid is True
        assert is_high_priority is True
        assert eco == "stellar"
        assert payout_val == 2500.0
        assert "STELLAR" in reason

    def test_verify_escrow_qualifies_grantfox_and_evm(self):
        node_gf = {
            "id": "I_gf_10",
            "number": 10,
            "title": "GrantFox OSS Task: SDK Nullifier",
            "body": "Grant pool funded: $1,500.00 USDC.",
            "repository": {"nameWithOwner": "grantfox/core", "isArchived": False},
            "labels": [{"name": "GrantFox OSS"}],
        }
        valid, reason, payout_str, payout_val, is_high, eco = verify_escrow(node_gf)
        assert valid is True
        assert is_high is True
        assert eco == "grantfox"
        assert payout_val == 1500.0

        node_evm = {
            "id": "I_evm_20",
            "number": 20,
            "title": "Foundry Solidity test harness on Base",
            "body": "$800 reward for fuzzing test suite.",
            "repository": {"nameWithOwner": "defi-protocol/contracts", "isArchived": False},
            "labels": [{"name": "bounty"}],
        }
        valid, reason, payout_str, payout_val, is_high, eco = verify_escrow(node_evm)
        assert valid is True
        assert is_high is True
        assert eco == "evm"
        assert payout_val == 800.0


class TestIntakeSidecarIngestion:
    """Tests IntakeSidecar deduplication, cache operations, and Firestore writes."""

    def test_intake_sidecar_ingests_and_deduplicates(self, tmp_path: Path, mock_firestore_db: MockFirestoreClient):
        seen_file = tmp_path / "seen_test.json"
        sidecar = IntakeSidecar(
            db=mock_firestore_db,
            seen_cache_path=str(seen_file),
            collection_name=COLLECTION_BOUNTY_LEADS,
        )

        test_issues = [
            {
                "id": "I_001",
                "number": 101,
                "title": "Soroban Token Verification ($500)",
                "body": "Implement verification logic with 500 USDC locked in escrow.",
                "repository": {"nameWithOwner": "stellar/token-verifier", "isArchived": False},
                "labels": [{"name": "bounty"}, {"name": "stellar"}],
            },
            {
                "id": "I_002",
                "number": 102,
                "title": "Generic Documentation Polish",
                "body": "Reward is $50.00",
                "repository": {"nameWithOwner": "docs/repo", "isArchived": False},
                "labels": [{"name": "bounty"}],
            },
            {
                "id": "I_003_BANNED",
                "number": 103,
                "title": "Algora bounty task",
                "body": "algora.io task",
                "repository": {"nameWithOwner": "banned/repo", "isArchived": False},
            },
        ]

        # First ingestion pass
        ingested = sidecar.ingest_bounties(test_issues)
        assert len(ingested) == 2

        # Verify docs in mock Firestore
        col = mock_firestore_db.collection(COLLECTION_BOUNTY_LEADS)
        docs = col.get()
        assert len(docs) == 2

        doc1 = col.document("stellar_token_verifier_101").get().to_dict()
        assert doc1 is not None
        assert doc1["status"] == "priority_triage"
        assert doc1["priority"] == "high"
        assert doc1["ecosystem"] == "stellar"
        assert doc1["projected_payout_usd"] == 500.0

        doc2 = col.document("docs_repo_102").get().to_dict()
        assert doc2 is not None
        assert doc2["status"] == "pending_triage"
        assert doc2["priority"] == "standard"
        assert doc2["projected_payout_usd"] == 50.0

        # Second ingestion pass with same issues — must deduplicate (0 new leads)
        ingested_round2 = sidecar.ingest_bounties(test_issues)
        assert len(ingested_round2) == 0

        # Verify seen cache file was written
        assert seen_file.exists()
        loaded_seen = json.loads(seen_file.read_text())
        assert len(loaded_seen) >= 2


# ==============================================================================
# 2. EXECUTOR SIDECAR TESTS
# ==============================================================================

class TestExecutorSidecarLifecycle:
    """Tests ExecutorSidecar claiming, container execution, and state transitions."""

    def test_executor_sandbox_path_guard_enforcement(self, tmp_path: Path, mock_firestore_db: MockFirestoreClient):
        # Attempting to initialize sandbox inside protected directory must raise ProtectedPathViolationError
        protected_sandbox = Path.home() / "teamwork_projects" / "odin" / "sandboxes"
        with pytest.raises(ProtectedPathViolationError):
            ExecutorSidecar(
                db=mock_firestore_db,
                sandbox_base_dir=str(protected_sandbox),
            )

    def test_executor_claims_and_executes_lead(self, tmp_path: Path, mock_firestore_db: MockFirestoreClient, orbstack_executor):
        sandbox_base = tmp_path / "test_sandboxes"
        executor_sidecar = ExecutorSidecar(
            db=mock_firestore_db,
            executor=orbstack_executor,
            sandbox_base_dir=str(sandbox_base),
            worker_id="test_worker_1",
            leads_collection=COLLECTION_BOUNTY_LEADS,
            operations_collection=COLLECTION_SWARM_OPERATIONS,
        )

        leads_col = mock_firestore_db.collection(COLLECTION_BOUNTY_LEADS)
        lead_id = "test_repo_issue_1"
        lead_data = {
            "id": lead_id,
            "repo": "test/repo",
            "issue_number": 1,
            "title": "Test Lead Execution",
            "status": "priority_triage",
            "target_command": ["python3", "-c", "print('ORBSTACK_UNIT_TEST_SUCCESS')"],
        }
        leads_col.document(lead_id).set(lead_data)

        # Execute lead
        result = executor_sidecar.execute_lead(lead_id, lead_data)
        assert result["success"] is True
        assert result["exit_code"] == 0
        assert result["final_lead_status"] == "pr_open"

        # Check bounty_leads doc status updated in Firestore
        updated_lead = leads_col.document(lead_id).get().to_dict()
        assert updated_lead["status"] == "pr_open"
        assert updated_lead["execution_success"] is True

        # Check swarm_operations doc created with status DESTROYED
        ops_col = mock_firestore_db.collection(COLLECTION_SWARM_OPERATIONS)
        ops_docs = ops_col.get()
        assert len(ops_docs) >= 1
        op = ops_docs[0].to_dict()
        assert op["status"] == "DESTROYED"
        assert op["success"] is True
        assert "ORBSTACK_UNIT_TEST_SUCCESS" in op["stdout_preview"]

        # Confirm sandbox was destroyed
        assert len(list(sandbox_base.glob(f"bounty_*_{result['op_id'][:8]}"))) == 0

    def test_executor_handles_failed_container_execution(self, tmp_path: Path, mock_firestore_db: MockFirestoreClient, orbstack_executor):
        sandbox_base = tmp_path / "test_sandboxes_fail"
        executor_sidecar = ExecutorSidecar(
            db=mock_firestore_db,
            executor=orbstack_executor,
            sandbox_base_dir=str(sandbox_base),
            worker_id="test_worker_fail",
        )

        leads_col = mock_firestore_db.collection(COLLECTION_BOUNTY_LEADS)
        lead_id = "failing_lead_2"
        lead_data = {
            "id": lead_id,
            "repo": "failing/repo",
            "issue_number": 2,
            "title": "Failing Lead Task",
            "status": "priority_triage",
            "target_command": ["python3", "-c", "import sys; sys.exit(42)"],
        }
        leads_col.document(lead_id).set(lead_data)

        result = executor_sidecar.execute_lead(lead_id, lead_data)
        assert result["success"] is False
        assert result["exit_code"] == 42
        assert result["final_lead_status"] == "rejected"

        updated_lead = leads_col.document(lead_id).get().to_dict()
        assert updated_lead["status"] == "rejected"
        assert updated_lead["execution_success"] is False

    def test_executor_real_time_listener_trigger(self, tmp_path: Path, mock_firestore_db: MockFirestoreClient, orbstack_executor):
        sandbox_base = tmp_path / "test_sandboxes_listener"
        executor_sidecar = ExecutorSidecar(
            db=mock_firestore_db,
            executor=orbstack_executor,
            sandbox_base_dir=str(sandbox_base),
            worker_id="test_listener_worker",
            auto_start=True,
        )

        try:
            leads_col = mock_firestore_db.collection(COLLECTION_BOUNTY_LEADS)
            lead_id = "realtime_trigger_3"
            lead_data = {
                "id": lead_id,
                "repo": "realtime/repo",
                "issue_number": 3,
                "title": "Realtime Trigger Lead",
                "status": "priority_triage",
                "target_command": ["python3", "-c", "print('REALTIME_TRIGGERED')"],
            }
            # Adding doc triggers on_snapshot listener
            leads_col.document(lead_id).set(lead_data)

            # Wait for asynchronous execution to complete
            max_wait = 10.0
            start_t = time.time()
            completed = False
            while (time.time() - start_t) < max_wait:
                snap = leads_col.document(lead_id).get().to_dict()
                if snap and snap.get("status") in ("pr_open", "rejected"):
                    completed = True
                    break
                time.sleep(0.2)

            assert completed is True
            final_snap = leads_col.document(lead_id).get().to_dict()
            assert final_snap["status"] == "pr_open"
        finally:
            executor_sidecar.stop()


# ==============================================================================
# 3. ESCORT SIDECAR TESTS
# ==============================================================================

class TestEscortSidecar:
    """Tests EscortSidecar PR telemetry, CI failure parsing, and staleness detection."""

    def test_escort_detects_ci_failure(self):
        sidecar = EscortSidecar(db=MockFirestoreClient())
        pr_data = {
            "pr_url": "https://github.com/test/repo/pull/1",
            "pr_number": 1,
            "repo": "test/repo",
            "commits": {
                "nodes": [
                    {
                        "commit": {
                            "statusCheckRollup": {"state": "FAILURE"},
                            "checkSuites": {
                                "nodes": [
                                    {
                                        "conclusion": "FAILURE",
                                        "checkRuns": {
                                            "nodes": [{"name": "test-suite", "conclusion": "FAILURE"}]
                                        },
                                    }
                                ]
                            },
                        }
                    }
                ]
            },
        }

        res = sidecar.inspect_pr_data(pr_data)
        assert res["ci_status"] == "FAILURE"
        assert res["needs_ci_fix"] is True
        assert "test-suite" in res["ci_failures"]

    def test_escort_detects_14_day_inactivity_staleness(self):
        sidecar = EscortSidecar(db=MockFirestoreClient(), stale_days_threshold=14)
        old_time = (datetime.now(timezone.utc) - timedelta(days=16)).isoformat()
        pr_data = {
            "pr_url": "https://github.com/test/repo/pull/2",
            "pr_number": 2,
            "repo": "test/repo",
            "is_draft": False,
            "created_at": old_time,
            "updated_at": old_time,
            "ci_status": "SUCCESS",
        }

        res = sidecar.inspect_pr_data(pr_data)
        assert res["is_stalled"] is True
        assert res["needs_maintainer_bump"] is True
        assert res["inactivity_days"] >= 15.0

    def test_escort_audit_and_update_memory_doc(self, mock_firestore_db: MockFirestoreClient):
        sidecar = EscortSidecar(db=mock_firestore_db, memory_collection=COLLECTION_BOUNTY_MEMORY)
        mem_col = mock_firestore_db.collection(COLLECTION_BOUNTY_MEMORY)

        doc_id = "test_owner_repo_10"
        mem_col.document(doc_id).set({
            "bounty_id": "test_owner_repo_10",
            "repo": "test_owner/repo",
            "pr_number": 10,
            "audit_status": "PASS",
            "ci_status": "PENDING",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })

        res = sidecar.audit_and_update_pr(doc_id, mem_col.document(doc_id).get().to_dict())
        assert res["review_status"] == "VICTORY_AUDIT_PASSED"

        updated_doc = mem_col.document(doc_id).get().to_dict()
        assert "escort_telemetry" in updated_doc
        assert updated_doc["escort_telemetry"]["ci_status"] == "PENDING"

    def test_escort_run_once(self, mock_firestore_db: MockFirestoreClient):
        sidecar = EscortSidecar(db=mock_firestore_db, memory_collection=COLLECTION_BOUNTY_MEMORY)
        mem_col = mock_firestore_db.collection(COLLECTION_BOUNTY_MEMORY)

        mem_col.document("doc_1").set({"title": "PR 1", "repo": "r/1", "pr_number": 1})
        mem_col.document("doc_2").set({"title": "PR 2", "repo": "r/2", "pr_number": 2})

        results = sidecar.run_once()
        assert len(results) == 2


# ==============================================================================
# 4. SYNC SIDECAR TESTS
# ==============================================================================

class TestSyncSidecar:
    """Tests SyncSidecar settlement recording, revenue aggregation, and coordinator state updates."""

    def test_extract_payout_numeric(self):
        d1 = {"projected_payout_usd": 1250.0, "projected_payout": "$1,250.00"}
        p_str, p_val = extract_payout_numeric(d1)
        assert p_val == 1250.0
        assert p_str == "$1,250.00"

        d2 = {"escrow": {"amount_usd": 500.0}}
        p_str, p_val = extract_payout_numeric(d2)
        assert p_val == 500.0

        d3 = {"title": "Soroban Contract Bounty (1000 XLM)"}
        p_str, p_val = extract_payout_numeric(d3)
        assert p_val == 1000.0
        assert "XLM" in p_str

    def test_sync_settlements_and_coordinator_state(self, mock_firestore_db: MockFirestoreClient):
        sidecar = SyncSidecar(
            db=mock_firestore_db,
            memory_collection=COLLECTION_BOUNTY_MEMORY,
            leads_collection=COLLECTION_BOUNTY_LEADS,
            coordinator_collection=COLLECTION_SWARM_COORDINATOR,
            settlements_collection=COLLECTION_BOUNTY_SETTLEMENTS,
        )

        mem_col = mock_firestore_db.collection(COLLECTION_BOUNTY_MEMORY)
        mem_col.document("merged_pr_1").set({
            "repo": "owner/repo1",
            "pr_number": 11,
            "title": "Implemented Keeper Feature",
            "state": "MERGED",
            "projected_payout_usd": 500.0,
        })
        mem_col.document("audit_passed_pr_2").set({
            "repo": "owner/repo2",
            "pr_number": 12,
            "title": "Solidity Security Fix",
            "audit_status": "PASS",
            "merge_allowed": True,
            "escrow": {"amount_usd": 750.0},
        })

        leads_col = mock_firestore_db.collection(COLLECTION_BOUNTY_LEADS)
        leads_col.document("completed_lead_3").set({
            "repo": "owner/repo3",
            "issue_number": 13,
            "title": "Completed Harvester Job",
            "status": "completed",
            "projected_payout_usd": 250.0,
        })

        # Run sync sweep
        res = sidecar.sync_settlements()
        assert res["synced_count"] == 3
        assert res["total_settled_usd"] == 1500.0

        # Verify settlement records written
        settle_col = mock_firestore_db.collection(COLLECTION_BOUNTY_SETTLEMENTS)
        settlements = settle_col.get()
        assert len(settlements) == 3

        # Verify coordinator state singleton doc
        coord_doc = mock_firestore_db.collection(COLLECTION_SWARM_COORDINATOR).document("state").get().to_dict()
        assert coord_doc["total_settled_usd"] == 1500.0
        assert coord_doc["total_settled_count"] == 3
        assert coord_doc["status"] == "HEALTHY"


# ==============================================================================
# 5. UNIFIED CLI TESTS
# ==============================================================================

class TestUnifiedCLI:
    """Tests the unified CLI commands and subparser configuration."""

    def test_cli_parser_build(self):
        parser = build_parser()
        assert parser.prog == "bounty-swarm"

        # Check subcommands via subparsers action
        subparsers_actions = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
        assert len(subparsers_actions) > 0
        choices = list(subparsers_actions[0].choices.keys())
        for expected in ["intake", "executor", "escort", "sync", "swarm", "status"]:
            assert expected in choices

    def test_cli_intake_once(self):
        with patch("src.cli.IntakeSidecar") as mock_intake:
            mock_inst = MagicMock()
            mock_inst.run_once.return_value = [{"id": "lead_1"}]
            mock_intake.return_value = mock_inst

            exit_code = main(["intake", "--once"])
            assert exit_code == 0
            mock_inst.run_once.assert_called_once()

    def test_cli_executor_once(self):
        with patch("src.cli.ExecutorSidecar") as mock_exec:
            mock_inst = MagicMock()
            mock_exec.return_value = mock_inst

            exit_code = main(["executor", "--once", "--worker-id", "cli_worker_test"])
            assert exit_code == 0
            mock_inst.stop.assert_called()

    def test_cli_escort_once(self):
        with patch("src.cli.EscortSidecar") as mock_escort:
            mock_inst = MagicMock()
            mock_inst.run_once.return_value = [{"pr_number": 1}]
            mock_escort.return_value = mock_inst

            exit_code = main(["escort", "--once", "--stale-days", "10"])
            assert exit_code == 0
            mock_inst.run_once.assert_called_once()

    def test_cli_sync_once(self):
        with patch("src.cli.SyncSidecar") as mock_sync:
            mock_inst = MagicMock()
            mock_inst.run_once.return_value = {"synced_count": 2, "total_settled_usd": 300.0}
            mock_sync.return_value = mock_inst

            exit_code = main(["sync", "--once"])
            assert exit_code == 0
            mock_inst.run_once.assert_called_once()

    def test_cli_status(self, mock_firestore_db: MockFirestoreClient):
        with patch("src.cli.get_firestore_client", return_value=mock_firestore_db):
            mock_firestore_db.collection(COLLECTION_BOUNTY_LEADS).document("l1").set({"status": "priority_triage"})
            mock_firestore_db.collection(COLLECTION_SWARM_COORDINATOR).document("state").set({
                "status": "HEALTHY",
                "total_settled_usd": 500.0,
                "total_settled_count": 1,
            })

            exit_code = main(["status"])
            assert exit_code == 0


# ==============================================================================
# 6. END-TO-END MULTI-SIDECAR SWARM INTEGRATION
# ==============================================================================

class TestEndToEndSwarmPipeline:
    """Full simulated E2E workflow testing all 4 sidecars interacting."""

    def test_full_pipeline_intake_to_sync(self, tmp_path: Path, mock_firestore_db: MockFirestoreClient, orbstack_executor):
        # 1. Intake
        seen_file = tmp_path / "e2e_seen.json"
        intake = IntakeSidecar(
            db=mock_firestore_db,
            seen_cache_path=str(seen_file),
            collection_name=COLLECTION_BOUNTY_LEADS,
        )

        raw_issue = {
            "id": "I_E2E_100",
            "number": 100,
            "title": "E2E Keeper Resolver Pipeline ($1,000)",
            "body": "Implement workable checker. 1,000 USDC funded in escrow.",
            "repository": {"nameWithOwner": "e2e/keeper-resolver", "isArchived": False},
            "labels": [{"name": "bounty"}, {"name": "ethereum"}],
        }

        ingested = intake.ingest_bounties([raw_issue])
        assert len(ingested) == 1
        lead_id = ingested[0]["id"]
        assert ingested[0]["status"] == "priority_triage"

        # 2. Executor
        sandbox_dir = tmp_path / "e2e_sandboxes"
        executor_sidecar = ExecutorSidecar(
            db=mock_firestore_db,
            executor=orbstack_executor,
            sandbox_base_dir=str(sandbox_dir),
            worker_id="e2e_worker",
        )

        lead_data = mock_firestore_db.collection(COLLECTION_BOUNTY_LEADS).document(lead_id).get().to_dict()
        lead_data["target_command"] = ["python3", "-c", "print('E2E_PIPELINE_RESOLVED')"]

        exec_res = executor_sidecar.execute_lead(lead_id, lead_data)
        assert exec_res["success"] is True
        assert exec_res["final_lead_status"] == "pr_open"

        # 3. Escort
        mem_col = mock_firestore_db.collection(COLLECTION_BOUNTY_MEMORY)
        memory_doc_id = "e2e_keeper_resolver_100"
        mem_col.document(memory_doc_id).set({
            "bounty_id": memory_doc_id,
            "repo": "e2e/keeper-resolver",
            "pr_number": 100,
            "title": "E2E Keeper Resolver Pipeline ($1,000)",
            "audit_status": "PASS",
            "ci_status": "SUCCESS",
            "state": "MERGED",
            "projected_payout_usd": 1000.0,
        })

        escort = EscortSidecar(db=mock_firestore_db)
        escort_res = escort.run_once()
        assert len(escort_res) == 1
        assert escort_res[0]["review_status"] == "MERGED"

        # 4. Sync
        sync = SyncSidecar(db=mock_firestore_db)
        sync_res = sync.run_once()
        assert sync_res["synced_count"] == 1
        assert sync_res["total_settled_usd"] == 1000.0

        coord_state = mock_firestore_db.collection(COLLECTION_SWARM_COORDINATOR).document("state").get().to_dict()
        assert coord_state["total_settled_usd"] == 1000.0
        assert coord_state["status"] == "HEALTHY"
