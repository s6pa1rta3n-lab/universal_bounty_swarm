"""
Configuration Management for Universal Bounty Swarm.
Enforces hardcoded security policies, GCP project settings, and container quotas.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Absolute hardcoded IGNORE_LIST preventing any operations on protected trading paths
DEFAULT_IGNORE_LIST: List[str] = [
    "~/teamwork_projects/keeper_daemon",
    "~/teamwork_projects/odin",
    "~/teamwork_projects/matt-berserker",
]

# GCP / Firebase Configuration Defaults
DEFAULT_GCP_PROJECT_ID: str = "odin-500008"
DEFAULT_FIRESTORE_DATABASE: str = "(default)"

# Firestore Collection Names
COLLECTION_BOUNTY_LEADS: str = "bounty_leads"
COLLECTION_SWARM_OPERATIONS: str = "swarm_operations"
COLLECTION_SWARM_COORDINATOR: str = "swarm_coordinator"
COLLECTION_BOUNTY_TASKS: str = "bounty_tasks"
COLLECTION_BOUNTY_MEMORY: str = "bounty_memory"

# Container Isolation Quotas
DEFAULT_DOCKER_IMAGE: str = "python:3.11-slim"
DEFAULT_CONTAINER_CPUS: str = "2"
DEFAULT_CONTAINER_MEMORY: str = "2g"
DEFAULT_CONTAINER_PIDS_LIMIT: int = 256
DEFAULT_CONTAINER_TIMEOUT_SEC: int = 300
DEFAULT_SANDBOX_BASE_DIR: str = "/tmp/bounty_sandboxes"

# Mandatory Web3 / Gitcoin Payout Routing
EVM_PAYOUT_ADDRESS: str = "0xF46C9F6d70C50BF81ef3588AB523a90a594a2F89"
STELLAR_PAYOUT_ADDRESS: str = "GCL6OXAMLD75BMTINA6EMRUDWK5THQUSHMYNLSNBCJAPZJHNYJTUNIBC"


@dataclass(frozen=True)
class SwarmConfig:
    """Immutable runtime configuration container."""

    gcp_project_id: str = field(
        default_factory=lambda: os.getenv("GCP_PROJECT_ID", DEFAULT_GCP_PROJECT_ID)
    )
    firestore_database: str = field(
        default_factory=lambda: os.getenv("FIRESTORE_DATABASE_ID", DEFAULT_FIRESTORE_DATABASE)
    )
    ignore_list: List[str] = field(default_factory=lambda: list(DEFAULT_IGNORE_LIST))
    docker_image: str = field(
        default_factory=lambda: os.getenv("SWARM_DOCKER_IMAGE", DEFAULT_DOCKER_IMAGE)
    )
    container_cpus: str = field(
        default_factory=lambda: os.getenv("SWARM_CONTAINER_CPUS", DEFAULT_CONTAINER_CPUS)
    )
    container_memory: str = field(
        default_factory=lambda: os.getenv("SWARM_CONTAINER_MEMORY", DEFAULT_CONTAINER_MEMORY)
    )
    container_pids_limit: int = field(
        default_factory=lambda: int(
            os.getenv("SWARM_CONTAINER_PIDS_LIMIT", str(DEFAULT_CONTAINER_PIDS_LIMIT))
        )
    )
    container_timeout_sec: int = field(
        default_factory=lambda: int(
            os.getenv("SWARM_CONTAINER_TIMEOUT_SEC", str(DEFAULT_CONTAINER_TIMEOUT_SEC))
        )
    )
    sandbox_base_dir: str = field(
        default_factory=lambda: os.getenv("SWARM_SANDBOX_DIR", DEFAULT_SANDBOX_BASE_DIR)
    )
    evm_payout_address: str = EVM_PAYOUT_ADDRESS
    stellar_payout_address: str = STELLAR_PAYOUT_ADDRESS


_CONFIG_INSTANCE: Optional[SwarmConfig] = None


def get_config() -> SwarmConfig:
    """Retrieves or initializes the global singleton configuration."""
    global _CONFIG_INSTANCE
    if _CONFIG_INSTANCE is None:
        _CONFIG_INSTANCE = SwarmConfig()
    return _CONFIG_INSTANCE
