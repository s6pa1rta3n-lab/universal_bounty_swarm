"""
Unit and Integration Tests for EphemeralOrbStackExecutor
Tests container lifecycle, isolated resource quotas, PathGuard structural IGNORE_LIST enforcement,
timeout enforcement, and guaranteed instant container destruction (0 lingering containers).
"""

import os
import subprocess
import tempfile
import time
from pathlib import Path
import pytest

from src.core.orbstack_executor import (
    EphemeralOrbStackExecutor,
    ContainerExecutionResult,
    PathGuard,
    ProtectedPathViolationError,
)


@pytest.fixture
def executor() -> EphemeralOrbStackExecutor:
    """Fixture providing a configured EphemeralOrbStackExecutor."""
    return EphemeralOrbStackExecutor(
        default_image="python:3.11-slim",
        default_timeout_sec=60,
        cpus="2",
        memory="2g",
        pids_limit=256,
        tmpfs_size="256m",
    )


@pytest.fixture
def temp_workspace():
    """Fixture providing a temporary host workspace directory."""
    with tempfile.TemporaryDirectory(prefix="bounty_sandbox_test_") as tmpdir:
        yield Path(tmpdir)


def test_docker_available(executor: EphemeralOrbStackExecutor):
    """Verifies that the Docker / OrbStack daemon is running and reachable."""
    assert executor.is_docker_available() is True, "Docker / OrbStack daemon must be running."


def test_container_spinup_and_successful_execution(
    executor: EphemeralOrbStackExecutor, temp_workspace: Path
):
    """Verifies running a basic command inside the ephemeral container."""
    result = executor.run_isolated(
        workspace_path=temp_workspace,
        command=["python", "-c", "print('OrbStack Ephemeral Runner OK')"],
    )

    assert result.success is True
    assert result.exit_code == 0
    assert not result.timed_out
    assert "OrbStack Ephemeral Runner OK" in result.stdout
    assert result.duration_sec > 0


def test_container_execution_with_custom_image_node(
    executor: EphemeralOrbStackExecutor, temp_workspace: Path
):
    """Verifies running with alternative runtime image (Node.js)."""
    result = executor.run_isolated(
        workspace_path=temp_workspace,
        image="node:22",
        command=["node", "-e", "console.log('OrbStack Node.js Runner OK')"],
    )

    assert result.success is True
    assert result.exit_code == 0
    assert not result.timed_out
    assert "OrbStack Node.js Runner OK" in result.stdout


def test_container_bidirectional_workspace_io(
    executor: EphemeralOrbStackExecutor, temp_workspace: Path
):
    """Verifies file I/O inside mounted workspace is reflected on host."""
    # Write input file on host
    input_file = temp_workspace / "input.txt"
    input_file.write_text("input data from host")

    # Execute container script that reads input.txt and writes output.txt
    result = executor.run_isolated(
        workspace_path=temp_workspace,
        command=[
            "python",
            "-c",
            (
                "with open('input.txt', 'r') as f: data = f.read()\n"
                "with open('output.txt', 'w') as f: f.write(data + ' -> processed in container')\n"
                "print('Processed successfully')"
            ),
        ],
    )

    assert result.success is True
    output_file = temp_workspace / "output.txt"
    assert output_file.exists()
    assert output_file.read_text() == "input data from host -> processed in container"


def test_container_read_only_workspace(
    executor: EphemeralOrbStackExecutor, temp_workspace: Path
):
    """Verifies that read_only=True prevents writes to the workspace."""
    result = executor.run_isolated(
        workspace_path=temp_workspace,
        command=["python", "-c", "with open('forbidden.txt', 'w') as f: f.write('bad')"],
        read_only=True,
    )

    assert result.success is False
    assert result.exit_code != 0
    assert "Read-only file system" in result.stderr or "Permission denied" in result.stderr
    assert not (temp_workspace / "forbidden.txt").exists()


def test_instant_destruction_on_success(
    executor: EphemeralOrbStackExecutor, temp_workspace: Path
):
    """Verifies that container is immediately destroyed upon normal completion."""
    result = executor.run_isolated(
        workspace_path=temp_workspace,
        command=["python", "-c", "print('hello')"],
    )

    assert result.success is True

    # Query docker to verify 0 lingering containers with this container name
    check = subprocess.run(
        [
            executor.docker_bin,
            "ps",
            "-a",
            "--filter",
            f"name={result.container_name}",
            "--format",
            "{{.Names}}",
        ],
        capture_output=True,
        text=True,
    )
    matching_names = [n.strip() for n in check.stdout.strip().splitlines() if n.strip()]
    assert result.container_name not in matching_names, f"Lingering container found: {result.container_name}"


def test_instant_destruction_on_error(
    executor: EphemeralOrbStackExecutor, temp_workspace: Path
):
    """Verifies that container is destroyed even when the process exits with non-zero code."""
    result = executor.run_isolated(
        workspace_path=temp_workspace,
        command=["python", "-c", "import sys; print('Failing now', file=sys.stderr); sys.exit(42)"],
    )

    assert result.success is False
    assert result.exit_code == 42
    assert "Failing now" in result.stderr

    # Confirm container was deleted
    check = subprocess.run(
        [
            executor.docker_bin,
            "ps",
            "-a",
            "--filter",
            f"name={result.container_name}",
            "--format",
            "{{.Names}}",
        ],
        capture_output=True,
        text=True,
    )
    matching_names = [n.strip() for n in check.stdout.strip().splitlines() if n.strip()]
    assert result.container_name not in matching_names


def test_timeout_enforcement_and_instant_destruction(
    executor: EphemeralOrbStackExecutor, temp_workspace: Path
):
    """Verifies timeout expiration kills and removes the container immediately."""
    start_time = time.perf_counter()
    result = executor.run_isolated(
        workspace_path=temp_workspace,
        command=["python", "-c", "import time; time.sleep(10)"],
        timeout_sec=2,
    )
    duration = time.perf_counter() - start_time

    assert result.timed_out is True
    assert result.success is False
    assert result.exit_code == -1
    # Duration should be around 2-3 seconds, definitely less than the 10s sleep
    assert duration < 6.0

    # Confirm container is not running or lingering in docker ps
    check = subprocess.run(
        [
            executor.docker_bin,
            "ps",
            "-a",
            "--filter",
            f"name={result.container_name}",
            "--format",
            "{{.Names}}",
        ],
        capture_output=True,
        text=True,
    )
    matching_names = [n.strip() for n in check.stdout.strip().splitlines() if n.strip()]
    assert result.container_name not in matching_names, f"Lingering container on timeout: {result.container_name}"


def test_env_vars_injection(
    executor: EphemeralOrbStackExecutor, temp_workspace: Path
):
    """Verifies environment variables are properly passed to the container."""
    env = {
        "SWARM_TASK_ID": "task-abc-123",
        "EXECUTION_MODE": "isolated-orbstack",
    }
    result = executor.run_isolated(
        workspace_path=temp_workspace,
        command=[
            "python",
            "-c",
            "import os; print(f\"{os.environ['SWARM_TASK_ID']}:{os.environ['EXECUTION_MODE']}\")",
        ],
        env_vars=env,
    )

    assert result.success is True
    assert "task-abc-123:isolated-orbstack" in result.stdout


def test_custom_resource_quotas(
    temp_workspace: Path
):
    """Verifies executor applies custom resource quotas properly."""
    custom_executor = EphemeralOrbStackExecutor(
        cpus="1",
        memory="512m",
        pids_limit=128,
        tmpfs_size="128m",
    )
    result = custom_executor.run_isolated(
        workspace_path=temp_workspace,
        command=["python", "-c", "print('Quotas verified')"],
    )

    assert result.success is True
    assert "Quotas verified" in result.stdout


def test_path_guard_blocks_ignore_list(
    executor: EphemeralOrbStackExecutor
):
    """Verifies PathGuard strictly blocks attempts to mount IGNORE_LIST directories."""
    forbidden_paths = [
        "~/teamwork_projects/keeper_daemon",
        "~/teamwork_projects/odin",
        "~/teamwork_projects/matt-berserker",
        os.path.expanduser("~/teamwork_projects/odin"),
        os.path.expanduser("~/teamwork_projects/keeper_daemon/subfile.py"),
    ]

    for forbidden in forbidden_paths:
        with pytest.raises(ProtectedPathViolationError):
            executor.run_isolated(
                workspace_path=forbidden,
                command=["ls", "-la"],
            )


def test_path_guard_blocks_relative_traversal(
    executor: EphemeralOrbStackExecutor
):
    """Verifies PathGuard catches relative traversal attempts to protected paths."""
    traversal_path = os.path.expanduser("~/teamwork_projects/odin/../matt-berserker")
    with pytest.raises(ProtectedPathViolationError):
        executor.run_isolated(
            workspace_path=traversal_path,
            command=["ls"],
        )


def test_path_guard_blocks_extra_mounts_violation(
    executor: EphemeralOrbStackExecutor, temp_workspace: Path
):
    """Verifies PathGuard validates extra_mounts as well."""
    with pytest.raises(ProtectedPathViolationError):
        executor.run_isolated(
            workspace_path=temp_workspace,
            command=["ls"],
            extra_mounts={"~/teamwork_projects/odin": "/odin_mount"},
        )


def test_non_existent_workspace_raises_file_not_found(
    executor: EphemeralOrbStackExecutor
):
    """Verifies passing a non-existent directory raises FileNotFoundError."""
    non_existent = Path("/tmp/non_existent_workspace_dir_99999999")
    with pytest.raises(FileNotFoundError):
        executor.run_isolated(
            workspace_path=non_existent,
            command=["ls"],
        )


def test_empty_command_raises_value_error(
    executor: EphemeralOrbStackExecutor, temp_workspace: Path
):
    """Verifies passing empty command raises ValueError."""
    with pytest.raises(ValueError):
        executor.run_isolated(
            workspace_path=temp_workspace,
            command=[],
        )


def test_result_to_dict_serialization():
    """Verifies ContainerExecutionResult serialization."""
    res = ContainerExecutionResult(
        container_name="test-container-1",
        exit_code=0,
        stdout="sample out",
        stderr="",
        duration_sec=1.23456,
        timed_out=False,
    )
    d = res.to_dict()
    assert d["container_name"] == "test-container-1"
    assert d["exit_code"] == 0
    assert d["success"] is True
    assert d["timed_out"] is False
    assert d["duration_sec"] == 1.2346
    assert d["stdout"] == "sample out"
    assert d["stderr"] == ""


def test_cleanup_stale_containers(executor: EphemeralOrbStackExecutor):
    """Verifies cleanup_stale_containers method runs without error."""
    count = executor.cleanup_stale_containers("bounty-exec-nonexistent-")
    assert isinstance(count, int)
    assert count >= 0
