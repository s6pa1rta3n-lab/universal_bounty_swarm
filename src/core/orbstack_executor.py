"""
Ephemeral OrbStack Docker Container Runner & Lifecycle Manager
Provides isolated, single-use container execution using Docker CLI / OrbStack socket.
Enforces strict resource quotas, non-privileged execution, structural IGNORE_LIST
path validation via PathGuard, and guaranteed instant container destruction in finally blocks.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Robust import of PathGuard and ProtectedPathViolationError
try:
    from src.core.path_guard import PathGuard, ProtectedPathViolationError
except ImportError:
    try:
        from core.path_guard import PathGuard, ProtectedPathViolationError
    except ImportError:
        try:
            from path_guard import PathGuard, ProtectedPathViolationError
        except ImportError:
            # Standalone fallback implementation if path_guard.py is not yet loaded in PYTHONPATH
            class ProtectedPathViolationError(PermissionError):
                """Raised when any operation attempts to read, write, traverse, delete, or mount a protected path."""
                pass

            class PathGuard:
                DEFAULT_IGNORE_LIST = [
                    "~/teamwork_projects/keeper_daemon",
                    "~/teamwork_projects/odin",
                    "~/teamwork_projects/matt-berserker",
                ]

                def __init__(self, ignore_list: Optional[List[str]] = None):
                    raw_list = ignore_list or self.DEFAULT_IGNORE_LIST
                    self.protected_paths: List[Path] = []
                    for raw_path in raw_list:
                        expanded = os.path.expanduser(os.path.expandvars(raw_path))
                        canonical = Path(expanded).resolve()
                        self.protected_paths.append(canonical)

                def is_protected(self, target_path: Union[str, Path]) -> bool:
                    if not target_path:
                        return False
                    try:
                        expanded_str = os.path.expanduser(os.path.expandvars(str(target_path)))
                        resolved_target = Path(expanded_str).resolve()
                    except Exception:
                        return True  # Fail-safe: if resolution fails, block access

                    for protected in self.protected_paths:
                        if resolved_target == protected:
                            return True
                        try:
                            if resolved_target.is_relative_to(protected):
                                return True
                        except AttributeError:
                            try:
                                if os.path.commonpath([str(resolved_target), str(protected)]) == str(protected):
                                    return True
                            except ValueError:
                                pass
                    return False

                def validate_access(self, target_path: Union[str, Path], operation: str = "access") -> Path:
                    if self.is_protected(target_path):
                        raise ProtectedPathViolationError(
                            f"[IGNORE_LIST_BLOCKED] Operation '{operation}' strictly forbidden on protected trading directory: {target_path}"
                        )
                    return Path(os.path.expanduser(os.path.expandvars(str(target_path)))).resolve()


class ExecutorError(RuntimeError):
    """Base exception for container executor errors."""
    pass


class ContainerExecutionResult:
    """
    Structured result of an ephemeral container execution.
    """

    def __init__(
        self,
        container_name: str,
        exit_code: int,
        stdout: str,
        stderr: str,
        duration_sec: float,
        timed_out: bool = False,
    ) -> None:
        self.container_name = container_name
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.duration_sec = duration_sec
        self.timed_out = timed_out
        self.success = (exit_code == 0) and not timed_out

    def to_dict(self) -> Dict[str, Any]:
        """Convert execution result to a serializable dictionary."""
        return {
            "container_name": self.container_name,
            "exit_code": self.exit_code,
            "success": self.success,
            "timed_out": self.timed_out,
            "duration_sec": round(self.duration_sec, 4),
            "stdout": self.stdout,
            "stderr": self.stderr,
        }

    def __repr__(self) -> str:
        return (
            f"<ContainerExecutionResult name={self.container_name} "
            f"exit_code={self.exit_code} success={self.success} "
            f"timed_out={self.timed_out} duration={self.duration_sec:.2f}s>"
        )


class EphemeralOrbStackExecutor:
    """
    Manages isolated, single-use container execution via Docker / OrbStack.
    Enforces resource bounds (--cpus, --memory, --pids-limit, --tmpfs, --security-opt),
    validates mounted paths against IGNORE_LIST via PathGuard, and guarantees
    immediate container destruction in a finally block (docker rm -f).
    """

    DEFAULT_ORBSTACK_SOCKET = Path.home() / ".orbstack" / "run" / "docker.sock"

    def __init__(
        self,
        default_image: str = "python:3.11-slim",
        default_timeout_sec: int = 300,
        cpus: str = "2",
        memory: str = "2g",
        pids_limit: int = 256,
        tmpfs_size: str = "256m",
        security_opt: str = "no-new-privileges",
        path_guard: Optional[PathGuard] = None,
        docker_bin: Optional[str] = None,
        docker_host: Optional[str] = None,
    ) -> None:
        self.default_image = default_image
        self.default_timeout_sec = default_timeout_sec
        self.cpus = str(cpus)
        self.memory = str(memory)
        self.pids_limit = int(pids_limit)
        self.tmpfs_size = str(tmpfs_size)
        self.security_opt = str(security_opt)
        self.path_guard = path_guard or PathGuard()

        # Locate docker binary
        self.docker_bin = docker_bin or shutil.which("docker") or "docker"

        # Resolve docker host / socket if present
        if docker_host:
            self.docker_host = docker_host
        elif "DOCKER_HOST" in os.environ:
            self.docker_host = os.environ["DOCKER_HOST"]
        elif self.DEFAULT_ORBSTACK_SOCKET.exists():
            self.docker_host = f"unix://{self.DEFAULT_ORBSTACK_SOCKET}"
        else:
            self.docker_host = None

    def _get_subprocess_env(self) -> Dict[str, str]:
        """Builds environment dictionary with DOCKER_HOST if configured."""
        env = dict(os.environ)
        if self.docker_host and "DOCKER_HOST" not in env:
            env["DOCKER_HOST"] = self.docker_host
        return env

    def is_docker_available(self) -> bool:
        """
        Verifies whether Docker daemon / OrbStack is accessible and responsive.
        """
        try:
            res = subprocess.run(
                [self.docker_bin, "info", "--format", "{{.ServerVersion}}"],
                capture_output=True,
                text=True,
                timeout=5,
                env=self._get_subprocess_env(),
            )
            return res.returncode == 0
        except Exception:
            return False

    def cleanup_container(self, container_name: str) -> bool:
        """
        Force-removes a container by name. Guarantees 0 lingering containers.
        """
        if not container_name:
            return True
        try:
            res = subprocess.run(
                [self.docker_bin, "rm", "-f", container_name],
                capture_output=True,
                text=True,
                timeout=10,
                env=self._get_subprocess_env(),
            )
            return res.returncode == 0
        except Exception:
            return False

    def cleanup_stale_containers(self, prefix: str = "bounty-exec-") -> int:
        """
        Scans for and removes all lingering containers matching the prefix.
        Returns the count of cleaned containers.
        """
        try:
            res = subprocess.run(
                [
                    self.docker_bin,
                    "ps",
                    "-a",
                    "--filter",
                    f"name={prefix}",
                    "--format",
                    "{{.Names}}",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                env=self._get_subprocess_env(),
            )
            if res.returncode != 0 or not res.stdout.strip():
                return 0

            names = [n.strip() for n in res.stdout.strip().splitlines() if n.strip()]
            cleaned_count = 0
            for name in names:
                if self.cleanup_container(name):
                    cleaned_count += 1
            return cleaned_count
        except Exception:
            return 0

    def run_isolated(
        self,
        workspace_path: Union[str, Path],
        command: List[str],
        image: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        timeout_sec: Optional[int] = None,
        read_only: bool = False,
        network_mode: str = "bridge",
        extra_mounts: Optional[Dict[str, str]] = None,
        working_dir: str = "/workspace",
        extra_flags: Optional[List[str]] = None,
    ) -> ContainerExecutionResult:
        """
        Executes a command inside an ephemeral OrbStack Docker container.

        1. Validates workspace_path (and any extra_mounts) against IGNORE_LIST using PathGuard.
        2. Spins up single-use container with strict quotas (--cpus, --memory, --pids-limit, --tmpfs, --security-opt).
        3. Enforces timeout.
        4. Guarantees container destruction in a finally block (docker rm -f).

        :param workspace_path: Host directory to mount into container.
        :param command: Command arguments to execute inside container.
        :param image: Docker image to use (defaults to default_image).
        :param env_vars: Optional environment variables dictionary.
        :param timeout_sec: Timeout in seconds for execution.
        :param read_only: Whether workspace mount is read-only (ro) or read-write (rw).
        :param network_mode: Docker network mode (e.g. 'bridge', 'none').
        :param extra_mounts: Optional additional host:container volume mounts.
        :param working_dir: Working directory inside container.
        :param extra_flags: Optional additional raw docker flags.
        :return: ContainerExecutionResult with exit code, outputs, and metrics.
        """
        if not command:
            raise ValueError("Command list must not be empty.")

        # 1. Structural PathGuard Validation Gate
        validated_workspace = self.path_guard.validate_access(
            workspace_path, operation="docker_volume_mount"
        )
        if not validated_workspace.is_dir():
            raise FileNotFoundError(
                f"Workspace directory does not exist or is not a directory: {validated_workspace}"
            )

        # Validate any extra mounts
        validated_extra_mounts: List[str] = []
        if extra_mounts:
            for host_p, cont_p in extra_mounts.items():
                val_host = self.path_guard.validate_access(
                    host_p, operation="docker_volume_mount_extra"
                )
                validated_extra_mounts.extend(["-v", f"{str(val_host)}:{cont_p}"])

        image_to_use = image or self.default_image
        timeout = timeout_sec or self.default_timeout_sec
        container_name = f"bounty-exec-{uuid.uuid4().hex[:12]}"

        # 2. Build Docker CLI Command
        mount_mode = "ro" if read_only else "rw"
        env_args: List[str] = []
        if env_vars:
            for k, v in env_vars.items():
                env_args.extend(["-e", f"{k}={v}"])

        docker_cmd = [
            self.docker_bin,
            "run",
            "--name",
            container_name,
            f"--cpus={self.cpus}",
            f"--memory={self.memory}",
            f"--pids-limit={self.pids_limit}",
            f"--security-opt={self.security_opt}",
            f"--tmpfs=/tmp:rw,size={self.tmpfs_size}",
            "--network",
            network_mode,
            "-v",
            f"{str(validated_workspace)}:{working_dir}:{mount_mode}",
            "-w",
            working_dir,
            *validated_extra_mounts,
            *env_args,
        ]

        if extra_flags:
            docker_cmd.extend(extra_flags)

        docker_cmd.append(image_to_use)
        docker_cmd.extend(command)

        # 3. Execution & Guaranteed Cleanup
        start_time = time.perf_counter()
        stdout, stderr = "", ""
        exit_code = -1
        timed_out = False

        try:
            res = subprocess.run(
                docker_cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=self._get_subprocess_env(),
            )
            stdout = res.stdout
            stderr = res.stderr
            exit_code = res.returncode
        except subprocess.TimeoutExpired as e:
            timed_out = True
            exit_code = -1
            stdout = e.stdout.decode(errors="replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
            stderr = e.stderr.decode(errors="replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
            if not stderr:
                stderr = f"Container execution timed out after {timeout} seconds."
        except Exception as e:
            exit_code = -1
            stderr = f"Container execution error: {str(e)}"
        finally:
            # 4. INSTANT DESTRUCTION GUARANTEE (0 Lingering Containers)
            self.cleanup_container(container_name)
            duration_sec = time.perf_counter() - start_time

        return ContainerExecutionResult(
            container_name=container_name,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            duration_sec=duration_sec,
            timed_out=timed_out,
        )
