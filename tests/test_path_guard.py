"""
Comprehensive Unit and Boundary Test Suite for PathGuard & SafeIO.
Verifies absolute isolation of protected trading paths across all traversal and bypass vectors.
"""

import os
import tempfile
from pathlib import Path
import pytest

from src.core.config import DEFAULT_IGNORE_LIST
from src.core.exceptions import ProtectedPathViolationError
from src.core.path_guard import (
    DEFAULT_PATH_GUARD,
    PathGuard,
    is_protected,
    validate_access,
)
from src.core.safe_io import SafeIO


# ============================================================================
# 1. Basic Protected Path Detection Tests
# ============================================================================


class TestPathGuardBasic:
    """Verifies standard detection of all baseline protected trading directories."""

    def test_default_ignore_list_paths(self):
        guard = PathGuard()
        for raw_path in DEFAULT_IGNORE_LIST:
            assert guard.is_protected(raw_path) is True
            expanded = os.path.expanduser(raw_path)
            assert guard.is_protected(expanded) is True
            assert guard.is_protected(Path(expanded)) is True

    def test_keeper_daemon_exact_and_nested(self):
        guard = PathGuard()
        assert guard.is_protected("~/teamwork_projects/keeper_daemon") is True
        assert guard.is_protected("~/teamwork_projects/keeper_daemon/") is True
        assert guard.is_protected("~/teamwork_projects/keeper_daemon/config.json") is True
        assert (
            guard.is_protected(
                "~/teamwork_projects/keeper_daemon/src/deep/nested/worker.py"
            )
            is True
        )

    def test_odin_exact_and_nested(self):
        guard = PathGuard()
        assert guard.is_protected("~/teamwork_projects/odin") is True
        assert guard.is_protected("~/teamwork_projects/odin/") is True
        assert guard.is_protected("~/teamwork_projects/odin/.env") is True
        assert guard.is_protected("~/teamwork_projects/odin/keys/private.pem") is True

    def test_matt_berserker_exact_and_nested(self):
        guard = PathGuard()
        assert guard.is_protected("~/teamwork_projects/matt-berserker") is True
        assert guard.is_protected("~/teamwork_projects/matt-berserker/") is True
        assert (
            guard.is_protected("~/teamwork_projects/matt-berserker/strategies/main.py")
            is True
        )

    def test_allowed_workspaces(self):
        guard = PathGuard()
        assert guard.is_protected("/tmp/bounty_sandboxes/issue_42") is False
        assert (
            guard.is_protected("~/Desktop/activeProjects/universal_bounty_swarm")
            is False
        )
        assert guard.is_protected("~/Desktop/activeProjects/bounty_operations") is False
        assert guard.is_protected("/private/tmp/safe_run") is False

    def test_prefix_collision_prevention(self):
        """Ensures that paths sharing a prefix name are NOT falsely blocked."""
        guard = PathGuard()
        # Similar names to 'odin'
        assert guard.is_protected("~/teamwork_projects/odin_backup") is False
        assert guard.is_protected("~/teamwork_projects/odin2") is False
        assert guard.is_protected("~/teamwork_projects/odin-production") is False
        assert guard.is_protected("~/teamwork_projects/odin_test_dir/file.py") is False

        # Similar names to 'keeper_daemon'
        assert guard.is_protected("~/teamwork_projects/keeper_daemon_v2") is False
        assert guard.is_protected("~/teamwork_projects/keeper_daemon_archive") is False

        # Similar names to 'matt-berserker'
        assert guard.is_protected("~/teamwork_projects/matt-berserker-v2") is False
        assert guard.is_protected("~/teamwork_projects/matt-berserker_copy") is False
        assert guard.is_protected("~/teamwork_projects/matt_berserker") is False

        # Sibling directories under teamwork_projects
        assert guard.is_protected("~/teamwork_projects/tea_octant_registration") is False
        assert guard.is_protected("~/teamwork_projects/general_project") is False


# ============================================================================
# 2. Traversal & Bypass Vector Tests
# ============================================================================


class TestPathGuardBypasses:
    """Verifies that all traversal, symlink, case-folding, and expansion bypasses are blocked."""

    def test_dot_dot_relative_traversal(self):
        guard = PathGuard()
        user_home = str(Path.home())
        # Traverse via parent
        assert guard.is_protected(f"{user_home}/Desktop/../teamwork_projects/odin") is True
        assert guard.is_protected(f"{user_home}/Desktop/sub/../../teamwork_projects/odin") is True
        assert guard.is_protected("/tmp/../../Users/solveetcoagula/teamwork_projects/odin") is True
        assert (
            guard.is_protected("~/teamwork_projects/keeper_daemon/../odin/file.txt")
            is True
        )
        assert (
            guard.is_protected(
                "~/teamwork_projects/matt-berserker/../../teamwork_projects/keeper_daemon"
            )
            is True
        )
        assert guard.is_protected("~/teamwork_projects/odin/./././secret.env") is True
        assert (
            guard.is_protected("~/teamwork_projects/odin/sub/../../teamwork_projects/odin")
            is True
        )

    def test_environment_variable_expansion(self, monkeypatch):
        guard = PathGuard()
        assert guard.is_protected("$HOME/teamwork_projects/odin") is True
        assert guard.is_protected("${HOME}/teamwork_projects/keeper_daemon/log.txt") is True

        monkeypatch.setenv("PROTECTED_SUB", "odin")
        assert guard.is_protected("~/teamwork_projects/$PROTECTED_SUB") is True

        monkeypatch.setenv("BASE_TP", str(Path.home() / "teamwork_projects"))
        assert guard.is_protected("$BASE_TP/matt-berserker/config.yaml") is True

    def test_case_folding_and_preservation(self):
        """Verifies case-insensitive containment checks for APFS/macOS filesystem safety."""
        guard = PathGuard()
        assert guard.is_protected("~/teamwork_projects/ODIN") is True
        assert guard.is_protected("~/teamwork_projects/Odin") is True
        assert guard.is_protected("~/teamwork_projects/ODIN/SECRETS.ENV") is True
        assert guard.is_protected("~/teamwork_projects/Keeper_Daemon") is True
        assert guard.is_protected("~/teamwork_projects/KEEPER_DAEMON/app.py") is True
        assert guard.is_protected("~/TEAMWORK_PROJECTS/matt-berserker") is True
        assert guard.is_protected("~/TEAMWORK_PROJECTS/ODIN/data.db") is True

    def test_non_existent_paths_blocked(self):
        """Verifies that non-existent subpaths inside protected folders are strictly blocked."""
        guard = PathGuard()
        assert (
            guard.is_protected(
                "~/teamwork_projects/odin/completely_non_existent_folder/file_xyz.txt"
            )
            is True
        )
        assert (
            guard.is_protected(
                "~/teamwork_projects/keeper_daemon/future/deep/hierarchy/test.py"
            )
            is True
        )

    def test_symlink_resolution_bypass(self):
        """Verifies that symlinks pointing to protected directories are resolved and blocked."""
        guard = PathGuard()
        target = Path(os.path.expanduser("~/teamwork_projects/odin"))

        with tempfile.TemporaryDirectory() as td:
            symlink_path = Path(td) / "innocent_looking_link"
            os.symlink(target, symlink_path)

            # Direct symlink
            assert guard.is_protected(symlink_path) is True
            # Subpath through symlink
            assert guard.is_protected(symlink_path / "secrets.env") is True
            # Non-existent subpath through symlink
            assert guard.is_protected(symlink_path / "deep" / "non_existent.txt") is True

    def test_chained_symlink_bypass(self):
        """Verifies that chained symlinks (link1 -> link2 -> protected) are blocked."""
        guard = PathGuard()
        target = Path(os.path.expanduser("~/teamwork_projects/keeper_daemon"))

        with tempfile.TemporaryDirectory() as td:
            link1 = Path(td) / "first_link"
            link2 = Path(td) / "second_link"
            os.symlink(target, link1)
            os.symlink(link1, link2)

            assert guard.is_protected(link2) is True
            assert guard.is_protected(link2 / "subfile.py") is True

    def test_relative_symlink_bypass(self):
        """Verifies that relative symlinks pointing outside sandbox to protected dir are blocked."""
        guard = PathGuard()
        user_home = Path.home()

        with tempfile.TemporaryDirectory(dir=user_home) as td:
            # Create link inside ~/tmp_test pointing relatively to teamwork_projects/odin
            rel_link = Path(td) / "rel_link"
            os.symlink("../teamwork_projects/odin", rel_link)

            assert guard.is_protected(rel_link) is True
            assert guard.is_protected(rel_link / "data.json") is True

    def test_boundary_inputs(self):
        guard = PathGuard()
        assert guard.is_protected(None) is False
        assert guard.is_protected("") is False
        assert guard.is_protected("   ") is False


# ============================================================================
# 3. PathGuard Validation API Tests
# ============================================================================


class TestPathGuardValidation:
    """Verifies validate_access, validate_mount, and exception throwing."""

    def test_validate_access_allowed_path(self):
        guard = PathGuard()
        with tempfile.TemporaryDirectory() as td:
            resolved = guard.validate_access(td, operation="read")
            assert isinstance(resolved, Path)
            assert resolved.exists()

    def test_validate_access_protected_path_raises_exception(self):
        guard = PathGuard()
        with pytest.raises(ProtectedPathViolationError) as exc_info:
            guard.validate_access("~/teamwork_projects/odin", operation="write")

        err = exc_info.value
        assert isinstance(err, PermissionError)
        assert err.operation == "write"
        assert "~/teamwork_projects/odin" in str(err) or "odin" in str(err)

    def test_validate_mount(self):
        guard = PathGuard()
        with pytest.raises(ProtectedPathViolationError) as exc_info:
            guard.validate_mount("~/teamwork_projects/keeper_daemon")
        assert exc_info.value.operation == "docker_volume_mount"

    def test_validate_access_empty_raises_value_error(self):
        guard = PathGuard()
        with pytest.raises(ValueError):
            guard.validate_access(None)
        with pytest.raises(ValueError):
            guard.validate_access("")
        with pytest.raises(ValueError):
            guard.validate_access("   ")

    def test_custom_additive_ignore_list(self):
        custom_ignore = ["/tmp/custom_secret_zone"]
        guard = PathGuard(ignore_list=custom_ignore)

        # Baseline protected paths are preserved
        assert guard.is_protected("~/teamwork_projects/odin") is True
        assert guard.is_protected("~/teamwork_projects/keeper_daemon") is True
        assert guard.is_protected("~/teamwork_projects/matt-berserker") is True

        # Custom path is also protected
        assert guard.is_protected("/tmp/custom_secret_zone") is True
        assert guard.is_protected("/tmp/custom_secret_zone/sub.txt") is True

    def test_module_level_helpers(self):
        assert is_protected("~/teamwork_projects/odin") is True
        assert is_protected("/tmp/safe_path") is False

        with pytest.raises(ProtectedPathViolationError):
            validate_access("~/teamwork_projects/odin", operation="test_op")


# ============================================================================
# 4. SafeIO Comprehensive Operations Tests
# ============================================================================


class TestSafeIO:
    """Verifies that SafeIO wrappers correctly enforce PathGuard validation."""

    def test_safe_io_read_and_write_text(self):
        with tempfile.TemporaryDirectory() as td:
            file_path = Path(td) / "sub" / "test.txt"
            written_bytes = SafeIO.write_text(file_path, "hello safe world")
            assert written_bytes == len("hello safe world")
            assert file_path.exists()

            content = SafeIO.read_text(file_path)
            assert content == "hello safe world"

    def test_safe_io_read_and_write_bytes(self):
        with tempfile.TemporaryDirectory() as td:
            file_path = Path(td) / "binary.dat"
            data = b"\x00\x01\x02\xfe\xff"
            written_count = SafeIO.write_bytes(file_path, data)
            assert written_count == len(data)

            read_back = SafeIO.read_bytes(file_path)
            assert read_back == data

    def test_safe_io_blocked_on_protected_read_write(self):
        with pytest.raises(ProtectedPathViolationError):
            SafeIO.read_text("~/teamwork_projects/odin/secrets.env")

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.write_text("~/teamwork_projects/odin/injected.txt", "payload")

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.read_bytes("~/teamwork_projects/keeper_daemon/data.bin")

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.write_bytes("~/teamwork_projects/matt-berserker/data.bin", b"evil")

    def test_safe_io_delete_file(self):
        with tempfile.TemporaryDirectory() as td:
            file_path = Path(td) / "to_delete.txt"
            SafeIO.write_text(file_path, "temp")
            assert file_path.exists()

            SafeIO.delete_file(file_path)
            assert not file_path.exists()

            # Missing ok
            SafeIO.delete_file(file_path, missing_ok=True)
            with pytest.raises(FileNotFoundError):
                SafeIO.delete_file(file_path, missing_ok=False)

    def test_safe_io_delete_protected_blocked(self):
        with pytest.raises(ProtectedPathViolationError):
            SafeIO.delete_file("~/teamwork_projects/odin/important.key")

    def test_safe_io_delete_file_on_directory_raises_is_a_directory_error(self):
        with tempfile.TemporaryDirectory() as td:
            dir_path = Path(td) / "sub_dir"
            SafeIO.mkdir(dir_path)
            with pytest.raises(IsADirectoryError):
                SafeIO.delete_file(dir_path)

    def test_safe_io_rmtree(self):
        with tempfile.TemporaryDirectory() as td:
            dir_to_remove = Path(td) / "tree_to_remove"
            SafeIO.mkdir(dir_to_remove / "sub")
            SafeIO.write_text(dir_to_remove / "sub" / "f.txt", "content")
            assert dir_to_remove.exists()

            SafeIO.rmtree(dir_to_remove)
            assert not dir_to_remove.exists()

    def test_safe_io_rmtree_protected_blocked(self):
        with pytest.raises(ProtectedPathViolationError):
            SafeIO.rmtree("~/teamwork_projects/odin")

    def test_safe_io_rmtree_parent_containing_protected_blocked(self):
        """Verifies defense-in-depth: cannot rmtree ~/teamwork_projects or ~"""
        with pytest.raises(ProtectedPathViolationError):
            SafeIO.rmtree("~/teamwork_projects")

    def test_safe_io_listdir_and_walk(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            SafeIO.write_text(d / "a.txt", "a")
            SafeIO.write_text(d / "b.txt", "b")
            SafeIO.mkdir(d / "sub")
            SafeIO.write_text(d / "sub" / "c.txt", "c")

            entries = SafeIO.listdir(d)
            assert set(entries) == {"a.txt", "b.txt", "sub"}

            walk_results = list(SafeIO.walk(d))
            assert len(walk_results) >= 2

    def test_safe_io_walk_filters_protected_symlinks(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            SafeIO.write_text(d / "normal.txt", "ok")
            os.symlink(
                os.path.expanduser("~/teamwork_projects/odin"),
                d / "symlink_to_odin",
            )

            for root, dirs, files in SafeIO.walk(d):
                assert "symlink_to_odin" not in dirs
                assert "symlink_to_odin" not in files

    def test_safe_io_open_file_context_manager(self):
        with tempfile.TemporaryDirectory() as td:
            file_path = Path(td) / "ctx_test.txt"
            with SafeIO.open_file(file_path, mode="w") as f:
                f.write("context manager write")

            with SafeIO.open_file(file_path, mode="r") as f:
                assert f.read() == "context manager write"

        with pytest.raises(ProtectedPathViolationError):
            with SafeIO.open_file("~/teamwork_projects/odin/secret.txt", mode="r"):
                pass

    def test_safe_io_mkdir_and_touch(self):
        with tempfile.TemporaryDirectory() as td:
            dir_path = Path(td) / "new_dir" / "nested"
            created_dir = SafeIO.mkdir(dir_path)
            assert created_dir.is_dir()

            file_path = dir_path / "touched.txt"
            created_file = SafeIO.touch(file_path)
            assert created_file.is_file()

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.mkdir("~/teamwork_projects/odin/new_sub")

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.touch("~/teamwork_projects/keeper_daemon/touched.txt")

    def test_safe_io_copy_and_move(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "source.txt"
            dst = Path(td) / "dest.txt"
            SafeIO.write_text(src, "copy me")

            SafeIO.copy_file(src, dst)
            assert dst.exists()
            assert SafeIO.read_text(dst) == "copy me"

            dst2 = Path(td) / "moved.txt"
            SafeIO.move(dst, dst2)
            assert not dst.exists()
            assert dst2.exists()

        # Copy/move with protected destination
        with pytest.raises(ProtectedPathViolationError):
            with tempfile.TemporaryDirectory() as td:
                src = Path(td) / "src.txt"
                SafeIO.write_text(src, "data")
                SafeIO.copy_file(src, "~/teamwork_projects/odin/copied.txt")

        # Copy/move with protected source
        with pytest.raises(ProtectedPathViolationError):
            with tempfile.TemporaryDirectory() as td:
                dst = Path(td) / "dst.txt"
                SafeIO.copy_file("~/teamwork_projects/odin/secrets.env", dst)

    def test_safe_io_exists_is_file_is_dir(self):
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "file.txt"
            SafeIO.write_text(f, "data")

            assert SafeIO.exists(f) is True
            assert SafeIO.is_file(f) is True
            assert SafeIO.is_dir(f) is False

            assert SafeIO.exists(td) is True
            assert SafeIO.is_file(td) is False
            assert SafeIO.is_dir(td) is True

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.exists("~/teamwork_projects/odin")

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.is_file("~/teamwork_projects/keeper_daemon")

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.is_dir("~/teamwork_projects/matt-berserker")
