"""
Safe Filesystem I/O Gate.
Wraps all standard filesystem I/O operations with strict PathGuard validation.
"""

import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, List, Optional, Tuple, Union

from src.core.exceptions import ProtectedPathViolationError
from src.core.path_guard import DEFAULT_PATH_GUARD, PathGuard


class SafeIO:
    """
    Drop-in safe filesystem utility class.
    All operations are strictly checked against PathGuard before execution.
    """

    _guard: PathGuard = DEFAULT_PATH_GUARD

    @classmethod
    def set_guard(cls, guard: PathGuard) -> None:
        """Sets an alternative PathGuard instance for SafeIO operations."""
        cls._guard = guard

    @classmethod
    def get_guard(cls) -> PathGuard:
        """Retrieves the active PathGuard instance."""
        return cls._guard

    @classmethod
    def read_text(
        cls,
        path: Union[str, Path, os.PathLike],
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> str:
        """Reads entire file content as string after validating path."""
        valid_path = cls._guard.validate_access(path, operation="read_text")
        return valid_path.read_text(encoding=encoding, errors=errors)

    @classmethod
    def write_text(
        cls,
        path: Union[str, Path, os.PathLike],
        data: str,
        encoding: str = "utf-8",
        errors: str = "strict",
    ) -> int:
        """Writes string data to file after validating path, creating parent dirs if needed."""
        valid_path = cls._guard.validate_access(path, operation="write_text")
        valid_path.parent.mkdir(parents=True, exist_ok=True)
        return valid_path.write_text(data, encoding=encoding, errors=errors)

    @classmethod
    def read_bytes(cls, path: Union[str, Path, os.PathLike]) -> bytes:
        """Reads raw binary bytes from file after validating path."""
        valid_path = cls._guard.validate_access(path, operation="read_bytes")
        return valid_path.read_bytes()

    @classmethod
    def write_bytes(cls, path: Union[str, Path, os.PathLike], data: bytes) -> int:
        """Writes binary bytes to file after validating path, creating parent dirs if needed."""
        valid_path = cls._guard.validate_access(path, operation="write_bytes")
        valid_path.parent.mkdir(parents=True, exist_ok=True)
        return valid_path.write_bytes(data)

    @classmethod
    def delete_file(
        cls, path: Union[str, Path, os.PathLike], missing_ok: bool = True
    ) -> None:
        """Deletes a single file or symlink after validating path."""
        valid_path = cls._guard.validate_access(path, operation="delete_file")
        if valid_path.exists() or valid_path.is_symlink():
            if valid_path.is_dir() and not valid_path.is_symlink():
                raise IsADirectoryError(
                    f"Cannot delete directory with delete_file: {valid_path}. Use rmtree instead."
                )
            valid_path.unlink()
        elif not missing_ok:
            raise FileNotFoundError(f"File not found: {valid_path}")

    @classmethod
    def rmtree(
        cls, path: Union[str, Path, os.PathLike], ignore_errors: bool = False
    ) -> None:
        """Recursively removes a directory tree after validating path and hierarchy containment."""
        valid_path = cls._guard.validate_access(path, operation="rmtree")

        # Defense-in-depth: Prevent deleting an ancestor directory that contains protected paths
        for protected in cls._guard.protected_paths:
            try:
                if protected == valid_path or protected.is_relative_to(valid_path):
                    raise ProtectedPathViolationError(
                        f"[IGNORE_LIST_BLOCKED] Operation 'rmtree' strictly forbidden on parent directory containing protected paths: {path}",
                        path=str(path),
                        operation="rmtree",
                    )
            except (ValueError, AttributeError):
                pass

        if valid_path.exists() and valid_path.is_dir():
            shutil.rmtree(valid_path, ignore_errors=ignore_errors)

    @classmethod
    def listdir(cls, path: Union[str, Path, os.PathLike]) -> List[str]:
        """Lists directory entries after validating path."""
        valid_path = cls._guard.validate_access(path, operation="listdir")
        return os.listdir(valid_path)

    @classmethod
    def walk(
        cls,
        top: Union[str, Path, os.PathLike],
        topdown: bool = True,
        onerror: Any = None,
        followlinks: bool = False,
    ) -> Iterator[Tuple[str, List[str], List[str]]]:
        """
        Safely walks a directory tree, dynamically filtering out any subdirectories
        or files that resolve to protected paths.
        """
        valid_top = cls._guard.validate_access(top, operation="walk")
        for root, dirs, files in os.walk(
            valid_top, topdown=topdown, onerror=onerror, followlinks=followlinks
        ):
            # Dynamic filter to prevent traversal into protected subpaths
            dirs[:] = [
                d for d in dirs if not cls._guard.is_protected(os.path.join(root, d))
            ]
            files_filtered = [
                f for f in files if not cls._guard.is_protected(os.path.join(root, f))
            ]
            yield root, dirs, files_filtered

    @classmethod
    @contextmanager
    def open_file(
        cls,
        path: Union[str, Path, os.PathLike],
        mode: str = "r",
        encoding: Optional[str] = None,
        **kwargs: Any,
    ) -> Iterator[Any]:
        """Context manager for safely opening files."""
        is_write = any(m in mode for m in ("w", "a", "x", "+"))
        op = "open_write" if is_write else "open_read"
        valid_path = cls._guard.validate_access(path, operation=op)

        if is_write:
            valid_path.parent.mkdir(parents=True, exist_ok=True)

        if "b" in mode:
            f = open(valid_path, mode=mode, **kwargs)
        else:
            enc = encoding or "utf-8"
            f = open(valid_path, mode=mode, encoding=enc, **kwargs)

        try:
            yield f
        finally:
            f.close()

    @classmethod
    def mkdir(
        cls,
        path: Union[str, Path, os.PathLike],
        parents: bool = True,
        exist_ok: bool = True,
    ) -> Path:
        """Creates directory after validating path."""
        valid_path = cls._guard.validate_access(path, operation="mkdir")
        valid_path.mkdir(parents=parents, exist_ok=exist_ok)
        return valid_path

    @classmethod
    def touch(
        cls, path: Union[str, Path, os.PathLike], exist_ok: bool = True
    ) -> Path:
        """Touches a file after validating path."""
        valid_path = cls._guard.validate_access(path, operation="touch")
        valid_path.parent.mkdir(parents=True, exist_ok=True)
        valid_path.touch(exist_ok=exist_ok)
        return valid_path

    @classmethod
    def copy_file(
        cls,
        src: Union[str, Path, os.PathLike],
        dst: Union[str, Path, os.PathLike],
        **kwargs: Any,
    ) -> Path:
        """Copies file after validating both source and destination paths."""
        valid_src = cls._guard.validate_access(src, operation="copy_source")
        valid_dst = cls._guard.validate_access(dst, operation="copy_destination")
        valid_dst.parent.mkdir(parents=True, exist_ok=True)
        res = shutil.copy2(valid_src, valid_dst, **kwargs)
        return Path(res)

    @classmethod
    def move(
        cls,
        src: Union[str, Path, os.PathLike],
        dst: Union[str, Path, os.PathLike],
    ) -> Path:
        """Moves file/directory after validating both source and destination paths."""
        valid_src = cls._guard.validate_access(src, operation="move_source")
        valid_dst = cls._guard.validate_access(dst, operation="move_destination")
        valid_dst.parent.mkdir(parents=True, exist_ok=True)
        res = shutil.move(str(valid_src), str(valid_dst))
        return Path(res)

    @classmethod
    def exists(cls, path: Union[str, Path, os.PathLike]) -> bool:
        """Checks if a valid non-protected path exists."""
        valid_path = cls._guard.validate_access(path, operation="exists_check")
        return valid_path.exists()

    @classmethod
    def is_file(cls, path: Union[str, Path, os.PathLike]) -> bool:
        """Checks if a valid non-protected path is a file."""
        valid_path = cls._guard.validate_access(path, operation="is_file_check")
        return valid_path.is_file()

    @classmethod
    def is_dir(cls, path: Union[str, Path, os.PathLike]) -> bool:
        """Checks if a valid non-protected path is a directory."""
        valid_path = cls._guard.validate_access(path, operation="is_dir_check")
        return valid_path.is_dir()
