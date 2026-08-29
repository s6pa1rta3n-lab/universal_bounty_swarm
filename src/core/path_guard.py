"""
Structural IGNORE_LIST Guardrail Engine.
Enforces non-bypassable physical and logical isolation for protected trading directories.
"""

import os
from pathlib import Path
from typing import List, Optional, Sequence, Union

from src.core.config import DEFAULT_IGNORE_LIST
from src.core.exceptions import ProtectedPathViolationError


class PathGuard:
    """
    Guards the filesystem against unauthorized read, write, traversal, delete,
    or Docker volume mount operations targeting protected system/trading paths.
    """

    DEFAULT_IGNORE_LIST: List[str] = list(DEFAULT_IGNORE_LIST)

    def __init__(self, ignore_list: Optional[Sequence[Union[str, Path]]] = None):
        # Always enforce baseline protected paths, allowing additive extensions
        raw_list = list(self.DEFAULT_IGNORE_LIST)
        if ignore_list:
            for item in ignore_list:
                str_item = str(item).strip()
                if str_item and str_item not in raw_list:
                    raw_list.append(str_item)

        self._raw_ignore_list: List[str] = raw_list
        self.protected_paths: List[Path] = []

        for raw_entry in raw_list:
            for canon in self._get_canonical_variants(raw_entry):
                if canon not in self.protected_paths:
                    self.protected_paths.append(canon)

    @classmethod
    def _get_canonical_variants(cls, p: Union[str, Path, os.PathLike]) -> List[Path]:
        """
        Generates canonical and filesystem alias variants of a path, resolving
        symlinks, tildes, environment variables, relative dots, and macOS firmlinks.
        """
        if p is None:
            return []

        p_str = str(p).strip()
        if not p_str:
            return []

        # 1. Expand environment variables and user tildes
        expanded = os.path.expanduser(os.path.expandvars(p_str))
        raw_path = Path(expanded)

        # 2. Resolve symlinks and relative path segments
        try:
            resolved = raw_path.resolve()
        except Exception:
            resolved = raw_path.absolute()

        variants: List[Path] = [resolved, raw_path.absolute()]
        resolved_str = str(resolved)

        # 3. Handle macOS /private/ prefix aliases (e.g., /tmp -> /private/tmp, /var -> /private/var)
        if resolved_str.startswith("/private/Users/"):
            variants.append(Path("/" + resolved_str.removeprefix("/private/")))
        elif resolved_str.startswith("/private/"):
            variants.append(Path("/" + resolved_str.removeprefix("/private/")))

        # 4. Handle macOS APFS /System/Volumes/Data firmlink prefixes
        if resolved_str.startswith("/System/Volumes/Data/"):
            variants.append(Path("/" + resolved_str.removeprefix("/System/Volumes/Data/")))

        # 5. Deduplicate preserving order
        unique_variants: List[Path] = []
        for v in variants:
            if v not in unique_variants:
                unique_variants.append(v)

        return unique_variants

    def is_protected(self, target_path: Union[str, Path, os.PathLike, None]) -> bool:
        """
        Determines whether target_path matches or is contained inside any protected path.
        Resolves symlinks, tilde expansions, relative path segments, and case folding.
        """
        if target_path is None:
            return False

        p_str = str(target_path).strip()
        if not p_str:
            return False

        target_variants = self._get_canonical_variants(target_path)

        for target in target_variants:
            target_str = str(target)
            target_str_lower = target_str.lower()
            target_parts_lower = [part.lower() for part in target.parts if part]

            for protected in self.protected_paths:
                prot_str = str(protected)
                prot_str_lower = prot_str.lower()
                prot_parts_lower = [part.lower() for part in protected.parts if part]

                # 1. Exact canonical and case-insensitive match
                if target == protected or target_str_lower == prot_str_lower:
                    return True

                # 2. Subpath / descendant match (exact)
                try:
                    if target.is_relative_to(protected):
                        return True
                except (ValueError, AttributeError):
                    pass

                # 3. Subpath / descendant match (case-insensitive parts hierarchy)
                if len(target_parts_lower) >= len(prot_parts_lower):
                    if target_parts_lower[: len(prot_parts_lower)] == prot_parts_lower:
                        return True

                # 4. Common path containment verification
                try:
                    if os.path.commonpath([target_str, prot_str]) == prot_str:
                        return True
                except (ValueError, Exception):
                    pass

                try:
                    if os.path.commonpath([target_str_lower, prot_str_lower]) == prot_str_lower:
                        return True
                except (ValueError, Exception):
                    pass

        return False

    def validate_access(
        self, target_path: Union[str, Path, os.PathLike], operation: str = "access"
    ) -> Path:
        """
        Validates that target_path is not protected under the IGNORE_LIST.
        Returns resolved Path if valid; raises ProtectedPathViolationError if protected.
        """
        if target_path is None:
            raise ValueError("Target path cannot be None")

        p_str = str(target_path).strip()
        if not p_str:
            raise ValueError("Target path cannot be empty")

        if self.is_protected(target_path):
            raise ProtectedPathViolationError(
                f"[IGNORE_LIST_BLOCKED] Operation '{operation}' strictly forbidden on protected trading directory: {target_path}",
                path=str(target_path),
                operation=operation,
            )

        expanded = os.path.expanduser(os.path.expandvars(p_str))
        try:
            return Path(expanded).resolve()
        except Exception:
            return Path(expanded).absolute()

    def validate_mount(self, target_path: Union[str, Path, os.PathLike]) -> Path:
        """
        Specialized validation gate for container volume mounting.
        """
        return self.validate_access(target_path, operation="docker_volume_mount")

    def canonicalize(self, target_path: Union[str, Path, os.PathLike]) -> Path:
        """
        Canonicalizes an allowed path after validation.
        """
        return self.validate_access(target_path, operation="canonicalize")

    def get_ignore_list(self) -> List[Path]:
        """
        Returns a copy of all active canonical protected paths.
        """
        return list(self.protected_paths)


# Global singleton instance for system-wide access
DEFAULT_PATH_GUARD = PathGuard()


def is_protected(target_path: Union[str, Path, os.PathLike, None]) -> bool:
    """Module-level helper to check if a path is protected."""
    return DEFAULT_PATH_GUARD.is_protected(target_path)


def validate_access(
    target_path: Union[str, Path, os.PathLike], operation: str = "access"
) -> Path:
    """Module-level helper to validate path access against the IGNORE_LIST."""
    return DEFAULT_PATH_GUARD.validate_access(target_path, operation=operation)
