"""Scanner skill — detect filesystem changes and read README / directory structure."""

from __future__ import annotations

import fnmatch
import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from dm_agent.base_skill import BaseSkill, RunContext, SkillResult

logger = logging.getLogger(__name__)


class ScannerSkill(BaseSkill):
    name = "scanner"
    description = "Scan directories for changes since last run, read READMEs and directory trees"
    phase = "scan"

    def run(self, context: RunContext) -> SkillResult:
        cfg = self.get_config(context.config)
        exclude_patterns = cfg.get("exclude_patterns", [".snapshot", ".Trash", "__pycache__"])
        max_depth = cfg.get("max_depth", 5)
        stale_threshold_days = cfg.get("stale_threshold_days", 90)

        all_results = []

        for target in context.config.scan_targets:
            target_path = target.path
            last_scan_ts = context.db.get_last_scan_ts(target_path)

            logger.info(f"Scanning {target_path} (last scan: {last_scan_ts or 'never'})")

            # Find changed files since last scan
            changed_dirs = self._find_changed_dirs(
                target_path, last_scan_ts, exclude_patterns, max_depth
            )

            # Also find stale directories
            stale_dirs = self._find_stale_dirs(
                target_path, stale_threshold_days, exclude_patterns, max_depth
            )

            logger.info(
                f"  Found {len(changed_dirs)} changed dirs, {len(stale_dirs)} stale dirs"
            )

            for dir_path in changed_dirs | stale_dirs:
                readme_content = self._read_readme(dir_path)
                dir_tree = None
                if readme_content is None:
                    dir_tree = self._get_dir_tree(dir_path, max_depth=2)

                # Match directory to member
                member_email = self._match_member(dir_path, context)

                result_id = context.db.save_scan_result(
                    scan_ts=context.run_timestamp.isoformat(),
                    target_path=target_path,
                    changed_dir=dir_path,
                    readme_content=readme_content,
                    dir_tree=dir_tree,
                    member_email=member_email,
                )

                entry = {
                    "id": result_id,
                    "target_path": target_path,
                    "dir_path": dir_path,
                    "readme_content": readme_content,
                    "dir_tree": dir_tree,
                    "member_email": member_email,
                    "is_stale": dir_path in stale_dirs,
                    "is_changed": dir_path in changed_dirs,
                }
                all_results.append(entry)

            # Update scan state
            context.db.update_scan_state(
                target_path,
                context.run_timestamp.isoformat(),
                len(changed_dirs),
            )

        context.scan_results = all_results
        return SkillResult(
            success=True,
            message=f"Scanned {len(context.config.scan_targets)} targets, found {len(all_results)} directories",
            data={"total_dirs": len(all_results)},
        )

    def _find_changed_dirs(
        self,
        target_path: str,
        last_scan_ts: Optional[str],
        exclude_patterns: List[str],
        max_depth: int,
    ) -> Set[str]:
        """Use find to detect directories with changes since last scan."""
        if last_scan_ts is None:
            # First scan: treat all top-level dirs as changed
            return self._get_top_level_dirs(target_path, exclude_patterns)

        # Create a reference timestamp file approach
        # Convert ISO timestamp to find's -newermt format
        cmd = [
            "find", target_path,
            "-maxdepth", str(max_depth),
            "-type", "f",
            "-newermt", last_scan_ts,
        ]

        # Add exclusions
        for pattern in exclude_patterns:
            cmd.extend(["-not", "-path", f"*/{pattern}/*"])

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=300
            )
            if result.returncode != 0 and result.stderr:
                logger.warning(f"find warnings: {result.stderr[:500]}")

            # Extract unique parent directories (one level below target)
            changed_dirs: Set[str] = set()
            for line in result.stdout.strip().split("\n"):
                if not line:
                    continue
                # Get the first-level subdirectory under target_path
                rel = os.path.relpath(line, target_path)
                top_dir = rel.split(os.sep)[0]
                full_dir = os.path.join(target_path, top_dir)
                if os.path.isdir(full_dir):
                    changed_dirs.add(full_dir)

            return changed_dirs

        except subprocess.TimeoutExpired:
            logger.error(f"find timed out for {target_path}")
            return set()
        except Exception as e:
            logger.error(f"find failed for {target_path}: {e}")
            return set()

    def _find_stale_dirs(
        self,
        target_path: str,
        stale_threshold_days: int,
        exclude_patterns: List[str],
        max_depth: int,
    ) -> Set[str]:
        """Find top-level directories not modified within stale_threshold_days."""
        stale_dirs: Set[str] = set()

        try:
            for entry in os.scandir(target_path):
                if not entry.is_dir():
                    continue
                if any(fnmatch.fnmatch(entry.name, p) for p in exclude_patterns):
                    continue

                # Find the most recent mtime in the directory
                cmd = [
                    "find", entry.path,
                    "-maxdepth", str(max_depth),
                    "-type", "f",
                    "-printf", "%T@\n",
                ]
                try:
                    result = subprocess.run(
                        cmd, capture_output=True, text=True, timeout=60
                    )
                    if result.stdout.strip():
                        timestamps = [float(t) for t in result.stdout.strip().split("\n") if t]
                        if timestamps:
                            latest = max(timestamps)
                            age_days = (datetime.now().timestamp() - latest) / 86400
                            if age_days > stale_threshold_days:
                                stale_dirs.add(entry.path)
                except (subprocess.TimeoutExpired, ValueError):
                    continue

        except PermissionError:
            logger.warning(f"Permission denied scanning {target_path}")

        return stale_dirs

    def _get_top_level_dirs(
        self, target_path: str, exclude_patterns: List[str]
    ) -> Set[str]:
        """Get all top-level directories (first scan)."""
        dirs: Set[str] = set()
        try:
            for entry in os.scandir(target_path):
                if entry.is_dir() and not any(
                    fnmatch.fnmatch(entry.name, p) for p in exclude_patterns
                ):
                    dirs.add(entry.path)
        except PermissionError:
            logger.warning(f"Permission denied: {target_path}")
        return dirs

    def _read_readme(self, dir_path: str) -> Optional[str]:
        """Try to read README file from a directory."""
        readme_names = ["README.md", "README.txt", "README", "readme.md"]
        for name in readme_names:
            readme_path = os.path.join(dir_path, name)
            if os.path.isfile(readme_path):
                try:
                    with open(readme_path, encoding="utf-8", errors="replace") as f:
                        content = f.read(10000)  # Cap at 10KB
                    return content
                except (PermissionError, OSError) as e:
                    logger.warning(f"Cannot read {readme_path}: {e}")
        return None

    def _get_dir_tree(self, dir_path: str, max_depth: int = 2) -> str:
        """Get a text representation of the directory tree."""
        lines = []
        try:
            result = subprocess.run(
                ["find", dir_path, "-maxdepth", str(max_depth), "-type", "f"],
                capture_output=True, text=True, timeout=30,
            )
            files = result.stdout.strip().split("\n")[:100]  # Cap at 100 files
            for f in files:
                if f:
                    rel = os.path.relpath(f, dir_path)
                    try:
                        size = os.path.getsize(f)
                        lines.append(f"{rel} ({self._human_size(size)})")
                    except OSError:
                        lines.append(rel)
        except (subprocess.TimeoutExpired, Exception) as e:
            lines.append(f"[error reading tree: {e}]")

        return "\n".join(lines) if lines else "[empty directory]"

    def _match_member(self, dir_path: str, context: RunContext) -> Optional[str]:
        """Match a directory to a lab member based on project mapping."""
        dir_name = os.path.basename(dir_path).lower()
        for member in context.config.members:
            for project in member.projects:
                if project.lower() in dir_name or dir_name in project.lower():
                    return member.email
        return None

    @staticmethod
    def _human_size(size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(size_bytes) < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}PB"
