"""Quota monitor skill — check disk usage and alert on thresholds."""

from __future__ import annotations

import logging
import os
import re
import subprocess
from typing import Any, Dict, List, Optional

from dm_agent.base_skill import BaseSkill, RunContext, SkillResult

logger = logging.getLogger(__name__)


class QuotaMonitorSkill(BaseSkill):
    name = "quota_monitor"
    description = "Monitor disk quota usage and generate alerts when thresholds are exceeded"
    phase = "scan"

    def run(self, context: RunContext) -> SkillResult:
        cfg = self.get_config(context.config)
        warning_threshold = cfg.get("warning_threshold", 0.8)
        critical_threshold = cfg.get("critical_threshold", 0.9)
        method = cfg.get("method", "auto")  # auto | lfs | mmlsquota | du

        results = []

        for member in context.config.members:
            for project_name in member.projects:
                project = context.config.projects.get(project_name)
                if not project:
                    continue

                # Find the project directory under scan targets
                for target in context.config.scan_targets:
                    project_dir = os.path.join(target.path, project_name)
                    if not os.path.isdir(project_dir):
                        continue

                    usage = self._check_usage(project_dir, method)
                    if usage is None:
                        continue

                    # Determine alert level
                    quota_limit = cfg.get("quotas", {}).get(project_name, {}).get("limit_bytes")
                    if quota_limit:
                        percent = usage / quota_limit
                        level = "ok"
                        if percent >= critical_threshold:
                            level = "critical"
                        elif percent >= warning_threshold:
                            level = "warning"

                        entry = {
                            "name": f"{member.name}/{project_name}",
                            "member_email": member.email,
                            "project": project_name,
                            "dir_path": project_dir,
                            "used_bytes": usage,
                            "limit_bytes": quota_limit,
                            "percent": percent * 100,
                            "used_human": self._human_size(usage),
                            "limit_human": self._human_size(quota_limit),
                            "level": level,
                        }
                        results.append(entry)

                        if level != "ok":
                            logger.warning(
                                f"Quota {level}: {entry['name']} at {entry['percent']:.1f}%"
                            )

        context.quota_results = results

        alerts = sum(1 for r in results if r["level"] != "ok")
        return SkillResult(
            success=True,
            message=f"Checked {len(results)} quotas, {alerts} alerts",
            data={"checked": len(results), "alerts": alerts},
        )

    def _check_usage(self, path: str, method: str) -> Optional[int]:
        """Get disk usage in bytes for a path."""
        if method == "auto":
            # Try lfs first (Lustre), fall back to du
            usage = self._try_lfs(path)
            if usage is not None:
                return usage
            return self._try_du(path)
        elif method == "lfs":
            return self._try_lfs(path)
        elif method == "mmlsquota":
            return self._try_mmlsquota(path)
        else:
            return self._try_du(path)

    def _try_lfs(self, path: str) -> Optional[int]:
        """Try Lustre lfs quota."""
        try:
            result = subprocess.run(
                ["lfs", "quota", "-q", path],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                # Parse lfs quota output — format varies by version
                for line in result.stdout.strip().split("\n"):
                    parts = line.split()
                    if len(parts) >= 2:
                        try:
                            return int(parts[1]) * 1024  # KB to bytes
                        except ValueError:
                            continue
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        return None

    def _try_mmlsquota(self, path: str) -> Optional[int]:
        """Try GPFS mmlsquota."""
        try:
            result = subprocess.run(
                ["mmlsquota", "-j", os.path.basename(path)],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            return int(parts[2]) * 1024
                        except ValueError:
                            continue
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        return None

    def _try_du(self, path: str) -> Optional[int]:
        """Fallback: use du -sb."""
        try:
            result = subprocess.run(
                ["du", "-sb", path],
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode == 0:
                return int(result.stdout.split()[0])
        except (subprocess.TimeoutExpired, ValueError, IndexError):
            pass
        return None

    @staticmethod
    def _human_size(size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(size_bytes) < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}PB"
