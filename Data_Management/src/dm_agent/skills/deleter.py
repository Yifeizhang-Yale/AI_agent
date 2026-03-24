"""Deleter skill — execute confirmed deletions with audit logging."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Any, Dict

from dm_agent.base_skill import BaseSkill, RunContext, SkillResult

logger = logging.getLogger(__name__)


class DeleterSkill(BaseSkill):
    name = "deleter"
    description = "Execute confirmed deletions with trash staging and audit logging"
    phase = "cleanup"

    def run(self, context: RunContext) -> SkillResult:
        cfg = self.get_config(context.config)
        dry_run = cfg.get("dry_run", True)
        trash_dir = cfg.get("trash_dir")

        confirmed = context.db.get_confirmed_deletions()
        if not confirmed:
            return SkillResult(success=True, message="No confirmed deletions to process")

        executed = 0
        failed = 0

        for req in confirmed:
            dir_path = req["dir_path"]
            request_id = req["id"]

            if not os.path.exists(dir_path):
                logger.warning(f"Directory no longer exists: {dir_path}")
                context.db.mark_deletion_executed(request_id)
                context.db.log_audit(
                    deletion_request_id=request_id,
                    dir_path=dir_path,
                    size_bytes=0,
                    confirmed_by=req["owner_email"],
                )
                executed += 1
                continue

            if dry_run:
                logger.info(f"[DRY RUN] Would delete: {dir_path}")
                executed += 1
                continue

            try:
                actual_size = self._get_size(dir_path)

                if trash_dir:
                    # Move to trash instead of permanent delete
                    self._move_to_trash(dir_path, trash_dir)
                    logger.info(f"Moved to trash: {dir_path}")
                else:
                    # Permanent delete
                    shutil.rmtree(dir_path)
                    logger.info(f"Deleted: {dir_path}")

                context.db.mark_deletion_executed(request_id)
                context.db.log_audit(
                    deletion_request_id=request_id,
                    dir_path=dir_path,
                    size_bytes=actual_size,
                    confirmed_by=req["owner_email"],
                )
                executed += 1

            except Exception as e:
                logger.error(f"Failed to delete {dir_path}: {e}")
                context.errors.append(f"Delete failed: {dir_path}: {e}")
                failed += 1

        return SkillResult(
            success=failed == 0,
            message=f"Processed {executed} deletions ({failed} failed), dry_run={dry_run}",
            data={"executed": executed, "failed": failed, "dry_run": dry_run},
        )

    def _move_to_trash(self, dir_path: str, trash_dir: str) -> None:
        """Move directory to trash with timestamp prefix to avoid collisions."""
        from datetime import datetime
        os.makedirs(trash_dir, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        basename = os.path.basename(dir_path)
        trash_dest = os.path.join(trash_dir, f"{timestamp}_{basename}")
        shutil.move(dir_path, trash_dest)

    def _get_size(self, dir_path: str) -> int:
        """Get directory size in bytes."""
        total = 0
        try:
            for dirpath, dirnames, filenames in os.walk(dir_path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    try:
                        total += os.path.getsize(fp)
                    except OSError:
                        pass
        except OSError:
            pass
        return total
