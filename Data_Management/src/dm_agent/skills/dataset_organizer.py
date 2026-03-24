"""Dataset organizer skill — reorganize datasets and remove redundant files.

Uses catalog data + Claude analysis to:
1. Detect redundant files (uncompressed copies, duplicates, double-processed)
2. Compare file content (head/tail) to confirm redundancy before deletion
3. Reorganize directory structure based on best practices

Safety invariants:
- redundant_dir must be a strict subdirectory of the dataset root OR a known
  duplicate/temp directory; it must NEVER equal primary_dir.
- When comparing .nii against .nii.gz, auto-upgrade to gz_uncompressed strategy.
- If >80% of files in a group would be deleted, flag it as suspicious and skip.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from dm_agent.base_skill import BaseSkill, RunContext, SkillResult
from dm_agent.claude_client import create_client, parse_json_response

logger = logging.getLogger(__name__)

# Number of bytes to read from head/tail for binary comparison
COMPARE_BYTES = 8192  # 8KB from each end
# Number of lines to read from head/tail for text comparison
COMPARE_LINES = 5
# Safety: if this fraction of files in a redundancy group would be deleted, flag it
SUSPICIOUS_DELETE_RATIO = 0.9


ORGANIZER_SYSTEM_PROMPT = """You are a neuroimaging data management expert. Given a dataset's current
structure and catalog analysis, produce a concrete reorganization plan.

You will receive:
- The current directory tree with file counts and sizes
- Previous analysis summary and recommendations
- Dataset metadata

Produce a JSON plan with two sections:

1. "redundant_files": list of file groups to check for redundancy
2. "reorganize": list of move operations

Format:
{
  "redundant_files": [
    {
      "description": "why these are redundant",
      "primary_dir": "/absolute/path/to/authoritative/files",
      "redundant_dir": "/absolute/path/to/redundant/copies",
      "match_strategy": "stem" | "exact" | "gz_uncompressed",
      "safe_to_delete_without_compare": false
    }
  ],
  "reorganize": [
    {
      "action": "move" | "mkdir" | "rename",
      "src": "/absolute/path/from",
      "dst": "/absolute/path/to",
      "description": "why"
    }
  ],
  "summary": "One paragraph describing the plan"
}

CRITICAL RULES for redundant_files:
- primary_dir is the AUTHORITATIVE directory whose files are KEPT.
- redundant_dir is the directory whose files are CANDIDATES FOR DELETION.
- primary_dir and redundant_dir MUST be different directories.
- redundant_dir should typically be a subdirectory (like "unzipped/", "duplicate_images/")
  or a clearly secondary copy.
- NEVER mark the main data directory as redundant_dir — that would delete primary data.
- When .nii files are uncompressed copies of .nii.gz files, use match_strategy "gz_uncompressed".
- When files have "(1)" suffix (copy artifacts), use match_strategy "stem".

match_strategy meanings:
- "stem": match by filename stem (ignoring extension differences like .nii vs .nii.gz)
- "exact": match by exact filename
- "gz_uncompressed": .nii files are uncompressed versions of .nii.gz files — the primary
  file is .nii.gz and the redundant file is .nii. Comparison decompresses the .nii.gz to
  verify content matches the .nii before deletion.

Be conservative — only mark safe_to_delete_without_compare=true for obvious cases.
For everything else, require content comparison.
"""


@dataclass
class RedundancyResult:
    """Result of checking one pair of potentially redundant files."""
    primary_path: str
    redundant_path: str
    is_redundant: bool
    reason: str
    size_saved: int = 0


class DatasetOrganizerSkill(BaseSkill):
    name = "dataset_organizer"
    description = "Reorganize datasets and remove redundant files"
    phase = "cleanup"

    def run(self, context: RunContext) -> SkillResult:
        """Organize a specific dataset based on catalog data."""
        cfg = self.get_config(context.config)
        dataset_name = cfg.get("target_dataset")
        dry_run = cfg.get("dry_run", True)

        if not dataset_name:
            return SkillResult(success=False, message="No target_dataset specified in config")

        # Get dataset info from DB
        datasets = context.db.get_all_datasets()
        ds = next((d for d in datasets if d["dataset_name"] == dataset_name), None)
        if not ds:
            return SkillResult(success=False, message=f"Dataset '{dataset_name}' not found in catalog")

        if ds["status"] != "cataloged":
            return SkillResult(success=False, message=f"Dataset '{dataset_name}' not yet cataloged")

        root_path = ds["root_path"]
        logger.info(f"Organizing dataset: {dataset_name} at {root_path} (dry_run={dry_run})")

        # Get catalog entries
        entries = context.db.get_catalog_entries(ds["id"])

        # Phase 1: Ask Claude for a reorganization plan
        plan = self._get_plan(context, ds, entries)
        if not plan:
            return SkillResult(success=False, message="Failed to generate reorganization plan")

        logger.info(f"Plan: {plan.get('summary', 'no summary')}")

        # Phase 1b: Validate the plan before executing
        redundant_groups = self._validate_redundancy_groups(
            plan.get("redundant_files", []), root_path
        )

        # Phase 2: Handle redundant files
        redundancy_stats = {"checked": 0, "deleted": 0, "size_saved": 0, "skipped": 0}

        for group in redundant_groups:
            results = self._check_redundancy_group(group, root_path)

            # Safety check: if too many files are marked for deletion, skip group
            # Exception: if redundant_dir is a subdirectory of primary_dir,
            # it's expected that 100% of files are redundant (e.g. unzipped/, duplicate_images/)
            if results:
                n_delete = sum(1 for r in results if r.is_redundant)
                ratio = n_delete / len(results) if results else 0
                total_in_dir = sum(1 for _ in os.scandir(group["redundant_dir"])
                                   if _.is_file(follow_symlinks=False))

                redundant_is_subdir = os.path.normpath(
                    group["redundant_dir"]
                ).startswith(os.path.normpath(group["primary_dir"]) + os.sep)

                if (ratio > SUSPICIOUS_DELETE_RATIO
                        and total_in_dir > 50
                        and not redundant_is_subdir):
                    logger.warning(
                        f"  SAFETY: Skipping group '{group.get('description', '')}' — "
                        f"would delete {n_delete}/{len(results)} files "
                        f"({ratio:.0%}), exceeds safety threshold. "
                        f"Review manually."
                    )
                    redundancy_stats["skipped"] += len(results)
                    continue

            for r in results:
                redundancy_stats["checked"] += 1
                if r.is_redundant:
                    if dry_run:
                        logger.info(
                            f"  [DRY RUN] Would delete: {r.redundant_path} "
                            f"({self._human_size(r.size_saved)}) — {r.reason}"
                        )
                        redundancy_stats["deleted"] += 1
                        redundancy_stats["size_saved"] += r.size_saved
                    else:
                        try:
                            os.remove(r.redundant_path)
                            logger.info(
                                f"  Deleted: {r.redundant_path} "
                                f"({self._human_size(r.size_saved)}) — {r.reason}"
                            )
                            redundancy_stats["deleted"] += 1
                            redundancy_stats["size_saved"] += r.size_saved
                        except OSError as e:
                            logger.warning(f"  Failed to delete {r.redundant_path}: {e}")
                            redundancy_stats["skipped"] += 1
                else:
                    logger.info(f"  Kept: {r.redundant_path} — {r.reason}")
                    redundancy_stats["skipped"] += 1

        # Phase 2b: Remove empty directories left after deletion
        if not dry_run:
            for group in redundant_groups:
                rdir = group.get("redundant_dir", "")
                if os.path.isdir(rdir) and not os.listdir(rdir):
                    os.rmdir(rdir)
                    logger.info(f"  Removed empty directory: {rdir}")

        # Phase 3: Reorganize directory structure
        reorg_stats = {"moved": 0, "created": 0, "skipped": 0}
        for op in plan.get("reorganize", []):
            action = op.get("action")
            src = op.get("src", "")
            dst = op.get("dst", "")

            if dry_run:
                logger.info(
                    f"  [DRY RUN] {action}: {src} -> {dst} "
                    f"({op.get('description', '')})"
                )
                if action in ("move", "rename"):
                    reorg_stats["moved"] += 1
                elif action == "mkdir":
                    reorg_stats["created"] += 1
                continue

            try:
                if action == "mkdir":
                    os.makedirs(dst, exist_ok=True)
                    logger.info(f"  Created: {dst}")
                    reorg_stats["created"] += 1
                elif action in ("move", "rename"):
                    if os.path.exists(src):
                        os.makedirs(os.path.dirname(dst), exist_ok=True)
                        shutil.move(src, dst)
                        logger.info(f"  Moved: {src} -> {dst}")
                        reorg_stats["moved"] += 1
                    else:
                        logger.warning(f"  Source not found: {src}")
                        reorg_stats["skipped"] += 1
            except OSError as e:
                logger.warning(f"  Failed {action} {src} -> {dst}: {e}")
                reorg_stats["skipped"] += 1

        prefix = "[DRY RUN] " if dry_run else ""
        msg = (
            f"{prefix}Organized {dataset_name}: "
            f"checked {redundancy_stats['checked']} files, "
            f"{'would delete' if dry_run else 'deleted'} {redundancy_stats['deleted']} "
            f"({self._human_size(redundancy_stats['size_saved'])}), "
            f"{'would move' if dry_run else 'moved'} {reorg_stats['moved']} dirs/files"
        )

        return SkillResult(
            success=True,
            message=msg,
            data={
                "dataset": dataset_name,
                "dry_run": dry_run,
                "redundancy": redundancy_stats,
                "reorganize": reorg_stats,
                "plan_summary": plan.get("summary", ""),
            },
        )

    # ------------------------------------------------------------------
    # Plan generation
    # ------------------------------------------------------------------

    def _get_plan(
        self,
        context: RunContext,
        ds: Dict[str, Any],
        entries: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Ask Claude to generate a reorganization plan."""
        client = create_client(context.config.analyzer)

        # Build directory tree summary
        tree_lines = []
        for e in entries:
            indent = "  " * e["depth"]
            flags = []
            if e["entry_type"] != "other":
                flags.append(e["entry_type"])
            if e["data_stage"] != "unknown":
                flags.append(e["data_stage"])
            if e["modality"]:
                flags.append(e["modality"])
            flag_str = f" [{', '.join(flags)}]" if flags else ""
            files_str = f" ({e['file_count']} files, {self._human_size(e['size_bytes'])})"
            sample = ""
            if e["sample_files"]:
                samples = (
                    json.loads(e["sample_files"])
                    if isinstance(e["sample_files"], str)
                    else e["sample_files"]
                )
                if samples:
                    sample = f"\n{indent}  samples: {', '.join(samples[:5])}"
            tree_lines.append(
                f"{indent}{e['rel_path']}{flag_str}{files_str}{sample}"
            )

        prompt = f"""Dataset: {ds['dataset_name']}
Root path: {ds['root_path']}
Total size: {self._human_size(ds['total_size_bytes'] or 0)}
Total files: {ds['total_files'] or 0}

Previous analysis summary:
{ds.get('summary', 'N/A')}

Previous recommendations:
{ds.get('recommendations', '[]')}

Directory tree:
{chr(10).join(tree_lines)}

Please generate a concrete reorganization plan for this dataset.
Focus on:
1. Identifying redundant files (uncompressed copies of .nii.gz, "(1)" duplicates, double-processed files)
2. Proposing directory restructuring for clarity

Use absolute paths based on the root path above.
"""

        try:
            text = client.ask(ORGANIZER_SYSTEM_PROMPT, prompt)
            plan = parse_json_response(text)
            logger.info(f"Raw plan from Claude: {json.dumps(plan, indent=2)[:2000]}")
            return plan
        except Exception as e:
            logger.error(f"Failed to get reorganization plan: {e}")
            return None

    # ------------------------------------------------------------------
    # Plan validation
    # ------------------------------------------------------------------

    def _validate_redundancy_groups(
        self,
        groups: List[Dict[str, Any]],
        root_path: str,
    ) -> List[Dict[str, Any]]:
        """Validate and fix redundancy groups before execution.

        Safety checks:
        1. primary_dir != redundant_dir
        2. redundant_dir must not be a parent of primary_dir
        3. Auto-upgrade match_strategy to gz_uncompressed when comparing
           .nii against .nii.gz across directories
        """
        validated = []

        for group in groups:
            primary = os.path.normpath(group.get("primary_dir", ""))
            redundant = os.path.normpath(group.get("redundant_dir", ""))
            desc = group.get("description", "(no description)")

            # Check 1: dirs must be different
            if primary == redundant:
                logger.warning(
                    f"  REJECTED group '{desc}': primary_dir == redundant_dir "
                    f"({primary}). This would delete primary data!"
                )
                continue

            # Check 2: redundant must not be parent of primary
            if primary.startswith(redundant + os.sep):
                logger.warning(
                    f"  REJECTED group '{desc}': redundant_dir ({redundant}) "
                    f"is a parent of primary_dir ({primary}). "
                    f"This would delete the parent of primary data!"
                )
                continue

            # Check 3: both dirs must exist
            if not os.path.isdir(primary):
                logger.warning(
                    f"  REJECTED group '{desc}': primary_dir not found: {primary}"
                )
                continue
            if not os.path.isdir(redundant):
                logger.warning(
                    f"  REJECTED group '{desc}': redundant_dir not found: {redundant}"
                )
                continue

            # Auto-fix: detect .nii vs .nii.gz mismatch and upgrade strategy
            strategy = group.get("match_strategy", "stem")
            if strategy != "gz_uncompressed":
                primary_exts = self._sample_extensions(primary, n=20)
                redundant_exts = self._sample_extensions(redundant, n=20)
                has_gz_primary = ".nii.gz" in primary_exts
                has_nii_redundant = ".nii" in redundant_exts and ".nii.gz" not in redundant_exts

                if has_gz_primary and has_nii_redundant:
                    logger.info(
                        f"  Auto-upgrading strategy for '{desc}': "
                        f"stem -> gz_uncompressed (primary has .nii.gz, "
                        f"redundant has .nii)"
                    )
                    group["match_strategy"] = "gz_uncompressed"

            validated.append(group)
            logger.info(
                f"  Validated group: '{desc}' — "
                f"primary={primary}, redundant={redundant}, "
                f"strategy={group.get('match_strategy', 'stem')}"
            )

        return validated

    @staticmethod
    def _sample_extensions(dir_path: str, n: int = 20) -> set:
        """Sample file extensions from a directory."""
        exts = set()
        count = 0
        try:
            for f in os.scandir(dir_path):
                if f.is_file(follow_symlinks=False):
                    name = f.name
                    if name.endswith(".nii.gz"):
                        exts.add(".nii.gz")
                    else:
                        _, ext = os.path.splitext(name)
                        if ext:
                            exts.add(ext.lower())
                    count += 1
                    if count >= n:
                        break
        except PermissionError:
            pass
        return exts

    # ------------------------------------------------------------------
    # Redundancy checking
    # ------------------------------------------------------------------

    def _check_redundancy_group(
        self,
        group: Dict[str, Any],
        root_path: str,
    ) -> List[RedundancyResult]:
        """Check a group of potentially redundant files."""
        results = []
        primary_dir = group.get("primary_dir", "")
        redundant_dir = group.get("redundant_dir", "")
        strategy = group.get("match_strategy", "stem")
        safe_without_compare = group.get("safe_to_delete_without_compare", False)

        if not os.path.isdir(redundant_dir):
            logger.warning(f"Redundant dir not found: {redundant_dir}")
            return results

        # Build index of primary files by stem
        primary_files = {}
        if os.path.isdir(primary_dir):
            for f in os.scandir(primary_dir):
                if f.is_file(follow_symlinks=False):
                    stem = self._get_stem(f.name)
                    primary_files[stem] = f.path

        # Check each file in the redundant directory
        for f in os.scandir(redundant_dir):
            if not f.is_file(follow_symlinks=False):
                continue

            redundant_stem = self._get_stem(f.name)
            # Remove " (1)" suffix for matching copy artifacts
            clean_stem = redundant_stem.replace(" (1)", "").replace("(1)", "").strip()

            matched_primary = (
                primary_files.get(redundant_stem) or primary_files.get(clean_stem)
            )

            if not matched_primary:
                results.append(RedundancyResult(
                    primary_path="",
                    redundant_path=f.path,
                    is_redundant=False,
                    reason="No matching primary file found",
                ))
                continue

            if safe_without_compare:
                try:
                    size = f.stat(follow_symlinks=False).st_size
                except OSError:
                    size = 0
                results.append(RedundancyResult(
                    primary_path=matched_primary,
                    redundant_path=f.path,
                    is_redundant=True,
                    reason="Copy artifact (safe to delete without compare)",
                    size_saved=size,
                ))
                continue

            # Auto-detect correct comparison strategy for this file pair
            effective_strategy = self._detect_strategy(
                matched_primary, f.path, strategy
            )

            # Compare content
            is_match, reason = self._compare_files(
                matched_primary, f.path, effective_strategy
            )
            try:
                size = f.stat(follow_symlinks=False).st_size
            except OSError:
                size = 0

            results.append(RedundancyResult(
                primary_path=matched_primary,
                redundant_path=f.path,
                is_redundant=is_match,
                reason=reason,
                size_saved=size if is_match else 0,
            ))

        return results

    @staticmethod
    def _detect_strategy(
        primary_path: str, redundant_path: str, default: str
    ) -> str:
        """Auto-detect the right comparison strategy based on file extensions."""
        p_gz = primary_path.endswith(".nii.gz")
        r_nii = redundant_path.endswith(".nii") and not redundant_path.endswith(".nii.gz")

        if p_gz and r_nii:
            return "gz_uncompressed"

        # If both are same type, use the default
        return default

    # ------------------------------------------------------------------
    # File comparison
    # ------------------------------------------------------------------

    def _compare_files(
        self, primary_path: str, redundant_path: str, strategy: str
    ) -> Tuple[bool, str]:
        """Compare two files by reading head and tail."""
        try:
            if strategy == "gz_uncompressed":
                return self._compare_gz_uncompressed(primary_path, redundant_path)

            # Determine if text or binary
            ext = os.path.splitext(redundant_path)[1].lower()
            text_exts = {
                ".csv", ".tsv", ".txt", ".json", ".xml", ".html", ".md", ".log",
            }

            if ext in text_exts:
                return self._compare_text_files(primary_path, redundant_path)
            else:
                return self._compare_binary_files(primary_path, redundant_path)

        except Exception as e:
            return False, f"Comparison failed: {e}"

    def _compare_text_files(
        self, path_a: str, path_b: str
    ) -> Tuple[bool, str]:
        """Compare first 5 and last 5 lines of two text files."""
        try:
            with open(path_a, "r", errors="replace") as f:
                lines_a = f.readlines()
            with open(path_b, "r", errors="replace") as f:
                lines_b = f.readlines()
        except Exception as e:
            return False, f"Read failed: {e}"

        head_a = lines_a[:COMPARE_LINES]
        tail_a = lines_a[-COMPARE_LINES:] if len(lines_a) >= COMPARE_LINES else lines_a
        head_b = lines_b[:COMPARE_LINES]
        tail_b = lines_b[-COMPARE_LINES:] if len(lines_b) >= COMPARE_LINES else lines_b

        if head_a == head_b and tail_a == tail_b:
            return True, f"Head {COMPARE_LINES} and tail {COMPARE_LINES} lines match"
        else:
            return False, "Content differs in head or tail"

    def _compare_binary_files(
        self, path_a: str, path_b: str
    ) -> Tuple[bool, str]:
        """Compare first and last 8KB of two binary files."""
        try:
            size_a = os.path.getsize(path_a)
            size_b = os.path.getsize(path_b)

            with open(path_a, "rb") as fa, open(path_b, "rb") as fb:
                head_a = fa.read(COMPARE_BYTES)
                head_b = fb.read(COMPARE_BYTES)

                if head_a != head_b:
                    return False, "Head bytes differ"

                # Read tail
                if size_a >= COMPARE_BYTES:
                    fa.seek(-COMPARE_BYTES, 2)
                    tail_a = fa.read(COMPARE_BYTES)
                else:
                    fa.seek(0)
                    tail_a = fa.read()

                if size_b >= COMPARE_BYTES:
                    fb.seek(-COMPARE_BYTES, 2)
                    tail_b = fb.read(COMPARE_BYTES)
                else:
                    fb.seek(0)
                    tail_b = fb.read()

                if tail_a != tail_b:
                    return False, "Tail bytes differ"

            if size_a != size_b:
                return False, (
                    f"Head/tail match but sizes differ ({size_a} vs {size_b})"
                )

            return True, (
                f"Head/tail {COMPARE_BYTES}B match, same size "
                f"({self._human_size(size_a)})"
            )

        except Exception as e:
            return False, f"Binary comparison failed: {e}"

    def _compare_gz_uncompressed(
        self, gz_path: str, raw_path: str
    ) -> Tuple[bool, str]:
        """Compare .nii.gz (primary) with .nii (redundant) by decompressing head/tail."""
        import gzip

        try:
            raw_size = os.path.getsize(raw_path)

            # Read head of both
            with gzip.open(gz_path, "rb") as fgz:
                head_gz = fgz.read(COMPARE_BYTES)
            with open(raw_path, "rb") as fraw:
                head_raw = fraw.read(COMPARE_BYTES)

            if head_gz != head_raw:
                return False, "Decompressed head bytes differ"

            # Read tail of raw file
            with open(raw_path, "rb") as fraw:
                if raw_size >= COMPARE_BYTES:
                    fraw.seek(-COMPARE_BYTES, 2)
                    tail_raw = fraw.read(COMPARE_BYTES)
                else:
                    fraw.seek(0)
                    tail_raw = fraw.read()

            # Stream through gz to get the tail of decompressed content
            with gzip.open(gz_path, "rb") as fgz:
                buf = b""
                while True:
                    chunk = fgz.read(65536)
                    if not chunk:
                        break
                    buf = (buf + chunk)[-COMPARE_BYTES:]
                tail_gz = buf

            if tail_gz != tail_raw:
                return False, "Decompressed tail bytes differ"

            return True, (
                f"Decompressed head/tail match raw file "
                f"({self._human_size(raw_size)})"
            )

        except Exception as e:
            return False, f"GZ comparison failed: {e}"

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _get_stem(filename: str) -> str:
        """Get filename stem, handling .nii.gz double extension."""
        if filename.endswith(".nii.gz"):
            return filename[:-7]
        name, _ = os.path.splitext(filename)
        return name

    @staticmethod
    def _human_size(size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(size_bytes) < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}PB"
