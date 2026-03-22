"""Data cataloger skill — incrementally catalog large datasets (UKB, ABCD, HCP, etc.).

Scans one dataset at a time, building a structured inventory:
- Directory structure and organization scheme (by subject / by modality / mixed)
- Data stages (raw / preprocessed / derivative)
- File types, sizes, subject/session counts
- Claude analysis (CLI or API) for classification and reorganization recommendations
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from dm_agent.base_skill import BaseSkill, RunContext, SkillResult
from dm_agent.claude_client import create_client, parse_json_response

logger = logging.getLogger(__name__)


# Heuristic patterns for classification
SUBJECT_PATTERNS = [
    re.compile(r"^sub[_-]?\d+", re.IGNORECASE),
    re.compile(r"^subj?[_-]?\d+", re.IGNORECASE),
    re.compile(r"^\d{4,}", re.IGNORECASE),  # Pure numeric IDs (e.g., UKB)
]

SESSION_PATTERNS = [
    re.compile(r"^ses[_-]?\d+", re.IGNORECASE),
    re.compile(r"^session[_-]?\d+", re.IGNORECASE),
    re.compile(r"^wave[_-]?\d+", re.IGNORECASE),
    re.compile(r"^tp[_-]?\d+", re.IGNORECASE),  # timepoint
]

MODALITY_KEYWORDS = {
    "anat": ["anat", "t1w", "t2w", "flair", "structural", "mprage"],
    "func": ["func", "fmri", "bold", "task", "rest", "resting"],
    "dwi": ["dwi", "dti", "diffusion", "dmri"],
    "eeg": ["eeg", "electroencephalography"],
    "meg": ["meg", "magnetoencephalography"],
    "pet": ["pet", "positron"],
    "fmap": ["fmap", "fieldmap", "field_map"],
    "perf": ["perf", "asl", "perfusion"],
    "micr": ["micr", "microscopy"],
    "beh": ["beh", "behavioral", "behaviour"],
}

RAW_KEYWORDS = ["raw", "source", "sourcedata", "original", "dicom", "nifti_raw"]
PREPROCESS_KEYWORDS = ["preprocess", "preprocessed", "proc", "processed", "cleaned", "fmriprep", "freesurfer", "qsiprep", "mriqc"]
DERIVATIVE_KEYWORDS = ["derivative", "derivatives", "results", "analysis", "output", "stats", "group"]


CATALOG_SYSTEM_PROMPT = """You are a neuroimaging data management expert. You understand BIDS format, common neuroimaging
pipelines (fMRIPrep, FreeSurfer, QSIPrep, MRIQC), and large-scale datasets (UKB, ABCD, HCP, etc.).

Analyze the directory structure of a dataset and provide:
1. How the data is organized (by subject, by modality, mixed, flat)
2. What data stages are present (raw, preprocessed, derivatives)
3. What modalities are present
4. Whether it follows BIDS conventions
5. Specific recommendations for better organization

Respond as JSON:
{
  "organization_scheme": "by_subject | by_modality | mixed | flat | bids",
  "has_raw": true/false,
  "has_preprocessed": true/false,
  "has_derivatives": true/false,
  "bids_compliant": true/false,
  "modalities_found": ["anat", "func", ...],
  "estimated_subjects": <number or null>,
  "summary": "One paragraph describing the dataset structure",
  "issues": ["list of organizational issues found"],
  "recommendations": [
    {
      "priority": "high | medium | low",
      "action": "what to do",
      "reason": "why",
      "example": "optional example of before/after path"
    }
  ]
}
"""


class DataCatalogerSkill(BaseSkill):
    name = "data_cataloger"
    description = "Incrementally catalog large datasets, classify structure, and recommend reorganization"
    phase = "analyze"

    def run(self, context: RunContext) -> SkillResult:
        """Run cataloging for the next uncataloged dataset in the queue."""
        cfg = self.get_config(context.config)
        datasets_config = cfg.get("datasets", [])
        scan_depth = cfg.get("scan_depth", 3)

        if not datasets_config:
            return SkillResult(success=True, message="No datasets configured for cataloging")

        # Find the next dataset to catalog
        target_dataset = None
        for ds in datasets_config:
            ds_name = ds["name"]
            ds_path = ds["path"]
            dataset_id = context.db.get_or_create_dataset(ds_name, ds_path)
            ds_record = context.db.get_dataset(dataset_id)
            if ds_record and ds_record["status"] in ("pending", "scanning"):
                target_dataset = ds
                target_dataset["_id"] = dataset_id
                break

        if target_dataset is None:
            return SkillResult(
                success=True,
                message="All configured datasets have been cataloged",
            )

        ds_name = target_dataset["name"]
        ds_path = target_dataset["path"]
        ds_id = target_dataset["_id"]

        logger.info(f"Cataloging dataset: {ds_name} at {ds_path}")
        context.db.update_dataset_status(ds_id, "scanning")

        try:
            # Phase 1: Scan directory structure
            entries = self._scan_directory(ds_path, scan_depth)
            logger.info(f"  Found {len(entries)} directories")

            # Phase 2: Classify each entry with heuristics
            for entry in entries:
                self._classify_entry(entry)

            # Phase 3: Store entries in DB
            for entry in entries:
                context.db.upsert_catalog_entry(
                    dataset_id=ds_id,
                    rel_path=entry["rel_path"],
                    depth=entry["depth"],
                    entry_type=entry["entry_type"],
                    data_stage=entry["data_stage"],
                    modality=entry.get("modality"),
                    subject_id=entry.get("subject_id"),
                    session_id=entry.get("session_id"),
                    file_count=entry["file_count"],
                    size_bytes=entry["size_bytes"],
                    file_types=json.dumps(entry["file_types"]),
                    sample_files=json.dumps(entry["sample_files"]),
                )

            # Phase 4: Claude API analysis for overall assessment
            analysis = self._analyze_with_claude(
                context, ds_name, ds_path, entries
            )

            # Phase 5: Store summary
            all_modalities = set()
            total_subjects = set()
            total_files = sum(e["file_count"] for e in entries)
            total_size = sum(e["size_bytes"] for e in entries)

            for e in entries:
                if e.get("modality"):
                    all_modalities.add(e["modality"])
                if e.get("subject_id"):
                    total_subjects.add(e["subject_id"])

            context.db.update_dataset_summary(
                dataset_id=ds_id,
                total_size=total_size,
                total_files=total_files,
                total_subjects=len(total_subjects),
                modalities=json.dumps(sorted(all_modalities)),
                organization_scheme=analysis.get("organization_scheme", "unknown"),
                has_raw=analysis.get("has_raw", False),
                has_preprocessed=analysis.get("has_preprocessed", False),
                has_derivatives=analysis.get("has_derivatives", False),
                bids_compliant=analysis.get("bids_compliant", False),
                summary=analysis.get("summary", ""),
                recommendations=json.dumps(analysis.get("recommendations", [])),
            )

            return SkillResult(
                success=True,
                message=f"Cataloged {ds_name}: {len(entries)} dirs, {total_files} files, {self._human_size(total_size)}",
                data={
                    "dataset": ds_name,
                    "dirs": len(entries),
                    "files": total_files,
                    "size": total_size,
                    "subjects": len(total_subjects),
                    "modalities": sorted(all_modalities),
                },
            )

        except Exception as e:
            logger.error(f"Failed to catalog {ds_name}: {e}", exc_info=True)
            context.db.update_dataset_status(ds_id, "error")
            return SkillResult(success=False, message=f"Catalog failed for {ds_name}: {e}")

    def _scan_directory(self, root_path: str, max_depth: int) -> List[Dict[str, Any]]:
        """Scan directory tree and collect metadata for each subdirectory."""
        entries = []

        for depth in range(1, max_depth + 1):
            # Get directories at this depth
            cmd = [
                "find", root_path,
                "-mindepth", str(depth),
                "-maxdepth", str(depth),
                "-type", "d",
            ]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                dirs = [d for d in result.stdout.strip().split("\n") if d]
            except (subprocess.TimeoutExpired, Exception) as e:
                logger.warning(f"find at depth {depth} failed: {e}")
                continue

            for dir_path in dirs:
                rel_path = os.path.relpath(dir_path, root_path)
                entry = {
                    "abs_path": dir_path,
                    "rel_path": rel_path,
                    "depth": depth,
                    "dir_name": os.path.basename(dir_path),
                    "entry_type": "other",
                    "data_stage": "unknown",
                    "modality": None,
                    "subject_id": None,
                    "session_id": None,
                    "file_count": 0,
                    "size_bytes": 0,
                    "file_types": [],
                    "sample_files": [],
                }

                # Get file stats for this directory (non-recursive, just this level)
                self._collect_file_stats(entry)
                entries.append(entry)

        return entries

    def _collect_file_stats(self, entry: Dict[str, Any]) -> None:
        """Collect file count, size, types for a single directory level."""
        dir_path = entry["abs_path"]
        extensions = Counter()
        sample = []
        total_size = 0
        file_count = 0

        try:
            for f in os.scandir(dir_path):
                if not f.is_file(follow_symlinks=False):
                    continue
                file_count += 1
                ext = os.path.splitext(f.name)[1].lower()
                if ext:
                    extensions[ext] += 1
                try:
                    total_size += f.stat(follow_symlinks=False).st_size
                except OSError:
                    pass
                if len(sample) < 20:
                    sample.append(f.name)
        except PermissionError:
            pass

        entry["file_count"] = file_count
        entry["size_bytes"] = total_size
        entry["file_types"] = [f"{ext}({cnt})" for ext, cnt in extensions.most_common(10)]
        entry["sample_files"] = sample

    def _classify_entry(self, entry: Dict[str, Any]) -> None:
        """Classify a directory entry using heuristic rules."""
        name = entry["dir_name"].lower()
        rel_parts = entry["rel_path"].lower().split(os.sep)

        # Check if it's a subject directory
        for pattern in SUBJECT_PATTERNS:
            if pattern.match(entry["dir_name"]):
                entry["entry_type"] = "subject_dir"
                entry["subject_id"] = entry["dir_name"]
                break

        # Check if it's a session directory
        for pattern in SESSION_PATTERNS:
            if pattern.match(entry["dir_name"]):
                entry["entry_type"] = "session_dir"
                entry["session_id"] = entry["dir_name"]
                # Inherit subject from parent
                if entry["depth"] > 1:
                    parent = os.path.basename(os.path.dirname(entry["abs_path"]))
                    for sp in SUBJECT_PATTERNS:
                        if sp.match(parent):
                            entry["subject_id"] = parent
                            break
                break

        # Check if it's a modality directory
        for modality, keywords in MODALITY_KEYWORDS.items():
            if any(kw in name for kw in keywords):
                entry["entry_type"] = "modality_dir"
                entry["modality"] = modality
                break

        # Classify data stage
        if any(kw in name for kw in RAW_KEYWORDS):
            entry["data_stage"] = "raw"
        elif any(kw in name for kw in PREPROCESS_KEYWORDS):
            entry["data_stage"] = "preprocessed"
        elif any(kw in name for kw in DERIVATIVE_KEYWORDS):
            entry["data_stage"] = "derivative"
        else:
            # Infer from path components
            for part in rel_parts:
                if any(kw in part for kw in RAW_KEYWORDS):
                    entry["data_stage"] = "raw"
                    break
                elif any(kw in part for kw in PREPROCESS_KEYWORDS):
                    entry["data_stage"] = "preprocessed"
                    break
                elif any(kw in part for kw in DERIVATIVE_KEYWORDS):
                    entry["data_stage"] = "derivative"
                    break

        # Infer modality from file extensions if not already set
        if not entry["modality"] and entry["file_types"]:
            ext_str = " ".join(entry["file_types"]).lower()
            if ".nii" in ext_str or ".nii.gz" in ext_str:
                # Could be any neuroimaging modality
                file_names = " ".join(entry["sample_files"]).lower()
                for modality, keywords in MODALITY_KEYWORDS.items():
                    if any(kw in file_names for kw in keywords):
                        entry["modality"] = modality
                        break
            elif ".edf" in ext_str or ".bdf" in ext_str or ".set" in ext_str:
                entry["modality"] = "eeg"
            elif ".fif" in ext_str:
                entry["modality"] = "meg"

    def _analyze_with_claude(
        self,
        context: RunContext,
        ds_name: str,
        ds_path: str,
        entries: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Use Claude (CLI or API) to analyze the dataset structure."""
        cfg = self.get_config(context.config)
        client = create_client(context.config.analyzer)

        # Build a concise structure summary for the prompt
        tree_lines = []
        by_depth: Dict[int, List[Dict]] = defaultdict(list)
        for e in entries:
            by_depth[e["depth"]].append(e)

        for depth in sorted(by_depth.keys()):
            depth_entries = by_depth[depth]
            indent = "  " * depth
            shown = depth_entries[:30]
            for e in shown:
                flags = []
                if e["entry_type"] != "other":
                    flags.append(e["entry_type"])
                if e["data_stage"] != "unknown":
                    flags.append(e["data_stage"])
                if e["modality"]:
                    flags.append(e["modality"])
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                files_str = f" ({e['file_count']} files, {self._human_size(e['size_bytes'])})"
                tree_lines.append(f"{indent}{e['rel_path']}{flag_str}{files_str}")

            if len(depth_entries) > 30:
                tree_lines.append(f"{indent}... and {len(depth_entries) - 30} more directories at depth {depth}")

        type_counts = Counter(e["entry_type"] for e in entries)
        stage_counts = Counter(e["data_stage"] for e in entries)

        prompt = f"""Dataset: {ds_name}
Path: {ds_path}
Total directories scanned: {len(entries)}
Directory type distribution: {dict(type_counts)}
Data stage distribution: {dict(stage_counts)}

Directory tree (with heuristic classification):
{chr(10).join(tree_lines)}

Lab context:
{yaml.dump(context.lab_context, default_flow_style=False)}

Please analyze this dataset structure and provide recommendations for better organization.
"""

        try:
            text = client.ask(CATALOG_SYSTEM_PROMPT, prompt)
            return parse_json_response(text)

        except Exception as e:
            logger.error(f"Claude analysis failed for {ds_name}: {e}")
            return {
                "organization_scheme": "unknown",
                "has_raw": any(e["data_stage"] == "raw" for e in entries),
                "has_preprocessed": any(e["data_stage"] == "preprocessed" for e in entries),
                "has_derivatives": any(e["data_stage"] == "derivative" for e in entries),
                "bids_compliant": False,
                "modalities_found": list(set(e["modality"] for e in entries if e["modality"])),
                "summary": f"Heuristic analysis only (Claude failed: {e})",
                "recommendations": [],
            }

    @staticmethod
    def _human_size(size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(size_bytes) < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}PB"
