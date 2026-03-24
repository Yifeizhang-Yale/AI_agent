"""Data cataloger skill — incrementally catalog large datasets (UKB, ABCD, HCP, etc.).

Scans one dataset at a time, building a structured inventory:
- Directory structure and organization scheme (by subject / by modality / mixed)
- Data stages (raw / preprocessed / derivative)
- File types, sizes, subject/session counts
- Per-modality breakdown with subject counts extracted from filenames
- Claude analysis (CLI or API) for classification and reorganization recommendations
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

# Optional imports for .mat file metadata
try:
    import scipy.io as sio
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

try:
    import h5py
    HAS_H5PY = True
except ImportError:
    HAS_H5PY = False

from dm_agent.base_skill import BaseSkill, RunContext, SkillResult
from dm_agent.claude_client import create_client, parse_json_response

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Heuristic patterns for classification
# ---------------------------------------------------------------------------

# Directory-level subject patterns
SUBJECT_DIR_PATTERNS = [
    re.compile(r"^sub[_-]?\w+", re.IGNORECASE),           # BIDS: sub-01, sub_01
    re.compile(r"^subj?[_-]?\d+", re.IGNORECASE),         # subj01
    re.compile(r"^\d{4,}$"),                                # Pure numeric: 100206 (HCP-YA)
    re.compile(r"^HCA\d+$", re.IGNORECASE),                # HCP-Aging
    re.compile(r"^HCD\d+$", re.IGNORECASE),                # HCP-Development
    re.compile(r"^NDAR\w+$", re.IGNORECASE),               # ABCD: NDARINV00CY2MDM
    re.compile(r"^\d{3}_S_\d{4}$"),                         # ADNI: 002_S_1155
]

# Filename-level subject ID extraction patterns
# Each returns a regex with a named group 'sid'
SUBJECT_FILE_PATTERNS = [
    # BIDS: sub-01_task-rest_bold.nii.gz
    re.compile(r"(?:^|[_/])(?P<sid>sub-\w+?)(?:_|\.)", re.IGNORECASE),
    # HCP-YA: 100206_GAMBLING_GSR_matrix.txt
    re.compile(r"^(?P<sid>\d{6})_"),
    # HCP-A: HCA6000030_bis_matrix...
    re.compile(r"^(?P<sid>HCA\d{7})_", re.IGNORECASE),
    # HCP-D: HCD0001305
    re.compile(r"^(?P<sid>HCD\d{7})_", re.IGNORECASE),
    # ABCD NDAR: NDARINV00CY2MDM
    re.compile(r"(?P<sid>NDARINV\w{8})", re.IGNORECASE),
    # ADNI: 002_S_1155 embedded
    re.compile(r"(?P<sid>\d{3}_S_\d{4})"),
    # A4 style: B10081264 embedded in filename
    re.compile(r"(?:^|_)(?P<sid>B\d{8})(?:_|\.)", re.IGNORECASE),
    # IMAGEN style: 000000001274_GSR...
    re.compile(r"^(?P<sid>\d{12})_"),
    # Generic sub-XXXXX in filename
    re.compile(r"(?P<sid>sub-[A-Za-z0-9]+)"),
]

def _normalize_subject_id(sid: str) -> str:
    """Normalize subject ID variants to a canonical form.

    Handles ABCD NDAR IDs that appear in multiple formats:
      sub-NDARINV00CY2MDM  →  NDAR_INV00CY2MDM
      NDARINV00CY2MDM      →  NDAR_INV00CY2MDM
      sub-NDARINV00CY2MDM_realign  →  NDAR_INV00CY2MDM
    """
    s = sid.strip()
    # Strip BIDS sub- prefix
    if s.lower().startswith("sub-"):
        s = s[4:]
    # Strip common suffixes like _realign
    s = re.sub(r"_(realign|resampled|filtered|cleaned)$", "", s, flags=re.IGNORECASE)
    # Normalize NDARINVXXXXXXXX → NDAR_INVXXXXXXXX
    m = re.match(r"^NDAR(?:_)?INV([A-Z0-9]{8})$", s, re.IGNORECASE)
    if m:
        s = f"NDAR_INV{m.group(1).upper()}"
    return s


SESSION_PATTERNS = [
    re.compile(r"^ses[_-]?\d+", re.IGNORECASE),
    re.compile(r"^session[_-]?\d+", re.IGNORECASE),
    re.compile(r"^wave[_-]?\d+", re.IGNORECASE),
    re.compile(r"^tp[_-]?\d+", re.IGNORECASE),
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

# Extended keywords for file-level modality inference (file/variable names)
FILE_MODALITY_KEYWORDS = {
    "anat": ["vbm", "t1w", "t2w", "flair", "structural", "mprage", "freesurfer",
             "brain_volume", "cortical_thickness", "grey_matter"],
    "func": ["fmri", "bold", "conn_mat", "connectivity", "resting", "rest",
             "functional", "task_"],
    "dwi": ["dti", "diffusion", "dmri", "structural_connectivity", "sc_",
            "tractography", "connectome"],
    "pet": ["pet", "fdg", "av45", "av1451", "amyloid", "tau", "suvr",
            "florbetapir", "flortaucipir", "tracer", "apet"],
    "mri": ["mrisbm", "mriqc", "mri_scan", "scan_mri"],
    "csf": ["csf", "biomarker", "lumbar", "fcsf"],
    "clinical": ["clinical", "uds", "ftld", "lbd", "diagnosis", "dementia",
                 "cognition", "neuropsych", "ftldlbd"],
    "genetic": ["genetic", "genotype", "imputed", "plink", "gwas", "snp",
                "prs", "polygenic", "allele", "apoe"],
    "eeg": ["eeg", "electroencephalography"],
    "meg": ["meg", "magnetoencephalography"],
    "perf": ["perf", "asl", "perfusion"],
    "beh": ["beh", "behavioral", "behaviour"],
}


# Filename patterns known to be false positives for modality inference.
# These are behavioural / survey CSVs whose names accidentally contain
# imaging-modality keywords (e.g. "ce_y_pet" = Peer Experience Tracker,
# NOT PET imaging).
_MODALITY_FALSE_POSITIVE_PATTERNS = [
    re.compile(r"^ce_[yp]_pet", re.IGNORECASE),    # ABCD culture-environment survey
]


def _infer_modality_from_name(name: str) -> Optional[str]:
    """Infer modality from a filename or variable name."""
    name_lower = name.lower()
    # Reject known false positives first
    for fp in _MODALITY_FALSE_POSITIVE_PATTERNS:
        if fp.search(name_lower):
            return None
    for modality, keywords in FILE_MODALITY_KEYWORDS.items():
        if any(kw in name_lower for kw in keywords):
            return modality
    return None

# CSV column names that likely contain subject/participant IDs
# Ordered by specificity — first match wins
SUBJECT_ID_COLUMNS = [
    re.compile(r"^(NACCID|PTID|src_subject_id|subjectkey)$", re.IGNORECASE),
    re.compile(r"^(participant_id|subject_id|SubjID|PatientID)$", re.IGNORECASE),
    re.compile(r"^(RID|SUBJID|PIDN|SCRNO)$", re.IGNORECASE),            # ADNI, clinical
    re.compile(r"^(eid|f\.eid)$", re.IGNORECASE),                         # UK Biobank
    re.compile(r"^(NDAR\w*|GUID)$", re.IGNORECASE),                       # NDA / ABCD
    re.compile(r"^(ID|SubID|Sub_ID|Subj_ID|Subject)$", re.IGNORECASE),    # generic
]

RAW_KEYWORDS = ["raw", "source", "sourcedata", "original", "dicom", "nifti_raw"]
PREPROCESS_KEYWORDS = ["preprocess", "preprocessed", "proc", "processed", "cleaned", "fmriprep", "freesurfer", "qsiprep", "mriqc"]
DERIVATIVE_KEYWORDS = ["derivative", "derivatives", "results", "analysis", "output", "stats", "group"]


CATALOG_SYSTEM_PROMPT = """You are a neuroimaging data management expert. You understand BIDS format, common neuroimaging
pipelines (fMRIPrep, FreeSurfer, QSIPrep, MRIQC), and large-scale datasets (UKB, ABCD, HCP, etc.).

Analyze the directory structure of a dataset and provide a DETAILED assessment:

1. How the data is organized (by subject, by modality, mixed, flat)
2. What data stages are present (raw, preprocessed, derivatives)
3. What modalities are present WITH per-modality details
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
  "modality_details": [
    {
      "modality": "func",
      "description": "resting-state fMRI, task fMRI (MID, SST, nBack)",
      "estimated_subjects": 1000,
      "data_stages": ["raw", "preprocessed"],
      "file_formats": [".nii.gz", ".csv"],
      "key_directories": ["path/to/func/data"],
      "notes": "any special notes"
    }
  ],
  "pipelines_detected": ["fMRIPrep", "FreeSurfer", "custom"],
  "summary": "Detailed paragraph describing the dataset structure, contents, and notable features",
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

            # Phase 3: Extract subject IDs from filenames
            self._extract_subjects_from_files(entries)

            # Phase 3b: Extract subject IDs from CSV/TSV column headers
            self._extract_subjects_from_csvs(entries)

            # Phase 3c: Extract subject counts from .mat file metadata
            self._extract_subjects_from_mats(entries)

            # Phase 3d: Find README content for Claude analysis
            readme_content = self._find_readme_content(ds_path)

            # Phase 4: Store entries in DB
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

            # Phase 5: Build per-modality statistics
            modality_stats = self._compute_modality_stats(entries)

            # Phase 6: Claude API analysis for overall assessment
            analysis = self._analyze_with_claude(
                context, ds_name, ds_path, entries, modality_stats, readme_content
            )

            # Phase 7: Store summary
            all_modalities = set()
            total_subjects = set()
            total_files = sum(e["file_count"] for e in entries)
            total_size = sum(e["size_bytes"] for e in entries)

            for e in entries:
                if e.get("modality"):
                    all_modalities.add(e["modality"])
                if e.get("subject_id"):
                    total_subjects.add(e["subject_id"])
                # Also count subjects from file-level extraction
                for sid in e.get("file_subjects", set()):
                    total_subjects.add(sid)

            # Check .mat-derived subject counts as additional signal
            mat_subject_estimates = [
                e["mat_subjects"] for e in entries if e.get("mat_subjects", 0) > 0
            ]
            mat_max = max(mat_subject_estimates) if mat_subject_estimates else 0
            heuristic_count = max(len(total_subjects), mat_max)

            # Use Claude's estimate if our heuristic found none
            if heuristic_count == 0 and analysis.get("estimated_subjects"):
                est = analysis["estimated_subjects"]
                if isinstance(est, int) and est > 0:
                    total_subjects_count = est
                else:
                    total_subjects_count = 0
            else:
                total_subjects_count = heuristic_count

            # Build detailed modality info JSON
            modality_details = analysis.get("modality_details", [])
            # Merge our computed stats with Claude's analysis
            merged_details = self._merge_modality_details(
                modality_stats, modality_details
            )

            # Phase 7b: Persist per-modality stats (subject lists) to DB
            context.db.delete_modality_stats(ds_id)
            for m in merged_details:
                context.db.upsert_modality_stats(
                    dataset_id=ds_id,
                    modality=m["modality"],
                    file_count=m.get("files", 0),
                    size_bytes=m.get("size_bytes", 0),
                    subject_count=m.get("subjects", 0),
                    subject_list=json.dumps(m.get("subject_list", [])),
                    dir_count=m.get("dirs", 0),
                    data_stages=json.dumps(m.get("data_stages", [])),
                    key_dirs=json.dumps(m.get("key_dirs", [])),
                    description=m.get("description", ""),
                    notes=m.get("notes", ""),
                )

            context.db.update_dataset_summary(
                dataset_id=ds_id,
                total_size=total_size,
                total_files=total_files,
                total_subjects=total_subjects_count,
                modalities=json.dumps(sorted(all_modalities)),
                organization_scheme=analysis.get("organization_scheme", "unknown"),
                has_raw=analysis.get("has_raw", False),
                has_preprocessed=analysis.get("has_preprocessed", False),
                has_derivatives=analysis.get("has_derivatives", False),
                bids_compliant=analysis.get("bids_compliant", False),
                summary=analysis.get("summary", ""),
                recommendations=json.dumps({
                    "recommendations": analysis.get("recommendations", []),
                    "modality_details": merged_details,
                    "pipelines_detected": analysis.get("pipelines_detected", []),
                    "issues": analysis.get("issues", []),
                    "readme_content": readme_content[:5000] if readme_content else "",
                }),
            )

            # Build result message
            mod_summary = []
            for m in merged_details:
                s = f"{m['modality']}: {m.get('subjects', '?')} subjects, {m.get('files', 0)} files"
                mod_summary.append(s)

            msg = (
                f"Cataloged {ds_name}: {len(entries)} dirs, {total_files} files, "
                f"{self._human_size(total_size)}, {total_subjects_count} subjects"
            )
            if mod_summary:
                msg += "\n  " + "\n  ".join(mod_summary)

            return SkillResult(
                success=True,
                message=msg,
                data={
                    "dataset": ds_name,
                    "dirs": len(entries),
                    "files": total_files,
                    "size": total_size,
                    "subjects": total_subjects_count,
                    "modalities": sorted(all_modalities),
                    "modality_details": merged_details,
                },
            )

        except Exception as e:
            logger.error(f"Failed to catalog {ds_name}: {e}", exc_info=True)
            context.db.update_dataset_status(ds_id, "error")
            return SkillResult(success=False, message=f"Catalog failed for {ds_name}: {e}")

    # ------------------------------------------------------------------
    # Scanning
    # ------------------------------------------------------------------

    def _scan_directory(self, root_path: str, max_depth: int) -> List[Dict[str, Any]]:
        """Scan directory tree and collect metadata for each subdirectory.

        Uses a two-pass strategy:
        1. List all directories at every depth.
        2. Classify each directory name to detect subject dirs.
        3. If the dataset has many subject directories (≥100), use
           *lightweight mode* for subject dirs and their children —
           only count files and total size, skip per-file parsing
           (CSV/mat reads, filename subject extraction).
        """
        # --- Pass 1: list all dirs at every depth ----------------------
        all_dirs_by_depth: Dict[int, List[str]] = {}
        for depth in range(1, max_depth + 1):
            dir_list = self._list_dirs_at_depth(root_path, depth)
            all_dirs_by_depth[depth] = dir_list
            # Stop early if no dirs at this depth
            if not dir_list:
                break

        # Count how many directories match subject patterns
        subject_dir_names: Set[str] = set()
        for depth, dir_list in all_dirs_by_depth.items():
            for dir_path in dir_list:
                name = os.path.basename(dir_path)
                for pat in SUBJECT_DIR_PATTERNS:
                    if pat.match(name):
                        subject_dir_names.add(name)
                        break

        use_lightweight = len(subject_dir_names) >= 100
        if use_lightweight:
            logger.info(
                f"  Detected {len(subject_dir_names)} subject directories "
                f"— using lightweight scan for subject subtrees"
            )

        # --- Pass 2: build entries ------------------------------------
        entries = []
        for depth in sorted(all_dirs_by_depth.keys()):
            for dir_path in all_dirs_by_depth[depth]:
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
                    "file_subjects": set(),
                }

                # Decide lightweight vs full scan
                if use_lightweight:
                    is_subj_dir = any(
                        pat.match(entry["dir_name"]) for pat in SUBJECT_DIR_PATTERNS
                    )
                    in_subject_subtree = any(
                        any(pat.match(part) for pat in SUBJECT_DIR_PATTERNS)
                        for part in rel_path.split(os.sep)[:-1]
                    )
                    if is_subj_dir or in_subject_subtree:
                        self._collect_file_stats_lightweight(entry)
                    else:
                        self._collect_file_stats(entry)
                else:
                    self._collect_file_stats(entry)

                entries.append(entry)

        return entries

    @staticmethod
    def _list_dirs_at_depth(root_path: str, depth: int) -> List[str]:
        """List directories at a specific depth under root_path."""
        cmd = [
            "find", root_path,
            "-mindepth", str(depth),
            "-maxdepth", str(depth),
            "-type", "d",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            return [d for d in result.stdout.strip().split("\n") if d]
        except (subprocess.TimeoutExpired, Exception) as e:
            logger.warning(f"find at depth {depth} failed: {e}")
            return []

    @staticmethod
    def _collect_file_stats_lightweight(entry: Dict[str, Any]) -> None:
        """Fast file stats for subject directories — count and size only, no per-file parsing."""
        dir_path = entry["abs_path"]
        total_size = 0
        file_count = 0
        extensions = Counter()
        try:
            for f in os.scandir(dir_path):
                if not f.is_file(follow_symlinks=False):
                    continue
                file_count += 1
                ext = os.path.splitext(f.name)[1].lower()
                if f.name.endswith(".nii.gz"):
                    ext = ".nii.gz"
                if ext:
                    extensions[ext] += 1
                try:
                    total_size += f.stat(follow_symlinks=False).st_size
                except OSError:
                    pass
        except PermissionError:
            pass
        entry["file_count"] = file_count
        entry["size_bytes"] = total_size
        entry["file_types"] = [f"{ext}({cnt})" for ext, cnt in extensions.most_common(5)]

    def _collect_file_stats(self, entry: Dict[str, Any]) -> None:
        """Collect file count, size, types, and extract subject IDs from ALL filenames."""
        dir_path = entry["abs_path"]
        extensions = Counter()
        sample = []
        total_size = 0
        file_count = 0
        all_subjects: Set[str] = set()
        # Per-modality subjects inferred from filenames (not CSV content)
        fname_modality_subjects: Dict[str, Set[str]] = {}

        try:
            for f in os.scandir(dir_path):
                if not f.is_file(follow_symlinks=False):
                    continue
                file_count += 1
                ext = os.path.splitext(f.name)[1].lower()
                if f.name.endswith(".nii.gz"):
                    ext = ".nii.gz"
                if ext:
                    extensions[ext] += 1
                try:
                    total_size += f.stat(follow_symlinks=False).st_size
                except OSError:
                    pass
                if len(sample) < 20:
                    sample.append(f.name)
                # Extract subject ID from EVERY file, not just samples
                sid = self._extract_subject_id(f.name)
                if sid:
                    nsid = _normalize_subject_id(sid)
                    all_subjects.add(nsid)
                    # Also infer modality from the filename so that subjects
                    # from per-subject data files (e.g. NDARINV...-rsfMRI.csv)
                    # are attributed to the correct modality even when the CSV
                    # has no ID column header.  Strip the raw subject ID from
                    # the filename first to avoid false positives from IDs that
                    # accidentally contain modality keywords (e.g.
                    # NDARINVPETWZ0JC matching "pet").
                    clean_name = f.name.replace(sid, "")
                    fmod = _infer_modality_from_name(clean_name)
                    if fmod:
                        fname_modality_subjects.setdefault(fmod, set()).add(nsid)
        except PermissionError:
            pass

        entry["file_count"] = file_count
        entry["size_bytes"] = total_size
        entry["file_types"] = [f"{ext}({cnt})" for ext, cnt in extensions.most_common(10)]
        entry["sample_files"] = sample
        entry["file_subjects"] = all_subjects
        # Seed file_modality_subjects from filename-inferred modalities
        if fname_modality_subjects:
            fms = entry.setdefault("file_modality_subjects", {})
            for fmod, sids in fname_modality_subjects.items():
                fms.setdefault(fmod, set()).update(sids)

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def _classify_entry(self, entry: Dict[str, Any]) -> None:
        """Classify a directory entry using heuristic rules."""
        name = entry["dir_name"].lower()
        rel_parts = entry["rel_path"].lower().split(os.sep)

        # Check if it's a subject directory
        for pattern in SUBJECT_DIR_PATTERNS:
            if pattern.match(entry["dir_name"]):
                entry["entry_type"] = "subject_dir"
                entry["subject_id"] = _normalize_subject_id(entry["dir_name"])
                break

        # Check if it's a session directory
        for pattern in SESSION_PATTERNS:
            if pattern.match(entry["dir_name"]):
                entry["entry_type"] = "session_dir"
                entry["session_id"] = entry["dir_name"]
                if entry["depth"] > 1:
                    parent = os.path.basename(os.path.dirname(entry["abs_path"]))
                    for sp in SUBJECT_DIR_PATTERNS:
                        if sp.match(parent):
                            entry["subject_id"] = _normalize_subject_id(parent)
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

        # Infer modality from file extensions/names if not already set
        if not entry["modality"] and entry["file_types"]:
            ext_str = " ".join(entry["file_types"]).lower()
            if ".nii" in ext_str or ".nii.gz" in ext_str:
                file_names = " ".join(entry["sample_files"]).lower()
                for modality, keywords in MODALITY_KEYWORDS.items():
                    if any(kw in file_names for kw in keywords):
                        entry["modality"] = modality
                        break
            elif ".edf" in ext_str or ".bdf" in ext_str or ".set" in ext_str:
                entry["modality"] = "eeg"
            elif ".fif" in ext_str:
                entry["modality"] = "meg"

    def _extract_subjects_from_files(self, entries: List[Dict[str, Any]]) -> None:
        """Post-process: set dir-level subject_id if only one subject found in files."""
        for entry in entries:
            subjects = entry.get("file_subjects", set())
            # If this directory has exactly one subject from files but no dir-level subject_id,
            # set it (useful for subject-level directories)
            if subjects and not entry.get("subject_id") and len(subjects) == 1:
                entry["subject_id"] = next(iter(subjects))

    @staticmethod
    def _extract_subject_id(filename: str) -> Optional[str]:
        """Try to extract a subject ID from a filename."""
        for pattern in SUBJECT_FILE_PATTERNS:
            m = pattern.search(filename)
            if m:
                return m.group("sid")
        return None

    def _extract_subjects_from_csvs(self, entries: List[Dict[str, Any]]) -> None:
        """Scan CSV/TSV files for ID-like columns and count unique subjects.

        For each directory entry, find CSV/TSV files, read their headers, and if
        an ID-like column is found, collect unique values as subject IDs.  Results
        are merged into ``entry["file_subjects"]`` and a per-entry
        ``csv_id_column`` field records which column was matched.
        """
        for entry in entries:
            csv_files = [
                f for f in entry.get("sample_files", [])
                if f.lower().endswith((".csv", ".tsv", ".txt"))
            ]
            if not csv_files:
                continue

            dir_path = entry["abs_path"]
            # Scan ALL csv/tsv files in the directory (sample_files is capped at 20)
            try:
                all_tabular = [
                    f.name for f in os.scandir(dir_path)
                    if f.is_file(follow_symlinks=False)
                    and f.name.lower().endswith((".csv", ".tsv", ".txt"))
                ]
            except PermissionError:
                continue

            for fname in all_tabular:
                fpath = os.path.join(dir_path, fname)
                id_col, raw_subjects = self._read_csv_id_column(fpath)
                subjects = {_normalize_subject_id(s) for s in raw_subjects} if raw_subjects else set()
                if id_col and subjects:
                    entry.setdefault("file_subjects", set()).update(subjects)
                    entry["csv_id_column"] = id_col
                    logger.debug(
                        f"  CSV {fname}: column '{id_col}' → {len(subjects)} unique IDs"
                    )
                    # Infer modality from CSV filename — store actual IDs
                    # Fall back to directory-level modality if filename gives no hint
                    mod = _infer_modality_from_name(fname) or entry.get("modality")
                    if mod:
                        fms = entry.setdefault("file_modality_subjects", {})
                        fms.setdefault(mod, set()).update(subjects)

    @staticmethod
    def _read_csv_id_column(
        filepath: str, max_rows: int = 500_000
    ) -> Tuple[Optional[str], Set[str]]:
        """Read a CSV/TSV, find an ID column, return (column_name, unique_ids).

        Returns ``(None, set())`` if no ID column is detected.
        """
        try:
            # Detect delimiter from extension
            is_tsv = filepath.lower().endswith(".tsv")
            delimiter = "\t" if is_tsv else ","

            with open(filepath, newline="", encoding="utf-8", errors="replace") as fh:
                reader = csv.reader(fh, delimiter=delimiter)
                try:
                    header = next(reader)
                except StopIteration:
                    return None, set()

                # Find first matching ID column
                id_col_idx: Optional[int] = None
                id_col_name: Optional[str] = None
                for pattern in SUBJECT_ID_COLUMNS:
                    for idx, col in enumerate(header):
                        if pattern.match(col.strip()):
                            id_col_idx = idx
                            id_col_name = col.strip()
                            break
                    if id_col_idx is not None:
                        break

                if id_col_idx is None:
                    return None, set()

                # Collect unique IDs — only count a subject if at least one
                # non-ID column has a non-empty value.  This avoids inflating
                # counts in wide phenotypic CSVs (e.g. UKB) where every
                # participant has a row but most columns are empty for subjects
                # without data for that modality.
                _null_vals = {"", "na", "nan", "null", "."}
                n_cols = len(header)
                ids: Set[str] = set()
                for i, row in enumerate(reader):
                    if i >= max_rows:
                        break
                    if id_col_idx < len(row):
                        val = row[id_col_idx].strip()
                        if not val or val.lower() in _null_vals:
                            continue
                        # Check that at least one non-ID column is non-empty
                        has_data = False
                        for j, cell in enumerate(row):
                            if j == id_col_idx:
                                continue
                            if cell.strip() and cell.strip().lower() not in _null_vals:
                                has_data = True
                                break
                        if has_data:
                            ids.add(val)

                return id_col_name, ids

        except Exception as e:
            logger.debug(f"  Could not read CSV {filepath}: {e}")
            return None, set()

    # ------------------------------------------------------------------
    # .mat file metadata extraction
    # ------------------------------------------------------------------

    def _extract_subjects_from_mats(self, entries: List[Dict[str, Any]]) -> None:
        """Extract subject counts from .mat files using metadata-only reads.

        For v5 .mat: uses ``scipy.io.whosmat`` (reads headers only).
        For v7.3 .mat (HDF5): uses ``h5py`` to read dataset shapes.

        Looks for variables with ``_id`` suffix — their length = subject count.
        If no ``_id`` variable found, uses first dimension of the largest 2D+
        array as the estimate.
        """
        if not HAS_SCIPY and not HAS_H5PY:
            logger.debug("Neither scipy nor h5py available; skipping .mat metadata extraction")
            return

        for entry in entries:
            mat_files = [
                f for f in entry.get("sample_files", [])
                if f.lower().endswith(".mat")
            ]
            if not mat_files:
                continue

            dir_path = entry["abs_path"]
            best_subject_count = 0
            all_metadata: List[Tuple[str, str, tuple]] = []
            # Per-modality subject counts derived from .mat files
            modality_counts: Dict[str, int] = {}

            try:
                mat_paths = [
                    os.path.join(dir_path, f.name)
                    for f in os.scandir(dir_path)
                    if f.is_file(follow_symlinks=False)
                    and f.name.lower().endswith(".mat")
                ]
            except PermissionError:
                continue

            for fpath in mat_paths[:10]:
                var_info: List[Tuple[str, tuple]] = []

                # Try scipy first (v5 format)
                if HAS_SCIPY:
                    try:
                        whos = sio.whosmat(fpath)
                        var_info = [(name, shape) for name, shape, _ in whos]
                    except Exception:
                        pass

                # If scipy didn't work (v7.3/HDF5), try h5py
                if not var_info and HAS_H5PY:
                    try:
                        with h5py.File(fpath, "r") as hf:
                            for key in hf.keys():
                                if key.startswith("#"):
                                    continue
                                item = hf[key]
                                if hasattr(item, "shape"):
                                    var_info.append((key, item.shape))
                    except Exception:
                        pass

                if not var_info:
                    continue

                fname = os.path.basename(fpath)
                all_metadata.extend([(fname, n, s) for n, s in var_info])

                # Determine subject count for this .mat file
                file_count = 0
                has_id_var = False

                # Strategy 1: Look for variables with '_id' suffix
                for vname, shape in var_info:
                    if vname.lower().endswith("_id") and len(shape) >= 1:
                        count = max(shape)
                        if count > file_count:
                            file_count = count
                            has_id_var = True
                            logger.debug(
                                f"  .mat {fname}: var '{vname}' shape {shape} → "
                                f"{count} subject IDs"
                            )

                # Infer modality from filename or variable names
                mod = _infer_modality_from_name(fname)
                if not mod:
                    for vname, _ in var_info:
                        mod = _infer_modality_from_name(vname)
                        if mod:
                            break

                # Only update best_subject_count from confirmed _id variables
                if has_id_var and file_count > best_subject_count:
                    best_subject_count = file_count

                if mod and file_count > 0:
                    # .mat: no individual IDs, store count as int
                    modality_counts[mod] = max(modality_counts.get(mod, 0), file_count)

            if best_subject_count > 0:
                entry["mat_subjects"] = best_subject_count
            if modality_counts:
                # Merge into file_modality_subjects — keep sets if present, add int counts
                fms = entry.setdefault("file_modality_subjects", {})
                for mod, count in modality_counts.items():
                    if mod not in fms or not isinstance(fms[mod], set):
                        fms[mod] = count  # int = count-only from .mat
            if all_metadata:
                entry["mat_metadata"] = [
                    {"file": fn, "var": vn, "shape": list(sh)}
                    for fn, vn, sh in all_metadata[:50]
                ]

    # ------------------------------------------------------------------
    # README extraction
    # ------------------------------------------------------------------

    def _find_readme_content(self, root_path: str) -> str:
        """Find and read README files in the dataset root and immediate subdirectories.

        Returns concatenated content (capped at 15 000 chars) for inclusion in
        the Claude analysis prompt.
        """
        readme_names = [
            "README.md", "README.txt", "README.rst", "README",
            "readme.md", "readme.txt", "Readme.md",
        ]

        contents: List[str] = []

        def _try_read(dirpath: str, label: str) -> None:
            for name in readme_names:
                fpath = os.path.join(dirpath, name)
                if os.path.isfile(fpath):
                    try:
                        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                            text = f.read(20_000)
                        contents.append(f"=== {label}/{name} ===\n{text}")
                        logger.debug(f"  Found README: {fpath} ({len(text)} chars)")
                    except (PermissionError, OSError):
                        pass

        # Root
        _try_read(root_path, os.path.basename(root_path))

        # Depth-1 subdirectories
        try:
            for d in sorted(os.scandir(root_path), key=lambda x: x.name):
                if d.is_dir(follow_symlinks=False):
                    _try_read(d.path, d.name)
        except PermissionError:
            pass

        # Depth-2 subdirectories
        try:
            for d in sorted(os.scandir(root_path), key=lambda x: x.name):
                if not d.is_dir(follow_symlinks=False):
                    continue
                try:
                    for dd in sorted(os.scandir(d.path), key=lambda x: x.name):
                        if dd.is_dir(follow_symlinks=False):
                            _try_read(dd.path, f"{d.name}/{dd.name}")
                except PermissionError:
                    pass
        except PermissionError:
            pass

        combined = "\n\n".join(contents)
        if len(combined) > 5_000:
            combined = combined[:5_000] + "\n... [truncated]"
        return combined

    # ------------------------------------------------------------------
    # Per-modality statistics
    # ------------------------------------------------------------------

    def _compute_modality_stats(
        self, entries: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Compute per-modality statistics from entries."""
        stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "files": 0,
            "size_bytes": 0,
            "subjects": set(),
            "subject_count_from_files": 0,  # count-based (from .mat / CSV without individual IDs)
            "dirs": 0,
            "data_stages": set(),
            "key_dirs": [],
        })

        for e in entries:
            mod = e.get("modality")

            # Distribute file_modality_subjects to the proper modalities
            # even when the directory itself has no modality classification
            for fmod, fval in e.get("file_modality_subjects", {}).items():
                s = stats[fmod]
                if isinstance(fval, set):
                    # Actual subject IDs from CSV — merge into subjects set
                    s["subjects"].update(fval)
                else:
                    # Count-only from .mat — keep as fallback
                    s["subject_count_from_files"] = max(s["subject_count_from_files"], fval)
                # If directory has no modality, attribute its files/size to the
                # first file-inferred modality (avoids double-counting)
                if not mod:
                    s["files"] += e["file_count"]
                    s["size_bytes"] += e["size_bytes"]
                    if e["depth"] <= 2:
                        s["key_dirs"].append(e["rel_path"])
                    if e.get("data_stage") and e["data_stage"] != "unknown":
                        s["data_stages"].add(e["data_stage"])
                    # Only attribute files/size once even if multiple modalities
                    mod = fmod  # mark as handled

            if not mod:
                continue

            s = stats[mod]
            # Avoid double-counting if already attributed above
            if mod not in e.get("file_modality_subjects", {}):
                s["files"] += e["file_count"]
                s["size_bytes"] += e["size_bytes"]

            s["dirs"] += 1

            if e.get("data_stage") and e["data_stage"] != "unknown":
                s["data_stages"].add(e["data_stage"])

            # Collect subjects from dir-level and file-level
            if e.get("subject_id"):
                s["subjects"].add(e["subject_id"])
            fms = e.get("file_modality_subjects")
            if fms:
                # file_modality_subjects already distributed classified subjects
                # above. For the REMAINING unclassified subjects (in file_subjects
                # but not in any file_modality_subjects set), attribute them to
                # the *directory-level* modality if one exists. This handles cases
                # like per-subject task files (MID, nBack) whose filenames don't
                # contain modality keywords but live in a func/imaging directory.
                dir_mod = e.get("modality")
                if dir_mod and dir_mod == mod:
                    classified = set()
                    for fval in fms.values():
                        if isinstance(fval, set):
                            classified.update(fval)
                    for sid in e.get("file_subjects", set()):
                        if sid not in classified:
                            s["subjects"].add(sid)
            else:
                for sid in e.get("file_subjects", set()):
                    s["subjects"].add(sid)

            # Keep top-level dirs as key directories
            if e["depth"] <= 2:
                s["key_dirs"].append(e["rel_path"])

        # Convert sets — keep subject list for detailed reporting
        result = {}
        for mod, s in stats.items():
            subject_list = sorted(s["subjects"])
            # Use the larger of: individual IDs found, or count-based estimate
            subject_count = max(len(subject_list), s["subject_count_from_files"])
            result[mod] = {
                "files": s["files"],
                "size": self._human_size(s["size_bytes"]),
                "size_bytes": s["size_bytes"],
                "subjects": subject_count,
                "subject_list": subject_list,
                "dirs": s["dirs"],
                "data_stages": sorted(s["data_stages"]),
                "key_dirs": list(dict.fromkeys(s["key_dirs"]))[:5],
            }

        return result

    def _merge_modality_details(
        self,
        computed_stats: Dict[str, Dict[str, Any]],
        claude_details: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Merge our computed stats with Claude's modality analysis."""
        # Start with computed stats
        result = {}
        for mod, stats in computed_stats.items():
            result[mod] = {
                "modality": mod,
                "files": stats["files"],
                "size": stats["size"],
                "size_bytes": stats.get("size_bytes", 0),
                "subjects": stats["subjects"],
                "subject_list": stats.get("subject_list", []),
                "dirs": stats["dirs"],
                "data_stages": stats["data_stages"],
                "key_dirs": stats["key_dirs"],
                "description": "",
                "notes": "",
            }

        # Overlay Claude's richer descriptions
        for cd in claude_details:
            mod = cd.get("modality", "")
            if mod in result:
                if cd.get("description"):
                    result[mod]["description"] = cd["description"]
                if cd.get("notes"):
                    result[mod]["notes"] = cd["notes"]
                # Use Claude's subject estimate if ours is 0
                if result[mod]["subjects"] == 0 and cd.get("estimated_subjects"):
                    result[mod]["subjects"] = cd["estimated_subjects"]
            else:
                # Claude found a modality we didn't detect
                result[mod] = {
                    "modality": mod,
                    "files": 0,
                    "size": "0.0B",
                    "size_bytes": 0,
                    "subjects": cd.get("estimated_subjects", 0),
                    "subject_list": [],
                    "dirs": 0,
                    "data_stages": cd.get("data_stages", []),
                    "key_dirs": cd.get("key_directories", []),
                    "description": cd.get("description", ""),
                    "notes": cd.get("notes", ""),
                }

        return sorted(result.values(), key=lambda x: x["modality"])

    # ------------------------------------------------------------------
    # Claude analysis
    # ------------------------------------------------------------------

    def _analyze_with_claude(
        self,
        context: RunContext,
        ds_name: str,
        ds_path: str,
        entries: List[Dict[str, Any]],
        modality_stats: Dict[str, Dict[str, Any]],
        readme_content: str = "",
    ) -> Dict[str, Any]:
        """Use Claude (CLI or API) to analyze the dataset structure."""
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
                n_subs = len(e.get("file_subjects", set()))
                if n_subs > 0:
                    flags.append(f"{n_subs} subjects from files")
                elif e.get("subject_id"):
                    flags.append(f"subject={e['subject_id']}")
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                files_str = f" ({e['file_count']} files, {self._human_size(e['size_bytes'])})"
                tree_lines.append(f"{indent}{e['rel_path']}{flag_str}{files_str}")

            if len(depth_entries) > 30:
                tree_lines.append(f"{indent}... and {len(depth_entries) - 30} more directories at depth {depth}")

        type_counts = Counter(e["entry_type"] for e in entries)
        stage_counts = Counter(e["data_stage"] for e in entries)

        # Format modality stats for the prompt
        mod_lines = []
        for mod, stats in sorted(modality_stats.items()):
            mod_lines.append(
                f"  {mod}: {stats['files']} files, {stats['size']}, "
                f"{stats['subjects']} subjects detected, "
                f"stages: {stats['data_stages']}"
            )

        # Gather .mat metadata summaries from entries
        mat_lines = []
        for e in entries:
            if e.get("mat_metadata"):
                for m in e["mat_metadata"][:5]:
                    mat_lines.append(
                        f"  {e['rel_path']}/{m['file']}: var '{m['var']}' shape {m['shape']}"
                    )
            if e.get("mat_subjects"):
                mat_lines.append(
                    f"  → {e['rel_path']}: ~{e['mat_subjects']} subjects estimated from .mat"
                )

        mat_section = ""
        if mat_lines:
            mat_section = f"\nMATLAB file metadata (variable shapes):\n{chr(10).join(mat_lines)}\n"

        readme_section = ""
        if readme_content:
            readme_section = f"\nREADME content found in dataset:\n{readme_content}\n"

        prompt = f"""Dataset: {ds_name}
Path: {ds_path}
Total directories scanned: {len(entries)}
Directory type distribution: {dict(type_counts)}
Data stage distribution: {dict(stage_counts)}

Heuristic modality statistics:
{chr(10).join(mod_lines) if mod_lines else '  (none detected)'}
{mat_section}{readme_section}
Directory tree (with heuristic classification):
{chr(10).join(tree_lines)}

Lab context:
{yaml.dump(context.lab_context, default_flow_style=False)}

Please analyze this dataset structure and provide:
1. Detailed per-modality breakdown (what types of data, estimated subjects per modality)
2. Pipelines/processing tools detected
3. Recommendations for better organization

IMPORTANT: If README content is provided above, use it as ground truth for subject counts
and study details. If MATLAB file metadata shows variable shapes with '_id' suffixes,
use those dimensions to corroborate subject count estimates.
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
                "estimated_subjects": None,
                "modality_details": [],
                "pipelines_detected": [],
                "summary": f"Heuristic analysis only (Claude failed: {e})",
                "issues": [],
                "recommendations": [],
            }

    @staticmethod
    def _human_size(size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(size_bytes) < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}PB"
