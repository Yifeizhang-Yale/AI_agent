"""MCP Server — exposes dm-agent READ-ONLY tools to lab members' Claude Code.

Lab members get:
  - Data search and discovery (read-only DB access)
  - Directory inspection (filesystem read)
  - Feedback submission (writes to a separate feedback file, not the main DB)

Admin operations (catalog, organize, delete) are NOT exposed here.
Those go through the CLI or agent mode.

Setup for lab members:
  bin/setup-claude-code.sh
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from mcp.server.fastmcp import FastMCP

from dm_agent.config import load_config
from dm_agent.db import Database

# ---------------------------------------------------------------------------
# Configuration — resolve from env or defaults
# ---------------------------------------------------------------------------

_config_path = os.environ.get(
    "DM_AGENT_CONFIG",
    str(Path(__file__).resolve().parent.parent.parent / "config.yaml"),
)
_config = load_config(_config_path)
_db = Database(_config.database_path)

_feedback_path = os.environ.get(
    "DM_AGENT_FEEDBACK",
    str(Path(_config.database_path).parent / "feedback.jsonl"),
)

# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "dm-agent",
    instructions=(
        "Zhao Lab HPC data management tools. Use these to help lab members "
        "find neuroimaging data across datasets: A4, ABCD, ADNI, Atlas, HCP, "
        "IMAGEN, NACC, OHSU, UKB. All tools are read-only. "
        "Use dm_submit_feedback to send requests to the data admin."
    ),
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _human_size(size_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f}PB"


# ---------------------------------------------------------------------------
# Resource: data manifest
# ---------------------------------------------------------------------------


@mcp.resource("dm://manifest")
def data_manifest() -> str:
    """Lab data manifest — compact YAML with per-dataset, per-modality summaries.

    Read this first for a quick overview of what data is available.
    """
    manifest_path = _config.skills.get("lab_overview", {}).get(
        "manifest_path",
        str(Path(_config.database_path).parent / "DATA_MANIFEST.yaml"),
    )
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            return f.read()
    return "Manifest not yet generated. Ask the data admin to run: dm-agent catalog"


# ---------------------------------------------------------------------------
# Tool: search data
# ---------------------------------------------------------------------------


@mcp.tool()
def dm_search_data(keywords: str) -> str:
    """Search Zhao Lab's neuroimaging data catalog by keywords.

    Searches across all cataloged datasets matching against modality names,
    descriptions, directory paths, file types, and summaries.

    Args:
        keywords: Search terms, e.g. "structural connectivity", "resting fMRI",
                  "amyloid PET", "VBM", "Schaefer atlas"

    Returns:
        Matching datasets and directories ranked by relevance.
    """
    kws = keywords.lower().split()
    datasets = _db.get_all_datasets()
    cataloged = [d for d in datasets if d["status"] == "cataloged"]

    if not cataloged:
        return json.dumps({"error": "No datasets cataloged yet. Ask the data admin."})

    matches: List[Dict[str, Any]] = []
    for ds in cataloged:
        mod_stats = _db.get_modality_stats(ds["id"])
        entries = _db.get_catalog_entries(ds["id"])

        # Build search text from modality descriptions (rich detail)
        mod_desc = " ".join(
            (ms.get("description") or "") + " " + (ms.get("notes") or "")
            for ms in mod_stats
        ).lower()

        ds_text = " ".join([
            ds["dataset_name"].lower(),
            (ds.get("summary") or "").lower(),
            (ds.get("modalities") or "").lower(),
            (ds.get("organization_scheme") or "").lower(),
            mod_desc,
        ])

        ds_score = sum(1 for kw in kws if kw in ds_text)

        # Entry-level matches
        entry_matches = []
        for e in entries:
            entry_text = " ".join([
                e["rel_path"].lower(),
                (e.get("modality") or "").lower(),
                (e.get("data_stage") or "").lower(),
                (e.get("file_types") or "").lower(),
                (e.get("sample_files") or "").lower(),
            ])
            entry_score = sum(1 for kw in kws if kw in entry_text)
            if entry_score > 0:
                entry_matches.append({
                    "rel_path": e["rel_path"],
                    "abs_path": os.path.join(ds["root_path"], e["rel_path"]),
                    "modality": e.get("modality"),
                    "data_stage": e.get("data_stage"),
                    "file_count": e["file_count"],
                    "size": _human_size(e["size_bytes"]),
                })

        if ds_score > 0 or entry_matches:
            matches.append({
                "dataset": ds["dataset_name"],
                "root_path": ds["root_path"],
                "total_size": _human_size(ds["total_size_bytes"] or 0),
                "total_subjects": ds["total_subjects"] or 0,
                "modalities": ds.get("modalities", "[]"),
                "summary": (ds.get("summary") or "")[:300],
                "score": ds_score + len(entry_matches),
                "matching_dirs": sorted(entry_matches, key=lambda x: -len(x.get("modality") or ""))[:15],
                "modality_stats": [
                    {
                        "modality": ms["modality"],
                        "subject_count": ms["subject_count"],
                        "file_count": ms["file_count"],
                        "size": _human_size(ms["size_bytes"] or 0),
                        "description": (ms.get("description") or "")[:200],
                    }
                    for ms in mod_stats
                ],
            })

    matches.sort(key=lambda x: -x["score"])
    return json.dumps({"query": keywords, "result_count": len(matches), "results": matches},
                       ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Tool: list datasets
# ---------------------------------------------------------------------------


@mcp.tool()
def dm_list_datasets() -> str:
    """List all available neuroimaging datasets with status, size, subjects, and modalities."""
    datasets = _db.get_all_datasets()

    result = []
    for ds in datasets:
        info: Dict[str, Any] = {
            "name": ds["dataset_name"],
            "path": ds["root_path"],
            "status": ds["status"],
        }
        if ds["status"] == "cataloged":
            info["total_size"] = _human_size(ds["total_size_bytes"] or 0)
            info["total_files"] = ds["total_files"]
            info["total_subjects"] = ds["total_subjects"]
            info["modalities"] = ds.get("modalities")
            info["has_raw"] = bool(ds.get("has_raw"))
            info["has_preprocessed"] = bool(ds.get("has_preprocessed"))
            info["has_derivatives"] = bool(ds.get("has_derivatives"))
            info["bids_compliant"] = bool(ds.get("bids_compliant"))
        result.append(info)

    return json.dumps(result, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Tool: dataset info
# ---------------------------------------------------------------------------


@mcp.tool()
def dm_dataset_info(dataset: str) -> str:
    """Get detailed information about a specific dataset.

    Returns per-modality breakdown with subject counts, file counts, sizes,
    data stages, key directories, and descriptions.

    Args:
        dataset: Dataset name (A4, ABCD, ADNI, Atlas, HCP, IMAGEN, NACC, OHSU, UKB)
    """
    datasets = _db.get_all_datasets()
    ds = next((d for d in datasets if d["dataset_name"] == dataset), None)

    if not ds:
        available = [d["dataset_name"] for d in datasets]
        return json.dumps({"error": f"Dataset '{dataset}' not found", "available": available})

    if ds["status"] != "cataloged":
        return json.dumps({"error": f"Dataset '{dataset}' not yet cataloged (status: {ds['status']})"})

    mod_stats = _db.get_modality_stats(ds["id"])

    info = {
        "name": ds["dataset_name"],
        "path": ds["root_path"],
        "total_size": _human_size(ds["total_size_bytes"] or 0),
        "total_files": ds["total_files"],
        "total_subjects": ds["total_subjects"],
        "organization": ds.get("organization_scheme"),
        "has_raw": bool(ds.get("has_raw")),
        "has_preprocessed": bool(ds.get("has_preprocessed")),
        "has_derivatives": bool(ds.get("has_derivatives")),
        "bids_compliant": bool(ds.get("bids_compliant")),
        "summary": ds.get("summary"),
        "modalities": [],
    }

    for ms in mod_stats:
        mod_info: Dict[str, Any] = {
            "modality": ms["modality"],
            "subject_count": ms["subject_count"],
            "file_count": ms["file_count"],
            "size": _human_size(ms["size_bytes"] or 0),
            "dir_count": ms.get("dir_count", 0),
            "description": ms.get("description"),
        }
        # Include data stages
        try:
            mod_info["data_stages"] = json.loads(ms.get("data_stages") or "[]")
        except (ValueError, TypeError):
            mod_info["data_stages"] = []
        # Include key directories
        try:
            mod_info["key_dirs"] = json.loads(ms.get("key_dirs") or "[]")
        except (ValueError, TypeError):
            mod_info["key_dirs"] = []
        info["modalities"].append(mod_info)

    # Include recommendations if available
    if ds.get("recommendations"):
        try:
            recs = json.loads(ds["recommendations"])
            rec_list = recs if isinstance(recs, list) else recs.get("recommendations", [])
            info["recommendations"] = rec_list
        except (ValueError, TypeError):
            pass

    return json.dumps(info, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Tool: inspect directory
# ---------------------------------------------------------------------------


@mcp.tool()
def dm_inspect_directory(path: str, max_depth: int = 3) -> str:
    """Inspect a directory on the HPC filesystem.

    Shows directory structure, file counts, total size.
    Use this to explore data directories and understand their layout.

    Args:
        path: Absolute path to the directory
        max_depth: Maximum listing depth (default 3)
    """
    if not os.path.isdir(path):
        return json.dumps({"error": f"Directory not found: {path}"})

    # Directory tree
    try:
        tree_proc = subprocess.run(
            ["find", path, "-maxdepth", str(max_depth)],
            capture_output=True, text=True, timeout=30,
        )
        entries = tree_proc.stdout.strip().split("\n") if tree_proc.stdout.strip() else []
    except subprocess.TimeoutExpired:
        entries = ["(timed out)"]
    except Exception as e:
        entries = [f"(error: {e})"]

    # File count
    try:
        fc_proc = subprocess.run(
            ["find", path, "-maxdepth", str(max_depth), "-type", "f"],
            capture_output=True, text=True, timeout=30,
        )
        file_count = len(fc_proc.stdout.strip().split("\n")) if fc_proc.stdout.strip() else 0
    except Exception:
        file_count = -1

    # Total size
    try:
        du_proc = subprocess.run(
            ["du", "-sh", path],
            capture_output=True, text=True, timeout=60,
        )
        total_size = du_proc.stdout.split()[0] if du_proc.returncode == 0 else "unknown"
    except Exception:
        total_size = "unknown"

    truncated = len(entries) > 200
    return json.dumps({
        "path": path,
        "total_size": total_size,
        "file_count": file_count,
        "entries_shown": min(len(entries), 200),
        "truncated": truncated,
        "tree": entries[:200],
    }, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Tool: submit feedback
# ---------------------------------------------------------------------------


@mcp.tool()
def dm_submit_feedback(message: str, category: str = "general") -> str:
    """Submit feedback or a data request to the data management admin.

    Use this when a lab member wants to:
    - Request a new dataset to be cataloged
    - Report an issue with existing data
    - Ask for data cleanup or reorganization
    - Suggest improvements to data organization
    - Request access to specific data

    Args:
        message: The feedback or request (be specific)
        category: One of "data_request", "issue", "cleanup", "suggestion", "general"
    """
    username = os.environ.get("USER", os.environ.get("LOGNAME", "unknown"))

    # Resolve to lab member if possible
    member_name = username
    member_email = ""
    for m in _config.members:
        if m.hpc_username == username:
            member_name = m.name
            member_email = m.email
            break

    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "user": username,
        "member_name": member_name,
        "member_email": member_email,
        "category": category,
        "message": message,
    }

    # Append to feedback file (not the main database)
    try:
        with open(_feedback_path, "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        return json.dumps({"error": f"Failed to save feedback: {e}"})

    return json.dumps({
        "success": True,
        "message": f"Feedback submitted. The data admin will review it.",
        "reference": entry["timestamp"],
    })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
