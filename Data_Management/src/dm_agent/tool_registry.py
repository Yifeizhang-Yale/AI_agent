"""Tool definitions for the agent loop — maps skills to Claude tool_use format."""

TOOL_DEFINITIONS = [
    {
        "name": "scan_storage",
        "description": (
            "Scan configured storage targets for recently changed or stale directories. "
            "Returns a list of directories that have been modified or are past the staleness threshold."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "analyze_directories",
        "description": (
            "Use Claude to analyze scanned directories — assess organization quality, "
            "identify issues (naming inconsistencies, redundant data), and flag deletion candidates. "
            "Requires scan_storage to have been run first in this session."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "catalog_dataset",
        "description": (
            "Deep-catalog a neuroimaging dataset: scan directory structure, identify modalities "
            "(anat, func, dwi, pet, eeg, SC, FC, etc.), count subjects per modality, classify "
            "data stages (raw/preprocessed/derivatives), and check BIDS compliance."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset": {
                    "type": "string",
                    "description": "Dataset name (e.g., NACC, ABCD, HCP, UKB, ADNI, A4, Atlas, IMAGEN, OHSU)",
                },
            },
            "required": ["dataset"],
        },
    },
    {
        "name": "query_catalog",
        "description": (
            "Search the data catalog using keywords. Matches against dataset names, modality "
            "descriptions, directory paths, file types, and Claude-generated summaries. "
            "Returns ranked results with per-modality subject counts."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "keywords": {
                    "type": "string",
                    "description": "Search keywords (e.g., 'SC matrix', 'resting-state fMRI', 'amyloid PET', 'VBM')",
                },
            },
            "required": ["keywords"],
        },
    },
    {
        "name": "check_quota",
        "description": "Check disk quota usage for lab storage. Returns usage levels and warning/critical alerts.",
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "organize_dataset",
        "description": (
            "Reorganize a dataset: remove redundant files (e.g., uncompressed .nii alongside .nii.gz, "
            "duplicate directories) and restructure directory layout. "
            "IMPORTANT: ALWAYS set dry_run=true first to preview the plan before executing."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset": {
                    "type": "string",
                    "description": "Dataset name to organize",
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "If true, show plan without executing. ALWAYS try dry_run=true first.",
                },
                "no_reorganize": {
                    "type": "boolean",
                    "description": "If true, only remove redundant files, skip directory restructuring.",
                },
            },
            "required": ["dataset"],
        },
    },
    {
        "name": "inspect_directory",
        "description": (
            "Look at a directory's structure, file counts, total size, and file types. "
            "Use this to understand what's in a directory before taking any action."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute path to the directory",
                },
                "max_depth": {
                    "type": "integer",
                    "description": "Maximum depth to list (default: 3)",
                },
            },
            "required": ["path"],
        },
    },
    {
        "name": "get_status",
        "description": (
            "Get current agent status: all cataloged datasets with sizes/subjects/modalities, "
            "pending datasets, pending deletion requests, and last scan timestamps."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "request_deletion",
        "description": (
            "Create a deletion request for a directory. Generates a confirmation token that the "
            "data owner must approve before deletion is executed. Safe — nothing is deleted immediately."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dir_path": {
                    "type": "string",
                    "description": "Absolute path to the directory to delete",
                },
                "reason": {
                    "type": "string",
                    "description": "Why this directory should be deleted",
                },
            },
            "required": ["dir_path", "reason"],
        },
    },
    {
        "name": "generate_readme",
        "description": (
            "Generate a README.md for a directory using Claude. Returns the generated content "
            "for review — does NOT write to disk automatically."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dir_path": {
                    "type": "string",
                    "description": "Absolute path to the directory",
                },
            },
            "required": ["dir_path"],
        },
    },
    {
        "name": "send_report",
        "description": (
            "Send data management email reports to lab members. Includes analysis summaries, "
            "cleanup recommendations, and deletion tokens. "
            "Requires scan_storage and analyze_directories to have been run first."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "member_email": {
                    "type": "string",
                    "description": "Specific member email. Leave empty to send to all members.",
                },
            },
        },
    },
    {
        "name": "refresh_overview",
        "description": (
            "Regenerate LAB_DATA_OVERVIEW.md and DATA_MANIFEST.yaml from the catalog database. "
            "Run this after cataloging or organizing datasets to keep documentation current."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
]
