"""Tests for data cataloger skill."""

import json
import os
from datetime import datetime

import pytest

from dm_agent.base_skill import RunContext
from dm_agent.db import Database
from dm_agent.skills.data_cataloger import DataCatalogerSkill


@pytest.fixture
def neuro_dataset(tmp_path):
    """Create a mock neuroimaging dataset structure."""
    ds_root = tmp_path / "datasets" / "HCP"
    ds_root.mkdir(parents=True)

    # BIDS-like structure: sub-001/anat/, sub-001/func/
    for subj in ["sub-001", "sub-002", "sub-003"]:
        for mod in ["anat", "func", "dwi"]:
            d = ds_root / subj / mod
            d.mkdir(parents=True)
            (d / f"{subj}_T1w.nii.gz").write_text("fake")
        # Session dir
        ses = ds_root / subj / "ses-01"
        ses.mkdir(exist_ok=True)
        (ses / "task-rest_bold.nii.gz").write_text("fake")

    # Derivatives
    deriv = ds_root / "derivatives" / "fmriprep"
    deriv.mkdir(parents=True)
    (deriv / "sub-001_desc-preproc_bold.nii.gz").write_text("fake")

    # Raw source
    raw = ds_root / "sourcedata"
    raw.mkdir()
    (raw / "dicoms.tar.gz").write_text("fake")

    return ds_root


@pytest.fixture
def cataloger_config(config, neuro_dataset):
    """Config with data_cataloger enabled."""
    config.skills["data_cataloger"] = {
        "enabled": True,
        "scan_depth": 3,
        "datasets": [
            {"name": "HCP", "path": str(neuro_dataset)},
        ],
    }
    return config


def test_cataloger_scan_and_classify(cataloger_config, db, neuro_dataset):
    """Test that cataloger correctly scans and classifies a neuroimaging dataset."""
    context = RunContext(
        config=cataloger_config,
        db=db,
        lab_context={},
        run_timestamp=datetime.utcnow(),
    )

    skill = DataCatalogerSkill()
    # Override Claude API call for testing
    skill._analyze_with_claude = lambda ctx, name, path, entries: {
        "organization_scheme": "by_subject",
        "has_raw": True,
        "has_preprocessed": True,
        "has_derivatives": True,
        "bids_compliant": True,
        "modalities_found": ["anat", "func", "dwi"],
        "summary": "Test dataset with BIDS structure",
        "recommendations": [],
    }

    result = skill.run(context)
    assert result.success
    assert "HCP" in result.message

    # Check DB
    datasets = db.get_all_datasets()
    assert len(datasets) == 1
    ds = datasets[0]
    assert ds["dataset_name"] == "HCP"
    assert ds["status"] == "cataloged"
    assert ds["organization_scheme"] == "by_subject"

    # Check entries
    entries = db.get_catalog_entries(ds["id"])
    assert len(entries) > 0

    # Should have subject directories
    subject_entries = [e for e in entries if e["entry_type"] == "subject_dir"]
    assert len(subject_entries) >= 3

    # Should have modality directories
    modality_entries = [e for e in entries if e["entry_type"] == "modality_dir"]
    assert len(modality_entries) > 0

    # Should detect derivatives
    deriv_entries = [e for e in entries if e["data_stage"] == "derivative"]
    assert len(deriv_entries) > 0


def test_cataloger_heuristic_classification():
    """Test the heuristic classification of directory names."""
    skill = DataCatalogerSkill()

    # Subject dir
    entry = {"dir_name": "sub-001", "rel_path": "sub-001", "depth": 1,
             "abs_path": "/fake", "entry_type": "other", "data_stage": "unknown",
             "modality": None, "subject_id": None, "session_id": None,
             "file_types": [], "sample_files": [], "file_count": 0, "size_bytes": 0}
    skill._classify_entry(entry)
    assert entry["entry_type"] == "subject_dir"
    assert entry["subject_id"] == "sub-001"

    # Modality dir
    entry2 = {**entry, "dir_name": "func", "rel_path": "sub-001/func", "depth": 2,
              "entry_type": "other", "subject_id": None}
    skill._classify_entry(entry2)
    assert entry2["entry_type"] == "modality_dir"
    assert entry2["modality"] == "func"

    # Preprocessed dir
    entry3 = {**entry, "dir_name": "fmriprep", "rel_path": "derivatives/fmriprep", "depth": 2,
              "entry_type": "other", "data_stage": "unknown", "subject_id": None}
    skill._classify_entry(entry3)
    assert entry3["data_stage"] == "preprocessed"

    # Numeric subject ID (UKB style)
    entry4 = {**entry, "dir_name": "1234567", "rel_path": "1234567", "depth": 1,
              "entry_type": "other", "subject_id": None}
    skill._classify_entry(entry4)
    assert entry4["entry_type"] == "subject_dir"


def test_cataloger_skips_already_cataloged(cataloger_config, db):
    """Datasets marked as 'cataloged' should be skipped."""
    ds_id = db.get_or_create_dataset("HCP", str(cataloger_config.skills["data_cataloger"]["datasets"][0]["path"]))
    db.update_dataset_status(ds_id, "cataloged")

    context = RunContext(
        config=cataloger_config,
        db=db,
        lab_context={},
        run_timestamp=datetime.utcnow(),
    )

    skill = DataCatalogerSkill()
    result = skill.run(context)
    assert result.success
    assert "All configured datasets have been cataloged" in result.message
