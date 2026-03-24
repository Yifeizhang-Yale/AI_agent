"""Shared test fixtures."""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from dm_agent.config import load_config
from dm_agent.db import Database


@pytest.fixture
def tmp_dir(tmp_path):
    """Create a temporary directory structure for testing."""
    # Create scan target directories
    scan_dir = tmp_path / "scan_target"
    scan_dir.mkdir()

    # Project A with README
    proj_a = scan_dir / "project_a"
    proj_a.mkdir()
    (proj_a / "README.md").write_text("# Project A\nSimulation data for protein folding.\n")
    (proj_a / "data.csv").write_text("col1,col2\n1,2\n")
    (proj_a / "results").mkdir()
    (proj_a / "results" / "output.txt").write_text("results here")

    # Project B without README
    proj_b = scan_dir / "project_b"
    proj_b.mkdir()
    (proj_b / "model.pt").write_text("fake model")
    (proj_b / "train.log").write_text("epoch 1: loss 0.5")

    return tmp_path


@pytest.fixture
def config_file(tmp_dir):
    """Create a test config file."""
    db_path = str(tmp_dir / "test.db")
    scan_path = str(tmp_dir / "scan_target")
    lab_context_path = str(tmp_dir / "lab_context.yaml")

    # Write lab context
    lab_context = {
        "lab": {"name": "Test Lab", "pi": "Dr. Test", "pi_email": "pi@test.edu"},
        "members": [
            {"name": "Alice", "email": "alice@test.edu", "hpc_username": "alice", "projects": ["project_a"], "role": "postdoc"},
            {"name": "Bob", "email": "bob@test.edu", "hpc_username": "bob", "projects": ["project_b"], "role": "phd_student"},
        ],
        "projects": {
            "project_a": {"description": "Protein folding", "data_types": ["pdb", "trajectory"], "retention": "permanent"},
            "project_b": {"description": "ML training", "data_types": ["checkpoints", "logs"], "retention": "6_months"},
        },
        "retention_policies": [
            {"pattern": "*/logs/*", "max_age_days": 60, "action": "recommend_delete"},
        ],
    }
    with open(lab_context_path, "w") as f:
        yaml.dump(lab_context, f)

    config = {
        "database_path": db_path,
        "lab_context_path": lab_context_path,
        "scan_targets": [{"path": scan_path, "description": "Test target"}],
        "email": {
            "smtp_host": "localhost",
            "smtp_port": 1025,
            "from_address": "test@test.edu",
            "use_tls": False,
        },
        "admin_users": ["admin_user"],
        "confirmation": {"method": "token_cli", "expiry_days": 7},
        "scanner": {"stale_threshold_days": 90, "max_depth": 3},
        "analyzer": {"model": "claude-sonnet-4-20250514"},
        "skills": {
            "scanner": {"enabled": True, "stale_threshold_days": 90, "max_depth": 3},
            "analyzer": {"enabled": True},
            "reporter": {"enabled": True},
            "confirmer": {"enabled": True},
            "deleter": {"enabled": True, "dry_run": True},
            "quota_monitor": {"enabled": False},
            "readme_generator": {"enabled": False},
        },
    }

    config_path = str(tmp_dir / "config.yaml")
    with open(config_path, "w") as f:
        yaml.dump(config, f)

    return config_path


@pytest.fixture
def db(tmp_dir):
    """Create a test database."""
    return Database(str(tmp_dir / "test.db"))


@pytest.fixture
def config(config_file):
    """Load test config."""
    return load_config(config_file)
