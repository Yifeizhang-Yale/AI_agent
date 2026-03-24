"""Tests for config module."""

import pytest

from dm_agent.config import load_config


def test_load_config(config_file):
    config = load_config(config_file)
    assert len(config.scan_targets) == 1
    assert config.email.from_address == "test@test.edu"
    assert len(config.members) == 2
    assert config.members[0].name == "Alice"
    assert "project_a" in config.projects
    assert len(config.retention_policies) == 1


def test_load_config_missing():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.yaml")


def test_lab_context_loaded(config):
    assert config.lab["name"] == "Test Lab"
    assert len(config.members) == 2
    assert config.members[0].email == "alice@test.edu"
    assert config.projects["project_b"].retention == "6_months"


def test_skills_config(config):
    assert config.skills["scanner"]["enabled"] is True
    assert config.skills["deleter"]["dry_run"] is True
    assert config.skills["quota_monitor"]["enabled"] is False
