"""Tests for scanner skill."""

import os
from datetime import datetime

from dm_agent.base_skill import RunContext
from dm_agent.skills.scanner import ScannerSkill


def test_scanner_first_run(config, db):
    context = RunContext(
        config=config,
        db=db,
        lab_context={},
        run_timestamp=datetime.utcnow(),
    )

    skill = ScannerSkill()
    result = skill.run(context)

    assert result.success
    assert len(context.scan_results) >= 2  # project_a and project_b

    # project_a should have README content
    proj_a_results = [r for r in context.scan_results if "project_a" in r["dir_path"]]
    assert len(proj_a_results) == 1
    assert proj_a_results[0]["readme_content"] is not None
    assert "Project A" in proj_a_results[0]["readme_content"]

    # project_b should have dir_tree (no README)
    proj_b_results = [r for r in context.scan_results if "project_b" in r["dir_path"]]
    assert len(proj_b_results) == 1
    assert proj_b_results[0]["dir_tree"] is not None


def test_scanner_member_matching(config, db):
    context = RunContext(
        config=config,
        db=db,
        lab_context={},
        run_timestamp=datetime.utcnow(),
    )

    skill = ScannerSkill()
    skill.run(context)

    proj_a_results = [r for r in context.scan_results if "project_a" in r["dir_path"]]
    assert proj_a_results[0]["member_email"] == "alice@test.edu"

    proj_b_results = [r for r in context.scan_results if "project_b" in r["dir_path"]]
    assert proj_b_results[0]["member_email"] == "bob@test.edu"


def test_scanner_updates_scan_state(config, db):
    context = RunContext(
        config=config,
        db=db,
        lab_context={},
        run_timestamp=datetime.utcnow(),
    )

    skill = ScannerSkill()
    skill.run(context)

    for target in config.scan_targets:
        ts = db.get_last_scan_ts(target.path)
        assert ts is not None
