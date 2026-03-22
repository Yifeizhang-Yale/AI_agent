"""Tests for skill auto-discovery."""

from dm_agent.skills import discover_skills
from dm_agent.base_skill import BaseSkill


def test_discover_all_skills():
    skills = discover_skills()
    expected = {"scanner", "analyzer", "reporter", "confirmer", "deleter", "quota_monitor", "readme_generator", "data_cataloger"}
    assert set(skills.keys()) == expected


def test_all_skills_have_required_attrs():
    skills = discover_skills()
    for name, cls in skills.items():
        instance = cls()
        assert instance.name, f"{cls.__name__} missing name"
        assert instance.description, f"{cls.__name__} missing description"
        assert instance.phase in BaseSkill.PHASE_ORDER, f"{cls.__name__} has invalid phase: {instance.phase}"
