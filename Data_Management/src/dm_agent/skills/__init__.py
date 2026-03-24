"""Auto-discovery of skill plugins."""

import importlib
import pkgutil
from pathlib import Path
from typing import Dict, Type

from dm_agent.base_skill import BaseSkill


def discover_skills() -> Dict[str, Type[BaseSkill]]:
    """Scan this package for all BaseSkill subclasses and return {name: class}."""
    skills: Dict[str, Type[BaseSkill]] = {}
    package_dir = Path(__file__).parent

    for module_info in pkgutil.iter_modules([str(package_dir)]):
        module = importlib.import_module(f"dm_agent.skills.{module_info.name}")
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, BaseSkill)
                and attr is not BaseSkill
            ):
                skills[attr.name] = attr

    return skills
