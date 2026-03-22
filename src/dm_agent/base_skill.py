"""Base class for all Agent skills."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from dm_agent.config import Config
from dm_agent.db import Database


@dataclass
class RunContext:
    """Shared context passed through the skill pipeline."""

    config: Config
    db: Database
    lab_context: Dict[str, Any]
    run_timestamp: datetime
    # Accumulated results from earlier skills in the pipeline
    scan_results: List[Dict[str, Any]] = field(default_factory=list)
    analysis_results: List[Dict[str, Any]] = field(default_factory=list)
    quota_results: List[Dict[str, Any]] = field(default_factory=list)
    readme_suggestions: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass
class SkillResult:
    """Return value from a skill run."""

    success: bool
    message: str
    data: Dict[str, Any] = field(default_factory=dict)


class BaseSkill(ABC):
    """Abstract base for pluggable skills.

    Subclasses must define class-level attributes:
        name:        unique identifier, matches the key in config.yaml skills section
        description: human-readable one-liner
        phase:       execution phase ordering — "scan" | "analyze" | "report" | "cleanup"
    """

    name: str = ""
    description: str = ""
    phase: str = "scan"  # scan -> analyze -> report -> cleanup

    # Phase ordering for the orchestrator
    PHASE_ORDER = {"scan": 0, "analyze": 1, "report": 2, "cleanup": 3}

    def is_enabled(self, config: Config) -> bool:
        """Check if this skill is enabled in the config."""
        skill_cfg = config.skills.get(self.name, {})
        return skill_cfg.get("enabled", False)

    def get_config(self, config: Config) -> Dict[str, Any]:
        """Get this skill's config section."""
        return config.skills.get(self.name, {})

    @abstractmethod
    def run(self, context: RunContext) -> SkillResult:
        """Execute the skill. Must be implemented by subclasses."""
        ...
