"""Orchestrator — discover skills, run them in phase order, coordinate the full cycle."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

import yaml

from dm_agent.base_skill import BaseSkill, RunContext, SkillResult
from dm_agent.config import Config
from dm_agent.db import Database
from dm_agent.skills import discover_skills

logger = logging.getLogger(__name__)


class Orchestrator:
    """Coordinates skill execution for the data management agent."""

    def __init__(self, config: Config, db: Database):
        self.config = config
        self.db = db
        self._skills = self._load_skills()

    def _load_skills(self) -> List[BaseSkill]:
        """Discover and instantiate enabled skills, sorted by phase."""
        skill_classes = discover_skills()
        enabled = []

        for name, cls in skill_classes.items():
            instance = cls()
            if instance.is_enabled(self.config):
                enabled.append(instance)
                logger.info(f"Loaded skill: {name} (phase: {instance.phase})")
            else:
                logger.debug(f"Skipped disabled skill: {name}")

        # Sort by phase order
        enabled.sort(key=lambda s: BaseSkill.PHASE_ORDER.get(s.phase, 99))
        return enabled

    def _build_context(self) -> RunContext:
        """Build the shared run context."""
        lab_context = self._load_lab_context()
        return RunContext(
            config=self.config,
            db=self.db,
            lab_context=lab_context,
            run_timestamp=datetime.utcnow(),
        )

    def _load_lab_context(self) -> Dict[str, Any]:
        """Load lab context as a raw dict for Claude API prompts."""
        return {
            "lab": self.config.lab,
            "members": [
                {"name": m.name, "email": m.email, "projects": m.projects, "role": m.role}
                for m in self.config.members
            ],
            "projects": {
                name: {"description": p.description, "data_types": p.data_types, "retention": p.retention}
                for name, p in self.config.projects.items()
            },
        }

    def run_full_cycle(self) -> int:
        """Run all enabled skills in phase order. Returns 0 on success, 1 on errors."""
        logger.info("=" * 60)
        logger.info(f"Starting full cycle at {datetime.utcnow().isoformat()}")
        logger.info(f"Enabled skills: {[s.name for s in self._skills]}")
        logger.info("=" * 60)

        context = self._build_context()

        for skill in self._skills:
            logger.info(f"Running skill: {skill.name} (phase: {skill.phase})")
            try:
                result = skill.run(context)
                logger.info(f"  Result: {result.message}")
                if not result.success:
                    logger.warning(f"  Skill {skill.name} reported failure: {result.message}")
            except Exception as e:
                logger.error(f"  Skill {skill.name} crashed: {e}", exc_info=True)
                context.errors.append(f"Skill {skill.name} crashed: {e}")

        if context.errors:
            logger.warning(f"Cycle completed with {len(context.errors)} errors:")
            for err in context.errors:
                logger.warning(f"  - {err}")
            return 1

        logger.info("Cycle completed successfully")
        return 0

    def check_email_confirmations(self) -> int:
        """Check for email-based deletion confirmations."""
        from dm_agent.skills.confirmer import ConfirmerSkill

        context = self._build_context()
        confirmer = ConfirmerSkill()
        return confirmer.check_email_replies(context)
