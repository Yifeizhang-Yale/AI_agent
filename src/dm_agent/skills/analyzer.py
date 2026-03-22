"""Analyzer skill — use Claude to analyze scan results and generate recommendations."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

import yaml

from dm_agent.base_skill import BaseSkill, RunContext, SkillResult
from dm_agent.claude_client import create_client, parse_json_response

logger = logging.getLogger(__name__)


SYSTEM_PROMPT_TEMPLATE = """You are a data management assistant for an HPC (High Performance Computing) lab.
Your role is to analyze directory changes on the shared filesystem and provide actionable recommendations.

## Lab Context
{lab_context}

## Retention Policies
{retention_policies}

## Your Tasks
1. Analyze the provided directory information (README content or file listing)
2. Assess whether the data is well-organized and properly documented
3. Identify potential issues: naming inconsistencies, redundant data, missing documentation
4. Flag directories that may be candidates for cleanup/deletion based on retention policies
5. Provide specific, actionable recommendations

## Response Format
Respond with a JSON object:
{{
  "summary": "Brief one-line summary of the directory",
  "status": "ok | needs_attention | recommend_delete",
  "recommendations": ["list", "of", "specific", "recommendations"],
  "deletion_candidate": {{
    "should_delete": false,
    "reason": "why or why not",
    "confidence": "low | medium | high"
  }}
}}
"""

USER_PROMPT_TEMPLATE = """Analyze this directory:

**Path**: {dir_path}
**Owner**: {member_email}
**Changed recently**: {is_changed}
**Stale (not modified recently)**: {is_stale}

{content_section}

Provide your analysis as JSON.
"""


class AnalyzerSkill(BaseSkill):
    name = "analyzer"
    description = "Analyze scan results using Claude and generate recommendations"
    phase = "analyze"

    def run(self, context: RunContext) -> SkillResult:
        if not context.scan_results:
            return SkillResult(success=True, message="No scan results to analyze")

        cfg = self.get_config(context.config)
        client = create_client(context.config.analyzer)

        system_prompt = self._build_system_prompt(context)
        analysis_results = []

        for scan_entry in context.scan_results:
            try:
                result = self._analyze_single(client, system_prompt, scan_entry)
                scan_entry["analysis"] = result
                analysis_results.append(scan_entry)

                context.db.update_scan_analysis(
                    scan_entry["id"], json.dumps(result)
                )

            except Exception as e:
                logger.error(f"Failed to analyze {scan_entry['dir_path']}: {e}")
                scan_entry["analysis"] = {
                    "summary": f"Analysis failed: {e}",
                    "status": "error",
                    "recommendations": [],
                    "deletion_candidate": {"should_delete": False, "reason": "analysis failed", "confidence": "low"},
                }
                analysis_results.append(scan_entry)

        context.analysis_results = analysis_results

        deletion_candidates = sum(
            1 for r in analysis_results
            if r.get("analysis", {}).get("deletion_candidate", {}).get("should_delete")
        )

        return SkillResult(
            success=True,
            message=f"Analyzed {len(analysis_results)} directories, {deletion_candidates} deletion candidates",
            data={"analyzed": len(analysis_results), "deletion_candidates": deletion_candidates},
        )

    def _build_system_prompt(self, context: RunContext) -> str:
        lab_context = yaml.dump(context.lab_context, default_flow_style=False)

        retention_lines = []
        for policy in context.config.retention_policies:
            line = f"- Pattern: {policy.pattern}, Action: {policy.action}"
            if policy.max_age_days:
                line += f", Max age: {policy.max_age_days} days"
            retention_lines.append(line)

        return SYSTEM_PROMPT_TEMPLATE.format(
            lab_context=lab_context,
            retention_policies="\n".join(retention_lines) or "No specific policies defined.",
        )

    def _analyze_single(
        self, client, system_prompt: str, scan_entry: Dict[str, Any]
    ) -> Dict[str, Any]:
        if scan_entry.get("readme_content"):
            content_section = f"**README Content**:\n```\n{scan_entry['readme_content']}\n```"
        elif scan_entry.get("dir_tree"):
            content_section = f"**Directory Structure** (no README found):\n```\n{scan_entry['dir_tree']}\n```"
        else:
            content_section = "**No README or directory listing available.**"

        user_prompt = USER_PROMPT_TEMPLATE.format(
            dir_path=scan_entry["dir_path"],
            member_email=scan_entry.get("member_email", "unknown"),
            is_changed=scan_entry.get("is_changed", False),
            is_stale=scan_entry.get("is_stale", False),
            content_section=content_section,
        )

        text = client.ask(system_prompt, user_prompt)
        return parse_json_response(text)
