"""README generator skill — auto-generate or suggest README updates for directories."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import yaml

from dm_agent.base_skill import BaseSkill, RunContext, SkillResult
from dm_agent.claude_client import create_client

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = "You are a technical documentation writer for an HPC lab. Generate concise, practical README files."

GENERATE_PROMPT = """Based on the following directory information, generate a concise README.md file.

**Directory Path**: {dir_path}
**Project**: {project_name}
**Owner**: {member_name} ({member_email})

**Directory Contents**:
```
{dir_tree}
```

**Lab Context**:
{lab_context}

Generate a README.md that includes:
1. A title (directory name)
2. Brief description of what this directory likely contains
3. File/folder structure overview
4. Data types present
5. Owner/contact info
6. Date generated

Keep it concise and practical. Output ONLY the README content in markdown format, no extra commentary.
"""


class ReadmeGeneratorSkill(BaseSkill):
    name = "readme_generator"
    description = "Auto-generate or suggest README updates for directories lacking documentation"
    phase = "analyze"

    def run(self, context: RunContext) -> SkillResult:
        if not context.scan_results:
            return SkillResult(success=True, message="No scan results to process")

        cfg = self.get_config(context.config)
        auto_write = cfg.get("auto_write", False)
        client = create_client(context.config.analyzer)
        suggestions = []

        for entry in context.scan_results:
            dir_path = entry["dir_path"]

            readme_path = os.path.join(dir_path, "README.md")
            needs_readme = not os.path.exists(readme_path)
            needs_update = False

            if not needs_readme:
                needs_update = self._is_readme_stale(readme_path, dir_path)
                if not needs_update:
                    continue

            dir_tree = entry.get("dir_tree") or self._get_dir_tree(dir_path)
            if not dir_tree:
                continue

            member_name, member_email = self._find_member(entry, context)
            project_name = self._find_project(dir_path, context)

            try:
                prompt = GENERATE_PROMPT.format(
                    dir_path=dir_path,
                    project_name=project_name or "unknown",
                    member_name=member_name or "unknown",
                    member_email=member_email or "unknown",
                    dir_tree=dir_tree,
                    lab_context=yaml.dump(context.lab_context, default_flow_style=False),
                )

                readme_content = client.ask(SYSTEM_PROMPT, prompt)

                if auto_write:
                    with open(readme_path, "w", encoding="utf-8") as f:
                        f.write(readme_content)
                    logger.info(f"Wrote README: {readme_path}")
                    suggestions.append({
                        "dir_path": dir_path,
                        "member_email": member_email,
                        "action": "written",
                        "suggestion": f"README.md auto-generated at {readme_path}",
                    })
                else:
                    action = "create" if needs_readme else "update"
                    suggestions.append({
                        "dir_path": dir_path,
                        "member_email": member_email,
                        "action": action,
                        "suggestion": f"Consider {'adding' if needs_readme else 'updating'} README.md for this directory",
                        "generated_content": readme_content,
                    })

            except Exception as e:
                logger.error(f"Failed to generate README for {dir_path}: {e}")

        context.readme_suggestions = suggestions

        return SkillResult(
            success=True,
            message=f"Generated {len(suggestions)} README suggestions",
            data={"suggestions": len(suggestions)},
        )

    def _is_readme_stale(self, readme_path: str, dir_path: str) -> bool:
        try:
            readme_mtime = os.path.getmtime(readme_path)
            for root, dirs, files in os.walk(dir_path):
                for f in files:
                    if f.startswith("README"):
                        continue
                    fp = os.path.join(root, f)
                    try:
                        if os.path.getmtime(fp) > readme_mtime:
                            return True
                    except OSError:
                        continue
                break
        except OSError:
            pass
        return False

    def _find_member(self, entry: Dict, context: RunContext) -> tuple:
        email = entry.get("member_email", "")
        for m in context.config.members:
            if m.email == email:
                return m.name, m.email
        return "", email

    def _find_project(self, dir_path: str, context: RunContext) -> str:
        dir_name = os.path.basename(dir_path).lower()
        for proj_name in context.config.projects:
            if proj_name.lower() in dir_name or dir_name in proj_name.lower():
                return proj_name
        return ""

    def _get_dir_tree(self, dir_path: str) -> str:
        import subprocess
        try:
            result = subprocess.run(
                ["find", dir_path, "-maxdepth", "2", "-type", "f"],
                capture_output=True, text=True, timeout=30,
            )
            files = result.stdout.strip().split("\n")[:100]
            lines = []
            for f in files:
                if f:
                    lines.append(os.path.relpath(f, dir_path))
            return "\n".join(lines)
        except Exception:
            return ""
