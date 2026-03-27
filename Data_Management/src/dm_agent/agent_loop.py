"""LLM-driven agent loop — the 'brain' that decides what tools to call and when.

Usage:
    agent = AgentLoop(config, db)

    # One-shot: give it a goal, it figures out the steps
    result = agent.run("帮我清理一下 NACC 数据集的重复文件")

    # Interactive: maintains conversation history across calls
    agent.chat("NACC 有什么数据？")
    agent.chat("那些重复的 .nii 帮我清理一下")
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from dm_agent.base_skill import RunContext
from dm_agent.config import Config
from dm_agent.db import Database
from dm_agent.tool_registry import TOOL_DEFINITIONS

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are DM Agent, an AI agent that manages neuroimaging data on Yale's HPC cluster \
for Zhao Lab. You autonomously decide what actions to take to accomplish the user's goal.

## Your capabilities
- Scan storage for changes and stale data
- Deep-catalog datasets (modality, subjects, data stages, BIDS compliance)
- Search across all cataloged data with keywords
- Organize datasets: remove redundancies, restructure directories
- Generate documentation (README.md) for undocumented directories
- Monitor disk quota usage
- Create deletion requests with owner confirmation workflow
- Send email reports to lab members

## Available datasets
{datasets}

## Lab members
{members}

## Storage targets
{storage_targets}

## Safety guidelines
1. ALWAYS use dry_run=true before any organize operation — review the plan first
2. Use inspect_directory to look at directories before taking action
3. Deletions require owner confirmation via token — nothing is deleted immediately
4. For large operations, explain your plan before executing
5. If a tool fails, investigate the cause before retrying
6. Prefer using get_status and query_catalog to understand current state before acting

## Current time
{timestamp}
"""

MAX_TURNS = 25
MAX_RESULT_CHARS = 50000


class AgentLoop:
    """LLM-driven agent that reasons about what skills to invoke."""

    def __init__(self, config: Config, db: Database, model: Optional[str] = None):
        self.config = config
        self.db = db
        self.model = model or config.skills.get("agent", {}).get(
            "model", "claude-sonnet-4-20250514"
        )
        self._context: Optional[RunContext] = None
        self._skill_cache: Dict[str, Any] = {}
        self._messages: List[Dict[str, Any]] = []

        try:
            import anthropic

            self.client = anthropic.Anthropic()
        except ImportError:
            raise ImportError(
                "Agent mode requires the anthropic SDK. "
                "Install with: pip install -e '.[api]'"
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, user_goal: str) -> str:
        """One-shot: give it a goal, get a result. Fresh context each time."""
        messages = [{"role": "user", "content": user_goal}]
        return self._agent_loop(messages)

    def chat(self, user_message: str) -> str:
        """Interactive: maintains conversation history across calls."""
        self._messages.append({"role": "user", "content": user_message})
        result = self._agent_loop(self._messages)
        return result

    def reset(self) -> None:
        """Clear conversation history and cached context."""
        self._messages = []
        self._context = None

    # ------------------------------------------------------------------
    # Core agent loop
    # ------------------------------------------------------------------

    def _agent_loop(self, messages: List[Dict[str, Any]]) -> str:
        """Run the observe-reason-act loop until the agent is done."""
        system = self._build_system_prompt()

        for turn in range(MAX_TURNS):
            logger.info(f"Agent turn {turn + 1}/{MAX_TURNS}")

            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=system,
                tools=TOOL_DEFINITIONS,
                messages=messages,
            )

            # Append assistant response
            messages.append({"role": "assistant", "content": response.content})

            # If the model is done (no tool calls), return the text
            if response.stop_reason == "end_turn":
                return self._extract_text(response.content)

            if response.stop_reason == "max_tokens":
                logger.warning("Response truncated by max_tokens")
                return self._extract_text(response.content) + "\n\n(response truncated)"

            # Execute tool calls and feed results back
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    self._print_tool_call(block.name, block.input)
                    result = self._execute_tool(block.name, block.input)
                    self._print_tool_result(block.name, result)
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": self._format_result(result),
                        }
                    )

            if tool_results:
                messages.append({"role": "user", "content": tool_results})

        return "Reached maximum turns limit. Please break the task into smaller goals."

    # ------------------------------------------------------------------
    # System prompt construction
    # ------------------------------------------------------------------

    def _build_system_prompt(self) -> str:
        # Datasets from config
        cat_cfg = self.config.skills.get("data_cataloger", {})
        datasets_list = cat_cfg.get("datasets", [])
        datasets_str = "\n".join(
            f"- {d['name']}: {d['path']}" for d in datasets_list
        ) or "(none configured)"

        # Members
        members_str = "\n".join(
            f"- {m.name} ({m.email}) — projects: {', '.join(m.projects) or 'none'}"
            for m in self.config.members
        ) or "(none configured)"

        # Storage targets
        targets_str = "\n".join(
            f"- {t.path} — {t.description}" for t in self.config.scan_targets
        ) or "(none configured)"

        return SYSTEM_PROMPT.format(
            datasets=datasets_str,
            members=members_str,
            storage_targets=targets_str,
            timestamp=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        )

    # ------------------------------------------------------------------
    # Tool execution router
    # ------------------------------------------------------------------

    def _execute_tool(self, name: str, params: dict) -> Any:
        handlers = {
            "scan_storage": self._tool_scan_storage,
            "analyze_directories": self._tool_analyze_directories,
            "catalog_dataset": self._tool_catalog_dataset,
            "query_catalog": self._tool_query_catalog,
            "check_quota": self._tool_check_quota,
            "organize_dataset": self._tool_organize_dataset,
            "inspect_directory": self._tool_inspect_directory,
            "get_status": self._tool_get_status,
            "request_deletion": self._tool_request_deletion,
            "generate_readme": self._tool_generate_readme,
            "send_report": self._tool_send_report,
            "refresh_overview": self._tool_refresh_overview,
        }
        handler = handlers.get(name)
        if not handler:
            return {"error": f"Unknown tool: {name}"}
        try:
            return handler(params)
        except Exception as e:
            logger.error(f"Tool {name} failed: {e}", exc_info=True)
            return {"error": f"{type(e).__name__}: {e}"}

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _get_context(self) -> RunContext:
        """Get or create the shared RunContext for this session."""
        if self._context is None:
            self._context = RunContext(
                config=self.config,
                db=self.db,
                lab_context=self._build_lab_context(),
                run_timestamp=datetime.utcnow(),
            )
        return self._context

    def _build_lab_context(self) -> dict:
        return {
            "lab": self.config.lab,
            "members": [
                {
                    "name": m.name,
                    "email": m.email,
                    "projects": m.projects,
                    "role": m.role,
                }
                for m in self.config.members
            ],
            "projects": {
                name: {
                    "description": p.description,
                    "data_types": p.data_types,
                    "retention": p.retention,
                }
                for name, p in self.config.projects.items()
            },
        }

    def _get_skill(self, name: str):
        """Lazy-load and cache a skill instance."""
        if name not in self._skill_cache:
            from dm_agent.skills import discover_skills

            classes = discover_skills()
            if name not in classes:
                raise ValueError(f"Skill '{name}' not found")
            self._skill_cache[name] = classes[name]()
        return self._skill_cache[name]

    def _run_skill(self, name: str) -> dict:
        """Run a skill and return its result as a dict."""
        skill = self._get_skill(name)
        result = skill.run(self._get_context())
        return {"success": result.success, "message": result.message, **result.data}

    def _human_size(self, size_bytes: int) -> str:
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(size_bytes) < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}PB"

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_text(content) -> str:
        """Extract text from a list of content blocks."""
        parts = []
        for block in content:
            if hasattr(block, "text"):
                parts.append(block.text)
        return "".join(parts)

    @staticmethod
    def _format_result(result: Any) -> str:
        """Format a tool result for the Claude message."""
        if isinstance(result, (dict, list)):
            text = json.dumps(result, ensure_ascii=False, indent=2)
        else:
            text = str(result)
        if len(text) > MAX_RESULT_CHARS:
            text = text[:MAX_RESULT_CHARS] + "\n\n... (truncated)"
        return text

    @staticmethod
    def _print_tool_call(name: str, params: dict) -> None:
        params_str = ""
        if params:
            parts = [f"{k}={v!r}" for k, v in params.items()]
            params_str = ", ".join(parts)
        print(f"  > {name}({params_str})")

    @staticmethod
    def _print_tool_result(name: str, result: Any) -> None:
        if isinstance(result, dict) and "error" in result:
            print(f"    x {result['error']}")
        elif isinstance(result, dict):
            msg = result.get("message", "")
            if msg:
                print(f"    {msg[:120]}")

    # ------------------------------------------------------------------
    # Tool implementations
    # ------------------------------------------------------------------

    def _tool_scan_storage(self, params: dict) -> dict:
        return self._run_skill("scanner")

    def _tool_analyze_directories(self, params: dict) -> dict:
        ctx = self._get_context()
        if not ctx.scan_results:
            return {"error": "No scan results available. Run scan_storage first."}
        return self._run_skill("analyzer")

    def _tool_catalog_dataset(self, params: dict) -> dict:
        dataset_name = params["dataset"]

        # Find the dataset in config
        cat_cfg = self.config.skills.get("data_cataloger", {})
        datasets = cat_cfg.get("datasets", [])
        matched = [d for d in datasets if d["name"] == dataset_name]
        if not matched:
            return {
                "error": f"Dataset '{dataset_name}' not found in config.",
                "available": [d["name"] for d in datasets],
            }

        # Reset to pending so it gets re-cataloged
        ds_id = self.db.get_or_create_dataset(matched[0]["name"], matched[0]["path"])
        self.db.update_dataset_status(ds_id, "pending")

        # Temporarily override config to only catalog this dataset
        original_datasets = cat_cfg.get("datasets", [])
        self.config.skills["data_cataloger"]["datasets"] = matched

        try:
            result = self._run_skill("data_cataloger")
        finally:
            self.config.skills["data_cataloger"]["datasets"] = original_datasets

        return result

    def _tool_query_catalog(self, params: dict) -> dict:
        keywords = params["keywords"].lower().split()
        datasets = self.db.get_all_datasets()
        cataloged = [d for d in datasets if d["status"] == "cataloged"]

        if not cataloged:
            return {"error": "No datasets cataloged yet. Run catalog_dataset first."}

        matches = []
        for ds in cataloged:
            mod_stats = self.db.get_modality_stats(ds["id"])
            entries = self.db.get_catalog_entries(ds["id"])

            # Build dataset-level search text
            mod_desc_text = " ".join(
                (ms.get("description") or "") + " " + (ms.get("notes") or "")
                for ms in mod_stats
            ).lower()

            ds_text = " ".join(
                [
                    ds["dataset_name"].lower(),
                    (ds.get("summary") or "").lower(),
                    (ds.get("modalities") or "").lower(),
                    (ds.get("organization_scheme") or "").lower(),
                    mod_desc_text,
                ]
            )

            ds_score = sum(1 for kw in keywords if kw in ds_text)

            # Entry-level matches
            entry_matches = []
            for e in entries:
                entry_text = " ".join(
                    [
                        e["rel_path"].lower(),
                        (e.get("modality") or "").lower(),
                        (e.get("data_stage") or "").lower(),
                        (e.get("file_types") or "").lower(),
                        (e.get("sample_files") or "").lower(),
                    ]
                )
                entry_score = sum(1 for kw in keywords if kw in entry_text)
                if entry_score > 0:
                    entry_matches.append(
                        {
                            "rel_path": e["rel_path"],
                            "abs_path": os.path.join(ds["root_path"], e["rel_path"]),
                            "modality": e.get("modality"),
                            "data_stage": e.get("data_stage"),
                            "file_count": e["file_count"],
                            "size": self._human_size(e["size_bytes"]),
                            "score": entry_score,
                        }
                    )

            if ds_score > 0 or entry_matches:
                matches.append(
                    {
                        "dataset": ds["dataset_name"],
                        "root_path": ds["root_path"],
                        "total_size": self._human_size(ds["total_size_bytes"] or 0),
                        "total_subjects": ds["total_subjects"] or 0,
                        "modalities": ds.get("modalities", "[]"),
                        "summary": (ds.get("summary") or "")[:300],
                        "ds_score": ds_score,
                        "matching_dirs": sorted(
                            entry_matches, key=lambda x: -x["score"]
                        )[:15],
                        "modality_stats": [
                            {
                                "modality": ms["modality"],
                                "subject_count": ms["subject_count"],
                                "file_count": ms["file_count"],
                                "size": self._human_size(ms["size_bytes"] or 0),
                            }
                            for ms in mod_stats
                        ],
                    }
                )

        matches.sort(key=lambda x: -(x["ds_score"] + len(x["matching_dirs"])))
        return {"query": params["keywords"], "result_count": len(matches), "results": matches}

    def _tool_check_quota(self, params: dict) -> dict:
        return self._run_skill("quota_monitor")

    def _tool_organize_dataset(self, params: dict) -> dict:
        dataset_name = params["dataset"]
        dry_run = params.get("dry_run", True)

        self.config.skills.setdefault("dataset_organizer", {})
        self.config.skills["dataset_organizer"]["enabled"] = True
        self.config.skills["dataset_organizer"]["target_dataset"] = dataset_name
        self.config.skills["dataset_organizer"]["dry_run"] = dry_run
        self.config.skills["dataset_organizer"]["no_reorganize"] = params.get(
            "no_reorganize", False
        )

        # Clear cache so it picks up new config
        self._skill_cache.pop("dataset_organizer", None)

        result = self._run_skill("dataset_organizer")

        # Tag the result so the agent knows which mode it ran in
        result["dry_run"] = dry_run
        return result

    def _tool_inspect_directory(self, params: dict) -> dict:
        path = params["path"]
        max_depth = params.get("max_depth", 3)

        if not os.path.isdir(path):
            return {"error": f"Directory not found: {path}"}

        # Get directory tree (dirs and files)
        try:
            tree_proc = subprocess.run(
                ["find", path, "-maxdepth", str(max_depth)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            all_entries = tree_proc.stdout.strip().split("\n") if tree_proc.stdout.strip() else []
        except subprocess.TimeoutExpired:
            all_entries = ["(timed out listing directory)"]
        except Exception as e:
            all_entries = [f"(error: {e})"]

        # Count files only
        try:
            file_proc = subprocess.run(
                ["find", path, "-maxdepth", str(max_depth), "-type", "f"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            file_count = len(file_proc.stdout.strip().split("\n")) if file_proc.stdout.strip() else 0
        except Exception:
            file_count = -1

        # Total size
        try:
            du_proc = subprocess.run(
                ["du", "-sh", path],
                capture_output=True,
                text=True,
                timeout=60,
            )
            total_size = du_proc.stdout.split()[0] if du_proc.returncode == 0 else "unknown"
        except Exception:
            total_size = "unknown"

        # Truncate tree for context window
        truncated = len(all_entries) > 200
        tree_display = all_entries[:200]

        return {
            "path": path,
            "total_size": total_size,
            "file_count": file_count,
            "entries_shown": len(tree_display),
            "truncated": truncated,
            "tree": tree_display,
        }

    def _tool_get_status(self, params: dict) -> dict:
        datasets = self.db.get_all_datasets()
        confirmed_dels = self.db.get_confirmed_deletions()

        ds_info = []
        for ds in datasets:
            info: Dict[str, Any] = {
                "name": ds["dataset_name"],
                "status": ds["status"],
                "path": ds["root_path"],
            }
            if ds["status"] == "cataloged":
                info["total_size"] = self._human_size(ds["total_size_bytes"] or 0)
                info["total_files"] = ds["total_files"]
                info["total_subjects"] = ds["total_subjects"]
                info["modalities"] = ds.get("modalities")
                info["bids_compliant"] = bool(ds.get("bids_compliant"))
            ds_info.append(info)

        # Scan state
        scan_states = []
        for target in self.config.scan_targets:
            ts = self.db.get_last_scan_ts(target.path)
            scan_states.append(
                {
                    "path": target.path,
                    "description": target.description,
                    "last_scan": ts or "never",
                }
            )

        # Pending deletions per member
        pending = {}
        for m in self.config.members:
            p = self.db.get_pending_deletions_for_email(m.email)
            if p:
                pending[m.name] = [
                    {"dir": d["dir_path"], "token": d["token"], "expires": d["expires_at"]}
                    for d in p
                ]

        return {
            "datasets": ds_info,
            "scan_targets": scan_states,
            "confirmed_deletions_ready": len(confirmed_dels),
            "pending_deletions_by_member": pending,
        }

    def _tool_request_deletion(self, params: dict) -> dict:
        dir_path = params["dir_path"]
        reason = params["reason"]

        if not os.path.isdir(dir_path):
            return {"error": f"Directory not found: {dir_path}"}

        # Estimate size
        size_bytes = None
        try:
            du_proc = subprocess.run(
                ["du", "-sb", dir_path],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if du_proc.returncode == 0:
                size_bytes = int(du_proc.stdout.split()[0])
        except Exception:
            pass

        # Find owner by matching project patterns in path
        owner_email = ""
        for member in self.config.members:
            for proj in member.projects:
                if proj.lower() in dir_path.lower():
                    owner_email = member.email
                    break
            if owner_email:
                break

        if not owner_email:
            # Fall back to first admin
            if self.config.members:
                owner_email = self.config.members[0].email
            else:
                return {
                    "error": "Cannot determine owner for this directory. No lab members configured."
                }

        # Find matching scan target
        target_path = ""
        for target in self.config.scan_targets:
            if dir_path.startswith(target.path):
                target_path = target.path
                break

        token = str(uuid.uuid4())
        expiry = datetime.utcnow() + timedelta(
            days=self.config.confirmation.expiry_days
        )

        self.db.create_deletion_request(
            token=token,
            target_path=target_path,
            dir_path=dir_path,
            reason=reason,
            size_bytes=size_bytes,
            owner_email=owner_email,
            expires_at=expiry.isoformat(),
        )

        return {
            "success": True,
            "message": f"Deletion request created. Owner must confirm with: dm-agent confirm {token}",
            "token": token,
            "dir_path": dir_path,
            "owner_email": owner_email,
            "size": self._human_size(size_bytes) if size_bytes else "unknown",
            "expires_at": expiry.isoformat(),
        }

    def _tool_generate_readme(self, params: dict) -> dict:
        dir_path = params["dir_path"]

        if not os.path.isdir(dir_path):
            return {"error": f"Directory not found: {dir_path}"}

        # Get directory tree
        try:
            tree_proc = subprocess.run(
                ["find", dir_path, "-maxdepth", "3", "-type", "f"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            tree = tree_proc.stdout.strip()
        except Exception:
            tree = "(could not list directory)"

        # Generate README with Claude
        from dm_agent.claude_client import create_client

        client = create_client(self.config.analyzer)

        system = (
            "You are a documentation assistant for a neuroimaging data lab. "
            "Generate a concise, informative README.md for the given directory. "
            "Include: purpose, data organization, file formats, and usage notes."
        )
        user_prompt = (
            f"Directory: {dir_path}\n\n"
            f"File listing (max depth 3):\n{tree[:8000]}\n\n"
            f"Generate a README.md for this directory."
        )

        readme_content = client.ask(system, user_prompt)

        return {
            "success": True,
            "message": "README generated (not written to disk).",
            "dir_path": dir_path,
            "readme_content": readme_content,
        }

    def _tool_send_report(self, params: dict) -> dict:
        ctx = self._get_context()
        if not ctx.analysis_results:
            return {
                "error": "No analysis results available. Run scan_storage and analyze_directories first."
            }
        return self._run_skill("reporter")

    def _tool_refresh_overview(self, params: dict) -> dict:
        return self._run_skill("lab_overview")
