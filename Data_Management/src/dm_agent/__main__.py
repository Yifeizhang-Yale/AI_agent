"""CLI entry point for the HPC Data Management Agent."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

from dm_agent.config import Config, MemberConfig, load_config
from dm_agent.db import Database
from dm_agent.orchestrator import Orchestrator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("dm_agent")


def get_current_user() -> str:
    """Get the current HPC username."""
    return os.environ.get("USER", os.environ.get("LOGNAME", ""))


def require_admin(config: Config, username: str) -> None:
    """Exit with error if user is not an admin."""
    if not config.is_admin(username):
        print(f"Permission denied: '{username}' is not an admin.")
        print(f"Admin users: {', '.join(config.admin_users) or '(none configured)'}")
        sys.exit(1)


def resolve_current_member(config: Config, username: str) -> Optional[MemberConfig]:
    """Resolve current user to a lab member."""
    return config.get_member_by_username(username)


# --- Commands ---


def cmd_run(args: argparse.Namespace) -> int:
    """Run the full weekly scan/analyze/report/cleanup cycle. Admin only."""
    config = load_config(args.config)
    require_admin(config, get_current_user())

    db = Database(config.database_path)
    orchestrator = Orchestrator(config, db)
    return orchestrator.run_full_cycle()


def cmd_confirm(args: argparse.Namespace) -> int:
    """Confirm a deletion request by token. Members can only confirm their own."""
    config = load_config(args.config)
    db = Database(config.database_path)
    username = get_current_user()

    # Check token ownership (admins can confirm any, members only their own)
    if not config.is_admin(username):
        member = resolve_current_member(config, username)
        if not member:
            print(f"Permission denied: '{username}' is not a recognized lab member.")
            print("Ask your admin to add your hpc_username to lab_context.yaml.")
            return 1

        request = db.get_deletion_request_by_token(args.token)
        if request and request["owner_email"] != member.email:
            print(f"Permission denied: this deletion belongs to {request['owner_email']}, not you.")
            return 1

    if db.confirm_deletion(args.token):
        print(f"Deletion confirmed for token: {args.token}")
        print("It will be executed in the next scheduled run.")
        return 0
    else:
        print(f"Token not found, already confirmed, or expired: {args.token}")
        return 1


def cmd_check_replies(args: argparse.Namespace) -> int:
    """Check email replies for deletion confirmations. Admin only."""
    config = load_config(args.config)
    require_admin(config, get_current_user())

    db = Database(config.database_path)
    orchestrator = Orchestrator(config, db)
    count = orchestrator.check_email_confirmations()
    print(f"Processed {count} email confirmations.")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show agent status. Members see only their own data; admins see everything."""
    config = load_config(args.config)
    db = Database(config.database_path)
    username = get_current_user()
    is_admin = config.is_admin(username)
    member = resolve_current_member(config, username)

    if not is_admin and not member:
        print(f"Permission denied: '{username}' is not a recognized lab member.")
        print("Ask your admin to add your hpc_username to lab_context.yaml.")
        return 1

    # Scan state (admins only)
    if is_admin:
        print("=== Scan State ===")
        for target in config.scan_targets:
            ts = db.get_last_scan_ts(target.path)
            print(f"  {target.path}: last scanned {ts or 'never'}")
        print()

    # Pending deletions
    print("=== Pending Deletions ===")
    if is_admin:
        # Admins see all members
        members_to_show = config.members
    else:
        # Members see only themselves
        members_to_show = [member]

    has_pending = False
    for m in members_to_show:
        pending = db.get_pending_deletions_for_email(m.email)
        if pending:
            has_pending = True
            print(f"  {m.name} ({m.email}): {len(pending)} pending")
            for p in pending:
                print(f"    - {p['dir_path']}")
                print(f"      Token: {p['token']}")
                print(f"      Reason: {p.get('reason', 'N/A')}")
                print(f"      Expires: {p['expires_at']}")
                print(f"      Confirm: dm-agent confirm {p['token']}")
                print()

    if not has_pending:
        print("  No pending deletions.")

    return 0


def cmd_catalog(args: argparse.Namespace) -> int:
    """Run data cataloging for the next uncataloged dataset. Admin only."""
    config = load_config(args.config)
    require_admin(config, get_current_user())

    db = Database(config.database_path)

    # If a specific dataset is given, temporarily override config to only catalog that one
    if args.dataset:
        cfg = config.skills.get("data_cataloger", {})
        datasets = cfg.get("datasets", [])
        matched = [d for d in datasets if d["name"] == args.dataset]
        if not matched:
            print(f"Dataset '{args.dataset}' not found in config. Available:")
            for d in datasets:
                print(f"  - {d['name']} ({d['path']})")
            return 1
        # Reset it to pending so it gets re-cataloged
        ds_id = db.get_or_create_dataset(matched[0]["name"], matched[0]["path"])
        db.update_dataset_status(ds_id, "pending")
        # Override config to only include this dataset so run() picks it
        config.skills["data_cataloger"]["datasets"] = matched

    from datetime import datetime
    from dm_agent.base_skill import RunContext
    from dm_agent.skills.data_cataloger import DataCatalogerSkill

    context = RunContext(
        config=config,
        db=db,
        lab_context=_build_lab_context(config),
        run_timestamp=datetime.utcnow(),
    )

    skill = DataCatalogerSkill()
    result = skill.run(context)
    print(result.message)

    if result.data.get("dataset"):
        print(f"\nDataset: {result.data['dataset']}")
        print(f"  Directories: {result.data.get('dirs', 0)}")
        print(f"  Files: {result.data.get('files', 0)}")
        print(f"  Size: {_human_size(result.data.get('size', 0))}")
        print(f"  Subjects: {result.data.get('subjects', 0)}")
        print(f"  Modalities: {', '.join(result.data.get('modalities', []))}")

    # Refresh lab overview whenever catalog data changes
    if result.success:
        _refresh_lab_overview(context)

    return 0 if result.success else 1


def cmd_catalog_report(args: argparse.Namespace) -> int:
    """Show catalog report for all or a specific dataset."""
    import json

    config = load_config(args.config)
    db = Database(config.database_path)

    datasets = db.get_all_datasets()
    if not datasets:
        print("No datasets cataloged yet. Run 'dm-agent catalog' first.")
        return 0

    if args.dataset:
        datasets = [d for d in datasets if d["dataset_name"] == args.dataset]
        if not datasets:
            print(f"Dataset '{args.dataset}' not found in catalog.")
            return 1

    for ds in datasets:
        print(f"\n{'=' * 60}")
        print(f"Dataset: {ds['dataset_name']}")
        print(f"Path: {ds['root_path']}")
        print(f"Status: {ds['status']}")
        print(f"{'=' * 60}")

        if ds["status"] != "cataloged":
            print("  (not yet cataloged)")
            continue

        print(f"  Size: {_human_size(ds['total_size_bytes'] or 0)}")
        print(f"  Files: {ds['total_files'] or 0}")
        print(f"  Subjects: {ds['total_subjects'] or 0}")
        print(f"  Modalities: {ds['modalities'] or '[]'}")
        print(f"  Organization: {ds['organization_scheme'] or 'unknown'}")
        print(f"  Raw data: {'Yes' if ds['has_raw'] else 'No'}")
        print(f"  Preprocessed: {'Yes' if ds['has_preprocessed'] else 'No'}")
        print(f"  Derivatives: {'Yes' if ds['has_derivatives'] else 'No'}")
        print(f"  BIDS compliant: {'Yes' if ds['bids_compliant'] else 'No'}")

        if ds["summary"]:
            print(f"\n  Summary:\n    {ds['summary']}")

        # Per-modality breakdown
        mod_stats = db.get_modality_stats(ds["id"])
        if mod_stats:
            print(f"\n  Modality Breakdown ({len(mod_stats)} modalities):")
            print(f"    {'Modality':<10} {'Subjects':>10} {'Files':>10} {'Size':>10} {'Dirs':>6}  {'Stages':<30}  Description")
            print(f"    {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*6}  {'-'*30}  {'-'*20}")
            for ms in mod_stats:
                stages = json.loads(ms.get("data_stages") or "[]")
                desc = (ms.get("description") or "")[:50]
                subj_count = ms.get("subject_count") or 0
                file_count = ms.get("file_count") or 0
                size_bytes = ms.get("size_bytes") or 0
                dir_count = ms.get("dir_count") or 0
                print(
                    f"    {ms['modality']:<10} {subj_count:>10} {file_count:>10} "
                    f"{_human_size(size_bytes):>10} {dir_count:>6}  "
                    f"{', '.join(stages):<30}  {desc}"
                )

            # Show subject lists if requested
            if args.subjects:
                print(f"\n  Subject Lists by Modality:")
                for ms in mod_stats:
                    subjects = json.loads(ms.get("subject_list") or "[]")
                    if subjects:
                        print(f"\n    {ms['modality']} ({len(subjects)} subjects):")
                        # Print in columns for readability
                        cols = 5
                        for i in range(0, len(subjects), cols):
                            row = subjects[i:i+cols]
                            print(f"      {' | '.join(f'{s:<20}' for s in row)}")
                    else:
                        print(f"\n    {ms['modality']}: (no subjects detected)")

        if ds["recommendations"]:
            recs = json.loads(ds["recommendations"])
            if recs:
                # Handle both old format (list) and new format (dict with recommendations key)
                rec_list = recs if isinstance(recs, list) else recs.get("recommendations", [])
                if rec_list:
                    print(f"\n  Recommendations ({len(rec_list)}):")
                    for i, rec in enumerate(rec_list, 1):
                        priority = rec.get("priority", "medium").upper()
                        print(f"    {i}. [{priority}] {rec.get('action', '')}")
                        if rec.get("reason"):
                            print(f"       Reason: {rec['reason']}")
                        if rec.get("example"):
                            print(f"       Example: {rec['example']}")

        # Show entry breakdown
        entries = db.get_catalog_entries(ds["id"])
        if entries and args.verbose:
            print(f"\n  Directory breakdown ({len(entries)} entries):")
            for e in entries:
                flags = []
                if e["entry_type"] != "other":
                    flags.append(e["entry_type"])
                if e["data_stage"] != "unknown":
                    flags.append(e["data_stage"])
                if e["modality"]:
                    flags.append(e["modality"])
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                print(f"    {e['rel_path']}{flag_str} ({e['file_count']} files, {_human_size(e['size_bytes'])})")

    return 0


def cmd_query(args: argparse.Namespace) -> int:
    """Search the data catalog and return structured results for Claude Code to interpret."""
    import json as _json
    import re

    config = load_config(args.config)
    db = Database(config.database_path)

    datasets = db.get_all_datasets()
    cataloged = [d for d in datasets if d["status"] == "cataloged"]

    if not cataloged:
        print("No datasets cataloged yet. Run 'dm-agent catalog' first.")
        return 1

    keywords = args.keywords.lower().split()
    output_format = args.format

    # Search across all datasets and entries
    matches = []
    for ds in cataloged:
        entries = db.get_catalog_entries(ds["id"])
        mod_stats = db.get_modality_stats(ds["id"])

        # Build comprehensive dataset-level search text
        # Include modality descriptions — they contain rich detail like
        # "structural connectivity matrices", "VBM", "Schaefer atlas", etc.
        mod_desc_text = " ".join(
            (ms.get("description") or "") + " " + (ms.get("notes") or "")
            for ms in mod_stats
        ).lower()
        recs_json = ds.get("recommendations") or ""
        readme_text = ""
        if recs_json:
            try:
                recs_data = _json.loads(recs_json)
                readme_text = (recs_data.get("readme_content") or "").lower()
            except (ValueError, TypeError):
                pass

        ds_text = " ".join([
            ds["dataset_name"].lower(),
            (ds.get("summary") or "").lower(),
            (ds.get("modalities") or "").lower(),
            (ds.get("organization_scheme") or "").lower(),
            mod_desc_text,
            readme_text,
        ])

        # Check dataset-level match
        ds_score = sum(1 for kw in keywords if kw in ds_text)

        # Check entry-level matches
        entry_matches = []
        for e in entries:
            entry_text = " ".join([
                e["rel_path"].lower(),
                (e.get("modality") or "").lower(),
                (e.get("data_stage") or "").lower(),
                (e.get("entry_type") or "").lower(),
                (e.get("file_types") or "").lower(),
                (e.get("sample_files") or "").lower(),
            ])
            entry_score = sum(1 for kw in keywords if kw in entry_text)
            if entry_score > 0:
                entry_matches.append({
                    "rel_path": e["rel_path"],
                    "abs_path": os.path.join(ds["root_path"], e["rel_path"]),
                    "modality": e.get("modality"),
                    "data_stage": e.get("data_stage"),
                    "entry_type": e.get("entry_type"),
                    "file_count": e["file_count"],
                    "size": _human_size(e["size_bytes"]),
                    "size_bytes": e["size_bytes"],
                    "sample_files": e.get("sample_files", "[]"),
                    "score": entry_score,
                })

        if ds_score > 0 or entry_matches:
            matches.append({
                "dataset": ds["dataset_name"],
                "root_path": ds["root_path"],
                "total_size": _human_size(ds["total_size_bytes"] or 0),
                "total_files": ds["total_files"] or 0,
                "subjects": ds["total_subjects"] or 0,
                "modalities": ds.get("modalities", "[]"),
                "organization": ds.get("organization_scheme", "unknown"),
                "has_raw": bool(ds["has_raw"]),
                "has_preprocessed": bool(ds["has_preprocessed"]),
                "has_derivatives": bool(ds["has_derivatives"]),
                "summary": ds.get("summary", ""),
                "ds_score": ds_score,
                "matching_dirs": sorted(entry_matches, key=lambda x: -x["score"]),
                "modality_stats": [
                    {
                        "modality": ms["modality"],
                        "subject_count": ms["subject_count"],
                        "file_count": ms["file_count"],
                        "size": _human_size(ms["size_bytes"] or 0),
                    }
                    for ms in mod_stats
                ],
            })

    # Sort by relevance
    matches.sort(key=lambda x: -(x["ds_score"] + len(x["matching_dirs"])))

    if output_format == "json":
        print(_json.dumps(matches, indent=2))
    else:
        # Human-readable output
        if not matches:
            print(f"No results for: {' '.join(keywords)}")
            print(f"\nAvailable datasets: {', '.join(d['dataset_name'] for d in cataloged)}")
            return 0

        print(f"\nSearch: {' '.join(keywords)}")
        print(f"{'=' * 60}")

        for m in matches:
            print(f"\n## {m['dataset']}")
            print(f"   Path: {m['root_path']}")
            print(f"   Size: {m['total_size']} | Files: {m['total_files']} | Subjects: {m['subjects']}")
            print(f"   Modalities: {m['modalities']}")

            # Show per-modality subject counts if available
            if m.get("modality_stats"):
                print(f"   Per-modality subjects:")
                for ms in m["modality_stats"]:
                    print(f"     {ms['modality']}: {ms['subject_count']} subjects, {ms['file_count']} files")
            if m["summary"]:
                # Truncate summary to 200 chars
                s = m["summary"][:200] + ("..." if len(m["summary"]) > 200 else "")
                print(f"   Summary: {s}")

            if m["matching_dirs"]:
                print(f"\n   Matching directories ({len(m['matching_dirs'])}):")
                for d in m["matching_dirs"][:15]:
                    flags = []
                    if d["modality"]:
                        flags.append(d["modality"])
                    if d["data_stage"] and d["data_stage"] != "unknown":
                        flags.append(d["data_stage"])
                    flag_str = f" [{', '.join(flags)}]" if flags else ""
                    print(f"     {d['rel_path']}{flag_str} — {d['file_count']} files, {d['size']}")

                    # Show sample files
                    try:
                        samples = _json.loads(d["sample_files"]) if isinstance(d["sample_files"], str) else d["sample_files"]
                        if samples:
                            relevant = [f for f in samples if any(kw in f.lower() for kw in keywords)]
                            if relevant:
                                print(f"       matched files: {', '.join(relevant[:5])}")
                            elif len(keywords) > 0:
                                print(f"       sample files: {', '.join(samples[:3])}")
                    except (ValueError, TypeError):
                        pass

                if len(m["matching_dirs"]) > 15:
                    print(f"     ... and {len(m['matching_dirs']) - 15} more")

        print()

    return 0


def cmd_organize(args: argparse.Namespace) -> int:
    """Reorganize a dataset and remove redundant files. Admin only."""
    config = load_config(args.config)
    require_admin(config, get_current_user())

    db = Database(config.database_path)

    # Set up the organizer config dynamically
    config.skills.setdefault("dataset_organizer", {})
    config.skills["dataset_organizer"]["enabled"] = True
    config.skills["dataset_organizer"]["target_dataset"] = args.dataset
    config.skills["dataset_organizer"]["dry_run"] = args.dry_run
    config.skills["dataset_organizer"]["no_reorganize"] = args.no_reorganize

    from datetime import datetime
    from dm_agent.base_skill import RunContext
    from dm_agent.skills.dataset_organizer import DatasetOrganizerSkill

    context = RunContext(
        config=config,
        db=db,
        lab_context=_build_lab_context(config),
        run_timestamp=datetime.utcnow(),
    )

    skill = DatasetOrganizerSkill()
    result = skill.run(context)
    print(result.message)

    if result.data:
        r = result.data.get("redundancy", {})
        o = result.data.get("reorganize", {})
        print(f"\nRedundancy: checked {r.get('checked', 0)}, "
              f"{'would delete' if args.dry_run else 'deleted'} {r.get('deleted', 0)}, "
              f"saved {_human_size(r.get('size_saved', 0))}, "
              f"skipped {r.get('skipped', 0)}")
        print(f"Reorganize: {'would move' if args.dry_run else 'moved'} {o.get('moved', 0)}, "
              f"created {o.get('created', 0)}, skipped {o.get('skipped', 0)}")
        if result.data.get("plan_summary"):
            print(f"\nPlan: {result.data['plan_summary']}")

    # Refresh lab overview when data actually changes (not dry-run)
    if result.success and not args.dry_run:
        _refresh_lab_overview(context)

    return 0 if result.success else 1


def cmd_participants(args: argparse.Namespace) -> int:
    """Generate participants.tsv for a dataset from catalog data."""
    import csv as _csv
    import json as _json

    config = load_config(args.config)
    db = Database(config.database_path)

    # Find the dataset
    datasets = db.get_all_datasets()
    ds = next((d for d in datasets if d["dataset_name"] == args.dataset), None)
    if not ds:
        print(f"Dataset '{args.dataset}' not found. Available:")
        for d in datasets:
            print(f"  - {d['dataset_name']}")
        return 1
    if ds["status"] != "cataloged":
        print(f"Dataset '{args.dataset}' not yet cataloged. Run 'catalog --dataset {args.dataset}' first.")
        return 1

    # Collect per-modality subject lists from DB
    mod_stats = db.get_modality_stats(ds["id"])
    modalities_with_ids = {}   # modality → set of subject IDs
    modalities_count_only = {} # modality → count (no individual IDs)

    for ms in mod_stats:
        mod = ms["modality"]
        slist = _json.loads(ms.get("subject_list") or "[]")
        count = ms["subject_count"] or 0
        if slist:
            modalities_with_ids[mod] = set(slist)
        elif count > 0:
            modalities_count_only[mod] = count

    # Supplement from catalog_entries: subject_id × modality from directory-level data
    entries = db.get_catalog_entries(ds["id"])
    for e in entries:
        sid = e.get("subject_id")
        mod = e.get("modality")
        if sid and mod:
            modalities_with_ids.setdefault(mod, set()).add(sid)
            modalities_count_only.pop(mod, None)  # promote to full list

    if not modalities_with_ids:
        print(f"No per-subject data available for {args.dataset}.")
        print("Subject lists are empty — the catalog only has aggregate counts.")
        if modalities_count_only:
            print(f"\nAggregate counts (no individual IDs):")
            for mod, cnt in sorted(modalities_count_only.items()):
                print(f"  {mod}: {cnt} subjects")
        return 1

    # Build union of all subjects
    all_subjects = set()
    for sids in modalities_with_ids.values():
        all_subjects.update(sids)

    # Sort modalities and subjects
    mod_names = sorted(modalities_with_ids.keys())
    sorted_subjects = sorted(all_subjects)

    # Build the cross-table
    default_dir = os.path.dirname(config.database_path)
    output_path = args.output or os.path.join(
        default_dir, "participants", f"{args.dataset}_participants.tsv"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", newline="") as f:
        writer = _csv.writer(f, delimiter="\t")
        writer.writerow(["participant_id"] + mod_names)
        for sid in sorted_subjects:
            row = [sid]
            for mod in mod_names:
                row.append(1 if sid in modalities_with_ids[mod] else 0)
            writer.writerow(row)

    # Summary
    print(f"Generated: {output_path}")
    print(f"  Subjects: {len(sorted_subjects)}")
    print(f"  Modalities with per-subject data ({len(mod_names)}):")
    for mod in mod_names:
        print(f"    {mod}: {len(modalities_with_ids[mod])} subjects")

    if modalities_count_only:
        print(f"\n  Modalities with counts only — no per-subject IDs ({len(modalities_count_only)}):")
        for mod, cnt in sorted(modalities_count_only.items()):
            print(f"    {mod}: ~{cnt} subjects (not in participants.tsv)")

    # Cross-modality stats
    if len(mod_names) > 1:
        print(f"\n  Cross-modality overlap:")
        for i, m1 in enumerate(mod_names):
            for m2 in mod_names[i + 1:]:
                overlap = len(modalities_with_ids[m1] & modalities_with_ids[m2])
                if overlap > 0:
                    print(f"    {m1} ∩ {m2}: {overlap} subjects")

    return 0


def cmd_agent(args: argparse.Namespace) -> int:
    """Interactive agent mode — give it natural language goals."""
    from dm_agent.agent_loop import AgentLoop

    config = load_config(args.config)
    db = Database(config.database_path)
    agent = AgentLoop(config, db, model=args.model)

    if args.goal:
        # One-shot mode
        print(agent.run(args.goal))
        return 0

    # Interactive REPL
    print("DM Agent interactive mode")
    print("Type your goal in natural language. Type 'exit' to quit, 'reset' to clear history.\n")

    while True:
        try:
            goal = input("[dm-agent] > ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not goal:
            continue
        if goal in ("exit", "quit", "q"):
            break
        if goal == "reset":
            agent.reset()
            print("Conversation history cleared.")
            continue

        try:
            response = agent.chat(goal)
            print(f"\n{response}\n")
        except KeyboardInterrupt:
            print("\n(interrupted)")
        except Exception as e:
            logger.error(f"Agent error: {e}", exc_info=True)
            print(f"Error: {e}")

    return 0


def _refresh_lab_overview(context) -> None:
    """Regenerate LAB_DATA_OVERVIEW.md after any data change."""
    try:
        from dm_agent.skills.lab_overview import LabOverviewSkill
        overview = LabOverviewSkill()
        result = overview.run(context)
        if result.success:
            print(f"\n{result.message}")
    except Exception as e:
        print(f"\nWarning: could not refresh lab overview: {e}")


def _build_lab_context(config: Config) -> dict:
    return {
        "lab": config.lab,
        "members": [
            {"name": m.name, "email": m.email, "projects": m.projects, "role": m.role}
            for m in config.members
        ],
        "projects": {
            name: {"description": p.description, "data_types": p.data_types, "retention": p.retention}
            for name, p in config.projects.items()
        },
    }


def _human_size(size_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f}PB"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dm-agent",
        description="HPC Data Management Agent",
    )
    parser.add_argument(
        "--config", "-c",
        default="config.yaml",
        help="Path to config file (default: config.yaml)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # run (admin only)
    subparsers.add_parser("run", help="Run full weekly cycle (admin only)")

    # confirm (members: own tokens only)
    p_confirm = subparsers.add_parser("confirm", help="Confirm a deletion token")
    p_confirm.add_argument("token", help="Confirmation token from the email")

    # check-replies (admin only)
    subparsers.add_parser("check-replies", help="Check email for deletion confirmations (admin only)")

    # status (members: own data; admins: all)
    subparsers.add_parser("status", help="Show agent status")

    # catalog (admin only) — run cataloging for next dataset
    p_catalog = subparsers.add_parser("catalog", help="Catalog the next dataset (admin only)")
    p_catalog.add_argument("--dataset", "-d", help="Specific dataset name to catalog (re-catalogs if already done)")

    # catalog-report — view catalog results
    p_report = subparsers.add_parser("catalog-report", help="View data catalog report")
    p_report.add_argument("--dataset", "-d", help="Specific dataset name")
    p_report.add_argument("--verbose", "-v", action="store_true", help="Show per-directory breakdown")
    p_report.add_argument("--subjects", "-s", action="store_true", help="Show per-modality subject lists")

    # query — search the catalog by keywords
    p_query = subparsers.add_parser("query", help="Search the data catalog by keywords")
    p_query.add_argument("keywords", help="Search keywords (e.g., 'connectivity matrix', 'resting fMRI')")
    p_query.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format (default: text)")

    # organize (admin only) — reorganize a dataset and remove redundant files
    p_organize = subparsers.add_parser("organize", help="Reorganize a dataset and remove redundant files (admin only)")
    p_organize.add_argument("--dataset", "-d", required=True, help="Dataset name to organize")
    p_organize.add_argument("--dry-run", action="store_true", default=False, help="Show plan without executing (default: execute)")
    p_organize.add_argument("--no-reorganize", action="store_true", help="Only remove redundant files, skip directory restructuring")

    p_participants = subparsers.add_parser("participants", help="Generate participants.tsv for a dataset")
    p_participants.add_argument("--dataset", "-d", required=True, help="Dataset name")
    p_participants.add_argument("--output", "-o", help="Output path (default: <dataset_path>/participants.tsv)")

    # agent — LLM-driven interactive agent mode
    p_agent = subparsers.add_parser("agent", help="Interactive agent mode (natural language goals)")
    p_agent.add_argument("goal", nargs="?", default=None, help="Goal to accomplish (omit for interactive REPL)")
    p_agent.add_argument("--model", help="Override Claude model (default: from config or claude-sonnet-4-20250514)")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    commands = {
        "run": cmd_run,
        "confirm": cmd_confirm,
        "check-replies": cmd_check_replies,
        "status": cmd_status,
        "catalog": cmd_catalog,
        "catalog-report": cmd_catalog_report,
        "query": cmd_query,
        "organize": cmd_organize,
        "participants": cmd_participants,
        "agent": cmd_agent,
    }

    try:
        return commands[args.command](args)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
