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

        if ds["recommendations"]:
            recs = json.loads(ds["recommendations"])
            if recs:
                print(f"\n  Recommendations ({len(recs)}):")
                for i, rec in enumerate(recs, 1):
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
    }

    try:
        return commands[args.command](args)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
