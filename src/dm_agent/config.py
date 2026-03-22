"""Configuration loading and validation."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class MemberConfig:
    name: str
    email: str
    projects: List[str]
    role: str = "member"
    hpc_username: str = ""  # Unix username on the HPC cluster


@dataclass
class ProjectConfig:
    name: str
    description: str = ""
    data_types: List[str] = field(default_factory=list)
    retention: str = "permanent"


@dataclass
class RetentionPolicy:
    pattern: str
    action: str = "recommend_delete"  # recommend_delete | never_delete
    max_age_days: Optional[int] = None


@dataclass
class ScanTarget:
    path: str
    description: str = ""


@dataclass
class EmailConfig:
    smtp_host: str = "localhost"
    smtp_port: int = 587
    from_address: str = ""
    use_tls: bool = True
    smtp_user: str = ""
    smtp_pass: str = ""

    def resolve_secrets(self) -> None:
        """Resolve credentials from environment variables."""
        if not self.smtp_user:
            self.smtp_user = os.environ.get("DM_SMTP_USER", "")
        if not self.smtp_pass:
            self.smtp_pass = os.environ.get("DM_SMTP_PASS", "")


@dataclass
class ConfirmationConfig:
    method: str = "token_cli"  # token_cli | email_reply
    expiry_days: int = 7
    imap_host: str = ""
    imap_user: str = ""
    imap_pass: str = ""

    def resolve_secrets(self) -> None:
        if not self.imap_user:
            self.imap_user = os.environ.get("DM_IMAP_USER", "")
        if not self.imap_pass:
            self.imap_pass = os.environ.get("DM_IMAP_PASS", "")


@dataclass
class Config:
    database_path: str = "dm_agent.db"
    lab_context_path: str = "lab_context.yaml"
    admin_users: List[str] = field(default_factory=list)  # HPC usernames with admin access
    scan_targets: List[ScanTarget] = field(default_factory=list)
    email: EmailConfig = field(default_factory=EmailConfig)
    confirmation: ConfirmationConfig = field(default_factory=ConfirmationConfig)
    skills: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    scanner: Dict[str, Any] = field(default_factory=dict)
    analyzer: Dict[str, Any] = field(default_factory=dict)

    # Lab context (loaded separately)
    lab: Dict[str, Any] = field(default_factory=dict)
    members: List[MemberConfig] = field(default_factory=list)
    projects: Dict[str, ProjectConfig] = field(default_factory=dict)
    retention_policies: List[RetentionPolicy] = field(default_factory=list)

    def get_member_by_username(self, username: str) -> Optional[MemberConfig]:
        """Find a member by their HPC username."""
        for member in self.members:
            if member.hpc_username == username:
                return member
        return None

    def is_admin(self, username: str) -> bool:
        """Check if a username has admin privileges."""
        return username in self.admin_users


def load_config(config_path: str) -> Config:
    """Load and validate configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    config = Config(
        database_path=raw.get("database_path", "dm_agent.db"),
        lab_context_path=raw.get("lab_context_path", "lab_context.yaml"),
        admin_users=raw.get("admin_users", []),
        skills=raw.get("skills", {}),
        scanner=raw.get("scanner", {}),
        analyzer=raw.get("analyzer", {}),
    )

    # Parse scan targets
    for target in raw.get("scan_targets", []):
        config.scan_targets.append(
            ScanTarget(
                path=target["path"],
                description=target.get("description", ""),
            )
        )

    # Parse email config
    email_raw = raw.get("email", {})
    config.email = EmailConfig(
        smtp_host=email_raw.get("smtp_host", "localhost"),
        smtp_port=email_raw.get("smtp_port", 587),
        from_address=email_raw.get("from_address", ""),
        use_tls=email_raw.get("use_tls", True),
        smtp_user=email_raw.get("smtp_user", ""),
        smtp_pass=email_raw.get("smtp_pass", ""),
    )
    config.email.resolve_secrets()

    # Parse confirmation config
    confirm_raw = raw.get("confirmation", {})
    config.confirmation = ConfirmationConfig(
        method=confirm_raw.get("method", "token_cli"),
        expiry_days=confirm_raw.get("expiry_days", 7),
        imap_host=confirm_raw.get("imap_host", ""),
        imap_user=confirm_raw.get("imap_user", ""),
        imap_pass=confirm_raw.get("imap_pass", ""),
    )
    config.confirmation.resolve_secrets()

    # Load lab context
    lab_context_path = Path(path.parent / config.lab_context_path)
    if lab_context_path.exists():
        _load_lab_context(config, lab_context_path)

    _validate(config)
    return config


def _load_lab_context(config: Config, path: Path) -> None:
    """Load lab context from a separate YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    config.lab = raw.get("lab", {})

    for member_raw in raw.get("members", []):
        config.members.append(
            MemberConfig(
                name=member_raw["name"],
                email=member_raw["email"],
                projects=member_raw.get("projects", []),
                role=member_raw.get("role", "member"),
                hpc_username=member_raw.get("hpc_username", ""),
            )
        )

    for proj_name, proj_raw in raw.get("projects", {}).items():
        config.projects[proj_name] = ProjectConfig(
            name=proj_name,
            description=proj_raw.get("description", ""),
            data_types=proj_raw.get("data_types", []),
            retention=proj_raw.get("retention", "permanent"),
        )

    for policy_raw in raw.get("retention_policies", []):
        config.retention_policies.append(
            RetentionPolicy(
                pattern=policy_raw["pattern"],
                action=policy_raw.get("action", "recommend_delete"),
                max_age_days=policy_raw.get("max_age_days"),
            )
        )


def _validate(config: Config) -> None:
    """Validate configuration."""
    if not config.scan_targets:
        raise ValueError("At least one scan_target is required")
    for target in config.scan_targets:
        if not target.path:
            raise ValueError("scan_target path cannot be empty")
    if not config.email.from_address:
        raise ValueError("email.from_address is required")
