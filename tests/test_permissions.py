"""Tests for permission and access control."""

from datetime import datetime, timedelta

from dm_agent.config import load_config
from dm_agent.db import Database


def test_admin_check(config):
    assert config.is_admin("admin_user") is True
    assert config.is_admin("alice") is False
    assert config.is_admin("unknown") is False


def test_member_lookup_by_username(config):
    alice = config.get_member_by_username("alice")
    assert alice is not None
    assert alice.email == "alice@test.edu"

    bob = config.get_member_by_username("bob")
    assert bob is not None
    assert bob.email == "bob@test.edu"

    unknown = config.get_member_by_username("unknown")
    assert unknown is None


def test_token_ownership(config, db):
    expires = (datetime.utcnow() + timedelta(days=7)).isoformat()

    # Create a deletion for Alice
    db.create_deletion_request(
        token="alice-token-001",
        target_path="/target",
        dir_path="/target/project_a/old",
        reason="stale",
        size_bytes=1024,
        owner_email="alice@test.edu",
        expires_at=expires,
    )

    # Lookup token
    req = db.get_deletion_request_by_token("alice-token-001")
    assert req is not None
    assert req["owner_email"] == "alice@test.edu"

    # Bob should not be able to confirm Alice's token (checked in CLI)
    bob = config.get_member_by_username("bob")
    assert bob is not None
    assert req["owner_email"] != bob.email

    # Alice can confirm her own
    alice = config.get_member_by_username("alice")
    assert alice is not None
    assert req["owner_email"] == alice.email


def test_nonexistent_token(db):
    req = db.get_deletion_request_by_token("nonexistent")
    assert req is None
