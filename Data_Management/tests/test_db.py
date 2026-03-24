"""Tests for database module."""

from datetime import datetime, timedelta

from dm_agent.db import Database


def test_scan_state(db):
    assert db.get_last_scan_ts("/test/path") is None

    db.update_scan_state("/test/path", "2024-01-01T00:00:00", 5)
    assert db.get_last_scan_ts("/test/path") == "2024-01-01T00:00:00"

    db.update_scan_state("/test/path", "2024-01-08T00:00:00", 3)
    assert db.get_last_scan_ts("/test/path") == "2024-01-08T00:00:00"


def test_deletion_request_lifecycle(db):
    expires = (datetime.utcnow() + timedelta(days=7)).isoformat()
    req_id = db.create_deletion_request(
        token="test-token-123",
        target_path="/target",
        dir_path="/target/old_dir",
        reason="stale data",
        size_bytes=1024,
        owner_email="user@test.edu",
        expires_at=expires,
    )
    assert req_id > 0

    # Confirm
    assert db.confirm_deletion("test-token-123") is True
    assert db.confirm_deletion("test-token-123") is False  # Already confirmed

    # Get confirmed
    confirmed = db.get_confirmed_deletions()
    assert len(confirmed) == 1
    assert confirmed[0]["dir_path"] == "/target/old_dir"

    # Execute
    db.mark_deletion_executed(req_id)
    assert len(db.get_confirmed_deletions()) == 0


def test_token_expiry(db):
    # Create an already-expired token
    expired = (datetime.utcnow() - timedelta(days=1)).isoformat()
    db.create_deletion_request(
        token="expired-token",
        target_path="/target",
        dir_path="/target/dir",
        reason="test",
        size_bytes=0,
        owner_email="user@test.edu",
        expires_at=expired,
    )

    # Cannot confirm expired token
    assert db.confirm_deletion("expired-token") is False

    # Expire marks it
    count = db.expire_old_tokens()
    assert count == 1


def test_audit_log(db):
    db.log_audit(
        deletion_request_id=1,
        dir_path="/target/deleted",
        size_bytes=2048,
        confirmed_by="user@test.edu",
    )
    # Just verify no exception — audit is write-only


def test_scan_results(db):
    result_id = db.save_scan_result(
        scan_ts="2024-01-01T00:00:00",
        target_path="/target",
        changed_dir="/target/proj",
        readme_content="# Test",
        dir_tree=None,
        member_email="user@test.edu",
    )
    assert result_id > 0

    db.update_scan_analysis(result_id, '{"status": "ok"}')
