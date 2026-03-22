"""Confirmer skill — manage deletion confirmation tokens and process email replies."""

from __future__ import annotations

import email
import imaplib
import logging
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List

from dm_agent.base_skill import BaseSkill, RunContext, SkillResult

logger = logging.getLogger(__name__)

TOKEN_PATTERN = re.compile(r"CONFIRM:([a-f0-9\-]{36})")


class ConfirmerSkill(BaseSkill):
    name = "confirmer"
    description = "Create deletion tokens and process email confirmations"
    phase = "report"  # runs during report phase to create tokens

    def run(self, context: RunContext) -> SkillResult:
        """Create deletion tokens for recommended deletions."""
        if not context.analysis_results:
            return SkillResult(success=True, message="No analysis results")

        expiry_days = context.config.confirmation.expiry_days
        tokens_created = 0

        for entry in context.analysis_results:
            analysis = entry.get("analysis", {})
            del_candidate = analysis.get("deletion_candidate", {})

            if not del_candidate.get("should_delete"):
                continue
            if del_candidate.get("confidence") == "low":
                continue  # Skip low-confidence suggestions

            token = str(uuid.uuid4())
            expires_at = (
                context.run_timestamp + timedelta(days=expiry_days)
            ).isoformat()

            context.db.create_deletion_request(
                token=token,
                target_path=entry["target_path"],
                dir_path=entry["dir_path"],
                reason=del_candidate.get("reason", ""),
                size_bytes=self._get_dir_size(entry["dir_path"]),
                owner_email=entry.get("member_email", ""),
                expires_at=expires_at,
            )

            # Attach token to entry so reporter can include it
            entry["deletion_token"] = token
            tokens_created += 1

        # Expire old tokens
        expired = context.db.expire_old_tokens()
        if expired:
            logger.info(f"Expired {expired} old deletion tokens")

        return SkillResult(
            success=True,
            message=f"Created {tokens_created} deletion tokens, expired {expired} old ones",
            data={"tokens_created": tokens_created, "expired": expired},
        )

    def check_email_replies(self, context: RunContext) -> int:
        """Check IMAP inbox for email replies containing confirmation tokens."""
        cfg = context.config.confirmation
        if cfg.method != "email_reply":
            return 0

        if not cfg.imap_host or not cfg.imap_user:
            logger.warning("IMAP not configured, skipping email reply check")
            return 0

        confirmed = 0
        try:
            with imaplib.IMAP4_SSL(cfg.imap_host) as mail:
                mail.login(cfg.imap_user, cfg.imap_pass)
                mail.select("INBOX")

                # Search for emails containing CONFIRM:
                _, message_ids = mail.search(None, '(BODY "CONFIRM:")')
                if not message_ids[0]:
                    return 0

                for msg_id in message_ids[0].split():
                    _, msg_data = mail.fetch(msg_id, "(RFC822)")
                    if not msg_data or not msg_data[0]:
                        continue

                    msg = email.message_from_bytes(msg_data[0][1])
                    body = self._extract_body(msg)

                    tokens = TOKEN_PATTERN.findall(body)
                    for token in tokens:
                        if context.db.confirm_deletion(token):
                            confirmed += 1
                            logger.info(f"Confirmed deletion via email reply: {token[:8]}...")

                    # Mark email as read
                    mail.store(msg_id, "+FLAGS", "\\Seen")

        except Exception as e:
            logger.error(f"Failed to check email replies: {e}")

        return confirmed

    def _extract_body(self, msg: email.message.Message) -> str:
        """Extract plain text body from email message."""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        return payload.decode("utf-8", errors="replace")
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                return payload.decode("utf-8", errors="replace")
        return ""

    def _get_dir_size(self, dir_path: str) -> int:
        """Get approximate directory size."""
        import subprocess
        try:
            result = subprocess.run(
                ["du", "-sb", dir_path],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                return int(result.stdout.split()[0])
        except Exception:
            pass
        return 0
