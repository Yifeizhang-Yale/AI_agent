"""Reporter skill — build and send email reports per member."""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List

from dm_agent.base_skill import BaseSkill, RunContext, SkillResult

logger = logging.getLogger(__name__)


class ReporterSkill(BaseSkill):
    name = "reporter"
    description = "Generate and send per-member email reports with analysis and deletion tokens"
    phase = "report"

    def run(self, context: RunContext) -> SkillResult:
        if not context.analysis_results:
            return SkillResult(success=True, message="No analysis results to report")

        # Group results by member email
        by_member: Dict[str, List[Dict[str, Any]]] = {}
        for entry in context.analysis_results:
            email = entry.get("member_email") or "unassigned"
            by_member.setdefault(email, []).append(entry)

        # Get PI email for CC
        pi_email = context.config.lab.get("pi_email", "")

        sent_count = 0
        for member_email, entries in by_member.items():
            if member_email == "unassigned":
                # Send unassigned directories to PI
                if pi_email:
                    member_email = pi_email
                else:
                    logger.warning(f"Skipping {len(entries)} unassigned directories (no PI email)")
                    continue

            try:
                body = self._build_report(member_email, entries, context)
                subject = f"[DM Agent] Weekly Data Report — {len(entries)} directories"

                cc_list = [pi_email] if pi_email and pi_email != member_email else []
                self._send_email(
                    context.config, member_email, cc_list, subject, body
                )
                sent_count += 1
                logger.info(f"Report sent to {member_email} ({len(entries)} dirs)")

            except Exception as e:
                logger.error(f"Failed to send report to {member_email}: {e}")
                context.errors.append(f"Email to {member_email} failed: {e}")

        # Include quota warnings if available
        if context.quota_results:
            self._send_quota_alerts(context, pi_email)

        return SkillResult(
            success=True,
            message=f"Sent {sent_count} reports",
            data={"sent": sent_count},
        )

    def _build_report(
        self,
        member_email: str,
        entries: List[Dict[str, Any]],
        context: RunContext,
    ) -> str:
        """Build a plain-text email report."""
        lines = [
            f"Data Management Report — {context.run_timestamp.strftime('%Y-%m-%d')}",
            "=" * 60,
            "",
        ]

        # Section 1: Changes summary
        changed = [e for e in entries if e.get("is_changed")]
        stale = [e for e in entries if e.get("is_stale")]

        if changed:
            lines.append(f"## Changed Directories ({len(changed)})")
            lines.append("")
            for entry in changed:
                analysis = entry.get("analysis", {})
                lines.append(f"  📁 {entry['dir_path']}")
                lines.append(f"     Summary: {analysis.get('summary', 'N/A')}")
                lines.append(f"     Status: {analysis.get('status', 'N/A')}")
                for rec in analysis.get("recommendations", []):
                    lines.append(f"     → {rec}")
                lines.append("")

        if stale:
            lines.append(f"## Stale Directories ({len(stale)})")
            lines.append("")
            for entry in stale:
                analysis = entry.get("analysis", {})
                lines.append(f"  📁 {entry['dir_path']}")
                lines.append(f"     Summary: {analysis.get('summary', 'N/A')}")
                lines.append(f"     Status: {analysis.get('status', 'N/A')}")
                lines.append("")

        # Section 2: Deletion candidates
        deletion_entries = [
            e for e in entries
            if e.get("analysis", {}).get("deletion_candidate", {}).get("should_delete")
        ]

        if deletion_entries:
            lines.append("## Deletion Recommendations")
            lines.append("")
            lines.append("The following directories are recommended for deletion.")
            lines.append("To confirm deletion, reply with the token or run the command on the cluster.")
            lines.append("")

            for entry in deletion_entries:
                analysis = entry.get("analysis", {})
                del_info = analysis.get("deletion_candidate", {})
                token = entry.get("deletion_token", "")
                lines.append(f"  📁 {entry['dir_path']}")
                lines.append(f"     Reason: {del_info.get('reason', 'N/A')}")
                lines.append(f"     Confidence: {del_info.get('confidence', 'N/A')}")
                if token:
                    lines.append(f"     ✅ To confirm: reply with CONFIRM:{token}")
                    lines.append(f"     ✅ Or run: dm-agent confirm {token}")
                lines.append("")

        # Section 3: README suggestions
        if context.readme_suggestions:
            member_suggestions = [
                s for s in context.readme_suggestions
                if s.get("member_email") == member_email
            ]
            if member_suggestions:
                lines.append("## README Suggestions")
                lines.append("")
                for s in member_suggestions:
                    lines.append(f"  📁 {s['dir_path']}")
                    lines.append(f"     {s.get('suggestion', 'Consider adding a README')}")
                    lines.append("")

        lines.append("—")
        lines.append("This report was generated by the HPC Data Management Agent.")
        lines.append("Questions? Contact your system administrator.")

        return "\n".join(lines)

    def _send_quota_alerts(self, context: RunContext, pi_email: str) -> None:
        """Send quota warning emails."""
        for quota in context.quota_results:
            if quota.get("level") in ("warning", "critical"):
                email = quota.get("member_email") or pi_email
                if not email:
                    continue

                subject = f"[DM Agent] {'⚠️ CRITICAL' if quota['level'] == 'critical' else 'Warning'}: Disk Quota Alert"
                body = (
                    f"Disk Quota Alert\n"
                    f"{'=' * 40}\n\n"
                    f"User/Project: {quota.get('name', 'N/A')}\n"
                    f"Usage: {quota.get('used_human', 'N/A')} / {quota.get('limit_human', 'N/A')}\n"
                    f"Percentage: {quota.get('percent', 0):.1f}%\n"
                    f"Level: {quota['level'].upper()}\n\n"
                    f"Please clean up unnecessary data to avoid running out of quota.\n"
                )

                cc_list = [pi_email] if pi_email and pi_email != email else []
                try:
                    self._send_email(context.config, email, cc_list, subject, body)
                except Exception as e:
                    logger.error(f"Failed to send quota alert to {email}: {e}")

    def _send_email(
        self,
        config,
        to_addr: str,
        cc_addrs: List[str],
        subject: str,
        body: str,
    ) -> None:
        """Send an email via SMTP, with sendmail pipe fallback.

        Many HPC nodes have a local MTA (postfix/sendmail) that accepts
        mail via the ``sendmail`` command but rejects SMTP protocol
        connections on port 25.  We try SMTP first, then fall back to
        piping through ``/usr/sbin/sendmail -t``.
        """
        msg = MIMEMultipart()
        msg["From"] = config.email.from_address
        msg["To"] = to_addr
        if cc_addrs:
            msg["Cc"] = ", ".join(cc_addrs)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        recipients = [to_addr] + cc_addrs

        try:
            if config.email.use_tls:
                with smtplib.SMTP(config.email.smtp_host, config.email.smtp_port) as server:
                    server.starttls()
                    if config.email.smtp_user:
                        server.login(config.email.smtp_user, config.email.smtp_pass)
                    server.sendmail(config.email.from_address, recipients, msg.as_string())
            else:
                with smtplib.SMTP(config.email.smtp_host, config.email.smtp_port) as server:
                    if config.email.smtp_user:
                        server.login(config.email.smtp_user, config.email.smtp_pass)
                    server.sendmail(config.email.from_address, recipients, msg.as_string())
        except (smtplib.SMTPException, OSError) as smtp_err:
            # Fallback: pipe through sendmail command
            import shutil
            import subprocess

            sendmail_bin = shutil.which("sendmail") or "/usr/sbin/sendmail"
            logger.info(
                f"SMTP failed ({smtp_err}), falling back to {sendmail_bin} -t"
            )
            proc = subprocess.run(
                [sendmail_bin, "-t"],
                input=msg.as_string(),
                capture_output=True,
                text=True,
                timeout=30,
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"sendmail pipe failed (rc={proc.returncode}): {proc.stderr}"
                ) from smtp_err
