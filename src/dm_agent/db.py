"""SQLite database for state persistence and audit logging."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS scan_state (
    target_path TEXT PRIMARY KEY,
    last_scan_ts TEXT NOT NULL,
    last_scan_changed_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS deletion_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token TEXT UNIQUE NOT NULL,
    target_path TEXT NOT NULL,
    dir_path TEXT NOT NULL,
    reason TEXT,
    size_bytes INTEGER,
    owner_email TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    confirmed_at TEXT,
    executed_at TEXT,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deletion_request_id INTEGER REFERENCES deletion_requests(id),
    dir_path TEXT NOT NULL,
    size_bytes INTEGER,
    confirmed_by TEXT,
    executed_at TEXT NOT NULL,
    executor TEXT DEFAULT 'agent'
);

-- Data catalog: one row per dataset
CREATE TABLE IF NOT EXISTS catalog_datasets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_name TEXT UNIQUE NOT NULL,
    root_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending | scanning | cataloged | error
    total_size_bytes INTEGER,
    total_files INTEGER,
    total_subjects INTEGER,
    modalities TEXT,             -- JSON list of modalities found
    organization_scheme TEXT,    -- "by_subject" | "by_modality" | "mixed" | "flat" | "unknown"
    has_raw INTEGER DEFAULT 0,
    has_preprocessed INTEGER DEFAULT 0,
    has_derivatives INTEGER DEFAULT 0,
    bids_compliant INTEGER DEFAULT 0,
    summary TEXT,                -- Claude-generated summary
    recommendations TEXT,       -- Claude-generated reorganization recommendations (JSON)
    cataloged_at TEXT,
    created_at TEXT NOT NULL
);

-- Data catalog: per-subdirectory entries within a dataset
CREATE TABLE IF NOT EXISTS catalog_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id INTEGER REFERENCES catalog_datasets(id),
    rel_path TEXT NOT NULL,      -- relative to dataset root
    depth INTEGER NOT NULL,
    entry_type TEXT NOT NULL,    -- "subject_dir" | "modality_dir" | "session_dir" | "data_dir" | "derivative_dir" | "other"
    data_stage TEXT,             -- "raw" | "preprocessed" | "derivative" | "auxiliary" | "unknown"
    modality TEXT,               -- e.g. "anat", "func", "dwi", "eeg", "pet", etc.
    subject_id TEXT,             -- extracted subject ID if identifiable
    session_id TEXT,             -- extracted session ID if identifiable
    file_count INTEGER DEFAULT 0,
    size_bytes INTEGER DEFAULT 0,
    file_types TEXT,             -- JSON list of file extensions found
    sample_files TEXT,           -- JSON list of sample filenames (up to 20)
    notes TEXT                   -- Claude-generated notes for this entry
);

-- Catalog progress tracker: which depth levels have been scanned
CREATE TABLE IF NOT EXISTS catalog_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id INTEGER REFERENCES catalog_datasets(id),
    depth INTEGER NOT NULL,
    dirs_scanned INTEGER DEFAULT 0,
    dirs_total INTEGER DEFAULT 0,
    completed_at TEXT,
    UNIQUE(dataset_id, depth)
);

CREATE TABLE IF NOT EXISTS scan_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_ts TEXT NOT NULL,
    target_path TEXT NOT NULL,
    changed_dir TEXT NOT NULL,
    readme_content TEXT,
    dir_tree TEXT,
    analysis_summary TEXT,
    member_email TEXT
);
"""


class Database:
    """SQLite wrapper for dm_agent state."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA_SQL)

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # --- scan_state ---

    def get_last_scan_ts(self, target_path: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_scan_ts FROM scan_state WHERE target_path = ?",
                (target_path,),
            ).fetchone()
            return row["last_scan_ts"] if row else None

    def update_scan_state(self, target_path: str, ts: str, changed_count: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO scan_state (target_path, last_scan_ts, last_scan_changed_count)
                   VALUES (?, ?, ?)
                   ON CONFLICT(target_path) DO UPDATE
                   SET last_scan_ts = excluded.last_scan_ts,
                       last_scan_changed_count = excluded.last_scan_changed_count""",
                (target_path, ts, changed_count),
            )

    # --- scan_results ---

    def save_scan_result(
        self,
        scan_ts: str,
        target_path: str,
        changed_dir: str,
        readme_content: Optional[str],
        dir_tree: Optional[str],
        member_email: Optional[str],
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """INSERT INTO scan_results
                   (scan_ts, target_path, changed_dir, readme_content, dir_tree, member_email)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (scan_ts, target_path, changed_dir, readme_content, dir_tree, member_email),
            )
            return cursor.lastrowid

    def update_scan_analysis(self, result_id: int, summary: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE scan_results SET analysis_summary = ? WHERE id = ?",
                (summary, result_id),
            )

    # --- deletion_requests ---

    def create_deletion_request(
        self,
        token: str,
        target_path: str,
        dir_path: str,
        reason: str,
        size_bytes: Optional[int],
        owner_email: str,
        expires_at: str,
    ) -> int:
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            cursor = conn.execute(
                """INSERT INTO deletion_requests
                   (token, target_path, dir_path, reason, size_bytes,
                    owner_email, status, created_at, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)""",
                (token, target_path, dir_path, reason, size_bytes, owner_email, now, expires_at),
            )
            return cursor.lastrowid

    def confirm_deletion(self, token: str) -> bool:
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE deletion_requests
                   SET status = 'confirmed', confirmed_at = ?
                   WHERE token = ? AND status = 'pending' AND expires_at > ?""",
                (now, token, now),
            )
            return cursor.rowcount > 0

    def get_confirmed_deletions(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM deletion_requests WHERE status = 'confirmed'"
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_deletion_executed(self, request_id: int) -> None:
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                "UPDATE deletion_requests SET status = 'executed', executed_at = ? WHERE id = ?",
                (now, request_id),
            )

    def expire_old_tokens(self) -> int:
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE deletion_requests SET status = 'expired' WHERE status = 'pending' AND expires_at <= ?",
                (now,),
            )
            return cursor.rowcount

    def get_pending_deletions_for_email(self, email: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM deletion_requests WHERE owner_email = ? AND status = 'pending'",
                (email,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_deletion_request_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM deletion_requests WHERE token = ?", (token,)
            ).fetchone()
            return dict(row) if row else None

    # --- catalog_datasets ---

    def get_or_create_dataset(self, dataset_name: str, root_path: str) -> int:
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM catalog_datasets WHERE dataset_name = ?",
                (dataset_name,),
            ).fetchone()
            if row:
                return row["id"]
            cursor = conn.execute(
                """INSERT INTO catalog_datasets (dataset_name, root_path, status, created_at)
                   VALUES (?, ?, 'pending', ?)""",
                (dataset_name, root_path, now),
            )
            return cursor.lastrowid

    def update_dataset_status(self, dataset_id: int, status: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE catalog_datasets SET status = ? WHERE id = ?",
                (status, dataset_id),
            )

    def update_dataset_summary(
        self,
        dataset_id: int,
        total_size: int,
        total_files: int,
        total_subjects: int,
        modalities: str,
        organization_scheme: str,
        has_raw: bool,
        has_preprocessed: bool,
        has_derivatives: bool,
        bids_compliant: bool,
        summary: str,
        recommendations: str,
    ) -> None:
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """UPDATE catalog_datasets SET
                   total_size_bytes = ?, total_files = ?, total_subjects = ?,
                   modalities = ?, organization_scheme = ?,
                   has_raw = ?, has_preprocessed = ?, has_derivatives = ?,
                   bids_compliant = ?, summary = ?, recommendations = ?,
                   status = 'cataloged', cataloged_at = ?
                   WHERE id = ?""",
                (
                    total_size, total_files, total_subjects,
                    modalities, organization_scheme,
                    int(has_raw), int(has_preprocessed), int(has_derivatives),
                    int(bids_compliant), summary, recommendations, now, dataset_id,
                ),
            )

    def get_all_datasets(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM catalog_datasets ORDER BY dataset_name").fetchall()
            return [dict(r) for r in rows]

    def get_dataset(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM catalog_datasets WHERE id = ?", (dataset_id,)).fetchone()
            return dict(row) if row else None

    # --- catalog_entries ---

    def upsert_catalog_entry(
        self,
        dataset_id: int,
        rel_path: str,
        depth: int,
        entry_type: str,
        data_stage: str,
        modality: Optional[str],
        subject_id: Optional[str],
        session_id: Optional[str],
        file_count: int,
        size_bytes: int,
        file_types: str,
        sample_files: str,
        notes: Optional[str] = None,
    ) -> int:
        with self._connect() as conn:
            # Check if entry exists
            row = conn.execute(
                "SELECT id FROM catalog_entries WHERE dataset_id = ? AND rel_path = ?",
                (dataset_id, rel_path),
            ).fetchone()
            if row:
                conn.execute(
                    """UPDATE catalog_entries SET
                       entry_type = ?, data_stage = ?, modality = ?,
                       subject_id = ?, session_id = ?,
                       file_count = ?, size_bytes = ?, file_types = ?,
                       sample_files = ?, notes = ?
                       WHERE id = ?""",
                    (entry_type, data_stage, modality, subject_id, session_id,
                     file_count, size_bytes, file_types, sample_files, notes, row["id"]),
                )
                return row["id"]
            else:
                cursor = conn.execute(
                    """INSERT INTO catalog_entries
                       (dataset_id, rel_path, depth, entry_type, data_stage, modality,
                        subject_id, session_id, file_count, size_bytes, file_types,
                        sample_files, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (dataset_id, rel_path, depth, entry_type, data_stage, modality,
                     subject_id, session_id, file_count, size_bytes, file_types,
                     sample_files, notes),
                )
                return cursor.lastrowid

    def get_catalog_entries(self, dataset_id: int) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM catalog_entries WHERE dataset_id = ? ORDER BY rel_path",
                (dataset_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    # --- catalog_progress ---

    def update_catalog_progress(
        self, dataset_id: int, depth: int, dirs_scanned: int, dirs_total: int
    ) -> None:
        now = datetime.utcnow().isoformat()
        completed_at = now if dirs_scanned >= dirs_total else None
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO catalog_progress (dataset_id, depth, dirs_scanned, dirs_total, completed_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(dataset_id, depth) DO UPDATE SET
                   dirs_scanned = excluded.dirs_scanned,
                   dirs_total = excluded.dirs_total,
                   completed_at = excluded.completed_at""",
                (dataset_id, depth, dirs_scanned, dirs_total, completed_at),
            )

    def get_catalog_progress(self, dataset_id: int) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM catalog_progress WHERE dataset_id = ? ORDER BY depth",
                (dataset_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    # --- audit_log ---

    def log_audit(
        self,
        deletion_request_id: int,
        dir_path: str,
        size_bytes: Optional[int],
        confirmed_by: str,
    ) -> None:
        now = datetime.utcnow().isoformat()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO audit_log
                   (deletion_request_id, dir_path, size_bytes, confirmed_by, executed_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (deletion_request_id, dir_path, size_bytes, confirmed_by, now),
            )
