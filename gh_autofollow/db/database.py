"""
SQLite persistence layer for gh-autofollow.

Tables:
  - followed_users    : record of every user we have followed
  - candidate_cache   : discovered users pending follow
  - rate_limit_log    : historical rate-limit snapshots
  - run_log           : per-batch-run summary
  - schema_migrations : applied migration versions

All writes use parameterised queries.  WAL mode is enabled by default for
concurrent read access.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Schema version ────────────────────────────────────────────────────────────
SCHEMA_VERSION = 4

# ── DDL ───────────────────────────────────────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  REAL    NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE IF NOT EXISTS followed_users (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    login           TEXT    NOT NULL UNIQUE,
    github_id       INTEGER,
    followed_at     REAL    NOT NULL DEFAULT (unixepoch()),
    via_strategy    TEXT,
    batch_run_id    TEXT,
    followers_count INTEGER DEFAULT 0,
    public_repos    INTEGER DEFAULT 0,
    is_org          INTEGER DEFAULT 0,
    profile_url     TEXT
);
CREATE INDEX IF NOT EXISTS idx_followed_login ON followed_users(login);
CREATE INDEX IF NOT EXISTS idx_followed_at    ON followed_users(followed_at);

CREATE TABLE IF NOT EXISTS candidate_cache (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    login           TEXT    NOT NULL UNIQUE,
    github_id       INTEGER,
    discovered_at   REAL    NOT NULL DEFAULT (unixepoch()),
    via_strategy    TEXT,
    followers_count INTEGER DEFAULT 0,
    public_repos    INTEGER DEFAULT 0,
    is_org          INTEGER DEFAULT 0,
    score           REAL    DEFAULT 0.0,
    attempted       INTEGER DEFAULT 0,
    skipped         INTEGER DEFAULT 0,
    skip_reason     TEXT
);
CREATE INDEX IF NOT EXISTS idx_candidate_score    ON candidate_cache(score DESC);
CREATE INDEX IF NOT EXISTS idx_candidate_login    ON candidate_cache(login);
CREATE INDEX IF NOT EXISTS idx_candidate_attempt  ON candidate_cache(attempted);

CREATE TABLE IF NOT EXISTS rate_limit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at REAL    NOT NULL DEFAULT (unixepoch()),
    core_limit  INTEGER,
    core_remaining INTEGER,
    core_reset  INTEGER,
    search_limit    INTEGER,
    search_remaining INTEGER,
    search_reset    INTEGER
);

CREATE TABLE IF NOT EXISTS run_log (
    id              TEXT    PRIMARY KEY,
    started_at      REAL    NOT NULL DEFAULT (unixepoch()),
    finished_at     REAL,
    status          TEXT    DEFAULT 'running',
    batch_size      INTEGER DEFAULT 0,
    followed_count  INTEGER DEFAULT 0,
    skipped_count   INTEGER DEFAULT 0,
    error_count     INTEGER DEFAULT 0,
    rate_limit_hit  INTEGER DEFAULT 0,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS blocked_users (
    login   TEXT PRIMARY KEY,
    reason  TEXT,
    added_at REAL NOT NULL DEFAULT (unixepoch())
);
"""

# ── Migration SQL keyed by target version ────────────────────────────────────
_MIGRATIONS: Dict[int, str] = {
    2: "ALTER TABLE candidate_cache ADD COLUMN score REAL DEFAULT 0.0;",
    3: "ALTER TABLE candidate_cache ADD COLUMN skip_reason TEXT;",
    4: """
        CREATE TABLE IF NOT EXISTS blocked_users (
            login   TEXT PRIMARY KEY,
            reason  TEXT,
            added_at REAL NOT NULL DEFAULT (unixepoch())
        );
    """,
}


@dataclass
class FollowedUser:
    login: str
    github_id: Optional[int] = None
    followed_at: Optional[float] = None
    via_strategy: Optional[str] = None
    batch_run_id: Optional[str] = None
    followers_count: int = 0
    public_repos: int = 0
    is_org: bool = False
    profile_url: Optional[str] = None


@dataclass
class Candidate:
    login: str
    github_id: Optional[int] = None
    via_strategy: Optional[str] = None
    followers_count: int = 0
    public_repos: int = 0
    is_org: bool = False
    score: float = 0.0


@dataclass
class RunRecord:
    id: str
    started_at: float
    status: str = "running"
    batch_size: int = 0
    followed_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    rate_limit_hit: bool = False
    finished_at: Optional[float] = None
    notes: Optional[str] = None


class Database:
    """
    Thread-safe SQLite wrapper with connection-per-thread pooling.
    """

    def __init__(self, db_path: str | Path, wal_mode: bool = True) -> None:
        self._path = str(Path(db_path).expanduser().resolve())
        self._wal = wal_mode
        self._local = threading.local()
        self._init_lock = threading.Lock()
        self._initialized = False

        # Ensure parent directory exists
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    # ── Connection management ─────────────────────────────────────────────────

    @property
    def _conn(self) -> sqlite3.Connection:
        """Return a per-thread connection, creating if needed."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self._path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL" if self._wal else "PRAGMA journal_mode = DELETE")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA cache_size = -8000")  # 8 MB
            conn.execute("PRAGMA temp_store = MEMORY")
            self._local.conn = conn
        return conn

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        conn = self._conn
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn:
            conn.close()
            self._local.conn = None

    # ── Schema bootstrap & migrations ─────────────────────────────────────────

    def _ensure_schema(self) -> None:
        with self._init_lock:
            if self._initialized:
                return
            conn = self._conn
            conn.executescript(_DDL)
            conn.commit()
            self._run_migrations(conn)
            self._initialized = True

    def _run_migrations(self, conn: sqlite3.Connection) -> None:
        applied = {
            row[0]
            for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
        }
        for version in sorted(_MIGRATIONS.keys()):
            if version in applied:
                continue
            logger.info("Applying schema migration v%d", version)
            try:
                conn.executescript(_MIGRATIONS[version])
                conn.execute(
                    "INSERT OR IGNORE INTO schema_migrations (version) VALUES (?)",
                    (version,),
                )
                conn.commit()
            except sqlite3.OperationalError as exc:
                # Column-already-exists errors are benign
                if "duplicate column" in str(exc).lower():
                    conn.execute(
                        "INSERT OR IGNORE INTO schema_migrations (version) VALUES (?)",
                        (version,),
                    )
                    conn.commit()
                else:
                    raise

    # ── followed_users ────────────────────────────────────────────────────────

    def record_follow(self, user: FollowedUser) -> None:
        """Insert a successfully followed user."""
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO followed_users
                    (login, github_id, followed_at, via_strategy, batch_run_id,
                     followers_count, public_repos, is_org, profile_url)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    user.login,
                    user.github_id,
                    user.followed_at or time.time(),
                    user.via_strategy,
                    user.batch_run_id,
                    user.followers_count,
                    user.public_repos,
                    int(user.is_org),
                    user.profile_url,
                ),
            )

    def is_followed(self, login: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM followed_users WHERE login = ? LIMIT 1", (login,)
        ).fetchone()
        return row is not None

    def followed_count(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) FROM followed_users").fetchone()
        return row[0] if row else 0

    def get_followed_logins(self, limit: int = 0) -> List[str]:
        sql = "SELECT login FROM followed_users ORDER BY followed_at DESC"
        if limit > 0:
            sql += f" LIMIT {limit}"
        return [r[0] for r in self._conn.execute(sql).fetchall()]

    def get_follow_stats(self) -> Dict[str, int]:
        """Return per-strategy follow counts."""
        rows = self._conn.execute(
            """
            SELECT via_strategy, COUNT(*) as cnt
            FROM followed_users
            GROUP BY via_strategy
            """
        ).fetchall()
        return {r["via_strategy"] or "unknown": r["cnt"] for r in rows}

    # ── candidate_cache ───────────────────────────────────────────────────────

    def add_candidates(self, candidates: List[Candidate]) -> int:
        """
        Bulk-insert candidates; skip if login already exists (followed or cached).
        Returns number of new rows inserted.
        """
        followed_set = set(self.get_followed_logins())
        blocked_set = set(self.get_blocked_logins())

        inserted = 0
        with self.transaction() as conn:
            for c in candidates:
                if c.login in followed_set or c.login in blocked_set:
                    continue
                try:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO candidate_cache
                            (login, github_id, via_strategy, followers_count,
                             public_repos, is_org, score)
                        VALUES (?,?,?,?,?,?,?)
                        """,
                        (
                            c.login,
                            c.github_id,
                            c.via_strategy,
                            c.followers_count,
                            c.public_repos,
                            int(c.is_org),
                            c.score,
                        ),
                    )
                    if conn.execute("SELECT changes()").fetchone()[0]:
                        inserted += 1
                except sqlite3.Error as exc:
                    logger.warning("Failed to insert candidate %s: %s", c.login, exc)
        return inserted

    def pop_candidates(self, n: int) -> List[Candidate]:
        """
        Return up to *n* unattempted candidates ordered by score desc.
        Marks them as attempted atomically.
        """
        with self.transaction() as conn:
            rows = conn.execute(
                """
                SELECT id, login, github_id, via_strategy,
                       followers_count, public_repos, is_org, score
                FROM candidate_cache
                WHERE attempted = 0 AND skipped = 0
                ORDER BY score DESC
                LIMIT ?
                """,
                (n,),
            ).fetchall()

            ids = [r["id"] for r in rows]
            if ids:
                conn.execute(
                    f"UPDATE candidate_cache SET attempted = 1 WHERE id IN ({','.join('?' * len(ids))})",
                    ids,
                )

        return [
            Candidate(
                login=r["login"],
                github_id=r["github_id"],
                via_strategy=r["via_strategy"],
                followers_count=r["followers_count"],
                public_repos=r["public_repos"],
                is_org=bool(r["is_org"]),
                score=r["score"],
            )
            for r in rows
        ]

    def skip_candidate(self, login: str, reason: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE candidate_cache SET skipped = 1, skip_reason = ? WHERE login = ?",
                (reason, login),
            )

    def candidate_count(self, unattempted_only: bool = True) -> int:
        if unattempted_only:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM candidate_cache WHERE attempted = 0 AND skipped = 0"
            ).fetchone()
        else:
            row = self._conn.execute("SELECT COUNT(*) FROM candidate_cache").fetchone()
        return row[0] if row else 0

    def prune_candidates(self, max_age_days: int = 7) -> int:
        """Remove stale candidates that have been attempted or are too old."""
        cutoff = time.time() - max_age_days * 86400
        with self.transaction() as conn:
            conn.execute(
                "DELETE FROM candidate_cache WHERE attempted = 1 OR discovered_at < ?",
                (cutoff,),
            )
            return conn.execute("SELECT changes()").fetchone()[0]

    # ── blocked_users ─────────────────────────────────────────────────────────

    def block_user(self, login: str, reason: str = "") -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO blocked_users (login, reason) VALUES (?,?)",
                (login, reason),
            )

    def is_blocked(self, login: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM blocked_users WHERE login = ? LIMIT 1", (login,)
        ).fetchone()
        return row is not None

    def get_blocked_logins(self) -> List[str]:
        return [r[0] for r in self._conn.execute("SELECT login FROM blocked_users").fetchall()]

    # ── rate_limit_log ────────────────────────────────────────────────────────

    def log_rate_limit(
        self,
        core_limit: int, core_remaining: int, core_reset: int,
        search_limit: int = 0, search_remaining: int = 0, search_reset: int = 0,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO rate_limit_log
                    (core_limit, core_remaining, core_reset,
                     search_limit, search_remaining, search_reset)
                VALUES (?,?,?,?,?,?)
                """,
                (core_limit, core_remaining, core_reset,
                 search_limit, search_remaining, search_reset),
            )

    def latest_rate_limit(self) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM rate_limit_log ORDER BY recorded_at DESC LIMIT 1"
        ).fetchone()

    # ── run_log ───────────────────────────────────────────────────────────────

    def start_run(self, run_id: str, batch_size: int) -> RunRecord:
        now = time.time()
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO run_log (id, started_at, batch_size) VALUES (?,?,?)",
                (run_id, now, batch_size),
            )
        return RunRecord(id=run_id, started_at=now, batch_size=batch_size)

    def finish_run(self, record: RunRecord) -> None:
        record.finished_at = time.time()
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE run_log SET
                    finished_at    = ?,
                    status         = ?,
                    followed_count = ?,
                    skipped_count  = ?,
                    error_count    = ?,
                    rate_limit_hit = ?,
                    notes          = ?
                WHERE id = ?
                """,
                (
                    record.finished_at,
                    record.status,
                    record.followed_count,
                    record.skipped_count,
                    record.error_count,
                    int(record.rate_limit_hit),
                    record.notes,
                    record.id,
                ),
            )

    def recent_runs(self, limit: int = 20) -> List[sqlite3.Row]:
        return self._conn.execute(
            "SELECT * FROM run_log ORDER BY started_at DESC LIMIT ?", (limit,)
        ).fetchall()

    # ── Housekeeping ──────────────────────────────────────────────────────────

    def vacuum(self) -> None:
        self._conn.execute("VACUUM")

    def integrity_check(self) -> bool:
        row = self._conn.execute("PRAGMA integrity_check").fetchone()
        return row and row[0] == "ok"

    def get_summary(self) -> Dict[str, int]:
        return {
            "total_followed": self.followed_count(),
            "candidates_pending": self.candidate_count(unattempted_only=True),
            "candidates_total": self.candidate_count(unattempted_only=False),
            "blocked_users": len(self.get_blocked_logins()),
            "total_runs": len(self.recent_runs(limit=999999)),
        }
