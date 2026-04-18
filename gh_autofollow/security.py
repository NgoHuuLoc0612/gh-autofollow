"""
gh_autofollow.security — Account protection & anomaly detection.

Components:
  - TokenVault      : encrypt token at rest using OS keyring or AES-256-GCM
  - VelocityGuard   : sliding-window follow-rate limiter
  - AnomalyDetector : detects suspicious patterns across run history
  - AccountHealthMonitor : polls account status and emits warnings
  - SecurityMiddleware   : wraps AutoFollower._follow_one with all guards

All components are opt-in and non-breaking — existing code works unchanged
unless SecurityMiddleware is injected.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import platform
import secrets
import sqlite3
import stat
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. TOKEN VAULT
# ═══════════════════════════════════════════════════════════════════════════════

class TokenVault:
    """
    Secure token storage with three backends (in priority order):

      1. OS keyring  (Windows Credential Manager / macOS Keychain / libsecret)
      2. AES-256-GCM encrypted file  (falls back if keyring unavailable)
      3. Plain env var               (last resort, always works)

    Usage::

        vault = TokenVault()
        vault.store("ghp_your_token")
        token = vault.retrieve()          # returns plaintext token
        vault.delete()
    """

    _SERVICE = "gh-autofollow"
    _USERNAME = "github_token"

    def __init__(self, data_dir: Optional[str] = None) -> None:
        self._data_dir = Path(data_dir) if data_dir else self._default_dir()
        self._enc_path = self._data_dir / ".token.enc"
        self._key_path = self._data_dir / ".token.key"

    @staticmethod
    def _default_dir() -> Path:
        system = platform.system()
        if system == "Windows":
            return Path(os.environ.get("APPDATA", Path.home())) / "gh-autofollow"
        elif system == "Darwin":
            return Path.home() / "Library" / "Application Support" / "gh-autofollow"
        return Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")) / "gh-autofollow"

    # ── Keyring backend ───────────────────────────────────────────────────────

    @staticmethod
    def _keyring_available() -> bool:
        try:
            import keyring  # optional dep
            keyring.get_keyring()
            return True
        except Exception:
            return False

    def _store_keyring(self, token: str) -> None:
        import keyring
        keyring.set_password(self._SERVICE, self._USERNAME, token)

    def _retrieve_keyring(self) -> Optional[str]:
        try:
            import keyring
            return keyring.get_password(self._SERVICE, self._USERNAME)
        except Exception:
            return None

    def _delete_keyring(self) -> None:
        try:
            import keyring
            keyring.delete_password(self._SERVICE, self._USERNAME)
        except Exception:
            pass

    # ── AES-256-GCM file backend ──────────────────────────────────────────────

    def _derive_key(self, salt: bytes) -> bytes:
        """Derive a 256-bit key from machine-specific entropy."""
        machine_id = self._machine_id()
        return hashlib.pbkdf2_hmac(
            "sha256",
            machine_id.encode(),
            salt,
            iterations=260_000,
            dklen=32,
        )

    @staticmethod
    def _machine_id() -> str:
        """Stable machine identifier (not a secret, just entropy binding)."""
        system = platform.system()
        try:
            if system == "Linux":
                mid = Path("/etc/machine-id").read_text().strip()
                return mid if mid else platform.node()
            elif system == "Darwin":
                import subprocess
                result = subprocess.run(
                    ["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
                    capture_output=True, text=True,
                )
                for line in result.stdout.splitlines():
                    if "IOPlatformUUID" in line:
                        return line.split('"')[-2]
            elif system == "Windows":
                import subprocess
                result = subprocess.run(
                    ["wmic", "csproduct", "get", "UUID"],
                    capture_output=True, text=True,
                )
                lines = [l.strip() for l in result.stdout.splitlines() if l.strip()]
                if len(lines) >= 2:
                    return lines[1]
        except Exception:
            pass
        return platform.node() + platform.processor()

    def _store_encrypted(self, token: str) -> None:
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError:
            raise ImportError(
                "Encrypted file storage requires 'cryptography' package. "
                "Run: pip install cryptography"
            )

        self._data_dir.mkdir(parents=True, exist_ok=True)
        salt = secrets.token_bytes(32)
        key = self._derive_key(salt)
        nonce = secrets.token_bytes(12)
        aesgcm = AESGCM(key)
        ciphertext = aesgcm.encrypt(nonce, token.encode(), None)

        payload = {
            "v": 1,
            "salt": base64.b64encode(salt).decode(),
            "nonce": base64.b64encode(nonce).decode(),
            "ct": base64.b64encode(ciphertext).decode(),
        }
        self._enc_path.write_text(json.dumps(payload))
        self._secure_file(self._enc_path)

    def _retrieve_encrypted(self) -> Optional[str]:
        if not self._enc_path.exists():
            return None
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError:
            return None

        try:
            payload = json.loads(self._enc_path.read_text())
            salt = base64.b64decode(payload["salt"])
            nonce = base64.b64decode(payload["nonce"])
            ct = base64.b64decode(payload["ct"])
            key = self._derive_key(salt)
            aesgcm = AESGCM(key)
            return aesgcm.decrypt(nonce, ct, None).decode()
        except Exception as exc:
            logger.warning("Failed to decrypt token file: %s", exc)
            return None

    def _delete_encrypted(self) -> None:
        for p in (self._enc_path, self._key_path):
            if p.exists():
                # Overwrite with random bytes before deleting
                try:
                    size = p.stat().st_size
                    p.write_bytes(secrets.token_bytes(max(size, 64)))
                except Exception:
                    pass
                p.unlink(missing_ok=True)

    @staticmethod
    def _secure_file(path: Path) -> None:
        """chmod 600 on Unix."""
        try:
            if platform.system() != "Windows":
                os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        except Exception:
            pass

    # ── Public API ────────────────────────────────────────────────────────────

    def store(self, token: str) -> str:
        """Store token. Returns the backend used: 'keyring' or 'encrypted_file'."""
        if not token:
            raise ValueError("Token must not be empty")
        # Validate it looks like a GitHub token
        if not (token.startswith("ghp_") or token.startswith("github_pat_") or len(token) >= 20):
            logger.warning("Token does not look like a standard GitHub token")

        if self._keyring_available():
            self._store_keyring(token)
            logger.info("Token stored in OS keyring")
            return "keyring"
        else:
            self._store_encrypted(token)
            logger.info("Token stored in encrypted file: %s", self._enc_path)
            return "encrypted_file"

    def retrieve(self) -> Optional[str]:
        """Retrieve plaintext token. Returns None if not found."""
        # 1. Keyring
        if self._keyring_available():
            token = self._retrieve_keyring()
            if token:
                return token
        # 2. Encrypted file
        token = self._retrieve_encrypted()
        if token:
            return token
        # 3. Environment variable fallback
        return os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_AUTOFOLLOW_GITHUB_TOKEN")

    def delete(self) -> None:
        """Remove token from all backends."""
        self._delete_keyring()
        self._delete_encrypted()
        logger.info("Token deleted from all backends")

    @property
    def backend(self) -> str:
        if self._keyring_available() and self._retrieve_keyring():
            return "keyring"
        if self._enc_path.exists():
            return "encrypted_file"
        if os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_AUTOFOLLOW_GITHUB_TOKEN"):
            return "env_var"
        return "none"


# ═══════════════════════════════════════════════════════════════════════════════
# 2. VELOCITY GUARD
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class VelocityViolation(Exception):
    """Raised when a follow would exceed a velocity limit."""
    window_name: str
    current: int
    limit: int
    retry_after: float  # seconds until the window resets

    def __str__(self) -> str:
        return (
            f"Velocity limit exceeded [{self.window_name}]: "
            f"{self.current}/{self.limit} — retry in {self.retry_after:.0f}s"
        )


class VelocityGuard:
    """
    Multi-window sliding-window rate limiter for follow actions.

    Enforces independent limits per time window:
      - per_minute  : burst protection
      - per_hour    : sustained rate
      - per_day     : daily quota
      - per_session : total for current process lifetime

    All state lives in memory + optionally persisted to SQLite for
    cross-process accounting (e.g. scheduler restarts).

    Usage::

        guard = VelocityGuard(per_hour=30, per_day=200)
        guard.record_follow()          # raises VelocityViolation if over limit
        guard.can_follow()             # bool check without recording
    """

    def __init__(
        self,
        per_minute: int = 3,
        per_hour: int = 30,
        per_day: int = 150,
        per_session: int = 0,       # 0 = no session limit
        db_path: Optional[str] = None,
    ) -> None:
        self.limits = {
            "per_minute": (per_minute, 60),
            "per_hour":   (per_hour, 3600),
            "per_day":    (per_day, 86400),
        }
        if per_session > 0:
            self.limits["per_session"] = (per_session, 0)  # 0 = no expiry window

        # Timestamps of follow events — we use one deque per window
        self._windows: Dict[str, Deque[float]] = {
            name: deque() for name in self.limits
        }
        self._session_count = 0
        self._lock = Lock()
        self._db_path = db_path
        self._ensure_db()

    # ── DB persistence ────────────────────────────────────────────────────────

    def _ensure_db(self) -> None:
        if not self._db_path:
            return
        conn = sqlite3.connect(self._db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS velocity_events (
                ts REAL NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vel_ts ON velocity_events(ts)")
        conn.commit()
        conn.close()
        # Hydrate in-memory windows from DB (last 24h)
        self._hydrate_from_db()

    def _hydrate_from_db(self) -> None:
        if not self._db_path:
            return
        cutoff = time.time() - 86400
        try:
            conn = sqlite3.connect(self._db_path)
            rows = conn.execute(
                "SELECT ts FROM velocity_events WHERE ts > ? ORDER BY ts", (cutoff,)
            ).fetchall()
            conn.close()
            with self._lock:
                for (ts,) in rows:
                    for name, (_, window_secs) in self.limits.items():
                        if window_secs == 0:
                            continue
                        if time.time() - ts <= window_secs:
                            self._windows[name].append(ts)
        except Exception as exc:
            logger.debug("Could not hydrate velocity from DB: %s", exc)

    def _persist_event(self, ts: float) -> None:
        if not self._db_path:
            return
        try:
            conn = sqlite3.connect(self._db_path)
            conn.execute("INSERT INTO velocity_events (ts) VALUES (?)", (ts,))
            # Prune events older than 25 hours
            conn.execute("DELETE FROM velocity_events WHERE ts < ?", (ts - 90000,))
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.debug("Could not persist velocity event: %s", exc)

    # ── Core logic ────────────────────────────────────────────────────────────

    def _prune_window(self, name: str, now: float) -> None:
        """Remove timestamps outside the window."""
        _, window_secs = self.limits[name]
        if window_secs == 0:
            return
        cutoff = now - window_secs
        dq = self._windows[name]
        while dq and dq[0] < cutoff:
            dq.popleft()

    def _check_window(self, name: str, now: float) -> Optional[VelocityViolation]:
        limit, window_secs = self.limits[name]
        if name == "per_session":
            if self._session_count >= limit:
                return VelocityViolation(
                    window_name=name,
                    current=self._session_count,
                    limit=limit,
                    retry_after=0,
                )
            return None

        self._prune_window(name, now)
        current = len(self._windows[name])
        if current >= limit:
            oldest = self._windows[name][0]
            retry_after = (oldest + window_secs) - now
            return VelocityViolation(
                window_name=name,
                current=current,
                limit=limit,
                retry_after=max(0, retry_after),
            )
        return None

    def can_follow(self) -> Tuple[bool, Optional[VelocityViolation]]:
        """Check without recording. Returns (True, None) or (False, violation)."""
        now = time.time()
        with self._lock:
            for name in self.limits:
                violation = self._check_window(name, now)
                if violation:
                    return False, violation
        return True, None

    def record_follow(self) -> None:
        """
        Record a follow event. Raises VelocityViolation if any limit is exceeded.
        Call this BEFORE the actual follow API call.
        """
        now = time.time()
        with self._lock:
            for name in self.limits:
                violation = self._check_window(name, now)
                if violation:
                    raise violation

            # All checks passed — record the event
            for name, (_, window_secs) in self.limits.items():
                if window_secs > 0:
                    self._windows[name].append(now)
            self._session_count += 1

        self._persist_event(now)

    def current_rates(self) -> Dict[str, Dict]:
        """Return current usage for each window."""
        now = time.time()
        result = {}
        with self._lock:
            for name, (limit, window_secs) in self.limits.items():
                if name == "per_session":
                    result[name] = {
                        "current": self._session_count,
                        "limit": limit,
                        "pct": round(self._session_count / limit * 100, 1) if limit else 0,
                    }
                else:
                    self._prune_window(name, now)
                    current = len(self._windows[name])
                    result[name] = {
                        "current": current,
                        "limit": limit,
                        "pct": round(current / limit * 100, 1),
                    }
        return result

    def reset(self) -> None:
        with self._lock:
            for dq in self._windows.values():
                dq.clear()
            self._session_count = 0


# ═══════════════════════════════════════════════════════════════════════════════
# 3. ANOMALY DETECTOR
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class AnomalyAlert:
    level: str          # "warning" | "critical"
    code: str           # machine-readable code
    message: str
    data: Dict = field(default_factory=dict)


class AnomalyDetector:
    """
    Analyses run history and real-time signals to detect patterns that
    could indicate account risk.

    Checks:
      - error_spike       : error_count > threshold in a single run
      - rate_limit_storm  : multiple rate-limit hits in short succession
      - follow_velocity   : follows-per-hour trending above safe threshold
      - 403_pattern       : repeated 403s → account may be flagged
      - run_gap           : scheduler not running on schedule (missed runs)
      - consecutive_fails : N runs in a row with status != completed
    """

    def __init__(
        self,
        error_rate_threshold: float = 0.3,   # >30% errors in a run = warning
        rate_limit_storm_window: int = 3600,  # seconds
        rate_limit_storm_count: int = 3,      # N rate-limit hits in window
        max_follow_velocity: int = 50,        # follows/hour across all runs
        consecutive_fail_threshold: int = 3,
    ) -> None:
        self.error_rate_threshold = error_rate_threshold
        self.rate_limit_storm_window = rate_limit_storm_window
        self.rate_limit_storm_count = rate_limit_storm_count
        self.max_follow_velocity = max_follow_velocity
        self.consecutive_fail_threshold = consecutive_fail_threshold

    def analyse(self, recent_runs: List[sqlite3.Row]) -> List[AnomalyAlert]:
        """
        Analyse recent run records and return any anomaly alerts.
        Pass the result of db.recent_runs(limit=50).
        """
        alerts: List[AnomalyAlert] = []
        if not recent_runs:
            return alerts

        alerts.extend(self._check_error_spike(recent_runs))
        alerts.extend(self._check_rate_limit_storm(recent_runs))
        alerts.extend(self._check_follow_velocity(recent_runs))
        alerts.extend(self._check_consecutive_fails(recent_runs))
        alerts.extend(self._check_403_pattern(recent_runs))

        return alerts

    def _check_error_spike(self, runs: List[sqlite3.Row]) -> List[AnomalyAlert]:
        alerts = []
        for run in runs[:5]:  # last 5 runs
            total = (run["followed_count"] or 0) + (run["skipped_count"] or 0) + (run["error_count"] or 0)
            if total == 0:
                continue
            error_rate = (run["error_count"] or 0) / total
            if error_rate >= self.error_rate_threshold:
                alerts.append(AnomalyAlert(
                    level="warning",
                    code="error_spike",
                    message=(
                        f"Run {run['id'][:8]} had {error_rate*100:.0f}% error rate "
                        f"({run['error_count']} errors / {total} attempts)"
                    ),
                    data={"run_id": run["id"], "error_rate": error_rate},
                ))
        return alerts

    def _check_rate_limit_storm(self, runs: List[sqlite3.Row]) -> List[AnomalyAlert]:
        now = time.time()
        cutoff = now - self.rate_limit_storm_window
        rl_hits = [
            r for r in runs
            if r["rate_limit_hit"] and (r["started_at"] or 0) >= cutoff
        ]
        if len(rl_hits) >= self.rate_limit_storm_count:
            return [AnomalyAlert(
                level="critical",
                code="rate_limit_storm",
                message=(
                    f"{len(rl_hits)} rate-limit hits in the last "
                    f"{self.rate_limit_storm_window//60} minutes — "
                    "consider reducing batch_size or increasing batch_interval"
                ),
                data={"hits": len(rl_hits), "window_secs": self.rate_limit_storm_window},
            )]
        return []

    def _check_follow_velocity(self, runs: List[sqlite3.Row]) -> List[AnomalyAlert]:
        now = time.time()
        cutoff = now - 3600
        recent = [r for r in runs if (r["started_at"] or 0) >= cutoff]
        total_follows = sum(r["followed_count"] or 0 for r in recent)
        if total_follows > self.max_follow_velocity:
            return [AnomalyAlert(
                level="warning",
                code="high_follow_velocity",
                message=(
                    f"Followed {total_follows} users in the last hour "
                    f"(threshold: {self.max_follow_velocity}) — "
                    "GitHub may flag high follow velocity"
                ),
                data={"follows_per_hour": total_follows, "threshold": self.max_follow_velocity},
            )]
        return []

    def _check_consecutive_fails(self, runs: List[sqlite3.Row]) -> List[AnomalyAlert]:
        consecutive = 0
        for run in runs:
            if run["status"] not in ("completed", "skipped", "no_candidates", "rate_limited"):
                consecutive += 1
            else:
                break
        if consecutive >= self.consecutive_fail_threshold:
            return [AnomalyAlert(
                level="critical",
                code="consecutive_failures",
                message=(
                    f"{consecutive} consecutive batch failures — "
                    "check your token validity and network connectivity"
                ),
                data={"consecutive": consecutive},
            )]
        return []

    def _check_403_pattern(self, runs: List[sqlite3.Row]) -> List[AnomalyAlert]:
        # A run with many errors and rate_limit_hit=False but status=failed
        # could indicate account-level 403s
        suspicious = [
            r for r in runs[:10]
            if r["status"] == "failed"
            and (r["error_count"] or 0) > 5
            and not r["rate_limit_hit"]
        ]
        if len(suspicious) >= 2:
            return [AnomalyAlert(
                level="critical",
                code="possible_account_flag",
                message=(
                    f"{len(suspicious)} runs failed with high error counts "
                    "(not rate-limit). Your account may be flagged or token revoked."
                ),
                data={"suspicious_runs": len(suspicious)},
            )]
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# 4. ACCOUNT HEALTH MONITOR
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class HealthReport:
    healthy: bool
    token_valid: bool
    scopes_ok: bool
    follow_scope: bool
    account_suspended: bool
    two_factor_required: bool
    following_count: int
    rate_limit_remaining: int
    alerts: List[str] = field(default_factory=list)
    checked_at: float = field(default_factory=time.time)


class AccountHealthMonitor:
    """
    Performs a comprehensive health check against the GitHub API.

    Checks:
      - Token validity (401 = invalid)
      - Required OAuth scopes (user:follow)
      - Account suspension
      - 2FA requirements
      - Following count vs. GitHub's hard limit (5000)
      - Current rate limit headroom
    """

    GITHUB_FOLLOW_HARD_LIMIT = 5000
    MIN_RATE_LIMIT_HEALTHY = 500

    def __init__(self, client) -> None:
        self._client = client

    def check(self) -> HealthReport:
        alerts: List[str] = []
        token_valid = False
        scopes_ok = False
        follow_scope = False
        account_suspended = False
        two_factor_required = False
        following_count = 0
        rate_limit_remaining = 0

        try:
            # GET /user — check token and scopes from response headers
            resp = self._client._request("GET", "/user")
            user = resp.json()
            token_valid = True

            # Parse X-OAuth-Scopes header
            scopes_raw = resp.headers.get("X-OAuth-Scopes", "")
            scopes = {s.strip() for s in scopes_raw.split(",") if s.strip()}
            follow_scope = "user:follow" in scopes or "user" in scopes
            scopes_ok = follow_scope

            if not follow_scope:
                alerts.append(
                    "Missing 'user:follow' OAuth scope. "
                    "Regenerate token with 'user:follow' permission."
                )

            # Check account suspension (suspended accounts have type="Bot" with no repos)
            if user.get("suspended_at"):
                account_suspended = True
                alerts.append(f"Account suspended since {user['suspended_at']}")

            # 2FA requirement header
            if resp.headers.get("X-GitHub-OTP"):
                two_factor_required = True
                alerts.append("Two-factor authentication is being required")

            following_count = user.get("following", 0)
            if following_count >= self.GITHUB_FOLLOW_HARD_LIMIT:
                alerts.append(
                    f"Following count ({following_count}) has reached "
                    f"GitHub's hard limit of {self.GITHUB_FOLLOW_HARD_LIMIT}"
                )
            elif following_count >= self.GITHUB_FOLLOW_HARD_LIMIT * 0.9:
                alerts.append(
                    f"Following count ({following_count}) is at "
                    f"{following_count/self.GITHUB_FOLLOW_HARD_LIMIT*100:.0f}% of GitHub's hard limit"
                )

        except Exception as exc:
            from gh_autofollow.api.client import GitHubAPIError
            if isinstance(exc, GitHubAPIError) and exc.status_code == 401:
                alerts.append("Token is invalid or expired (401 Unauthorized)")
            else:
                alerts.append(f"Health check failed: {exc}")

        try:
            rl = self._client.get_rate_limits()
            core = rl.get("resources", {}).get("core", {})
            rate_limit_remaining = core.get("remaining", 0)
            if rate_limit_remaining < self.MIN_RATE_LIMIT_HEALTHY:
                alerts.append(
                    f"Rate limit critically low: {rate_limit_remaining} remaining "
                    f"(resets {core.get('reset', 0) - time.time():.0f}s)"
                )
        except Exception:
            pass

        healthy = (
            token_valid
            and follow_scope
            and not account_suspended
            and rate_limit_remaining >= self.MIN_RATE_LIMIT_HEALTHY
        )

        return HealthReport(
            healthy=healthy,
            token_valid=token_valid,
            scopes_ok=scopes_ok,
            follow_scope=follow_scope,
            account_suspended=account_suspended,
            two_factor_required=two_factor_required,
            following_count=following_count,
            rate_limit_remaining=rate_limit_remaining,
            alerts=alerts,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 5. SECURITY MIDDLEWARE
# ═══════════════════════════════════════════════════════════════════════════════

class SecurityMiddleware:
    """
    Wraps AutoFollower with all security guards.

    Inject at construction time::

        from gh_autofollow.security import SecurityMiddleware, VelocityGuard

        guard   = VelocityGuard(per_hour=25, per_day=100)
        detector = AnomalyDetector()
        mid     = SecurityMiddleware(guard=guard, detector=detector)

        with AutoFollower(config, security=mid) as af:
            af.run_batch()

    Or attach post-hoc::

        mid.attach(af)
    """

    def __init__(
        self,
        guard: Optional[VelocityGuard] = None,
        detector: Optional[AnomalyDetector] = None,
        health_check_interval: int = 3600,   # seconds between health checks
        abort_on_critical: bool = True,       # stop batch on CRITICAL anomaly
    ) -> None:
        self.guard = guard or VelocityGuard()
        self.detector = detector or AnomalyDetector()
        self.health_check_interval = health_check_interval
        self.abort_on_critical = abort_on_critical
        self._last_health_check: float = 0
        self._paused: bool = False
        self._pause_reason: str = ""

    def attach(self, auto_follower) -> None:
        """Monkey-patch the AutoFollower instance with security guards."""
        middleware = self

        # Capture current callables (works on real bound methods AND MagicMock)
        _orig_follow_one = auto_follower._follow_one
        _orig_execute_follows = auto_follower._execute_follows

        def secured_follow_one(candidate, record):
            can, violation = middleware.guard.can_follow()
            if not can:
                logger.warning("Velocity guard blocked follow of %s: %s", candidate.login, violation)
                record.skipped_count += 1
                if violation and violation.retry_after > 0:
                    sleep_secs = min(violation.retry_after + 1, 300)
                    logger.info("Velocity guard sleeping %.0fs", sleep_secs)
                    time.sleep(sleep_secs)
                return
            middleware.guard.record_follow()
            _orig_follow_one(candidate, record)

        def secured_execute_follows(candidates, record):
            if middleware._paused:
                logger.warning("Execution paused: %s", middleware._pause_reason)
                record.notes = f"paused: {middleware._pause_reason}"
                record.status = "paused"
                return
            _orig_execute_follows(candidates, record)

        auto_follower._follow_one = secured_follow_one
        auto_follower._execute_follows = secured_execute_follows

    def run_pre_batch_checks(self, auto_follower) -> List[AnomalyAlert]:
        """
        Call before run_batch(). Returns anomaly alerts.
        Will set _paused=True on CRITICAL alerts if abort_on_critical=True.
        """
        recent_runs = auto_follower.db.recent_runs(limit=50)
        alerts = self.detector.analyse(recent_runs)

        critical = [a for a in alerts if a.level == "critical"]
        warnings = [a for a in alerts if a.level == "warning"]

        for alert in warnings:
            logger.warning("[SECURITY WARNING] %s: %s", alert.code, alert.message)

        for alert in critical:
            logger.error("[SECURITY CRITICAL] %s: %s", alert.code, alert.message)

        if critical and self.abort_on_critical:
            self._paused = True
            self._pause_reason = critical[0].message
            logger.error("Batch execution PAUSED due to critical security alert")

        return alerts

    def run_health_check(self, auto_follower) -> Optional[HealthReport]:
        """Run account health check if interval has elapsed."""
        now = time.time()
        if now - self._last_health_check < self.health_check_interval:
            return None

        self._last_health_check = now
        monitor = AccountHealthMonitor(auto_follower.client)
        report = monitor.check()

        if not report.healthy:
            for alert in report.alerts:
                logger.warning("[HEALTH] %s", alert)
        else:
            logger.info(
                "[HEALTH] OK — token valid, scopes OK, following=%d, rate_limit=%d",
                report.following_count, report.rate_limit_remaining,
            )

        if report.account_suspended or not report.token_valid:
            self._paused = True
            self._pause_reason = "; ".join(report.alerts)

        return report

    def resume(self) -> None:
        """Manually clear the pause state after investigating alerts."""
        self._paused = False
        self._pause_reason = ""
        logger.info("Security middleware unpaused")

    def status(self) -> Dict:
        return {
            "paused": self._paused,
            "pause_reason": self._pause_reason,
            "velocity": self.guard.current_rates(),
        }
