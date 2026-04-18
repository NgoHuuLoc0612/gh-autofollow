"""
Configuration management for gh-autofollow.

Supports:
  - TOML config file (~/.config/gh-autofollow/config.toml)
  - JSON config file
  - Environment variables (GH_AUTOFOLLOW_*)
  - Programmatic overrides
"""

from __future__ import annotations

import json
import logging
import os
import platform
import sys
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Platform-aware default paths ────────────────────────────────────────────

def _default_config_dir() -> Path:
    system = platform.system()
    if system == "Windows":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    elif system == "Darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base / "gh-autofollow"


def _default_data_dir() -> Path:
    system = platform.system()
    if system == "Windows":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    elif system == "Darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return base / "gh-autofollow"


def _default_log_dir() -> Path:
    system = platform.system()
    if system == "Windows":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    elif system == "Darwin":
        base = Path.home() / "Library" / "Logs"
    else:
        base = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    return base / "gh-autofollow"


# ── Default constants ────────────────────────────────────────────────────────

DEFAULT_BATCH_SIZE = 10          # users followed per batch run
DEFAULT_BATCH_INTERVAL = 3600    # seconds between automatic batch runs (1 h)
DEFAULT_FOLLOW_DELAY_MIN = 2.0   # min delay between individual follow calls (s)
DEFAULT_FOLLOW_DELAY_MAX = 7.0   # max delay between individual follow calls (s)
DEFAULT_RATE_LIMIT_BUFFER = 100  # remaining API calls to keep in reserve
DEFAULT_MAX_FOLLOWING = 5000     # stop following when we reach this count
DEFAULT_CANDIDATE_POOL = 200     # how many candidates to discover per refresh

# GitHub REST API v3
GITHUB_API_BASE = "https://api.github.com"
GITHUB_RATE_LIMIT_URL = f"{GITHUB_API_BASE}/rate_limit"


@dataclass
class Config:
    """
    Central configuration object.  All fields have sensible defaults so the
    library works out-of-the-box with only a token supplied.
    """

    # ── Required ─────────────────────────────────────────────────────────────
    github_token: str = ""

    # ── API ──────────────────────────────────────────────────────────────────
    api_base_url: str = GITHUB_API_BASE
    api_timeout: int = 30          # HTTP request timeout in seconds
    api_max_retries: int = 5
    api_retry_backoff: float = 2.0  # exponential backoff base (seconds)

    # ── Follow behaviour ─────────────────────────────────────────────────────
    batch_size: int = DEFAULT_BATCH_SIZE
    batch_interval: int = DEFAULT_BATCH_INTERVAL
    follow_delay_min: float = DEFAULT_FOLLOW_DELAY_MIN
    follow_delay_max: float = DEFAULT_FOLLOW_DELAY_MAX
    rate_limit_buffer: int = DEFAULT_RATE_LIMIT_BUFFER
    max_following: int = DEFAULT_MAX_FOLLOWING

    # ── Discovery strategies ──────────────────────────────────────────────────
    # Allowed values: "trending", "followers_of_following", "starred_repos",
    #                 "topic_search", "random_explore"
    strategies: List[str] = field(default_factory=lambda: [
        "trending",
        "followers_of_following",
        "starred_repos",
        "topic_search",
    ])
    candidate_pool_size: int = DEFAULT_CANDIDATE_POOL

    # Topics used by the topic_search strategy
    topics: List[str] = field(default_factory=lambda: [
        "python", "javascript", "rust", "go", "machine-learning",
        "open-source", "developer-tools",
    ])

    # Languages used by the trending strategy
    trending_languages: List[str] = field(default_factory=lambda: [
        "python", "javascript", "typescript", "rust", "go",
    ])

    # ── Filters ───────────────────────────────────────────────────────────────
    min_followers: int = 0
    max_followers: int = 0          # 0 = no upper limit
    min_public_repos: int = 0
    skip_orgs: bool = True          # skip organization accounts
    skip_bots: bool = True          # skip accounts with [bot] in login
    skip_no_bio: bool = False

    # Explicit allow / block lists (GitHub logins)
    allowlist: List[str] = field(default_factory=list)
    blocklist: List[str] = field(default_factory=list)

    # ── Scheduler ─────────────────────────────────────────────────────────────
    autostart_enabled: bool = False    # install OS autostart entry
    scheduler_pid_file: str = ""       # filled in at runtime if empty

    # ── Persistence / cache ───────────────────────────────────────────────────
    data_dir: str = ""    # resolved below
    db_filename: str = "gh_autofollow.db"
    db_wal_mode: bool = True

    # ── Logging ───────────────────────────────────────────────────────────────
    log_dir: str = ""     # resolved below
    log_level: str = "INFO"
    log_max_bytes: int = 5 * 1024 * 1024   # 5 MB
    log_backup_count: int = 3

    # ── Security ──────────────────────────────────────────────────────────────
    security_enabled: bool = True
    velocity_per_minute: int = 3       # max follows per minute
    velocity_per_hour: int = 30        # max follows per hour
    velocity_per_day: int = 150        # max follows per day
    anomaly_abort_on_critical: bool = True
    health_check_interval: int = 3600  # seconds between account health checks
    token_vault_enabled: bool = False  # store token encrypted at rest

    # ── Misc ──────────────────────────────────────────────────────────────────
    dry_run: bool = False   # simulate without calling follow endpoint
    verbose: bool = False

    def __post_init__(self) -> None:
        if not self.data_dir:
            self.data_dir = str(_default_data_dir())
        if not self.log_dir:
            self.log_dir = str(_default_log_dir())
        if not self.scheduler_pid_file:
            self.scheduler_pid_file = str(
                Path(self.data_dir) / "scheduler.pid"
            )

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def db_path(self) -> Path:
        return Path(self.data_dir) / self.db_filename

    @property
    def log_path(self) -> Path:
        return Path(self.log_dir) / "gh-autofollow.log"

    # ── Loaders ───────────────────────────────────────────────────────────────

    @classmethod
    def from_file(cls, path: str | Path) -> "Config":
        """Load from a TOML or JSON config file."""
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {p}")

        if p.suffix.lower() == ".toml":
            return cls._from_toml(p)
        elif p.suffix.lower() in (".json", ".jsonc"):
            return cls._from_json(p)
        else:
            raise ValueError(f"Unsupported config format: {p.suffix}")

    @classmethod
    def _from_toml(cls, path: Path) -> "Config":
        try:
            import tomllib  # Python 3.11+
        except ImportError:
            try:
                import tomli as tomllib  # backport
            except ImportError:
                raise ImportError(
                    "TOML support requires Python 3.11+ or 'tomli' package. "
                    "Run: pip install tomli"
                )
        with open(path, "rb") as fh:
            data = tomllib.load(fh)
        return cls._from_dict(data)

    @classmethod
    def _from_json(cls, path: Path) -> "Config":
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return cls._from_dict(data)

    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "Config":
        """Build Config from a raw dictionary, ignoring unknown keys."""
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}
        return cls(**filtered)

    @classmethod
    def from_env(cls, base: Optional["Config"] = None) -> "Config":
        """
        Override config fields with GH_AUTOFOLLOW_* environment variables.

        e.g.  GH_AUTOFOLLOW_GITHUB_TOKEN=...
              GH_AUTOFOLLOW_BATCH_SIZE=20
              GH_AUTOFOLLOW_DRY_RUN=true
        """
        cfg = base or cls()
        prefix = "GH_AUTOFOLLOW_"

        field_map: Dict[str, Any] = {f.name: f for f in fields(cls)}

        for key, raw_val in os.environ.items():
            if not key.startswith(prefix):
                continue
            field_name = key[len(prefix):].lower()
            if field_name not in field_map:
                continue

            f = field_map[field_name]
            origin = getattr(f.type, "__origin__", None)
            try:
                if f.type in (bool, "bool") or (
                    hasattr(f, "default") and isinstance(f.default, bool)
                ):
                    value = raw_val.lower() in ("1", "true", "yes", "on")
                elif f.type in (int, "int"):
                    value = int(raw_val)
                elif f.type in (float, "float"):
                    value = float(raw_val)
                elif origin is list or str(f.type).startswith("List"):
                    value = [v.strip() for v in raw_val.split(",") if v.strip()]
                else:
                    value = raw_val
                object.__setattr__(cfg, field_name, value)
            except (ValueError, TypeError) as exc:
                logger.warning("Failed to parse env %s=%r: %s", key, raw_val, exc)

        return cfg

    @classmethod
    def load(cls, config_file: Optional[str] = None) -> "Config":
        """
        Canonical loader: file → env overrides.

        Searches for config in standard locations if config_file is not given.
        """
        cfg: "Config"

        if config_file:
            cfg = cls.from_file(config_file)
        else:
            default_paths = [
                _default_config_dir() / "config.toml",
                _default_config_dir() / "config.json",
                Path.cwd() / "gh-autofollow.toml",
                Path.cwd() / "gh-autofollow.json",
            ]
            for p in default_paths:
                if p.exists():
                    logger.debug("Loading config from %s", p)
                    cfg = cls.from_file(p)
                    break
            else:
                cfg = cls()

        cfg = cls.from_env(base=cfg)

        # Final: GITHUB_TOKEN env var (common convention) as fallback
        if not cfg.github_token:
            cfg.github_token = os.environ.get("GITHUB_TOKEN", "")

        return cfg

    def validate(self) -> None:
        """Raise ValueError if configuration is invalid."""
        errors: List[str] = []

        if not self.github_token:
            errors.append("github_token is required (set GH_AUTOFOLLOW_GITHUB_TOKEN or GITHUB_TOKEN)")

        if self.batch_size < 1:
            errors.append("batch_size must be >= 1")

        if self.batch_interval < 60:
            errors.append("batch_interval must be >= 60 seconds")

        if self.follow_delay_min < 0:
            errors.append("follow_delay_min must be >= 0")

        if self.follow_delay_max < self.follow_delay_min:
            errors.append("follow_delay_max must be >= follow_delay_min")

        if self.rate_limit_buffer < 0:
            errors.append("rate_limit_buffer must be >= 0")

        valid_strategies = {
            "trending", "followers_of_following",
            "starred_repos", "topic_search", "random_explore",
        }
        for s in self.strategies:
            if s not in valid_strategies:
                errors.append(f"Unknown strategy: {s!r}. Valid: {valid_strategies}")

        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level.upper() not in valid_levels:
            errors.append(f"log_level must be one of {valid_levels}")

        if errors:
            raise ValueError("Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors))

    def ensure_dirs(self) -> None:
        """Create data / log directories if they don't exist."""
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dictionary (suitable for JSON/TOML export)."""
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
        }

    def save(self, path: Optional[str | Path] = None) -> Path:
        """Persist configuration as JSON."""
        target = Path(path) if path else (_default_config_dir() / "config.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "w", encoding="utf-8") as fh:
            # Mask token in saved output
            data = self.to_dict()
            data["github_token"] = "*** REDACTED ***"
            json.dump(data, fh, indent=2)
        return target

    def __repr__(self) -> str:
        token_preview = (self.github_token[:4] + "****") if self.github_token else "<not set>"
        return (
            f"Config(token={token_preview}, batch_size={self.batch_size}, "
            f"strategies={self.strategies}, dry_run={self.dry_run})"
        )
