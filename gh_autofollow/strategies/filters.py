"""
Filter pipeline that validates discovered candidates against configured rules.

Each filter is a callable: (candidate, config, db) -> (bool, reason)
True = keep,  False = skip.
"""

from __future__ import annotations

import logging
import re
from typing import Callable, List, Optional, Tuple

from gh_autofollow.config import Config
from gh_autofollow.db.database import Candidate, Database

logger = logging.getLogger(__name__)

FilterResult = Tuple[bool, str]
FilterFn = Callable[[Candidate, Config, Database], FilterResult]


# ── Individual filters ────────────────────────────────────────────────────────

def filter_already_followed(c: Candidate, cfg: Config, db: Database) -> FilterResult:
    if db.is_followed(c.login):
        return False, "already_followed"
    return True, ""


def filter_blocked(c: Candidate, cfg: Config, db: Database) -> FilterResult:
    if db.is_blocked(c.login):
        return False, "blocked"
    return True, ""


def filter_blocklist(c: Candidate, cfg: Config, db: Database) -> FilterResult:
    if c.login in cfg.blocklist:
        return False, "in_blocklist"
    return True, ""


def filter_orgs(c: Candidate, cfg: Config, db: Database) -> FilterResult:
    if cfg.skip_orgs and c.is_org:
        return False, "is_org"
    return True, ""


def filter_bots(c: Candidate, cfg: Config, db: Database) -> FilterResult:
    if cfg.skip_bots:
        bot_patterns = [r"\[bot\]", r"-bot$", r"^bot-"]
        for pat in bot_patterns:
            if re.search(pat, c.login, re.IGNORECASE):
                return False, "is_bot"
    return True, ""


def filter_min_followers(c: Candidate, cfg: Config, db: Database) -> FilterResult:
    if cfg.min_followers > 0 and c.followers_count < cfg.min_followers:
        return False, f"followers_below_{cfg.min_followers}"
    return True, ""


def filter_max_followers(c: Candidate, cfg: Config, db: Database) -> FilterResult:
    if cfg.max_followers > 0 and c.followers_count > cfg.max_followers:
        return False, f"followers_above_{cfg.max_followers}"
    return True, ""


def filter_min_repos(c: Candidate, cfg: Config, db: Database) -> FilterResult:
    if cfg.min_public_repos > 0 and c.public_repos < cfg.min_public_repos:
        return False, f"repos_below_{cfg.min_public_repos}"
    return True, ""


def filter_invalid_login(c: Candidate, cfg: Config, db: Database) -> FilterResult:
    if not c.login or len(c.login) < 1:
        return False, "empty_login"
    # GitHub login: alphanumeric + hyphens, no leading/trailing hyphen, max 39 chars
    if not re.match(r"^[a-zA-Z0-9]([a-zA-Z0-9-]{0,37}[a-zA-Z0-9])?$", c.login):
        return False, "invalid_login"
    return True, ""


# ── Pipeline ──────────────────────────────────────────────────────────────────

_DEFAULT_FILTERS: List[FilterFn] = [
    filter_invalid_login,
    filter_already_followed,
    filter_blocked,
    filter_blocklist,
    filter_orgs,
    filter_bots,
    filter_min_followers,
    filter_max_followers,
    filter_min_repos,
]


class FilterPipeline:
    """
    Runs candidates through a sequence of filters.
    Short-circuits on first rejection.
    """

    def __init__(self, filters: Optional[List[FilterFn]] = None) -> None:
        self._filters = filters if filters is not None else list(_DEFAULT_FILTERS)

    def add_filter(self, fn: FilterFn) -> None:
        self._filters.append(fn)

    def check(
        self,
        candidate: Candidate,
        config: Config,
        db: Database,
    ) -> Tuple[bool, str]:
        """
        Returns (True, "") if candidate passes all filters,
        or (False, reason) on first rejection.
        """
        for fn in self._filters:
            ok, reason = fn(candidate, config, db)
            if not ok:
                return False, reason
        return True, ""

    def filter_batch(
        self,
        candidates: List[Candidate],
        config: Config,
        db: Database,
    ) -> Tuple[List[Candidate], List[Tuple[Candidate, str]]]:
        """
        Returns (accepted, [(rejected_candidate, reason), ...]).
        """
        accepted: List[Candidate] = []
        rejected: List[Tuple[Candidate, str]] = []

        for c in candidates:
            ok, reason = self.check(c, config, db)
            if ok:
                accepted.append(c)
            else:
                rejected.append((c, reason))

        return accepted, rejected
