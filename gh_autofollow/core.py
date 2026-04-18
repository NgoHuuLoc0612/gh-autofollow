"""
Core AutoFollower engine.

Orchestrates:
  - Candidate discovery (via strategies)
  - Filtering
  - Follow execution with jitter delays
  - Rate-limit awareness and back-off
  - Run logging
  - Max-following enforcement
"""

from __future__ import annotations

import logging
import random
import time
import uuid
from typing import Callable, Dict, List, Optional, Tuple

from gh_autofollow.api.client import (
    GitHubAPIError,
    GitHubClient,
    RateLimitExceeded,
)
from gh_autofollow.config import Config
from gh_autofollow.db.database import Candidate, Database, FollowedUser, RunRecord
from gh_autofollow.strategies.discovery import get_all_strategies
from gh_autofollow.strategies.filters import FilterPipeline

logger = logging.getLogger(__name__)

# Callback signature: (event_name, payload_dict)
EventCallback = Callable[[str, Dict], None]


class AutoFollower:
    """
    High-level orchestrator for the follow automation.

    Usage::

        config = Config.load()
        config.validate()
        config.ensure_dirs()

        with AutoFollower(config) as af:
            result = af.run_batch()
    """

    def __init__(
        self,
        config: Config,
        db: Optional[Database] = None,
        client: Optional[GitHubClient] = None,
        filter_pipeline: Optional[FilterPipeline] = None,
        on_event: Optional[EventCallback] = None,
        security=None,
    ) -> None:
        self.config = config
        self._db = db
        self._client = client
        self._filters = filter_pipeline or FilterPipeline()
        self._on_event = on_event
        self._me: Optional[Dict] = None
        self._security = security

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "AutoFollower":
        self._open()
        return self

    def __exit__(self, *_) -> None:
        self._close()

    def _open(self) -> None:
        if self._db is None:
            self._db = Database(self.config.db_path, wal_mode=self.config.db_wal_mode)
        if self._client is None:
            self._client = GitHubClient(
                token=self.config.github_token,
                base_url=self.config.api_base_url,
                timeout=self.config.api_timeout,
                max_retries=self.config.api_max_retries,
                retry_backoff=self.config.api_retry_backoff,
                rate_limit_buffer=self.config.rate_limit_buffer,
            )
        # Build and attach security middleware if enabled
        if self.config.security_enabled and self._security is None:
            from gh_autofollow.security import (
                AnomalyDetector, SecurityMiddleware, VelocityGuard
            )
            self._security = SecurityMiddleware(
                guard=VelocityGuard(
                    per_minute=self.config.velocity_per_minute,
                    per_hour=self.config.velocity_per_hour,
                    per_day=self.config.velocity_per_day,
                    db_path=str(self.config.db_path),
                ),
                detector=AnomalyDetector(),
                health_check_interval=self.config.health_check_interval,
                abort_on_critical=self.config.anomaly_abort_on_critical,
            )
        if self._security:
            self._security.attach(self)
        # Load and cache auth
        self._me = self._client.get_authenticated_user()
        logger.info("Authenticated as %s", self._me.get("login"))
        self._emit("authenticated", {"login": self._me.get("login")})

    def _close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
        if self._db:
            self._db.close()
            self._db = None

    # ── Event bus ─────────────────────────────────────────────────────────────

    def _emit(self, event: str, payload: Dict) -> None:
        if self._on_event:
            try:
                self._on_event(event, payload)
            except Exception as exc:
                logger.debug("Event callback raised: %s", exc)

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def db(self) -> Database:
        if self._db is None:
            raise RuntimeError("AutoFollower must be used as context manager or _open() called")
        return self._db

    @property
    def client(self) -> GitHubClient:
        if self._client is None:
            raise RuntimeError("AutoFollower must be used as context manager or _open() called")
        return self._client

    def run_batch(self) -> RunRecord:
        """
        Execute a single batch: discover candidates, filter, follow.
        Returns a RunRecord with stats for this batch.
        """
        run_id = str(uuid.uuid4())
        record = self.db.start_run(run_id, self.config.batch_size)
        logger.info("=== Batch run %s started ===", run_id)
        self._emit("batch_start", {"run_id": run_id, "batch_size": self.config.batch_size})

        try:
            # Security pre-batch checks (anomaly detection + health)
            if self._security:
                security_alerts = self._security.run_pre_batch_checks(self)
                self._security.run_health_check(self)
                if self._security._paused:
                    record.status = "paused_security"
                    record.notes = self._security._pause_reason
                    return record

            self._check_max_following(record)
            if record.status == "skipped":
                return record

            self._refresh_rate_limits()

            # Replenish candidate pool if needed
            pending = self.db.candidate_count(unattempted_only=True)
            if pending < self.config.batch_size * 2:
                logger.info("Candidate pool low (%d); discovering more...", pending)
                self._discover_candidates()

            # Pop candidates and follow them
            candidates = self.db.pop_candidates(self.config.batch_size)
            if not candidates:
                logger.warning("No candidates available; try running discovery first")
                record.status = "no_candidates"
                record.notes = "candidate pool empty"
                return record

            self._execute_follows(candidates, record)

            record.status = "completed"
            self._emit("batch_complete", {
                "run_id": run_id,
                "followed": record.followed_count,
                "skipped": record.skipped_count,
                "errors": record.error_count,
            })

        except RateLimitExceeded as exc:
            record.rate_limit_hit = True
            record.status = "rate_limited"
            record.notes = str(exc)
            logger.warning("Rate limit hit during batch: %s", exc)
            self._emit("rate_limit_hit", {"reset_at": exc.reset_at})

        except Exception as exc:
            record.status = "failed"
            record.notes = str(exc)
            logger.error("Batch run failed: %s", exc, exc_info=True)
            self._emit("batch_error", {"error": str(exc)})

        finally:
            self.db.finish_run(record)
            logger.info(
                "=== Batch run %s finished: status=%s followed=%d skipped=%d errors=%d ===",
                run_id, record.status, record.followed_count,
                record.skipped_count, record.error_count,
            )

        return record

    def discover_candidates(self) -> int:
        """
        Public entry point to run discovery without following.
        Returns number of new candidates added to the cache.
        """
        return self._discover_candidates()

    def get_stats(self) -> Dict:
        return {
            "db": self.db.get_summary(),
            "strategy_breakdown": self.db.get_follow_stats(),
            "rate_limit": {
                "core_remaining": self.client.rate_limit.core_remaining,
                "core_limit": self.client.rate_limit.core_limit,
                "core_reset_in": self.client.rate_limit.seconds_until_core_reset(),
                "search_remaining": self.client.rate_limit.search_remaining,
            },
        }

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _check_max_following(self, record: RunRecord) -> None:
        """Abort the batch if we have already hit max_following."""
        if self.config.max_following <= 0:
            return
        current = self.db.followed_count()
        # Also try to get live following count from GitHub
        try:
            me = self.client.get_authenticated_user()
            live_following = me.get("following", current)
            current = max(current, live_following)
        except Exception:
            pass

        if current >= self.config.max_following:
            logger.info(
                "Max following limit reached (%d/%d); skipping batch",
                current, self.config.max_following,
            )
            record.status = "skipped"
            record.notes = f"max_following={self.config.max_following} reached"
            self._emit("max_following_reached", {
                "current": current, "max": self.config.max_following
            })

    def _refresh_rate_limits(self) -> None:
        try:
            data = self.client.get_rate_limits()
            core = data.get("resources", {}).get("core", {})
            search = data.get("resources", {}).get("search", {})
            self.db.log_rate_limit(
                core_limit=core.get("limit", 0),
                core_remaining=core.get("remaining", 0),
                core_reset=core.get("reset", 0),
                search_limit=search.get("limit", 0),
                search_remaining=search.get("remaining", 0),
                search_reset=search.get("reset", 0),
            )
            logger.debug(
                "Rate limits: core %d/%d (reset %ds)",
                core.get("remaining", 0),
                core.get("limit", 0),
                max(0, core.get("reset", 0) - time.time()),
            )
        except Exception as exc:
            logger.warning("Could not refresh rate limits: %s", exc)

    def _discover_candidates(self) -> int:
        """Run all configured strategies and populate the candidate cache."""
        strategies = get_all_strategies(self.config.strategies)
        all_candidates: List[Candidate] = []

        for strategy in strategies:
            if self.client.rate_limit.is_core_exhausted(self.config.rate_limit_buffer):
                logger.warning("Rate limit low; stopping discovery early")
                break
            try:
                logger.info("Running discovery strategy: %s", strategy.name)
                candidates = strategy.discover(self.client, self.config, self.db)
                all_candidates.extend(candidates)
                self._emit("strategy_complete", {
                    "strategy": strategy.name,
                    "found": len(candidates),
                })
            except RateLimitExceeded as exc:
                logger.warning("Rate limit during strategy %s: %s", strategy.name, exc)
                break
            except Exception as exc:
                logger.error("Strategy %s failed: %s", strategy.name, exc, exc_info=True)

        # Filter candidates before caching
        _, rejected = self._filters.filter_batch(all_candidates, self.config, self.db)
        accepted_candidates = [
            c for c in all_candidates
            if not any(c.login == r.login for r, _ in rejected)
        ]

        # Mark rejected candidates in DB
        for candidate, reason in rejected:
            self.db.skip_candidate(candidate.login, reason)

        inserted = self.db.add_candidates(accepted_candidates)
        logger.info(
            "Discovery complete: %d total, %d new, %d filtered out",
            len(all_candidates), inserted, len(rejected),
        )
        return inserted

    def _execute_follows(self, candidates: List[Candidate], record: RunRecord) -> None:
        """Follow each candidate with jitter, updating the run record."""
        for candidate in candidates:
            if self.client.rate_limit.is_core_exhausted(self.config.rate_limit_buffer):
                logger.warning("Rate limit buffer reached; stopping batch early")
                record.rate_limit_hit = True
                break

            ok, reason = self._filters.check(candidate, self.config, self.db)
            if not ok:
                logger.debug("Filtered out %s: %s", candidate.login, reason)
                self.db.skip_candidate(candidate.login, reason)
                record.skipped_count += 1
                continue

            self._follow_one(candidate, record)

            # Jitter delay between follows to look human
            delay = random.uniform(
                self.config.follow_delay_min,
                self.config.follow_delay_max,
            )
            logger.debug("Sleeping %.2fs before next follow", delay)
            time.sleep(delay)

    def _follow_one(self, candidate: Candidate, record: RunRecord) -> None:
        login = candidate.login
        try:
            if self.config.dry_run:
                logger.info("[DRY RUN] Would follow %s", login)
                record.followed_count += 1
                self._emit("follow", {"login": login, "dry_run": True})
                return

            success = self.client.follow_user(login)
            if success:
                self.db.record_follow(
                    FollowedUser(
                        login=login,
                        github_id=candidate.github_id,
                        followed_at=time.time(),
                        via_strategy=candidate.via_strategy,
                        batch_run_id=record.id,
                        followers_count=candidate.followers_count,
                        public_repos=candidate.public_repos,
                        is_org=candidate.is_org,
                    )
                )
                record.followed_count += 1
                logger.info("Followed %s (followers=%d, repos=%d)", login,
                            candidate.followers_count, candidate.public_repos)
                self._emit("follow", {"login": login, "strategy": candidate.via_strategy})
            else:
                # 304 or already following
                self.db.skip_candidate(login, "already_following_on_github")
                record.skipped_count += 1

        except RateLimitExceeded:
            raise  # propagate to run_batch

        except GitHubAPIError as exc:
            logger.warning("API error following %s: %s", login, exc)
            record.error_count += 1
            if exc.status_code == 404:
                self.db.skip_candidate(login, "user_not_found")
            elif exc.status_code == 403:
                self.db.skip_candidate(login, "forbidden")

        except Exception as exc:
            logger.error("Unexpected error following %s: %s", login, exc)
            record.error_count += 1
