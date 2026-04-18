"""
Tests for gh-autofollow.

Run with:  pytest tests/ -v
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_dir(tmp_path):
    return tmp_path


@pytest.fixture
def config(tmp_dir):
    from gh_autofollow.config import Config
    cfg = Config(
        github_token="ghp_test_token_1234567890abcdef",
        data_dir=str(tmp_dir / "data"),
        log_dir=str(tmp_dir / "logs"),
        batch_size=5,
        batch_interval=300,
        dry_run=True,
    )
    cfg.ensure_dirs()
    return cfg


@pytest.fixture
def db(config):
    from gh_autofollow.db.database import Database
    database = Database(config.db_path)
    yield database
    database.close()


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.rate_limit = MagicMock()
    client.rate_limit.core_remaining = 4000
    client.rate_limit.core_limit = 5000
    client.rate_limit.core_reset = int(time.time()) + 3600
    client.rate_limit.search_remaining = 25
    client.rate_limit.is_core_exhausted.return_value = False
    client.get_authenticated_user.return_value = {
        "login": "testuser",
        "id": 12345,
        "following": 10,
        "followers": 50,
    }
    client.get_rate_limits.return_value = {
        "resources": {
            "core": {"limit": 5000, "remaining": 4000, "reset": int(time.time()) + 3600},
            "search": {"limit": 30, "remaining": 25, "reset": int(time.time()) + 60},
        }
    }
    return client


# ── Config tests ──────────────────────────────────────────────────────────────

class TestConfig:

    def test_defaults(self):
        from gh_autofollow.config import Config
        cfg = Config(github_token="token")
        assert cfg.batch_size == 10
        assert cfg.batch_interval == 3600
        assert "trending" in cfg.strategies

    def test_validation_missing_token(self):
        from gh_autofollow.config import Config
        cfg = Config()
        with pytest.raises(ValueError, match="github_token"):
            cfg.validate()

    def test_validation_bad_batch_interval(self):
        from gh_autofollow.config import Config
        cfg = Config(github_token="tok", batch_interval=5)
        with pytest.raises(ValueError, match="batch_interval"):
            cfg.validate()

    def test_validation_bad_delay(self):
        from gh_autofollow.config import Config
        cfg = Config(github_token="tok", follow_delay_min=10, follow_delay_max=5)
        with pytest.raises(ValueError, match="follow_delay_max"):
            cfg.validate()

    def test_from_json(self, tmp_dir):
        from gh_autofollow.config import Config
        data = {
            "github_token": "ghp_abc",
            "batch_size": 20,
            "dry_run": True,
        }
        cfg_file = tmp_dir / "cfg.json"
        cfg_file.write_text(json.dumps(data))
        cfg = Config.from_file(cfg_file)
        assert cfg.github_token == "ghp_abc"
        assert cfg.batch_size == 20
        assert cfg.dry_run is True

    def test_from_env(self, monkeypatch):
        from gh_autofollow.config import Config
        monkeypatch.setenv("GH_AUTOFOLLOW_GITHUB_TOKEN", "ghp_env_token")
        monkeypatch.setenv("GH_AUTOFOLLOW_BATCH_SIZE", "25")
        monkeypatch.setenv("GH_AUTOFOLLOW_DRY_RUN", "true")
        cfg = Config.from_env()
        assert cfg.github_token == "ghp_env_token"
        assert cfg.batch_size == 25
        assert cfg.dry_run is True

    def test_db_path(self, config):
        assert config.db_path.name == "gh_autofollow.db"

    def test_ensure_dirs(self, config, tmp_dir):
        assert (tmp_dir / "data").exists()
        assert (tmp_dir / "logs").exists()

    def test_to_dict_round_trip(self, config):
        from gh_autofollow.config import Config
        data = config.to_dict()
        cfg2 = Config._from_dict(data)
        assert cfg2.batch_size == config.batch_size
        assert cfg2.strategies == config.strategies


# ── Database tests ────────────────────────────────────────────────────────────

class TestDatabase:

    def test_schema_created(self, db):
        conn = db._conn
        tables = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "followed_users" in tables
        assert "candidate_cache" in tables
        assert "run_log" in tables
        assert "blocked_users" in tables

    def test_record_and_check_follow(self, db):
        from gh_autofollow.db.database import FollowedUser
        assert not db.is_followed("alice")
        db.record_follow(FollowedUser(login="alice", github_id=1001))
        assert db.is_followed("alice")
        assert db.followed_count() == 1

    def test_add_and_pop_candidates(self, db):
        from gh_autofollow.db.database import Candidate
        candidates = [
            Candidate(login=f"user{i}", score=float(i)) for i in range(10)
        ]
        added = db.add_candidates(candidates)
        assert added == 10
        assert db.candidate_count() == 10

        popped = db.pop_candidates(3)
        assert len(popped) == 3
        # Should be highest scores first
        assert popped[0].score >= popped[1].score >= popped[2].score
        # Those 3 should no longer be in the unattempted pool
        assert db.candidate_count(unattempted_only=True) == 7

    def test_add_candidate_skip_already_followed(self, db):
        from gh_autofollow.db.database import Candidate, FollowedUser
        db.record_follow(FollowedUser(login="bob"))
        added = db.add_candidates([Candidate(login="bob")])
        assert added == 0

    def test_block_user(self, db):
        db.block_user("spammer", reason="spam")
        assert db.is_blocked("spammer")
        blocked = db.get_blocked_logins()
        assert "spammer" in blocked

    def test_add_candidate_skip_blocked(self, db):
        from gh_autofollow.db.database import Candidate
        db.block_user("evil_user")
        added = db.add_candidates([Candidate(login="evil_user")])
        assert added == 0

    def test_skip_candidate(self, db):
        from gh_autofollow.db.database import Candidate
        db.add_candidates([Candidate(login="skipme")])
        db.skip_candidate("skipme", "test_reason")
        assert db.candidate_count(unattempted_only=True) == 0

    def test_run_log(self, db):
        record = db.start_run("run-001", batch_size=10)
        assert record.status == "running"
        record.followed_count = 5
        record.status = "completed"
        db.finish_run(record)

        runs = db.recent_runs(limit=5)
        assert len(runs) == 1
        assert runs[0]["followed_count"] == 5
        assert runs[0]["status"] == "completed"

    def test_prune_candidates(self, db):
        from gh_autofollow.db.database import Candidate
        db.add_candidates([Candidate(login="fresh")])
        # Mark one as attempted
        db.pop_candidates(1)
        removed = db.prune_candidates(max_age_days=0)
        assert removed >= 1

    def test_get_summary(self, db):
        summary = db.get_summary()
        assert "total_followed" in summary
        assert "candidates_pending" in summary
        assert "blocked_users" in summary

    def test_rate_limit_log(self, db):
        db.log_rate_limit(5000, 3500, int(time.time()) + 3600)
        row = db.latest_rate_limit()
        assert row is not None
        assert row["core_remaining"] == 3500

    def test_duplicate_follow_is_idempotent(self, db):
        from gh_autofollow.db.database import FollowedUser
        db.record_follow(FollowedUser(login="dupuser"))
        db.record_follow(FollowedUser(login="dupuser"))  # should not raise
        assert db.followed_count() == 1

    def test_integrity_check(self, db):
        assert db.integrity_check() is True


# ── Filter tests ─────────────────────────────────────────────────────────────

class TestFilters:

    def test_filter_already_followed(self, config, db):
        from gh_autofollow.db.database import Candidate, FollowedUser
        from gh_autofollow.strategies.filters import filter_already_followed
        db.record_follow(FollowedUser(login="alice"))
        c = Candidate(login="alice")
        ok, reason = filter_already_followed(c, config, db)
        assert not ok
        assert reason == "already_followed"

    def test_filter_org_skip(self, config, db):
        from gh_autofollow.db.database import Candidate
        from gh_autofollow.strategies.filters import filter_orgs
        config.skip_orgs = True
        c = Candidate(login="myorg", is_org=True)
        ok, reason = filter_orgs(c, config, db)
        assert not ok

    def test_filter_bot(self, config, db):
        from gh_autofollow.db.database import Candidate
        from gh_autofollow.strategies.filters import filter_bots
        config.skip_bots = True
        c = Candidate(login="dependabot[bot]")
        ok, reason = filter_bots(c, config, db)
        assert not ok

    def test_filter_min_followers(self, config, db):
        from gh_autofollow.db.database import Candidate
        from gh_autofollow.strategies.filters import filter_min_followers
        config.min_followers = 100
        c = Candidate(login="newbie", followers_count=5)
        ok, reason = filter_min_followers(c, config, db)
        assert not ok

    def test_filter_invalid_login(self, config, db):
        from gh_autofollow.db.database import Candidate
        from gh_autofollow.strategies.filters import filter_invalid_login
        c = Candidate(login="")
        ok, reason = filter_invalid_login(c, config, db)
        assert not ok

    def test_filter_pipeline_accept(self, config, db):
        from gh_autofollow.db.database import Candidate
        from gh_autofollow.strategies.filters import FilterPipeline
        pipeline = FilterPipeline()
        c = Candidate(login="validuser", followers_count=50, public_repos=10)
        ok, reason = pipeline.check(c, config, db)
        assert ok
        assert reason == ""

    def test_filter_pipeline_batch(self, config, db):
        from gh_autofollow.db.database import Candidate, FollowedUser
        from gh_autofollow.strategies.filters import FilterPipeline
        db.record_follow(FollowedUser(login="followed_user"))
        pipeline = FilterPipeline()
        candidates = [
            Candidate(login="validuser"),
            Candidate(login="followed_user"),
            Candidate(login=""),
        ]
        accepted, rejected = pipeline.filter_batch(candidates, config, db)
        assert len(accepted) == 1
        assert accepted[0].login == "validuser"
        assert len(rejected) == 2


# ── Core AutoFollower tests ───────────────────────────────────────────────────

class TestAutoFollower:

    def test_dry_run_follow_batch(self, config, mock_client):
        from gh_autofollow.core import AutoFollower
        from gh_autofollow.db.database import Candidate, Database

        config.dry_run = True
        db = Database(config.db_path)

        # Seed candidates
        candidates = [Candidate(login=f"dryuser{i}", score=float(i)) for i in range(5)]
        db.add_candidates(candidates)

        af = AutoFollower(config, db=db, client=mock_client)
        af._open = lambda: None   # skip re-init
        af._me = {"login": "testuser"}

        record = af.run_batch()
        assert record.followed_count == 5
        assert record.status == "completed"
        db.close()

    def test_max_following_limit(self, config, mock_client):
        from gh_autofollow.core import AutoFollower
        from gh_autofollow.db.database import Database

        config.max_following = 5
        mock_client.get_authenticated_user.return_value = {
            "login": "testuser", "following": 5
        }
        db = Database(config.db_path)
        af = AutoFollower(config, db=db, client=mock_client)
        af._open = lambda: None
        af._me = {"login": "testuser"}

        record = af.run_batch()
        assert record.status == "skipped"
        db.close()

    def test_events_emitted(self, config, mock_client):
        from gh_autofollow.core import AutoFollower
        from gh_autofollow.db.database import Candidate, Database

        events = []
        def on_event(name, payload):
            events.append(name)

        config.dry_run = True
        db = Database(config.db_path)
        db.add_candidates([Candidate(login="eve1"), Candidate(login="eve2")])

        af = AutoFollower(config, db=db, client=mock_client, on_event=on_event)
        af._open = lambda: None
        af._me = {"login": "testuser"}

        af.run_batch()
        assert "batch_start" in events
        assert "batch_complete" in events
        db.close()


# ── Strategy tests ────────────────────────────────────────────────────────────

class TestStrategies:

    def test_get_strategy_valid(self):
        from gh_autofollow.strategies.discovery import get_strategy
        s = get_strategy("trending")
        assert s.name == "trending"

    def test_get_strategy_invalid(self):
        from gh_autofollow.strategies.discovery import get_strategy
        with pytest.raises(ValueError):
            get_strategy("nonexistent_strategy")

    def test_get_all_strategies(self):
        from gh_autofollow.strategies.discovery import get_all_strategies
        strategies = get_all_strategies(["trending", "random_explore"])
        assert len(strategies) == 2

    def test_scoring(self):
        from gh_autofollow.strategies.discovery import _score
        # More followers/repos → higher score
        high = _score({"followers": 5000, "public_repos": 50, "following": 100})
        low = _score({"followers": 2, "public_repos": 1, "following": 0})
        assert high > low


# ── CLI tests ─────────────────────────────────────────────────────────────────

class TestCLI:

    def test_help(self):
        from gh_autofollow.cli import main
        with pytest.raises(SystemExit) as exc:
            main(["--help"])
        assert exc.value.code == 0

    def test_config_show(self, monkeypatch, tmp_path):
        from gh_autofollow.cli import main
        monkeypatch.setenv("GH_AUTOFOLLOW_GITHUB_TOKEN", "ghp_test")
        monkeypatch.setenv("GH_AUTOFOLLOW_DATA_DIR", str(tmp_path / "data"))
        monkeypatch.setenv("GH_AUTOFOLLOW_LOG_DIR", str(tmp_path / "logs"))
        ret = main(["config", "show"])
        assert ret == 0

    def test_config_validate_missing_token(self, monkeypatch, tmp_path):
        from gh_autofollow.cli import main
        monkeypatch.delenv("GH_AUTOFOLLOW_GITHUB_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        monkeypatch.setenv("GH_AUTOFOLLOW_DATA_DIR", str(tmp_path / "data"))
        monkeypatch.setenv("GH_AUTOFOLLOW_LOG_DIR", str(tmp_path / "logs"))
        ret = main(["config", "validate"])
        assert ret == 1

    def test_db_summary(self, monkeypatch, tmp_path):
        from gh_autofollow.cli import main
        monkeypatch.setenv("GH_AUTOFOLLOW_GITHUB_TOKEN", "ghp_test")
        monkeypatch.setenv("GH_AUTOFOLLOW_DATA_DIR", str(tmp_path / "data"))
        monkeypatch.setenv("GH_AUTOFOLLOW_LOG_DIR", str(tmp_path / "logs"))
        # Create DB first
        from gh_autofollow.config import Config
        cfg = Config.load()
        cfg.ensure_dirs()
        from gh_autofollow.db.database import Database
        db = Database(cfg.db_path)
        db.close()

        ret = main(["db", "summary"])
        assert ret == 0

    def test_blocklist_add_and_list(self, monkeypatch, tmp_path):
        from gh_autofollow.cli import main
        monkeypatch.setenv("GH_AUTOFOLLOW_GITHUB_TOKEN", "ghp_test")
        monkeypatch.setenv("GH_AUTOFOLLOW_DATA_DIR", str(tmp_path / "data"))
        monkeypatch.setenv("GH_AUTOFOLLOW_LOG_DIR", str(tmp_path / "logs"))

        from gh_autofollow.config import Config
        cfg = Config.load()
        cfg.ensure_dirs()

        ret = main(["blocklist", "add", "baduser1"])
        assert ret == 0
        ret = main(["blocklist", "list"])
        assert ret == 0
