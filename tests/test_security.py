"""
Tests for gh_autofollow.security module.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def tmp_dir(tmp_path):
    return tmp_path


# ═══════════════════════════════════════════════════════════════════════════════
# TokenVault
# ═══════════════════════════════════════════════════════════════════════════════

class TestTokenVault:

    def test_encrypted_file_store_retrieve(self, tmp_dir):
        pytest.importorskip("cryptography")
        from gh_autofollow.security import TokenVault
        vault = TokenVault(data_dir=str(tmp_dir))
        # Force encrypted path by pretending keyring is unavailable
        with patch.object(TokenVault, "_keyring_available", return_value=False):
            backend = vault.store("ghp_testtoken1234567890")
            assert backend == "encrypted_file"
            retrieved = vault.retrieve()
            assert retrieved == "ghp_testtoken1234567890"

    def test_encrypted_file_is_not_plaintext(self, tmp_dir):
        pytest.importorskip("cryptography")
        from gh_autofollow.security import TokenVault
        vault = TokenVault(data_dir=str(tmp_dir))
        with patch.object(TokenVault, "_keyring_available", return_value=False):
            vault.store("ghp_supersecrettoken123")
            enc_content = (tmp_dir / ".token.enc").read_text()
            assert "ghp_supersecrettoken123" not in enc_content
            # It should be valid JSON with ct/nonce/salt
            data = json.loads(enc_content)
            assert "ct" in data
            assert "nonce" in data
            assert "salt" in data

    def test_delete_removes_file(self, tmp_dir):
        pytest.importorskip("cryptography")
        from gh_autofollow.security import TokenVault
        vault = TokenVault(data_dir=str(tmp_dir))
        with patch.object(TokenVault, "_keyring_available", return_value=False):
            vault.store("ghp_token_to_delete")
            assert (tmp_dir / ".token.enc").exists()
            vault.delete()
            assert not (tmp_dir / ".token.enc").exists()

    def test_env_var_fallback(self, tmp_dir, monkeypatch):
        from gh_autofollow.security import TokenVault
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_from_env")
        vault = TokenVault(data_dir=str(tmp_dir))
        with patch.object(TokenVault, "_keyring_available", return_value=False):
            token = vault.retrieve()
            assert token == "ghp_from_env"

    def test_backend_property_none(self, tmp_dir, monkeypatch):
        from gh_autofollow.security import TokenVault
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        monkeypatch.delenv("GH_AUTOFOLLOW_GITHUB_TOKEN", raising=False)
        vault = TokenVault(data_dir=str(tmp_dir))
        with patch.object(TokenVault, "_keyring_available", return_value=False):
            assert vault.backend == "none"

    def test_empty_token_raises(self, tmp_dir):
        from gh_autofollow.security import TokenVault
        vault = TokenVault(data_dir=str(tmp_dir))
        with pytest.raises(ValueError):
            vault.store("")


# ═══════════════════════════════════════════════════════════════════════════════
# VelocityGuard
# ═══════════════════════════════════════════════════════════════════════════════

class TestVelocityGuard:

    def test_records_within_limits(self):
        from gh_autofollow.security import VelocityGuard
        guard = VelocityGuard(per_minute=5, per_hour=20, per_day=100)
        for _ in range(3):
            guard.record_follow()
        can, violation = guard.can_follow()
        assert can
        assert violation is None

    def test_per_minute_limit(self):
        from gh_autofollow.security import VelocityGuard, VelocityViolation
        guard = VelocityGuard(per_minute=3, per_hour=100, per_day=1000)
        for _ in range(3):
            guard.record_follow()
        with pytest.raises(VelocityViolation) as exc_info:
            guard.record_follow()
        assert exc_info.value.window_name == "per_minute"
        assert exc_info.value.current == 3
        assert exc_info.value.limit == 3

    def test_per_hour_limit(self):
        from gh_autofollow.security import VelocityGuard, VelocityViolation
        guard = VelocityGuard(per_minute=100, per_hour=5, per_day=1000)
        for _ in range(5):
            guard.record_follow()
        with pytest.raises(VelocityViolation) as exc_info:
            guard.record_follow()
        assert exc_info.value.window_name == "per_hour"

    def test_per_day_limit(self):
        from gh_autofollow.security import VelocityGuard, VelocityViolation
        guard = VelocityGuard(per_minute=100, per_hour=100, per_day=3)
        for _ in range(3):
            guard.record_follow()
        with pytest.raises(VelocityViolation) as exc_info:
            guard.record_follow()
        assert exc_info.value.window_name == "per_day"

    def test_per_session_limit(self):
        from gh_autofollow.security import VelocityGuard, VelocityViolation
        guard = VelocityGuard(per_minute=100, per_hour=100, per_day=1000, per_session=2)
        guard.record_follow()
        guard.record_follow()
        with pytest.raises(VelocityViolation) as exc_info:
            guard.record_follow()
        assert exc_info.value.window_name == "per_session"

    def test_can_follow_returns_false_at_limit(self):
        from gh_autofollow.security import VelocityGuard
        guard = VelocityGuard(per_minute=2, per_hour=100, per_day=1000)
        guard.record_follow()
        guard.record_follow()
        can, violation = guard.can_follow()
        assert not can
        assert violation is not None
        assert violation.retry_after >= 0

    def test_reset_clears_all_windows(self):
        from gh_autofollow.security import VelocityGuard
        guard = VelocityGuard(per_minute=2, per_hour=100, per_day=1000)
        guard.record_follow()
        guard.record_follow()
        guard.reset()
        can, _ = guard.can_follow()
        assert can

    def test_current_rates(self):
        from gh_autofollow.security import VelocityGuard
        guard = VelocityGuard(per_minute=10, per_hour=50, per_day=200)
        guard.record_follow()
        guard.record_follow()
        rates = guard.current_rates()
        assert rates["per_minute"]["current"] == 2
        assert rates["per_minute"]["limit"] == 10
        assert rates["per_hour"]["current"] == 2
        assert rates["per_day"]["current"] == 2

    def test_db_persistence(self, tmp_dir):
        from gh_autofollow.security import VelocityGuard
        db_path = str(tmp_dir / "test.db")
        guard = VelocityGuard(per_minute=100, per_hour=100, per_day=100, db_path=db_path)
        guard.record_follow()
        guard.record_follow()

        # New guard instance should hydrate from DB
        guard2 = VelocityGuard(per_minute=100, per_hour=100, per_day=100, db_path=db_path)
        rates = guard2.current_rates()
        assert rates["per_hour"]["current"] >= 2

    def test_velocity_violation_str(self):
        from gh_autofollow.security import VelocityViolation
        v = VelocityViolation(window_name="per_hour", current=30, limit=30, retry_after=1800)
        s = str(v)
        assert "per_hour" in s
        assert "30/30" in s


# ═══════════════════════════════════════════════════════════════════════════════
# AnomalyDetector
# ═══════════════════════════════════════════════════════════════════════════════

def _make_run(status="completed", followed=10, skipped=2, errors=0, rl_hit=False, age_secs=0):
    """Build a mock sqlite3.Row-like dict for a run."""
    return {
        "id": f"run-{time.time()}-{age_secs}",
        "started_at": time.time() - age_secs,
        "finished_at": time.time() - age_secs + 5,
        "status": status,
        "followed_count": followed,
        "skipped_count": skipped,
        "error_count": errors,
        "rate_limit_hit": int(rl_hit),
        "notes": None,
    }


class _Row(dict):
    """Minimal sqlite3.Row-compatible dict subclass."""
    def __getitem__(self, key):
        return super().__getitem__(key)
    def keys(self):
        return super().keys()


def _row(**kwargs):
    return _Row(_make_run(**kwargs))


class TestAnomalyDetector:

    def test_no_anomalies_clean_history(self):
        from gh_autofollow.security import AnomalyDetector
        det = AnomalyDetector()
        runs = [_row(status="completed", followed=10, errors=0) for _ in range(5)]
        alerts = det.analyse(runs)
        assert alerts == []

    def test_detects_error_spike(self):
        from gh_autofollow.security import AnomalyDetector
        det = AnomalyDetector(error_rate_threshold=0.3)
        # 8 errors out of 10 total = 80% error rate
        runs = [_row(status="completed", followed=1, skipped=1, errors=8)]
        alerts = det.analyse(runs)
        codes = [a.code for a in alerts]
        assert "error_spike" in codes

    def test_detects_rate_limit_storm(self):
        from gh_autofollow.security import AnomalyDetector
        det = AnomalyDetector(rate_limit_storm_window=3600, rate_limit_storm_count=3)
        runs = [_row(rl_hit=True, age_secs=i * 100) for i in range(4)]
        alerts = det.analyse(runs)
        codes = [a.code for a in alerts]
        assert "rate_limit_storm" in codes

    def test_detects_high_follow_velocity(self):
        from gh_autofollow.security import AnomalyDetector
        det = AnomalyDetector(max_follow_velocity=20)
        # 3 runs of 10 follows each in the last hour = 30 > 20
        runs = [_row(followed=10, age_secs=i * 600) for i in range(3)]
        alerts = det.analyse(runs)
        codes = [a.code for a in alerts]
        assert "high_follow_velocity" in codes

    def test_detects_consecutive_failures(self):
        from gh_autofollow.security import AnomalyDetector
        det = AnomalyDetector(consecutive_fail_threshold=3)
        runs = [_row(status="failed") for _ in range(4)]
        alerts = det.analyse(runs)
        codes = [a.code for a in alerts]
        assert "consecutive_failures" in codes
        critical = [a for a in alerts if a.level == "critical"]
        assert len(critical) >= 1

    def test_detects_possible_403_pattern(self):
        from gh_autofollow.security import AnomalyDetector
        det = AnomalyDetector()
        runs = [_row(status="failed", errors=10, rl_hit=False) for _ in range(3)]
        alerts = det.analyse(runs)
        codes = [a.code for a in alerts]
        assert "possible_account_flag" in codes

    def test_empty_history_returns_no_alerts(self):
        from gh_autofollow.security import AnomalyDetector
        det = AnomalyDetector()
        alerts = det.analyse([])
        assert alerts == []

    def test_anomaly_alert_fields(self):
        from gh_autofollow.security import AnomalyDetector
        det = AnomalyDetector(consecutive_fail_threshold=2)
        runs = [_row(status="failed") for _ in range(3)]
        alerts = det.analyse(runs)
        for a in alerts:
            assert a.level in ("warning", "critical")
            assert a.code
            assert a.message


# ═══════════════════════════════════════════════════════════════════════════════
# SecurityMiddleware
# ═══════════════════════════════════════════════════════════════════════════════

class TestSecurityMiddleware:

    def test_velocity_blocks_in_middleware(self, tmp_dir):
        from gh_autofollow.security import SecurityMiddleware, VelocityGuard

        guard = VelocityGuard(per_minute=2, per_hour=100, per_day=1000)
        mid = SecurityMiddleware(guard=guard, abort_on_critical=False)

        # Use a simple object instead of MagicMock so __func__ isn't needed
        class FakeAF:
            def __init__(self):
                self.called = False
            def _follow_one(self, candidate, record):
                self.called = True
            def _execute_follows(self, candidates, record):
                pass

        fake_af = FakeAF()

        # Exhaust the per_minute limit
        guard.record_follow()
        guard.record_follow()

        mid.attach(fake_af)

        # The next follow should be blocked by velocity guard (skipped_count incremented)
        class FakeRecord:
            skipped_count = 0
            status = "running"
            notes = None
        class FakeCandidate:
            login = "targetuser"

        record = FakeRecord()
        fake_af._follow_one(FakeCandidate(), record)
        assert record.skipped_count == 1
        assert not fake_af.called  # original was NOT called

    def test_critical_alert_pauses_execution(self, tmp_dir):
        from gh_autofollow.security import AnomalyDetector, SecurityMiddleware, VelocityGuard

        guard = VelocityGuard(per_minute=100, per_hour=100, per_day=1000)
        det = AnomalyDetector(consecutive_fail_threshold=2)
        mid = SecurityMiddleware(guard=guard, detector=det, abort_on_critical=True)

        class FakeAF:
            def _follow_one(self, candidate, record): pass
            def _execute_follows(self, candidates, record): pass
            class db:
                @staticmethod
                def recent_runs(limit=50):
                    return [_row(status="failed") for _ in range(3)]

        fake_af = FakeAF()
        mid.attach(fake_af)
        mid.run_pre_batch_checks(fake_af)

        assert mid._paused is True
        assert mid._pause_reason != ""

    def test_resume_clears_pause(self):
        from gh_autofollow.security import SecurityMiddleware
        mid = SecurityMiddleware()
        mid._paused = True
        mid._pause_reason = "test reason"
        mid.resume()
        assert mid._paused is False
        assert mid._pause_reason == ""

    def test_status_output(self):
        from gh_autofollow.security import SecurityMiddleware
        mid = SecurityMiddleware()
        status = mid.status()
        assert "paused" in status
        assert "velocity" in status

    def test_no_double_attach(self):
        """Attaching twice should not cause errors."""
        from gh_autofollow.security import SecurityMiddleware, VelocityGuard
        guard = VelocityGuard(per_minute=100, per_hour=100, per_day=1000)
        mid = SecurityMiddleware(guard=guard)

        class FakeAF:
            def _follow_one(self, candidate, record): pass
            def _execute_follows(self, candidates, record): pass

        fake_af = FakeAF()
        mid.attach(fake_af)
        mid.attach(fake_af)  # second attach — should not raise


# ═══════════════════════════════════════════════════════════════════════════════
# Integration: Config security fields
# ═══════════════════════════════════════════════════════════════════════════════

class TestSecurityConfig:

    def test_security_fields_defaults(self):
        from gh_autofollow.config import Config
        cfg = Config(github_token="tok")
        assert cfg.security_enabled is True
        assert cfg.velocity_per_minute == 3
        assert cfg.velocity_per_hour == 30
        assert cfg.velocity_per_day == 150
        assert cfg.anomaly_abort_on_critical is True
        assert cfg.health_check_interval == 3600
        assert cfg.token_vault_enabled is False

    def test_security_fields_from_env(self, monkeypatch):
        from gh_autofollow.config import Config
        monkeypatch.setenv("GH_AUTOFOLLOW_SECURITY_ENABLED", "false")
        monkeypatch.setenv("GH_AUTOFOLLOW_VELOCITY_PER_HOUR", "15")
        monkeypatch.setenv("GH_AUTOFOLLOW_VELOCITY_PER_DAY", "75")
        cfg = Config.from_env()
        assert cfg.security_enabled is False
        assert cfg.velocity_per_hour == 15
        assert cfg.velocity_per_day == 75

    def test_security_fields_in_to_dict(self):
        from gh_autofollow.config import Config
        cfg = Config(github_token="tok")
        d = cfg.to_dict()
        assert "security_enabled" in d
        assert "velocity_per_minute" in d
        assert "velocity_per_hour" in d
        assert "velocity_per_day" in d
