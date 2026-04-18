"""
Command-line interface for gh-autofollow.

Subcommands:
  run           Run a single batch now
  scheduler     Start the background scheduler loop
  discover      Only run discovery (no follows)
  stats         Show statistics
  history       Show recent run history
  autostart     Manage OS-level autostart
  config        Print / validate / save config
  db            Database maintenance (vacuum, prune, etc.)
  blocklist     Manage the user blocklist
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import textwrap
from pathlib import Path
from typing import List, Optional

# ── Lazy imports so CLI is fast even when only checking --help ────────────────

def _get_config(args) -> "Config":
    from gh_autofollow.config import Config
    cfg = Config.load(config_file=getattr(args, "config", None))
    if getattr(args, "token", None):
        cfg.github_token = args.token
    if getattr(args, "batch_size", None):
        cfg.batch_size = args.batch_size
    if getattr(args, "dry_run", False):
        cfg.dry_run = True
    if getattr(args, "verbose", False):
        cfg.verbose = True
        cfg.log_level = "DEBUG"
    return cfg


# ── Sub-command handlers ──────────────────────────────────────────────────────

def cmd_run(args) -> int:
    from gh_autofollow.config import Config
    from gh_autofollow.core import AutoFollower
    from gh_autofollow.logging_setup import setup_logging

    cfg = _get_config(args)
    cfg.ensure_dirs()
    cfg.validate()
    setup_logging(cfg)

    with AutoFollower(cfg) as af:
        record = af.run_batch()

    print(
        f"Batch complete: status={record.status} "
        f"followed={record.followed_count} "
        f"skipped={record.skipped_count} "
        f"errors={record.error_count}"
    )
    return 0 if record.status in ("completed", "skipped", "no_candidates") else 1


def cmd_scheduler(args) -> int:
    from gh_autofollow.config import Config
    from gh_autofollow.core import AutoFollower
    from gh_autofollow.logging_setup import setup_logging
    from gh_autofollow.scheduler.runner import Scheduler

    daemon = getattr(args, "daemon", False)
    cfg = _get_config(args)
    cfg.ensure_dirs()
    cfg.validate()
    setup_logging(cfg, daemon=daemon)

    import logging
    logger = logging.getLogger("gh_autofollow.cli")

    if daemon:
        # Double-fork on Unix to fully daemonize
        _daemonize(cfg)

    af = AutoFollower(cfg)
    af._open()

    try:
        sched = Scheduler(cfg, batch_fn=af.run_batch)
        logger.info("Starting scheduler (interval=%ds)", cfg.batch_interval)
        sched.run_forever()
    finally:
        af._close()

    return 0


def cmd_discover(args) -> int:
    from gh_autofollow.core import AutoFollower
    from gh_autofollow.logging_setup import setup_logging

    cfg = _get_config(args)
    cfg.ensure_dirs()
    cfg.validate()
    setup_logging(cfg)

    with AutoFollower(cfg) as af:
        added = af.discover_candidates()

    print(f"Discovery complete: {added} new candidates added to cache")
    return 0


def cmd_stats(args) -> int:
    from gh_autofollow.core import AutoFollower
    from gh_autofollow.logging_setup import setup_logging

    cfg = _get_config(args)
    cfg.ensure_dirs()
    cfg.validate()
    setup_logging(cfg, daemon=True)

    with AutoFollower(cfg) as af:
        stats = af.get_stats()

    if getattr(args, "json", False):
        print(json.dumps(stats, indent=2))
        return 0

    db = stats["db"]
    rl = stats["rate_limit"]
    print("\n=== gh-autofollow stats ===")
    print(f"  Total followed  : {db['total_followed']}")
    print(f"  Candidates      : {db['candidates_pending']} pending / {db['candidates_total']} total")
    print(f"  Blocked users   : {db['blocked_users']}")
    print(f"  Total runs      : {db['total_runs']}")
    print(f"\n  Rate limit      : {rl['core_remaining']}/{rl['core_limit']} core remaining")
    print(f"  Resets in       : {rl['core_reset_in']:.0f}s")
    print(f"  Search remaining: {rl['search_remaining']}")

    print("\n  Strategy breakdown:")
    for strategy, count in stats.get("strategy_breakdown", {}).items():
        print(f"    {strategy:<30} {count}")
    return 0


def cmd_history(args) -> int:
    from gh_autofollow.db.database import Database
    from gh_autofollow.logging_setup import setup_logging

    cfg = _get_config(args)
    cfg.ensure_dirs()
    setup_logging(cfg, daemon=True)

    import time
    db = Database(cfg.db_path)
    runs = db.recent_runs(limit=getattr(args, "limit", 20))
    db.close()

    if getattr(args, "json", False):
        rows = [dict(r) for r in runs]
        print(json.dumps(rows, indent=2, default=str))
        return 0

    print(f"\n{'ID':<36}  {'Started':<19}  {'Status':<14}  {'Followed':>8}  {'Skipped':>7}  {'Errors':>6}")
    print("-" * 100)
    for r in runs:
        started = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["started_at"]))
        print(
            f"{r['id']:<36}  {started}  {r['status']:<14}  "
            f"{r['followed_count']:>8}  {r['skipped_count']:>7}  {r['error_count']:>6}"
        )
    return 0


def cmd_autostart(args) -> int:
    from gh_autofollow.logging_setup import setup_logging
    from gh_autofollow.scheduler.runner import AutostartManager

    cfg = _get_config(args)
    cfg.ensure_dirs()
    setup_logging(cfg, daemon=True)

    mgr = AutostartManager(cfg)
    action = args.action

    if action == "install":
        path = mgr.install()
        print(f"Autostart installed: {path}")
    elif action == "remove":
        mgr.remove()
        print("Autostart removed")
    elif action == "status":
        status = mgr.status()
        print(f"Autostart status: {status}")
    else:
        print(f"Unknown action: {action}", file=sys.stderr)
        return 1
    return 0


def cmd_config(args) -> int:
    from gh_autofollow.logging_setup import setup_logging

    cfg = _get_config(args)
    action = getattr(args, "action", "show")

    if action == "validate":
        cfg.ensure_dirs()
        setup_logging(cfg, daemon=True)
        try:
            cfg.validate()
            print("Configuration is valid.")
        except ValueError as exc:
            print(f"Configuration errors:\n{exc}", file=sys.stderr)
            return 1

    elif action == "save":
        cfg.ensure_dirs()
        path = cfg.save()
        print(f"Configuration saved to {path}")

    else:  # show
        data = cfg.to_dict()
        data["github_token"] = ("****" if cfg.github_token else "<not set>")
        if getattr(args, "json", False):
            print(json.dumps(data, indent=2))
        else:
            for key, val in data.items():
                print(f"  {key:<28} = {val}")

    return 0


def cmd_db(args) -> int:
    from gh_autofollow.db.database import Database
    from gh_autofollow.logging_setup import setup_logging

    cfg = _get_config(args)
    cfg.ensure_dirs()
    setup_logging(cfg, daemon=True)

    db = Database(cfg.db_path)
    action = args.action

    if action == "vacuum":
        db.vacuum()
        print("VACUUM complete")
    elif action == "prune":
        days = getattr(args, "days", 7)
        removed = db.prune_candidates(max_age_days=days)
        print(f"Pruned {removed} stale candidates")
    elif action == "check":
        ok = db.integrity_check()
        print(f"Integrity check: {'OK' if ok else 'FAILED'}")
        return 0 if ok else 1
    elif action == "summary":
        summary = db.get_summary()
        for k, v in summary.items():
            print(f"  {k:<30} {v}")
    else:
        print(f"Unknown db action: {action}", file=sys.stderr)
        return 1

    db.close()
    return 0


def cmd_blocklist(args) -> int:
    from gh_autofollow.db.database import Database
    from gh_autofollow.logging_setup import setup_logging

    cfg = _get_config(args)
    cfg.ensure_dirs()
    setup_logging(cfg, daemon=True)

    db = Database(cfg.db_path)
    action = args.action

    if action == "add":
        for login in args.logins:
            db.block_user(login.strip(), reason=getattr(args, "reason", "manual"))
            print(f"Blocked: {login}")
    elif action == "list":
        logins = db.get_blocked_logins()
        if logins:
            for l in logins:
                print(l)
        else:
            print("(blocklist is empty)")
    else:
        print(f"Unknown blocklist action: {action}", file=sys.stderr)
        return 1

    db.close()
    return 0


# ── Daemonize helper (Unix only) ──────────────────────────────────────────────


def cmd_security(args) -> int:
    from gh_autofollow.db.database import Database
    from gh_autofollow.logging_setup import setup_logging
    from gh_autofollow.security import (
        AccountHealthMonitor, AnomalyDetector, TokenVault, VelocityGuard
    )

    cfg = _get_config(args)
    cfg.ensure_dirs()
    setup_logging(cfg, daemon=True)
    action = args.action

    if action == "health":
        from gh_autofollow.api.client import GitHubClient
        client = GitHubClient(token=cfg.github_token, base_url=cfg.api_base_url, timeout=cfg.api_timeout)
        monitor = AccountHealthMonitor(client)
        report = monitor.check()
        client.close()
        if getattr(args, "json_out", False):
            print(json.dumps({"healthy": report.healthy, "token_valid": report.token_valid,
                "follow_scope": report.follow_scope, "account_suspended": report.account_suspended,
                "following_count": report.following_count,
                "rate_limit_remaining": report.rate_limit_remaining, "alerts": report.alerts}, indent=2))
            return 0 if report.healthy else 1
        status = "HEALTHY" if report.healthy else "UNHEALTHY"
        print(f"\n=== Account Health: {status} ===")
        print(f"  Token valid       : {'Yes' if report.token_valid else 'NO'}")
        print(f"  follow scope      : {'Yes' if report.follow_scope else 'NO — token needs user:follow'}")
        print(f"  Account suspended : {'YES' if report.account_suspended else 'No'}")
        print(f"  Following count   : {report.following_count} / 5000")
        print(f"  Rate limit left   : {report.rate_limit_remaining}")
        if report.alerts:
            print("\n  Alerts:")
            for a in report.alerts:
                print(f"    [!] {a}")
        return 0 if report.healthy else 1

    elif action == "anomalies":
        db = Database(cfg.db_path)
        recent_runs = db.recent_runs(limit=50)
        db.close()
        detector = AnomalyDetector()
        alerts = detector.analyse(recent_runs)
        if not alerts:
            print("No anomalies detected.")
            return 0
        for a in alerts:
            prefix = "CRITICAL" if a.level == "critical" else "WARNING "
            print(f"[{prefix}] {a.code}: {a.message}")
        return 1 if any(a.level == "critical" for a in alerts) else 0

    elif action == "velocity":
        guard = VelocityGuard(per_minute=cfg.velocity_per_minute,
                              per_hour=cfg.velocity_per_hour, per_day=cfg.velocity_per_day,
                              db_path=str(cfg.db_path))
        rates = guard.current_rates()
        print("\n=== Follow Velocity ===")
        for window, info in rates.items():
            bar_len = int(info["pct"] / 5)
            bar = "#" * bar_len + "-" * (20 - bar_len)
            print(f"  {window:<14} [{bar}] {info['current']:>4}/{info['limit']:<4} ({info['pct']}%)")
        return 0

    elif action == "token-store":
        token = cfg.github_token
        if not token:
            print("Error: no token set", file=sys.stderr)
            return 1
        vault = TokenVault(data_dir=cfg.data_dir)
        backend = vault.store(token)
        print(f"Token stored via: {backend}")
        return 0

    elif action == "token-status":
        vault = TokenVault(data_dir=cfg.data_dir)
        token = vault.retrieve()
        print(f"Backend  : {vault.backend}")
        print(f"Token set: {'Yes (' + token[:4] + '****)' if token else 'No'}")
        return 0

    elif action == "token-delete":
        vault = TokenVault(data_dir=cfg.data_dir)
        vault.delete()
        print("Token deleted.")
        return 0

    else:
        print(f"Unknown security action: {action}", file=sys.stderr)
        return 1


def _daemonize(cfg) -> None:
    """Double-fork to create a true Unix daemon."""
    import resource

    if os.fork() > 0:
        sys.exit(0)

    os.setsid()

    if os.fork() > 0:
        sys.exit(0)

    os.chdir("/")
    os.umask(0)

    # Close all open file descriptors except stdout/stderr
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    for fd in range(3, maxfd if maxfd != resource.RLIM_INFINITY else 1024):
        try:
            os.close(fd)
        except OSError:
            pass

    # Redirect stdin to /dev/null
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 0)


# ── Argument parser ───────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gh-autofollow",
        description="GitHub auto-follow automation — batch follow with caching",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
            Environment variables:
              GH_AUTOFOLLOW_GITHUB_TOKEN   GitHub personal access token
              GITHUB_TOKEN                 Fallback token variable
              GH_AUTOFOLLOW_BATCH_SIZE     Users to follow per batch
              GH_AUTOFOLLOW_DRY_RUN        Set to 'true' to simulate

            Config file locations (auto-detected):
              ~/.config/gh-autofollow/config.toml
              ~/.config/gh-autofollow/config.json
              ./gh-autofollow.toml
        """),
    )

    # Global flags
    parser.add_argument("--config", "-c", metavar="FILE", help="Path to config file")
    parser.add_argument("--token", "-t", metavar="TOKEN", help="GitHub personal access token")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without following")
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # ── run ──────────────────────────────────────────────────────────────────
    p_run = sub.add_parser("run", help="Execute a single follow batch now")
    p_run.add_argument("--batch-size", type=int, metavar="N", help="Override batch size")
    p_run.set_defaults(func=cmd_run)

    # ── scheduler ─────────────────────────────────────────────────────────────
    p_sched = sub.add_parser("scheduler", help="Start the background scheduler")
    p_sched.add_argument("--daemon", "-d", action="store_true", help="Daemonize (Unix only)")
    p_sched.set_defaults(func=cmd_scheduler)

    # ── discover ──────────────────────────────────────────────────────────────
    p_disc = sub.add_parser("discover", help="Run discovery without following")
    p_disc.set_defaults(func=cmd_discover)

    # ── stats ─────────────────────────────────────────────────────────────────
    p_stats = sub.add_parser("stats", help="Show follow statistics")
    p_stats.add_argument("--json", action="store_true", help="Output as JSON")
    p_stats.set_defaults(func=cmd_stats)

    # ── history ───────────────────────────────────────────────────────────────
    p_hist = sub.add_parser("history", help="Show recent batch run history")
    p_hist.add_argument("--limit", type=int, default=20, metavar="N")
    p_hist.add_argument("--json", action="store_true")
    p_hist.set_defaults(func=cmd_history)

    # ── autostart ─────────────────────────────────────────────────────────────
    p_auto = sub.add_parser("autostart", help="Manage OS autostart entry")
    p_auto.add_argument("action", choices=["install", "remove", "status"])
    p_auto.set_defaults(func=cmd_autostart)

    # ── config ────────────────────────────────────────────────────────────────
    p_conf = sub.add_parser("config", help="Show / validate / save configuration")
    p_conf.add_argument("action", nargs="?", choices=["show", "validate", "save"], default="show")
    p_conf.add_argument("--json", action="store_true")
    p_conf.set_defaults(func=cmd_config)

    # ── db ────────────────────────────────────────────────────────────────────
    p_db = sub.add_parser("db", help="Database maintenance")
    p_db.add_argument("action", choices=["vacuum", "prune", "check", "summary"])
    p_db.add_argument("--days", type=int, default=7, help="Age threshold for prune (days)")
    p_db.set_defaults(func=cmd_db)

    # ── blocklist ─────────────────────────────────────────────────────────────
    p_bl = sub.add_parser("blocklist", help="Manage the follow blocklist")
    p_bl.add_argument("action", choices=["add", "list"])
    p_bl.add_argument("logins", nargs="*", metavar="LOGIN")
    p_bl.add_argument("--reason", default="manual", help="Block reason annotation")
    p_bl.set_defaults(func=cmd_blocklist)

    # -- security -----------------------------------------------------------------
    p_sec = sub.add_parser("security", help="Security checks and token management")
    p_sec.add_argument("action", choices=["health","anomalies","velocity","token-store","token-status","token-delete"])
    p_sec.add_argument("--json", dest="json_out", action="store_true", help="Output as JSON")
    p_sec.set_defaults(func=cmd_security)

    return parser


# ── Entry point ───────────────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        return args.func(args) or 0
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130
    except SystemExit as exc:
        return int(exc.code) if exc.code is not None else 0
    except Exception as exc:
        print(f"Fatal error: {exc}", file=sys.stderr)
        if getattr(args, "verbose", False):
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
