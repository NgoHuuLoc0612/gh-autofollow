"""
Scheduler for gh-autofollow.

Provides:
  - A blocking event-loop scheduler (runs in-process)
  - OS-level autostart installation:
      Linux  → systemd user service  OR  ~/.config/autostart/
      macOS  → launchd plist
      Windows → Task Scheduler via schtasks.exe
"""

from __future__ import annotations

import atexit
import logging
import os
import platform
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Callable, Optional

from gh_autofollow.config import Config

logger = logging.getLogger(__name__)


# ── Scheduler ────────────────────────────────────────────────────────────────

class Scheduler:
    """
    Runs AutoFollower batch jobs on a configurable interval.

    Example::

        with AutoFollower(config) as af:
            sched = Scheduler(config, batch_fn=af.run_batch)
            sched.run_forever()
    """

    def __init__(
        self,
        config: Config,
        batch_fn: Callable,
        on_tick: Optional[Callable[[int], None]] = None,
    ) -> None:
        self.config = config
        self._batch_fn = batch_fn
        self._on_tick = on_tick
        self._running = False
        self._tick = 0
        self._pid_file = Path(config.scheduler_pid_file)

    # ── PID management ────────────────────────────────────────────────────────

    def _write_pid(self) -> None:
        self._pid_file.parent.mkdir(parents=True, exist_ok=True)
        self._pid_file.write_text(str(os.getpid()))
        atexit.register(self._remove_pid)

    def _remove_pid(self) -> None:
        try:
            self._pid_file.unlink(missing_ok=True)
        except Exception:
            pass

    @classmethod
    def is_running(cls, pid_file: Path) -> bool:
        """Check if a scheduler process is alive using the PID file."""
        if not pid_file.exists():
            return False
        try:
            pid = int(pid_file.read_text().strip())
        except (ValueError, OSError):
            return False

        try:
            os.kill(pid, 0)  # no-op; raises if process is gone
            return True
        except (ProcessLookupError, PermissionError):
            return False

    # ── Signal handling ───────────────────────────────────────────────────────

    def _setup_signals(self) -> None:
        def _graceful_shutdown(signum, frame):
            logger.info("Received signal %d; shutting down scheduler", signum)
            self._running = False

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _graceful_shutdown)
            except (OSError, ValueError):
                pass  # not on main thread

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run_forever(self) -> None:
        """Block and run batch jobs indefinitely until stopped."""
        self._write_pid()
        self._setup_signals()
        self._running = True
        interval = self.config.batch_interval

        logger.info(
            "Scheduler started (interval=%ds, batch_size=%d, dry_run=%s)",
            interval, self.config.batch_size, self.config.dry_run,
        )

        # Run immediately on start
        self._run_tick()

        while self._running:
            sleep_remaining = interval
            while sleep_remaining > 0 and self._running:
                time.sleep(min(1, sleep_remaining))
                sleep_remaining -= 1

            if self._running:
                self._run_tick()

        logger.info("Scheduler stopped after %d ticks", self._tick)

    def _run_tick(self) -> None:
        self._tick += 1
        logger.info("--- Scheduler tick #%d ---", self._tick)
        if self._on_tick:
            try:
                self._on_tick(self._tick)
            except Exception as exc:
                logger.debug("on_tick callback error: %s", exc)
        try:
            self._batch_fn()
        except Exception as exc:
            logger.error("Batch function raised: %s", exc, exc_info=True)

    def stop(self) -> None:
        self._running = False


# ── Autostart installation ────────────────────────────────────────────────────

class AutostartManager:
    """
    Installs / removes OS-level autostart entries so the scheduler
    launches automatically on user login.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self._system = platform.system()

    @property
    def _executable(self) -> str:
        return sys.executable

    @property
    def _script(self) -> str:
        """Path to the gh-autofollow CLI entry point or __main__ module."""
        # If installed as a script
        for candidate in [
            Path(sys.executable).parent / "gh-autofollow",
            Path(sys.executable).parent / "gh-autofollow.exe",
        ]:
            if candidate.exists():
                return str(candidate)
        # Fallback: python -m gh_autofollow
        return f"{self._executable} -m gh_autofollow"

    # ── Linux ─────────────────────────────────────────────────────────────────

    def _systemd_service_path(self) -> Path:
        return (
            Path.home()
            / ".config"
            / "systemd"
            / "user"
            / "gh-autofollow.service"
        )

    def _systemd_unit(self) -> str:
        token = self.config.github_token
        return textwrap.dedent(f"""\
            [Unit]
            Description=gh-autofollow GitHub auto-follow daemon
            After=network-online.target
            Wants=network-online.target

            [Service]
            Type=simple
            ExecStart={self._script} scheduler --daemon
            Environment=GH_AUTOFOLLOW_GITHUB_TOKEN={token}
            Restart=on-failure
            RestartSec=60
            StandardOutput=journal
            StandardError=journal

            [Install]
            WantedBy=default.target
        """)

    def _xdg_autostart_path(self) -> Path:
        return (
            Path.home()
            / ".config"
            / "autostart"
            / "gh-autofollow.desktop"
        )

    def _xdg_desktop_entry(self) -> str:
        return textwrap.dedent(f"""\
            [Desktop Entry]
            Type=Application
            Name=gh-autofollow
            Comment=GitHub auto-follow daemon
            Exec={self._script} scheduler --daemon
            Hidden=false
            NoDisplay=false
            X-GNOME-Autostart-enabled=true
        """)

    def _install_linux(self) -> str:
        # Prefer systemd if available
        systemd_dir = Path.home() / ".config" / "systemd" / "user"
        if self._has_systemd():
            systemd_dir.mkdir(parents=True, exist_ok=True)
            service_path = self._systemd_service_path()
            service_path.write_text(self._systemd_unit())

            subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
            subprocess.run(
                ["systemctl", "--user", "enable", "--now", "gh-autofollow.service"],
                check=False,
            )
            return str(service_path)
        else:
            # XDG autostart fallback
            autostart_dir = Path.home() / ".config" / "autostart"
            autostart_dir.mkdir(parents=True, exist_ok=True)
            desktop_path = self._xdg_autostart_path()
            desktop_path.write_text(self._xdg_desktop_entry())
            return str(desktop_path)

    def _remove_linux(self) -> None:
        if self._has_systemd():
            subprocess.run(
                ["systemctl", "--user", "disable", "--now", "gh-autofollow.service"],
                check=False,
            )
            self._systemd_service_path().unlink(missing_ok=True)
            subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
        else:
            self._xdg_autostart_path().unlink(missing_ok=True)

    @staticmethod
    def _has_systemd() -> bool:
        return Path("/run/systemd/system").exists()

    # ── macOS ─────────────────────────────────────────────────────────────────

    def _launchd_plist_path(self) -> Path:
        return (
            Path.home()
            / "Library"
            / "LaunchAgents"
            / "com.gh-autofollow.plist"
        )

    def _launchd_plist(self) -> str:
        token = self.config.github_token
        script = self._script
        return textwrap.dedent(f"""\
            <?xml version="1.0" encoding="UTF-8"?>
            <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
              "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
            <plist version="1.0">
            <dict>
                <key>Label</key>
                <string>com.gh-autofollow</string>
                <key>ProgramArguments</key>
                <array>
                    <string>/bin/sh</string>
                    <string>-c</string>
                    <string>{script} scheduler --daemon</string>
                </array>
                <key>EnvironmentVariables</key>
                <dict>
                    <key>GH_AUTOFOLLOW_GITHUB_TOKEN</key>
                    <string>{token}</string>
                </dict>
                <key>RunAtLoad</key>
                <true/>
                <key>KeepAlive</key>
                <dict>
                    <key>SuccessfulExit</key>
                    <false/>
                </dict>
                <key>StandardOutPath</key>
                <string>{Path.home()}/Library/Logs/gh-autofollow.log</string>
                <key>StandardErrorPath</key>
                <string>{Path.home()}/Library/Logs/gh-autofollow-error.log</string>
            </dict>
            </plist>
        """)

    def _install_macos(self) -> str:
        plist_path = self._launchd_plist_path()
        plist_path.parent.mkdir(parents=True, exist_ok=True)
        plist_path.write_text(self._launchd_plist())
        subprocess.run(
            ["launchctl", "load", "-w", str(plist_path)],
            check=False,
        )
        return str(plist_path)

    def _remove_macos(self) -> None:
        plist_path = self._launchd_plist_path()
        if plist_path.exists():
            subprocess.run(
                ["launchctl", "unload", "-w", str(plist_path)],
                check=False,
            )
            plist_path.unlink()

    # ── Windows ───────────────────────────────────────────────────────────────

    _TASK_NAME = "gh-autofollow"

    def _install_windows(self) -> str:
        token = self.config.github_token
        cmd = (
            f'schtasks /Create /TN "{self._TASK_NAME}" /TR '
            f'"{self._script} scheduler --daemon" '
            f'/SC ONLOGON /RL HIGHEST /F'
        )
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"schtasks failed: {result.stderr}")
        # Store token in Windows Credential Manager or env is not ideal;
        # user should set GITHUB_TOKEN via System Properties → Environment Variables
        return self._TASK_NAME

    def _remove_windows(self) -> None:
        subprocess.run(
            f'schtasks /Delete /TN "{self._TASK_NAME}" /F',
            shell=True,
            check=False,
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def install(self) -> str:
        """
        Install OS autostart entry.
        Returns a description of what was installed.
        """
        if self._system == "Linux":
            path = self._install_linux()
        elif self._system == "Darwin":
            path = self._install_macos()
        elif self._system == "Windows":
            path = self._install_windows()
        else:
            raise NotImplementedError(f"Autostart not supported on {self._system}")

        logger.info("Autostart installed: %s", path)
        return path

    def remove(self) -> None:
        """Remove OS autostart entry."""
        if self._system == "Linux":
            self._remove_linux()
        elif self._system == "Darwin":
            self._remove_macos()
        elif self._system == "Windows":
            self._remove_windows()
        else:
            raise NotImplementedError(f"Autostart not supported on {self._system}")
        logger.info("Autostart removed")

    def status(self) -> str:
        """Return a human-readable status string."""
        if self._system == "Linux":
            if self._has_systemd():
                result = subprocess.run(
                    ["systemctl", "--user", "is-active", "gh-autofollow.service"],
                    capture_output=True, text=True,
                )
                return f"systemd: {result.stdout.strip()}"
            return "xdg: " + (
                "installed" if self._xdg_autostart_path().exists() else "not installed"
            )
        elif self._system == "Darwin":
            return "launchd: " + (
                "installed" if self._launchd_plist_path().exists() else "not installed"
            )
        elif self._system == "Windows":
            result = subprocess.run(
                f'schtasks /Query /TN "{self._TASK_NAME}"',
                shell=True, capture_output=True, text=True,
            )
            return "task: " + ("installed" if result.returncode == 0 else "not installed")
        return "unknown"
