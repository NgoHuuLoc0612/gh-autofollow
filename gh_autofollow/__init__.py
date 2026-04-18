"""
gh-autofollow: A production-grade GitHub auto-follow library.

Supports batch following, caching (SQLite), rate-limit awareness,
multiple discovery strategies, and OS-level autostart.
"""

from gh_autofollow.core import AutoFollower
from gh_autofollow.config import Config
from gh_autofollow.__version__ import __version__
from gh_autofollow.security import (
    TokenVault,
    VelocityGuard,
    AnomalyDetector,
    AccountHealthMonitor,
    SecurityMiddleware,
)

__all__ = [
    "AutoFollower",
    "Config",
    "__version__",
    "TokenVault",
    "VelocityGuard",
    "AnomalyDetector",
    "AccountHealthMonitor",
    "SecurityMiddleware",
]
