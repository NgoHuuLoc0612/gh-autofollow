"""
pytest configuration — makes gh_autofollow importable from the project root.
"""
import sys
from pathlib import Path

# Ensure the package is importable when running pytest from the project root
sys.path.insert(0, str(Path(__file__).parent.parent))
