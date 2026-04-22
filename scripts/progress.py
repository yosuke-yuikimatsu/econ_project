from __future__ import annotations

import sys
from pathlib import Path

# Robust import path when script is executed as `python scripts/progress.py`
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.storage.state import StateStore

if __name__ == "__main__":
    print(StateStore().summary())
