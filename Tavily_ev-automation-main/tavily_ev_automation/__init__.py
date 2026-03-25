"""Core package for Tavily EV automation utilities."""

from __future__ import annotations

import sys
from pathlib import Path


_ROOT = Path(__file__).resolve().parent.parent
_VENDOR = _ROOT / ".vendor"
if _VENDOR.exists():
    vendor_path = str(_VENDOR)
    if vendor_path not in sys.path:
        sys.path.insert(0, vendor_path)
