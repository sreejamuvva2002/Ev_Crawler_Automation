"""Local Python startup customization for workspace-only dependencies.

This lets the repo use packages installed into `.vendor/` without requiring a
system-wide install. Python imports `sitecustomize` automatically when present
on the import path.
"""

from __future__ import annotations

import sys
from pathlib import Path


VENDOR_DIR = Path(__file__).resolve().parent / ".vendor"
if VENDOR_DIR.exists():
    vendor_path = str(VENDOR_DIR)
    if vendor_path not in sys.path:
        sys.path.insert(0, vendor_path)
