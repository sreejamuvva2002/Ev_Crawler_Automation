"""Legacy entrypoint for the non-canonical ComparisonRunner workflow.

For thesis/research runs, use eval_runner.py.
"""

from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ev_llm_compare.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
