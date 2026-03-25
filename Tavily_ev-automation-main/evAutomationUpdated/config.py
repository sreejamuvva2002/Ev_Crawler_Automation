from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ev_llm_compare.settings import AppConfig, load_config


__all__ = ["AppConfig", "load_config"]
