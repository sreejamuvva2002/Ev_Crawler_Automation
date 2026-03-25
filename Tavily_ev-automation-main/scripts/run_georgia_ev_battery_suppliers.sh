#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

QUERY="Georgia EV battery manufacturing suppliers"
OUT_DIR="outputs/crawler/georgia_ev_battery_suppliers"
OUT="$OUT_DIR/georgia_ev_battery_manufacturing_suppliers.xlsx"
DIR="$OUT_DIR/downloads"

mkdir -p "$OUT_DIR" "$DIR"

python -m tavily_ev_automation.tavily_crawler "$QUERY" -n 20 --search-depth basic -d "$DIR" --download-mode all -o "$OUT"
printf "\nDone: %s\n" "$OUT"
echo "Downloads in: $DIR"
