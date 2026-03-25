#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

QUERIES_FILE="data/queries/queries_1000.txt"
RUNS_DIR="outputs/crawler/runs_1000"
DOWNLOAD_DIR="outputs/crawler/downloads_all_1000"
MASTER_OUT="$RUNS_DIR/master_1000_queries.xlsx"

MAX_RESULTS=20
DEPTH="basic"
MODE="all"

mkdir -p "$RUNS_DIR" "$RUNS_DIR/logs" "$DOWNLOAD_DIR"

i=1
while IFS= read -r q || [[ -n "$q" ]]; do
  [[ -z "${q// }" ]] && continue
  out_xlsx=$(printf "%s/q%03d.xlsx" "$RUNS_DIR" "$i")
  log_file=$(printf "%s/logs/q%03d.log" "$RUNS_DIR" "$i")
  echo "[$i] $q"
  python -m tavily_ev_automation.tavily_crawler "$q" \
    -n "$MAX_RESULTS" \
    --search-depth "$DEPTH" \
    -d "$DOWNLOAD_DIR" \
    --download-mode "$MODE" \
    -o "$out_xlsx" | tee "$log_file"
  i=$((i+1))
  sleep 1
done < "$QUERIES_FILE"

QUERIES_FILE_PY="$QUERIES_FILE" RUNS_DIR_PY="$RUNS_DIR" MASTER_OUT_PY="$MASTER_OUT" python - <<'PY'
from pathlib import Path
import os
import pandas as pd

runs_dir = Path(os.environ["RUNS_DIR_PY"])
queries = [l.strip() for l in Path(os.environ["QUERIES_FILE_PY"]).read_text(encoding="utf-8").splitlines() if l.strip()]
xlsx_files = sorted(runs_dir.glob("q*.xlsx"))

if not xlsx_files:
    raise SystemExit("No per-query workbooks were created.")

dfs = []
for idx, xf in enumerate(xlsx_files, start=1):
    df = pd.read_excel(xf)
    q = queries[idx-1] if idx-1 < len(queries) else ""
    df.insert(0, "Run_No", idx)
    df.insert(1, "Query", q)
    if "Document_ID" in df.columns:
        df["Document_ID"] = df["Document_ID"].astype(str).apply(lambda x: f"RUN{idx:03d}_{x}")
    dfs.append(df)

merged = pd.concat(dfs, ignore_index=True)
if "URL" in merged.columns:
    merged = merged.drop_duplicates(subset=["URL"], keep="first")

out = Path(os.environ["MASTER_OUT_PY"])
merged.to_excel(out, index=False)
print("Wrote:", out, "rows:", len(merged))
PY

echo "DONE."
echo "Master Excel: $MASTER_OUT"
echo "Downloads folder: $DOWNLOAD_DIR"
