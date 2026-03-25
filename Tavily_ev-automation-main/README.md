# Tavily EV Automation

Tavily-based collection and GNEM phase-1 filtering for Georgia and Southeast EV battery supply-chain research.

## Repository layout
```text
.
|-- data/
|   |-- corpus/
|   |   |-- documents/
|   |   `-- text/
|   |-- grounding/
|   `-- queries/
|-- docs/
|-- examples/
|-- scripts/
|-- requirements.txt
`-- tavily_ev_automation/
    |-- __init__.py
    |-- generate_gnem_queries.py
    |-- gnem_pipeline.py
    |-- gnem_rag_helpers.py
    `-- tavily_crawler.py
```

Notes:
- `data/grounding/GA_Automotive Landscape_All_Companies (1).xlsx` is expected locally but is not committed.
- Live run artifacts now default into `outputs/`.
- Sample review workbooks that were already in the repo live under `examples/pipeline_runs/`.

## Setup
PowerShell:
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

macOS/Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set `TAVILY_API_KEY` in `.env` before running Tavily-backed commands.

## Quick start
Single crawler run:
```bash
python -m tavily_ev_automation.tavily_crawler "Georgia EV battery manufacturing suppliers" \
  -n 20 \
  --search-depth basic \
  -d outputs/crawler/georgia_ev_battery_suppliers/downloads \
  --download-mode all \
  -o outputs/crawler/georgia_ev_battery_suppliers/georgia_ev_battery_manufacturing_suppliers.xlsx
```

Shell shortcut:
```bash
bash scripts/run_georgia_ev_battery_suppliers.sh
```

## GNEM query generation
```bash
python -m tavily_ev_automation.generate_gnem_queries \
  --output data/queries/queries_1000.txt \
  --grounding-xlsx "data/grounding/GA_Automotive Landscape_All_Companies (1).xlsx" \
  --grounding-counties-geojson data/grounding/Counties_Georgia.geojson \
  --grounding-docx "data/grounding/GNEM Supply Chain.docx"
```

The generator and pipeline both keep backward-compatible fallbacks for the older root-level asset locations.

## GNEM phase-1 filtering pipeline
The pipeline in `tavily_ev_automation/gnem_pipeline.py` performs:
1. Tavily retrieval or metadata ingest.
2. Metadata scoring and gating.
3. Lightweight document-card generation.
4. Heuristic, embedding, and hybrid scoring.
5. Classifier reranking.
6. Shortlist enrichment.
7. Optional LLM judging.
8. Credibility and diversity review export.

Run from Tavily:
```bash
python -m tavily_ev_automation.gnem_pipeline \
  --queries-file data/queries/queries_1000.txt \
  --max-queries 1000 \
  --max-results 20 \
  --query-mode hybrid \
  --search-depth basic \
  --metadata-threshold 25 \
  --metadata-target-ratio 0.5 \
  --heuristic-threshold 45 \
  --hybrid-threshold 65 \
  --direct-usecase-threshold 0.70 \
  --credibility-threshold 60
```

Run in 250-query batches by combining `--query-offset` with `--max-queries 250`:
```bash
python -m tavily_ev_automation.gnem_pipeline \
  --queries-file data/queries/queries_1000.txt \
  --query-offset 0 \
  --max-queries 250 \
  --max-results 20 \
  --query-mode web_only \
  --search-depth basic \
  --output-dir outputs/pipeline_runs/batch_000_249
```

Subsequent batches use:
- `--query-offset 250` for queries `251-500`
- `--query-offset 500` for queries `501-750`
- `--query-offset 750` for queries `751-1000`

Offline validation with local corpus:
```bash
python -m tavily_ev_automation.gnem_pipeline \
  --input-metadata-xlsx runs/gnem_corpus_registry.xlsx \
  --local-pdf-dir data/corpus/documents \
  --local-text-dir data/corpus/text \
  --sample-size 50 \
  --metadata-target-ratio 1.0 \
  --disable-metadata-rule-gate \
  --metadata-threshold 20 \
  --heuristic-threshold 45 \
  --hybrid-threshold 65 \
  --direct-usecase-threshold 0.70 \
  --credibility-threshold 60
```

Strict relevance mode with an LLM judge:
```bash
python -m tavily_ev_automation.gnem_pipeline \
  --queries-file data/queries/queries_1000.txt \
  --max-queries 1000 \
  --max-results 20 \
  --search-depth advanced \
  --metadata-threshold 25 \
  --metadata-target-ratio 0.5 \
  --heuristic-threshold 45 \
  --hybrid-threshold 65 \
  --direct-usecase-threshold 0.70 \
  --credibility-threshold 60 \
  --llm-provider ollama \
  --llm-model qwen2.5:7b \
  --llm-base-url http://localhost:11434 \
  --llm-timeout 60 \
  --llm-max-text-chars 7000
```

If you want OpenAI instead of Ollama, use:
- `--llm-provider openai`
- `--llm-base-url https://api.openai.com`
- `--llm-api-key <YOUR_KEY>`

Kimi works through the same OpenAI-compatible path. For a Kimi judge run, use Moonshot's OpenAI-compatible endpoint and a Kimi model such as `kimi-k2.5`.

By default, pipeline stage outputs are written to `outputs/pipeline_runs/<timestamp>/`.
The pipeline now treats retrieval mode explicitly via `--query-mode {pdf_only,web_only,hybrid}` and defaults to `hybrid`.
SQLite export is optional; add `--write-sqlite-registry` if you want a local registry DB alongside the Excel/CSV outputs.
