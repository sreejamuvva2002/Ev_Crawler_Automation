# Tavily EV Automation

Tavily-based collection and GNEM phase-1 filtering for Georgia and Southeast EV battery supply-chain research, plus a separate research evaluation pipeline for benchmarking EV questions across `no_rag`, `local_rag`, and `hybrid_rag` conditions.

## What this repo contains

This repository currently has two related but separate workflows:

1. `tavily_ev_automation/`
   - Tavily search and document crawling
   - GNEM query generation
   - GNEM phase-1 filtering and review exports
2. `evAutomationUpdated/`
   - Excel-driven EV supply-chain question answering
   - canonical research runner: `eval_runner.py`
   - one-model, one-mode evaluation runs
   - golden-answer scoring plus judge-based evidence metrics
   - reproducible manifests and workbook exports

Important:
- The crawler/filtering workflow and the LLM comparison workflow live in the same repository.
- They are not yet wired into a single end-to-end pipeline that automatically feeds crawler outputs into the comparison app.
- Use the crawler and GNEM tools when you want to collect and filter source material.
- Use `evAutomationUpdated/` when you want to benchmark LLM answers against EV workbook data.

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
|-- evAutomationUpdated/
|   |-- README.md
|   |-- eval_runner.py
|   |-- main.py
|   |-- src/ev_llm_compare/
|   |-- tests/
|   `-- artifacts/
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
- The LLM comparison app has its own README and Python package metadata under `evAutomationUpdated/`.

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

## Which entrypoint to use

- Crawl EV web sources:
  - `python -m tavily_ev_automation.tavily_crawler ...`
- Generate GNEM queries:
  - `python -m tavily_ev_automation.generate_gnem_queries ...`
- Run GNEM phase-1 filtering:
  - `python -m tavily_ev_automation.gnem_pipeline ...`
- Run LLM comparisons on the EV workbook:
  - `cd evAutomationUpdated`
  - `python eval_runner.py ...`

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

## EV LLM comparison app

The `evAutomationUpdated/` directory contains the newer benchmarking code for comparing multiple LLMs on EV supply-chain questions from Excel workbooks. It supports:

- canonical thesis/research runs through `eval_runner.py`
- model keys `qwen25_14b`, `gemma27b`, and `gemini25_flash`
- modes `no_rag`, `local_rag`, and `hybrid_rag`
- offline-only hybrid retrieval from local workbook data plus downloaded Tavily documents
- golden-answer scoring and judge-based evidence metrics
- per-run manifests, JSONL records, summary exports, and a hybrid-value comparison workbook

Setup for the comparison app:

```bash
cd evAutomationUpdated
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

Typical environment variables:

```bash
export GEMINI_API_KEY=your_key_here
export OLLAMA_BASE_URL=http://localhost:11434
export QWEN_MODEL=qwen2.5:14b
export GEMMA_MODEL=gemma3:27b
export EVALUATION_JUDGE_PROVIDER=ollama
export EVALUATION_JUDGE_MODEL=mistral-small3.2:24b
```

Canonical single-run invocation:

```bash
cd evAutomationUpdated
python eval_runner.py \
  --model qwen25_14b \
  --mode local_rag \
  --questions "data/GNEM_Golden_Questions.xlsx" \
  --data_workbook "data/GNEM updated excel.xlsx" \
  --golden_answers "artifacts/Golden_answers.xlsx"
```

Hybrid run with offline Tavily documents only:

```bash
cd evAutomationUpdated
python eval_runner.py \
  --model gemini25_flash \
  --mode hybrid_rag \
  --questions "data/GNEM_Golden_Questions.xlsx" \
  --data_workbook "data/GNEM updated excel.xlsx" \
  --tavily_dir "data/tavily ready documents" \
  --golden_answers "artifacts/Golden_answers.xlsx" \
  --study_id thesis_eval_round1
```

If you run both `local_rag` and `hybrid_rag` for the same `study_id`, the runner also writes `artifacts/results/<study_id>_hybrid_value.xlsx` so you can inspect whether offline web evidence changed the answers, added `WEB:` citations, or improved support/accuracy metrics.

For more detail on the comparison application, see:

- `evAutomationUpdated/README.md`
- `evAutomationUpdated/CODEBASE_OVERVIEW.md`
