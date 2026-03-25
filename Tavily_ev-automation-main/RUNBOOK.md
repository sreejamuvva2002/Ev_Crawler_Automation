# Runbook

This runbook reflects what the code currently supports. Where the repo relies on implicit behavior, that is called out explicitly.

## 1. Environment Setup

Root workflow:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Comparison workflow:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main/evAutomationUpdated
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
cp .env.example .env
```

Environment variables typically needed:

```bash
export TAVILY_API_KEY=replace_me
export OLLAMA_BASE_URL=http://localhost:11434
export QWEN_MODEL=qwen2.5:14b
export GEMMA_MODEL=gemma3:27b
export GEMINI_API_KEY=replace_me
export EVALUATION_JUDGE_PROVIDER=ollama
export EVALUATION_JUDGE_MODEL=mistral-small3.2:24b
export MODEL_TEMPERATURE=0.1
export MODEL_MAX_TOKENS=1600
```

Notes:

- Root `.env.example` currently contains a real-looking Tavily key and should be sanitized before sharing further.
- `data/grounding/GA_Automotive Landscape_All_Companies (1).xlsx` is required for some GNEM runs and was not present in the uploaded ZIP.

## 2. Generate GNEM Queries

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main
python -m tavily_ev_automation.generate_gnem_queries \
  --output data/queries/queries_1000.txt \
  --grounding-xlsx "data/grounding/GA_Automotive Landscape_All_Companies (1).xlsx" \
  --grounding-counties-geojson data/grounding/Counties_Georgia.geojson \
  --grounding-docx "data/grounding/GNEM Supply Chain.docx"
```

## 3. Run a Simple Crawl

One canned query:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main
python -m tavily_ev_automation.tavily_crawler \
  "Georgia EV battery manufacturing suppliers" \
  -n 20 \
  --search-depth basic \
  -d outputs/crawler/georgia_ev_battery_suppliers/downloads \
  --download-mode all \
  -o outputs/crawler/georgia_ev_battery_suppliers/georgia_ev_battery_manufacturing_suppliers.xlsx
```

Shell shortcut:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main
bash scripts/run_georgia_ev_battery_suppliers.sh
```

Batch all queries and merge:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main
bash scripts/run_all_and_merge.sh
```

Current limitation:

- `run_all_and_merge.sh` dedupes merged rows only on raw `URL`.

## 4. Run the GNEM Filtering / Dedup Pipeline

Live Tavily -> scored shortlist -> ready-doc publish:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main
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
  --credibility-threshold 60 \
  --write-stage-artifacts \
  --write-csv-exports \
  --write-sqlite-registry \
  --publish-ready-docs-dir "evAutomationUpdated/data/tavily ready documents"
```

First 500 queries for your offline hybrid corpus:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main
python -m tavily_ev_automation.gnem_pipeline \
  --queries-file data/queries/queries_1000.txt \
  --query-offset 0 \
  --max-queries 500 \
  --max-results 20 \
  --query-mode hybrid \
  --search-depth basic \
  --metadata-threshold 25 \
  --metadata-target-ratio 0.5 \
  --heuristic-threshold 45 \
  --hybrid-threshold 65 \
  --direct-usecase-threshold 0.70 \
  --credibility-threshold 60 \
  --write-stage-artifacts \
  --write-csv-exports \
  --publish-ready-docs-dir "evAutomationUpdated/data/tavily ready documents" \
  --output-dir outputs/pipeline_runs/batch_000_499
```

This is the folder the canonical `hybrid_rag` runner will read later:

- `evAutomationUpdated/data/tavily ready documents`

Metadata-only replay starting from an existing XLSX:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main
python -m tavily_ev_automation.gnem_pipeline \
  --input-metadata-xlsx path/to/metadata.xlsx \
  --sample-size 50 \
  --disable-metadata-rule-gate \
  --metadata-threshold 20 \
  --heuristic-threshold 45 \
  --hybrid-threshold 65 \
  --direct-usecase-threshold 0.70 \
  --credibility-threshold 60
```

Outputs written by this pipeline:

- final review-ready workbook / CSV / JSONL
- curated and rejected JSONL files
- document and chunk registries
- optional SQLite registry
- optional ready-doc publish into `evAutomationUpdated/data/tavily ready documents`
- `pipeline_report.json`

## 5. Build / Refresh Retrieval Indexes

There is no standalone index-build CLI in the current codebase.

Index creation happens implicitly inside:

- `evAutomationUpdated/main.py`
- `evAutomationUpdated/eval_runner.py`

The closest thing to a forced rebuild is to run `eval_runner.py` with `--reindex`.

Example:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main/evAutomationUpdated
python eval_runner.py \
  --model qwen25_14b \
  --mode hybrid_rag \
  --questions data/GNEM_Golden_Questions.xlsx \
  --max_questions 1 \
  --reindex \
  --out artifacts/response_outputs/index_probe.jsonl
```

This both rebuilds the named collections and runs one question.

## 6. Run Evaluation for One Model / One Mode

This is the most research-friendly runner in the repo today.

No-RAG:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main/evAutomationUpdated
python eval_runner.py \
  --model qwen25_14b \
  --mode no_rag \
  --questions data/GNEM_Golden_Questions.xlsx \
  --golden_answers artifacts/Golden_answers.xlsx \
  --study_id thesis_eval_round1 \
  --out artifacts/response_outputs/qwen25_14b_no_rag.jsonl
```

Local-RAG:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main/evAutomationUpdated
python eval_runner.py \
  --model qwen25_14b \
  --mode local_rag \
  --questions data/GNEM_Golden_Questions.xlsx \
  --data_workbook "data/GNEM updated excel.xlsx" \
  --golden_answers artifacts/Golden_answers.xlsx \
  --study_id thesis_eval_round1 \
  --out artifacts/response_outputs/qwen25_14b_local_rag.jsonl
```

Hybrid-RAG:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main/evAutomationUpdated
python eval_runner.py \
  --model qwen25_14b \
  --mode hybrid_rag \
  --questions data/GNEM_Golden_Questions.xlsx \
  --data_workbook "data/GNEM updated excel.xlsx" \
  --tavily_dir "data/tavily ready documents" \
  --golden_answers artifacts/Golden_answers.xlsx \
  --study_id thesis_eval_round1 \
  --reindex \
  --out artifacts/response_outputs/qwen25_14b_hybrid_rag.jsonl
```

Online model example:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main/evAutomationUpdated
python eval_runner.py \
  --model gemini25_flash \
  --mode local_rag \
  --questions data/GNEM_Golden_Questions.xlsx \
  --golden_answers artifacts/Golden_answers.xlsx \
  --study_id thesis_eval_round1 \
  --out artifacts/response_outputs/gemini25_flash_local_rag.jsonl
```

Outputs:

- append-safe JSONL at `--out`
- answers workbook at `artifacts/results/<run_id>_answers.xlsx`
- metrics workbook at `artifacts/results/<run_id>_metrics.xlsx`
- run manifest at `artifacts/results/<run_id>_manifest.json`
- study summary at `artifacts/results/<study_id>_summary.xlsx`
- leaderboard at `artifacts/results/<study_id>_leaderboard.csv`
- hybrid-value workbook at `artifacts/results/<study_id>_hybrid_value.xlsx` after both `local_rag` and `hybrid_rag` exist for the same model

Recommended research sequence for one model:

1. Run `local_rag` with a fixed `--study_id`.
2. Run `hybrid_rag` with the same `--study_id`.
3. Open `artifacts/results/<study_id>_hybrid_value.xlsx`.
4. Inspect the `per_question` sheet for:
   - `hybrid_answer_changed`
   - `hybrid_has_web_citations`
   - `hybrid_value_signal`
   - metric deltas versus `local_rag`

## 7. Run the Multi-Model Comparison App

This path is legacy and non-canonical. Use it only for backward-compatible workbook generation, not for thesis-quality experiments.

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main/evAutomationUpdated
python main.py \
  --data-workbook "GNEM updated excel (1).xlsx" \
  --question-workbook "Sample questions.xlsx" \
  --run-name qwen_rag \
  --run-name qwen_no_rag \
  --run-name gemma_rag \
  --run-name gemma_no_rag \
  --run-name gemini_rag \
  --run-name gemini_no_rag \
  --golden-workbook "artifacts/Golden_answers.xlsx" \
  --output-dir "artifacts/results/sample_run"
```

Single-model workbook:

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main/evAutomationUpdated
python main.py \
  --data-workbook "GNEM updated excel (1).xlsx" \
  --question-workbook "Sample questions.xlsx" \
  --run-name qwen_rag \
  --golden-workbook "artifacts/Golden_answers.xlsx" \
  --output-dir "artifacts/results/qwen_single_model" \
  --single-model-report
```

## 8. Generate Analysis Outputs

The analysis helper expects `.xlsx` outputs for `no_rag`, `local_rag`, and `hybrid_rag`.

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation/Tavily_ev-automation-main/evAutomationUpdated
python analyze_generated_reports.py \
  --no_rag artifacts/response_outputs/qwen14b_no_rag.xlsx \
  --local_rag artifacts/response_outputs/qwen14b_local_rag.xlsx \
  --hybrid_rag artifacts/response_outputs/qwen14b_hybrid_rag.xlsx \
  --out_dir analysis_out/
```

Expected analysis outputs go under `analysis_out/`.

## 9. Tests

```bash
cd /home/sm11926/Downloads/Ev_Crawler_Automation
pytest -q Tavily_ev-automation-main/evAutomationUpdated/tests
```

Current test coverage exists for:

- workbook loading
- retrieval query planning / summaries
- export behavior
- settings loading

Still missing:

- offline corpus HTML/PDF parsing
- citation validation
- evidence guard behavior
- crawler/download robustness
- end-to-end mode isolation
