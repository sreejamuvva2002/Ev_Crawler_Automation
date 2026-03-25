# EV LLM Comparison Pipeline

This repository compares multiple LLMs on Excel-based EV supply-chain questions, with and without RAG, then exports workbooks containing responses, retrieval evidence, reference answers, and evaluation metrics.

The active application code lives in `src/ev_llm_compare/`. For a code-oriented walkthrough of the modules and runtime flow, see `CODEBASE_OVERVIEW.md`.

## Supported runs

- Qwen with RAG
- Qwen without RAG
- Gemma 3 12B with RAG
- Gemma 3 12B without RAG
- Gemini 2.5 Flash with RAG
- Gemini 2.5 Flash without RAG

## Repo shape

- `src/ev_llm_compare/`: production code
- `tests/`: unit tests for loader, retrieval, settings, and exports
- `artifacts/qdrant/`: local vector index storage
- `artifacts/results/`: generated comparison workbooks and response exports
- `GNEM updated excel (1).xlsx`: checked-in source workbook
- `Sample questions.xlsx`: checked-in workbook with 100 questions
- `artifacts/Golden_answers.xlsx`: checked-in workbook with 100 golden answers
- `Grouped_use_cases_for_Sample 100 questions.xlsx`: reference grouping of the 100 sample questions
- `main.py`: CLI entrypoint

## Setup

Using `pip`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

Using `uv`:

```bash
uv sync
source .venv/bin/activate
```

Environment variables:

```bash
export GEMINI_API_KEY=your_key_here
export OLLAMA_BASE_URL=http://localhost:11434
export QWEN_MODEL=qwen2.5:14b
export GEMMA_MODEL=gemma3:12b
export EVALUATION_JUDGE_PROVIDER=ollama
export EVALUATION_JUDGE_MODEL=mistral-small3.2:24b
export EVALUATION_MAX_RETRIES=2
```

Legacy `RAGAS_*` environment variables are still accepted as compatibility aliases, but the evaluation layer is now documented as judge-based metrics because it does not call the `ragas` Python API directly.

Make sure the local Ollama models are pulled if you are using Ollama-backed runs:

```bash
ollama pull qwen3:8b
ollama pull gemma3:12b
ollama pull mistral-small3.2:24b
```

## Run

Default run:

```bash
python main.py \
  --data-workbook "GNEM updated excel (1).xlsx" \
  --question-workbook "Sample questions.xlsx"
```

Example: compare the first 10 questions with a single-sheet report:

```bash
python main.py \
  --data-workbook "GNEM updated excel (1).xlsx" \
  --question-workbook "Sample questions.xlsx" \
  --question-limit 10 \
  --run-name qwen_rag \
  --run-name qwen_no_rag \
  --run-name gemma_rag \
  --run-name gemma_no_rag \
  --run-name gemini_rag \
  --run-name gemini_no_rag \
  --golden-workbook "artifacts/Golden_answers.xlsx" \
  --output-dir "artifacts/results/sample_run" \
  --single-sheet-only \
  --no-response-exports
```

Example: generate a dedicated single-model workbook with per-question metrics plus `overall_response`, `knowledge_source_data`, and `pretrained_data`:

```bash
python main.py \
  --data-workbook "GNEM updated excel (1).xlsx" \
  --question-workbook "Sample questions.xlsx" \
  --run-name qwen_rag \
  --golden-workbook "artifacts/Golden_answers.xlsx" \
  --output-dir "artifacts/results/qwen_single_model" \
  --single-model-report
```

To skip evaluation while validating model access:

```bash
python main.py --skip-evaluation
```

`--skip-ragas` is still accepted as a hidden compatibility alias.

Optional runtime overrides:

```bash
export MODEL_MAX_TOKENS=1600
export MODEL_TEMPERATURE=0.1
```

## Retrieval and chunking

- Every tabular row is expanded into multiple chunk views:
  - `row_full`
  - `company_profile`
  - `identity_theme`
  - `location_theme`
  - `supply_chain_theme`
  - `product_theme`
- Single-column sheets such as methodology or definitions are indexed as `note_reference` chunks.
- Retrieval combines:
  - query planning and metadata-aware routing
  - sentence-transformer dense search
  - lexical overlap scoring
  - reciprocal-rank fusion
  - optional cross-encoder reranking
- Exact metadata matches can produce structured summaries before prompt generation.
- Context selection caps duplicate companies and trims oversized structured outputs.

## Outputs

The default full workbook writes these sheets in `artifacts/results/`:

- `all_in_one`
- `responses`
- `responses_raw`
- `retrieval`
- `references`
- `metrics_per_question`
- `metrics_summary`

If you pass `--single-sheet-only`, the output workbook contains only `all_in_one`.

That sheet includes:

- `Question`
- `reference_answer`
- `reference_source`
- one response column per model
- six metric columns per model:
  - `answer_accuracy`
  - `faithfulness`
  - `response_groundedness`
  - `grounded_claim_ratio`
  - `unsupported_claim_ratio`
  - `contradicted_claim_ratio`

Per-run response exports are written to `artifacts/correct_responses/` by default:

- `all_runs_responses.csv`
- `all_runs_metrics.xlsx`
- `all_runs_single_sheet.xlsx`
- `<run_name>_responses.csv`
- `<run_name>_responses.md`

When `--single-model-report` is used with exactly one run, an additional workbook is written to the output directory with:

- `report`
- `attribution_units`
- `metrics_per_question`
- `metrics_summary`

## Notes

- The pipeline is workbook-driven, so you can point it at updated Excel files without changing code.
- If `artifacts/Golden_answers.xlsx` exists, it is used automatically for `answer_accuracy`.
- Any question missing from the golden workbook falls back to generated reference answers.
- Non-RAG runs only receive `answer_accuracy`; grounding metrics are reserved for RAG runs with retrieved context.
- The checked-in sample assets are aligned: the source workbook, 100-question workbook, and 100-answer golden workbook are all ready for local comparisons.
