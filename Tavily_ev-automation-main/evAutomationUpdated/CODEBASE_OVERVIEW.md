# Codebase Overview

This document is the fastest way to understand the code paths in this repository.

Important:
- The canonical thesis/research runner is `eval_runner.py`.
- `main.py` and `src/ev_llm_compare/cli.py` remain for legacy `ComparisonRunner` workflows and are not the primary experiment surface.

## Primary purpose

The repo benchmarks EV supply-chain question answering against workbook data and offline web evidence. The canonical runner supports:

- one model and one mode per invocation
- modes `no_rag`, `local_rag`, and `hybrid_rag`
- external golden-answer loading keyed by `q_id`
- offline evaluation using judge-based metrics
- reproducible manifests plus JSONL/XLSX/CSV outputs for downstream review

## Canonical runtime path

1. `eval_runner.py`
   - resolves the canonical model registry
   - loads questions and optional golden answers
   - enforces exactly one model and one mode per run
   - skips retrieval entirely for `no_rag`
   - builds local-only or local-plus-offline-Tavily retrieval for RAG modes
   - validates RAG citations and evidence support
   - writes JSONL, XLSX, manifest, study summary, leaderboard, and hybrid-value comparison outputs

## Legacy runtime path

1. `main.py`
   - adds `src/` to `sys.path`
   - calls `ev_llm_compare.cli.main()`
2. `src/ev_llm_compare/cli.py`
   - parses CLI arguments
   - loads config
   - creates `ComparisonRunner`
3. `src/ev_llm_compare/runner.py`
   - preserves the older multi-run workflow for compatibility and non-canonical experiments

## Core modules

### `settings.py`

Defines the application configuration:

- `RetrievalSettings`
- `RuntimeSettings`
- `EvaluationSettings`
- `ModelSpec`
- `AppConfig`

`load_config()` supports both current `EVALUATION_*` environment variables and legacy `RAGAS_*` aliases.

### `schemas.py`

Defines the shared data structures:

- `TableRow`
- `WorkbookNote`
- `Chunk`
- `RetrievalResult`
- `ModelResponse`

These objects are the contract between loader, chunker, retriever, runner, and exporters.

### `excel_loader.py`

Turns Excel files into structured in-memory records.

- Tabular sheets become `TableRow` objects.
- Single-column or note-like sheets become `WorkbookNote` objects.
- Question and golden-answer loading use flexible column matching.

### `chunking.py`

Builds retrieval chunks from rows and notes.

Each tabular row is expanded into multiple retrieval views:

- `row_full`
- `company_profile`
- `identity_theme`
- `location_theme`
- `supply_chain_theme`
- `product_theme`

This is why retrieval can answer both direct fact questions and grouped/analytic questions from the same workbook.

### `retrieval.py`

This is the most complex module in the repo.

It combines:

- query planning
- exact metadata filtering
- dense vector search in local Qdrant
- lexical scoring with IDF weighting
- reciprocal-rank fusion
- optional cross-encoder reranking
- structured analytic summary generation

The structured-summary path is especially important. For some question patterns, the retriever does more than fetch rows: it synthesizes grouped, counted, or ranked summaries directly from matched records before prompt construction.

### `prompts.py`

Builds prompts for:

- RAG answering
- non-RAG answering
- reference-answer generation

It also compacts retrieval context so the model sees short evidence blocks instead of raw duplicated rows when that is sufficient.

### `models.py`

Defines the LLM client boundary:

- `OllamaClient`
- `GeminiClient`
- `create_client()`
- `safe_generate()`

`safe_generate()` normalizes failures into `(answer, latency, success, error)` tuples so runner logic stays simple.

### `evaluation.py`

Handles:

- fallback reference generation
- judge-based scoring
- workbook exports
- per-run CSV/Markdown exports

The metric set currently includes:

- `answer_accuracy`
- `faithfulness`
- `response_groundedness`
- `grounded_claim_ratio`
- `unsupported_claim_ratio`
- `contradicted_claim_ratio`

Although the repo historically used `RAGAS` terminology, the current implementation uses custom judge prompts and parsers rather than the `ragas` library API.

## Data flow

The main flow is:

1. Workbook rows and notes are loaded.
2. Chunks are built and embedded.
3. Retrieval results are cached per question.
4. Each model run answers the same question set.
5. Golden answers are loaded when available.
6. Missing references are generated from workbook evidence.
7. Judge-based metrics are computed.
8. Reports are written to `artifacts/results/` or the requested output directory.

## Checked-in data assets

The current repository includes:

- source workbook: `GNEM updated excel (1).xlsx`
- question workbook: `Sample questions.xlsx`
- grouped question workbook: `Grouped_use_cases_for_Sample 100 questions.xlsx`
- golden-answer workbook: `artifacts/Golden_answers.xlsx`

The sample question and golden-answer workbooks are already populated with 100 questions/answers.

## Outputs

The main workbook export contains:

- `all_in_one`
- `responses`
- `responses_raw`
- `retrieval`
- `references`
- `metrics_per_question`
- `metrics_summary`

Per-run exports can also include:

- `all_runs_responses.csv`
- `all_runs_metrics.xlsx`
- `all_runs_single_sheet.xlsx`
- `<run_name>_responses.csv`
- `<run_name>_responses.md`

## What is benchmark-specific

Some logic is intentionally tuned to the checked-in question set and workbook schema.

Examples:

- exact matching on known companies, categories, locations, OEMs, and role terms
- analytic branches for grouped, counted, ranked, and EV-relevance questions
- field-priority rules that preserve workbook column semantics

This makes the repo strong for the current EV workbook use case, but less generic than a reusable document-QA framework.

## Extension points

If you want to add new operations, the safest entry points are:

- new model runs in `settings.py`
- new question-pattern handling in `retrieval.py`
- new export formats in `evaluation.py`
- new chunk views in `chunking.py`
- new providers in `models.py`

If you need to generalize the repo beyond the current workbook shape, start with `excel_loader.py`, `chunking.py`, and the structured-summary logic in `retrieval.py`.
