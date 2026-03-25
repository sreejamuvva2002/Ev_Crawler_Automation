# EV Research Evaluation Pipeline

This package contains the canonical research evaluation runner for EV supply-chain question answering experiments. The thesis-grade comparison surface is `eval_runner.py`, which runs exactly one model and one mode per invocation and writes reproducible manifests plus per-question outputs.

The supported model keys are:

- `qwen25_14b`
- `gemma27b`
- `gemini25_flash`

The supported modes are:

- `no_rag`
- `local_rag`
- `hybrid_rag`

Methodological guarantees in the canonical runner:

- `no_rag` answers from model knowledge only and does not build or touch retrieval indexes.
- `local_rag` retrieves only from the local workbook-derived corpus.
- `hybrid_rag` retrieves only from the local workbook-derived corpus plus already-downloaded offline Tavily documents stored locally.
- `hybrid_rag` does not call the live Tavily API during evaluation.
- RAG answers are context-only, citation-constrained, and post-validated for citation presence, citation validity, and evidence support.

`main.py` remains in the repository as a legacy convenience entrypoint, but it is not the canonical thesis/research runner.

## Repo shape

- `eval_runner.py`: canonical single-model, single-mode evaluation runner
- `main.py`: legacy multi-run entrypoint, not the primary research surface
- `src/ev_llm_compare/`: retrieval, prompting, export, and scoring modules
- `tests/`: unit tests for loading, retrieval, validation, and manifests
- `artifacts/response_outputs/`: per-run JSONL response records
- `artifacts/results/`: manifests, answer workbooks, metric workbooks, study summaries, leaderboard CSVs

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
export GEMMA_MODEL=gemma3:27b
export EVALUATION_JUDGE_PROVIDER=ollama
export EVALUATION_JUDGE_MODEL=mistral-small3.2:24b
export EVALUATION_MAX_RETRIES=2
```

Legacy `RAGAS_*` environment variables are still accepted as compatibility aliases, but the default evaluation layer is documented as judge-based metrics because it does not call the `ragas` Python package directly.

If you are using Ollama-backed models locally:

```bash
ollama pull qwen2.5:14b
ollama pull gemma3:27b
ollama pull mistral-small3.2:24b
```

## Run

No-RAG baseline:

```bash
python eval_runner.py \
  --model qwen25_14b \
  --mode no_rag \
  --questions "data/GNEM_Golden_Questions.xlsx" \
  --golden_answers "artifacts/Golden_answers.xlsx"
```

Local-RAG run:

```bash
python eval_runner.py \
  --model gemma27b \
  --mode local_rag \
  --questions "data/GNEM_Golden_Questions.xlsx" \
  --data_workbook "data/GNEM updated excel.xlsx" \
  --golden_answers "artifacts/Golden_answers.xlsx"
```

Hybrid-RAG run with offline Tavily documents only:

```bash
python eval_runner.py \
  --model gemini25_flash \
  --mode hybrid_rag \
  --questions "data/GNEM_Golden_Questions.xlsx" \
  --data_workbook "data/GNEM updated excel.xlsx" \
  --tavily_dir "data/tavily ready documents" \
  --golden_answers "artifacts/Golden_answers.xlsx" \
  --study_id thesis_eval_round1
```

Optional overrides:

```bash
python eval_runner.py \
  --model qwen25_14b \
  --mode local_rag \
  --questions "data/GNEM_Golden_Questions.xlsx" \
  --data_workbook "data/GNEM updated excel.xlsx" \
  --golden_answers "artifacts/Golden_answers.xlsx" \
  --top_k_local 6 \
  --top_k_tavily 4 \
  --context_budget_tokens 1200 \
  --seed 7
```

## Golden answers

The canonical runner accepts a workbook or CSV through `--golden_answers` with this schema:

- required: `q_id`, `question`, `golden_answer`
- optional: `question_type`, `golden_key_facts`, `answer_format`, `notes`

Matching behavior:

- primary key is `q_id`
- if a golden answer exists for a row, it is treated as the authoritative scoring reference
- once `--golden_answers` is provided, the runner does not fall back to question-row `expected_answer` values for accuracy-style scoring
- question-row `expected_answer` values are only used when no external golden-answer file is supplied

## Evaluation semantics

`no_rag` uses a separate prompt that allows model-knowledge answers and does not mention missing context.

`local_rag` and `hybrid_rag` use context-only prompting. If no supported answer exists, the model must output exactly:

```text
Not found in provided context.
```

RAG answer format requirements:

- 3 to 7 factual bullets maximum
- every factual bullet ends with one or more evidence IDs such as `[DOC:<id>]`, `[WEB:<id>]`, `[ANALYTIC:<id>]`, or `[GEO:<id>]`
- if support is partial, answer only the supported portion and add `Missing info:`
- post-generation validation checks bullet parsing, citation presence, citation validity against retrieved IDs, and whether cited evidence supports each claim

Metric families:

- golden-answer metrics: normalized exact match, semantic similarity, list/set precision-recall-F1 where applicable, and numeric exact/tolerance matching where applicable
- judge-based evidence metrics for RAG runs: citation coverage, citation validity, support rate, unsupported claim rate, and abstention correctness

## Outputs

Each run writes:

- `artifacts/response_outputs/<run_id>.jsonl`
- `artifacts/results/<run_id>_answers.xlsx`
- `artifacts/results/<run_id>_metrics.xlsx`
- `artifacts/results/<run_id>_manifest.json`
- `artifacts/results/<study_id>_summary.xlsx`
- `artifacts/results/<study_id>_leaderboard.csv`
- `artifacts/results/<study_id>_hybrid_value.xlsx`

Per-run manifests and JSONL records capture reproducibility fields such as:

- `run_id`
- model key and resolved provider/model ID
- mode
- prompt text and system prompt
- temperature, max tokens, and seed when available
- retrieval top-k settings
- workbook hash
- golden-answer file hash
- offline Tavily manifest hash
- embedding model
- collection/index fingerprints
- git commit hash
- timestamp

Per-question records include question metadata, generated answer, retrieved context IDs, extracted citations, golden answer when available, metric fields, validation flags, and error fields.

`<study_id>_hybrid_value.xlsx` is the study-level inspection workbook for judging whether offline web evidence added value. It pairs `local_rag` and `hybrid_rag` runs for the same model and shows, per question:

- the local answer and hybrid answer side by side
- whether the hybrid answer changed
- whether the hybrid answer used `WEB:` citations
- metric deltas such as support rate, citation coverage, and golden-answer similarity
- a `hybrid_value_signal` flag for questions where hybrid both used web evidence and materially changed or improved the answer

## Migration notes

- `eval_runner.py` is now the canonical thesis/research runner.
- `main.py` is preserved for backward compatibility, but it is no longer the primary comparison surface.
- Research runs are intentionally single-model and single-mode; study-level summaries are built by aggregating per-run manifests and outputs.
- Custom grounding and evidence checks are documented as judge-based metrics, not RAGAS, unless the real `ragas` package is integrated later.
