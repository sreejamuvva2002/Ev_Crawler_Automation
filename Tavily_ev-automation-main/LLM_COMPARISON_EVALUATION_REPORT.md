# LLM Comparison And Evaluation Report

Date: 2026-03-25

Scope: the EV-answer-generation and evaluation subsystem centered on:

- `evAutomationUpdated/eval_runner.py`
- `evAutomationUpdated/src/ev_llm_compare/research_eval.py`
- `evAutomationUpdated/src/ev_llm_compare/retrieval.py`
- `evAutomationUpdated/src/ev_llm_compare/settings.py`
- `evAutomationUpdated/src/ev_llm_compare/offline_corpus.py`
- `evAutomationUpdated/src/ev_llm_compare/models.py`
- `evAutomationUpdated/src/ev_llm_compare/chunking.py`
- `evAutomationUpdated/src/ev_llm_compare/derived_analytics.py`
- legacy surfaces: `evAutomationUpdated/main.py`, `evAutomationUpdated/src/ev_llm_compare/cli.py`, `evAutomationUpdated/src/ev_llm_compare/runner.py`

Companion repo-wide review documents:

- `REPORT_CODE_REVIEW.md`
- `CHANGE_LIST_CODE.md`
- `RUNBOOK.md`

## 1. Executive Summary

This subsystem compares EV-supply-chain question answering across three models and three experimental conditions:

Models:

- `qwen25_14b`
- `gemma27b`
- `gemini25_flash`

Modes:

- `no_rag`
- `local_rag`
- `hybrid_rag`

The canonical research runner is now:

- `evAutomationUpdated/eval_runner.py`

The correct way to explain the current design is:

- `eval_runner.py` is the thesis/research surface.
- `main.py` and `ComparisonRunner` still exist for backward compatibility, but they are not the main experiment path.

## 2. What This System Is Trying To Do

The evaluation pipeline is trying to answer a controlled research question:

How do different LLMs perform on EV-supply-chain questions under three conditions?

1. `no_rag`
   - answer from model knowledge only
2. `local_rag`
   - answer from the local workbook-derived corpus only
3. `hybrid_rag`
   - answer from the local workbook-derived corpus plus the offline Tavily document folder only

The goal is not a demo chatbot. The goal is a controlled comparison with:

- fixed run conditions
- explicit model registry
- explicit mode definitions
- reproducible manifests
- golden-answer scoring
- evidence-grounding checks for RAG runs

## 3. Canonical Versus Legacy Paths

### Canonical research path

`evAutomationUpdated/eval_runner.py`

This is the one you should talk about in your thesis or professor meeting.

### Legacy path

- `evAutomationUpdated/main.py`
- `evAutomationUpdated/src/ev_llm_compare/cli.py`
- `evAutomationUpdated/src/ev_llm_compare/runner.py`

These are still in the repo, but they are now clearly marked as legacy/non-canonical.

Important wording:

- They are not the primary experiment surface anymore.
- They should not be used as the basis for the formal research comparison.

## 4. What Happens In The Canonical Runner

The implemented flow in `eval_runner.py` is:

```text
questions file
  -> load questions
  -> resolve canonical model registry
  -> load optional golden answers
  -> build run_id and manifest paths
  -> mode-specific retrieval setup
  -> retrieve local/offline evidence when required
  -> build prompt
  -> generate answer
  -> extract citations
  -> validate RAG grounding
  -> score against golden answers
  -> write JSONL / workbooks / manifest
  -> update study summary / leaderboard
  -> compare local_rag vs hybrid_rag in a hybrid-value workbook
```

## 5. Exact Study Matrix

### Supported model registry

The current model registry is resolved in `build_model_spec(...)`.

Canonical model keys:

- `qwen25_14b`
- `gemma27b`
- `gemini25_flash`

These are mapped to provider/model IDs and logged in every run manifest.

That means the system records both:

- the study-friendly model key
- the resolved runtime provider/model identifier

### Supported modes

The canonical modes are exactly:

- `no_rag`
- `local_rag`
- `hybrid_rag`

Their meanings are now explicit:

- `no_rag`: no retrieval, no index touch, no workbook dependency for answering
- `local_rag`: local workbook-derived corpus only
- `hybrid_rag`: local workbook-derived corpus + offline Tavily folder only

Important methodological point:

- `hybrid_rag` does not call live Tavily search during evaluation

## 6. Inputs

### Question file

Loaded by `load_eval_questions(...)`.

The runner supports tabular question files and preserves:

- `q_id`
- `question`
- optional question-row expected answer fields

### Local structured corpus

Loaded from the workbook through:

- `load_workbook(...)`
- `ExcelChunkBuilder(...)`
- `build_derived_summary_chunks(...)`

This produces the local retrieval corpus.

### Offline Tavily corpus

Loaded only in `hybrid_rag` through:

- `resolve_tavily_root(...)`
- `load_offline_documents(...)`
- `build_document_chunks(...)`

This means the web evidence is pre-collected and local at evaluation time.

### Golden answers

Loaded by:

- `load_golden_answers(...)`
- `resolve_golden_answer(...)`

Required schema:

- `q_id`
- `question`
- `golden_answer`

Optional schema:

- `question_type`
- `golden_key_facts`
- `answer_format`
- `notes`

Important correctness rule now implemented:

- if `--golden_answers` is supplied, the runner does not fall back to question-row `expected_answer` values for scored rows

That is important because external gold labels must stay external.

## 7. Retrieval And Indexing

Retrieval is handled by `HybridRetriever` in `retrieval.py`.

What it does:

- builds or reuses persistent named Qdrant collections
- supports local and Tavily collections
- computes collection fingerprints
- uses routing and retrieval planning
- returns ranked chunks with provenance metadata

Current collections:

- `local`
- `tavily`

Important methodological point:

- `no_rag` does not build or touch these collections inside `eval_runner.py`

That is the correct control-condition behavior.

## 8. Prompting Policy

### No-RAG prompt

Built by `build_prompt(...)` when mode is `no_rag`.

Current intent:

- use model knowledge only
- do not use retrieved context
- do not mention missing workbook/context

### RAG prompt

Built by `build_prompt(...)` when mode is `local_rag` or `hybrid_rag`.

Current policy:

- use only the provided context
- do not guess
- if partially supported, answer only the supported part
- if unsupported, output exactly `Not found in provided context.`
- every factual bullet must cite evidence IDs

This is the correct context-only methodology for RAG evaluation.

## 9. Post-Generation Validation

This is one of the biggest improvements in the current system.

RAG validation is handled in `research_eval.py`, mainly through:

- `extract_citations(...)`
- `parse_answer_bullets(...)`
- `validate_rag_answer(...)`
- `_judge_support(...)`
- `_judge_answerability(...)`

The runner now validates:

1. whether the answer abstained
2. whether factual bullets contain citations
3. whether cited IDs were actually retrieved
4. whether the cited evidence supports the bullet claim

Separate failure flags are recorded:

- `citation_missing`
- `citation_invalid`
- `support_failed`

This is critical because citation presence alone is not enough.

## 10. Golden-Answer Metrics

Golden-answer scoring is computed by `compute_golden_metrics(...)`.

Implemented metrics:

- normalized exact match
- semantic similarity
- list precision
- list recall
- list F1
- numeric exact match
- numeric tolerance match

These metrics are only meaningful when a golden answer exists.

This is why the system distinguishes:

- gold-covered questions
- questions without external golden answers

## 11. RAG Grounding Metrics

For `local_rag` and `hybrid_rag`, the system computes judge-based evidence metrics:

- citation coverage
- citation validity
- support rate
- unsupported claim rate
- abstention correctness

Important terminology correction:

- these are judge-based metrics
- they are not true RAGAS metrics, because the current code does not import or call the `ragas` Python package

That wording is now aligned in the active runner/docs.

## 12. Outputs

Per run, the canonical runner writes:

- `artifacts/response_outputs/<run_id>.jsonl`
- `artifacts/results/<run_id>_answers.xlsx`
- `artifacts/results/<run_id>_metrics.xlsx`
- `artifacts/results/<run_id>_manifest.json`

Per study, it writes:

- `artifacts/results/<study_id>_summary.xlsx`
- `artifacts/results/<study_id>_leaderboard.csv`
- `artifacts/results/<study_id>_hybrid_value.xlsx`

### Why the hybrid-value workbook matters

This was added so you can answer a very important professor question:

Did the offline web documents actually add value beyond local workbook RAG?

The `hybrid_value.xlsx` file compares `local_rag` and `hybrid_rag` for the same model and study ID, and shows:

- local answer vs hybrid answer
- whether the answer changed
- whether the hybrid answer used `WEB:` citations
- metric deltas such as support and golden-answer similarity
- a `hybrid_value_signal` flag

This is the right artifact to open when you want to justify whether hybrid web evidence is useful.

## 13. Run Manifest And Reproducibility

Each run manifest now captures:

- `run_id`
- `study_id`
- `model_key`
- resolved provider/model ID
- mode
- prompt text
- system prompt
- temperature
- max tokens
- seed
- question file path and count
- golden-answer path and hash
- local workbook path and hash
- offline Tavily manifest path and hash
- embedding model
- reranker settings
- local and Tavily collection fingerprints
- git commit hash
- results paths

This is a major reproducibility improvement over the earlier state.

## 14. Final Changes Made In This Review Round

These are the important changes that were actually implemented.

### 14.1 Canonical runner decision

Implemented:

- `eval_runner.py` is now the declared canonical runner
- legacy `main.py` and `cli.py` now warn that they are non-canonical

### 14.2 Experiment matrix frozen

Implemented:

- exact model keys
- exact mode names
- one model and one mode per invocation

### 14.3 True no-RAG isolation

Implemented:

- `no_rag` does not instantiate retrievers
- `no_rag` does not build or touch retrieval indexes inside the canonical runner

### 14.4 Golden-answer independence

Implemented:

- external `--golden_answers` disables fallback to question-sheet expected answers for scoring

### 14.5 Citation and support validation

Implemented:

- citation extraction
- bullet parsing
- citation validity checks
- support checks
- separate validation flags

### 14.6 Terminology cleanup

Implemented:

- active research outputs use `judge-based metrics`
- package metadata and dependency list were aligned so the repo no longer advertises `ragas` as if it were integrated

### 14.7 Hybrid-value analysis artifact

Implemented:

- `export_hybrid_value_report(...)`
- study-level workbook showing whether offline web evidence changes/improves answers

### 14.8 Tests

Added/updated tests for:

- golden answer loading
- q_id matching
- no-RAG retrieval isolation
- citation extraction
- citation validation
- support validation
- hybrid offline-folder behavior
- manifest generation
- hybrid-value workbook export

## 15. What This System Is Doing Correctly Now

The strongest points are:

1. The canonical experiment surface is now clearly defined.
2. The study matrix is explicit and constrained.
3. `no_rag`, `local_rag`, and `hybrid_rag` now have methodologically distinct behavior.
4. Hybrid uses offline web evidence only, which makes the evaluation more stable.
5. Golden-answer scoring is externalized correctly when a golden file is provided.
6. RAG outputs are no longer treated as grounded merely because they mention citations.
7. Run manifests are much stronger and more replayable.
8. The new hybrid-value workbook makes the “does web help?” question measurable.

## 16. What Is Still Weak Or Not Perfect

This is the honest part you should keep in mind.

### 16.1 Judge-based support validation is still not human annotation

Problem:

- support checking uses heuristics and/or an LLM judge
- this is better than no validation, but it is not the same as human evidence annotation

Consequence:

- groundedness metrics are still approximate

### 16.2 Legacy code is still present

Problem:

- `ComparisonRunner` still exists in the repo

Consequence:

- there is still some architectural residue from the old split-surface design, even though the canonical path is now explicit

### 16.3 No standalone index-build CLI

Problem:

- index creation still happens implicitly in evaluation runs

Consequence:

- setup is less explicit than ideal

### 16.4 Semantic similarity has a fallback mode

Problem:

- `SemanticSimilarityScorer` uses `SentenceTransformer` if available, otherwise falls back to `SequenceMatcher`

Consequence:

- metric behavior can vary across environments if embedding dependencies are not consistently installed

### 16.5 Hybrid quality still depends on crawler quality

Problem:

- `hybrid_rag` is only as good as the offline Tavily corpus published by the crawler/filtering system

Consequence:

- if the web corpus is weak, hybrid may add little or even noisy value

## 17. Are We Doing The LLM Comparison Correctly?

Short answer:

- yes, much more correctly than before
- but still with important caveats

More precise answer:

- For a controlled engineering research comparison, the current canonical runner is in much better shape.
- The key methodological problems called out in the review were addressed: canonical runner choice, no-RAG isolation, external gold handling, offline-only hybrid, citation validation, and stronger reproducibility.
- The remaining weaknesses are mostly about how strong the evidence-validation layer is, not about the experiment matrix itself.

So the correct professor-facing answer is:

The current evaluation setup is substantially more defensible and close to research-grade for controlled experiments, but the judge-based grounding metrics should still be presented as approximate automated checks rather than perfect truth labels.

## 18. How To Explain This To Your Professor

You can say:

1. I run each experiment through one canonical script, `eval_runner.py`, using exactly one model and one mode at a time.
2. My three modes are `no_rag`, `local_rag`, and `hybrid_rag`, with `hybrid_rag` using only local offline web documents, not live Tavily calls.
3. I evaluate three models: Qwen2.5 14B, Gemma 27B, and Gemini 2.5 Flash.
4. If a golden-answer file is provided, that file is the authoritative accuracy reference; I do not replace it with LLM-generated references.
5. For RAG runs, I require citations, verify that cited IDs were actually retrieved, and check whether the cited evidence supports the claim.
6. I log full per-run manifests and per-question JSONL records so the experiment is reproducible.
7. I compare local RAG against hybrid RAG in a dedicated workbook to see whether offline web documents actually add value.

## 19. Bottom-Line Verdict

This subsystem is now organized in a way that is appropriate for a controlled model comparison study.

It is doing the important things correctly:

- canonical runner
- fixed study matrix
- true no-RAG control
- offline-only hybrid
- external golden-answer handling
- evidence validation
- reproducible run manifests

It is not perfect, and you should not oversell it. The remaining weakness is that the grounding metrics are judge-based automation rather than human-labeled evidence truth. But as an engineering research pipeline, it is now in a much stronger and more defensible state than the original split-runner design.
