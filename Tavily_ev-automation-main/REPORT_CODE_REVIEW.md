# Code Review Report

Date: 2026-03-25

Scope: full scan of the uploaded `Tavily_ev-automation-main` repository, covering both the Tavily/GNEM crawler pipeline and the `evAutomationUpdated` LLM comparison pipeline.

Method: static code review, checked-in artifact review, README/CLI review, and local test execution for `evAutomationUpdated/tests`.

Verdict: the repo contains real, useful building blocks for a research pipeline, but it is not yet research-grade end-to-end. The strongest parts are the GNEM filtering/dedup stack and the single-model `eval_runner.py` workflow. The biggest blockers are reproducibility gaps, split evaluation surfaces, weak citation enforcement, and one committed secret.

Most important sequencing note: before adding features to both evaluation stacks, the project should declare one canonical research runner. Until that decision is made, feature-parity work on the non-canonical runner is at risk of being wasteful.

## Repo Scan

### High-level tree

```text
Tavily_ev-automation-main/
├── README.md
├── requirements.txt
├── data/
│   ├── grounding/
│   └── queries/
├── docs/
│   └── rag_filtering_approach.md
├── scripts/
│   ├── run_all_and_merge.sh
│   └── run_georgia_ev_battery_suppliers.sh
├── tavily_ev_automation/
│   ├── tavily_crawler.py
│   ├── generate_gnem_queries.py
│   ├── gnem_pipeline.py
│   ├── gnem_rag_helpers.py
│   └── embedding_runtime.py
└── evAutomationUpdated/
    ├── README.md
    ├── main.py
    ├── eval_runner.py
    ├── analyze_generated_reports.py
    ├── pyproject.toml
    ├── requirements.txt
    ├── uv.lock
    ├── data/
    │   ├── GNEM updated excel.xlsx
    │   ├── GNEM_Golden_Questions.xlsx
    │   └── tavily ready documents/
    ├── artifacts/
    │   ├── Golden_answers.xlsx
    │   ├── qdrant/
    │   ├── response_outputs/
    │   ├── results/
    │   └── sample50_runs/
    ├── src/ev_llm_compare/
    │   ├── cli.py
    │   ├── runner.py
    │   ├── retrieval.py
    │   ├── offline_corpus.py
    │   ├── prompts.py
    │   ├── models.py
    │   ├── evaluation.py
    │   ├── settings.py
    │   └── excel_loader.py
    └── tests/
        ├── test_excel_loader.py
        ├── test_retrieval.py
        ├── test_evaluation_export.py
        └── test_settings.py
```

### Entrypoints and execution surfaces

| Area | Entrypoint | Purpose |
|---|---|---|
| Crawl/search | `tavily_ev_automation/tavily_crawler.py` | Single-query Tavily search + optional download + Excel export |
| Query generation | `tavily_ev_automation/generate_gnem_queries.py` | Builds GNEM-focused query list from grounding assets |
| GNEM filtering | `tavily_ev_automation/gnem_pipeline.py` | Multi-stage retrieval, scoring, dedup, curation, ready-doc publishing |
| Crawl shell batching | `scripts/run_georgia_ev_battery_suppliers.sh` | One canned crawl run |
| Crawl shell batching | `scripts/run_all_and_merge.sh` | Batch all queries, merge XLSX outputs |
| Multi-model comparison | `evAutomationUpdated/main.py` -> `src/ev_llm_compare/cli.py` | Workbook-driven multi-run comparison (`rag` vs `no_rag`) |
| Single-model research eval | `evAutomationUpdated/eval_runner.py` | One model + one mode per invocation (`no_rag`, `local_rag`, `hybrid_rag`) |
| Post-hoc analysis | `evAutomationUpdated/analyze_generated_reports.py` | Compares generated `.xlsx` mode outputs and computes structural metrics |

Not found during scan: Streamlit apps, notebooks, web UI servers.

### Key modules by responsibility

| Responsibility | Code evidence |
|---|---|
| URL canonicalization / deterministic IDs | `tavily_ev_automation/gnem_pipeline.py:611-626`, `:1159-1164` |
| Tavily API search | `tavily_ev_automation/gnem_pipeline.py:1200-1314`, `tavily_ev_automation/tavily_crawler.py:195-250` |
| Download/acquisition | `tavily_ev_automation/tavily_crawler.py:269-380`, `tavily_ev_automation/gnem_pipeline.py:1565-1611`, `:2849-2889` |
| Exact duplicate pass | `tavily_ev_automation/gnem_pipeline.py:2241-2289` |
| Near-duplicate pass | `tavily_ev_automation/gnem_pipeline.py:2292-2396` |
| Document-card building | `tavily_ev_automation/gnem_rag_helpers.py:1614-1735` |
| Hybrid scoring + classifier | `tavily_ev_automation/gnem_rag_helpers.py:1783-2011` |
| Credibility scoring | `tavily_ev_automation/gnem_rag_helpers.py:2014-2055` |
| Ready-doc publish bridge | `tavily_ev_automation/gnem_pipeline.py:1997-2081`, `:3327-3339` |
| Local workbook retrieval | `evAutomationUpdated/src/ev_llm_compare/runner.py:68-80`, `evAutomationUpdated/eval_runner.py:796-813` |
| Offline Tavily folder ingestion | `evAutomationUpdated/src/ev_llm_compare/offline_corpus.py:56-145`, `evAutomationUpdated/eval_runner.py:814-839` |
| Prompting | `evAutomationUpdated/src/ev_llm_compare/prompts.py:8-18`, `:114-149`; `evAutomationUpdated/eval_runner.py:57-80`, `:544-556` |
| Model wrappers | `evAutomationUpdated/src/ev_llm_compare/models.py:58-172` |
| Judge-based metrics | `evAutomationUpdated/src/ev_llm_compare/evaluation.py:17-24`, `:520-608` |

### Storage / artifacts observed in the ZIP

| Area | Observed path(s) | Notes |
|---|---|---|
| Offline Tavily corpus | `evAutomationUpdated/data/tavily ready documents/` | Folder-based offline corpus for hybrid runs |
| Ready-doc manifest | `evAutomationUpdated/data/tavily ready documents/tavily_ready_documents_manifest.csv` | Handoff manifest from GNEM pipeline |
| Ready-doc unmatched list | `evAutomationUpdated/data/tavily ready documents/tavily_ready_documents_unmatched.csv` | Tracks publish misses |
| Local workbook corpus | `evAutomationUpdated/data/GNEM updated excel.xlsx` | Local structured corpus for retrieval |
| Question workbooks | `evAutomationUpdated/data/GNEM_Golden_Questions.xlsx`, `GNEM_Generated_Questions.xlsx`, `Sample 50 questions.xlsx` | Eval question inputs |
| Golden answers | `evAutomationUpdated/artifacts/Golden_answers.xlsx` | Human-curated answer file for at least one sample set |
| Vector store manifests | `evAutomationUpdated/artifacts/qdrant/_index_manifests/local.json`, `tavily.json` | Evidence that persistent named indexes were built |
| Eval outputs | `evAutomationUpdated/artifacts/response_outputs/*.jsonl`, `*.xlsx` | Single-model mode outputs |
| Multi-run outputs | `evAutomationUpdated/artifacts/results/`, `artifacts/sample50_runs/` | Workbook exports from comparison runs |

Observed repo/documentation drift:

- `README.md:41-55` references `examples/` and `outputs/`, but those directories were not present in the uploaded ZIP.
- `README.md:28-33` references `data/corpus/`, but the uploaded root `data/` only contained `grounding/` and `queries/`.
- `README.md:53` says `data/grounding/GA_Automotive Landscape_All_Companies (1).xlsx` is expected locally but not committed. That asset is required evidence for some GNEM runs and was missing from the ZIP.

## Architecture Reconstruction

### End-to-end pipeline as implemented

```text
GNEM / Tavily side
------------------
grounding assets + query file
        │
        ├─ generate_gnem_queries.py
        │
        ▼
Tavily search (API) or metadata ingest from XLSX
        │
        ├─ gnem_pipeline.py::tavily_search_rows
        │    - query variants
        │    - metadata scoring
        │    - canonical URLs
        │    - candidate IDs
        │
        ▼
Stage 1 dedupe / ranking
        │
        ▼
Stage 2 resolve local docs or download remote docs
        │
        ▼
Text/profile extraction -> document cards
        │
        ▼
exact duplicate pass + near duplicate pass
        │
        ▼
heuristic score + embedding score + hybrid score
        │
        ▼
classifier rerank + optional LLM judge + credibility
        │
        ▼
review-ready outputs / registries / JSONL / XLSX / optional SQLite
        │
        ▼
publish curated files to evAutomationUpdated/data/tavily ready documents/
        │
        ▼
tavily_ready_documents_manifest.csv

LLM comparison / evaluation side
--------------------------------
local workbook (GNEM updated excel.xlsx)
        │
        ├─ ExcelChunkBuilder -> local chunks
        │
offline Tavily docs folder
        │
        ├─ offline_corpus.py -> parse HTML/PDF/TXT -> document chunks
        │
        ▼
Qdrant index build / reuse
        │
        ├─ local collection
        └─ tavily collection
        │
        ▼
retrieve context by mode
        │
        ├─ no_rag: no retrieval
        ├─ local_rag: workbook only
        └─ hybrid_rag: workbook + offline Tavily folder
        │
        ▼
prompt build
        │
        ▼
model generation
        │
        ▼
citation extraction / provenance split
        │
        ▼
JSONL + XLSX export
        │
        ▼
judge-based metrics and/or post-hoc analysis
```

### Offline Tavily docs vs live Tavily API

Two distinct data paths exist and they are important to keep separate:

1. Live Tavily API path
   - Used by `tavily_ev_automation/tavily_crawler.py` and `tavily_ev_automation/gnem_pipeline.py::tavily_search_rows`.
   - Search happens through `TavilyClient.search(...)` with live network calls (`gnem_pipeline.py:1211-1241`).
   - This path is temporally unstable unless results are cached and versioned.

2. Folder-based offline Tavily path
   - Used by `evAutomationUpdated/src/ev_llm_compare/offline_corpus.py`.
   - The retriever loads documents from `data/tavily` or `data/tavily ready documents` (`offline_corpus.py:56-67`), extracts text from local HTML/PDF/TXT files, then chunks them (`offline_corpus.py:69-145`).
   - Hybrid evaluation in `evAutomationUpdated/eval_runner.py` uses this offline folder, not the Tavily API directly (`eval_runner.py:814-839`).

This separation is good for experiment control, but only if the folder contents are themselves reproducibly tied back to a specific crawl/run. That linkage is incomplete today.

### What the system does well

- The GNEM pipeline is not a toy crawler. It has progressive filtering, document-card construction, exact and near-duplicate removal, source credibility scoring, and exportable document/chunk registries.
- The `eval_runner.py` workflow is close to a proper experiment runner: it enforces one model + one mode per invocation, uses persistent named collections (`local`, `tavily`), and writes append-safe JSONL plus workbook outputs.
- The offline Tavily handoff is explicit: GNEM can publish curated documents into the comparison app’s ready-doc folder via `--publish-ready-docs-dir`.

## Requirements Checklist

| # | Requirement | Status | Evidence | Review note |
|---|---|---|---|---|
| 1 | Supports `no_rag` / `local_rag` / `hybrid_rag` | MET | `evAutomationUpdated/eval_runner.py:154-155`, `:796-839`, `:870-889` | `eval_runner.py` supports all three research modes. |
| 2 | One model+mode per invocation or explicit orchestration | MET | `evAutomationUpdated/eval_runner.py:150-188`, `:855-904` | `eval_runner.py` is single-model/single-mode by design. |
| 3 | Online LLM tested with and without RAG | MET | `evAutomationUpdated/eval_runner.py:154-155`, `:344-369`; `evAutomationUpdated/src/ev_llm_compare/settings.py:117-130` | Gemini is available in RAG and non-RAG conditions. |
| 4 | Prompt policy separation | PARTIAL | Good: `evAutomationUpdated/eval_runner.py:57-80`, `:544-556`; Weak: `evAutomationUpdated/src/ev_llm_compare/prompts.py:8-18`, `:114-149`, `runner.py:95-115` | `eval_runner.py` is clean; `ComparisonRunner` RAG stack still allows general-knowledge fallback through `SYSTEM_PROMPT`. |
| 5 | Strict RAG evidence/citation policy + post-gen validation | MISSING | `evAutomationUpdated/eval_runner.py:67-75`, `:559-586`, `:642-668` | Citations are requested, but not strictly enforced; uncited RAG text can still be treated as grounded. |
| 6 | Reproducibility logging: run_id, prompt, params, retrievals, citations, errors | PARTIAL | `evAutomationUpdated/eval_runner.py:849-954`, `:748-767`, `:344-369` | `run_id`, retrievals, citations, timings, and errors are logged, but full prompt text, system prompt, temperature, max_tokens, and seed are not persisted in the run records. |
| 7 | Metrics support: abstention, unsupported claim proxy, citation-match, RAGAS clarity | PARTIAL | `evAutomationUpdated/src/ev_llm_compare/evaluation.py:17-24`, `:542-564`, `:567-608`; `evAutomationUpdated/analyze_generated_reports.py:218-265`, `:313-328` | Unsupported-claim proxy exists in judge metrics; abstention and citation-match exist only in post-hoc analysis, not in the main experiment runner. |
| 8 | No silent retrieval fallbacks or they are explicit and logged | MISSING | `tavily_ev_automation/gnem_pipeline.py:2765-2787`, `:2927-2933`; `evAutomationUpdated/src/ev_llm_compare/retrieval.py:953-967` | GNEM embedding fallback is visible, but `HybridRetriever` silently falls back to a temp Qdrant path for non-persistent collections. |
| 9 | Deterministic document IDs + canonicalized URLs | PARTIAL | Good: `tavily_ev_automation/gnem_pipeline.py:611-626`, `:1159-1164`; Bad: `tavily_ev_automation/tavily_crawler.py:138`, `:220`, `:371` | GNEM path is deterministic; the simpler crawler path is not. |
| 10 | Dedup covers mirrors, updates, HTML boilerplate | PARTIAL | `tavily_ev_automation/gnem_pipeline.py:2241-2396`; weak merge: `scripts/run_all_and_merge.sh` | GNEM has exact + near dup logic; the simple crawler/merge path only dedupes on raw `URL`. |
| 11 | Source credibility metadata captured | PARTIAL | `tavily_ev_automation/gnem_pipeline.py:1268-1308`; `tavily_ev_automation/gnem_rag_helpers.py:1691-1707`, `:2014-2055` | Domain/title/snippet/file type are captured and scored; publication date is often empty and not reliably populated from source metadata. |
| 12 | Search/crawl caching and stable run ID | MISSING | `tavily_ev_automation/gnem_pipeline.py:1200-1241`; `tavily_ev_automation/tavily_crawler.py:210-248` | No search-result cache or explicit stable crawler run manifest was found. |
| 13 | Retry/backoff/timeouts for external calls | PARTIAL | Downloads: `tavily_ev_automation/tavily_crawler.py:269-330`, `tavily_ev_automation/gnem_pipeline.py:1565-1611`; LLM retries: `tavily_ev_automation/gnem_pipeline.py:726-813` | LLM judge calls retry, but search and document download paths are still one-shot. |
| 14 | Clear mapping from downloaded docs -> chunks -> citations | PARTIAL | `tavily_ev_automation/gnem_pipeline.py:1997-2081`, `:2410-2561`; `evAutomationUpdated/src/ev_llm_compare/offline_corpus.py:111-145`; `evAutomationUpdated/eval_runner.py:525-541`, `:559-586` | There is a visible provenance chain, but it is not unified into one end-to-end run manifest. |
| 15 | API keys from env; no secrets committed | MISSING | `/.env.example:2`; `evAutomationUpdated/.env.example:1-16` | Root `.env.example` contains a real-looking Tavily key string. |
| 16 | Dependencies pinned | PARTIAL | `/requirements.txt:1-8`; `evAutomationUpdated/requirements.txt:1-17`; `evAutomationUpdated/uv.lock` | Subproject has a lockfile, but both requirements files are loose `>=` specs and root has no lockfile. |
| 17 | Minimal tests for parsing/retrieval/evidence guard/citation extraction | PARTIAL | `evAutomationUpdated/tests/test_excel_loader.py`, `test_retrieval.py`, `test_evaluation_export.py`, `test_settings.py` | Some core pieces are tested, but offline corpus parsing, evidence guard enforcement, citation extraction, and crawler robustness are not. |
| 18 | Clear README / run instructions / output locations | PARTIAL | `README.md`, `evAutomationUpdated/README.md` | Both READMEs help, but they do not cleanly unify the two evaluation paths and the root README references folders not present in the ZIP. |

Checklist summary: MET = 3, PARTIAL = 10, MISSING = 5.

## Top Issues

### P0

1. Secret exposure: root `.env.example` contains a real-looking Tavily API key (`.env.example:2`).
2. The repo has not declared one canonical experiment runner, even though `eval_runner.py` and `ComparisonRunner` do not represent the same experiment surface.
3. `ComparisonRunner` RAG prompting is leaky because the shared `SYSTEM_PROMPT` explicitly allows general-knowledge fallback when workbook evidence is absent (`evAutomationUpdated/src/ev_llm_compare/prompts.py:8-18`).
4. `eval_runner.py::split_answer_source_data` marks uncited RAG output as grounded whenever retrieval was non-empty (`evAutomationUpdated/eval_runner.py:642-668`).
5. There is no hard citation validator after generation; `extract_citations` only harvests matching IDs, and the run proceeds even when the answer violates the policy (`evAutomationUpdated/eval_runner.py:559-586`).
6. There is also no support-level validator checking whether cited retrieved text actually supports the claim text; the current system can prove citation presence, not groundedness.
7. Reproducibility logs are incomplete: the main JSONL output does not persist full prompt text or a run-manifest reference, and it does not log workbook hash, offline Tavily manifest hash, git commit hash, embedding model version, or collection/index fingerprint (`evAutomationUpdated/eval_runner.py:915-954`).
8. Retrieval behavior can silently change because non-persistent collections fall back to a temp Qdrant directory when the local path is locked (`evAutomationUpdated/src/ev_llm_compare/retrieval.py:953-967`).
9. The simple crawler uses non-deterministic IDs and raw Python `hash(url)` fallbacks (`tavily_ev_automation/tavily_crawler.py:138`, `:220`, `:371`).
10. Live Tavily search is not cached or versioned, so the collected corpus is not replayable from code alone (`tavily_ev_automation/gnem_pipeline.py:1200-1241`).
11. Search/download robustness is not adequate for a research pipeline because search and document acquisition are still one-shot network operations (`tavily_ev_automation/tavily_crawler.py:269-330`, `tavily_ev_automation/gnem_pipeline.py:1565-1611`).

### P1

1. The repo has two evaluation stacks with overlapping purpose and different semantics; this invites accidental apples-to-oranges comparisons.
2. `evAutomationUpdated/src/ev_llm_compare/excel_loader.py:79-106` deduplicates repeated questions by text, while `eval_runner.py` preserves duplicates with `q_id`. That inconsistency can change the evaluated dataset.
3. `safe_generate_with_metadata` writes `"ERROR: ..."` into answer text on failure (`evAutomationUpdated/src/ev_llm_compare/models.py:194-219`), which can leak into downstream exports if consumers ignore `success=False`.
4. Root documentation references runtime folders not present in the ZIP (`README.md:41-55`), which weakens reproducibility and onboarding.
5. `scripts/run_all_and_merge.sh` dedupes only on raw `URL`, not canonical URL or content identity.
6. The code and README still say “RAGAS” in places, but `evaluation.py` is actually a custom judge-based metric layer, not the `ragas` API.

### P2

1. There is no dedicated standalone index-build CLI; indexing is implicit inside evaluation commands.
2. Output schema, question schema, and run-manifest schema are not documented as stable contracts.
3. The repo could benefit from a root lockfile and a single canonical experiment config format.

## Concrete Defects and Risks

### Verified logic / behavior defects

- Grounding misclassification bug:
  - `evAutomationUpdated/eval_runner.py:642-668`
  - If a RAG answer has retrieved context but no valid citations, the code still labels the response body as `knowledge_source_data`.
  - Research consequence: unsupported claims can be exported as grounded evidence.

- Silent retrieval fallback:
  - `evAutomationUpdated/src/ev_llm_compare/retrieval.py:953-967`
  - If the Qdrant path is locked and the collection was not requested as persistent, the code silently switches to a temp index.
  - Research consequence: two runs with the same command can use different underlying index state.

- Nondeterministic crawler IDs:
  - `tavily_ev_automation/tavily_crawler.py:138`, `:220`, `:371`
  - Sequential `DOC_###` IDs and `hash(url)`-based filename fallbacks are not stable across Python processes and do not survive merges well.

### Evaluation validity gaps

- Mode leakage in one runner:
  - `evAutomationUpdated/src/ev_llm_compare/prompts.py:8-18`
  - The RAG-enabled comparison path still tells the model to answer from general knowledge when workbook evidence is absent.

- Split experiment surface:
  - `evAutomationUpdated/eval_runner.py` is the only place with `hybrid_rag`.
  - `evAutomationUpdated/src/ev_llm_compare/runner.py` is the main documented comparison app, but it only supports `rag_enabled=True/False`.
  - Research consequence: unless one runner is declared canonical, “the experiment” is ambiguous.

- Generated references are not independent gold labels:
  - `evAutomationUpdated/src/ev_llm_compare/runner.py:153-179`
  - When golden answers are missing, the code uses an LLM judge to synthesize reference answers from retrieved context.
  - This is operationally useful, but it is not the same as a human gold standard.

- Citation policy is prompt-level only:
  - `evAutomationUpdated/eval_runner.py:67-75`, `:559-586`
  - The system asks for `[DOC:...]` / `[WEB:...]` citations, but it does not reject or down-rank non-compliant outputs at generation time.

- Support validation is missing:
  - The current pipeline checks for citation tokens, but not whether the cited retrieved evidence actually supports the claim text.
  - Research consequence: “cited” can still mean unsupported or only loosely related.

### Data / metadata risks

- Offline Tavily corpus provenance is only partial:
  - `tavily_ready_documents_manifest.csv` helps, but the exact query/run provenance for every offline file is not carried all the way into the eval JSONL outputs.

- Publication date capture is inconsistent:
  - `tavily_ev_automation/gnem_pipeline.py:1288` initializes `Publication_Date_Metadata` as empty.
  - The pipeline later tries to infer publication date from content (`gnem_rag_helpers.py:1628`), but that is not equivalent to source metadata provenance.

- Simple crawl merge logic is weak:
  - `scripts/run_all_and_merge.sh`
  - Final merge drops duplicates only on `URL`, which misses canonical URL equivalence and cross-site mirrors.

### Performance / operational risks

- GNEM search runs up to 1000 queries with live Tavily calls and no cache.
- Document acquisition uses direct network calls without backoff, increasing variance and failure rates on large runs.

### Checked and not found as a defect

- The example bug “OEM contracts -> Category=OEM” is explicitly guarded against:
  - `evAutomationUpdated/src/ev_llm_compare/retrieval.py:982-1036`
  - `evAutomationUpdated/tests/test_retrieval.py:94-107`
  - This particular routing bug appears to be handled correctly.
  - It should not stay on the active-risk list unless new evidence appears.

## Research Validity Critique

These are the questions a professor or thesis reviewer is likely to raise first:

1. What exactly is the canonical experiment runner?
   - The repo has both `evAutomationUpdated/src/ev_llm_compare/runner.py` and `evAutomationUpdated/eval_runner.py`.
   - They do not represent the same experiment surface.

2. Are the control conditions clean?
   - `eval_runner.py` is reasonably clean.
   - `ComparisonRunner` is not, because its RAG system prompt still permits general knowledge fallback.

3. Are RAG claims actually grounded?
   - Not strictly.
   - The code requests citations, but does not enforce citation coverage or evidence alignment, and uncited RAG text can still be exported as grounded.

4. Can another researcher replay the data collection?
   - Not from code alone.
   - Live Tavily search is uncached, root crawl IDs are not deterministic, and no single crawl run manifest captures all query parameters and returned results.

5. What exactly is frozen for a run?
   - Today the answer is incomplete.
   - A defensible run fingerprint should include at least input workbook hash, offline Tavily manifest hash, git commit hash, resolved model identifier, provider/model tag or quantization where relevant, embedding model version, and collection/index fingerprint.

6. Is the evaluation independent?
   - Only partially.
   - Answer-accuracy fallback references can be LLM-generated if the golden workbook does not cover all questions.

7. Are unsupported claims measured by citation presence or actual support?
   - Currently closer to citation presence and judge-based proxies than to strict support verification.

8. Are “RAGAS” claims accurate?
   - Not really.
   - The current implementation uses custom judge prompts, not the `ragas` Python API, so metric naming/documentation needs to be tightened.

## Missing Evidence / TODOs

These items were not verifiable from the uploaded ZIP and should be treated as missing evidence, not assumptions:

- Exact experiment config files that freeze model versions, temperatures, `top_k`, context budgets, reranker settings, seeds, and question sets per run.
- A committed search-result cache or run manifest for Tavily API responses.
- A stable documented schema for question files beyond code inference.
- A documented stable schema for JSONL/XLSX outputs.
- The grounding workbook named in `README.md:53`, which is required for some GNEM query-generation runs.
- The root `data/corpus/documents` and `data/corpus/text` directories referenced in the README.
- A dedicated index-build command separate from evaluation execution.
- Tests covering offline document parsing, citation extraction/validation, and end-to-end mode isolation.

## Concrete Next Steps To Make It Research-Grade

1. Declare one canonical experiment runner.
   - Recommendation: make `evAutomationUpdated/eval_runner.py` the research runner and either extend `ComparisonRunner` to match it or clearly deprecate it for thesis-quality comparisons.

2. Enforce strict grounding for RAG answers.
   - Reject or mark failed any RAG answer without valid retrieved citations.
   - Add citation-to-retrieval validation and unsupported-claim counters directly in the main runner, not only in post-hoc analysis.
   - Add support-level validation so the pipeline checks whether cited text actually supports the claim, not only whether the claim contains a citation.

3. Make every run replayable.
   - Persist full prompts and system prompts in JSONL or a run-manifest JSON, not necessarily in spreadsheets.
   - Persist prompt hashes and manifest paths in spreadsheet-friendly exports.
   - Persist generation params, seed, retrieval config, manifest fingerprints, input workbook hash, offline Tavily manifest hash, git commit hash, resolved model identifier, provider/model tag or quantization where available, embedding model version, and collection/index fingerprint.
   - Add a search cache and crawl manifest for Tavily results.

4. Remove behavior-changing hidden fallbacks.
   - Fail loudly on temp-Qdrant fallback for research runs.
   - Make embedding fallback an explicit run mode with its own logged flag and separate analysis bucket.

5. Make document provenance deterministic.
   - Replace `hash(url)` and sequential IDs in the simple crawler with canonical URL digests.
   - Carry source-document IDs from GNEM publish manifest through chunking and final citation logs.

6. Strengthen evaluation validity.
   - Keep human gold answers for all scored questions whenever possible.
   - If generated references are used, report them as a separate condition and do not mix them silently with human gold metrics.

7. Make the no-RAG control condition truly isolated.
   - Ensure no-RAG runs do not build or touch retrieval/index infrastructure at all.
   - Treat this as a methodology requirement, not just a runtime optimization.

8. Add missing tests before claiming robustness.
   - Offline HTML/PDF extraction
   - citation extraction and validation
   - support-level evidence checking
   - evidence-guard behavior
   - end-to-end `no_rag` vs `local_rag` vs `hybrid_rag` isolation

## Verification Performed

- `pytest -q Tavily_ev-automation-main/evAutomationUpdated/tests`
- Result: `15 passed, 1 warning`
