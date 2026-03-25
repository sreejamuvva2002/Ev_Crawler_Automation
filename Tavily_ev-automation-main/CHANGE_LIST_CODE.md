# Change List

This is the actionable follow-up list derived from the code review. Priorities reflect research risk, not just engineering convenience.

## P0

| Priority | File / Function | What to change | Why it matters |
|---|---|---|---|
| P0 | `.env.example` | Remove the committed Tavily key, replace with placeholder text, and rotate the key if it was ever real. | Secret exposure is an immediate security issue. |
| P0 | `evAutomationUpdated/README.md`, `evAutomationUpdated/main.py`, `evAutomationUpdated/eval_runner.py`, `evAutomationUpdated/src/ev_llm_compare/cli.py` | Declare the canonical research runner. If `eval_runner.py` is canonical, document that explicitly and treat feature-parity work on `ComparisonRunner` as secondary. | Otherwise time can be wasted improving the wrong experiment surface. |
| P0 | `evAutomationUpdated/src/ev_llm_compare/prompts.py::SYSTEM_PROMPT` and `runner.py` RAG branch | Remove general-knowledge fallback from RAG runs, or split into explicit `context_only` and `context_plus_knowledge` conditions. | Otherwise RAG results leak model prior knowledge. |
| P0 | `evAutomationUpdated/eval_runner.py::split_answer_source_data` | Treat uncited RAG content as unsupported/pretrained, or fail the row outright. | Current behavior can label unsupported RAG claims as grounded. |
| P0 | `evAutomationUpdated/eval_runner.py::extract_citations` plus call site after generation | Add strict citation validation: every factual RAG bullet must cite retrieved IDs, and invalid outputs should be marked failed. | Citation presence is currently requested but not enforced. |
| P0 | `evAutomationUpdated/eval_runner.py` post-generation validation path | Add support-level validation, not just citation validation: check whether the cited retrieved text actually supports the claim text, and record support failures separately from citation failures. | Citation presence alone does not establish groundedness. |
| P0 | `evAutomationUpdated/src/ev_llm_compare/retrieval.py::_create_client` | Remove silent temp-Qdrant fallback for research runs; fail loudly or require an explicit override flag. | Silent index fallback breaks reproducibility. |
| P0 | `evAutomationUpdated/eval_runner.py` record-building path plus new run-manifest writer | Persist full prompt text and system prompt in JSONL or a run-manifest JSON, and log prompt hash + manifest path in XLSX-friendly exports. Also log workbook hash, offline Tavily manifest hash, git commit hash, resolved model identifier, provider/model tag or quantization when available, embedding model version, and collection/index fingerprint. | Reproducibility is incomplete without exact run configuration and corpus/index fingerprints. |
| P0 | `tavily_ev_automation/tavily_crawler.py::url_to_filename`, `run_search`, `download_documents` | Replace `hash(url)` and sequential `DOC_###` IDs with canonical URL digests / stable IDs. | Crawler outputs are currently nondeterministic. |
| P0 | `tavily_ev_automation/gnem_pipeline.py::tavily_search_rows` and `tavily_ev_automation/tavily_crawler.py::run_search` | Add search-result caching and write a run manifest with query text, query mode, timestamp, and raw Tavily response. | Live Tavily collection is not replayable today. |
| P0 | `tavily_ev_automation/tavily_crawler.py::download_url`, `tavily_ev_automation/gnem_pipeline.py::download_document`, search call sites | Add retries, exponential backoff, and clearer transient-vs-permanent error logging. | One-shot network calls make the corpus unstable. |

## P1

| Priority | File / Function | What to change | Why it matters |
|---|---|---|---|
| P1 | `scripts/run_all_and_merge.sh` | Canonicalize URLs before merge dedupe, and optionally dedupe by content hash when files exist. | Raw-URL dedupe misses mirrors and tracking-param variants. |
| P1 | `evAutomationUpdated/src/ev_llm_compare/excel_loader.py::load_questions` | Decide whether duplicate question texts are allowed; if yes, preserve them with IDs. Align behavior with `eval_runner.py`. | Inconsistent question handling changes the evaluated dataset. |
| P1 | `evAutomationUpdated/src/ev_llm_compare/models.py::safe_generate_with_metadata` | Stop writing `"ERROR: ..."` as the answer text, or ensure downstream exporters always blank failed answers. | Failure strings can contaminate reports. |
| P1 | `evAutomationUpdated/src/ev_llm_compare/evaluation.py` | Add abstention rate and citation-to-retrieval match directly into the main metric export path. | Important structural metrics exist only in post-hoc analysis today. |
| P1 | `evAutomationUpdated/src/ev_llm_compare/evaluation.py`, `README.md`, `pyproject.toml` | Rename “RAGAS” references to “judge-based metrics”, or actually integrate the `ragas` API. | Terminology mismatch will be challenged in review. |
| P1 | `tavily_ev_automation/gnem_pipeline.py::publish_curated_documents_to_ready_dir`, `offline_corpus.py`, `eval_runner.py` | Carry a stable document provenance ID from ready-doc manifest to chunk metadata and final citations. | End-to-end source traceability should be explicit, not inferred. |
| P1 | `evAutomationUpdated/tests/` | Add tests for offline HTML/PDF parsing, citation extraction, citation validation, and evidence-guard behavior. | These are critical research behaviors but currently untested. |
| P1 | `tavily_ev_automation/gnem_pipeline.py` | Persist crawl/search run IDs and query-plan metadata into final registries and manifests. | Makes curated corpora auditable. |
| P1 | `evAutomationUpdated/eval_runner.py`, `evAutomationUpdated/src/ev_llm_compare/runner.py` | Make true `no_rag` runs skip retrieval/index creation entirely and confirm that no retrieval artifacts are touched for the control condition. | This is a methodology and isolation issue, not just an efficiency improvement. |
| P1 | `evAutomationUpdated/src/ev_llm_compare/settings.py`, `runner.py`, `cli.py` | Only add explicit `hybrid_rag` support to the comparison app after the canonical-runner decision is made; otherwise document why it remains non-canonical. | This prevents premature feature work on a possibly non-canonical surface. |

## P2

| Priority | File / Function | What to change | Why it matters |
|---|---|---|---|
| P2 | `README.md`, `evAutomationUpdated/README.md` | Consolidate the two workflows into one canonical runbook and remove references to absent directories. | Reduces onboarding ambiguity. |
| P2 | repo root packaging | Add a root lockfile or unified environment management strategy. | Simplifies environment reproducibility. |
| P2 | output schema docs | Document stable schemas for question files, JSONL outputs, retrieval sheets, and manifests. | Helps future analysis and replication. |
| P2 | indexing UX | Add a dedicated index-build CLI instead of implicit index creation during evaluation. | Makes experiment setup cleaner and more explicit. |
