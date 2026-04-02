# Web Crawler And Filtering Report

Date: 2026-03-25

Scope: the Tavily and GNEM-side data acquisition, filtering, deduplication, curation, and offline-document publishing workflow in:

- `tavily_ev_automation/tavily_crawler.py`
- `tavily_ev_automation/generate_gnem_queries.py`
- `tavily_ev_automation/gnem_pipeline.py`
- `tavily_ev_automation/gnem_rag_helpers.py`
- `scripts/run_georgia_ev_battery_suppliers.sh`
- `scripts/run_all_and_merge.sh`

Companion repo-wide review documents:

- `REPORT_CODE_REVIEW.md`
- `CHANGE_LIST_CODE.md`
- `RUNBOOK.md`

## 1. Executive Summary

This subsystem is the document acquisition and curation side of the project. Its job is to start from GNEM/EV-supply-chain questions or search queries, retrieve candidate web documents, score them against the research topic, remove duplicates, classify which ones are useful, and publish the best curated files into an offline folder that the LLM evaluation pipeline can later use for `hybrid_rag`.

The important architectural point is this:

- `tavily_crawler.py` is a simple single-query search-and-download utility.
- `gnem_pipeline.py` is the real multi-stage research curation pipeline.

If you are explaining this to your professor, the correct statement is:

`gnem_pipeline.py` is the main dataset-building workflow; `tavily_crawler.py` is a lightweight helper, not the final curation engine.

## 2. What This System Is Trying To Do

The crawler/filtering side is trying to solve a real research problem:

1. Search the web for EV-supply-chain-relevant documents.
2. Avoid downloading and indexing junk.
3. Prefer documents that are relevant to Georgia/Southeast EV and battery supply-chain research.
4. Remove exact duplicates and near-duplicates.
5. Keep source and provenance metadata.
6. Produce a curated offline document set that can later be used in `hybrid_rag`.

This is a data curation problem, not just a search problem.

## 3. Main Code Paths

### 3.1 Query generation

`tavily_ev_automation/generate_gnem_queries.py`

Purpose:

- Reads grounding assets such as the automotive workbook, Georgia counties GeoJSON, and the GNEM supply-chain document.
- Builds a large query set tailored to the EV supply-chain domain.

Key functions:

- `load_grounding_context(...)`
- `build_topics(...)`
- `generate_queries(...)`
- `main()`

What it contributes:

- A structured query file such as `data/queries/queries_1000.txt`.
- Domain specificity before any web search starts.

### 3.2 Simple crawler

`tavily_ev_automation/tavily_crawler.py`

Purpose:

- Runs live Tavily search for one query.
- Optionally downloads returned documents.
- Exports rows to Excel.

Key functions:

- `run_search(...)`
- `download_url(...)`
- `download_documents(...)`
- `export_to_excel(...)`
- `main()`

What it is good for:

- Quick ad hoc search.
- One-query exploration.
- Fast manual inspection.

What it is not:

- It is not the most rigorous end-to-end research curation path in the repo.

### 3.3 Main GNEM filtering pipeline

`tavily_ev_automation/gnem_pipeline.py`

Purpose:

- Ingest live Tavily search results or replay metadata from an XLSX file.
- Score candidate documents.
- Resolve local paths or download files.
- Build document cards.
- Deduplicate.
- Rerank.
- Optionally use an LLM judge.
- Score credibility.
- Export curated and rejected outputs.
- Publish curated ready-docs into the offline Tavily folder for the LLM pipeline.

Key entrypoints and functions:

- `tavily_search_rows(...)`
- `metadata_rows_from_excel(...)`
- `download_document(...)`
- `dedupe_by_url_best_score(...)`
- `apply_exact_duplicate_pass(...)`
- `apply_near_duplicate_pass(...)`
- `publish_curated_documents_to_ready_dir(...)`
- `document_registry_dataframe(...)`
- `chunk_registry_dataframe(...)`
- `run_rag_filtering_pipeline(...)`
- `main()`

### 3.4 Supporting scoring and extraction helpers

`tavily_ev_automation/gnem_rag_helpers.py`

This file is the support engine behind `gnem_pipeline.py`. It is the module that converts raw downloaded documents into structured research evidence.

Purpose of this file:

- tell the pipeline what counts as EV-supply-chain evidence
- extract the useful text from raw files
- summarize the important parts of each document
- build a structured document card for scoring
- score usefulness, groundedness, and credibility
- support the final keep/review/discard decision

What each helper family is used for:

- Grounding dictionary builders:
  load known companies, counties, OEMs, ports, value-chain terms, and other domain vocabulary so the pipeline can recognize research-relevant evidence instead of treating every word equally.

- Local document and text indexing:
  check whether a document already exists locally before downloading it again, and map metadata rows to existing files or extracted text files.

- Content extraction:
  read PDF, HTML, TXT, and MD files and pull out the main body text, headings, publication date, sampled segments, and page-level entries.

- Chunk scoring:
  split a long document into smaller candidate sections and score which chunks are most relevant to the GNEM/EV question space.

- Document-card construction:
  build one structured record per document with metadata summary, first-page summary, first-two-pages summary, sampled-page summaries, headings, top relevant chunks, extracted entities, and page references.

- Heuristic and classifier-style scoring:
  estimate whether the document is direct-use evidence, adjacent background, research-only material, generic news, or marketing noise.

- Credibility scoring:
  judge trustworthiness using source quality, evidence depth, grounded entities, document type, date evidence, and noise penalties.

- Diversity constraints:
  stop the final selected set from being dominated by only one subtopic or one type of evidence.

How `build_grounding_dictionaries(...)` actually builds the grounding layer:

- It reads the automotive workbook first. From that file it pulls:
  - `Company` -> known company names
  - `Primary Facility Type` -> plant/facility categories
  - `EV Supply Chain Role` -> supplier-role vocabulary
  - `Product / Service` -> product and process terms
  - `Primary OEMs` -> OEM names, split from multi-value cells
  - `Location` -> county names when a `... County` pattern appears

- It then reads the Georgia counties GeoJSON. From that file it pulls:
  - county names from fields such as `NAMELSAD10` or `NAME10`
  - county-to-region mappings from `Reg_Comm`

- It reads the GNEM supply-chain DOCX and keeps a short reference excerpt. This gives the pipeline a compact policy/localization and strategic-reference text.

- It adds built-in EV/logistics vocabulary from constants already defined in code, especially:
  - port terms
  - value-chain keywords

- It normalizes and deduplicates all of those values, then builds alias maps. For example:
  - company aliases remove suffixes like `Inc`, `LLC`, `Corp`, and `Ltd`
  - county aliases can match both `Bartow County` and `Bartow`

- It then creates two kinds of synthetic reference text:
  - `facet_texts`: four focused grounding descriptions for
    - who/what/where/scale
    - supplier/tier relationships
    - logistics/infrastructure risk
    - policy/incentives/localization
  - `global_reference`: one larger combined reference text built from the golden summary, DOCX excerpt, and sampled entities such as companies, counties, OEMs, ports, facility types, roles, and value-chain terms

Why this is useful:

- The pipeline now has a controlled domain vocabulary before it scores anything.
- Later stages can check whether a candidate document mentions real Georgia EV entities, real supply-chain roles, real counties, or real logistics/policy concepts.
- This improves keyword matching, entity extraction, chunk ranking, and document classification, because the system is comparing documents against a research-specific grounding set instead of only using generic text similarity.

## 4. End-To-End Data Flow

The actual implemented flow is:

```text
grounding files
  -> generate GNEM queries
  -> live Tavily search or XLSX metadata replay
  -> metadata scoring and URL canonicalization
  -> Stage 1 dedupe
  -> Stage 2 local resolve or remote download
  -> Stage 3 lightweight document cards
  -> Stage 4 heuristic + embedding + hybrid scoring
  -> Stage 5 classifier reranking
  -> Stage 6 enriched document cards for shortlist rows
  -> exact duplicate pass + near duplicate pass
  -> Stage 7 optional LLM judge
  -> Stage 8 credibility scoring
  -> final keep/review/discard decision
  -> registry + review exports
  -> publish curated ready-docs to evAutomationUpdated/data/tavily ready documents
```

## 5. What Happens In Each Stage

### Stage 0: Grounding

Inside `run_rag_filtering_pipeline(...)`, the pipeline first loads grounding assets through `build_grounding_dictionaries(...)`.

Why this matters:

- The crawler is not searching blindly.
- It uses domain context such as company names, counties, OEMs, and EV terminology.

How Stage 0 grounding is built:

- From the automotive workbook, it extracts known companies, facility types, EV supply-chain roles, products/services, OEM names, and county mentions from location fields.
- From the counties GeoJSON, it adds county names and county-to-region mappings.
- From the GNEM DOCX, it reads a short reference text that captures the intended policy/localization framing.
- From built-in constants, it adds port names and EV value-chain keyword lists.
- It normalizes these values, removes duplicates, and creates alias forms so the system can match real-world variations like `SK On`, `SK On Co.`, or county names with and without the word `County`.
- It packages the result into:
  - raw entity lists
  - alias dictionaries
  - a combined global reference text
  - four facet-specific grounding texts

What the pipeline uses grounding for later:

- early metadata relevance scoring
- extracting entities from downloaded documents
- chunk-level relevance scoring
- document-card scoring and usefulness classification
- credibility and final keep/review/discard decisions

### Stage 1: Search and metadata scoring

Search comes either from:

- live Tavily API calls through `tavily_search_rows(...)`, or
- replayed metadata through `metadata_rows_from_excel(...)`.

At this point the system:

- canonicalizes URLs via `canonicalize_url(...)`
- builds stable candidate IDs via `stable_id_from_url(...)`
- calculates metadata relevance features
- keeps query and source metadata

This is already better than a naive crawler because it does early rejection before expensive document processing.

#### How Stage 1 scoring against the research topic works

This score is computed in `gnem_pipeline.py::metadata_score_for_candidate(...)`.

The pipeline builds one combined candidate text from:

- title
- content snippet
- description
- query
- URL

It then scores that text against the GNEM/EV research topic using these signal families:

- category hits
- EV value-chain hits
- Southeast/Georgia region hits
- Georgia logistics/manufacturing node hits
- policy and logistics hits
- question-coverage hits
- specificity hits
- document-quality hits
- lexical similarity to the GNEM golden summary
- source-domain credibility

The weighted metadata score is:

```text
0.12 * Tavily score
+ 0.24 * category score
+ 0.12 * value-chain score
+ 0.12 * region score
+ 0.08 * Georgia-node score
+ 0.08 * policy/logistics score
+ 0.08 * question score
+ 0.06 * specificity score
+ 0.06 * document-quality score
+ 0.10 * lexical similarity
+ source credibility
+ rule bonus
- penalties
```

Important rule boosts:

- `two_signal_rule`: triggered when at least two category groups are hit
- `question_specifics_rule`: triggered when the candidate both covers a target question and includes specific details

Important penalties:

- negative/noisy keywords
- national/global documents that do not connect back to Georgia or the Southeast

Plain-English meaning:

- a document scores well if it is specifically about the EV supply chain, tied to Georgia/Southeast, contains concrete details, and looks similar to the research topic summary
- a document scores badly if it is vague, generic, noisy, or not grounded in the target geography/use case

### Stage 2: Document acquisition

The pipeline tries local files first, then downloads when needed.

Functions involved:

- `resolve_document_paths(...)`
- `download_document(...)`
- `download_pdf(...)`

What happens:

- If a local file is already available, it is reused.
- Otherwise the document is downloaded into `stage2_downloads`.
- Acquisition status, size, content type, and path are logged.

### Stage 3: Lightweight document cards

For candidate documents that pass the metadata gate, `build_document_card(...)` creates a structured summary.

This includes:

- extracted text
- top chunks
- source metadata
- initial relevance evidence

This is the point where raw downloaded files become structured research objects.

#### How extraction, “useful text,” and summarization work

The extraction and summarization logic is mostly in:

- `build_document_content_profile(...)`
- `extract_html_content(...)`
- `extract_pdf_page_entries(...)`
- `sample_text_entries(...)`
- `sequential_text_entries(...)`
- `summarize_first_page(...)`
- `summarize_entry_set(...)`
- `build_document_card(...)`

##### What “pulling out the useful text” means

It does not mean taking the entire raw file blindly.

For HTML documents, the pipeline tries to keep the main article/body content and avoid boilerplate such as:

- navigation bars
- headers
- footers
- sidebars
- social/share blocks
- cookie banners
- menus
- scripts/styles/noscript sections

With BeautifulSoup available, the code explicitly searches for likely content containers such as:

- `article`
- `main`
- elements whose class/id suggests `content`, `story`, `post`, `entry`, `body`, `report`, or `rich-text`

Each HTML candidate block is scored using:

- text length
- paragraph count
- heading count
- punctuation density
- low link density
- whether the tag is `article` or `main`
- whether the class/id looks like real content

Then the best-scoring body block is selected.

For PDFs, “useful text” means extracted page text from pages that are likely to contain the substance of the document, not just the filename or metadata.

For TXT/MD/local text files, the pipeline reads and normalizes the text directly.

##### How much text is extracted

The extraction budget is controlled by `max_text_chars`.

The document-card pipeline uses that budget differently depending on the card level:

- lightweight card:
  usually uses sampled content only
- enriched card:
  uses a larger extraction scope and more chunks

For PDFs:

- lightweight mode samples key pages through `sample_page_indices(...)`
- those sample pages are:
  - page 1
  - page 2
  - a middle page
  - the last page
- enriched mode can extract sequentially across all pages until the character budget is exhausted

For text/HTML:

- `first_page_text` is the first `2500` characters
- `first_two_pages_text` is the first `5000` characters
- sampled text segments use roughly `2200` characters each
- enriched sequential segments use up to `12` segments of roughly `2200` characters each

##### How summarization works

The pipeline does not use one generic LLM summary at this stage.

Instead, it uses rule-based extractive summarization functions that keep the most relevant sentences.

`summarize_first_page(...)`:

- splits text into sentences
- scores sentences using:
  - EV/value-chain keywords
  - Georgia/Southeast terms
  - policy/logistics terms
  - numbers
  - specificity signals
- always keeps the first sentence
- then adds the best-scoring sentences
- default summary size:
  - up to `4` sentences
  - up to `750` characters

For first-two-pages summaries, the card builder uses:

- up to `5` sentences
- up to `950` characters

`summarize_entry_set(...)`:

- summarizes up to `4` sampled entries
- each entry summary uses roughly:
  - up to `2` sentences
  - up to `240` characters
- then joins those into one compact sampled-page summary

##### What the document card contains after summarization

`build_document_card(...)` produces a structured record with fields such as:

- metadata summary
- first-page summary
- first-two-pages summary
- sampled-page summaries
- headings / table-of-contents style clues
- top relevant chunks
- top evidence snippets
- evidence page numbers
- extracted companies, counties, OEMs, ports, facilities, capacities, dates, and value-chain terms

So the document card is the pipeline’s compact research representation of one document.

### Stage 4: Heuristic, embedding, and hybrid scoring

For each document card, `score_document_card(...)` computes:

- heuristic score
- embedding score
- hybrid score

This is one of the most important parts of the pipeline because it combines:

- symbolic/domain features
- text similarity
- research-topic alignment

#### How Stage 4 scoring works

The detailed logic is in `gnem_rag_helpers.py::score_document_card(...)`.

After the document is downloaded and text is extracted, the pipeline scores the document card in three main ways.

##### 1. Semantic embedding score

The system compares the document-card text against grounding texts and facet texts.

It uses:

- global similarity to the overall grounding summary
- query similarity to the current search query
- facet-specific similarities
- facet coverage

The semantic embedding score is:

```text
0.30 * global similarity
+ 0.25 * top-2 facet average
+ 0.20 * facet coverage
+ 0.25 * query similarity
```

##### What "relevant to the EV supply chain" means in code

The pipeline does not manually read the document and say "this looks relevant." It converts the problem into text-similarity scoring.

First, the document is split into chunks. A chunk is just a small text window, usually around 180 words with overlap between neighboring chunks.

Then each chunk is compared against three kinds of reference text:

- the `global_reference`, which represents the overall Georgia EV battery supply-chain study
- the `facet_texts`, which represent specific subtopics such as supplier relationships, logistics risk, or policy/localization
- the current query text, which represents what this particular search result was supposed to answer

How "matching" works:

- If dense embeddings are available, the code embeds the chunk and the reference texts and uses cosine similarity.
- If embeddings are not available, it falls back to a hashed bag-of-words similarity method.
- In both cases, a chunk gets a higher score when its language is closer to the reference texts in meaning or token-pattern overlap.

So when the report says a chunk is "relevant to the EV supply chain," it means the chunk contains language that is similar to the project grounding texts. In practice, that usually means the chunk mentions combinations such as:

- real companies or OEMs
- counties or Georgia/Southeast locations
- facilities, plants, ports, rail, logistics, freight
- products, materials, capacity numbers, dates, supplier roles, or offtake/joint-venture relationships

For example, a chunk like:

`SK On battery plant in Bartow County will supply EV battery modules through a multi-tier supplier network and relies on Savannah port logistics`

will likely score well because it overlaps with:

- the overall EV battery supply-chain scope
- the who/where/scale facet
- the supplier/tiering facet
- the logistics-risk facet

By contrast, a chunk like:

`Company shares rose after quarterly earnings beat analyst expectations`

will score poorly because it may mention a company, but it does not meaningfully overlap with the project grounding for supply-chain structure, facilities, logistics, regional context, or policy.

##### What "matches the specific query" means

The query is the exact search instruction that brought this candidate into the pipeline. So query matching asks:

"Does this chunk actually talk about the thing we searched for?"

If the query is about Georgia cathode suppliers, then chunks mentioning Georgia, cathodes, suppliers, facilities, production, or related customer links will score higher on `query_similarity`.

If the chunk is only broadly about EVs, but not about the specific searched topic, then its query score will be lower even if it is somewhat relevant to the overall study.

##### Why the pipeline uses both global and query matching

- `global_similarity` keeps the scoring aligned with the overall thesis topic
- `facet_texts` stop the system from missing narrow but important evidence such as logistics or policy
- `query_similarity` checks whether the chunk is actually answering the current search intent

That is why the pipeline can say a chunk is useful: not because of a human judgment at that stage, but because it is quantitatively more similar to the study grounding and the query than other chunks are.

##### How `global_similarity` and `query_similarity` are actually computed

These scores are created inside `score_texts_against_grounding(...)`.

Step 1:

- normalize the candidate text
- normalize the `global_reference`
- normalize the query text if a query exists

Step 2:

- compare the candidate text to `global_reference` to get `global_similarity`
- compare the candidate text to the query text to get `query_similarity`

There are two possible backends.

If embeddings are available:

- the code embeds the candidate text and the reference texts
- then it uses cosine similarity

Formula:

```text
cosine_similarity(a, b) = dot(a, b) / (||a|| * ||b||)
```

The score is then converted to the pipeline scale:

```text
global_similarity = 100 * cosine_similarity(chunk_embedding, global_reference_embedding)
query_similarity = 100 * cosine_similarity(chunk_embedding, query_embedding)
```

What this means in simple terms:

- the embedding model turns each text into a long list of numbers called a vector
- one vector is created for the chunk
- one vector is created for the overall study reference text
- one vector is created for the current query text

Those vectors are then compared by direction, not by raw length.

Why cosine similarity is used:

- the `dot(a, b)` part checks how much the two vectors point in the same direction
- the `||a|| * ||b||` part normalizes for size, so a longer text does not automatically get a larger score just because it has more words
- the final value is a similarity score between 0 and 1 in this implementation, and then the pipeline multiplies it by 100

So:

- if two texts are very similar in meaning, their vectors point in a similar direction, cosine similarity is close to `1`, and the pipeline score is close to `100`
- if two texts are weakly related, cosine similarity is closer to `0`, and the pipeline score is low

Important interpretation:

- this is not a probability
- it does not mean "95% correct"
- it only means "this text is very similar to that reference text under the embedding model"

Plain-English reading of each score:

- `global_similarity`: how much this chunk looks like the overall EV battery supply-chain study description
- `query_similarity`: how much this chunk looks like the exact search question or query that retrieved the document

Simple intuition example:

- if the query is about `Georgia cathode suppliers and battery plant capacity`
- and the chunk says `a Georgia battery materials supplier will expand cathode production capacity for EV manufacturing`
- then the chunk embedding and query embedding should point in a similar direction, so `query_similarity` should be high

- if another chunk says `the company reported quarterly earnings and market reactions`
- then even if it mentions the same company, its vector will be less aligned with the EV-supply-chain query, so `query_similarity` should be much lower

You can think of it like this:

- embeddings convert text into coordinates in a semantic space
- cosine similarity measures how close the meanings are by checking the angle between those coordinates
- the pipeline uses that angle-based closeness as a relevance score

If embeddings are not available, the code falls back to a hashed bag-of-words similarity:

- tokenize the text
- include both single-word tokens and adjacent two-word phrases
- hash them into a fixed-size vector
- compare those vectors with cosine similarity

So the fallback is still a cosine-style similarity, but over hashed token-count vectors instead of dense embeddings.

Fallback formula:

```text
hashed_similarity(text_a, text_b)
= dot(counter_a, counter_b) / (||counter_a|| * ||counter_b||)

global_similarity = 100 * hashed_similarity(chunk_text, global_reference)
query_similarity = 100 * hashed_similarity(chunk_text, query_text)
```

Important detail:

- if no query text is present, the fallback code uses `global_similarity` as the query score
- later chunk-scoring code also defaults missing `query_similarity` to `global_similarity`

So in plain English:

- `global_similarity` asks: how close is this chunk to the overall study definition?
- `query_similarity` asks: how close is this chunk to the exact thing we searched for?

Higher similarity means higher overlap in meaning or token-pattern structure with those reference texts.

##### Chunk combined score

At chunk level, the ranking formula is:

```text
combined_score
= 0.30 * global_similarity
+ 0.30 * best_facet_score
+ 0.25 * query_similarity
+ 0.15 * top2_facet_average
```

Meaning:

- `global_similarity`: broad relevance to the full study
- `best_facet_score`: strong relevance to at least one important subtopic
- `query_similarity`: relevance to the specific search/query
- `top2_facet_average`: bonus if the chunk is useful across more than one facet

So a top chunk is not just "text from the document." It is the part of the document that best matches the study scope, the subtopic structure, and the specific query that retrieved it.

##### 2. Lexical/entity score

This is computed in `lexical_entity_score(...)`.

It rewards documents that name concrete and grounded entities such as:

- companies
- counties
- OEMs
- ports
- facilities
- capacities
- dates
- value-chain terms
- supplier/customer/joint-venture relationships
- logistics terms
- policy terms

This matters because a useful research document usually names real actors, places, facilities, or capacities rather than speaking only in broad generic language.

##### 3. Hybrid score

The hybrid score combines semantic and lexical/entity evidence:

```text
Hybrid_Score
= 0.55 * semantic_embedding_score
+ 0.45 * lexical_entity_score
```

So a document is strongest when it is both:

- semantically aligned with the EV/Georgia research topic
- grounded in concrete entities and evidence

##### 4. Heuristic score

The heuristic score is a broader evidence-quality score. It combines:

- signal/category count
- question coverage
- grounded entity strength
- top relevant chunk score
- region score
- source credibility
- evidence depth
- late-page evidence
- metadata score
- document-quality score
- penalties for negative, marketing, generic-news, and research-only noise

This score is intended to answer:

Does this document contain enough grounded, specific, evidence-rich material to be useful for the research question?

### Stage 5: Classifier-style reranking

`classify_document_card(...)` produces richer classification signals and a rerank score.

The shortlist logic then uses thresholds such as:

- `heuristic_threshold`
- `hybrid_threshold`
- `direct_usecase_threshold`

This means the final shortlist is not based on one score alone.

#### How the pipeline classifies which documents are useful

The classification logic is in `gnem_rag_helpers.py::classify_document_card(...)`.

The system does not simply say relevant or irrelevant. It tries to separate documents into usefulness classes:

- direct use-case evidence
- adjacent background
- research-only material
- generic news
- marketing noise

The most important derived score is `Direct_Usecase_Score`.

This score goes up when the document has:

- grounded entities
- strong hybrid relevance
- strong semantic alignment
- strong rerank strength
- strong lexical/entity evidence
- clear question coverage
- supplier/customer relationship terms
- logistics signals
- policy/localization signals
- Georgia/Southeast regional signals

It goes down when the document looks like:

- generic news
- marketing/promotional content
- abstract research content with weak grounding

The pipeline also computes:

- `Adjacent_Background_Score`
- `Research_Only_Score`
- `Generic_News_Score`
- `Marketing_Noise_Score`

Then it creates a final `Rerank_Score`, which combines:

- heuristic score
- hybrid score
- embedding rerank score
- semantic embedding score
- direct-usecase evidence
- penalties for generic news and marketing noise

Plain-English meaning:

- the pipeline is trying to decide not only whether the document is relevant, but whether it is directly useful for the GNEM research question, only indirectly useful, or mostly noise

### Stage 6: Enriched cards

Shortlist-seed rows are reprocessed with a larger text budget.

Why this exists:

- The pipeline keeps Stage 3 relatively cheap.
- Only stronger candidates get the more expensive enriched extraction path.

### Exact and near duplicate passes

After enrichment, the pipeline runs two duplicate filters:

- `apply_exact_duplicate_pass(...)`
- `apply_near_duplicate_pass(...)`

Important internals:

- exact duplicate logic uses hashes and normalized content
- near duplicate logic uses `simhash_signature(...)` and text similarity

This is a real strength of the repo. It means the system is trying to remove both:

- identical mirrors
- almost-the-same documents with small formatting differences

#### How exact duplicate removal works

This logic is in `gnem_pipeline.py::apply_exact_duplicate_pass(...)`.

For each document row, the pipeline tries to create an exact-duplicate key in this order:

1. file bytes SHA256
2. extracted text SHA256
3. canonical URL

This means the system prefers to group duplicates by:

- identical file contents first
- identical extracted text second
- same normalized URL third

Within each duplicate group, it chooses one master version using `duplicate_rank_key(...)`, which favors the strongest version based on document quality and ranking signals.

All non-master exact duplicates are marked and removed from shortlist progression.

#### How near duplicate removal works

This logic is in `gnem_pipeline.py::apply_near_duplicate_pass(...)`.

The pipeline:

- normalizes the comparison text
- removes HTML when necessary
- computes a 64-bit SimHash signature
- compares documents using:
  - Hamming distance between SimHash signatures
  - cosine similarity between document texts
  - cosine similarity between titles
  - document length ratio

The goal is to catch:

- mirrors
- lightly edited copies
- reformatted versions of the same report
- pages that are not byte-identical but are still substantively the same document

Again, one master is kept and the others are suppressed from the shortlist.

### Stage 7: Optional LLM judge

If LLM judging is enabled, shortlist documents are passed through `llm_document_judge(...)`.

Purpose:

- add another relevance check
- improve final keep/review/discard decisions

Important caveat:

- this is optional
- it is not the main deterministic scoring path

### Stage 8: Credibility scoring

`assess_document_credibility(...)` estimates source credibility.

Why this matters:

- research quality is not only about relevance
- source trustworthiness also matters

#### How credibility scoring works

The credibility logic is in `gnem_rag_helpers.py::assess_document_credibility(...)`.

The score combines:

- source-domain quality
- evidence depth
- grounded entity strength
- publication-date evidence
- document-type quality
- optional LLM-quality signals
- penalties for generic-news and marketing behavior

So credibility is not just “is this from a well-known site?”

It is:

- who published it
- how much usable evidence it contains
- whether it names specific grounded entities
- whether it looks like a serious document type
- whether it behaves like a noisy marketing/news page

### Final decision and export

The pipeline then:

- assigns `keep`, `review`, or `discard`
- applies diversity control with `apply_diversity_pass(...)`
- copies curated local documents into `final_docs`
- writes review-ready and rejected outputs
- writes document and chunk registries
- optionally writes SQLite
- optionally publishes curated files to `evAutomationUpdated/data/tavily ready documents`

#### How the pipeline selects final documents

The final rule set is in `gnem_rag_helpers.py::final_decision_reason(...)`.

A document is not selected because of one score alone. The final decision depends on:

- whether it passed the shortlist
- whether the LLM judge passed, if LLM mode is enabled
- credibility score
- whether the source file was actually preserved locally
- whether the domain is blocked or low trust
- direct-usecase score
- adjacent-background score
- hybrid score
- heuristic score
- rerank score
- generic-news score
- marketing-noise score

The practical rules are:

- `discard` if the source file is missing
- `discard` if the source domain is blocked
- `discard` if the source is low trust and still fails the stronger credibility/noise bar
- `keep` if it passed shortlist, passed the LLM judge, and clears the keep-level credibility threshold
- `review` if it passed shortlist but still has some weaker non-fatal condition
- `review` if it did not fully pass shortlist but still has enough evidence/ranking strength to merit manual inspection
- `discard` if it has insufficient grounded supply-chain evidence or too much generic/news/marketing noise

The pipeline also applies a diversity pass:

- if too many `keep` documents cluster in one subtopic, some are moved to `review`

That prevents the final kept set from being dominated by only one type of document.

#### How curated files are published

The publishing logic is in `gnem_pipeline.py::publish_curated_documents_to_ready_dir(...)`.

Only documents with:

- `Final_Decision = keep`, or
- `Final_Decision = review`

are published into the ready-doc folder.

They are copied into:

- `keep/`
- `review/`

inside the offline ready-doc directory.

At the same time the pipeline writes:

- `tavily_ready_documents_manifest.csv`
- `tavily_ready_documents_unmatched.csv`

So the offline folder used later by `hybrid_rag` is not a raw dump of everything found on the web. It is the output of the full GNEM scoring, deduplication, classification, and final-decision pipeline.

## 6. Outputs Produced By The Crawler / Filtering System

The important outputs are:

- `review_ready_documents.xlsx`
- `review_ready_documents.csv`
- `review_ready_documents.jsonl`
- `rejected_documents.xlsx`
- `curated_documents.jsonl`
- `document_registry.xlsx`
- `chunk_registry.xlsx`
- `rag_data_management_registry.xlsx`
- `pipeline_report.json`
- `final_docs/`
- published ready-docs in `evAutomationUpdated/data/tavily ready documents/`

The most important handoff into the LLM system is:

- the offline ready-doc folder
- the ready-doc manifest CSV

That is what later powers `hybrid_rag`.

## 7. What The Simple Tavily Crawler Does Differently

`tavily_crawler.py` is much simpler than `gnem_pipeline.py`.

It does:

- search
- optional download
- one Excel export

It does not do the full GNEM-stage workflow of:

- richer grounding dictionaries
- multi-stage scoring
- exact and near-duplicate passes at the same level of rigor
- final keep/review/discard governance
- curated registry outputs
- ready-doc publishing logic

So the clean way to explain this is:

- `tavily_crawler.py` is a convenience crawler.
- `gnem_pipeline.py` is the real research curation pipeline.

## 8. What This System Is Doing Correctly

The strongest parts are:

1. It is not just crawling; it is doing curated relevance filtering.
2. The GNEM pipeline uses domain grounding rather than generic keyword search alone.
3. It canonicalizes URLs and assigns stable candidate IDs in the main pipeline.
4. It has both exact duplicate and near-duplicate passes.
5. It records a lot of metadata and writes registry outputs.
6. It can publish the curated documents directly into the offline folder used by the LLM comparison system.
7. It supports a metadata-replay mode, which is useful when you want to rerun filtering without hitting Tavily again.

## 9. What Is Still Weak Or Wrong

This is the part you should be honest about with your professor.

### 9.1 Live Tavily search is still not fully reproducible

Problem:

- live search results can change over time
- the pipeline does not yet write a full search replay manifest with raw Tavily responses per query

Consequence:

- two runs at different times may not start from identical candidate sets

### 9.2 Network behavior is still fragile

Problem:

- download and search paths do not yet implement the strongest retry/backoff/replay behavior expected for a research-grade acquisition pipeline

Consequence:

- transient failures can alter the final corpus

### 9.3 The simple crawler is less rigorous than the GNEM pipeline

Problem:

- `tavily_crawler.py` is useful, but it is not the canonical curation surface
- it still relies on simpler naming/download behavior than the GNEM pipeline

Consequence:

- it should not be presented as the main research corpus-building method

### 9.4 Some required grounding assets are external to the uploaded ZIP

Problem:

- files such as the grounding automotive workbook are expected locally but were not included in the uploaded ZIP

Consequence:

- complete reproducibility depends on assets not fully tracked inside the repository snapshot

### 9.5 Caching is still incomplete

Problem:

- the system can replay from metadata XLSX, which is good
- but there is not yet a complete first-class search cache and query-response manifest for every live run

Consequence:

- replayability is partial, not complete

## 10. Are We Doing The Crawler Side Correctly?

Short answer:

- conceptually: yes
- research-grade reproducibility: not fully yet

More precise answer:

- The architecture is correct for a research curation pipeline.
- The GNEM pipeline is doing many of the right things: grounding, staged scoring, duplicate removal, credibility checks, and curated publishing.
- The biggest remaining weakness is reproducibility of the live search/acquisition layer, not the general pipeline design.

So if your professor asks, the best honest answer is:

The filtering logic is serious and multi-stage, but the live Tavily acquisition side still needs better caching/manifests/retry guarantees before I would call it fully research-grade.

## 11. Final Changes Made In This Review Round

For the crawler/filtering side, the main changes in this round were documentation and workflow alignment, not a full algorithmic rewrite.

Changes made:

- clarified the run instructions in `RUNBOOK.md`
- aligned the README/runbook with the canonical handoff into `evAutomationUpdated/data/tavily ready documents`
- sanitized `.env.example` so it no longer contains a real-looking Tavily key

Changes not yet made on the crawler side:

- no search-cache manifest layer yet
- no major retry/backoff rewrite yet
- no full deterministic replay package for live Tavily runs yet

## 12. How To Explain This To Your Professor

You can say:

1. I use Tavily and GNEM grounding files to generate domain-specific EV supply-chain search queries.
2. I retrieve candidate web documents either live from Tavily or from a replayable metadata file.
3. I filter candidates through metadata gates before expensive content processing.
4. I resolve local files or download remote files, then build structured document cards from extracted text.
5. I rank documents with heuristic, embedding, and hybrid relevance signals, then run duplicate removal, optional LLM judging, and credibility scoring.
6. I export review-ready and rejected sets, plus document/chunk registries for traceability.
7. I publish the final curated offline document set into the LLM evaluation folder, where it is later used only as offline evidence for `hybrid_rag`.

## 13. Bottom-Line Verdict

This subsystem is a meaningful curation pipeline, not a toy crawler.

It is doing the right high-level things:

- domain grounding
- staged filtering
- duplicate control
- provenance outputs
- offline handoff to the LLM system

But it is not perfect yet. The main remaining issue is that the live Tavily search layer still needs stronger reproducibility and failure-handling guarantees to be fully defensible as a research acquisition pipeline.
