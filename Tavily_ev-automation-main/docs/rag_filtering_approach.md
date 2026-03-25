# GNEM RAG Filtering Approach

## 1. What we are building

This repo is not building the full GNEM digital twin yet.

The current scope is:

- collect open-source documents for the Georgia and Southeast EV battery supply chain
- filter them down to a high-quality set
- send the final shortlist for human review
- use the reviewed set in a RAG chatbot

The business goal is precision, not volume.

If we retrieve around `4000` documents, it is acceptable to keep only `80-150` if those are the right ones.

## 2. What context defines the use case

Use these project assets as grounding context:

- `../data/grounding/GNEM Supply Chain.docx`
- `../data/grounding/GA_Automotive Landscape_All_Companies (1).xlsx`
- `../data/grounding/Counties_Georgia.geojson`

What these tell us:

- the use case is Georgia and Southeast EV battery supply-chain intelligence
- we care about real companies, facilities, locations, capacities, products, OEM links, logistics, risks, and localization decisions
- we need documents that can support a future supply-chain chatbot, not just generic background reading

Short use-case summary:

`Build a high-precision open-source document corpus for a Georgia and Southeast EV battery supply-chain RAG chatbot. Keep documents that provide useful evidence about companies, facilities, products, capacities, supplier relationships, logistics, risks, incentives, and localization decisions. The final set is sent for human review before being used in RAG.`

Concise summary to reuse in prompts:

`GNEM needs open-source documents that help map and understand the Georgia and Southeast EV battery supply chain. The most valuable documents identify real companies, facilities, products, capacities, supplier links, logistics routes, risks, policy/incentives, and localization opportunities. The goal is to create a high-precision reviewed document corpus for a supply-chain RAG chatbot, not to collect generic EV news or unrelated research.`

## 3. Updated target pipeline

Use this filtering pipeline:

`query expansion -> retrieval -> progressive parsing -> lightweight document card -> heuristic scoring -> hybrid filtering -> topic/use-case classification -> reranking -> enriched document card -> LLM judge -> credibility check -> diversity pass -> human review pack`

This is the best practical approach for this repo because:

- it does not require a fully labeled training dataset
- it works on open-source data, which is noisy
- it avoids spending full parsing cost on every document
- it improves precision step by step
- it handles documents whose key evidence appears after page 1

## 4. The core idea: use two document cards

Do not judge a document only from title or first page.

Use two levels of document card.

### Lightweight document card

Build this for all documents using cheap extraction:

- title
- source domain
- URL or local path
- file type
- publication date if available
- metadata summary
- first page summary
- headings or TOC if available
- sampled-page summaries such as first, middle, and later pages
- extracted entities:
  - company names
  - counties
  - Georgia and Southeast locations
  - OEM names
  - facility terms
  - capacity and date mentions

### Enriched document card

Build this only for shortlisted documents:

- everything from the lightweight card
- top 3 to 5 most relevant chunks from anywhere in the document
- evidence page numbers
- key table lines or captions if available
- best supporting snippets for why the document should be kept

Why this matters:

- some good documents hide the useful details after the first page
- some documents are useful because of a later section, table, or appendix
- the lightweight card keeps cost low for thousands of documents
- the enriched card lets us capture page-12 or page-20 evidence without deeply parsing every file at the start

## 5. The final stages in simple terms

### Stage A. Query expansion

Input:

- use-case summary
- Georgia company list
- county names
- supply-chain facets

How it works:

- generate queries for each facet, not one generic query
- include specific Georgia entities and supply-chain functions

Example queries:

- `Georgia battery recycling facility capacity report pdf`
- `Hyundai Kia Georgia battery supplier tier report pdf`
- `Savannah port EV battery logistics risk report pdf`
- `Georgia cathode anode separator battery material facility pdf`

Output:

- a broader and more targeted search set

### Stage B. Progressive parsing and normalization

Input:

- retrieved PDF, HTML, DOCX, TXT, and similar files

How it works:

- first do cheap extraction for all documents:
  - metadata
  - first page
  - first 1 to 2 pages if available
  - headings or TOC if available
  - a few sampled pages
- build the lightweight document card
- only for shortlisted documents:
  - split text into chunks
  - score chunks against the use case and facets
  - keep the best chunks from anywhere in the document
  - build the enriched document card

Output:

- normalized documents with lightweight cards for all docs and enriched cards for shortlisted docs

### Stage C. Heuristic score

Purpose:

- remove obvious junk early

Simple score:

`heuristic_score = source_quality + doc_quality + geography_signal + supply_chain_signal + specificity - noise_penalty`

Each part can be on a simple `0-20` or `0-25` scale.

What we reward:

- trusted source
- readable text
- PDF report, filing, whitepaper, official deck
- Georgia or Southeast mentions
- company, facility, capacity, timeline, product, logistics, or risk evidence

What we penalize:

- generic blogs
- marketing pages
- low-text or broken OCR files
- chemistry-only research with no supply-chain relevance
- duplicate or mirrored content

Example:

Good document:

- source quality = `18`
- doc quality = `16`
- geography signal = `20`
- supply-chain signal = `18`
- specificity = `15`
- penalty = `2`
- heuristic score = `85`

Bad document:

- source quality = `5`
- doc quality = `6`
- geography signal = `0`
- supply-chain signal = `4`
- specificity = `1`
- penalty = `10`
- heuristic score = `6`

Use this stage to drop obvious noise.

### Stage D. Hybrid filtering score

Purpose:

- measure semantic match with the business use case
- preserve exact entity and geography matches that embeddings alone can miss

Use two signals:

- `semantic_embedding_score`
- `lexical_entity_score`

What is similarity:

- text is converted into vectors using an embedding model
- then compare vectors using cosine similarity

Simple meaning:

- high similarity = similar meaning
- low similarity = weak or unrelated meaning

Use this semantic score:

`semantic_embedding_score = 0.4 * global_similarity + 0.4 * top2_facet_average + 0.2 * facet_coverage`

Definitions:

- `global_similarity`: similarity between use-case summary and document card
- `top2_facet_average`: average similarity of the 2 strongest facet queries
- `facet_coverage`: how many important facets the doc matches

Use these main facets:

- who makes what where at what scale
- supplier and tier relationships
- logistics and infrastructure risk
- policy, incentives, and localization

Use this exact-match score:

`lexical_entity_score`

It should reward exact mentions of:

- Georgia counties
- company names
- OEM names
- ports and logistics nodes
- facility terms
- capacities, dates, and locations
- battery value-chain terms

Combine them like this:

`hybrid_score = 0.6 * semantic_embedding_score + 0.4 * lexical_entity_score`

Example:

- global similarity = `0.66`
- facet similarities = `[0.84, 0.76, 0.29, 0.21]`
- top2 average = `0.80`
- facet coverage = `0.50`

Then:

`semantic_embedding_score = 0.4*0.66 + 0.4*0.80 + 0.2*0.50 = 0.684`

If `lexical_entity_score = 0.72`, then:

`hybrid_score = 0.6*0.684 + 0.4*0.72 = 0.6984`

Convert to `69.84 / 100` if needed.

### Stage E. Topic and use-case classifier

Yes, this stage should exist.

Because you do not have a labeled dataset yet, do not start with a supervised classifier.

Use a weakly supervised classifier first.

The classifier should produce scores for:

- `direct_usecase`
- `adjacent_background`
- `research_only`
- `generic_news`
- `marketing_noise`

How to implement it now:

- use rules + hybrid signals + entity evidence to estimate class scores
- optionally use a small LLM or zero-shot classifier later

Simple logic:

- `direct_usecase` goes up when the document has:
  - Georgia or Southeast grounding
  - real company or facility names
  - product or supply-chain role
  - capacity, timeline, logistics, risk, or localization evidence

- `research_only` goes up when the document has:
  - science and chemistry language
  - weak Georgia grounding
  - weak company or facility evidence
  - no business or network value

- `generic_news` goes up when the document has:
  - short article style
  - high narrative, low structure
  - no detailed evidence

- `marketing_noise` goes up when the document has:
  - promotional tone
  - vague benefits
  - no verifiable operational details

Simple score example:

- direct_usecase = `0.82`
- adjacent_background = `0.10`
- research_only = `0.04`
- generic_news = `0.03`
- marketing_noise = `0.01`

Decision:

- keep moving forward if `direct_usecase >= 0.70`
- review band if `0.50 to 0.70`
- drop if `< 0.50` and off-topic scores dominate

### Stage F. Reranking

Purpose:

- reorder the remaining documents more precisely

How it works:

- use a reranker on the enriched document card against the use case or facet query
- rerank only the top candidates from the hybrid stage

Example:

- after heuristic and hybrid filtering, maybe `4000 -> 500`
- reranking narrows `500 -> 150`

### Stage G. Enriched evidence selection

Purpose:

- make sure late-page evidence can influence the final decision

How it works:

- take the top documents after reranking
- select the best chunks from anywhere in the document
- capture evidence page numbers
- include key table lines or captions if available
- rebuild the enriched document card with the strongest evidence

This is the step that helps when the useful information is on page 12 or later.

### Stage H. LLM judge

Purpose:

- final semantic and business fit check

Use the LLM judge only on the top shortlist.
Do not run it on all documents.

Input:

- use-case summary
- query or facet
- enriched document card

Expected JSON:

```json
{
  "relevance_score": 0,
  "usecase_match": 0,
  "information_quality": 0,
  "noise_level": 0,
  "decision": "keep"
}
```

Simple meaning:

- `relevance_score`: does this document talk about the same problem?
- `usecase_match`: does it directly help the GNEM use case?
- `information_quality`: is it concrete and useful?
- `noise_level`: how much fluff or junk is present?

Simple judge total:

`judge_score = 0.35*relevance + 0.35*usecase_match + 0.20*information_quality + 0.10*(10-noise_level)`

Example:

- relevance = `9`
- usecase_match = `8`
- information_quality = `8`
- noise_level = `2`

Then:

`judge_score = 8.3 / 10`

Keep rule:

- relevance `>= 8`
- usecase_match `>= 8`
- information_quality `>= 7`
- noise_level `<= 3`

### Stage I. Credibility check

Purpose:

- separate source trust from topical relevance

Simple idea:

- a document can be highly relevant but still weak or promotional
- credibility should be scored separately, not hidden inside the relevance score

Simple credibility signals:

- strong source domain
- operational detail with named entities and quantities
- consistency between title, source, and extracted evidence
- low promotional language

Simple output:

- `credibility_score` from `0 to 100`
- `credibility_reason`

### Stage J. Diversity pass

Purpose:

- stop the final set from being overly concentrated in one subtopic

Simple idea:

- bucket final documents by facet:
  - facilities and capacity
  - supplier and tier mapping
  - logistics and infrastructure
  - policy and incentives
  - resilience and risk
- keep the strongest documents in each bucket

This is not a relevance stage.
It is a final balancing stage before human review.

### Stage K. Human review pack

Final output should not be just a single score.

For every final document, export:

- title
- source
- URL or path
- heuristic score
- hybrid score
- classifier scores
- judge JSON
- credibility score
- top evidence snippets
- evidence page numbers
- reason for keep

This is what you send to reviewers.

## 6. Recommended final metrics

Use these metrics in the pipeline.

### Metric 1. Heuristic score

Range:

- `0 to 100`

Meaning:

- basic quality and relevance screening

### Metric 2. Hybrid score

Range:

- `0 to 100`

Meaning:

- semantic closeness plus exact entity relevance

### Metric 3. Topic/use-case classifier scores

Range:

- each class gets `0 to 1`

Meaning:

- probability-like score for document type

Important one:

- `direct_usecase_score`

### Metric 4. LLM judge score

Range:

- `0 to 10` per field
- convert to total if needed

Meaning:

- final semantic and business-fit check

### Metric 5. Credibility score

Range:

- `0 to 100`

Meaning:

- trust and evidence quality independent of relevance

## 7. Recommended filtering thresholds

These are practical starting points, not permanent truths.

### Recommended pass logic

1. Keep after heuristic stage if:

- `heuristic_score >= 45`

2. Keep after hybrid stage if:

- `hybrid_score >= 65`

3. Keep after classifier stage if:

- `direct_usecase_score >= 0.70`

4. Keep after LLM judge if:

- `relevance_score >= 8`
- `usecase_match >= 8`
- `information_quality >= 7`
- `noise_level <= 3`

5. Keep after credibility check if:

- `credibility_score >= 60`

### Review band

If a document is close but not clear:

- heuristic `35-45`
- hybrid `55-65`
- direct use case `0.50-0.70`
- judge total around `6.5-8.0`
- credibility `45-60`

Send those to manual review rather than dropping immediately.

## 8. Example: how one document moves through the pipeline

Document:

`Battery recycling facility in Covington, Georgia with 30,000 metric ton capacity and opening timeline`

Stage by stage:

- heuristic score = `83`
  - strong source
  - PDF
  - Georgia location
  - facility and capacity

- hybrid score = `79`
  - matches global use case
  - matches battery recycling and facility capacity facets
  - has exact Georgia and capacity evidence

- classifier:
  - direct_usecase = `0.88`
  - research_only = `0.03`
  - generic_news = `0.06`

- LLM judge:
  - relevance = `9`
  - usecase_match = `9`
  - information_quality = `8`
  - noise = `2`
  - decision = `keep`

- credibility:
  - score = `82`
  - reason = `credible source with named facility, location, and capacity`

Result:

- keep and send to human review

Bad document example:

`Academic paper on cathode electrochemistry without Georgia companies or facilities`

- heuristic = `24`
- hybrid = `38`
- direct_usecase = `0.18`
- research_only = `0.77`
- LLM judge = discard
- credibility may still be moderate, but relevance is weak

Result:

- discard

## 9. Page-12 problem and how to solve it

This is the main weakness of a first-page-heavy pipeline.

Example:

- page 1 is generic background
- page 12 contains the Georgia facility, capacity, and timeline
- page 18 contains the supplier map

If we judge only page 1, we may discard the document.

The fix is:

- lightweight card for all docs
- progressive parsing
- chunk scoring for shortlisted docs
- enriched card with evidence pages from anywhere in the document

That way, the final judge sees the useful evidence even if it is buried later in the file.

## 10. What should change next in the current repo

If the current baseline approach from this document is already implemented, the next improvement cycle should focus on these additions:

- progressive parsing so later useful pages can be discovered
- two-level document cards:
  - lightweight cards for all docs
  - enriched cards for shortlisted docs
- hybrid filtering:
  - semantic embeddings
  - exact entity and geography scoring
- evidence-page and top-chunk extraction from anywhere in the document
- separate credibility check after LLM judging
- final diversity pass so one facet does not dominate the final corpus
- richer review export with evidence pages and keep reasons

If the current repo still depends mainly on metadata and first-page scoring, then these same items are the missing implementation pieces.

## 11. Recommended implementation order

Implement in this order:

1. add progressive parsing and page sampling
2. add lightweight and enriched document cards
3. add hybrid filtering with semantic plus lexical entity scoring
4. improve topic/use-case classifier inputs using enriched evidence
5. add reranking on enriched cards
6. add separate credibility check
7. add diversity pass
8. export review-ready Excel or JSONL with evidence pages and chunk snippets

This order gives the highest improvement with the least risk.

## 12. Prompt to use in another chat

Copy-paste this prompt into another chat if you want the next round of implementation help in this repo:

```text
You are modifying the repo at:
L:\SREEJARAGPROJECT\Taviliy_HTML_PDF\Taviliy_HTML_PDF

Use the provided `docs/rag_filtering_approach.md` file as the main design context.

Context:
- This repo is for phase-1 GNEM RAG corpus construction, not the full digital twin.
- The repo may already contain a baseline filtering implementation based on the earlier design.
- The next improvement cycle should focus on documents whose key evidence may appear after the first page.
- We need a higher-precision open-source document filtering pipeline for Georgia and Southeast EV battery supply-chain documents.
- Final documents will be sent for human review before RAG ingestion.

Use-case summary:
- GNEM needs open-source documents that help map and understand the Georgia and Southeast EV battery supply chain.
- The most valuable documents identify real companies, facilities, products, capacities, supplier links, logistics routes, risks, policy/incentives, and localization opportunities.
- The goal is to create a high-precision reviewed document corpus for a supply-chain RAG chatbot, not to collect generic EV news or unrelated research.

Grounding files to use:
- GNEM Supply Chain.docx
- GA_Automotive Landscape_All_Companies (1).xlsx
- Counties_Georgia.geojson

Goal:
Extend the existing implementation with the next-level filtering improvements.
Keep the baseline behavior, but add better handling for late-page evidence, stronger filtering precision, and better reviewer outputs.

Design requirements:
1. Keep the existing repo structure and extend gnem_pipeline.py rather than replacing everything.
2. Keep and improve query generation in queries_1000.txt using:
   - GNEM Supply Chain.docx
   - GA_Automotive Landscape_All_Companies (1).xlsx
   - Counties_Georgia.geojson
   Query generation should use:
   - Georgia and Southeast geography
   - company names and OEM anchors where useful
   - battery value-chain stages
   - supply-chain facets such as capacity, facility, tiering, logistics, risk, incentives, and localization
3. Add progressive parsing:
   - cheap extraction for all docs:
     - metadata
     - first page
     - first 1 to 2 pages if available
     - headings or TOC if available
     - sampled pages such as first, middle, and late pages
   - deeper chunk extraction only for shortlisted docs
4. Use two document-card levels:
   - lightweight card for all docs
   - enriched card for shortlisted docs
   The enriched card should contain:
   - title
   - source domain
   - URL or local path
   - file type
   - publication date if available
   - metadata summary
   - first page summary
   - headings or TOC if available
   - top 3 to 5 relevant chunks
   - evidence page numbers
   - extracted entities such as company names, counties, OEMs, facilities, capacities, and dates
5. Build grounding dictionaries from:
   - GA_Automotive Landscape_All_Companies (1).xlsx
   - Counties_Georgia.geojson
6. Add a hybrid filtering stage.
   - Use document cards, not only first page text.
   - Use this semantic score:
     semantic_embedding_score = 0.4 * global_similarity + 0.4 * top2_facet_average + 0.2 * facet_coverage
   - Add lexical_entity_score using exact matches for counties, companies, OEMs, ports, facilities, capacities, dates, and value-chain terms
   - Combine them as:
     hybrid_score = 0.6 * semantic_embedding_score + 0.4 * lexical_entity_score
   - Facets:
     - who makes what where at what scale
     - supplier and tier relationships
     - logistics and infrastructure risk
     - policy, incentives, and localization
7. Add a weakly supervised topic/use-case classifier stage that outputs these scores:
   - direct_usecase_score
   - adjacent_background_score
   - research_only_score
   - generic_news_score
   - marketing_noise_score
8. Use simple rubric-based logic for the classifier because we do not have a labeled dataset yet.
9. Add reranking on enriched document cards after hybrid filtering and classifier scoring.
10. Keep the LLM judge stage, but apply it only to the shortlisted documents after reranking.
11. Add a separate credibility check after LLM judgment.
12. Add a final diversity pass so the final set is not dominated by one subtopic such as recycling or policy only.
13. Export a final review-ready file with:
   - title
   - source
   - file path or URL
   - heuristic score
   - hybrid score
   - classifier scores
   - LLM judge scores
   - credibility score
   - keep or review or discard reason
   - top evidence snippets
   - evidence page numbers

Recommended threshold logic:
- heuristic_score >= 45
- hybrid_score >= 65
- direct_usecase_score >= 0.70
- LLM judge keep only if:
  - relevance_score >= 8
  - usecase_match >= 8
  - information_quality >= 7
  - noise_level <= 3
- credibility_score >= 60

Implementation constraints:
- make incremental changes
- preserve existing CLI behavior where possible
- add new CLI flags only where needed
- keep outputs transparent and easy to audit
- use apply_patch for edits
- include compile or smoke-test verification

Deliverables:
1. Improved generate_gnem_queries.py and regenerated queries_1000.txt if needed
2. Updated gnem_pipeline.py with progressive parsing, hybrid filtering, credibility, and diversity
3. Any helper functions needed in the repo
4. Updated README usage examples
5. New output columns and output files for review

Before coding, inspect the existing gnem_pipeline.py and explain which parts of this design already exist and which parts are still missing. Then implement the missing pieces incrementally end to end.
```

## 13. Final decision philosophy

This pipeline is not trying to be perfect.

It is trying to be:

- conservative
- explainable
- strong enough that reviewers mostly see good documents

That is the correct target for open-source corpus construction.
