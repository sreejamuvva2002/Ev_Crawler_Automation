# Qwen2.5 14B Generated Report Analysis

## 1) Dataset Summary

| mode       |   rows |   unique_q_ids | workbook                                                                                          |
|:-----------|-------:|---------------:|:--------------------------------------------------------------------------------------------------|
| no_rag     |     15 |             15 | /home/sm11926/Downloads/evAutomationUpdated/artifacts/response_outputs/qwen14b_no_rag.xlsx        |
| local_rag  |     15 |             15 | /home/sm11926/Downloads/evAutomationUpdated/artifacts/response_outputs/qwen14b_local_rag_v2.xlsx  |
| hybrid_rag |     15 |             15 | /home/sm11926/Downloads/evAutomationUpdated/artifacts/response_outputs/qwen14b_hybrid_rag_v2.xlsx |

Common q_id coverage across all modes: 15 / 15

## 2) Mode Isolation Correctness

| mode       |   rows |   pass_count |   unexpected_local_retrieval |   unexpected_web_retrieval |   unexpected_context |   missing_local_retrieval |   missing_web_retrieval |
|:-----------|-------:|-------------:|-----------------------------:|---------------------------:|---------------------:|--------------------------:|------------------------:|
| no_rag     |     15 |           15 |                            0 |                          0 |                    0 |                         0 |                       0 |
| local_rag  |     15 |           15 |                            0 |                          0 |                    0 |                         0 |                       0 |
| hybrid_rag |     15 |           15 |                            0 |                          0 |                    0 |                         0 |                       0 |

Violations for `no_rag`:
- unexpected_local_retrieval: none
- unexpected_web_retrieval: none
- unexpected_context: none
- missing_local_retrieval: none
- missing_web_retrieval: none

Violations for `local_rag`:
- unexpected_local_retrieval: none
- unexpected_web_retrieval: none
- unexpected_context: none
- missing_local_retrieval: none
- missing_web_retrieval: none

Violations for `hybrid_rag`:
- unexpected_local_retrieval: none
- unexpected_web_retrieval: none
- unexpected_context: none
- missing_local_retrieval: none
- missing_web_retrieval: none

## 3) Citation Parseability + Consistency

| mode       |   answers_with_any_citations_pct |   avg_citations_per_answer |   citation_mismatch_rate_pct |
|:-----------|---------------------------------:|---------------------------:|-----------------------------:|
| no_rag     |                                0 |                       0    |                            0 |
| local_rag  |                              100 |                       2.13 |                            0 |
| hybrid_rag |                              100 |                       2.33 |                            0 |

Recommended strict format:
- Always cite retrieved local evidence as `[DOC:<retrieved_id>]`
- Always cite retrieved web evidence as `[WEB:<retrieved_id>]`
- Avoid `Source 1`, plain URLs, or unlabeled ids in the final answer

No citation formatting mismatches were detected from the current spreadsheets.

## 4) Abstention/Coverage Comparison Across Modes

| mode       |   abstention_rate_pct |   answerable_proxy_rate_pct |   avg_answer_words |
|:-----------|----------------------:|----------------------------:|-------------------:|
| no_rag     |                  0    |                      100    |             143.4  |
| local_rag  |                 46.67 |                       53.33 |              52.33 |
| hybrid_rag |                 40    |                       60    |              59.33 |

Hybrid answerable proxy uplift vs local_rag: 6.67 percentage points.

Hybrid abstention delta vs local_rag: -6.67 percentage points.

## 5) Retrieval Diagnostics

| mode       | corpus   |   avg_top_score |   min_top_score |   p10_threshold | low_score_qids   |
|:-----------|:---------|----------------:|----------------:|----------------:|:-----------------|
| no_rag     | local    |       nan       |       nan       |       nan       |                  |
| no_rag     | web      |       nan       |       nan       |       nan       |                  |
| local_rag  | local    |         0.17726 |         0.00738 |         0.03051 | 11, 8            |
| local_rag  | web      |       nan       |       nan       |       nan       |                  |
| hybrid_rag | local    |         0.17726 |         0.00738 |         0.03051 | 11, 8            |
| hybrid_rag | web      |         0.01414 |         0.00902 |         0.00902 | 11, 4, 6         |

| mode       |   avg_local_retrieval_count |   avg_web_retrieval_count |   avg_citation_match_ratio |
|:-----------|----------------------------:|--------------------------:|---------------------------:|
| no_rag     |                        0    |                      0    |                    nan     |
| local_rag  |                        5.53 |                      0    |                      0.97  |
| hybrid_rag |                        5.53 |                      3.93 |                      0.953 |

## 6) Hybrid Value Evidence

- Questions where hybrid abstained less than local_rag: 1
- q_ids: ['8']
- Hybrid adds value structurally when web retrieval exists, abstention falls, citation counts rise, or citation-to-retrieval match improves.
- TODO: requires answer-level gold labels to conclude whether hybrid improved factual correctness, not just coverage proxies.

## 7) Actionable Fixes

- Enforce one strict citation format in answers: `[DOC:<retrieved_id>]` and `[WEB:<retrieved_id>]` only.
- Write parsed citation columns directly from answer generation or post-processing so `doc_citations` and `web_citations` always match the answer text.
- Log retrieval ids and scores in a normalized per-question structure even if the retrieval sheets are missing. This keeps analysis stable.
- For hybrid runs, preserve explicit `WEB:` citations in the prompt/instructions; web evidence is retrieved, but answers may still cite only DOC ids.
- TODO: requires gold/reference answers to measure factual correctness, not just structure, abstention, and citation-proxy grounding.

