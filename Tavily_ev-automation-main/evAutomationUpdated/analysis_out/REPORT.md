# Qwen2.5 14B Generated Report Analysis

## 1) Dataset Summary

| mode       |   rows |   unique_q_ids | workbook                                                                                       |
|:-----------|-------:|---------------:|:-----------------------------------------------------------------------------------------------|
| no_rag     |     15 |             15 | /home/sm11926/Downloads/evAutomationUpdated/artifacts/response_outputs/qwen14b_no_rag.xlsx     |
| local_rag  |     15 |             15 | /home/sm11926/Downloads/evAutomationUpdated/artifacts/response_outputs/qwen14b_local_rag.xlsx  |
| hybrid_rag |     15 |             15 | /home/sm11926/Downloads/evAutomationUpdated/artifacts/response_outputs/qwen14b_hybrid_rag.xlsx |

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
| no_rag     |                                0 |                       0    |                         0    |
| local_rag  |                              100 |                       4.13 |                         0    |
| hybrid_rag |                              100 |                       4.2  |                        13.33 |

Recommended strict format:
- Always cite retrieved local evidence as `[DOC:<retrieved_id>]`
- Always cite retrieved web evidence as `[WEB:<retrieved_id>]`
- Avoid `Source 1`, plain URLs, or unlabeled ids in the final answer

Examples of citation formatting / logging mismatch:

| mode       |   q_id | question                                                                         | answer_excerpt                                                                                                                                                                                                                                                                                               |
|:-----------|-------:|:---------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hybrid_rag |      3 | Which companies in the region manufacture battery modules versus complete packs? | - A123 Systems, Inc. manufactures battery cells and modules. - EnerDel, Inc. produces lithium-ion cells and packs for hybrid and electric vehicles.  Evidence: WEB:6bd430777b692d0040f30771250d5323afeb17b4 WEB:6f4c7576b516de96f661d5351c2951fe6659505e                                                     |
| hybrid_rag |      6 | List Tier 2 suppliers supporting autonomous vehicle sensor systems.              | Not found in provided context.  Evidence: - DOC:09c6ccae-f41d-5e93-b03c-3142f7017df0 - DOC:da0f4961-1ff2-513f-b847-6080eb85604a - DOC:de262649-2c2f-51e9-a464-17be9273af17 - DOC:dca72302-0eb9-55a3-87fd-24cf15d88b0 - DOC:582b24ef-b262-5250-8ca1-68e862f46e0d - WEB:403eec33523d27f80809add9277fb7a17b42e5 |

## 4) Abstention/Coverage Comparison Across Modes

| mode       |   abstention_rate_pct |   answerable_proxy_rate_pct |   avg_answer_words |
|:-----------|----------------------:|----------------------------:|-------------------:|
| no_rag     |                 93.33 |                        6.67 |              73    |
| local_rag  |                 73.33 |                       26.67 |              22.73 |
| hybrid_rag |                 53.33 |                       46.67 |              32.93 |

Hybrid answerable proxy uplift vs local_rag: 20.0 percentage points.

Hybrid abstention delta vs local_rag: -20.0 percentage points.

## 5) Retrieval Diagnostics

| mode       | corpus   |   avg_top_score |   min_top_score |   p10_threshold | low_score_qids   |
|:-----------|:---------|----------------:|----------------:|----------------:|:-----------------|
| no_rag     | local    |       nan       |       nan       |       nan       |                  |
| no_rag     | web      |       nan       |       nan       |       nan       |                  |
| local_rag  | local    |         0.41467 |         0.00738 |         0.01409 | 15, 8            |
| local_rag  | web      |       nan       |       nan       |       nan       |                  |
| hybrid_rag | local    |         0.41752 |         0.00738 |         0.01947 | 15, 8            |
| hybrid_rag | web      |         0.01976 |         0.01449 |         0.01462 | 4, 6             |

| mode       |   avg_local_retrieval_count |   avg_web_retrieval_count |   avg_citation_match_ratio |
|:-----------|----------------------------:|--------------------------:|---------------------------:|
| no_rag     |                           0 |                         0 |                    nan     |
| local_rag  |                           5 |                         0 |                      0.948 |
| hybrid_rag |                           5 |                         3 |                      0.909 |

## 6) Hybrid Value Evidence

- Questions where hybrid abstained less than local_rag: 3
- q_ids: ['15', '3', '8']
- Hybrid adds value structurally when web retrieval exists, abstention falls, citation counts rise, or citation-to-retrieval match improves.
- TODO: requires answer-level gold labels to conclude whether hybrid improved factual correctness, not just coverage proxies.

## 7) Actionable Fixes

- Enforce one strict citation format in answers: `[DOC:<retrieved_id>]` and `[WEB:<retrieved_id>]` only.
- Write parsed citation columns directly from answer generation or post-processing so `doc_citations` and `web_citations` always match the answer text.
- Log retrieval ids and scores in a normalized per-question structure even if the retrieval sheets are missing. This keeps analysis stable.
- For hybrid runs, preserve explicit `WEB:` citations in the prompt/instructions; web evidence is retrieved, but answers may still cite only DOC ids.
- TODO: requires gold/reference answers to measure factual correctness, not just structure, abstention, and citation-proxy grounding.

