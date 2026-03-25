# Qwen14B RAG Improvement Report

| label         |   rows |   abstention_rate_pct |   avg_citation_match_ratio |   avg_unsupported_claim_ratio |
|:--------------|-------:|----------------------:|---------------------------:|------------------------------:|
| local_before  |     15 |                 73.33 |                          1 |                         0.238 |
| local_after   |     15 |                 46.67 |                          1 |                         0.105 |
| hybrid_before |     15 |                 53.33 |                          1 |                         0.108 |
| hybrid_after  |     15 |                 40    |                          1 |                         0.12  |

Local-RAG abstention drop: 26.66 percentage points

Hybrid-RAG abstention drop: 13.33 percentage points

Local-RAG improved q_ids: ['13', '3', '5', '6']

Hybrid-RAG improved q_ids: ['13', '5', '6']

Retrieval relevance proxy: avg_citation_match_ratio (higher is better).

Unsupported claim proxy: avg_unsupported_claim_ratio from the existing judge packet using mistral-small3.2:24b (lower is better).
