from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from src.ev_llm_compare.evaluation import (
    attribute_response_sources,
    export_results,
    export_single_model_report,
    run_evaluation_metrics,
)
from src.ev_llm_compare.models import LLMClient
from src.ev_llm_compare.schemas import ModelResponse, RetrievalResult


class _DummyJudgeClient(LLMClient):
    provider = "dummy"
    model_name = "dummy-model"

    def generate(
        self,
        prompt: str,
        temperature: float,
        max_tokens: int,
        system_prompt: str | None = None,
    ) -> str:
        raise AssertionError("safe_generate should be patched in this test")


class EvaluationExportTests(unittest.TestCase):
    def test_export_results_writes_current_metrics_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            responses = [
                ModelResponse(
                    run_name="qwen_rag",
                    provider="ollama",
                    model_name="qwen3:8b",
                    rag_enabled=True,
                    question="What is A?",
                    answer="Answer A with RAG",
                    latency_seconds=1.1,
                    retrieved_chunks=[],
                    prompt_tokens_estimate=111,
                    success=True,
                ),
                ModelResponse(
                    run_name="qwen_no_rag",
                    provider="ollama",
                    model_name="qwen3:8b",
                    rag_enabled=False,
                    question="What is A?",
                    answer="Answer A without RAG",
                    latency_seconds=0.9,
                    retrieved_chunks=[],
                    prompt_tokens_estimate=77,
                    success=True,
                ),
                ModelResponse(
                    run_name="qwen_rag",
                    provider="ollama",
                    model_name="qwen3:8b",
                    rag_enabled=True,
                    question="What is B?",
                    answer="Answer B with RAG",
                    latency_seconds=1.3,
                    retrieved_chunks=[],
                    prompt_tokens_estimate=120,
                    success=True,
                ),
                ModelResponse(
                    run_name="qwen_no_rag",
                    provider="ollama",
                    model_name="qwen3:8b",
                    rag_enabled=False,
                    question="What is B?",
                    answer="Answer B without RAG",
                    latency_seconds=1.0,
                    retrieved_chunks=[],
                    prompt_tokens_estimate=80,
                    success=True,
                ),
            ]
            retrievals = {
                "What is A?": [
                    RetrievalResult(
                        chunk_id="c1",
                        text="Chunk A",
                        metadata={"company": "A Co", "sheet_name": "Data", "chunk_type": "row"},
                        dense_score=0.9,
                        lexical_score=0.8,
                        final_score=0.85,
                    )
                ],
                "What is B?": [],
            }
            references = {"What is A?": "Ref A", "What is B?": "Ref B"}
            reference_sources = {"What is A?": "golden", "What is B?": "generated"}
            metrics_per_run = pd.DataFrame(
                [
                    {
                        "run_name": "qwen_rag",
                        "question": "What is A?",
                        "answer_accuracy": 0.95,
                        "faithfulness": 0.9,
                        "response_groundedness": 0.85,
                        "grounded_claim_ratio": 0.85,
                        "unsupported_claim_ratio": 0.1,
                        "contradicted_claim_ratio": 0.05,
                    },
                    {
                        "run_name": "qwen_no_rag",
                        "question": "What is A?",
                        "answer_accuracy": 0.8,
                        "faithfulness": None,
                        "response_groundedness": None,
                        "grounded_claim_ratio": None,
                        "unsupported_claim_ratio": None,
                        "contradicted_claim_ratio": None,
                    },
                ]
            )

            workbook_path = export_results(
                output_dir=output_dir,
                responses=responses,
                retrievals=retrievals,
                references=references,
                reference_sources=reference_sources,
                metrics_per_run=metrics_per_run,
                metrics_summary=None,
            )

            df = pd.read_excel(workbook_path, sheet_name="responses")
            self.assertEqual(
                df.columns.tolist(),
                [
                    "Question",
                    "reference_answer",
                    "reference_source",
                    "qwen_rag",
                    "qwen_no_rag",
                    "qwen_rag_answer_accuracy",
                    "qwen_rag_faithfulness",
                    "qwen_rag_response_groundedness",
                    "qwen_rag_grounded_claim_ratio",
                    "qwen_rag_unsupported_claim_ratio",
                    "qwen_rag_contradicted_claim_ratio",
                    "qwen_no_rag_answer_accuracy",
                    "qwen_no_rag_faithfulness",
                    "qwen_no_rag_response_groundedness",
                    "qwen_no_rag_grounded_claim_ratio",
                    "qwen_no_rag_unsupported_claim_ratio",
                    "qwen_no_rag_contradicted_claim_ratio",
                    "qwen_rag_latency_seconds",
                    "qwen_no_rag_latency_seconds",
                    "qwen_rag_prompt_tokens_estimate",
                    "qwen_no_rag_prompt_tokens_estimate",
                ],
            )
            self.assertEqual(df.iloc[0]["Question"], "What is A?")
            self.assertEqual(df.iloc[0]["reference_answer"], "Ref A")
            self.assertEqual(df.iloc[0]["reference_source"], "golden")
            self.assertEqual(df.iloc[0]["qwen_rag"], "Answer A with RAG")
            self.assertEqual(df.iloc[0]["qwen_no_rag"], "Answer A without RAG")
            self.assertAlmostEqual(df.iloc[0]["qwen_rag_answer_accuracy"], 0.95)
            self.assertAlmostEqual(df.iloc[0]["qwen_rag_faithfulness"], 0.9)
            self.assertAlmostEqual(df.iloc[0]["qwen_rag_grounded_claim_ratio"], 0.85)
            self.assertAlmostEqual(df.iloc[0]["qwen_no_rag_answer_accuracy"], 0.8)
            self.assertTrue(pd.isna(df.iloc[0]["qwen_no_rag_faithfulness"]))
            self.assertAlmostEqual(df.iloc[0]["qwen_rag_latency_seconds"], 1.1)
            self.assertEqual(df.iloc[0]["qwen_no_rag_prompt_tokens_estimate"], 77)

            raw_df = pd.read_excel(workbook_path, sheet_name="responses_raw")
            self.assertIn("run_name", raw_df.columns)

            single_sheet_df = pd.read_excel(workbook_path, sheet_name="all_in_one")
            self.assertEqual(
                single_sheet_df.columns.tolist(),
                [
                    "Question",
                    "reference_answer",
                    "reference_source",
                    "qwen_rag",
                    "qwen_rag_answer_accuracy",
                    "qwen_rag_faithfulness",
                    "qwen_rag_response_groundedness",
                    "qwen_rag_grounded_claim_ratio",
                    "qwen_rag_unsupported_claim_ratio",
                    "qwen_rag_contradicted_claim_ratio",
                    "qwen_no_rag",
                    "qwen_no_rag_answer_accuracy",
                    "qwen_no_rag_faithfulness",
                    "qwen_no_rag_response_groundedness",
                    "qwen_no_rag_grounded_claim_ratio",
                    "qwen_no_rag_unsupported_claim_ratio",
                    "qwen_no_rag_contradicted_claim_ratio",
                ],
            )

    def test_run_evaluation_metrics_uses_retry_budget_and_scores_rag_metrics(self) -> None:
        response = ModelResponse(
            run_name="qwen_rag",
            provider="ollama",
            model_name="qwen3:8b",
            rag_enabled=True,
            question="What is A?",
            answer="A is supported by the evidence.",
            latency_seconds=0.8,
            retrieved_chunks=[
                RetrievalResult(
                    chunk_id="c1",
                    text="Company: A | Category: Tier 1",
                    metadata={"company": "A", "sheet_name": "Data", "chunk_type": "row_full"},
                    dense_score=0.9,
                    lexical_score=0.8,
                    final_score=0.85,
                )
            ],
            prompt_tokens_estimate=42,
            success=True,
        )

        answers = iter(
            [
                ("not-a-score", 0.01, True, None),
                ("SCORE=0.80", 0.01, True, None),
                (
                    "\n".join(
                        [
                            "FAITHFULNESS=0.90",
                            "RESPONSE_GROUNDEDNESS=0.85",
                            "GROUNDED_CLAIM_RATIO=0.85",
                            "UNSUPPORTED_CLAIM_RATIO=0.10",
                            "CONTRADICTED_CLAIM_RATIO=0.05",
                        ]
                    ),
                    0.01,
                    True,
                    None,
                ),
            ]
        )

        with patch(
            "src.ev_llm_compare.evaluation._make_judge_client",
            return_value=_DummyJudgeClient(),
        ), patch(
            "src.ev_llm_compare.evaluation.safe_generate",
            side_effect=lambda *args, **kwargs: next(answers),
        ) as mocked_generate:
            metrics_per_run, metrics_summary = run_evaluation_metrics(
                responses=[response],
                reference_answers={"What is A?": "Reference A"},
                judge_provider="ollama",
                judge_model="judge-model",
                max_retries=1,
                context_result_limit=4,
                context_char_budget=1000,
            )

        self.assertEqual(mocked_generate.call_count, 3)
        self.assertAlmostEqual(metrics_per_run.iloc[0]["answer_accuracy"], 0.8)
        self.assertAlmostEqual(metrics_per_run.iloc[0]["faithfulness"], 0.9)
        self.assertAlmostEqual(metrics_per_run.iloc[0]["response_groundedness"], 0.85)
        self.assertAlmostEqual(metrics_per_run.iloc[0]["contradicted_claim_ratio"], 0.05)
        self.assertAlmostEqual(metrics_summary.iloc[0]["answer_accuracy"], 0.8)

    def test_attribute_response_sources_treats_no_rag_response_as_pretrained(self) -> None:
        response = ModelResponse(
            run_name="qwen_no_rag",
            provider="ollama",
            model_name="qwen3:8b",
            rag_enabled=False,
            question="What is A?",
            answer="A is a battery supplier in Georgia.",
            latency_seconds=0.5,
            retrieved_chunks=[],
            prompt_tokens_estimate=25,
            success=True,
        )

        attribution = attribute_response_sources(
            response,
            judge_client=None,
        )

        self.assertEqual(attribution["knowledge_source_data"], "")
        self.assertEqual(attribution["pretrained_data"], "A is a battery supplier in Georgia.")
        self.assertEqual(attribution["overall_response"], "A is a battery supplier in Georgia.")

    def test_export_single_model_report_writes_attribution_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            response = ModelResponse(
                run_name="qwen_rag",
                provider="ollama",
                model_name="qwen3:8b",
                rag_enabled=True,
                question="What is A?",
                answer="Company A is in Atlanta. It is a major battery innovator.",
                latency_seconds=0.8,
                retrieved_chunks=[
                    RetrievalResult(
                        chunk_id="c1",
                        text="Company: A | Location: Atlanta",
                        metadata={"company": "A", "sheet_name": "Data", "chunk_type": "row_full"},
                        dense_score=0.9,
                        lexical_score=0.8,
                        final_score=0.85,
                    )
                ],
                prompt_tokens_estimate=42,
                success=True,
            )
            metrics_per_run = pd.DataFrame(
                [
                    {
                        "run_name": "qwen_rag",
                        "question": "What is A?",
                        "answer_accuracy": 0.85,
                        "faithfulness": 0.9,
                        "response_groundedness": 0.5,
                        "grounded_claim_ratio": 0.5,
                        "unsupported_claim_ratio": 0.5,
                        "contradicted_claim_ratio": 0.0,
                    }
                ]
            )

            with patch(
                "src.ev_llm_compare.evaluation._make_judge_client",
                return_value=_DummyJudgeClient(),
            ), patch(
                "src.ev_llm_compare.evaluation.safe_generate",
                return_value=(
                    '{"labels":[{"unit_id":1,"label":"knowledge_source"},{"unit_id":2,"label":"pretrained"}]}',
                    0.01,
                    True,
                    None,
                ),
            ):
                workbook_path = export_single_model_report(
                    output_dir=Path(tmp_dir),
                    responses=[response],
                    references={"What is A?": "Ref A"},
                    reference_sources={"What is A?": "golden"},
                    judge_provider="ollama",
                    judge_model="judge-model",
                    metrics_per_run=metrics_per_run,
                )

            report_df = pd.read_excel(workbook_path, sheet_name="report")
            self.assertEqual(
                report_df.columns.tolist(),
                [
                    "Question",
                    "reference_answer",
                    "reference_source",
                    "overall_response",
                    "knowledge_source_data",
                    "pretrained_data",
                    "answer_accuracy",
                    "faithfulness",
                    "response_groundedness",
                    "grounded_claim_ratio",
                    "unsupported_claim_ratio",
                    "contradicted_claim_ratio",
                ],
            )
            self.assertEqual(report_df.iloc[0]["knowledge_source_data"], "Company A is in Atlanta.")
            self.assertEqual(report_df.iloc[0]["pretrained_data"], "It is a major battery innovator.")
            self.assertEqual(
                report_df.iloc[0]["overall_response"],
                "Company A is in Atlanta. It is a major battery innovator.",
            )

            attribution_df = pd.read_excel(workbook_path, sheet_name="attribution_units")
            self.assertEqual(attribution_df.iloc[0]["label"], "knowledge_source")
            self.assertEqual(attribution_df.iloc[1]["label"], "pretrained")


if __name__ == "__main__":
    unittest.main()
