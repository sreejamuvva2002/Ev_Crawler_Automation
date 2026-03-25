from pathlib import Path
import tempfile
import unittest

import pandas as pd

from src.ev_llm_compare.research_eval import (
    GoldenAnswerRecord,
    export_hybrid_value_report,
    extract_citations,
    load_golden_answers,
    resolve_golden_answer,
    validate_rag_answer,
)


class ResearchEvalTests(unittest.TestCase):
    def test_load_golden_answers_reads_required_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "golden.xlsx"
            pd.DataFrame(
                {
                    "q_id": ["Q1", "Q2"],
                    "question": ["What is A?", "What is B?"],
                    "golden_answer": ["Answer A", "Answer B"],
                    "answer_format": ["list", "numeric"],
                }
            ).to_excel(path, index=False)

            records = load_golden_answers(path)

            self.assertEqual(sorted(records), ["Q1", "Q2"])
            self.assertEqual(records["Q1"].golden_answer, "Answer A")
            self.assertEqual(records["Q2"].answer_format, "numeric")

    def test_resolve_golden_answer_prefers_qid_even_when_question_text_differs(self) -> None:
        records = {
            "Q7": GoldenAnswerRecord(
                q_id="Q7",
                question="Original wording",
                golden_answer="Golden answer",
            )
        }

        match = resolve_golden_answer(
            q_id="Q7",
            question="Different wording now",
            golden_answers=records,
            fallback_answer_text="Fallback should not win",
        )

        self.assertIsNotNone(match.record)
        self.assertEqual(match.record.golden_answer, "Golden answer")
        self.assertEqual(match.match_type, "q_id")
        self.assertTrue(match.question_mismatch)

    def test_extract_citations_supports_allowed_kinds(self) -> None:
        citations = extract_citations(
            "- A [DOC:d1]\n- B [WEB:w1] [ANALYTIC:a1] [GEO:g1]"
        )

        self.assertEqual(
            [citation.token for citation in citations],
            ["DOC:d1", "WEB:w1", "ANALYTIC:a1", "GEO:g1"],
        )

    def test_resolve_golden_answer_does_not_fallback_when_external_gold_is_present(self) -> None:
        records = {
            "Q1": GoldenAnswerRecord(
                q_id="Q1",
                question="Original wording",
                golden_answer="Golden answer",
            )
        }

        match = resolve_golden_answer(
            q_id="Q2",
            question="Question without external gold",
            golden_answers=records,
            fallback_answer_text="Expected answer in question sheet",
            allow_question_row_fallback=False,
        )

        self.assertIsNone(match.record)
        self.assertEqual(match.match_type, "missing")

    def test_validate_rag_answer_computes_support_and_flags_failures(self) -> None:
        evidence_registry = {
            "DOC:doc1": {
                "citation_key": "DOC:doc1",
                "text": "Alpha supplies battery packs in Georgia for Hyundai.",
            }
        }

        validated = validate_rag_answer(
            question="Which companies supply battery packs in Georgia?",
            answer_text=(
                "- Alpha supplies battery packs in Georgia [DOC:doc1]\n"
                "- Beta employs 500 workers [DOC:doc1]"
            ),
            evidence_registry=evidence_registry,
            judge_client=None,
            judge_max_retries=0,
        )

        self.assertFalse(validated["citation_missing"])
        self.assertFalse(validated["citation_invalid"])
        self.assertTrue(validated["support_failed"])
        self.assertAlmostEqual(validated["support_rate"], 0.5)
        self.assertAlmostEqual(validated["unsupported_claim_rate"], 0.5)

    def test_validate_rag_answer_flags_missing_and_invalid_citations(self) -> None:
        evidence_registry = {
            "DOC:doc1": {
                "citation_key": "DOC:doc1",
                "text": "Alpha supplies battery packs in Georgia.",
            }
        }

        validated = validate_rag_answer(
            question="Which companies supply battery packs in Georgia?",
            answer_text=(
                "- Alpha supplies battery packs in Georgia\n"
                "- Another claim [WEB:missing]"
            ),
            evidence_registry=evidence_registry,
            judge_client=None,
            judge_max_retries=0,
        )

        self.assertTrue(validated["citation_missing"])
        self.assertTrue(validated["citation_invalid"])
        self.assertTrue(validated["support_failed"])
        self.assertEqual(validated["citation_missing_count"], 1)
        self.assertEqual(validated["citation_invalid_count"], 1)

    def test_export_hybrid_value_report_highlights_web_added_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            local_jsonl = root / "local.jsonl"
            hybrid_jsonl = root / "hybrid.jsonl"
            local_jsonl.write_text(
                (
                    '{"q_id":"Q1","question":"Who supplies battery packs?","answer_text":"Not found in provided context.",'
                    '"citation_tokens":[],"retrieved_context_ids":["DOC:doc1"],"answer_abstained":true,'
                    '"golden_normalized_exact_match":0.0,"golden_semantic_similarity":0.1}\n'
                ),
                encoding="utf-8",
            )
            hybrid_jsonl.write_text(
                (
                    '{"q_id":"Q1","question":"Who supplies battery packs?","answer_text":"- Alpha supplies battery packs [WEB:web1]",'
                    '"citation_tokens":["WEB:web1"],"retrieved_context_ids":["DOC:doc1","WEB:web1"],'
                    '"answer_abstained":false,"support_rate":1.0,"citation_coverage":1.0,'
                    '"unsupported_claim_rate":0.0,"golden_normalized_exact_match":1.0,'
                    '"golden_semantic_similarity":0.95}\n'
                ),
                encoding="utf-8",
            )
            (root / "20260325T170000Z_qwen25_14b_local_rag_manifest.json").write_text(
                (
                    '{"study_id":"study_a","model_key":"qwen25_14b","mode":"local_rag",'
                    '"timestamp":"2026-03-25T17:00:00Z","run_id":"run_local",'
                    f'"response_jsonl_path":"{local_jsonl}"}'
                ),
                encoding="utf-8",
            )
            (root / "20260325T171000Z_qwen25_14b_hybrid_rag_manifest.json").write_text(
                (
                    '{"study_id":"study_a","model_key":"qwen25_14b","mode":"hybrid_rag",'
                    '"timestamp":"2026-03-25T17:10:00Z","run_id":"run_hybrid",'
                    f'"response_jsonl_path":"{hybrid_jsonl}"}'
                ),
                encoding="utf-8",
            )

            report_path = export_hybrid_value_report(study_id="study_a", results_dir=root)

            self.assertIsNotNone(report_path)
            summary_df = pd.read_excel(report_path, sheet_name="summary")
            per_question_df = pd.read_excel(report_path, sheet_name="per_question")
            self.assertEqual(int(summary_df.loc[0, "hybrid_value_signal_questions"]), 1)
            self.assertTrue(bool(per_question_df.loc[0, "hybrid_has_web_citations"]))
            self.assertTrue(bool(per_question_df.loc[0, "hybrid_value_signal"]))


if __name__ == "__main__":
    unittest.main()
