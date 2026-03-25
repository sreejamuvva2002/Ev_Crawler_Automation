from pathlib import Path
import json
import sys
import tempfile
import unittest
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import eval_runner
from src.ev_llm_compare.models import GenerationMetadata
from src.ev_llm_compare.schemas import Chunk, RetrievalResult
from src.ev_llm_compare.settings import AppConfig


class _FakeExcelChunkBuilder:
    def __init__(self, settings: object):
        self.settings = settings

    def build(self, rows: list[object], notes: list[object]) -> list[Chunk]:
        return [
            Chunk(
                chunk_id="local_chunk",
                text="Alpha supplies battery packs in Georgia.",
                metadata={
                    "chunk_type": "row_full",
                    "source_file": "local.xlsx",
                    "sheet_name": "Data",
                },
            )
        ]


class _FakeOfflineDocs:
    def __init__(self) -> None:
        self.records = [object()]
        self.issues: list[object] = []


class _FakeRetriever:
    def __init__(self, chunks: list[Chunk], settings: object, qdrant_path: Path, **kwargs: object):
        self.chunks = chunks
        self.settings = settings
        self.collection_name = str(kwargs.get("collection_name") or "fake_collection")
        self.collection_fingerprint = f"fingerprint_{self.collection_name}"
        self.collection_manifest_path = Path(qdrant_path) / "_index_manifests" / f"{self.collection_name}.json"
        self.collection_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.collection_manifest_path.write_text("{}", encoding="utf-8")
        self.client = object()

    def collection_manifest_metadata(self) -> dict[str, object]:
        return {
            "collection_name": self.collection_name,
            "fingerprint": self.collection_fingerprint,
            "chunk_count": len(self.chunks),
        }

    def retrieve(self, question: str, top_k: int | None = None) -> list[RetrievalResult]:
        if self.collection_name == "local":
            return [
                RetrievalResult(
                    chunk_id="local_chunk",
                    text="Alpha supplies battery packs in Georgia.",
                    metadata={
                        "chunk_type": "row_full",
                        "source_file": "local.xlsx",
                        "sheet_name": "Data",
                    },
                    dense_score=0.9,
                    lexical_score=0.8,
                    final_score=0.85,
                )
            ]
        return [
            RetrievalResult(
                chunk_id="web_chunk",
                text="Alpha also appears in offline Tavily evidence.",
                metadata={
                    "chunk_type": "document_chunk",
                    "filepath": "data/tavily ready documents/doc.html",
                    "url": "https://example.com/doc",
                },
                dense_score=0.88,
                lexical_score=0.7,
                final_score=0.8,
            )
        ]

    def close(self) -> None:
        return None


class CanonicalEvalRunnerTests(unittest.TestCase):
    def test_no_rag_does_not_touch_retrieval_or_workbook_loading(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            questions_path = tmp_root / "questions.csv"
            questions_path.write_text("q_id,question\nQ1,What is the EV supply chain?\n", encoding="utf-8")
            out_path = tmp_root / "responses.jsonl"
            results_dir = tmp_root / "results"

            with patch.object(
                sys,
                "argv",
                [
                    "eval_runner.py",
                    "--model",
                    "qwen25_14b",
                    "--mode",
                    "no_rag",
                    "--questions",
                    str(questions_path),
                    "--out",
                    str(out_path),
                    "--results_dir",
                    str(results_dir),
                ],
            ), patch.object(eval_runner, "create_client", return_value=object()), patch.object(
                eval_runner,
                "safe_generate_with_metadata",
                return_value=(
                    "- Baseline answer",
                    0.01,
                    True,
                    None,
                    GenerationMetadata(prompt_tokens=10, completion_tokens=20, total_tokens=30),
                ),
            ), patch.object(
                eval_runner,
                "load_workbook",
                side_effect=AssertionError("no_rag should not load the workbook"),
            ), patch.object(
                eval_runner,
                "HybridRetriever",
                side_effect=AssertionError("no_rag should not instantiate retrievers"),
            ):
                rc = eval_runner.main()

            self.assertEqual(rc, 0)
            manifest_path = results_dir / next(name for name in (results_dir.iterdir()) if name.name.endswith("_manifest.json")).name
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["mode"], "no_rag")
            self.assertEqual(payload["model_key"], "qwen25_14b")
            self.assertEqual(payload["local_collection_name"], "")

    def test_hybrid_uses_offline_tavily_folder_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            questions_path = tmp_root / "questions.csv"
            questions_path.write_text("q_id,question\nQ1,Who supplies battery packs?\n", encoding="utf-8")
            local_workbook = tmp_root / "local.xlsx"
            local_workbook.write_text("placeholder", encoding="utf-8")
            tavily_dir = tmp_root / "tavily ready documents"
            tavily_dir.mkdir(parents=True, exist_ok=True)
            (tavily_dir / "tavily_ready_documents_manifest.csv").write_text(
                "Row_Number,Candidate_ID\n2,CAND_1\n",
                encoding="utf-8",
            )
            out_path = tmp_root / "responses.jsonl"
            results_dir = tmp_root / "results"

            with patch.object(
                sys,
                "argv",
                [
                    "eval_runner.py",
                    "--model",
                    "gemini25_flash",
                    "--mode",
                    "hybrid_rag",
                    "--questions",
                    str(questions_path),
                    "--data_workbook",
                    str(local_workbook),
                    "--tavily_dir",
                    str(tavily_dir),
                    "--out",
                    str(out_path),
                    "--results_dir",
                    str(results_dir),
                ],
            ), patch.object(eval_runner, "create_client", return_value=object()), patch.object(
                eval_runner,
                "safe_generate_with_metadata",
                return_value=(
                    "- Alpha supplies battery packs [DOC:local_chunk]\n"
                    "- Alpha also appears in offline web evidence [WEB:web_chunk]",
                    0.01,
                    True,
                    None,
                    GenerationMetadata(prompt_tokens=10, completion_tokens=20, total_tokens=30),
                ),
            ), patch.object(
                eval_runner,
                "load_workbook",
                return_value=(["row"], []),
            ), patch.object(
                eval_runner,
                "ExcelChunkBuilder",
                _FakeExcelChunkBuilder,
            ), patch.object(
                eval_runner,
                "build_derived_summary_chunks",
                return_value=[],
            ), patch.object(
                eval_runner,
                "HybridRetriever",
                _FakeRetriever,
            ), patch.object(
                eval_runner,
                "load_offline_documents",
                return_value=_FakeOfflineDocs(),
            ) as offline_loader, patch.object(
                eval_runner,
                "build_document_chunks",
                return_value=[
                    Chunk(
                        chunk_id="web_chunk",
                        text="Alpha also appears in offline Tavily evidence.",
                        metadata={
                            "chunk_type": "document_chunk",
                            "filepath": "data/tavily ready documents/doc.html",
                            "url": "https://example.com/doc",
                        },
                    )
                ],
            ), patch.object(
                eval_runner,
                "make_judge_client",
                return_value=(None, None),
            ):
                rc = eval_runner.main()

            self.assertEqual(rc, 0)
            offline_loader.assert_called_once()
            called_root = offline_loader.call_args.kwargs["root"]
            self.assertEqual(Path(called_root), tavily_dir.resolve())

            manifest_path = next(results_dir.glob("*_manifest.json"))
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["mode"], "hybrid_rag")
            self.assertEqual(Path(payload["offline_tavily_path"]), tavily_dir.resolve())
            self.assertTrue(payload["offline_tavily_manifest_path"].endswith("tavily_ready_documents_manifest.csv"))


if __name__ == "__main__":
    unittest.main()

