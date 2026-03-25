#!/usr/bin/env python3
"""
Single-model evaluation runner for EV_Research RAG experiments.

Usage examples:
  python eval_runner.py --model qwen14b --mode no_rag --out artifacts/response_outputs/qwen14b_no_rag.jsonl
  python eval_runner.py --model gemma27b --mode local_rag --out artifacts/response_outputs/gemma27b_local_rag.jsonl
  python eval_runner.py --model gemini_flash --mode hybrid_rag --out artifacts/response_outputs/gemini_hybrid.jsonl --reindex

Question source:
  By default the runner reads questions from data/GNEM_Golden_Questions.xlsx.
  You can still override this with --questions and pass .xlsx, .csv, or .json.

CHANGELOG:
  - Preserves question ids and duplicate question text for reproducible logging instead of deduping by question.
  - Uses persistent named Qdrant collections ("local" and "tavily") with fingerprint manifests so an index is only treated as current when the source content still matches.
  - Keeps non-RAG execution fully context-free and never attaches retrieved chunks to non-RAG answers.
  - Reuses the repo's existing non-RAG prompt/system instructions so standalone answers stay substantive instead of defaulting to "I don't know."
  - Writes an analyst-facing Excel workbook alongside the raw JSONL log, with separate context and pretrained-data columns.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ev_llm_compare.chunking import ExcelChunkBuilder
from ev_llm_compare.derived_analytics import build_derived_summary_chunks
from ev_llm_compare.excel_loader import load_workbook, normalize_cell
from ev_llm_compare.models import GenerationMetadata, create_client, safe_generate_with_metadata
from ev_llm_compare.offline_corpus import build_document_chunks, load_offline_documents, resolve_tavily_root
from ev_llm_compare.prompts import NON_RAG_SYSTEM_PROMPT
from ev_llm_compare.retrieval import HybridRetriever
from ev_llm_compare.schemas import RetrievalResult
from ev_llm_compare.settings import ModelSpec, load_config

try:
    import tiktoken
except ImportError:
    tiktoken = None  # type: ignore[assignment]

PROMPT_A = (
    "Answer from your general model knowledge only.\n"
    "Do not use retrieved context.\n\n"
    "Goal: provide the most useful baseline answer while avoiding fabricated specifics.\n"
    "- If you are confident about a specific company, location, or partnership, you may name it.\n"
    "- If you are not confident, do NOT invent names or exact locations. Instead describe likely categories and give 1-2 clearly labeled examples (for example: 'illustrative examples (may not be GA/AL-specific)').\n"
    "- If asked for a list of specific entities you cannot verify, say so briefly and provide: (a) what types of entities would be Tier 1/Tier 2, and (b) a cautious short list labeled 'illustrative examples'.\n"
    "- Do not mention missing workbooks or datasets.\n"
    "Provide 3-7 bullet points."
)
PROMPT_B = (
    "Use ONLY the CONTEXT. Do not guess.\n"
    "If the CONTEXT partially answers the question, answer only the supported part.\n"
    "If some requested details are missing, add a section titled 'Missing info:' and list the missing pieces.\n"
    "Only say 'Not found in provided context.' if the CONTEXT contains no relevant evidence at all.\n"
    "Provide 3-7 bullet points for supported findings.\n"
    "Every factual bullet must end with one or more citations using only these formats: [DOC:<id>] and/or [WEB:<id>].\n"
    "Then include: Evidence: list the context IDs you used."
)
EVAL_SYSTEM_PROMPT = "Follow the user instructions exactly."
NON_RAG_EVAL_SYSTEM_PROMPT = (
    NON_RAG_SYSTEM_PROMPT
    + "\nPrefer a substantive domain answer over abstaining when you can provide useful general knowledge."
)

QUESTION_COLUMN_CANDIDATES = {
    "question",
    "questions",
    "query",
    "prompt",
    "sample query",
}
QID_COLUMN_CANDIDATES = {"q_id", "id", "question id", "question_id", "#"}
EXPECTED_ANSWER_COLUMN_CANDIDATES = {
    "expected_answer",
    "expected answer",
    "golden answer",
    "golden_answer",
    "answer",
    "reference answer",
    "reference_answer",
}
KEY_FACTS_COLUMN_CANDIDATES = {"key_facts", "key facts", "facts"}


@dataclass(slots=True)
class EvalQuestion:
    q_id: str
    question: str
    expected_answer: str | None = None
    key_facts: Any = None


@dataclass(slots=True)
class ContextBuildResult:
    text: str
    local_text: str
    web_text: str
    local_chars: int
    local_tokens: int
    web_chars: int
    web_tokens: int
    total_chars: int
    total_tokens: int


class TokenCounter:
    def __init__(self) -> None:
        self._encoder = None
        if tiktoken is not None:
            try:
                self._encoder = tiktoken.get_encoding("cl100k_base")
            except Exception:
                self._encoder = None

    def count(self, text: str) -> int:
        if not text:
            return 0
        if self._encoder is not None:
            return len(self._encoder.encode(text))
        return max(1, len(text) // 4)

    def truncate(self, text: str, max_tokens: int) -> str:
        if max_tokens <= 0 or not text:
            return ""
        if self.count(text) <= max_tokens:
            return text
        if self._encoder is not None:
            tokens = self._encoder.encode(text)[:max_tokens]
            return self._encoder.decode(tokens).strip()
        return text[: max(0, max_tokens * 4)].strip()


def build_parser(default_questions_path: Path) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one selected model and one selected mode for offline EV RAG evaluation."
    )
    parser.add_argument("--model", choices=["qwen14b", "gemma27b", "gemini_flash"], required=True)
    parser.add_argument("--mode", choices=["no_rag", "local_rag", "hybrid_rag"], required=True)
    parser.add_argument(
        "--questions",
        default=str(default_questions_path),
        help="Path to the questions file (.xlsx, .csv, or .json). Defaults to data/GNEM_Golden_Questions.xlsx.",
    )
    parser.add_argument("--out", required=True, help="Append-safe JSONL output path.")
    parser.add_argument("--top_k_local", type=int, default=None, help="Local retrieval top-k.")
    parser.add_argument("--top_k_tavily", type=int, default=3, help="Offline Tavily retrieval top-k.")
    parser.add_argument(
        "--context_budget_tokens",
        type=int,
        default=None,
        help="Approximate context token budget used for assembled context blocks.",
    )
    parser.add_argument("--reindex", action="store_true", help="Rebuild the Tavily index before retrieval.")
    parser.add_argument("--seed", type=int, default=None, help="Optional generation seed.")
    parser.add_argument("--max_questions", type=int, default=None, help="Optional max question count.")
    parser.add_argument(
        "--data_workbook",
        default=None,
        help="Optional override for the local workbook corpus. Defaults to data/GNEM updated excel.xlsx when present.",
    )
    parser.add_argument(
        "--tavily_dir",
        default=None,
        help="Optional override for the offline Tavily directory. Defaults to data/tavily or data/tavily ready documents.",
    )
    parser.add_argument(
        "--excel_out",
        default=None,
        help="Optional workbook output path. Defaults to the JSONL path with an .xlsx suffix.",
    )
    return parser


def resolve_default_questions_path() -> Path:
    candidates = [
        PROJECT_ROOT / "data" / "GNEM_Golden_Questions.xlsx",
        PROJECT_ROOT / "data" / "GNEM_Generated_Questions.xlsx",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError("Could not find a default questions workbook in the data directory.")


def resolve_default_workbook_path() -> Path:
    candidates = [
        PROJECT_ROOT / "data" / "GNEM updated excel.xlsx",
        PROJECT_ROOT / "GNEM updated excel (1).xlsx",
        PROJECT_ROOT / "GNEM updated excel.xlsx",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    searched = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"Could not find the local workbook corpus. Checked: {searched}")


def normalize_header(value: object) -> str:
    return normalize_cell(value).lower()


def load_eval_questions(path: str | Path, max_questions: int | None = None) -> list[EvalQuestion]:
    question_path = Path(path).expanduser().resolve()
    suffix = question_path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        df = _load_tabular_with_header_detection(question_path, excel=True)
    elif suffix == ".csv":
        df = _load_tabular_with_header_detection(question_path, excel=False)
    elif suffix == ".json":
        return _load_json_questions(question_path, max_questions=max_questions)
    else:
        raise ValueError("Questions file must be .xlsx, .xls, .csv, or .json")

    questions = _questions_from_dataframe(df)
    if max_questions is not None:
        questions = questions[:max_questions]
    if not questions:
        raise ValueError(f"No valid questions found in {question_path}")
    return questions


def _load_tabular_with_header_detection(path: Path, *, excel: bool) -> pd.DataFrame:
    reader = pd.read_excel if excel else pd.read_csv
    df = reader(path)
    if _has_question_columns(df.columns):
        return df

    raw_df = reader(path, header=None)
    promoted = _promote_header_row(raw_df)
    if promoted is None:
        return df
    return promoted


def _has_question_columns(columns: Any) -> bool:
    normalized = {normalize_header(column) for column in columns}
    return bool(normalized & QUESTION_COLUMN_CANDIDATES)


def _promote_header_row(raw_df: pd.DataFrame) -> pd.DataFrame | None:
    for row_index in range(min(10, len(raw_df))):
        candidate_headers = [normalize_cell(value) for value in raw_df.iloc[row_index].tolist()]
        normalized = {header.lower() for header in candidate_headers if header}
        if normalized & QUESTION_COLUMN_CANDIDATES:
            headers = [
                header if header else f"column_{column_index + 1}"
                for column_index, header in enumerate(candidate_headers)
            ]
            data = raw_df.iloc[row_index + 1 :].copy().reset_index(drop=True)
            data.columns = headers
            return data
    return None


def _questions_from_dataframe(df: pd.DataFrame) -> list[EvalQuestion]:
    cleaned_columns = [normalize_cell(column) for column in df.columns]
    df = df.copy()
    df.columns = cleaned_columns
    question_column = _pick_column(cleaned_columns, QUESTION_COLUMN_CANDIDATES)
    if question_column is None:
        raise ValueError("Questions sheet must contain a question column.")

    qid_column = _pick_column(cleaned_columns, QID_COLUMN_CANDIDATES)
    expected_answer_column = _pick_column(cleaned_columns, EXPECTED_ANSWER_COLUMN_CANDIDATES)
    key_facts_column = _pick_column(cleaned_columns, KEY_FACTS_COLUMN_CANDIDATES)

    questions: list[EvalQuestion] = []
    for index, row in df.iterrows():
        question_text = normalize_cell(row.get(question_column))
        if len(question_text) < 5:
            continue
        q_id = normalize_cell(row.get(qid_column)) if qid_column else str(index + 1)
        expected_answer = normalize_cell(row.get(expected_answer_column)) if expected_answer_column else ""
        key_facts = row.get(key_facts_column) if key_facts_column else None
        if isinstance(key_facts, float) and pd.isna(key_facts):
            key_facts = None
        questions.append(
            EvalQuestion(
                q_id=q_id or str(index + 1),
                question=question_text,
                expected_answer=expected_answer or None,
                key_facts=key_facts,
            )
        )
    return questions


def _load_json_questions(path: Path, max_questions: int | None = None) -> list[EvalQuestion]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        if isinstance(payload.get("questions"), list):
            items = payload["questions"]
        else:
            items = [{"q_id": key, "question": value} for key, value in payload.items()]
    elif isinstance(payload, list):
        items = payload
    else:
        raise ValueError("JSON questions file must contain a list or an object.")

    questions: list[EvalQuestion] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise ValueError("Each JSON question entry must be an object.")
        question_text = normalize_cell(item.get("question") or item.get("query") or item.get("prompt"))
        if len(question_text) < 5:
            continue
        questions.append(
            EvalQuestion(
                q_id=normalize_cell(item.get("q_id") or item.get("id") or index),
                question=question_text,
                expected_answer=normalize_cell(item.get("expected_answer")),
                key_facts=item.get("key_facts"),
            )
        )
        if max_questions is not None and len(questions) >= max_questions:
            break
    return questions


def _pick_column(columns: list[str], candidates: set[str]) -> str | None:
    for column in columns:
        if normalize_header(column) in candidates:
            return column
    return None


def build_model_spec(model_name: str, mode: str) -> ModelSpec:
    default_temperature = float(os.getenv("MODEL_TEMPERATURE", "0.1"))
    default_max_tokens = int(os.getenv("MODEL_MAX_TOKENS", "1600"))
    model_map = {
        "qwen14b": (
            "ollama",
            os.getenv("QWEN14B_MODEL") or os.getenv("QWEN_MODEL") or "qwen3:14b",
        ),
        "gemma27b": (
            "ollama",
            os.getenv("GEMMA27B_MODEL") or os.getenv("GEMMA_MODEL") or "gemma3:27b",
        ),
        "gemini_flash": (
            "gemini",
            os.getenv("GEMINI_FLASH_MODEL") or os.getenv("GEMINI_MODEL") or "gemini-2.5-flash",
        ),
    }
    provider, resolved_name = model_map[model_name]
    return ModelSpec(
        run_name=f"{model_name}_{mode}",
        provider=provider,
        model_name=resolved_name,
        rag_enabled=mode != "no_rag",
        temperature=default_temperature,
        max_tokens=default_max_tokens,
    )


def route_question(question: str) -> str:
    normalized = re.sub(r"\s+", " ", question.lower()).strip()
    web_needed_terms = {
        "international import",
        "international imports",
        "imports",
        "growing fastest",
        "growing",
        "demand",
        "regionally",
        "workforce capacity",
        "shifting from ice",
        "shifting from ice",
        "ice components",
    }
    analytic_terms = {
        "highest",
        "lowest",
        "versus",
        " vs ",
        "compare",
        "comparison",
        "concentration",
        "risk",
        "bottleneck",
        "dependencies",
        "cluster",
        "clusters",
        "outside of major metros",
        "combine",
        "counties",
    }
    if any(term in normalized for term in web_needed_terms):
        return "web_needed"
    if any(term in normalized for term in analytic_terms):
        return "analytic"
    return "lookup"


def question_top_k_local(route_label: str, default_top_k: int) -> int:
    if route_label == "analytic":
        return max(default_top_k, 7)
    return default_top_k


def question_top_k_tavily(route_label: str, default_top_k: int) -> int:
    if route_label == "web_needed":
        return max(default_top_k, 5)
    if route_label == "analytic":
        return max(default_top_k, 4)
    return default_top_k


def build_context_result(
    mode: str,
    local_results: list[RetrievalResult],
    tavily_results: list[RetrievalResult],
    context_budget_tokens: int,
    token_counter: TokenCounter,
    *,
    route_label: str | None = None,
) -> ContextBuildResult:
    local_header = "LOCAL_CONTEXT:\n"
    web_header = "WEB_CONTEXT:\n"
    total_budget = max(1, context_budget_tokens)

    local_blocks = [_format_local_block(result) for result in local_results]
    web_blocks = [_format_web_block(result) for result in tavily_results]

    if mode == "hybrid_rag" and route_label == "web_needed":
        local_budget = max(1, int(total_budget * 0.45))
        web_budget = max(1, total_budget - local_budget)
    else:
        local_budget = total_budget
        web_budget = None

    local_section, local_tokens = _fit_context_blocks(
        local_header,
        local_blocks,
        local_budget,
        token_counter,
        fallback_text="No local context retrieved.",
    )

    sections = [local_section]
    local_section_chars = len(local_section)

    web_section = ""
    web_tokens = 0
    if mode == "hybrid_rag":
        remaining_budget = web_budget if web_budget is not None else max(0, total_budget - local_tokens)
        web_section, web_tokens = _fit_context_blocks(
            web_header,
            web_blocks,
            remaining_budget,
            token_counter,
            fallback_text="No web context retained within the current budget.",
        )
        sections.append(web_section)

    context_text = "\n\n".join(section for section in sections if section)
    return ContextBuildResult(
        text=context_text,
        local_text=local_section,
        web_text=web_section,
        local_chars=local_section_chars,
        local_tokens=local_tokens,
        web_chars=len(web_section),
        web_tokens=web_tokens,
        total_chars=len(context_text),
        total_tokens=token_counter.count(context_text),
    )


def _fit_context_blocks(
    header: str,
    blocks: list[str],
    budget_tokens: int,
    token_counter: TokenCounter,
    *,
    fallback_text: str,
) -> tuple[str, int]:
    header_tokens = token_counter.count(header)
    if budget_tokens <= header_tokens:
        section_text = header + fallback_text
        return section_text, token_counter.count(section_text)

    remaining_budget = budget_tokens - header_tokens
    selected_blocks: list[str] = []
    used_tokens = header_tokens
    if not blocks:
        section_text = header + fallback_text
        return section_text, token_counter.count(section_text)

    for block in blocks:
        block_tokens = token_counter.count(block)
        if block_tokens <= remaining_budget:
            selected_blocks.append(block)
            used_tokens += block_tokens
            remaining_budget -= block_tokens
            continue

        truncated = token_counter.truncate(block, remaining_budget)
        if truncated:
            selected_blocks.append(truncated)
            used_tokens = header_tokens + sum(token_counter.count(item) for item in selected_blocks)
        break

    section_body = "\n\n".join(selected_blocks) if selected_blocks else fallback_text
    section_text = header + section_body
    return section_text, token_counter.count(section_text)


def _format_local_block(result: RetrievalResult) -> str:
    return f"[DOC:{result.chunk_id}] {result.text}"


def _format_web_block(result: RetrievalResult) -> str:
    metadata = result.metadata
    prefix_parts = []
    filepath = normalize_cell(metadata.get("filepath"))
    url = normalize_cell(metadata.get("url"))
    if filepath:
        prefix_parts.append(f"filepath={filepath}")
    if url:
        prefix_parts.append(f"url={url}")
    prefix = " | ".join(prefix_parts)
    if prefix:
        return f"[WEB:{result.chunk_id}] {prefix} | {result.text}"
    return f"[WEB:{result.chunk_id}] {result.text}"


def build_prompt(mode: str, question: str, context_text: str | None = None) -> tuple[str, str]:
    if mode == "no_rag":
        return f"{PROMPT_A}\n\nQuestion: {question}", "A"
    if not context_text:
        context_text = "LOCAL_CONTEXT:\nNo local context retrieved."
    prompt = f"{PROMPT_B}\n\nCONTEXT:\n{context_text}\n\nQuestion: {question}"
    return prompt, "B"


def build_system_prompt(mode: str) -> str:
    if mode == "no_rag":
        return NON_RAG_EVAL_SYSTEM_PROMPT
    return EVAL_SYSTEM_PROMPT


def extract_citations(answer_text: str, valid_doc_ids: set[str], valid_web_ids: set[str]) -> dict[str, list[str]]:
    doc_ids = {
        match.group(1)
        for match in re.finditer(r"\[DOC:([^\]]+)\]", answer_text or "", flags=re.IGNORECASE)
        if match.group(1) in valid_doc_ids
    }
    web_ids = {
        match.group(1)
        for match in re.finditer(r"\[WEB:([^\]]+)\]", answer_text or "", flags=re.IGNORECASE)
        if match.group(1) in valid_web_ids
    }
    if not doc_ids and not web_ids:
        evidence_lines = [
            line.strip()
            for line in (answer_text or "").splitlines()
            if line.strip().lower().startswith("evidence:")
        ]
        for line in evidence_lines:
            for candidate in valid_doc_ids:
                if candidate in line:
                    doc_ids.add(candidate)
            for candidate in valid_web_ids:
                if candidate in line:
                    web_ids.add(candidate)
    return {
        "doc_ids": sorted(doc_ids),
        "web_ids": sorted(web_ids),
    }


def preview_text(text: str, limit: int = 220) -> str:
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def build_retrieval_log(results: list[RetrievalResult], *, source_kind: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for result in results:
        metadata = result.metadata
        entry = {
            "chunk_id": result.chunk_id,
            "score": result.final_score,
            "text_preview": preview_text(result.text),
            "chunk_type": normalize_cell(metadata.get("chunk_type")),
        }
        if source_kind == "local":
            source_file = normalize_cell(metadata.get("source_file"))
            sheet_name = normalize_cell(metadata.get("sheet_name"))
            entry["source"] = "::".join(part for part in [source_file, sheet_name] if part) or source_file
            entry["analysis_type"] = normalize_cell(metadata.get("analysis_type"))
        else:
            entry["filepath"] = normalize_cell(metadata.get("filepath"))
            entry["url"] = normalize_cell(metadata.get("url"))
        entries.append(entry)
    return entries


def generation_usage_for_log(model_name: str, metadata: GenerationMetadata) -> dict[str, Any]:
    if model_name != "gemini_flash":
        return {
            "tokens_in": None,
            "tokens_out": None,
            "tokens_total": None,
            "cost_usd": None,
        }
    return {
        "tokens_in": metadata.prompt_tokens,
        "tokens_out": metadata.completion_tokens,
        "tokens_total": metadata.total_tokens,
        "cost_usd": metadata.cost_usd,
    }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def split_answer_source_data(
    mode: str,
    question: str,
    answer_text: str,
    retrieved_chunks: list[RetrievalResult],
    citations: dict[str, list[str]],
) -> tuple[str, str]:
    if mode == "no_rag":
        return "", answer_text

    lines = [line.rstrip() for line in (answer_text or "").splitlines()]
    if not lines:
        return "", ""

    evidence_lines = [line for line in lines if line.strip().lower().startswith("evidence:")]
    content_lines = [line for line in lines if line not in evidence_lines]
    if citations["doc_ids"] or citations["web_ids"]:
        knowledge = "\n".join(content_lines).strip()
        pretrained = ""
    else:
        # Prompt B is context-only, so uncited content is still treated as grounded unless retrieval was empty.
        knowledge = "\n".join(content_lines).strip() if retrieved_chunks else ""
        pretrained = "" if retrieved_chunks else "\n".join(content_lines).strip()

    if evidence_lines:
        knowledge = "\n".join(part for part in [knowledge, "\n".join(evidence_lines).strip()] if part).strip()
    return knowledge, pretrained


def retrieval_blocks_for_excel(results: list[RetrievalResult], *, source_kind: str) -> str:
    blocks: list[str] = []
    for result in results:
        if source_kind == "local":
            header = f"{result.chunk_id} | {normalize_cell(result.metadata.get('source_file'))} | {normalize_cell(result.metadata.get('sheet_name'))}"
            blocks.append(f"{header}\n{result.text}")
        else:
            metadata = result.metadata
            header = " | ".join(
                part
                for part in [
                    result.chunk_id,
                    normalize_cell(metadata.get("filepath")),
                    normalize_cell(metadata.get("url")),
                ]
                if part
            )
            blocks.append(f"{header}\n{result.text}")
    return "\n\n".join(blocks).strip()


def build_excel_row(
    record: dict[str, Any],
    context_result: ContextBuildResult | None,
    local_results: list[RetrievalResult],
    tavily_results: list[RetrievalResult],
    knowledge_source_data: str,
    pretrained_data: str,
) -> dict[str, Any]:
    citations = record["citations_extracted"]
    local_sources = [entry.get("source", "") for entry in record["local_retrieval"]]
    tavily_sources = [
        " | ".join(part for part in [entry.get("filepath", ""), entry.get("url", "")] if part)
        for entry in record["tavily_retrieval"]
    ]
    return {
        "run_id": record["run_id"],
        "q_id": record["q_id"],
        "question": record["question"],
        "expected_answer": record.get("expected_answer"),
        "model": record["model"],
        "resolved_model_name": record["resolved_model_name"],
        "mode": record["mode"],
        "question_route": record.get("question_route"),
        "prompt_used": record["prompt_used"],
        "model_response": record["answer_text"],
        "knowledge_source_data": knowledge_source_data,
        "pretrained_data": pretrained_data,
        "local_context_used": context_result.local_text if context_result else "",
        "web_context_used": context_result.web_text if context_result else "",
        "combined_context_used": context_result.text if context_result else "",
        "local_chunk_ids": "\n".join(entry["chunk_id"] for entry in record["local_retrieval"]),
        "local_chunk_sources": "\n".join(source for source in local_sources if source),
        "local_chunk_texts": retrieval_blocks_for_excel(local_results, source_kind="local"),
        "tavily_chunk_ids": "\n".join(entry["chunk_id"] for entry in record["tavily_retrieval"]),
        "tavily_chunk_sources": "\n".join(source for source in tavily_sources if source),
        "tavily_chunk_texts": retrieval_blocks_for_excel(tavily_results, source_kind="tavily"),
        "doc_citations": ", ".join(citations["doc_ids"]),
        "web_citations": ", ".join(citations["web_ids"]),
        "created_at": record["created_at"],
    }


def export_excel_workbook(
    workbook_path: Path,
    excel_rows: list[dict[str, Any]],
    raw_records: list[dict[str, Any]],
    *,
    local_retrieval_rows: list[dict[str, Any]],
    tavily_retrieval_rows: list[dict[str, Any]],
) -> None:
    ensure_parent(workbook_path)
    responses_df = pd.DataFrame(excel_rows)
    raw_df = pd.DataFrame(raw_records)
    local_df = pd.DataFrame(local_retrieval_rows)
    tavily_df = pd.DataFrame(tavily_retrieval_rows)

    raw_columns = [
        "run_id",
        "q_id",
        "question",
        "model",
        "resolved_model_name",
        "mode",
        "question_route",
        "prompt_used",
        "answer_text",
        "success",
        "error_message",
        "created_at",
        "timing",
        "tokens_in",
        "tokens_out",
        "tokens_total",
        "cost_usd",
    ]
    raw_export_df = raw_df[[column for column in raw_columns if column in raw_df.columns]] if not raw_df.empty else raw_df

    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        responses_df.to_excel(writer, sheet_name="responses", index=False)
        raw_export_df.to_excel(writer, sheet_name="raw_log", index=False)
        local_df.to_excel(writer, sheet_name="local_retrieval", index=False)
        tavily_df.to_excel(writer, sheet_name="tavily_retrieval", index=False)


def main() -> int:
    config = load_config()
    default_questions_path = resolve_default_questions_path()
    parser = build_parser(default_questions_path)
    args = parser.parse_args()

    top_k_local = args.top_k_local or max(1, config.retrieval.generation_context_result_limit or 5)
    context_budget_tokens = (
        args.context_budget_tokens
        or max(800, config.retrieval.generation_context_char_budget // 4)
    )
    questions = load_eval_questions(args.questions, max_questions=args.max_questions)
    spec = build_model_spec(args.model, args.mode)
    client = create_client(spec, config.runtime)
    token_counter = TokenCounter()

    local_retriever: HybridRetriever | None = None
    tavily_retriever: HybridRetriever | None = None
    tavily_issues: list[str] = []

    try:
        if args.mode in {"local_rag", "hybrid_rag"}:
            data_workbook = Path(args.data_workbook).expanduser().resolve() if args.data_workbook else resolve_default_workbook_path()
            rows, notes = load_workbook(data_workbook)
            local_chunks = ExcelChunkBuilder(config.retrieval).build(rows, notes)
            local_chunks.extend(build_derived_summary_chunks(rows))
            local_retriever = HybridRetriever(
                chunks=local_chunks,
                settings=config.retrieval,
                qdrant_path=config.runtime.qdrant_path,
                collection_name="local",
            )
            print(
                f"[eval_runner] Local corpus ready from {data_workbook} "
                f"with {len(local_chunks)} chunks in collection 'local'.",
                flush=True,
            )

        if args.mode == "hybrid_rag":
            tavily_root = (
                Path(args.tavily_dir).expanduser().resolve()
                if args.tavily_dir
                else resolve_tavily_root(PROJECT_ROOT)
            )
            tavily_docs = load_offline_documents(tavily_root, source_type="tavily")
            tavily_chunks = build_document_chunks(tavily_docs.records, config.retrieval)
            tavily_retriever = HybridRetriever(
                chunks=tavily_chunks,
                settings=config.retrieval,
                qdrant_path=config.runtime.qdrant_path,
                collection_name="tavily",
                force_reindex=args.reindex,
                client=local_retriever.client if local_retriever is not None else None,
            )
            tavily_issues = [f"{issue.filepath}: {issue.reason}" for issue in tavily_docs.issues]
            print(
                f"[eval_runner] Offline Tavily corpus ready from {tavily_root} "
                f"with {len(tavily_docs.records)} documents and {len(tavily_chunks)} chunks in collection 'tavily'.",
                flush=True,
            )
            if tavily_issues:
                print(
                    f"[eval_runner] Skipped {len(tavily_issues)} Tavily files during ingestion.",
                    flush=True,
                )

        out_path = Path(args.out).expanduser().resolve()
        excel_out_path = (
            Path(args.excel_out).expanduser().resolve()
            if args.excel_out
            else out_path.with_suffix(".xlsx")
        )
        ensure_parent(out_path)
        run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{args.model}_{args.mode}"
        raw_records: list[dict[str, Any]] = []
        excel_rows: list[dict[str, Any]] = []
        local_retrieval_rows: list[dict[str, Any]] = []
        tavily_retrieval_rows: list[dict[str, Any]] = []

        with out_path.open("a", encoding="utf-8") as handle:
            for index, item in enumerate(questions, start=1):
                print(
                    f"[eval_runner] {index}/{len(questions)} | {item.q_id} | {args.model} | {args.mode}",
                    flush=True,
                )

                question_route = route_question(item.question) if args.mode != "no_rag" else "no_rag"
                effective_top_k_local = question_top_k_local(question_route, top_k_local)
                effective_top_k_tavily = question_top_k_tavily(question_route, args.top_k_tavily)
                local_results: list[RetrievalResult] = []
                tavily_results: list[RetrievalResult] = []
                retrieval_ms_local = 0.0
                retrieval_ms_tavily = 0.0

                if args.mode in {"local_rag", "hybrid_rag"} and local_retriever is not None:
                    retrieval_start = time.perf_counter()
                    local_results = local_retriever.retrieve(item.question, top_k=effective_top_k_local)
                    retrieval_ms_local = round((time.perf_counter() - retrieval_start) * 1000, 2)

                if args.mode == "hybrid_rag" and tavily_retriever is not None:
                    retrieval_start = time.perf_counter()
                    tavily_results = tavily_retriever.retrieve(item.question, top_k=effective_top_k_tavily)
                    retrieval_ms_tavily = round((time.perf_counter() - retrieval_start) * 1000, 2)

                context_result = None
                if args.mode != "no_rag":
                    context_result = build_context_result(
                        args.mode,
                        local_results,
                        tavily_results,
                        context_budget_tokens,
                        token_counter,
                        route_label=question_route,
                    )

                prompt_text, prompt_used = build_prompt(
                    args.mode,
                    item.question,
                    context_result.text if context_result else None,
                )
                answer_text, generation_seconds, success, error_message, generation_metadata = safe_generate_with_metadata(
                    client,
                    prompt_text,
                    temperature=spec.temperature,
                    max_tokens=spec.max_tokens,
                    system_prompt=build_system_prompt(args.mode),
                    seed=args.seed,
                )

                valid_doc_ids = {result.chunk_id for result in local_results}
                valid_web_ids = {result.chunk_id for result in tavily_results}
                citations = extract_citations(answer_text, valid_doc_ids, valid_web_ids)
                knowledge_source_data, pretrained_data = split_answer_source_data(
                    args.mode,
                    item.question,
                    answer_text,
                    local_results + tavily_results,
                    citations,
                )
                record = {
                    "run_id": run_id,
                    "q_id": item.q_id,
                    "question": item.question,
                    "expected_answer": item.expected_answer,
                    "key_facts": item.key_facts,
                    "model": args.model,
                    "resolved_model_name": spec.model_name,
                    "mode": args.mode,
                    "question_route": question_route,
                    "local_retrieval": build_retrieval_log(local_results, source_kind="local"),
                    "tavily_retrieval": build_retrieval_log(tavily_results, source_kind="tavily"),
                    "context_lengths": {
                        "local_chars": context_result.local_chars if context_result else 0,
                        "local_tokens": context_result.local_tokens if context_result else 0,
                        "web_chars": context_result.web_chars if context_result else 0,
                        "web_tokens": context_result.web_tokens if context_result else 0,
                        "total_chars": context_result.total_chars if context_result else 0,
                        "total_tokens": context_result.total_tokens if context_result else 0,
                    },
                    "prompt_used": prompt_used,
                    "answer_text": answer_text,
                    "citations_extracted": citations,
                    "timing": {
                        "retrieval_ms_local": retrieval_ms_local,
                        "retrieval_ms_tavily": retrieval_ms_tavily,
                        "generation_ms": round(generation_seconds * 1000, 2),
                    },
                    "tokens_in": generation_usage_for_log(args.model, generation_metadata)["tokens_in"],
                    "tokens_out": generation_usage_for_log(args.model, generation_metadata)["tokens_out"],
                    "tokens_total": generation_usage_for_log(args.model, generation_metadata)["tokens_total"],
                    "cost_usd": generation_usage_for_log(args.model, generation_metadata)["cost_usd"],
                    "success": success,
                    "error_message": error_message,
                    "created_at": utc_now_iso(),
                }
                if tavily_issues and args.mode == "hybrid_rag":
                    record["tavily_ingestion_issues"] = tavily_issues
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                handle.flush()
                raw_records.append(record)
                excel_rows.append(
                    build_excel_row(
                        record,
                        context_result,
                        local_results,
                        tavily_results,
                        knowledge_source_data,
                        pretrained_data,
                    )
                )
                for rank, result in enumerate(local_results, start=1):
                    local_retrieval_rows.append(
                        {
                            "run_id": run_id,
                            "q_id": item.q_id,
                            "question": item.question,
                            "question_route": question_route,
                            "rank": rank,
                            "chunk_id": result.chunk_id,
                            "score": result.final_score,
                            "chunk_type": normalize_cell(result.metadata.get("chunk_type")),
                            "analysis_type": normalize_cell(result.metadata.get("analysis_type")),
                            "source_file": normalize_cell(result.metadata.get("source_file")),
                            "sheet_name": normalize_cell(result.metadata.get("sheet_name")),
                            "text": result.text,
                        }
                    )
                for rank, result in enumerate(tavily_results, start=1):
                    tavily_retrieval_rows.append(
                        {
                            "run_id": run_id,
                            "q_id": item.q_id,
                            "question": item.question,
                            "question_route": question_route,
                            "rank": rank,
                            "chunk_id": result.chunk_id,
                            "score": result.final_score,
                            "chunk_type": normalize_cell(result.metadata.get("chunk_type")),
                            "filepath": normalize_cell(result.metadata.get("filepath")),
                            "url": normalize_cell(result.metadata.get("url")),
                            "text": result.text,
                        }
                    )

        export_excel_workbook(
            excel_out_path,
            excel_rows,
            raw_records,
            local_retrieval_rows=local_retrieval_rows,
            tavily_retrieval_rows=tavily_retrieval_rows,
        )
        print(f"[eval_runner] Wrote {len(questions)} records to {out_path}", flush=True)
        print(f"[eval_runner] Wrote Excel workbook to {excel_out_path}", flush=True)
        return 0
    finally:
        if local_retriever is not None:
            local_retriever.close()
        if tavily_retriever is not None:
            tavily_retriever.close()


if __name__ == "__main__":
    raise SystemExit(main())
