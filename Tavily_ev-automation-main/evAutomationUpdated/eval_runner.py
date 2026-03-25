#!/usr/bin/env python3
"""
Canonical research runner for EV supply-chain evaluation experiments.

This runner is the thesis/research-grade evaluation surface for:
  - qwen25_14b
  - gemma27b
  - gemini25_flash

Experimental modes:
  - no_rag
  - local_rag
  - hybrid_rag

Methodological guarantees:
  - One model + one mode per invocation.
  - `no_rag` never builds or touches retrieval indexes.
  - `local_rag` retrieves only from the local workbook-derived corpus.
  - `hybrid_rag` retrieves from the local workbook-derived corpus plus offline Tavily docs
    stored locally. It never calls the live Tavily API.
  - RAG answers are context-only and must cite retrieved evidence IDs.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
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
from ev_llm_compare.models import create_client, safe_generate_with_metadata
from ev_llm_compare.offline_corpus import build_document_chunks, load_offline_documents, resolve_tavily_root
from ev_llm_compare.prompts import NON_RAG_SYSTEM_PROMPT
from ev_llm_compare.research_eval import (
    ABSTENTION_EXACT,
    SemanticSimilarityScorer,
    build_evidence_registry,
    citation_kind_for_result,
    citation_token_for_result,
    compute_golden_metrics,
    export_answers_workbook,
    export_hybrid_value_report,
    export_metrics_workbook,
    extract_citations,
    flatten_answer_row,
    flatten_metrics_row,
    load_golden_answers,
    resolve_git_commit,
    resolve_golden_answer,
    resolve_offline_tavily_manifest_hash,
    sha256_file,
    sha256_json,
    summarize_run_metrics,
    update_study_outputs,
    validate_rag_answer,
)
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
    "- If you are not confident, do NOT invent names or exact locations. Instead describe likely categories and give 1-2 clearly labeled examples.\n"
    "- If asked for a list of specific entities you cannot verify, say so briefly and provide a cautious short list labeled 'illustrative examples'.\n"
    "- Do not mention missing workbooks, missing datasets, or missing context.\n"
    "Provide 3-7 bullet points."
)
PROMPT_B = (
    "Use ONLY the CONTEXT. Do not guess.\n"
    "If the CONTEXT partially answers the question, answer only the supported part.\n"
    "If some requested details are missing, add a section titled 'Missing info:' and list the missing pieces.\n"
    "If nothing is supported, output exactly: Not found in provided context.\n"
    "Answer with 3-7 bullet points maximum for supported findings.\n"
    "Every factual bullet must end with one or more citations using only these formats: "
    "[DOC:<id>], [WEB:<id>], [ANALYTIC:<id>], [GEO:<id>].\n"
    "Then include an 'Evidence:' line listing the citation IDs you used."
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
MODEL_REGISTRY: dict[str, dict[str, Any]] = {
    "qwen25_14b": {
        "provider": "ollama",
        "env_names": ["QWEN25_14B_MODEL", "QWEN14B_MODEL", "QWEN_MODEL"],
        "default": "qwen2.5:14b",
    },
    "gemma27b": {
        "provider": "ollama",
        "env_names": ["GEMMA27B_MODEL", "GEMMA_MODEL"],
        "default": "gemma3:27b",
    },
    "gemini25_flash": {
        "provider": "gemini",
        "env_names": ["GEMINI25_FLASH_MODEL", "GEMINI_FLASH_MODEL", "GEMINI_MODEL"],
        "default": "gemini-2.5-flash",
    },
}
MODEL_ALIASES = {
    "qwen14b": "qwen25_14b",
    "gemma27b": "gemma27b",
    "gemini_flash": "gemini25_flash",
}
ABSTENTION_PATTERN = re.compile(
    r"\b(i don['’]t know|not found|cannot find|can['’]t find|insufficient information|not enough information|unable to find)\b",
    flags=re.IGNORECASE,
)


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


def available_model_choices() -> list[str]:
    return sorted(set(MODEL_REGISTRY) | set(MODEL_ALIASES))


def canonical_model_key(model_name: str) -> str:
    return MODEL_ALIASES.get(model_name, model_name)


def build_parser(default_questions_path: Path) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Canonical single-model/single-mode research runner for offline EV QA comparisons."
    )
    parser.add_argument("--model", choices=available_model_choices(), required=True)
    parser.add_argument("--mode", choices=["no_rag", "local_rag", "hybrid_rag"], required=True)
    parser.add_argument(
        "--questions",
        default=str(default_questions_path),
        help="Path to the questions file (.xlsx, .csv, or .json). Defaults to data/GNEM_Golden_Questions.xlsx.",
    )
    parser.add_argument(
        "--golden_answers",
        default=None,
        help="Optional workbook/CSV containing q_id, question, golden_answer, and optional metadata columns.",
    )
    parser.add_argument(
        "--golden_sheet",
        default=None,
        help="Optional sheet name when --golden_answers points to an Excel workbook.",
    )
    parser.add_argument(
        "--out",
        default="",
        help="Append-safe JSONL output path. Defaults to artifacts/response_outputs/<run_id>.jsonl.",
    )
    parser.add_argument("--top_k_local", type=int, default=None, help="Local retrieval top-k.")
    parser.add_argument("--top_k_tavily", type=int, default=3, help="Offline Tavily retrieval top-k.")
    parser.add_argument(
        "--context_budget_tokens",
        type=int,
        default=None,
        help="Approximate context token budget used for assembled context blocks.",
    )
    parser.add_argument("--reindex", action="store_true", help="Rebuild retrieval indexes before retrieval.")
    parser.add_argument("--seed", type=int, default=None, help="Optional generation seed.")
    parser.add_argument("--max_questions", type=int, default=None, help="Optional max question count.")
    parser.add_argument(
        "--data_workbook",
        default=None,
        help="Optional override for the local workbook corpus. Defaults to data/GNEM updated excel.xlsx when needed.",
    )
    parser.add_argument(
        "--tavily_dir",
        default=None,
        help="Optional override for the offline Tavily directory. Defaults to data/tavily or data/tavily ready documents.",
    )
    parser.add_argument(
        "--excel_out",
        default=None,
        help="Legacy alias for the answers workbook path. Defaults to artifacts/results/<run_id>_answers.xlsx.",
    )
    parser.add_argument(
        "--results_dir",
        default=None,
        help="Optional directory for answers/metrics/manifest outputs. Defaults to artifacts/results.",
    )
    parser.add_argument(
        "--study_id",
        default=None,
        help="Optional study identifier used for aggregated summary outputs. Defaults to the question file stem.",
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


def build_model_spec(model_name: str, mode: str) -> tuple[str, ModelSpec]:
    canonical_key = canonical_model_key(model_name)
    if canonical_key not in MODEL_REGISTRY:
        raise ValueError(f"Unsupported model key: {model_name}")
    default_temperature = float(os.getenv("MODEL_TEMPERATURE", "0.1"))
    default_max_tokens = int(os.getenv("MODEL_MAX_TOKENS", "1600"))
    registry = MODEL_REGISTRY[canonical_key]
    resolved_name = next(
        (
            os.getenv(env_name)
            for env_name in registry["env_names"]
            if os.getenv(env_name)
        ),
        registry["default"],
    )
    spec = ModelSpec(
        run_name=f"{canonical_key}_{mode}",
        provider=registry["provider"],
        model_name=resolved_name,
        rag_enabled=mode != "no_rag",
        temperature=default_temperature,
        max_tokens=default_max_tokens,
    )
    return canonical_key, spec


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

    local_blocks = [_format_result_block(result, source_kind="local") for result in local_results]
    web_blocks = [_format_result_block(result, source_kind="tavily") for result in tavily_results]

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
        local_chars=len(local_section),
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
    if not blocks:
        section_text = header + fallback_text
        return section_text, token_counter.count(section_text)

    for block in blocks:
        block_tokens = token_counter.count(block)
        if block_tokens <= remaining_budget:
            selected_blocks.append(block)
            remaining_budget -= block_tokens
            continue
        truncated = token_counter.truncate(block, remaining_budget)
        if truncated:
            selected_blocks.append(truncated)
        break

    section_body = "\n\n".join(selected_blocks) if selected_blocks else fallback_text
    section_text = header + section_body
    return section_text, token_counter.count(section_text)


def _format_result_block(result: RetrievalResult, *, source_kind: str) -> str:
    token = citation_token_for_result(result.chunk_id, result.metadata, source_kind)
    if source_kind == "tavily":
        filepath = normalize_cell(result.metadata.get("filepath"))
        url = normalize_cell(result.metadata.get("url"))
        prefix = " | ".join(part for part in [filepath, url] if part)
        if prefix:
            return f"{token} {prefix} | {result.text}"
    return f"{token} {result.text}"


def build_prompt(mode: str, question: str, context_text: str | None = None) -> tuple[str, str]:
    if mode == "no_rag":
        return f"{PROMPT_A}\n\nQuestion: {question}", "no_rag_v2"
    if not context_text:
        context_text = "LOCAL_CONTEXT:\nNo local context retrieved."
    prompt = f"{PROMPT_B}\n\nCONTEXT:\n{context_text}\n\nQuestion: {question}"
    return prompt, "rag_context_only_v2"


def build_system_prompt(mode: str) -> str:
    if mode == "no_rag":
        return NON_RAG_EVAL_SYSTEM_PROMPT
    return EVAL_SYSTEM_PROMPT


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
            "citation_key": f"{citation_kind_for_result(metadata, source_kind)}:{result.chunk_id}",
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


def generation_usage_for_log(metadata: Any) -> dict[str, Any]:
    return {
        "tokens_in": getattr(metadata, "prompt_tokens", None),
        "tokens_out": getattr(metadata, "completion_tokens", None),
        "tokens_total": getattr(metadata, "total_tokens", None),
        "cost_usd": getattr(metadata, "cost_usd", None),
    }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def split_answer_source_data(
    *,
    mode: str,
    answer_text: str,
    citation_missing: bool,
    citation_invalid: bool,
    support_failed: bool,
) -> tuple[str, str]:
    if mode == "no_rag":
        return "", answer_text
    if (answer_text or "").strip() == ABSTENTION_EXACT:
        return "", ""
    if citation_missing or citation_invalid or support_failed:
        return "", answer_text
    return answer_text, ""


def build_study_id(args: argparse.Namespace) -> str:
    if args.study_id:
        raw = args.study_id.strip()
    else:
        raw = Path(args.questions).stem
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("_") or "ev_research_study"


def default_output_paths(
    *,
    run_id: str,
    args: argparse.Namespace,
    results_dir: Path,
) -> tuple[Path, Path, Path]:
    response_dir = PROJECT_ROOT / "artifacts" / "response_outputs"
    out_path = (
        Path(args.out).expanduser().resolve()
        if args.out
        else (response_dir / f"{run_id}.jsonl").resolve()
    )
    answers_path = (
        Path(args.excel_out).expanduser().resolve()
        if args.excel_out
        else (results_dir / f"{run_id}_answers.xlsx").resolve()
    )
    metrics_path = (results_dir / f"{run_id}_metrics.xlsx").resolve()
    return out_path, answers_path, metrics_path


def make_judge_client(config: Any) -> tuple[Any | None, str | None]:
    judge_spec = ModelSpec(
        run_name="judge_based_metrics",
        provider=config.evaluation.judge_provider,
        model_name=config.evaluation.judge_model,
        rag_enabled=False,
    )
    try:
        return create_client(judge_spec, config.runtime), None
    except Exception as exc:
        return None, str(exc)


def detected_abstention(answer_text: str) -> bool:
    text = (answer_text or "").strip()
    if not text:
        return True
    if text == ABSTENTION_EXACT:
        return True
    return bool(ABSTENTION_PATTERN.search(text))


def main() -> int:
    config = load_config()
    default_questions_path = resolve_default_questions_path()
    parser = build_parser(default_questions_path)
    args = parser.parse_args()

    top_k_local = args.top_k_local or max(1, config.retrieval.generation_context_result_limit or 5)
    context_budget_tokens = args.context_budget_tokens or max(800, config.retrieval.generation_context_char_budget // 4)
    questions = load_eval_questions(args.questions, max_questions=args.max_questions)

    model_key, spec = build_model_spec(args.model, args.mode)
    study_id = build_study_id(args)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{timestamp}_{model_key}_{args.mode}"

    results_dir = (
        Path(args.results_dir).expanduser().resolve()
        if args.results_dir
        else (PROJECT_ROOT / "artifacts" / "results").resolve()
    )
    out_path, answers_out_path, metrics_out_path = default_output_paths(
        run_id=run_id,
        args=args,
        results_dir=results_dir,
    )
    manifest_path = (results_dir / f"{run_id}_manifest.json").resolve()
    ensure_parent(out_path)
    ensure_parent(answers_out_path)
    ensure_parent(metrics_out_path)
    ensure_parent(manifest_path)

    golden_answers = (
        load_golden_answers(args.golden_answers, sheet_name=args.golden_sheet)
        if args.golden_answers
        else None
    )
    golden_path = Path(args.golden_answers).expanduser().resolve() if args.golden_answers else None
    similarity_scorer = (
        SemanticSimilarityScorer(config.retrieval.embedding_model)
        if args.golden_answers or any(question.expected_answer for question in questions)
        else None
    )

    local_retriever: HybridRetriever | None = None
    tavily_retriever: HybridRetriever | None = None
    judge_client = None
    judge_error: str | None = None
    tavily_issues: list[str] = []
    tavily_root: Path | None = None
    local_workbook_path: Path | None = None
    token_counter = TokenCounter()

    local_collection_manifest: dict[str, Any] | None = None
    tavily_collection_manifest: dict[str, Any] | None = None

    try:
        client = create_client(spec, config.runtime)
        if args.mode != "no_rag":
            judge_client, judge_error = make_judge_client(config)

        if args.mode in {"local_rag", "hybrid_rag"} or args.data_workbook:
            local_workbook_path = (
                Path(args.data_workbook).expanduser().resolve()
                if args.data_workbook
                else resolve_default_workbook_path()
            )

        if args.mode in {"local_rag", "hybrid_rag"}:
            assert local_workbook_path is not None
            rows, notes = load_workbook(local_workbook_path)
            local_chunks = ExcelChunkBuilder(config.retrieval).build(rows, notes)
            local_chunks.extend(build_derived_summary_chunks(rows))
            local_retriever = HybridRetriever(
                chunks=local_chunks,
                settings=config.retrieval,
                qdrant_path=config.runtime.qdrant_path,
                collection_name="local",
                force_reindex=args.reindex,
            )
            local_collection_manifest = local_retriever.collection_manifest_metadata()
            print(
                f"[eval_runner] Local corpus ready from {local_workbook_path} "
                f"with {len(local_chunks)} chunks in collection '{local_retriever.collection_name}'.",
                flush=True,
            )

        if args.mode == "hybrid_rag":
            tavily_root = (
                Path(args.tavily_dir).expanduser().resolve()
                if args.tavily_dir
                else resolve_tavily_root(PROJECT_ROOT)
            )
            tavily_docs = load_offline_documents(root=tavily_root, source_type="tavily")
            tavily_chunks = build_document_chunks(tavily_docs.records, config.retrieval)
            tavily_retriever = HybridRetriever(
                chunks=tavily_chunks,
                settings=config.retrieval,
                qdrant_path=config.runtime.qdrant_path,
                collection_name="tavily",
                force_reindex=args.reindex,
                client=local_retriever.client if local_retriever is not None else None,
            )
            tavily_collection_manifest = tavily_retriever.collection_manifest_metadata()
            tavily_issues = [f"{issue.filepath}: {issue.reason}" for issue in tavily_docs.issues]
            print(
                f"[eval_runner] Offline Tavily corpus ready from {tavily_root} "
                f"with {len(tavily_docs.records)} documents and {len(tavily_chunks)} chunks in collection '{tavily_retriever.collection_name}'.",
                flush=True,
            )
            if tavily_issues:
                print(
                    f"[eval_runner] Skipped {len(tavily_issues)} offline Tavily files during ingestion.",
                    flush=True,
                )

        workbook_hash = sha256_file(local_workbook_path)
        golden_hash = sha256_file(golden_path)
        offline_tavily_hash, offline_tavily_manifest_path = resolve_offline_tavily_manifest_hash(tavily_root)
        git_commit = resolve_git_commit(PROJECT_ROOT)

        run_manifest: dict[str, Any] = {
            "run_id": run_id,
            "study_id": study_id,
            "timestamp": utc_now_iso(),
            "canonical_runner": str(Path(__file__).resolve()),
            "model_key": model_key,
            "resolved_provider": spec.provider,
            "resolved_model_name": spec.model_name,
            "resolved_provider_model_id": f"{spec.provider}:{spec.model_name}",
            "mode": args.mode,
            "temperature": spec.temperature,
            "max_tokens": spec.max_tokens,
            "seed": args.seed,
            "questions_path": str(Path(args.questions).expanduser().resolve()),
            "questions_count": len(questions),
            "golden_answers_path": str(golden_path) if golden_path else "",
            "golden_answers_hash": golden_hash,
            "local_workbook_path": str(local_workbook_path) if local_workbook_path else "",
            "local_workbook_hash": workbook_hash,
            "offline_tavily_path": str(tavily_root) if tavily_root else "",
            "offline_tavily_manifest_path": offline_tavily_manifest_path or "",
            "offline_tavily_manifest_hash": offline_tavily_hash,
            "embedding_model": config.retrieval.embedding_model,
            "reranker_enabled": config.retrieval.reranker_enabled,
            "reranker_model": config.retrieval.reranker_model,
            "reranker_top_k": config.retrieval.reranker_top_k,
            "reranker_weight": config.retrieval.reranker_weight,
            "context_budget_tokens": context_budget_tokens,
            "default_top_k_local": top_k_local,
            "default_top_k_tavily": args.top_k_tavily,
            "qdrant_path": str(config.runtime.qdrant_path.resolve()),
            "local_collection_name": local_retriever.collection_name if local_retriever is not None else "",
            "local_collection_fingerprint": local_retriever.collection_fingerprint if local_retriever is not None else None,
            "local_collection_manifest_path": str(local_retriever.collection_manifest_path.resolve()) if local_retriever is not None else "",
            "local_collection_manifest": local_collection_manifest,
            "tavily_collection_name": tavily_retriever.collection_name if tavily_retriever is not None else "",
            "tavily_collection_fingerprint": tavily_retriever.collection_fingerprint if tavily_retriever is not None else None,
            "tavily_collection_manifest_path": str(tavily_retriever.collection_manifest_path.resolve()) if tavily_retriever is not None else "",
            "tavily_collection_manifest": tavily_collection_manifest,
            "git_commit_hash": git_commit,
            "judge_provider": config.evaluation.judge_provider if args.mode != "no_rag" else "",
            "judge_model": config.evaluation.judge_model if args.mode != "no_rag" else "",
            "judge_error": judge_error,
            "results_dir": str(results_dir),
            "response_jsonl_path": str(out_path),
            "answers_workbook_path": str(answers_out_path),
            "metrics_workbook_path": str(metrics_out_path),
            "manifest_path": str(manifest_path),
            "prompt_templates": {
                "no_rag_prompt": PROMPT_A,
                "rag_prompt": PROMPT_B,
                "no_rag_system_prompt": NON_RAG_EVAL_SYSTEM_PROMPT,
                "rag_system_prompt": EVAL_SYSTEM_PROMPT,
            },
        }
        run_manifest["manifest_fingerprint"] = sha256_json(run_manifest)

        raw_records: list[dict[str, Any]] = []
        answer_rows: list[dict[str, Any]] = []
        metrics_rows: list[dict[str, Any]] = []
        local_retrieval_rows: list[dict[str, Any]] = []
        tavily_retrieval_rows: list[dict[str, Any]] = []

        with out_path.open("a", encoding="utf-8") as handle:
            for index, item in enumerate(questions, start=1):
                print(
                    f"[eval_runner] {index}/{len(questions)} | {item.q_id} | {model_key} | {args.mode}",
                    flush=True,
                )

                question_route = route_question(item.question) if args.mode != "no_rag" else "no_rag"
                effective_top_k_local = question_top_k_local(question_route, top_k_local)
                effective_top_k_tavily = question_top_k_tavily(question_route, args.top_k_tavily)

                local_results: list[RetrievalResult] = []
                tavily_results: list[RetrievalResult] = []
                retrieval_ms_local = 0.0
                retrieval_ms_tavily = 0.0
                context_result: ContextBuildResult | None = None

                if args.mode in {"local_rag", "hybrid_rag"} and local_retriever is not None:
                    retrieval_start = time.perf_counter()
                    local_results = local_retriever.retrieve(item.question, top_k=effective_top_k_local)
                    retrieval_ms_local = round((time.perf_counter() - retrieval_start) * 1000, 2)

                if args.mode == "hybrid_rag" and tavily_retriever is not None:
                    retrieval_start = time.perf_counter()
                    tavily_results = tavily_retriever.retrieve(item.question, top_k=effective_top_k_tavily)
                    retrieval_ms_tavily = round((time.perf_counter() - retrieval_start) * 1000, 2)

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
                system_prompt = build_system_prompt(args.mode)
                answer_text, generation_seconds, success, error_message, generation_metadata = safe_generate_with_metadata(
                    client,
                    prompt_text,
                    temperature=spec.temperature,
                    max_tokens=spec.max_tokens,
                    system_prompt=system_prompt,
                    seed=args.seed,
                )

                evidence_registry = build_evidence_registry(
                    local_results=local_results,
                    tavily_results=tavily_results,
                )
                citation_refs = extract_citations(answer_text)
                citation_tokens = [citation.token for citation in citation_refs]
                rag_validation = {
                    "answer_abstained": detected_abstention(answer_text),
                    "abstention_correct": None,
                    "citation_coverage": None,
                    "citation_validity": None,
                    "support_rate": None,
                    "unsupported_claim_rate": None,
                    "citation_missing": False,
                    "citation_invalid": False,
                    "support_failed": False,
                    "citation_missing_count": 0,
                    "citation_invalid_count": 0,
                    "support_failed_count": 0,
                    "bullets": [],
                }
                if args.mode != "no_rag":
                    rag_validation = validate_rag_answer(
                        question=item.question,
                        answer_text=answer_text,
                        evidence_registry=evidence_registry,
                        judge_client=judge_client,
                        judge_max_retries=config.evaluation.max_retries,
                    )

                knowledge_source_data, pretrained_data = split_answer_source_data(
                    mode=args.mode,
                    answer_text=answer_text,
                    citation_missing=bool(rag_validation.get("citation_missing", False)),
                    citation_invalid=bool(rag_validation.get("citation_invalid", False)),
                    support_failed=bool(rag_validation.get("support_failed", False)),
                )

                golden_match = resolve_golden_answer(
                    q_id=item.q_id,
                    question=item.question,
                    golden_answers=golden_answers,
                    fallback_answer_text=item.expected_answer,
                    allow_question_row_fallback=not bool(args.golden_answers),
                )
                golden_metrics = compute_golden_metrics(
                    answer_text=answer_text,
                    golden_match=golden_match,
                    similarity_scorer=similarity_scorer,
                )

                record = {
                    "run_id": run_id,
                    "study_id": study_id,
                    "q_id": item.q_id,
                    "question": item.question,
                    "model_key": model_key,
                    "resolved_provider": spec.provider,
                    "resolved_model_name": spec.model_name,
                    "resolved_provider_model_id": f"{spec.provider}:{spec.model_name}",
                    "mode": args.mode,
                    "question_route": question_route,
                    "prompt_template_key": prompt_used,
                    "prompt_text": prompt_text,
                    "prompt_hash": sha256_json({"prompt": prompt_text, "system_prompt": system_prompt}),
                    "system_prompt": system_prompt,
                    "manifest_path": str(manifest_path),
                    "temperature": spec.temperature,
                    "max_tokens": spec.max_tokens,
                    "seed": args.seed,
                    "top_k_local": effective_top_k_local if args.mode != "no_rag" else 0,
                    "top_k_tavily": effective_top_k_tavily if args.mode == "hybrid_rag" else 0,
                    "answer_text": answer_text,
                    "knowledge_source_data": knowledge_source_data,
                    "pretrained_data": pretrained_data,
                    "golden_answer": golden_match.record.golden_answer if golden_match.record else None,
                    "golden_question_type": golden_match.record.question_type if golden_match.record else "",
                    "golden_answer_format": golden_match.record.answer_format if golden_match.record else "",
                    "golden_notes": golden_match.record.notes if golden_match.record else "",
                    "golden_key_facts": golden_match.record.golden_key_facts if golden_match.record else "",
                    "golden_source_path": golden_match.record.source_path if golden_match.record else "",
                    "golden_source_row_number": golden_match.record.source_row_number if golden_match.record else None,
                    "golden_match_type": golden_match.match_type,
                    "golden_question_mismatch": golden_match.question_mismatch,
                    "local_retrieval": build_retrieval_log(local_results, source_kind="local"),
                    "tavily_retrieval": build_retrieval_log(tavily_results, source_kind="tavily"),
                    "retrieved_context_ids": sorted(evidence_registry),
                    "citations_extracted": [asdict(citation) for citation in citation_refs],
                    "citation_tokens": citation_tokens,
                    "validation_bullets": rag_validation.get("bullets", []),
                    "context_lengths": {
                        "local_chars": context_result.local_chars if context_result else 0,
                        "local_tokens": context_result.local_tokens if context_result else 0,
                        "web_chars": context_result.web_chars if context_result else 0,
                        "web_tokens": context_result.web_tokens if context_result else 0,
                        "total_chars": context_result.total_chars if context_result else 0,
                        "total_tokens": context_result.total_tokens if context_result else 0,
                    },
                    "timing": {
                        "retrieval_ms_local": retrieval_ms_local,
                        "retrieval_ms_tavily": retrieval_ms_tavily,
                        "generation_ms": round(generation_seconds * 1000, 2),
                    },
                    "tokens_in": generation_usage_for_log(generation_metadata)["tokens_in"],
                    "tokens_out": generation_usage_for_log(generation_metadata)["tokens_out"],
                    "tokens_total": generation_usage_for_log(generation_metadata)["tokens_total"],
                    "cost_usd": generation_usage_for_log(generation_metadata)["cost_usd"],
                    "success": success,
                    "error_message": error_message,
                    "created_at": utc_now_iso(),
                }
                record.update(golden_metrics)
                record.update(rag_validation)
                record["abstention_rate"] = 1.0 if bool(record.get("answer_abstained")) else 0.0
                record["abstention_correctness"] = (
                    1.0 if record.get("abstention_correct") is True else
                    0.0 if record.get("abstention_correct") is False else None
                )
                if tavily_issues and args.mode == "hybrid_rag":
                    record["tavily_ingestion_issues"] = tavily_issues

                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                handle.flush()
                raw_records.append(record)
                answer_rows.append(flatten_answer_row(record=record, golden_match=golden_match))
                metrics_rows.append(flatten_metrics_row(record))

                for rank, result in enumerate(local_results, start=1):
                    local_retrieval_rows.append(
                        {
                            "run_id": run_id,
                            "q_id": item.q_id,
                            "question": item.question,
                            "question_route": question_route,
                            "rank": rank,
                            "citation_key": f"{citation_kind_for_result(result.metadata, 'local')}:{result.chunk_id}",
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
                            "citation_key": f"{citation_kind_for_result(result.metadata, 'tavily')}:{result.chunk_id}",
                            "chunk_id": result.chunk_id,
                            "score": result.final_score,
                            "chunk_type": normalize_cell(result.metadata.get("chunk_type")),
                            "filepath": normalize_cell(result.metadata.get("filepath")),
                            "url": normalize_cell(result.metadata.get("url")),
                            "text": result.text,
                        }
                    )

        summary_row = summarize_run_metrics(metrics_rows)
        summary_row.update(
            {
                "study_id": study_id,
                "run_id": run_id,
                "model_key": model_key,
                "mode": args.mode,
                "resolved_provider": spec.provider,
                "resolved_model_name": spec.model_name,
            }
        )
        run_manifest["summary_row"] = summary_row
        run_manifest["answers_count"] = len(answer_rows)
        run_manifest["metrics_count"] = len(metrics_rows)
        run_manifest["judge_metric_family"] = "judge_based_metrics"
        run_manifest["manifest_fingerprint"] = sha256_json(run_manifest)
        manifest_path.write_text(json.dumps(run_manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        study_summary_path, leaderboard_path = update_study_outputs(
            study_id=study_id,
            results_dir=results_dir,
        )
        hybrid_value_report_path = export_hybrid_value_report(
            study_id=study_id,
            results_dir=results_dir,
        )
        run_manifest["study_summary_path"] = str(study_summary_path)
        run_manifest["leaderboard_path"] = str(leaderboard_path)
        run_manifest["hybrid_value_report_path"] = str(hybrid_value_report_path) if hybrid_value_report_path else ""
        run_manifest["manifest_fingerprint"] = sha256_json(run_manifest)
        manifest_path.write_text(json.dumps(run_manifest, indent=2, ensure_ascii=False), encoding="utf-8")

        export_answers_workbook(
            path=answers_out_path,
            answer_rows=answer_rows,
            local_retrieval_rows=local_retrieval_rows,
            tavily_retrieval_rows=tavily_retrieval_rows,
            manifest=run_manifest,
        )
        export_metrics_workbook(
            path=metrics_out_path,
            per_question_rows=metrics_rows,
            summary_row=summary_row,
        )

        print(f"[eval_runner] Wrote {len(raw_records)} records to {out_path}", flush=True)
        print(f"[eval_runner] Wrote answers workbook to {answers_out_path}", flush=True)
        print(f"[eval_runner] Wrote metrics workbook to {metrics_out_path}", flush=True)
        print(f"[eval_runner] Wrote run manifest to {manifest_path}", flush=True)
        print(f"[eval_runner] Updated study summary: {study_summary_path}", flush=True)
        print(f"[eval_runner] Updated leaderboard: {leaderboard_path}", flush=True)
        if hybrid_value_report_path is not None:
            print(f"[eval_runner] Updated hybrid value report: {hybrid_value_report_path}", flush=True)
        return 0
    finally:
        if local_retriever is not None:
            local_retriever.close()
        if tavily_retriever is not None:
            tavily_retriever.close()


if __name__ == "__main__":
    raise SystemExit(main())
