from __future__ import annotations

from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Any

import pandas as pd

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    SentenceTransformer = None  # type: ignore[assignment]

from .models import LLMClient, safe_generate_with_metadata

ALLOWED_CITATION_KINDS = {"DOC", "WEB", "ANALYTIC", "GEO"}
ABSTENTION_EXACT = "Not found in provided context."
BULLET_PREFIX_RE = re.compile(r"^\s*(?:[-*•]|\d+[.)])\s+")
CITATION_RE = re.compile(r"\[(DOC|WEB|ANALYTIC|GEO):([^\]]+)\]", flags=re.IGNORECASE)
NORMALIZE_RE = re.compile(r"[^a-z0-9]+")
NUMBER_RE = re.compile(r"-?\d[\d,]*(?:\.\d+)?%?")
REQUIRED_GOLDEN_COLUMNS = {"q_id", "question", "golden_answer"}
OPTIONAL_GOLDEN_COLUMNS = {"question_type", "golden_key_facts", "answer_format", "notes"}
SUPPORT_JUDGE_SYSTEM_PROMPT = (
    "You are a strict grounding validator for context-only question answering. "
    "Return strict JSON only."
)
ANSWERABILITY_JUDGE_SYSTEM_PROMPT = (
    "You are a strict answerability validator for context-only question answering. "
    "Return strict JSON only."
)
SIMILARITY_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "with",
}


@dataclass(slots=True)
class GoldenAnswerRecord:
    q_id: str
    question: str
    golden_answer: str
    question_type: str = ""
    golden_key_facts: str = ""
    answer_format: str = ""
    notes: str = ""
    source_path: str = ""
    source_row_number: int = 0


@dataclass(slots=True)
class GoldenMatchResult:
    record: GoldenAnswerRecord | None
    match_type: str
    question_mismatch: bool


@dataclass(slots=True)
class CitationRef:
    kind: str
    citation_id: str
    token: str


@dataclass(slots=True)
class BulletValidation:
    index: int
    text: str
    cleaned_text: str
    citations: list[dict[str, str]]
    citation_missing: bool
    citation_invalid: bool
    support_failed: bool
    support_label: str
    support_reason: str
    support_backend: str


class SemanticSimilarityScorer:
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.backend = "sequence_matcher"
        self._model = None
        if SentenceTransformer is not None:
            try:
                self._model = SentenceTransformer(model_name)
                self.backend = f"sentence_transformer::{model_name}"
            except Exception:
                self._model = None

    def similarity(self, left: str, right: str) -> float | None:
        cleaned_left = strip_nonsemantic_sections(left)
        cleaned_right = strip_nonsemantic_sections(right)
        if not cleaned_left or not cleaned_right:
            return None
        if self._model is not None:
            try:
                vectors = self._model.encode(
                    [cleaned_left, cleaned_right],
                    normalize_embeddings=True,
                    show_progress_bar=False,
                )
                return round(float(vectors[0] @ vectors[1]), 6)
            except Exception:
                pass
        return round(SequenceMatcher(None, cleaned_left, cleaned_right).ratio(), 6)


def normalize_cell(value: object) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_match_text(value: str) -> str:
    return NORMALIZE_RE.sub(" ", value.lower()).strip()


def normalize_header(value: object) -> str:
    return normalize_cell(value).lower()


def _read_tabular_with_header_detection(path: Path, *, sheet_name: str | int | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(path, sheet_name=sheet_name or 0)
        if _has_named_columns(df.columns):
            return df
        raw_df = pd.read_excel(path, sheet_name=sheet_name or 0, header=None)
    elif suffix == ".csv":
        df = pd.read_csv(path)
        if _has_named_columns(df.columns):
            return df
        raw_df = pd.read_csv(path, header=None)
    else:
        raise ValueError(f"Unsupported golden answers file type: {path.suffix}")

    promoted = _promote_header_row(raw_df)
    if promoted is None:
        return df
    return promoted


def _has_named_columns(columns: Any) -> bool:
    normalized = {normalize_header(column) for column in columns}
    return bool(normalized & REQUIRED_GOLDEN_COLUMNS)


def _promote_header_row(raw_df: pd.DataFrame) -> pd.DataFrame | None:
    for row_index in range(min(10, len(raw_df))):
        headers = [normalize_cell(value) for value in raw_df.iloc[row_index].tolist()]
        normalized = {header.lower() for header in headers if header}
        if normalized & REQUIRED_GOLDEN_COLUMNS:
            promoted = raw_df.iloc[row_index + 1 :].copy().reset_index(drop=True)
            promoted.columns = [
                header if header else f"column_{column_index + 1}"
                for column_index, header in enumerate(headers)
            ]
            return promoted
    return None


def load_golden_answers(path: str | Path, *, sheet_name: str | None = None) -> dict[str, GoldenAnswerRecord]:
    golden_path = Path(path).expanduser().resolve()
    df = _read_tabular_with_header_detection(golden_path, sheet_name=sheet_name)
    df = df.copy()
    df.columns = [normalize_cell(column) for column in df.columns]
    column_lookup = {normalize_header(column): column for column in df.columns}
    missing = REQUIRED_GOLDEN_COLUMNS - set(column_lookup)
    if missing:
        raise ValueError(
            "Golden answers file is missing required columns: "
            + ", ".join(sorted(missing))
        )

    records: dict[str, GoldenAnswerRecord] = {}
    for row_index, (_, row) in enumerate(df.iterrows(), start=2):
        q_id = normalize_cell(row.get(column_lookup["q_id"]))
        question = normalize_cell(row.get(column_lookup["question"]))
        golden_answer = normalize_cell(row.get(column_lookup["golden_answer"]))
        if not q_id or not question or not golden_answer:
            continue
        if q_id in records:
            raise ValueError(f"Duplicate q_id '{q_id}' found in golden answers file: {golden_path}")
        records[q_id] = GoldenAnswerRecord(
            q_id=q_id,
            question=question,
            golden_answer=golden_answer,
            question_type=normalize_cell(row.get(column_lookup.get("question_type", ""))),
            golden_key_facts=normalize_cell(row.get(column_lookup.get("golden_key_facts", ""))),
            answer_format=normalize_cell(row.get(column_lookup.get("answer_format", ""))),
            notes=normalize_cell(row.get(column_lookup.get("notes", ""))),
            source_path=str(golden_path),
            source_row_number=row_index,
        )
    return records


def resolve_golden_answer(
    *,
    q_id: str,
    question: str,
    golden_answers: dict[str, GoldenAnswerRecord] | None,
    fallback_answer_text: str | None = None,
    allow_question_row_fallback: bool = True,
) -> GoldenMatchResult:
    if golden_answers and q_id in golden_answers:
        record = golden_answers[q_id]
        return GoldenMatchResult(
            record=record,
            match_type="q_id",
            question_mismatch=normalize_match_text(record.question) != normalize_match_text(question),
        )
    if fallback_answer_text and allow_question_row_fallback:
        return GoldenMatchResult(
            record=GoldenAnswerRecord(
                q_id=q_id,
                question=question,
                golden_answer=fallback_answer_text,
            ),
            match_type="question_row_expected_answer",
            question_mismatch=False,
        )
    return GoldenMatchResult(record=None, match_type="missing", question_mismatch=False)


def sha256_file(path: Path | None) -> str | None:
    if path is None or not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def sha256_json(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
    ).hexdigest()


def file_or_directory_hash(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    if path.is_file():
        return sha256_file(path)
    digest = hashlib.sha256()
    for file_path in sorted(candidate for candidate in path.rglob("*") if candidate.is_file()):
        digest.update(str(file_path.relative_to(path)).encode("utf-8"))
        digest.update(b"\0")
        file_hash = sha256_file(file_path) or ""
        digest.update(file_hash.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def resolve_offline_tavily_manifest_hash(root: Path | None) -> tuple[str | None, str | None]:
    if root is None:
        return None, None
    manifest_candidates = [
        root / "tavily_ready_documents_manifest.csv",
        root / "manifest.csv",
    ]
    for candidate in manifest_candidates:
        if candidate.exists():
            return sha256_file(candidate), str(candidate.resolve())
    return file_or_directory_hash(root), str(root.resolve())


def resolve_git_commit(repo_root: Path) -> str | None:
    try:
        output = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except Exception:
        return None
    value = output.strip()
    return value or None


def citation_kind_for_result(metadata: dict[str, Any], source_kind: str) -> str:
    if source_kind == "tavily":
        return "WEB"
    chunk_type = normalize_cell(metadata.get("chunk_type")).lower()
    analysis_type = normalize_cell(metadata.get("analysis_type")).lower()
    sheet_name = normalize_cell(metadata.get("sheet_name")).lower()
    if chunk_type == "derived_analytic_summary" or analysis_type:
        return "ANALYTIC"
    if chunk_type in {"location_theme", "geo_reference"} or "geo" in sheet_name:
        return "GEO"
    return "DOC"


def citation_token_for_result(chunk_id: str, metadata: dict[str, Any], source_kind: str) -> str:
    kind = citation_kind_for_result(metadata, source_kind)
    return f"[{kind}:{chunk_id}]"


def build_evidence_registry(
    *,
    local_results: list[Any],
    tavily_results: list[Any],
) -> dict[str, dict[str, Any]]:
    registry: dict[str, dict[str, Any]] = {}
    for source_kind, results in (("local", local_results), ("tavily", tavily_results)):
        for result in results:
            metadata = getattr(result, "metadata", {}) or {}
            citation_kind = citation_kind_for_result(metadata, source_kind)
            key = f"{citation_kind}:{result.chunk_id}"
            registry[key] = {
                "citation_key": key,
                "citation_kind": citation_kind,
                "chunk_id": result.chunk_id,
                "source_kind": source_kind,
                "text": getattr(result, "text", ""),
                "metadata": metadata,
            }
    return registry


def extract_citations(text: str) -> list[CitationRef]:
    citations: list[CitationRef] = []
    for match in CITATION_RE.finditer(text or ""):
        kind = match.group(1).upper()
        citation_id = match.group(2).strip()
        if kind in ALLOWED_CITATION_KINDS and citation_id:
            citations.append(
                CitationRef(
                    kind=kind,
                    citation_id=citation_id,
                    token=f"{kind}:{citation_id}",
                )
            )
    return citations


def strip_citations(text: str) -> str:
    return re.sub(CITATION_RE, "", text or "").strip()


def strip_nonsemantic_sections(text: str) -> str:
    if not text:
        return ""
    if text.strip() == ABSTENTION_EXACT:
        return text.strip()
    kept_lines: list[str] = []
    in_missing = False
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        lower = stripped.lower()
        if lower.startswith("evidence:"):
            continue
        if lower.startswith("missing info"):
            in_missing = True
            continue
        if in_missing:
            if BULLET_PREFIX_RE.match(stripped):
                continue
            if stripped.endswith(":"):
                continue
        kept_lines.append(strip_citations(BULLET_PREFIX_RE.sub("", stripped)).strip())
    return "\n".join(line for line in kept_lines if line).strip()


def parse_answer_bullets(answer_text: str) -> dict[str, Any]:
    text = (answer_text or "").strip()
    if not text:
        return {
            "abstained": False,
            "bullets": [],
            "missing_info": [],
            "evidence_line": "",
        }
    if text == ABSTENTION_EXACT:
        return {
            "abstained": True,
            "bullets": [],
            "missing_info": [],
            "evidence_line": "",
        }

    bullets: list[str] = []
    missing_info: list[str] = []
    evidence_line = ""
    in_missing = False
    prose_buffer: list[str] = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        lower = stripped.lower()
        if lower.startswith("evidence:"):
            evidence_line = stripped
            continue
        if lower.startswith("missing info"):
            in_missing = True
            payload = stripped.split(":", 1)[1].strip() if ":" in stripped else ""
            if payload:
                missing_info.append(payload)
            continue
        bullet_match = BULLET_PREFIX_RE.match(stripped)
        if bullet_match:
            target = missing_info if in_missing else bullets
            target.append(BULLET_PREFIX_RE.sub("", stripped).strip())
            continue
        prose_buffer.append(stripped)

    if prose_buffer and not bullets and not missing_info:
        bullets = prose_buffer
    elif prose_buffer and in_missing:
        missing_info.extend(prose_buffer)
    return {
        "abstained": False,
        "bullets": bullets,
        "missing_info": missing_info,
        "evidence_line": evidence_line,
    }


def _normalize_tokens(text: str) -> list[str]:
    return [
        token
        for token in normalize_match_text(text).split()
        if token and token not in SIMILARITY_STOPWORDS
    ]


def _heuristic_support_check(bullet_text: str, evidence_texts: list[str]) -> tuple[str, str, str]:
    evidence_blob = " ".join(evidence_texts)
    bullet_clean = strip_citations(bullet_text)
    bullet_tokens = _normalize_tokens(bullet_clean)
    if not bullet_tokens:
        return "nonfactual", "No factual tokens remained after normalization.", "heuristic"
    evidence_tokens = set(_normalize_tokens(evidence_blob))
    overlap_ratio = (
        sum(1 for token in bullet_tokens if token in evidence_tokens) / max(1, len(bullet_tokens))
    )
    bullet_numbers = [match.group(0) for match in NUMBER_RE.finditer(bullet_clean)]
    if bullet_numbers and any(number not in evidence_blob for number in bullet_numbers):
        return "unsupported", "At least one numeric value from the bullet was not found in cited evidence.", "heuristic"
    if overlap_ratio >= 0.55:
        return "supported", f"Token overlap ratio={overlap_ratio:.2f}.", "heuristic"
    if overlap_ratio >= 0.30:
        return "partial", f"Token overlap ratio={overlap_ratio:.2f}.", "heuristic"
    return "unsupported", f"Token overlap ratio={overlap_ratio:.2f}.", "heuristic"


def _extract_json_payload(raw_text: str) -> Any | None:
    if not raw_text:
        return None
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", raw_text, flags=re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def _judge_support(
    *,
    question: str,
    bullet_text: str,
    evidence_texts: list[str],
    judge_client: LLMClient | None,
    max_retries: int,
) -> tuple[str, str, str]:
    if judge_client is None:
        return _heuristic_support_check(bullet_text, evidence_texts)

    evidence_block = "\n\n".join(
        f"[Evidence {index}] {text}" for index, text in enumerate(evidence_texts, start=1)
    )
    prompt = (
        "Determine whether the cited evidence fully supports the bullet claim.\n"
        "Return JSON only in this form:\n"
        '{"label":"supported|partial|unsupported|nonfactual","reason":"short text"}\n\n'
        f"Question:\n{question}\n\n"
        f"Bullet claim:\n{bullet_text}\n\n"
        f"Cited evidence:\n{evidence_block}\n\n"
        "Rules:\n"
        "- supported: all material facts in the bullet are supported by the cited evidence.\n"
        "- partial: some, but not all, material facts are supported.\n"
        "- unsupported: the bullet is not supported by the cited evidence.\n"
        "- nonfactual: the line is not making a factual claim.\n"
    )
    for _ in range(max(1, max_retries + 1)):
        raw_text, _, success, _, _ = safe_generate_with_metadata(
            judge_client,
            prompt,
            temperature=0.0,
            max_tokens=220,
            system_prompt=SUPPORT_JUDGE_SYSTEM_PROMPT,
        )
        if not success:
            continue
        payload = _extract_json_payload(raw_text)
        if not isinstance(payload, dict):
            continue
        label = normalize_cell(payload.get("label")).lower()
        reason = normalize_cell(payload.get("reason"))
        if label in {"supported", "partial", "unsupported", "nonfactual"}:
            return label, reason, "judge"
    return _heuristic_support_check(bullet_text, evidence_texts)


def _judge_answerability(
    *,
    question: str,
    evidence_texts: list[str],
    judge_client: LLMClient | None,
    max_retries: int,
) -> tuple[bool | None, str]:
    if not evidence_texts:
        return False, "heuristic_empty_context"
    if judge_client is None:
        return True, "heuristic_nonempty_context"

    evidence_block = "\n\n".join(
        f"[Evidence {index}] {text}" for index, text in enumerate(evidence_texts, start=1)
    )
    prompt = (
        "Determine whether the retrieved evidence contains enough information to support at least one direct factual answer to the question.\n"
        "Return JSON only in this form:\n"
        '{"answerable":true,"reason":"short text"}\n\n'
        f"Question:\n{question}\n\n"
        f"Retrieved evidence:\n{evidence_block}\n"
    )
    for _ in range(max(1, max_retries + 1)):
        raw_text, _, success, _, _ = safe_generate_with_metadata(
            judge_client,
            prompt,
            temperature=0.0,
            max_tokens=160,
            system_prompt=ANSWERABILITY_JUDGE_SYSTEM_PROMPT,
        )
        if not success:
            continue
        payload = _extract_json_payload(raw_text)
        if isinstance(payload, dict) and isinstance(payload.get("answerable"), bool):
            return bool(payload["answerable"]), "judge"
    return True, "heuristic_nonempty_context"


def validate_rag_answer(
    *,
    question: str,
    answer_text: str,
    evidence_registry: dict[str, dict[str, Any]],
    judge_client: LLMClient | None,
    judge_max_retries: int,
) -> dict[str, Any]:
    parsed = parse_answer_bullets(answer_text)
    abstained = bool(parsed["abstained"])
    bullet_details: list[BulletValidation] = []

    if abstained:
        answerable, answerability_backend = _judge_answerability(
            question=question,
            evidence_texts=[entry["text"] for entry in evidence_registry.values()],
            judge_client=judge_client,
            max_retries=judge_max_retries,
        )
        return {
            "answer_abstained": True,
            "answerable_from_context": answerable,
            "abstention_correct": bool(answerable is False),
            "abstention_backend": answerability_backend,
            "bullet_count": 0,
            "factual_bullet_count": 0,
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

    bullets = parsed["bullets"]
    factual_bullets = [bullet for bullet in bullets if bullet]
    citation_missing_count = 0
    citation_invalid_count = 0
    support_failed_count = 0
    supported_count = 0
    cited_count = 0
    valid_cited_count = 0

    for index, bullet_text in enumerate(factual_bullets, start=1):
        citations = extract_citations(bullet_text)
        citation_dicts = [asdict(citation) for citation in citations]
        citation_missing = len(citations) == 0
        citation_invalid = False
        support_failed = False
        support_label = "nonfactual"
        support_reason = ""
        support_backend = "none"
        if citation_missing:
            citation_missing_count += 1
            citation_invalid = False
            support_failed = True
            support_failed_count += 1
            support_label = "unsupported"
            support_reason = "Factual bullet is missing evidence citations."
            support_backend = "rule"
        else:
            cited_count += 1
            invalid_tokens = [
                citation.token for citation in citations if citation.token not in evidence_registry
            ]
            if invalid_tokens:
                citation_invalid = True
                citation_invalid_count += 1
                support_failed = True
                support_failed_count += 1
                support_label = "unsupported"
                support_reason = "At least one cited evidence ID was not retrieved for this question."
                support_backend = "rule"
            else:
                valid_cited_count += 1
                evidence_texts = [evidence_registry[citation.token]["text"] for citation in citations]
                support_label, support_reason, support_backend = _judge_support(
                    question=question,
                    bullet_text=bullet_text,
                    evidence_texts=evidence_texts,
                    judge_client=judge_client,
                    max_retries=judge_max_retries,
                )
                if support_label == "supported":
                    supported_count += 1
                else:
                    support_failed = True
                    support_failed_count += 1

        bullet_details.append(
            BulletValidation(
                index=index,
                text=bullet_text,
                cleaned_text=strip_citations(bullet_text),
                citations=citation_dicts,
                citation_missing=citation_missing,
                citation_invalid=citation_invalid,
                support_failed=support_failed,
                support_label=support_label,
                support_reason=support_reason,
                support_backend=support_backend,
            )
        )

    answerable, answerability_backend = _judge_answerability(
        question=question,
        evidence_texts=[entry["text"] for entry in evidence_registry.values()],
        judge_client=judge_client,
        max_retries=judge_max_retries,
    )
    factual_count = len(factual_bullets)
    citation_coverage = cited_count / factual_count if factual_count else None
    citation_validity = valid_cited_count / cited_count if cited_count else None
    support_rate = supported_count / factual_count if factual_count else None
    unsupported_claim_rate = support_failed_count / factual_count if factual_count else None
    abstention_correct = None
    if answerable is not None:
        abstention_correct = True if answerable else False
    return {
        "answer_abstained": False,
        "answerable_from_context": answerable,
        "abstention_correct": abstention_correct,
        "abstention_backend": answerability_backend,
        "bullet_count": len(bullets),
        "factual_bullet_count": factual_count,
        "citation_coverage": round(citation_coverage, 6) if citation_coverage is not None else None,
        "citation_validity": round(citation_validity, 6) if citation_validity is not None else None,
        "support_rate": round(support_rate, 6) if support_rate is not None else None,
        "unsupported_claim_rate": round(unsupported_claim_rate, 6) if unsupported_claim_rate is not None else None,
        "citation_missing": citation_missing_count > 0,
        "citation_invalid": citation_invalid_count > 0,
        "support_failed": support_failed_count > 0,
        "citation_missing_count": citation_missing_count,
        "citation_invalid_count": citation_invalid_count,
        "support_failed_count": support_failed_count,
        "bullets": [asdict(detail) for detail in bullet_details],
    }


def _answer_format_lower(record: GoldenAnswerRecord | None) -> str:
    if record is None:
        return ""
    return " ".join(
        [
            record.answer_format.lower(),
            record.question_type.lower(),
        ]
    ).strip()


def _extract_list_items(text: str) -> list[str]:
    parsed = parse_answer_bullets(text)
    if parsed["abstained"]:
        return []
    candidates = parsed["bullets"] or [
        part.strip()
        for part in re.split(r"[\n;]+", strip_nonsemantic_sections(text))
        if part.strip()
    ]
    items: list[str] = []
    for candidate in candidates:
        cleaned = strip_citations(candidate)
        if "," in cleaned and len(candidates) == 1:
            parts = [part.strip() for part in cleaned.split(",") if part.strip()]
            items.extend(parts)
        else:
            items.append(cleaned.strip())
    normalized = [normalize_match_text(item) for item in items if normalize_match_text(item)]
    deduped: list[str] = []
    seen: set[str] = set()
    for item in normalized:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _extract_numeric_values(text: str) -> list[float]:
    values: list[float] = []
    for match in NUMBER_RE.finditer(text or ""):
        raw = match.group(0).replace(",", "").strip()
        is_percent = raw.endswith("%")
        if is_percent:
            raw = raw[:-1]
        try:
            value = float(raw)
        except ValueError:
            continue
        if is_percent:
            value = value / 100.0
        values.append(value)
    return values


def _numeric_tolerance(answer_format: str) -> float:
    lowered = answer_format.lower()
    for pattern in [
        r"tolerance\s*=\s*([0-9.]+)%?",
        r"tol\s*=\s*([0-9.]+)%?",
    ]:
        match = re.search(pattern, lowered)
        if match:
            value = float(match.group(1))
            return value / 100.0 if value > 1 else value
    return 0.05


def compute_golden_metrics(
    *,
    answer_text: str,
    golden_match: GoldenMatchResult,
    similarity_scorer: SemanticSimilarityScorer | None,
) -> dict[str, Any]:
    record = golden_match.record
    metrics = {
        "golden_available": record is not None,
        "golden_match_type": golden_match.match_type,
        "golden_question_mismatch": golden_match.question_mismatch,
        "golden_normalized_exact_match": None,
        "golden_semantic_similarity": None,
        "golden_similarity_backend": similarity_scorer.backend if similarity_scorer is not None else None,
        "golden_list_precision": None,
        "golden_list_recall": None,
        "golden_list_f1": None,
        "golden_numeric_exact_match": None,
        "golden_numeric_tolerance_match": None,
    }
    if record is None:
        return metrics

    cleaned_answer = normalize_match_text(strip_nonsemantic_sections(answer_text))
    cleaned_golden = normalize_match_text(record.golden_answer)
    metrics["golden_normalized_exact_match"] = 1.0 if cleaned_answer == cleaned_golden else 0.0
    if similarity_scorer is not None:
        metrics["golden_semantic_similarity"] = similarity_scorer.similarity(answer_text, record.golden_answer)

    answer_format = _answer_format_lower(record)
    if any(token in answer_format for token in {"list", "set", "items", "companies"}):
        predicted_items = set(_extract_list_items(answer_text))
        golden_items = set(_extract_list_items(record.golden_answer))
        if predicted_items or golden_items:
            true_positive = len(predicted_items & golden_items)
            precision = true_positive / len(predicted_items) if predicted_items else 0.0
            recall = true_positive / len(golden_items) if golden_items else 0.0
            f1 = (
                2 * precision * recall / (precision + recall)
                if precision + recall
                else 0.0
            )
            metrics["golden_list_precision"] = round(precision, 6)
            metrics["golden_list_recall"] = round(recall, 6)
            metrics["golden_list_f1"] = round(f1, 6)

    numeric_expected = any(token in answer_format for token in {"number", "numeric", "count", "percent", "percentage"})
    answer_numbers = _extract_numeric_values(answer_text)
    golden_numbers = _extract_numeric_values(record.golden_answer)
    if numeric_expected or (len(answer_numbers) == 1 and len(golden_numbers) == 1):
        if len(answer_numbers) == 1 and len(golden_numbers) == 1:
            answer_value = answer_numbers[0]
            golden_value = golden_numbers[0]
            metrics["golden_numeric_exact_match"] = 1.0 if answer_value == golden_value else 0.0
            tolerance = _numeric_tolerance(record.answer_format)
            metrics["golden_numeric_tolerance_match"] = (
                1.0
                if abs(answer_value - golden_value) <= max(tolerance * max(1.0, abs(golden_value)), 1e-9)
                else 0.0
            )
        else:
            metrics["golden_numeric_exact_match"] = 0.0
            metrics["golden_numeric_tolerance_match"] = 0.0
    return metrics


def summarize_run_metrics(per_question_rows: list[dict[str, Any]]) -> dict[str, Any]:
    df = pd.DataFrame(per_question_rows)
    if df.empty:
        return {}
    summary: dict[str, Any] = {
        "question_count": int(len(df)),
        "questions_with_golden_answers": int(df["golden_available"].fillna(False).sum()) if "golden_available" in df else 0,
    }
    metric_columns = [
        "golden_normalized_exact_match",
        "golden_semantic_similarity",
        "golden_list_f1",
        "citation_coverage",
        "citation_validity",
        "support_rate",
        "unsupported_claim_rate",
        "abstention_rate",
        "abstention_correctness",
    ]
    for column in metric_columns:
        if column not in df.columns:
            continue
        numeric = pd.to_numeric(df[column], errors="coerce")
        summary[column] = round(float(numeric.mean()), 6) if numeric.notna().any() else None
    return summary


def flatten_answer_row(
    *,
    record: dict[str, Any],
    golden_match: GoldenMatchResult,
) -> dict[str, Any]:
    return {
        "run_id": record["run_id"],
        "study_id": record["study_id"],
        "q_id": record["q_id"],
        "question": record["question"],
        "model_key": record["model_key"],
        "resolved_provider": record["resolved_provider"],
        "resolved_model_name": record["resolved_model_name"],
        "mode": record["mode"],
        "answer_text": record["answer_text"],
        "golden_answer": golden_match.record.golden_answer if golden_match.record else "",
        "golden_match_type": golden_match.match_type,
        "golden_question_mismatch": golden_match.question_mismatch,
        "prompt_hash": record["prompt_hash"],
        "manifest_path": record["manifest_path"],
        "answer_abstained": record.get("answer_abstained"),
        "citation_missing": record.get("citation_missing"),
        "citation_invalid": record.get("citation_invalid"),
        "support_failed": record.get("support_failed"),
        "created_at": record["created_at"],
        "success": record["success"],
        "error_message": record["error_message"],
        "retrieved_context_ids": "\n".join(record.get("retrieved_context_ids", [])),
        "citations_extracted": "\n".join(record.get("citation_tokens", [])),
    }


def flatten_metrics_row(record: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "run_id",
        "study_id",
        "q_id",
        "question",
        "model_key",
        "mode",
        "golden_available",
        "golden_normalized_exact_match",
        "golden_semantic_similarity",
        "golden_list_precision",
        "golden_list_recall",
        "golden_list_f1",
        "golden_numeric_exact_match",
        "golden_numeric_tolerance_match",
        "citation_coverage",
        "citation_validity",
        "support_rate",
        "unsupported_claim_rate",
        "answer_abstained",
        "abstention_correctness",
        "citation_missing",
        "citation_invalid",
        "support_failed",
        "success",
        "error_message",
    ]
    return {key: record.get(key) for key in keys}


def export_answers_workbook(
    *,
    path: Path,
    answer_rows: list[dict[str, Any]],
    local_retrieval_rows: list[dict[str, Any]],
    tavily_retrieval_rows: list[dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    answers_df = pd.DataFrame(answer_rows)
    local_df = pd.DataFrame(local_retrieval_rows)
    tavily_df = pd.DataFrame(tavily_retrieval_rows)
    manifest_df = pd.DataFrame(
        [{"key": key, "value": json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value} for key, value in manifest.items()]
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        answers_df.to_excel(writer, sheet_name="answers", index=False)
        local_df.to_excel(writer, sheet_name="local_retrieval", index=False)
        tavily_df.to_excel(writer, sheet_name="offline_tavily_retrieval", index=False)
        manifest_df.to_excel(writer, sheet_name="manifest", index=False)


def export_metrics_workbook(
    *,
    path: Path,
    per_question_rows: list[dict[str, Any]],
    summary_row: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    per_question_df = pd.DataFrame(per_question_rows)
    summary_df = pd.DataFrame([summary_row])
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        per_question_df.to_excel(writer, sheet_name="per_question_metrics", index=False)
        summary_df.to_excel(writer, sheet_name="summary", index=False)


def update_study_outputs(
    *,
    study_id: str,
    results_dir: Path,
) -> tuple[Path, Path]:
    manifest_paths = sorted(results_dir.glob("*_manifest.json"))
    rows: list[dict[str, Any]] = []
    for manifest_path in manifest_paths:
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if payload.get("study_id") != study_id:
            continue
        summary_row = payload.get("summary_row")
        if not isinstance(summary_row, dict):
            continue
        row = dict(summary_row)
        row["manifest_path"] = str(manifest_path.resolve())
        rows.append(row)

    summary_df = pd.DataFrame(rows)
    if summary_df.empty:
        summary_df = pd.DataFrame(
            columns=[
                "study_id",
                "run_id",
                "model_key",
                "mode",
                "question_count",
                "questions_with_golden_answers",
                "golden_normalized_exact_match",
                "golden_semantic_similarity",
                "golden_list_f1",
                "citation_coverage",
                "support_rate",
                "unsupported_claim_rate",
                "abstention_rate",
                "abstention_correctness",
            ]
        )

    leaderboard_df = summary_df.copy()
    if not leaderboard_df.empty:
        sort_columns = [
            column
            for column in [
                "golden_normalized_exact_match",
                "golden_semantic_similarity",
                "support_rate",
                "citation_coverage",
            ]
            if column in leaderboard_df.columns
        ]
        if sort_columns:
            leaderboard_df = leaderboard_df.sort_values(
                by=sort_columns,
                ascending=[False] * len(sort_columns),
                na_position="last",
            )

    summary_path = results_dir / f"{study_id}_summary.xlsx"
    leaderboard_path = results_dir / f"{study_id}_leaderboard.csv"
    with pd.ExcelWriter(summary_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="study_summary", index=False)
    leaderboard_df.to_csv(leaderboard_path, index=False)
    return summary_path, leaderboard_path


def export_hybrid_value_report(
    *,
    study_id: str,
    results_dir: Path,
) -> Path | None:
    latest_manifests = _latest_run_manifests_by_model_and_mode(study_id=study_id, results_dir=results_dir)
    comparison_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []

    model_keys = sorted(
        {
            model_key
            for model_key, mode in latest_manifests
            if mode in {"local_rag", "hybrid_rag"}
        }
    )
    for model_key in model_keys:
        local_manifest = latest_manifests.get((model_key, "local_rag"))
        hybrid_manifest = latest_manifests.get((model_key, "hybrid_rag"))
        if local_manifest is None or hybrid_manifest is None:
            continue

        local_records = _load_jsonl_records(Path(local_manifest["response_jsonl_path"]))
        hybrid_records = _load_jsonl_records(Path(hybrid_manifest["response_jsonl_path"]))
        ordered_qids = list(local_records)
        for q_id in hybrid_records:
            if q_id not in local_records:
                ordered_qids.append(q_id)

        model_rows: list[dict[str, Any]] = []
        for q_id in ordered_qids:
            local_record = local_records.get(q_id, {})
            hybrid_record = hybrid_records.get(q_id, {})
            question = normalize_cell(hybrid_record.get("question") or local_record.get("question"))
            local_answer = normalize_cell(local_record.get("answer_text"))
            hybrid_answer = normalize_cell(hybrid_record.get("answer_text"))
            local_answer_normalized = normalize_match_text(strip_nonsemantic_sections(local_answer))
            hybrid_answer_normalized = normalize_match_text(strip_nonsemantic_sections(hybrid_answer))

            local_citations = [
                normalize_cell(token)
                for token in (local_record.get("citation_tokens") or [])
                if normalize_cell(token)
            ]
            hybrid_citations = [
                normalize_cell(token)
                for token in (hybrid_record.get("citation_tokens") or [])
                if normalize_cell(token)
            ]
            local_web_citations = [token for token in local_citations if token.startswith("WEB:")]
            hybrid_web_citations = [token for token in hybrid_citations if token.startswith("WEB:")]

            local_answer_abstained = bool(local_record.get("answer_abstained")) if local_record else None
            hybrid_answer_abstained = bool(hybrid_record.get("answer_abstained")) if hybrid_record else None
            answer_changed = bool(
                local_answer_normalized
                or hybrid_answer_normalized
            ) and local_answer_normalized != hybrid_answer_normalized

            support_delta = _numeric_delta(
                hybrid_record.get("support_rate"),
                local_record.get("support_rate"),
            )
            semantic_delta = _numeric_delta(
                hybrid_record.get("golden_semantic_similarity"),
                local_record.get("golden_semantic_similarity"),
            )
            exact_match_delta = _numeric_delta(
                hybrid_record.get("golden_normalized_exact_match"),
                local_record.get("golden_normalized_exact_match"),
            )
            citation_coverage_delta = _numeric_delta(
                hybrid_record.get("citation_coverage"),
                local_record.get("citation_coverage"),
            )
            unsupported_delta = _numeric_delta(
                hybrid_record.get("unsupported_claim_rate"),
                local_record.get("unsupported_claim_rate"),
            )
            abstention_delta = _numeric_delta(
                _bool_as_float(hybrid_answer_abstained),
                _bool_as_float(local_answer_abstained),
            )

            hybrid_value_signal = bool(
                hybrid_web_citations and (
                    answer_changed
                    or (local_answer_abstained is True and hybrid_answer_abstained is False)
                    or (support_delta is not None and support_delta > 0)
                    or (semantic_delta is not None and semantic_delta > 0)
                    or (exact_match_delta is not None and exact_match_delta > 0)
                )
            )

            row = {
                "study_id": study_id,
                "model_key": model_key,
                "q_id": q_id,
                "question": question,
                "local_run_id": local_manifest.get("run_id", ""),
                "hybrid_run_id": hybrid_manifest.get("run_id", ""),
                "local_answer": local_answer,
                "hybrid_answer": hybrid_answer,
                "local_answer_abstained": local_answer_abstained,
                "hybrid_answer_abstained": hybrid_answer_abstained,
                "local_citations": "\n".join(local_citations),
                "hybrid_citations": "\n".join(hybrid_citations),
                "local_retrieved_context_ids": "\n".join(local_record.get("retrieved_context_ids", []) or []),
                "hybrid_retrieved_context_ids": "\n".join(hybrid_record.get("retrieved_context_ids", []) or []),
                "local_support_rate": local_record.get("support_rate"),
                "hybrid_support_rate": hybrid_record.get("support_rate"),
                "support_rate_delta": support_delta,
                "local_citation_coverage": local_record.get("citation_coverage"),
                "hybrid_citation_coverage": hybrid_record.get("citation_coverage"),
                "citation_coverage_delta": citation_coverage_delta,
                "local_unsupported_claim_rate": local_record.get("unsupported_claim_rate"),
                "hybrid_unsupported_claim_rate": hybrid_record.get("unsupported_claim_rate"),
                "unsupported_claim_rate_delta": unsupported_delta,
                "local_golden_normalized_exact_match": local_record.get("golden_normalized_exact_match"),
                "hybrid_golden_normalized_exact_match": hybrid_record.get("golden_normalized_exact_match"),
                "golden_exact_match_delta": exact_match_delta,
                "local_golden_semantic_similarity": local_record.get("golden_semantic_similarity"),
                "hybrid_golden_semantic_similarity": hybrid_record.get("golden_semantic_similarity"),
                "golden_semantic_similarity_delta": semantic_delta,
                "local_has_web_citations": bool(local_web_citations),
                "hybrid_has_web_citations": bool(hybrid_web_citations),
                "local_web_citation_count": len(local_web_citations),
                "hybrid_web_citation_count": len(hybrid_web_citations),
                "hybrid_answer_changed": answer_changed,
                "hybrid_value_signal": hybrid_value_signal,
                "abstention_delta": abstention_delta,
            }
            comparison_rows.append(row)
            model_rows.append(row)

        if model_rows:
            model_df = pd.DataFrame(model_rows)
            summary_rows.append(
                {
                    "study_id": study_id,
                    "model_key": model_key,
                    "local_run_id": local_manifest.get("run_id", ""),
                    "hybrid_run_id": hybrid_manifest.get("run_id", ""),
                    "paired_questions": int(len(model_df)),
                    "hybrid_answers_changed": int(model_df["hybrid_answer_changed"].fillna(False).sum()),
                    "hybrid_questions_with_web_citations": int(model_df["hybrid_has_web_citations"].fillna(False).sum()),
                    "hybrid_value_signal_questions": int(model_df["hybrid_value_signal"].fillna(False).sum()),
                    "avg_local_golden_exact_match": _mean_or_none(model_df["local_golden_normalized_exact_match"]),
                    "avg_hybrid_golden_exact_match": _mean_or_none(model_df["hybrid_golden_normalized_exact_match"]),
                    "avg_golden_exact_match_delta": _mean_or_none(model_df["golden_exact_match_delta"]),
                    "avg_local_golden_semantic_similarity": _mean_or_none(model_df["local_golden_semantic_similarity"]),
                    "avg_hybrid_golden_semantic_similarity": _mean_or_none(model_df["hybrid_golden_semantic_similarity"]),
                    "avg_golden_semantic_similarity_delta": _mean_or_none(model_df["golden_semantic_similarity_delta"]),
                    "avg_local_support_rate": _mean_or_none(model_df["local_support_rate"]),
                    "avg_hybrid_support_rate": _mean_or_none(model_df["hybrid_support_rate"]),
                    "avg_support_rate_delta": _mean_or_none(model_df["support_rate_delta"]),
                    "avg_local_citation_coverage": _mean_or_none(model_df["local_citation_coverage"]),
                    "avg_hybrid_citation_coverage": _mean_or_none(model_df["hybrid_citation_coverage"]),
                    "avg_citation_coverage_delta": _mean_or_none(model_df["citation_coverage_delta"]),
                    "avg_local_unsupported_claim_rate": _mean_or_none(model_df["local_unsupported_claim_rate"]),
                    "avg_hybrid_unsupported_claim_rate": _mean_or_none(model_df["hybrid_unsupported_claim_rate"]),
                    "avg_unsupported_claim_rate_delta": _mean_or_none(model_df["unsupported_claim_rate_delta"]),
                    "avg_abstention_delta": _mean_or_none(model_df["abstention_delta"]),
                }
            )

    if not comparison_rows:
        return None

    summary_df = pd.DataFrame(summary_rows)
    comparison_df = pd.DataFrame(comparison_rows)
    output_path = results_dir / f"{study_id}_hybrid_value.xlsx"
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        comparison_df.to_excel(writer, sheet_name="per_question", index=False)
    return output_path


def _latest_run_manifests_by_model_and_mode(
    *,
    study_id: str,
    results_dir: Path,
) -> dict[tuple[str, str], dict[str, Any]]:
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for manifest_path in sorted(results_dir.glob("*_manifest.json")):
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if payload.get("study_id") != study_id:
            continue
        model_key = normalize_cell(payload.get("model_key"))
        mode = normalize_cell(payload.get("mode"))
        response_jsonl_path = normalize_cell(payload.get("response_jsonl_path"))
        if not model_key or not mode or not response_jsonl_path:
            continue
        key = (model_key, mode)
        existing = latest.get(key)
        if existing is None or str(payload.get("timestamp", "")) >= str(existing.get("timestamp", "")):
            payload["manifest_path"] = str(manifest_path.resolve())
            latest[key] = payload
    return latest


def _load_jsonl_records(path: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return records
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            q_id = normalize_cell(payload.get("q_id"))
            if q_id:
                records[q_id] = payload
    return records


def _numeric_delta(current: Any, baseline: Any) -> float | None:
    current_value = _to_float(current)
    baseline_value = _to_float(baseline)
    if current_value is None or baseline_value is None:
        return None
    return round(current_value - baseline_value, 6)


def _mean_or_none(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce")
    if not numeric.notna().any():
        return None
    return round(float(numeric.mean()), 6)


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_as_float(value: bool | None) -> float | None:
    if value is None:
        return None
    return 1.0 if value else 0.0
