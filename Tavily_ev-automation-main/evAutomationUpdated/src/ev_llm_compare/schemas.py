from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class TableRow:
    workbook_path: Path
    sheet_name: str
    row_number: int
    values: dict[str, str]


@dataclass(slots=True)
class WorkbookNote:
    workbook_path: Path
    sheet_name: str
    text: str


@dataclass(slots=True)
class Chunk:
    chunk_id: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)
    token_set: set[str] = field(default_factory=set)


@dataclass(slots=True)
class RetrievalResult:
    chunk_id: str
    text: str
    metadata: dict[str, Any]
    dense_score: float
    lexical_score: float
    final_score: float


@dataclass(slots=True)
class ModelResponse:
    run_name: str
    provider: str
    model_name: str
    rag_enabled: bool
    question: str
    answer: str
    latency_seconds: float
    retrieved_chunks: list[RetrievalResult]
    prompt_tokens_estimate: int
    success: bool
    error_message: str | None = None
