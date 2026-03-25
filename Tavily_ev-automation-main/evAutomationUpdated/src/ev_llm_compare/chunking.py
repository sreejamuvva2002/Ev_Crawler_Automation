from __future__ import annotations

import hashlib
import re
import uuid

from .schemas import Chunk, TableRow, WorkbookNote
from .settings import RetrievalSettings


TOKEN_PATTERN = re.compile(r"[a-z0-9]+")


def tokenize(text: str) -> set[str]:
    return set(TOKEN_PATTERN.findall(text.lower()))


def sliding_window_chunks(text: str, chunk_size: int, chunk_overlap: int) -> list[tuple[int, str]]:
    chunks: list[tuple[int, str]] = []
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if chunk_overlap < 0:
        raise ValueError("chunk_overlap must be non-negative")

    start = 0
    while start < len(text):
        end = min(len(text), start + chunk_size)
        candidate = text[start:end].strip()
        if candidate:
            chunks.append((start, candidate))
        if end == len(text):
            break
        start = max(end - chunk_overlap, start + 1)
    return chunks


class ExcelChunkBuilder:
    def __init__(self, settings: RetrievalSettings):
        self.settings = settings

    def build(self, rows: list[TableRow], notes: list[WorkbookNote]) -> list[Chunk]:
        chunks: list[Chunk] = []
        for row in rows:
            chunks.extend(self._build_row_chunks(row))
        for note in notes:
            chunks.extend(self._build_note_chunks(note))
        return chunks

    def _build_row_chunks(self, row: TableRow) -> list[Chunk]:
        company = row.values.get("Company", "")
        important_order = self._ordered_columns(row.values)
        full_text = " | ".join(f"{column}: {row.values[column]}" for column in important_order)
        row_key = f"{row.workbook_path.name}::{row.sheet_name}::{row.row_number}"
        row_summary = self._row_summary_text(row)

        chunk_specs = [
            ("row_full", full_text),
            ("company_profile", self._company_profile_text(row, important_order)),
        ]
        chunk_specs.extend(self._thematic_chunks(row))

        chunks: list[Chunk] = []
        for chunk_type, text in chunk_specs:
            if not text.strip():
                continue
            chunk_id = self._make_chunk_id(
                f"{row.sheet_name}-{row.row_number}-{chunk_type}-{text[:64]}"
            )
            metadata = {
                "source_file": row.workbook_path.name,
                "sheet_name": row.sheet_name,
                "row_number": row.row_number,
                "row_key": row_key,
                "row_summary": row_summary,
                "company": company,
                "category": row.values.get("Category", ""),
                "industry_group": row.values.get("Industry Group", ""),
                "ev_supply_chain_role": row.values.get("EV Supply Chain Role", ""),
                "product_service": row.values.get("Product / Service", ""),
                "primary_oems": row.values.get("Primary OEMs", ""),
                "location": row.values.get("Location", ""),
                "primary_facility_type": row.values.get("Primary Facility Type", ""),
                "supplier_or_affiliation_type": row.values.get("Supplier or Affiliation Type", ""),
                "classification_method": row.values.get("Classification Method", ""),
                "employment": row.values.get("Employment", ""),
                "ev_battery_relevant": row.values.get("EV / Battery Relevant", ""),
                "chunk_type": chunk_type,
                "fields": list(row.values.keys()),
            }
            chunks.append(
                Chunk(
                    chunk_id=chunk_id,
                    text=text,
                    metadata=metadata,
                    token_set=tokenize(text),
                )
            )
        return chunks

    def _build_note_chunks(self, note: WorkbookNote) -> list[Chunk]:
        chunks: list[Chunk] = []
        for start, candidate in sliding_window_chunks(
            note.text,
            self.settings.note_chunk_size,
            self.settings.note_chunk_overlap,
        ):
            chunk_id = self._make_chunk_id(f"{note.sheet_name}-note-{start}")
            chunks.append(
                Chunk(
                    chunk_id=chunk_id,
                    text=candidate,
                    metadata={
                        "source_file": note.workbook_path.name,
                        "sheet_name": note.sheet_name,
                        "chunk_type": "note_reference",
                    },
                    token_set=tokenize(candidate),
                )
            )
        return chunks

    def _company_profile_text(self, row: TableRow, ordered_columns: list[str]) -> str:
        priority = [
            "Company",
            "Category",
            "Industry Group",
            "EV Supply Chain Role",
            "Primary OEMs",
            "Location",
            "Employment",
            "Product / Service",
            "EV / Battery Relevant",
        ]
        selected = [column for column in priority if column in row.values]
        remaining = [column for column in ordered_columns if column not in selected][:4]
        fields = selected + remaining
        return " | ".join(f"{column}: {row.values[column]}" for column in fields)

    def _row_summary_text(self, row: TableRow) -> str:
        summary_fields = [
            "Company",
            "Category",
            "Industry Group",
            "EV Supply Chain Role",
            "Primary OEMs",
            "Product / Service",
            "Location",
            "Primary Facility Type",
            "Employment",
            "EV / Battery Relevant",
            "Supplier or Affiliation Type",
            "Classification Method",
        ]
        available = [column for column in summary_fields if row.values.get(column)]
        return " | ".join(f"{column}: {row.values[column]}" for column in available)

    def _thematic_chunks(self, row: TableRow) -> list[tuple[str, str]]:
        themes = {
            "identity_theme": [
                "Company",
                "Category",
                "Industry Group",
                "Classification Method",
            ],
            "location_theme": [
                "Company",
                "Location",
                "Primary Facility Type",
                "Employment",
            ],
            "supply_chain_theme": [
                "Company",
                "EV Supply Chain Role",
                "Supplier or Affiliation Type",
                "Primary OEMs",
            ],
            "product_theme": [
                "Company",
                "Product / Service",
                "EV / Battery Relevant",
                "Category",
            ],
        }
        chunks: list[tuple[str, str]] = []
        for theme_name, columns in themes.items():
            available = [column for column in columns if column in row.values]
            if len(available) >= 2:
                text = " | ".join(f"{column}: {row.values[column]}" for column in available)
                chunks.append((theme_name, text))
        return chunks

    def _ordered_columns(self, values: dict[str, str]) -> list[str]:
        preferred = [
            "Company",
            "Category",
            "Industry Group",
            "Location",
            "Primary Facility Type",
            "EV Supply Chain Role",
            "Primary OEMs",
            "Supplier or Affiliation Type",
            "Employment",
            "Product / Service",
            "EV / Battery Relevant",
            "Classification Method",
        ]
        ordered = [column for column in preferred if column in values]
        ordered.extend(column for column in values if column not in ordered)
        return ordered

    def _make_chunk_id(self, raw: str) -> str:
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
        return str(uuid.uuid5(uuid.NAMESPACE_URL, digest))
