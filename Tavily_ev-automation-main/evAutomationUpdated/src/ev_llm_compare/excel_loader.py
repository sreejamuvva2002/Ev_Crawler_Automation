from __future__ import annotations

from pathlib import Path
import re

import pandas as pd

from .schemas import TableRow, WorkbookNote


def normalize_cell(value: object) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return re.sub(r"\s+", " ", str(value)).strip()


def _is_tabular_sheet(df: pd.DataFrame) -> bool:
    populated_columns = [
        column for column in df.columns if normalize_cell(column) and not str(column).startswith("Unnamed")
    ]
    return len(populated_columns) >= 2 and len(df.index) > 0


def load_workbook(workbook_path: str | Path) -> tuple[list[TableRow], list[WorkbookNote]]:
    path = Path(workbook_path).expanduser().resolve()
    rows: list[TableRow] = []
    notes: list[WorkbookNote] = []

    with pd.ExcelFile(path) as excel_file:
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            df = df.dropna(how="all").dropna(axis=1, how="all")
            if df.empty:
                continue

            cleaned_columns = [normalize_cell(column) for column in df.columns]
            df.columns = cleaned_columns

            if _is_tabular_sheet(df):
                for row_idx, (_, series) in enumerate(df.iterrows(), start=1):
                    values = {
                        column: normalize_cell(value)
                        for column, value in series.to_dict().items()
                        if normalize_cell(value)
                    }
                    if values:
                        rows.append(
                            TableRow(
                                workbook_path=path,
                                sheet_name=sheet_name,
                                row_number=row_idx,
                                values=values,
                            )
                        )
            else:
                parts: list[str] = []
                for column in df.columns:
                    for value in df[column].tolist():
                        text = normalize_cell(value)
                        if text:
                            parts.append(text)
                if parts:
                    notes.append(
                        WorkbookNote(
                            workbook_path=path,
                            sheet_name=sheet_name,
                            text="\n".join(parts),
                        )
                    )

    if not rows and not notes:
        raise ValueError(f"No usable content found in workbook: {path}")

    return rows, notes


def load_questions(question_workbook: str | Path, sheet_name: str | None = None) -> list[str]:
    path = Path(question_workbook).expanduser().resolve()
    df = pd.read_excel(path, sheet_name=sheet_name or 0)
    df = df.dropna(how="all").dropna(axis=1, how="all")
    if df.empty:
        raise ValueError(f"No questions found in workbook: {path}")

    preferred_columns = [
        column
        for column in df.columns
        if normalize_cell(column).lower() in {"question", "questions", "query", "prompt"}
    ]
    target_column = preferred_columns[0] if preferred_columns else df.columns[0]

    questions: list[str] = []
    seen: set[str] = set()
    for value in df[target_column].tolist():
        text = normalize_cell(value)
        if len(text) < 5:
            continue
        if text not in seen:
            seen.add(text)
            questions.append(text)

    if not questions:
        raise ValueError(f"No valid questions found in workbook: {path}")

    return questions


def load_reference_answers(
    reference_workbook: str | Path,
    sheet_name: str | None = None,
) -> dict[str, str]:
    path = Path(reference_workbook).expanduser().resolve()
    df = pd.read_excel(path, sheet_name=sheet_name or 0)
    df = df.dropna(how="all").dropna(axis=1, how="all")
    if df.empty:
        raise ValueError(f"No reference answers found in workbook: {path}")

    cleaned_columns = [normalize_cell(column) for column in df.columns]
    df.columns = cleaned_columns

    question_candidates = [
        column
        for column in df.columns
        if normalize_cell(column).lower() in {"question", "questions", "query", "prompt"}
    ]
    answer_candidates = [
        column
        for column in df.columns
        if normalize_cell(column).lower()
        in {
            "golden_answer",
            "golden answer",
            "reference_answer",
            "reference answer",
            "reference",
            "answer",
            "golden_summary",
            "golden summary",
        }
    ]

    if not question_candidates:
        raise ValueError(f"No question column found in reference workbook: {path}")

    question_column = question_candidates[0]
    if answer_candidates:
        answer_column = answer_candidates[0]
    elif len(df.columns) >= 2:
        answer_column = df.columns[1]
    else:
        raise ValueError(f"No answer column found in reference workbook: {path}")

    references: dict[str, str] = {}
    for _, row in df.iterrows():
        question = normalize_cell(row.get(question_column))
        answer = normalize_cell(row.get(answer_column))
        if len(question) < 5 or not answer:
            continue
        references[question] = answer

    if not references:
        raise ValueError(f"No usable reference answers found in workbook: {path}")

    return references
