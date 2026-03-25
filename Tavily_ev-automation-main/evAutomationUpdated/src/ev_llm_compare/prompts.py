from __future__ import annotations

import re

from .schemas import RetrievalResult


SYSTEM_PROMPT = """You answer questions about one or more Excel workbooks.
Use the provided evidence when available. Prefer exact values, company names, counts,
locations, roles, and employment numbers from the source data.
When no workbook evidence is provided, answer from general knowledge as helpfully as possible.
Do not ask the user to re-upload the workbook unless the task explicitly requires exact workbook-only facts."""

NON_RAG_SYSTEM_PROMPT = """You answer from general model knowledge only.
Do not say that a workbook, spreadsheet, or uploaded file is missing.
Do not give spreadsheet, filtering, pivot-table, or workbook instructions unless the user explicitly asks for a method.
If the question asks for exact dataset-specific facts that you cannot verify, say so briefly and then give the closest general answer you can without inventing facts.
Prefer direct domain answers over process descriptions."""

FIELD_LABELS = {
    "category": "Category",
    "industry_group": "Industry Group",
    "ev_supply_chain_role": "EV Supply Chain Role",
    "product_service": "Product / Service",
    "primary_oems": "Primary OEMs",
    "location": "Location",
    "primary_facility_type": "Primary Facility Type",
    "employment": "Employment",
    "ev_battery_relevant": "EV / Battery Relevant",
    "supplier_or_affiliation_type": "Supplier or Affiliation Type",
    "classification_method": "Classification Method",
}


def compact_context_segments(
    question: str,
    results: list[RetrievalResult],
    max_results: int = 5,
    max_chars: int = 4200,
) -> list[str]:
    if not results:
        return ["No retrieved evidence."]

    normalized_question = _normalize_question(question)
    selected = _select_compact_results(normalized_question, results, max_results)
    blocks: list[str] = []
    char_budget = max(600, max_chars)
    reserved_summary_budget = int(char_budget * 0.62)

    for index, result in enumerate(selected, start=1):
        remaining = char_budget - sum(len(block) for block in blocks)
        if remaining < 180:
            break
        if index == 1 and _chunk_type(result) == "structured_match_summary":
            if len(selected) == 1:
                summary_budget = remaining
            else:
                summary_budget = min(remaining, reserved_summary_budget)
            block = _render_structured_summary(
                result,
                summary_budget,
                index=index,
            )
        else:
            block = _render_compact_block(
                normalized_question,
                result,
                remaining,
                index=index,
            )
        if not block:
            continue
        if len(block) > remaining:
            block = block[: max(0, remaining - 3)].rstrip() + "..."
        blocks.append(block)
        if len(blocks) >= max_results:
            break

    return blocks or ["No retrieved evidence."]


def format_context(
    results: list[RetrievalResult],
    question: str | None = None,
    max_results: int = 5,
    max_chars: int = 4200,
    compact: bool = True,
) -> str:
    if not results:
        return "No retrieved evidence."
    if question and compact:
        blocks = compact_context_segments(
            question,
            results,
            max_results=max_results,
            max_chars=max_chars,
        )
        return "\n\n".join(blocks)

    blocks: list[str] = []
    for index, result in enumerate(results, start=1):
        company = result.metadata.get("company") or "n/a"
        chunk_type = result.metadata.get("chunk_type") or "retrieved_chunk"
        source = f"{result.metadata.get('source_file')}::{result.metadata.get('sheet_name')}"
        row_number = result.metadata.get("row_number")
        header = (
            f"[Evidence {index}] type={chunk_type} company={company} source={source}"
            + (f" row={row_number}" if row_number else "")
        )
        blocks.append(f"{header}\n{result.text}")
    return "\n\n".join(blocks)


def build_rag_prompt(question: str, context: str) -> str:
    return (
        "Answer the user question using only the retrieved workbook evidence.\n"
        "If a structured workbook match summary is present, treat it as the primary evidence.\n"
        "The evidence may be compacted to only the most relevant rows and fields.\n\n"
        f"Retrieved evidence:\n{context}\n\n"
        f"Question: {question}\n\n"
        "Instructions:\n"
        "- Be specific and concise.\n"
        "- When the evidence includes exact counts, totals, or requested field values, copy those directly.\n"
        "- If the structured summary already states the final grouped or ranked answer, reproduce that answer directly instead of recomputing it.\n"
        "- For ranked or counted results, include the numeric value on every output line.\n"
        "- When listing companies, preserve names exactly.\n"
        "- Answer with the requested fields only; do not substitute Product / Service for Industry Group or omit Primary Facility Type when it is provided.\n"
        "- Include all supported matches from the evidence, not just one example.\n"
        "- If the evidence already groups companies by EV Supply Chain Role, copy that grouping directly.\n"
        "- Group results when the question asks for grouping.\n"
        "- Do not repeat evidence headers such as [Evidence 1].\n"
        "- Start directly with the answer.\n"
        "- Prefer EV Supply Chain Role over Product / Service when both appear.\n"
        "- Mention if the workbook evidence is incomplete for the question.\n"
        "- Do not invent values that are not in evidence.\n\n"
        "Answer:"
    )


def build_non_rag_prompt(question: str) -> str:
    return (
        "Answer the user question directly from general model knowledge without relying on retrieved workbook evidence.\n"
        "Do not mention missing workbooks, missing datasets, or ask the user to provide files.\n"
        "Do not give spreadsheet, filtering, or pivot-table instructions unless the question explicitly asks for a method.\n"
        "If the question depends on exact workbook-specific facts, say briefly that the exact dataset answer is unknown from general knowledge and then provide the closest domain-level answer you can.\n"
        "Prefer a short substantive answer over an explanation of process.\n\n"
        f"Question: {question}\n\n"
        "Answer:"
    )


def build_reference_prompt(question: str, context: str) -> str:
    return (
        "Create a high-quality reference answer from the workbook evidence.\n"
        "This answer will be used as the ground truth for evaluation.\n\n"
        f"Evidence:\n{context}\n\n"
        f"Question: {question}\n\n"
        "Requirements:\n"
        "- Use only supported facts.\n"
        "- If the evidence does not fully answer the question, state the limitation.\n"
        "- Keep the answer concise but complete.\n\n"
        "Reference answer:"
    )


def _normalize_question(question: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", question.lower()).strip()


def _chunk_type(result: RetrievalResult) -> str:
    return str(result.metadata.get("chunk_type", "") or "retrieved_chunk")


def _select_compact_results(
    normalized_question: str,
    results: list[RetrievalResult],
    max_results: int,
) -> list[RetrievalResult]:
    analytic_question = _is_analytic_question(normalized_question)
    grouped_listing_question = (
        "ev supply chain role" in normalized_question
        and "group" in normalized_question
        and any(term in normalized_question for term in {"show all", "list all", "provide all"})
    )
    definition_question = any(
        term in normalized_question for term in {"methodology", "define", "definition", "meaning"}
    )
    has_structured_summary = any(_chunk_type(result) == "structured_match_summary" for result in results)
    summary_result = next(
        (result for result in results if _chunk_type(result) == "structured_match_summary"),
        None,
    )
    if (
        (analytic_question or grouped_listing_question)
        and summary_result
        and _summary_is_self_sufficient(summary_result.text)
    ):
        result_limit = 1
    elif analytic_question and has_structured_summary:
        result_limit = min(max_results, 3)
    else:
        result_limit = max_results
    ranked = sorted(
        results,
        key=lambda result: (_compact_priority(normalized_question, result), result.final_score),
        reverse=True,
    )
    selected: list[RetrievalResult] = []
    seen_rows: set[str] = set()
    seen_companies: set[str] = set()
    note_used = False

    for result in ranked:
        chunk_type = _chunk_type(result)
        row_key = str(result.metadata.get("row_key", "")).strip()
        company = str(result.metadata.get("company", "")).strip()
        if chunk_type == "note_reference" and not definition_question:
            continue
        if chunk_type == "note_reference" and note_used:
            continue
        if row_key and row_key in seen_rows:
            continue
        if company and company in seen_companies and chunk_type in {"row_full", "company_profile"}:
            continue
        selected.append(result)
        if row_key:
            seen_rows.add(row_key)
        if company:
            seen_companies.add(company)
        if chunk_type == "note_reference":
            note_used = True
        if len(selected) >= result_limit:
            break
    return selected


def _compact_priority(normalized_question: str, result: RetrievalResult) -> float:
    chunk_type = _chunk_type(result)
    if chunk_type == "structured_match_summary":
        return 4.0
    if chunk_type == "structured_row_match":
        return 3.2
    if chunk_type == "note_reference":
        return 3.0 if any(
            term in normalized_question for term in {"methodology", "define", "definition", "meaning"}
        ) else -1.0
    if chunk_type in {"location_theme", "identity_theme", "supply_chain_theme", "product_theme"}:
        return 2.4
    if chunk_type == "company_profile":
        return 2.0
    if chunk_type == "row_full":
        return 1.0
    return 1.4


def _render_structured_summary(result: RetrievalResult, budget: int, index: int) -> str:
    header = _evidence_header(index, result)
    lines = [line.rstrip() for line in result.text.splitlines() if line.strip()]
    kept: list[str] = []
    total = len(header) + 1
    for line in lines:
        projected = total + len(line) + 1
        if projected > budget:
            break
        kept.append(line)
        total = projected
    if not kept:
        kept = [result.text[: max(0, budget - len(header) - 5)].rstrip() + "..."]
    elif len(kept) < len(lines):
        kept.append("...")
    return f"{header}\n" + "\n".join(kept)


def _render_compact_block(
    normalized_question: str,
    result: RetrievalResult,
    budget: int,
    index: int,
) -> str:
    header = _evidence_header(index, result)
    chunk_type = _chunk_type(result)
    if chunk_type == "note_reference":
        body = result.text
    else:
        body = _compact_metadata_line(normalized_question, result)
    if not body:
        body = result.text
    allowance = max(120, budget - len(header) - 1)
    if len(body) > allowance:
        body = body[: max(0, allowance - 3)].rstrip() + "..."
    return f"{header}\n{body}"


def _evidence_header(index: int | None, result: RetrievalResult) -> str:
    company = result.metadata.get("company") or "n/a"
    chunk_type = _chunk_type(result)
    source = f"{result.metadata.get('source_file')}::{result.metadata.get('sheet_name')}"
    row_number = result.metadata.get("row_number")
    prefix = f"[Evidence {index}]" if index is not None else "[Evidence]"
    return (
        f"{prefix} type={chunk_type} company={company} source={source}"
        + (f" row={row_number}" if row_number else "")
    )


def _compact_metadata_line(normalized_question: str, result: RetrievalResult) -> str:
    metadata = result.metadata
    company = str(metadata.get("company", "")).strip()
    chunk_type = _chunk_type(result)
    requested_fields = _requested_fields(normalized_question, chunk_type)
    parts: list[str] = []
    if company:
        parts.append(f"Company: {company}")
    for field_name in requested_fields:
        value = str(metadata.get(field_name, "")).strip()
        if not value:
            continue
        parts.append(f"{FIELD_LABELS[field_name]}: {value}")
    return " | ".join(dict.fromkeys(parts))


def _requested_fields(normalized_question: str, chunk_type: str) -> list[str]:
    fields: list[str] = []
    if "category" in normalized_question:
        fields.append("category")
    if "industry group" in normalized_question:
        fields.append("industry_group")
    if "ev supply chain role" in normalized_question or "role" in normalized_question:
        fields.append("ev_supply_chain_role")
    if "product / service" in normalized_question or "product service" in normalized_question:
        fields.append("product_service")
    if "primary oem" in normalized_question or "linked to" in normalized_question:
        fields.append("primary_oems")
    if any(term in normalized_question for term in {"location", "county", "city"}):
        fields.append("location")
    if "primary facility type" in normalized_question or "facility" in normalized_question:
        fields.append("primary_facility_type")
    if "employment" in normalized_question:
        fields.append("employment")
    if "ev / battery relevant" in normalized_question or "ev battery relevant" in normalized_question:
        fields.append("ev_battery_relevant")
    if "supplier or affiliation type" in normalized_question:
        fields.append("supplier_or_affiliation_type")
    if "classification method" in normalized_question:
        fields.append("classification_method")
    if fields:
        return fields
    if chunk_type == "location_theme":
        return ["location", "primary_facility_type", "employment"]
    if chunk_type == "identity_theme":
        return ["category", "industry_group", "classification_method"]
    if chunk_type == "supply_chain_theme":
        return ["ev_supply_chain_role", "primary_oems", "supplier_or_affiliation_type"]
    if chunk_type == "product_theme":
        return ["product_service", "ev_battery_relevant", "category"]
    return ["category", "ev_supply_chain_role", "location", "employment"]


def _is_analytic_question(normalized_question: str) -> bool:
    return any(
        term in normalized_question
        for term in {
            "count",
            "how many",
            "total",
            "average",
            "median",
            "highest",
            "lowest",
            "top",
            "bottom",
            "range",
            "represented",
            "summary",
            "summarize",
            "compare",
            "only one",
            "at least one company",
            "matching companies",
            "list all companies",
            "show all",
            "group by",
            "group them by",
        }
    )


def _summary_is_self_sufficient(summary_text: str) -> bool:
    normalized = summary_text.lower()
    return any(
        marker in normalized
        for marker in {
            "counts by ",
            "total employment by ",
            "average employment by ",
            "industry groups represented",
            "public oem footprint / supplier listing",
            "cities with both tier 1 and tier 2/3 companies",
            "companies appearing multiple times",
            "ev / battery relevant groups",
            "ev / battery relevant = yes companies",
            "ev supply chain roles with ev / battery relevant = yes companies",
            "matching product / service entries",
            "primary facility type matches",
            "employment-ranked companies",
            "linked companies:",
            "grouped by ev supply chain role",
        }
    )
