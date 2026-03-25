#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import pandas as pd


def to_snake_case(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "_", str(value).strip())
    text = re.sub(r"_+", "_", text).strip("_")
    return text.lower()


def normalize_qid(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def normalize_question(value: Any) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def is_nonempty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    return bool(str(value).strip())


def ordered_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def split_logged_list(value: Any) -> list[str]:
    if not is_nonempty(value):
        return []
    raw = str(value)
    pieces = re.split(r"[\n,;]+", raw)
    return ordered_unique([piece.strip() for piece in pieces if piece.strip()])


DOC_PATTERNS = [
    re.compile(r"\[\s*doc\s*:\s*([^\]\n]+?)\s*\]", flags=re.IGNORECASE),
    re.compile(r"\(\s*doc\s*:\s*([^)]+?)\s*\)", flags=re.IGNORECASE),
    re.compile(r"\bdoc\s*:\s*([A-Za-z0-9:_./-]+)", flags=re.IGNORECASE),
]
WEB_PATTERNS = [
    re.compile(r"\[\s*web\s*:\s*([^\]\n]+?)\s*\]", flags=re.IGNORECASE),
    re.compile(r"\(\s*web\s*:\s*([^)]+?)\s*\)", flags=re.IGNORECASE),
    re.compile(r"\bweb\s*:\s*([A-Za-z0-9:_./-]+)", flags=re.IGNORECASE),
]
URL_PATTERN = re.compile(r"https?://[^\s)\]]+", flags=re.IGNORECASE)
SOURCE_PATTERN = re.compile(r"\[\s*source\s+([0-9]+)\s*\]", flags=re.IGNORECASE)

ABSTENTION_PATTERN = re.compile(
    r"\b(i don['’]t know|not found|cannot find|can['’]t find|insufficient information|not enough information|unable to find)\b",
    flags=re.IGNORECASE,
)


def clean_citation_token(token: str) -> str:
    return token.strip().strip("[](){}").strip(" ,.;:")


def extract_citations_from_answer(answer: Any) -> tuple[list[str], list[str]]:
    text = "" if not is_nonempty(answer) else str(answer)
    doc_hits: list[str] = []
    web_hits: list[str] = []
    for pattern in DOC_PATTERNS:
        doc_hits.extend(clean_citation_token(match.group(1)) for match in pattern.finditer(text))
    for pattern in WEB_PATTERNS:
        web_hits.extend(clean_citation_token(match.group(1)) for match in pattern.finditer(text))
    web_hits.extend(clean_citation_token(match.group(0)) for match in URL_PATTERN.finditer(text))
    web_hits.extend(f"source_{match.group(1)}" for match in SOURCE_PATTERN.finditer(text))
    return ordered_unique(doc_hits), ordered_unique(web_hits)


def word_count(text: Any) -> int:
    if not is_nonempty(text):
        return 0
    return len(re.findall(r"\S+", str(text)))


@dataclass(slots=True)
class LoadedWorkbook:
    path: Path
    mode: str
    responses: pd.DataFrame
    local_retrieval: pd.DataFrame
    web_retrieval: pd.DataFrame
    warnings: list[str]


def resolve_default_path(candidates: list[str]) -> Path:
    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            return path.resolve()
    raise FileNotFoundError(f"None of the default paths exist: {', '.join(candidates)}")


def choose_sheet_name(xl: pd.ExcelFile, preferred: str) -> str | None:
    normalized = {to_snake_case(sheet): sheet for sheet in xl.sheet_names}
    return normalized.get(preferred)


def load_workbook(path: Path, mode: str) -> LoadedWorkbook:
    xl = pd.ExcelFile(path)
    warnings: list[str] = []

    response_sheet = choose_sheet_name(xl, "responses") or xl.sheet_names[0]
    local_sheet = choose_sheet_name(xl, "local_retrieval")
    web_sheet = choose_sheet_name(xl, "tavily_retrieval")

    responses = pd.read_excel(path, sheet_name=response_sheet)
    responses.columns = [to_snake_case(column) for column in responses.columns]

    local_retrieval = pd.read_excel(path, sheet_name=local_sheet) if local_sheet else pd.DataFrame()
    if not local_retrieval.empty:
        local_retrieval.columns = [to_snake_case(column) for column in local_retrieval.columns]

    web_retrieval = pd.read_excel(path, sheet_name=web_sheet) if web_sheet else pd.DataFrame()
    if not web_retrieval.empty:
        web_retrieval.columns = [to_snake_case(column) for column in web_retrieval.columns]

    qid_column = next((col for col in responses.columns if col in {"q_id", "qid", "question_id"}), None)
    if qid_column is None:
        responses["q_id"] = [str(index + 1) for index in range(len(responses))]
        warnings.append(f"{mode}: q_id column missing, generated sequential ids.")
    elif qid_column != "q_id":
        responses["q_id"] = responses[qid_column]

    question_column = next((col for col in responses.columns if col == "question"), None)
    if question_column is None:
        raise ValueError(f"{mode}: could not find a question column in {path}")

    responses["q_id"] = responses["q_id"].map(normalize_qid)
    responses["question"] = responses["question"].map(normalize_question)
    responses = responses[responses["q_id"].ne("") & responses["question"].ne("")].copy()

    if "model_response" not in responses.columns:
        for candidate in ("answer", "generated_response", "answer_text"):
            if candidate in responses.columns:
                responses["model_response"] = responses[candidate]
                break
    if "model_response" not in responses.columns:
        raise ValueError(f"{mode}: could not find an answer/model_response column in {path}")

    responses["model_response"] = responses["model_response"].fillna("")
    responses["doc_citations"] = responses["doc_citations"].fillna("") if "doc_citations" in responses.columns else ""
    responses["web_citations"] = responses["web_citations"].fillna("") if "web_citations" in responses.columns else ""
    responses["local_chunk_ids"] = responses["local_chunk_ids"].fillna("") if "local_chunk_ids" in responses.columns else ""
    responses["tavily_chunk_ids"] = responses["tavily_chunk_ids"].fillna("") if "tavily_chunk_ids" in responses.columns else ""
    responses["combined_context_used"] = responses["combined_context_used"].fillna("") if "combined_context_used" in responses.columns else ""

    for retrieval_df in (local_retrieval, web_retrieval):
        if retrieval_df.empty:
            continue
        if "q_id" in retrieval_df.columns:
            retrieval_df["q_id"] = retrieval_df["q_id"].map(normalize_qid)

    return LoadedWorkbook(
        path=path,
        mode=mode,
        responses=responses,
        local_retrieval=local_retrieval,
        web_retrieval=web_retrieval,
        warnings=warnings,
    )


def build_retrieval_lookup(df: pd.DataFrame, id_column: str = "chunk_id") -> dict[str, list[str]]:
    if df.empty or "q_id" not in df.columns or id_column not in df.columns:
        return {}
    lookup: dict[str, list[str]] = {}
    for q_id, frame in df.groupby("q_id", dropna=False):
        lookup[str(q_id)] = ordered_unique(
            [str(value).strip() for value in frame[id_column].tolist() if is_nonempty(value)]
        )
    return lookup


def build_score_lookup(df: pd.DataFrame) -> dict[str, list[float]]:
    if df.empty or "q_id" not in df.columns or "score" not in df.columns:
        return {}
    lookup: dict[str, list[float]] = {}
    for q_id, frame in df.groupby("q_id", dropna=False):
        scores = pd.to_numeric(frame["score"], errors="coerce").dropna().tolist()
        lookup[str(q_id)] = scores
    return lookup


def add_derived_columns(report: LoadedWorkbook) -> None:
    local_sheet_lookup = build_retrieval_lookup(report.local_retrieval)
    web_sheet_lookup = build_retrieval_lookup(report.web_retrieval)

    responses = report.responses
    responses["logged_doc_citations_list"] = responses["doc_citations"].map(split_logged_list)
    responses["logged_web_citations_list"] = responses["web_citations"].map(split_logged_list)
    extracted = responses["model_response"].map(extract_citations_from_answer)
    responses["extracted_doc_citations"] = extracted.map(lambda item: item[0])
    responses["extracted_web_citations"] = extracted.map(lambda item: item[1])
    responses["citation_count_total"] = (
        responses["extracted_doc_citations"].map(len) + responses["extracted_web_citations"].map(len)
    )
    responses["has_any_citations"] = responses["citation_count_total"] > 0
    responses["citation_column_empty"] = (
        responses["logged_doc_citations_list"].map(len) + responses["logged_web_citations_list"].map(len)
    ) == 0
    responses["citation_mismatch"] = responses["has_any_citations"] & responses["citation_column_empty"]
    responses["abstained"] = responses["model_response"].str.contains(ABSTENTION_PATTERN, na=False)
    responses["answer_word_count"] = responses["model_response"].map(word_count)

    responses["local_retrieved_ids"] = responses.apply(
        lambda row: local_sheet_lookup.get(row["q_id"], split_logged_list(row.get("local_chunk_ids", ""))),
        axis=1,
    )
    responses["web_retrieved_ids"] = responses.apply(
        lambda row: web_sheet_lookup.get(row["q_id"], split_logged_list(row.get("tavily_chunk_ids", ""))),
        axis=1,
    )
    responses["local_retrieval_count"] = responses["local_retrieved_ids"].map(len)
    responses["web_retrieval_count"] = responses["web_retrieved_ids"].map(len)

    responses["citation_match_ratio"] = responses.apply(citation_match_ratio, axis=1)
    responses["answerable_proxy"] = False
    if report.mode == "no_rag":
        responses["answerable_proxy"] = ~responses["abstained"]
    elif report.mode == "local_rag":
        responses["answerable_proxy"] = (responses["local_retrieval_count"] > 0) & ~responses["abstained"]
    else:
        responses["answerable_proxy"] = (
            (responses["local_retrieval_count"] > 0) & ~responses["abstained"]
        )


def citation_match_ratio(row: pd.Series) -> float | None:
    cited_ids = ordered_unique(row["extracted_doc_citations"] + row["extracted_web_citations"])
    if not cited_ids:
        return None
    retrieved_ids = set(row["local_retrieved_ids"]) | set(row["web_retrieved_ids"])
    if not retrieved_ids:
        return 0.0
    matched = sum(1 for citation in cited_ids if citation in retrieved_ids)
    return matched / len(cited_ids)


def mode_isolation_checks(report: LoadedWorkbook) -> dict[str, Any]:
    responses = report.responses
    violations: dict[str, list[str]] = {
        "unexpected_local_retrieval": [],
        "unexpected_web_retrieval": [],
        "unexpected_context": [],
        "missing_local_retrieval": [],
        "missing_web_retrieval": [],
    }

    for _, row in responses.iterrows():
        q_id = row["q_id"]
        has_local = row["local_retrieval_count"] > 0
        has_web = row["web_retrieval_count"] > 0
        has_context = is_nonempty(row.get("combined_context_used", ""))

        if report.mode == "no_rag":
            if has_local:
                violations["unexpected_local_retrieval"].append(q_id)
            if has_web:
                violations["unexpected_web_retrieval"].append(q_id)
            if has_context:
                violations["unexpected_context"].append(q_id)
        elif report.mode == "local_rag":
            if not has_local:
                violations["missing_local_retrieval"].append(q_id)
            if has_web:
                violations["unexpected_web_retrieval"].append(q_id)
        elif report.mode == "hybrid_rag":
            if not has_local:
                violations["missing_local_retrieval"].append(q_id)
            if not has_web:
                violations["missing_web_retrieval"].append(q_id)

    row_count = len(responses)
    return {
        "mode": report.mode,
        "row_count": row_count,
        "violations": violations,
        "pass_count": row_count - len({qid for ids in violations.values() for qid in ids}),
    }


def build_mode_summary(report: LoadedWorkbook) -> dict[str, Any]:
    responses = report.responses
    citation_count_series = pd.to_numeric(responses["citation_count_total"], errors="coerce").fillna(0)
    answer_word_counts = pd.to_numeric(responses["answer_word_count"], errors="coerce").fillna(0)
    return {
        "mode": report.mode,
        "rows": len(responses),
        "answers_with_any_citations_pct": round(float((responses["has_any_citations"].mean()) * 100), 2),
        "avg_citations_per_answer": round(float(citation_count_series.mean()), 2),
        "citation_mismatch_rate_pct": round(float((responses["citation_mismatch"].mean()) * 100), 2),
        "abstention_rate_pct": round(float((responses["abstained"].mean()) * 100), 2),
        "answerable_proxy_rate_pct": round(float((responses["answerable_proxy"].mean()) * 100), 2),
        "avg_answer_words": round(float(answer_word_counts.mean()), 2),
        "avg_local_retrieval_count": round(float(pd.to_numeric(responses["local_retrieval_count"]).mean()), 2),
        "avg_web_retrieval_count": round(float(pd.to_numeric(responses["web_retrieval_count"]).mean()), 2),
        "avg_citation_match_ratio": round(
            float(pd.Series([value for value in responses["citation_match_ratio"] if value is not None]).mean())
            if any(value is not None for value in responses["citation_match_ratio"])
            else float("nan"),
            3,
        ),
    }


def top_score_stats(report: LoadedWorkbook, which: str) -> tuple[pd.Series, dict[str, Any]]:
    retrieval_df = report.local_retrieval if which == "local" else report.web_retrieval
    if retrieval_df.empty or "q_id" not in retrieval_df.columns or "score" not in retrieval_df.columns:
        return pd.Series(dtype=float), {"available": False}
    top_scores = (
        retrieval_df.assign(score=pd.to_numeric(retrieval_df["score"], errors="coerce"))
        .dropna(subset=["score"])
        .groupby("q_id")["score"]
        .max()
        .sort_index()
    )
    if top_scores.empty:
        return top_scores, {"available": False}
    threshold = float(top_scores.quantile(0.10))
    low_qids = top_scores[top_scores <= threshold].index.tolist()
    return top_scores, {
        "available": True,
        "avg_top_score": round(float(top_scores.mean()), 5),
        "min_top_score": round(float(top_scores.min()), 5),
        "p10_threshold": round(threshold, 5),
        "low_score_qids": low_qids,
    }


def comparative_table(reports: dict[str, LoadedWorkbook]) -> pd.DataFrame:
    merged = None
    for mode, report in reports.items():
        frame = report.responses[[
            "q_id",
            "question",
            "model_response",
            "abstained",
            "citation_count_total",
            "citation_match_ratio",
            "local_retrieval_count",
            "web_retrieval_count",
        ]].copy()
        rename_map = {
            "model_response": f"{mode}_answer",
            "abstained": f"abstained_{mode}",
            "citation_count_total": f"citation_count_{mode}",
            "citation_match_ratio": f"citation_retrieval_match_{mode}",
            "local_retrieval_count": f"local_retrieval_count_{mode}",
            "web_retrieval_count": f"web_retrieval_count_{mode}",
        }
        frame = frame.rename(columns=rename_map)
        if merged is None:
            merged = frame
        else:
            merged = merged.merge(frame, on=["q_id", "question"], how="outer")

    if merged is None:
        return pd.DataFrame()
    merged["local_retrieval_count"] = merged.get("local_retrieval_count_local_rag", 0)
    merged["web_retrieval_count"] = merged.get("web_retrieval_count_hybrid_rag", 0)
    columns = [
        "q_id",
        "question",
        "no_rag_answer",
        "local_rag_answer",
        "hybrid_rag_answer",
        "local_retrieval_count",
        "web_retrieval_count",
        "abstained_no_rag",
        "abstained_local_rag",
        "abstained_hybrid_rag",
        "citation_count_local_rag",
        "citation_count_hybrid_rag",
        "citation_retrieval_match_local_rag",
        "citation_retrieval_match_hybrid_rag",
    ]
    existing_columns = [column for column in columns if column in merged.columns]
    return merged[existing_columns].sort_values(by="q_id", key=lambda series: series.map(normalize_qid))


def human_review_template(merged: pd.DataFrame, reports: dict[str, LoadedWorkbook]) -> pd.DataFrame:
    local_ids = (
        reports["local_rag"].responses.set_index("q_id")["local_retrieved_ids"].map(lambda items: ", ".join(items))
        if "local_rag" in reports
        else pd.Series(dtype=str)
    )
    web_ids = (
        reports["hybrid_rag"].responses.set_index("q_id")["web_retrieved_ids"].map(lambda items: ", ".join(items))
        if "hybrid_rag" in reports
        else pd.Series(dtype=str)
    )
    review = merged.copy()
    review["local_evidence_ids"] = review["q_id"].map(local_ids).fillna("")
    review["web_evidence_ids"] = review["q_id"].map(web_ids).fillna("")
    review["groundedness"] = ""
    review["completeness"] = ""
    review["usefulness"] = ""
    review["comments"] = ""
    return review


def mismatch_examples(report: LoadedWorkbook, limit: int = 3) -> list[dict[str, str]]:
    subset = report.responses[report.responses["citation_mismatch"]].head(limit)
    examples: list[dict[str, str]] = []
    for _, row in subset.iterrows():
        examples.append(
            {
                "q_id": row["q_id"],
                "question": row["question"],
                "answer_excerpt": str(row["model_response"])[:300].replace("\n", " "),
            }
        )
    return examples


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No data available._"
    return df.to_markdown(index=False)


def build_problem_score(reports: dict[str, LoadedWorkbook], isolation_results: dict[str, dict[str, Any]], low_score_flags: dict[str, set[str]]) -> pd.DataFrame:
    qid_scores: dict[str, int] = {}
    for mode, result in isolation_results.items():
        for ids in result["violations"].values():
            for q_id in ids:
                qid_scores[q_id] = qid_scores.get(q_id, 0) + 2
    for mode, report in reports.items():
        for _, row in report.responses.iterrows():
            score = 0
            if row["abstained"]:
                score += 1
            if row["citation_mismatch"]:
                score += 1
            if row["citation_match_ratio"] == 0.0:
                score += 1
            if row["q_id"] in low_score_flags.get(mode, set()):
                score += 1
            if score:
                qid_scores[row["q_id"]] = qid_scores.get(row["q_id"], 0) + score
    ranked = sorted(qid_scores.items(), key=lambda item: (-item[1], normalize_qid(item[0])))
    return pd.DataFrame(ranked, columns=["q_id", "issue_score"])


def write_report(
    out_dir: Path,
    reports: dict[str, LoadedWorkbook],
    isolation_results: dict[str, dict[str, Any]],
    mode_summaries: pd.DataFrame,
    merged: pd.DataFrame,
    low_score_stats: dict[str, dict[str, Any]],
) -> Path:
    mismatch_rows: list[dict[str, str]] = []
    for mode, report in reports.items():
        for example in mismatch_examples(report):
            mismatch_rows.append({"mode": mode, **example})
    mismatch_df = pd.DataFrame(mismatch_rows)

    qid_sets = {mode: set(report.responses["q_id"]) for mode, report in reports.items()}
    common_qids = set.intersection(*qid_sets.values()) if qid_sets else set()
    union_qids = set.union(*qid_sets.values()) if qid_sets else set()
    hybrid_answerable = mode_summaries.set_index("mode").loc["hybrid_rag", "answerable_proxy_rate_pct"] if "hybrid_rag" in mode_summaries["mode"].values else float("nan")
    local_answerable = mode_summaries.set_index("mode").loc["local_rag", "answerable_proxy_rate_pct"] if "local_rag" in mode_summaries["mode"].values else float("nan")
    hybrid_abstention = mode_summaries.set_index("mode").loc["hybrid_rag", "abstention_rate_pct"] if "hybrid_rag" in mode_summaries["mode"].values else float("nan")
    local_abstention = mode_summaries.set_index("mode").loc["local_rag", "abstention_rate_pct"] if "local_rag" in mode_summaries["mode"].values else float("nan")

    action_items = [
        "Enforce one strict citation format in answers: `[DOC:<retrieved_id>]` and `[WEB:<retrieved_id>]` only.",
        "Write parsed citation columns directly from answer generation or post-processing so `doc_citations` and `web_citations` always match the answer text.",
        "Log retrieval ids and scores in a normalized per-question structure even if the retrieval sheets are missing. This keeps analysis stable.",
        "For hybrid runs, preserve explicit `WEB:` citations in the prompt/instructions; web evidence is retrieved, but answers may still cite only DOC ids.",
        "TODO: requires gold/reference answers to measure factual correctness, not just structure, abstention, and citation-proxy grounding.",
    ]

    report_path = out_dir / "REPORT.md"
    with report_path.open("w", encoding="utf-8") as handle:
        handle.write("# Qwen2.5 14B Generated Report Analysis\n\n")
        handle.write("## 1) Dataset Summary\n\n")
        dataset_summary = pd.DataFrame(
            [
                {
                    "mode": mode,
                    "rows": len(report.responses),
                    "unique_q_ids": report.responses["q_id"].nunique(),
                    "workbook": str(report.path),
                }
                for mode, report in reports.items()
            ]
        )
        handle.write(markdown_table(dataset_summary) + "\n\n")
        handle.write(f"Common q_id coverage across all modes: {len(common_qids)} / {len(union_qids)}\n\n")
        if any(report.warnings for report in reports.values()):
            handle.write("Warnings:\n")
            for mode, report in reports.items():
                for warning in report.warnings:
                    handle.write(f"- {warning}\n")
            handle.write("\n")

        handle.write("## 2) Mode Isolation Correctness\n\n")
        isolation_rows = []
        for mode, result in isolation_results.items():
            isolation_rows.append(
                {
                    "mode": mode,
                    "rows": result["row_count"],
                    "pass_count": result["pass_count"],
                    "unexpected_local_retrieval": len(result["violations"]["unexpected_local_retrieval"]),
                    "unexpected_web_retrieval": len(result["violations"]["unexpected_web_retrieval"]),
                    "unexpected_context": len(result["violations"]["unexpected_context"]),
                    "missing_local_retrieval": len(result["violations"]["missing_local_retrieval"]),
                    "missing_web_retrieval": len(result["violations"]["missing_web_retrieval"]),
                }
            )
        isolation_df = pd.DataFrame(isolation_rows)
        handle.write(markdown_table(isolation_df) + "\n\n")
        for mode, result in isolation_results.items():
            handle.write(f"Violations for `{mode}`:\n")
            for name, qids in result["violations"].items():
                handle.write(f"- {name}: {qids if qids else 'none'}\n")
            handle.write("\n")

        handle.write("## 3) Citation Parseability + Consistency\n\n")
        handle.write(markdown_table(mode_summaries[[
            "mode",
            "answers_with_any_citations_pct",
            "avg_citations_per_answer",
            "citation_mismatch_rate_pct",
        ]]) + "\n\n")
        handle.write("Recommended strict format:\n")
        handle.write("- Always cite retrieved local evidence as `[DOC:<retrieved_id>]`\n")
        handle.write("- Always cite retrieved web evidence as `[WEB:<retrieved_id>]`\n")
        handle.write("- Avoid `Source 1`, plain URLs, or unlabeled ids in the final answer\n\n")
        if mismatch_df.empty:
            handle.write("No citation formatting mismatches were detected from the current spreadsheets.\n\n")
        else:
            handle.write("Examples of citation formatting / logging mismatch:\n\n")
            handle.write(markdown_table(mismatch_df) + "\n\n")

        handle.write("## 4) Abstention/Coverage Comparison Across Modes\n\n")
        handle.write(markdown_table(mode_summaries[[
            "mode",
            "abstention_rate_pct",
            "answerable_proxy_rate_pct",
            "avg_answer_words",
        ]]) + "\n\n")
        handle.write(
            f"Hybrid answerable proxy uplift vs local_rag: {round(hybrid_answerable - local_answerable, 2) if pd.notna(hybrid_answerable) and pd.notna(local_answerable) else 'TODO: insufficient data'} percentage points.\n\n"
        )
        handle.write(
            f"Hybrid abstention delta vs local_rag: {round(hybrid_abstention - local_abstention, 2) if pd.notna(hybrid_abstention) and pd.notna(local_abstention) else 'TODO: insufficient data'} percentage points.\n\n"
        )

        handle.write("## 5) Retrieval Diagnostics\n\n")
        retrieval_rows = []
        for mode, stats in low_score_stats.items():
            for corpus in ("local", "web"):
                corpus_stats = stats.get(corpus, {"available": False})
                retrieval_rows.append(
                    {
                        "mode": mode,
                        "corpus": corpus,
                        "avg_top_score": corpus_stats.get("avg_top_score"),
                        "min_top_score": corpus_stats.get("min_top_score"),
                        "p10_threshold": corpus_stats.get("p10_threshold"),
                        "low_score_qids": ", ".join(corpus_stats.get("low_score_qids", [])),
                    }
                )
        retrieval_df = pd.DataFrame(retrieval_rows)
        handle.write(markdown_table(retrieval_df) + "\n\n")
        handle.write(markdown_table(mode_summaries[[
            "mode",
            "avg_local_retrieval_count",
            "avg_web_retrieval_count",
            "avg_citation_match_ratio",
        ]]) + "\n\n")

        handle.write("## 6) Hybrid Value Evidence\n\n")
        hybrid_better = merged[
            merged.get("abstained_local_rag", False).fillna(False)
            & ~merged.get("abstained_hybrid_rag", False).fillna(False)
        ] if not merged.empty else pd.DataFrame()
        handle.write(
            f"- Questions where hybrid abstained less than local_rag: {len(hybrid_better)}\n"
        )
        if not hybrid_better.empty:
            handle.write(f"- q_ids: {hybrid_better['q_id'].tolist()}\n")
        handle.write(
            "- Hybrid adds value structurally when web retrieval exists, abstention falls, citation counts rise, or citation-to-retrieval match improves.\n"
        )
        handle.write("- TODO: requires answer-level gold labels to conclude whether hybrid improved factual correctness, not just coverage proxies.\n\n")

        handle.write("## 7) Actionable Fixes\n\n")
        for item in action_items:
            handle.write(f"- {item}\n")
        handle.write("\n")

    return report_path


def save_outputs(
    out_dir: Path,
    merged: pd.DataFrame,
    human_review: pd.DataFrame,
    reports: dict[str, LoadedWorkbook],
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    merged_csv = out_dir / "qwen14b_merged.csv"
    merged_xlsx = out_dir / "qwen14b_merged.xlsx"
    review_xlsx = out_dir / "human_review_template.xlsx"

    merged.to_csv(merged_csv, index=False)
    with pd.ExcelWriter(merged_xlsx, engine="openpyxl") as writer:
        merged.to_excel(writer, sheet_name="merged", index=False)
    with pd.ExcelWriter(review_xlsx, engine="openpyxl") as writer:
        human_review.to_excel(writer, sheet_name="review", index=False)


def console_summary(
    isolation_results: dict[str, dict[str, Any]],
    mode_summaries: pd.DataFrame,
    problem_df: pd.DataFrame,
) -> None:
    print("Qwen14B report analysis summary")
    for _, row in mode_summaries.iterrows():
        print(
            f"- {row['mode']}: rows={row['rows']}, abstention_rate={row['abstention_rate_pct']}%, "
            f"citations_any={row['answers_with_any_citations_pct']}%, avg_citations={row['avg_citations_per_answer']}"
        )
    print("Mode isolation checks")
    for mode, result in isolation_results.items():
        violation_total = sum(len(ids) for ids in result["violations"].values())
        print(f"- {mode}: pass_count={result['pass_count']}/{result['row_count']}, violation_hits={violation_total}")
    top_problem_qids = problem_df.head(10)["q_id"].tolist() if not problem_df.empty else []
    print(f"Top 10 problematic q_ids: {top_problem_qids}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze generated Qwen14B evaluation reports.")
    parser.add_argument(
        "--no_rag",
        default=str(resolve_default_path([
            "qwen14b_no_rag.xlsx",
            "data/qwen14b_no_rag.xlsx",
            "artifacts/response_outputs/qwen14b_no_rag.xlsx",
        ])),
    )
    parser.add_argument(
        "--local_rag",
        default=str(resolve_default_path([
            "qwen14b_local_rag.xlsx",
            "data/qwen14b_local_rag.xlsx",
            "artifacts/response_outputs/qwen14b_local_rag.xlsx",
        ])),
    )
    parser.add_argument(
        "--hybrid_rag",
        default=str(resolve_default_path([
            "qwen14b_hybrid_rag.xlsx",
            "data/qwen14b_hybrid_rag.xlsx",
            "artifacts/response_outputs/qwen14b_hybrid_rag.xlsx",
        ])),
    )
    parser.add_argument("--out_dir", default="analysis_out/")
    args = parser.parse_args()

    reports = {
        "no_rag": load_workbook(Path(args.no_rag).expanduser().resolve(), "no_rag"),
        "local_rag": load_workbook(Path(args.local_rag).expanduser().resolve(), "local_rag"),
        "hybrid_rag": load_workbook(Path(args.hybrid_rag).expanduser().resolve(), "hybrid_rag"),
    }
    for report in reports.values():
        add_derived_columns(report)

    row_counts = {mode: len(report.responses) for mode, report in reports.items()}
    if len(set(row_counts.values())) != 1:
        print(f"WARNING: row-count mismatch across modes: {row_counts}")

    qid_sets = {mode: set(report.responses["q_id"]) for mode, report in reports.items()}
    if len({frozenset(values) for values in qid_sets.values()}) != 1:
        print(f"WARNING: q_id mismatch across modes: { {mode: len(values) for mode, values in qid_sets.items()} }")

    isolation_results = {mode: mode_isolation_checks(report) for mode, report in reports.items()}
    mode_summaries = pd.DataFrame([build_mode_summary(report) for report in reports.values()])

    low_score_stats: dict[str, dict[str, Any]] = {}
    low_score_flags: dict[str, set[str]] = {}
    for mode, report in reports.items():
        _, local_stats = top_score_stats(report, "local")
        _, web_stats = top_score_stats(report, "web")
        low_score_stats[mode] = {"local": local_stats, "web": web_stats}
        low_score_flags[mode] = set(local_stats.get("low_score_qids", [])) | set(web_stats.get("low_score_qids", []))

    merged = comparative_table(reports)
    human_review = human_review_template(merged, reports)

    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    save_outputs(out_dir, merged, human_review, reports)
    report_path = write_report(out_dir, reports, isolation_results, mode_summaries, merged, low_score_stats)
    problem_df = build_problem_score(reports, isolation_results, low_score_flags)
    console_summary(isolation_results, mode_summaries, problem_df)
    print(f"Artifacts written to {out_dir}")
    print(f"Markdown report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
