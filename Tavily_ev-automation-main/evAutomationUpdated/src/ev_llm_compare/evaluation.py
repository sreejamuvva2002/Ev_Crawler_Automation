from __future__ import annotations

from collections import defaultdict
import json
import os
from pathlib import Path
import re
from typing import Any

import pandas as pd

from .models import LLMClient, create_client, safe_generate
from .prompts import build_reference_prompt, compact_context_segments, format_context
from .schemas import ModelResponse
from .settings import ModelSpec, RuntimeSettings

REPORT_METRIC_NAMES = [
    "answer_accuracy",
    "faithfulness",
    "response_groundedness",
    "grounded_claim_ratio",
    "unsupported_claim_ratio",
    "contradicted_claim_ratio",
]

LLM_JUDGE_SYSTEM_PROMPT = """You are a strict evaluation judge for workbook question answering.
Return only one line in the form SCORE=<value>.
Use a continuous score from 0.00 to 1.00 with exactly two decimal places.
Do not add explanation, JSON, markdown, or extra text."""
LLM_JUDGE_PACKET_SYSTEM_PROMPT = """You are a strict evaluation judge for workbook question answering.
Return only the requested metric lines with no explanation, JSON, markdown, or extra text.
Use continuous scores from 0.00 to 1.00 with exactly two decimal places."""
LLM_ATTRIBUTION_SYSTEM_PROMPT = """You are performing claim-level provenance attribution for workbook question answering.
Return only valid JSON.
Do not add markdown, explanation, or prose before or after the JSON."""

ATTRIBUTION_BATCH_SIZE = 24


def _split_text_blocks(text: str) -> list[str]:
    blocks: list[str] = []
    current: list[str] = []
    for line in text.replace("\r\n", "\n").split("\n"):
        if line.strip():
            current.append(line.rstrip())
            continue
        if current:
            blocks.append("\n".join(current).strip())
            current = []
    if current:
        blocks.append("\n".join(current).strip())
    return blocks


def _is_list_like_line(line: str) -> bool:
    stripped = line.lstrip()
    if not stripped:
        return False
    if stripped.startswith(("-", "*", "•")):
        return True
    if stripped[:1].isdigit():
        return True
    return False


def _sentence_units(text: str) -> list[str]:
    if not text.strip():
        return []

    abbreviations = {
        "co",
        "corp",
        "inc",
        "llc",
        "ltd",
        "mr",
        "mrs",
        "ms",
        "dr",
        "st",
        "vs",
        "etc",
    }
    units: list[str] = []
    current: list[str] = []
    length = len(text)

    for index, char in enumerate(text):
        current.append(char)
        if char not in {".", "!", "?", ";"}:
            continue

        segment = "".join(current).strip()
        if not segment:
            current = []
            continue

        if char == ".":
            tail = segment.rstrip(".").split()[-1].lower() if segment.rstrip(".").split() else ""
            if tail in abbreviations:
                continue

        next_non_space = ""
        cursor = index + 1
        while cursor < length:
            candidate = text[cursor]
            if not candidate.isspace():
                next_non_space = candidate
                break
            cursor += 1
        if next_non_space and next_non_space.islower():
            continue

        units.append(segment)
        current = []

    trailing = "".join(current).strip()
    if trailing:
        units.append(trailing)
    return units


def _segment_response_units(answer: str) -> list[str]:
    text = (answer or "").replace("\r\n", "\n").strip()
    if not text:
        return []

    units: list[str] = []
    for block in _split_text_blocks(text):
        lines = [line.strip() for line in block.split("\n") if line.strip()]
        if not lines:
            continue
        if len(lines) > 1 and sum(1 for line in lines if _is_list_like_line(line)) >= max(1, len(lines) // 2):
            units.extend(lines)
            continue
        if len(lines) > 1 and all(len(line) <= 140 for line in lines):
            units.extend(lines)
            continue
        units.extend(_sentence_units(" ".join(lines)))
    return [unit for unit in units if unit.strip()]


def _build_attribution_prompt(
    question: str,
    retrieved_contexts: list[str],
    unit_batch: list[tuple[int, str]],
) -> str:
    context = "\n\n".join(retrieved_contexts) if retrieved_contexts else "No retrieved evidence."
    units_text = "\n".join(f"{unit_id}. {text}" for unit_id, text in unit_batch)
    return (
        "Task: attribute each response unit to one provenance label.\n"
        "Labels:\n"
        "- knowledge_source: the unit is directly supported by the retrieved workbook evidence.\n"
        "- pretrained: the unit depends on general model knowledge, unsupported synthesis, filler, or anything not directly grounded in the retrieved evidence.\n"
        "Rules:\n"
        "- Use only the two labels above.\n"
        "- If a unit mixes supported and unsupported material, label it pretrained.\n"
        "- If you are uncertain, label it pretrained.\n"
        "- Preserve the unit ids exactly.\n\n"
        f"Question:\n{question}\n\n"
        f"Retrieved evidence:\n{context}\n\n"
        f"Response units:\n{units_text}\n\n"
        "Return JSON in exactly this shape:\n"
        '{"labels":[{"unit_id":1,"label":"knowledge_source"}]}'
    )


def _extract_json_payload(raw_text: str) -> Any | None:
    if not raw_text:
        return None
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        pass

    start_positions = [
        index for index, char in enumerate(raw_text) if char in {"{", "["}
    ]
    for start in start_positions:
        stack: list[str] = []
        in_string = False
        escape = False
        for end in range(start, len(raw_text)):
            char = raw_text[end]
            if in_string:
                if escape:
                    escape = False
                elif char == "\\":
                    escape = True
                elif char == '"':
                    in_string = False
                continue
            if char == '"':
                in_string = True
                continue
            if char in {"{", "["}:
                stack.append("}" if char == "{" else "]")
                continue
            if char in {"}", "]"}:
                if not stack or char != stack[-1]:
                    break
                stack.pop()
                if not stack:
                    candidate = raw_text[start : end + 1]
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        break
    return None


def _parse_attribution_labels(raw_text: str) -> dict[int, str] | None:
    payload = _extract_json_payload(raw_text)
    if not isinstance(payload, dict):
        return None
    labels = payload.get("labels")
    if not isinstance(labels, list):
        return None

    parsed: dict[int, str] = {}
    for item in labels:
        if not isinstance(item, dict):
            return None
        unit_id = item.get("unit_id")
        label = str(item.get("label", "")).strip().lower()
        if not isinstance(unit_id, int):
            return None
        if label not in {"knowledge_source", "pretrained"}:
            return None
        parsed[unit_id] = label
    return parsed


def _llm_judge_attribution(
    judge_client: LLMClient,
    prompt: str,
    retries: int = 2,
) -> dict[int, str] | None:
    for _ in range(max(1, retries + 1)):
        answer, _, success, _ = safe_generate(
            judge_client,
            prompt,
            temperature=0.0,
            max_tokens=1200,
            system_prompt=LLM_ATTRIBUTION_SYSTEM_PROMPT,
        )
        if not success:
            continue
        parsed = _parse_attribution_labels(answer)
        if parsed is not None:
            return parsed
    return None


def attribute_response_sources(
    response: ModelResponse,
    judge_client: LLMClient | None,
    context_result_limit: int = 4,
    context_char_budget: int = 2600,
    compact_context: bool = True,
    max_retries: int = 2,
) -> dict[str, Any]:
    overall_response = response.answer or ""
    if not overall_response.strip():
        return {
            "overall_response": overall_response,
            "knowledge_source_data": "",
            "pretrained_data": "",
            "attribution_units": [],
        }

    # No external knowledge source was available to the model, so the whole answer is treated as pretrained/general.
    if not response.rag_enabled or not response.retrieved_chunks or judge_client is None:
        return {
            "overall_response": overall_response,
            "knowledge_source_data": "",
            "pretrained_data": overall_response,
            "attribution_units": [
                {"unit_id": 1, "unit_text": overall_response, "label": "pretrained"}
            ],
        }

    units = _segment_response_units(overall_response)
    if not units:
        units = [overall_response]

    retrieved_contexts = (
        compact_context_segments(
            response.question,
            response.retrieved_chunks,
            max_results=context_result_limit,
            max_chars=context_char_budget,
        )
        if compact_context
        else [chunk.text for chunk in response.retrieved_chunks]
    )

    labels_by_id: dict[int, str] = {}
    indexed_units = list(enumerate(units, start=1))
    for start in range(0, len(indexed_units), ATTRIBUTION_BATCH_SIZE):
        batch = indexed_units[start : start + ATTRIBUTION_BATCH_SIZE]
        prompt = _build_attribution_prompt(
            question=response.question,
            retrieved_contexts=retrieved_contexts,
            unit_batch=batch,
        )
        parsed = _llm_judge_attribution(
            judge_client,
            prompt,
            retries=max_retries,
        )
        batch_ids = {unit_id for unit_id, _ in batch}
        if parsed is None or not batch_ids.issubset(parsed):
            for unit_id in batch_ids:
                labels_by_id[unit_id] = "pretrained"
            continue
        labels_by_id.update({unit_id: parsed[unit_id] for unit_id in batch_ids})

    knowledge_units: list[str] = []
    pretrained_units: list[str] = []
    attribution_units: list[dict[str, Any]] = []
    for unit_id, unit_text in indexed_units:
        label = labels_by_id.get(unit_id, "pretrained")
        attribution_units.append(
            {
                "unit_id": unit_id,
                "unit_text": unit_text,
                "label": label,
            }
        )
        if label == "knowledge_source":
            knowledge_units.append(unit_text)
        else:
            pretrained_units.append(unit_text)

    return {
        "overall_response": overall_response,
        "knowledge_source_data": "\n".join(knowledge_units).strip(),
        "pretrained_data": "\n".join(pretrained_units).strip(),
        "attribution_units": attribution_units,
    }


def build_reference_answers(
    questions: list[str],
    retrievals: dict[str, list[Any]],
    judge_client: LLMClient,
    context_result_limit: int = 5,
    context_char_budget: int = 4200,
    compact_context: bool = True,
) -> dict[str, str]:
    references: dict[str, str] = {}
    for question in questions:
        context = format_context(
            retrievals[question],
            question=question,
            max_results=context_result_limit,
            max_chars=context_char_budget,
            compact=compact_context,
        )
        prompt = build_reference_prompt(question, context)
        answer, _, success, _ = safe_generate(judge_client, prompt, temperature=0.0, max_tokens=500)
        references[question] = answer if success else "Reference generation failed."
    return references


def _make_judge_client(provider: str, model: str) -> LLMClient:
    runtime = RuntimeSettings()
    runtime.ollama_base_url = os.getenv("OLLAMA_BASE_URL", runtime.ollama_base_url)
    spec = ModelSpec(
        run_name="llm_judge",
        provider=provider,
        model_name=model,
        rag_enabled=False,
    )
    return create_client(spec, runtime)


def _llm_judge_prompt_answer_accuracy(
    question: str,
    answer: str,
    reference_answer: str,
) -> str:
    return (
        "Task: score how well the candidate answer matches the reference answer for a workbook question.\n"
        "Scoring rubric:\n"
        "- 1.00 means fully correct, all major requested facts present, no material errors.\n"
        "- Use the full continuous range 0.00 to 1.00; do not restrict yourself to quarter-step buckets.\n"
        "- Prefer precise decimals like 0.13, 0.42, 0.68, or 0.91 when appropriate.\n"
        "- For list, grouped, and count questions, score by factual coverage and precision.\n"
        "- Missing requested items should reduce the score proportionally.\n"
        "- Wrong extra entities or wrong numbers should reduce the score.\n"
        "- If the answer explicitly says it lacks the workbook/dataset, gives a general method, or gives generic industry examples instead of the requested workbook answer, score 0.00.\n"
        "- If the answer is largely a non-answer or refusal, score 0.00.\n\n"
        f"Question:\n{question}\n\n"
        f"Reference answer:\n{reference_answer}\n\n"
        f"Candidate answer:\n{answer}\n\n"
        "Return exactly one line: SCORE=<0.00-1.00>"
    )


def _llm_judge_prompt_grounding_packet(
    question: str,
    answer: str,
    retrieved_contexts: list[str],
) -> str:
    context = "\n\n".join(retrieved_contexts)
    return (
        "Task: evaluate the candidate answer against the retrieved workbook evidence.\n"
        "Use continuous scores from 0.00 to 1.00 with exactly two decimal places.\n"
        "Definitions:\n"
        "- FAITHFULNESS: how free the answer is from claims contradicted by evidence.\n"
        "- RESPONSE_GROUNDEDNESS: how much of the answer is grounded in evidence.\n"
        "- GROUNDED_CLAIM_RATIO: fraction of substantive claims supported by evidence.\n"
        "- UNSUPPORTED_CLAIM_RATIO: fraction of substantive claims not supported by evidence.\n"
        "- CONTRADICTED_CLAIM_RATIO: fraction of substantive claims contradicted by evidence.\n"
        "Consistency rules:\n"
        "- Prefer precise decimals like 0.18, 0.37, 0.64, or 0.92 when appropriate.\n"
        "- RESPONSE_GROUNDEDNESS should usually match GROUNDED_CLAIM_RATIO.\n"
        "- Higher CONTRADICTED_CLAIM_RATIO should lower FAITHFULNESS.\n"
        "- GROUNDED_CLAIM_RATIO, UNSUPPORTED_CLAIM_RATIO, and CONTRADICTED_CLAIM_RATIO should approximately sum to 1.\n\n"
        f"Question:\n{question}\n\n"
        f"Retrieved evidence:\n{context}\n\n"
        f"Candidate answer:\n{answer}\n\n"
        "Return exactly these five lines:\n"
        "FAITHFULNESS=<0.00-1.00>\n"
        "RESPONSE_GROUNDEDNESS=<0.00-1.00>\n"
        "GROUNDED_CLAIM_RATIO=<0.00-1.00>\n"
        "UNSUPPORTED_CLAIM_RATIO=<0.00-1.00>\n"
        "CONTRADICTED_CLAIM_RATIO=<0.00-1.00>"
    )


def _parse_llm_judge_score(raw_text: str) -> float | None:
    if not raw_text:
        return None
    match = re.search(r"(?<![\d.])(0(?:\.\d+)?|1(?:\.0+)?)\b", raw_text)
    if not match:
        return None
    try:
        score = float(match.group(1))
    except ValueError:
        return None
    if not (0.0 <= score <= 1.0):
        return None
    return round(score, 4)


def _parse_llm_judge_packet(raw_text: str) -> dict[str, float] | None:
    if not raw_text:
        return None
    metric_map = {
        "FAITHFULNESS": "faithfulness",
        "RESPONSE_GROUNDEDNESS": "response_groundedness",
        "GROUNDED_CLAIM_RATIO": "grounded_claim_ratio",
        "UNSUPPORTED_CLAIM_RATIO": "unsupported_claim_ratio",
        "CONTRADICTED_CLAIM_RATIO": "contradicted_claim_ratio",
    }
    parsed: dict[str, float] = {}
    for label, metric_name in metric_map.items():
        match = re.search(
            rf"{label}\s*=\s*(0(?:\.\d+)?|1(?:\.0+)?)\b",
            raw_text,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        score = float(match.group(1))
        if not (0.0 <= score <= 1.0):
            return None
        parsed[metric_name] = round(score, 4)
    return parsed


def _llm_judge_metric(
    judge_client: LLMClient,
    prompt: str,
    retries: int = 2,
) -> float | None:
    for _ in range(max(1, retries + 1)):
        answer, _, success, _ = safe_generate(
            judge_client,
            prompt,
            temperature=0.0,
            max_tokens=12,
            system_prompt=LLM_JUDGE_SYSTEM_PROMPT,
        )
        if not success:
            continue
        parsed = _parse_llm_judge_score(answer)
        if parsed is not None:
            return parsed
    return None


def _llm_judge_grounding_packet(
    judge_client: LLMClient,
    prompt: str,
    retries: int = 2,
) -> dict[str, float] | None:
    for _ in range(max(1, retries + 1)):
        answer, _, success, _ = safe_generate(
            judge_client,
            prompt,
            temperature=0.0,
            max_tokens=80,
            system_prompt=LLM_JUDGE_PACKET_SYSTEM_PROMPT,
        )
        if not success:
            continue
        parsed = _parse_llm_judge_packet(answer)
        if parsed is not None:
            return parsed
    return None


def _score_response_metrics(
    response: ModelResponse,
    reference_answers: dict[str, str],
    judge_client: LLMClient,
    max_retries: int,
    context_result_limit: int,
    context_char_budget: int,
    compact_context: bool,
) -> dict[str, Any]:
    record = {
        "run_name": response.run_name,
        "question": response.question,
        **{metric_name: None for metric_name in REPORT_METRIC_NAMES},
    }
    reference_answer = reference_answers.get(response.question, "")
    if response.success and reference_answer:
        record["answer_accuracy"] = _llm_judge_metric(
            judge_client,
            _llm_judge_prompt_answer_accuracy(
                response.question,
                response.answer,
                reference_answer,
            ),
            retries=max_retries,
        )
    if response.success and response.rag_enabled and response.retrieved_chunks:
        retrieved_contexts = (
            compact_context_segments(
                response.question,
                response.retrieved_chunks,
                max_results=context_result_limit,
                max_chars=context_char_budget,
            )
            if compact_context
            else [chunk.text for chunk in response.retrieved_chunks]
        )
        grounding_packet = _llm_judge_grounding_packet(
            judge_client,
            _llm_judge_prompt_grounding_packet(
                response.question,
                response.answer,
                retrieved_contexts,
            ),
            retries=max_retries,
        )
        if grounding_packet is not None:
            record.update(grounding_packet)
    return record


def run_evaluation_metrics(
    responses: list[ModelResponse],
    reference_answers: dict[str, str],
    judge_provider: str,
    judge_model: str,
    max_retries: int = 2,
    context_result_limit: int = 4,
    context_char_budget: int = 2600,
    compact_context: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    judge_client = _make_judge_client(judge_provider, judge_model)

    record_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for response in responses:
        record_lookup[(response.run_name, response.question)] = _score_response_metrics(
            response=response,
            reference_answers=reference_answers,
            judge_client=judge_client,
            max_retries=max_retries,
            context_result_limit=context_result_limit,
            context_char_budget=context_char_budget,
            compact_context=compact_context,
        )

    per_run_records = [
        record_lookup[(response.run_name, response.question)]
        for response in responses
    ]

    per_run_df = pd.DataFrame(
        per_run_records,
        columns=["run_name", "question", *REPORT_METRIC_NAMES],
    )
    for metric_name in REPORT_METRIC_NAMES:
        per_run_df[metric_name] = pd.to_numeric(per_run_df[metric_name], errors="coerce")
    summary_df = (
        per_run_df.groupby("run_name", dropna=False)
        .agg({metric_name: "mean" for metric_name in REPORT_METRIC_NAMES})
        .reset_index()
        .sort_values(by=REPORT_METRIC_NAMES[0], ascending=False, na_position="last")
    )
    return per_run_df, summary_df


def export_results(
    output_dir: Path,
    responses: list[ModelResponse],
    retrievals: dict[str, list[Any]],
    references: dict[str, str],
    reference_sources: dict[str, str],
    metrics_per_run: pd.DataFrame | None = None,
    metrics_summary: pd.DataFrame | None = None,
    filename_prefix: str = "comparison_report",
    single_sheet_only: bool = False,
    **legacy_kwargs: Any,
) -> Path:
    if "ragas_per_run" in legacy_kwargs and metrics_per_run is None:
        metrics_per_run = legacy_kwargs.pop("ragas_per_run")
    if "ragas_summary" in legacy_kwargs and metrics_summary is None:
        metrics_summary = legacy_kwargs.pop("ragas_summary")
    if legacy_kwargs:
        unknown = ", ".join(sorted(legacy_kwargs))
        raise TypeError(f"Unexpected keyword argument(s): {unknown}")

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    workbook_path = output_dir / f"{filename_prefix}_{timestamp}.xlsx"

    response_rows: list[dict[str, Any]] = []
    retrieval_rows: list[dict[str, Any]] = []
    for response in responses:
        response_rows.append(
            {
                "run_name": response.run_name,
                "provider": response.provider,
                "model_name": response.model_name,
                "rag_enabled": response.rag_enabled,
                "question": response.question,
                "reference_answer": references.get(response.question, ""),
                "reference_source": reference_sources.get(response.question, ""),
                "answer": response.answer,
                "latency_seconds": response.latency_seconds,
                "prompt_tokens_estimate": response.prompt_tokens_estimate,
                "success": response.success,
                "error_message": response.error_message,
            }
        )

    for question, chunks in retrievals.items():
        for rank, chunk in enumerate(chunks, start=1):
            retrieval_rows.append(
                {
                    "question": question,
                    "rank": rank,
                    "company": chunk.metadata.get("company"),
                    "sheet_name": chunk.metadata.get("sheet_name"),
                    "chunk_type": chunk.metadata.get("chunk_type"),
                    "final_score": chunk.final_score,
                    "dense_score": chunk.dense_score,
                    "lexical_score": chunk.lexical_score,
                    "text": chunk.text,
                }
            )

    comparison_df = _build_comparison_sheet(response_rows, metrics_per_run)
    single_sheet_df = _build_single_sheet_report(response_rows, metrics_per_run)

    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        single_sheet_df.to_excel(writer, sheet_name="all_in_one", index=False)
        if not single_sheet_only:
            comparison_df.to_excel(writer, sheet_name="responses", index=False)
            pd.DataFrame(response_rows).to_excel(writer, sheet_name="responses_raw", index=False)
            pd.DataFrame(retrieval_rows).to_excel(writer, sheet_name="retrieval", index=False)
            pd.DataFrame(
                [
                    {
                        "question": question,
                        "reference_answer": answer,
                        "reference_source": reference_sources.get(question, ""),
                    }
                    for question, answer in references.items()
                ]
            ).to_excel(writer, sheet_name="references", index=False)
            if metrics_per_run is not None:
                metrics_per_run.to_excel(writer, sheet_name="metrics_per_question", index=False)
            if metrics_summary is not None:
                metrics_summary.to_excel(writer, sheet_name="metrics_summary", index=False)

    return workbook_path


def export_metrics_workbook(
    output_dir: Path,
    metrics_per_run: pd.DataFrame | None = None,
    metrics_summary: pd.DataFrame | None = None,
    filename_prefix: str = "metrics_report",
    **legacy_kwargs: Any,
) -> Path | None:
    if "ragas_per_run" in legacy_kwargs and metrics_per_run is None:
        metrics_per_run = legacy_kwargs.pop("ragas_per_run")
    if "ragas_summary" in legacy_kwargs and metrics_summary is None:
        metrics_summary = legacy_kwargs.pop("ragas_summary")
    if legacy_kwargs:
        unknown = ", ".join(sorted(legacy_kwargs))
        raise TypeError(f"Unexpected keyword argument(s): {unknown}")

    if metrics_per_run is None and metrics_summary is None:
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    workbook_path = output_dir / f"{filename_prefix}_{timestamp}.xlsx"
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        if metrics_per_run is not None:
            metrics_per_run.to_excel(writer, sheet_name="metrics_per_question", index=False)
        if metrics_summary is not None:
            metrics_summary.to_excel(writer, sheet_name="metrics_summary", index=False)
    return workbook_path


def export_single_model_report(
    output_dir: Path,
    responses: list[ModelResponse],
    references: dict[str, str],
    reference_sources: dict[str, str],
    judge_provider: str,
    judge_model: str,
    max_retries: int = 2,
    context_result_limit: int = 4,
    context_char_budget: int = 2600,
    compact_context: bool = True,
    metrics_per_run: pd.DataFrame | None = None,
    filename_prefix: str | None = None,
) -> Path:
    if not responses:
        raise ValueError("Cannot export a single-model report with no responses.")

    run_names = sorted({response.run_name for response in responses})
    if len(run_names) != 1:
        joined = ", ".join(run_names)
        raise ValueError(
            "Single-model report requires exactly one run. "
            f"Received: {joined}"
        )

    run_name = run_names[0]
    judge_client = None
    if any(response.rag_enabled and response.retrieved_chunks for response in responses):
        judge_client = _make_judge_client(judge_provider, judge_model)

    metric_lookup = _metric_lookup(metrics_per_run)
    report_rows: list[dict[str, Any]] = []
    attribution_rows: list[dict[str, Any]] = []
    for response in responses:
        attribution = attribute_response_sources(
            response,
            judge_client=judge_client,
            context_result_limit=context_result_limit,
            context_char_budget=context_char_budget,
            compact_context=compact_context,
            max_retries=max_retries,
        )
        metrics = metric_lookup.get((response.question, response.run_name), {})
        report_row: dict[str, Any] = {
            "Question": response.question,
            "reference_answer": references.get(response.question, ""),
            "reference_source": reference_sources.get(response.question, ""),
            "overall_response": attribution["overall_response"],
            "knowledge_source_data": attribution["knowledge_source_data"],
            "pretrained_data": attribution["pretrained_data"],
        }
        for metric_name in REPORT_METRIC_NAMES:
            report_row[metric_name] = metrics.get(metric_name)
        report_rows.append(report_row)

        for unit in attribution["attribution_units"]:
            attribution_rows.append(
                {
                    "question": response.question,
                    "run_name": response.run_name,
                    "unit_id": unit["unit_id"],
                    "label": unit["label"],
                    "unit_text": unit["unit_text"],
                }
            )

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    prefix = filename_prefix or f"{run_name}_single_model_report"
    workbook_path = output_dir / f"{prefix}_{timestamp}.xlsx"

    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        pd.DataFrame(report_rows).to_excel(writer, sheet_name="report", index=False)
        pd.DataFrame(attribution_rows).to_excel(
            writer,
            sheet_name="attribution_units",
            index=False,
        )
        if metrics_per_run is not None:
            metrics_per_run.to_excel(writer, sheet_name="metrics_per_question", index=False)
            summary_df = (
                metrics_per_run.groupby("run_name", dropna=False)
                .agg({metric_name: "mean" for metric_name in REPORT_METRIC_NAMES})
                .reset_index()
            )
            summary_df.to_excel(writer, sheet_name="metrics_summary", index=False)

    return workbook_path


def export_response_sets(
    output_dir: Path,
    responses: list[ModelResponse],
    references: dict[str, str],
    reference_sources: dict[str, str],
    metrics_per_run: pd.DataFrame | None = None,
    metrics_summary: pd.DataFrame | None = None,
    **legacy_kwargs: Any,
) -> Path:
    if "ragas_per_run" in legacy_kwargs and metrics_per_run is None:
        metrics_per_run = legacy_kwargs.pop("ragas_per_run")
    if "ragas_summary" in legacy_kwargs and metrics_summary is None:
        metrics_summary = legacy_kwargs.pop("ragas_summary")
    if legacy_kwargs:
        unknown = ", ".join(sorted(legacy_kwargs))
        raise TypeError(f"Unexpected keyword argument(s): {unknown}")

    output_dir.mkdir(parents=True, exist_ok=True)
    run_dir = output_dir

    metric_lookup = _metric_lookup(metrics_per_run)
    all_rows: list[dict[str, Any]] = []
    grouped: defaultdict[str, list[ModelResponse]] = defaultdict(list)
    for response in responses:
        grouped[response.run_name].append(response)
        all_rows.append(
            {
                "run_name": response.run_name,
                "provider": response.provider,
                "model_name": response.model_name,
                "rag_enabled": response.rag_enabled,
                "question": response.question,
                "reference_answer": references.get(response.question, ""),
                "reference_source": reference_sources.get(response.question, ""),
                "answer": response.answer,
                "latency_seconds": response.latency_seconds,
                "success": response.success,
                "error_message": response.error_message,
                **_response_metrics(response, metric_lookup),
            }
        )

    pd.DataFrame(all_rows).to_csv(run_dir / "all_runs_responses.csv", index=False)
    single_sheet_df = _build_single_sheet_report(all_rows, metrics_per_run)
    with pd.ExcelWriter(run_dir / "all_runs_single_sheet.xlsx", engine="openpyxl") as writer:
        single_sheet_df.to_excel(writer, sheet_name="all_in_one", index=False)
    if metrics_per_run is not None or metrics_summary is not None:
        with pd.ExcelWriter(run_dir / "all_runs_metrics.xlsx", engine="openpyxl") as writer:
            if metrics_per_run is not None:
                metrics_per_run.to_excel(writer, sheet_name="metrics_per_question", index=False)
            if metrics_summary is not None:
                metrics_summary.to_excel(writer, sheet_name="metrics_summary", index=False)

    for run_name, run_responses in grouped.items():
        rows = [
            {
                "question": response.question,
                "reference_answer": references.get(response.question, ""),
                "reference_source": reference_sources.get(response.question, ""),
                "answer": response.answer,
                "latency_seconds": response.latency_seconds,
                "success": response.success,
                "error_message": response.error_message,
                **_response_metrics(response, metric_lookup),
            }
            for response in run_responses
        ]
        pd.DataFrame(rows).to_csv(run_dir / f"{run_name}_responses.csv", index=False)

        markdown_lines = [f"# {run_name}", ""]
        for index, response in enumerate(run_responses, start=1):
            markdown_lines.extend(
                [
                    f"## Question {index}",
                    f"Question: {response.question}",
                    "",
                    "Answer:",
                    response.answer or "(empty)",
                    "",
                ]
            )
        (run_dir / f"{run_name}_responses.md").write_text(
            "\n".join(markdown_lines),
            encoding="utf-8",
        )

    return run_dir


def _metric_lookup(
    metrics_per_run: pd.DataFrame | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    if metrics_per_run is None or metrics_per_run.empty:
        return {}
    return {
        (row["question"], row["run_name"]): row
        for row in metrics_per_run.to_dict(orient="records")
    }


def _response_metrics(
    response: ModelResponse,
    metric_lookup: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    metrics = metric_lookup.get((response.question, response.run_name), {})
    return {metric_name: metrics.get(metric_name) for metric_name in REPORT_METRIC_NAMES}


def _build_comparison_sheet(
    response_rows: list[dict[str, Any]],
    metrics_per_run: pd.DataFrame | None,
) -> pd.DataFrame:
    question_order = list(dict.fromkeys(row["question"] for row in response_rows))
    run_order = list(dict.fromkeys(row["run_name"] for row in response_rows))

    response_lookup = {
        (row["question"], row["run_name"]): row
        for row in response_rows
    }

    metric_names: list[str] = list(REPORT_METRIC_NAMES)
    metric_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    if metrics_per_run is not None and not metrics_per_run.empty:
        metric_lookup = _metric_lookup(metrics_per_run)

    comparison_rows: list[dict[str, Any]] = []
    for question in question_order:
        first_response = next(
            (response_lookup[(question, run_name)] for run_name in run_order if (question, run_name) in response_lookup),
            {},
        )
        row: dict[str, Any] = {
            "Question": question,
            "reference_answer": first_response.get("reference_answer", ""),
            "reference_source": first_response.get("reference_source", ""),
        }

        for run_name in run_order:
            response = response_lookup.get((question, run_name), {})
            row[run_name] = response.get("answer", "")

        for run_name in run_order:
            metrics = metric_lookup.get((question, run_name), {})
            for metric_name in metric_names:
                row[f"{run_name}_{metric_name}"] = metrics.get(metric_name)

        for run_name in run_order:
            response = response_lookup.get((question, run_name), {})
            row[f"{run_name}_latency_seconds"] = response.get("latency_seconds")

        for run_name in run_order:
            response = response_lookup.get((question, run_name), {})
            row[f"{run_name}_prompt_tokens_estimate"] = response.get("prompt_tokens_estimate")

        comparison_rows.append(row)

    return pd.DataFrame(comparison_rows)


def _build_single_sheet_report(
    response_rows: list[dict[str, Any]],
    metrics_per_run: pd.DataFrame | None,
) -> pd.DataFrame:
    question_order = list(dict.fromkeys(row["question"] for row in response_rows))
    run_order = list(dict.fromkeys(row["run_name"] for row in response_rows))

    response_lookup = {
        (row["question"], row["run_name"]): row
        for row in response_rows
    }

    metric_names: list[str] = list(REPORT_METRIC_NAMES)
    metric_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    if metrics_per_run is not None and not metrics_per_run.empty:
        metric_lookup = _metric_lookup(metrics_per_run)

    rows: list[dict[str, Any]] = []
    for question in question_order:
        first_response = next(
            (response_lookup[(question, run_name)] for run_name in run_order if (question, run_name) in response_lookup),
            {},
        )
        row: dict[str, Any] = {
            "Question": question,
            "reference_answer": first_response.get("reference_answer", ""),
            "reference_source": first_response.get("reference_source", ""),
        }
        for run_name in run_order:
            response = response_lookup.get((question, run_name), {})
            row[run_name] = response.get("answer", "")
            metrics = metric_lookup.get((question, run_name), {})
            for metric_name in metric_names:
                row[f"{run_name}_{metric_name}"] = metrics.get(metric_name)
        rows.append(row)

    ordered_columns = ["Question", "reference_answer", "reference_source"]
    for run_name in run_order:
        ordered_columns.append(run_name)
        for metric_name in metric_names:
            ordered_columns.append(f"{run_name}_{metric_name}")
    return pd.DataFrame(rows, columns=ordered_columns)


run_ragas = run_evaluation_metrics
