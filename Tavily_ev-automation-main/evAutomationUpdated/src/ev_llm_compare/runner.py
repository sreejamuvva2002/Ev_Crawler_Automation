from __future__ import annotations

from pathlib import Path

from .chunking import ExcelChunkBuilder
from .evaluation import (
    build_reference_answers,
    export_metrics_workbook,
    export_response_sets,
    export_results,
    export_single_model_report,
    run_evaluation_metrics,
)
from .excel_loader import load_questions, load_reference_answers, load_workbook
from .models import create_client, safe_generate
from .prompts import (
    NON_RAG_SYSTEM_PROMPT,
    SYSTEM_PROMPT,
    build_non_rag_prompt,
    build_rag_prompt,
    format_context,
)
from .retrieval import HybridRetriever
from .schemas import ModelResponse
from .settings import AppConfig, ModelSpec


class ComparisonRunner:
    def __init__(self, config: AppConfig):
        self.config = config

    def run(
        self,
        data_workbook: str,
        question_workbook: str,
        question_sheet: str | None = None,
        skip_evaluation: bool = False,
        question_limit: int | None = None,
        selected_run_names: list[str] | None = None,
        output_dir: str | None = None,
        response_output_dir: str | None = None,
        single_sheet_only: bool = False,
        export_response_files: bool = True,
        golden_workbook: str | None = None,
        golden_sheet: str | None = None,
        write_checkpoint: bool = False,
        single_model_report: bool = False,
        **legacy_kwargs: object,
    ) -> Path:
        if "skip_ragas" in legacy_kwargs:
            skip_evaluation = skip_evaluation or bool(legacy_kwargs.pop("skip_ragas"))
        if legacy_kwargs:
            unknown = ", ".join(sorted(legacy_kwargs))
            raise TypeError(f"Unexpected keyword argument(s): {unknown}")

        active_models = self._select_models(selected_run_names)
        if output_dir is not None:
            self.config.runtime.output_dir = Path(output_dir)
        self._log(f"Loading data workbook: {data_workbook}")
        rows, notes = load_workbook(data_workbook)
        self._log(f"Loaded workbook content: {len(rows)} tabular rows, {len(notes)} note sheets")
        self._log(f"Loading question workbook: {question_workbook}")
        questions = load_questions(question_workbook, sheet_name=question_sheet)
        if question_limit is not None:
            questions = questions[:question_limit]
        self._log(f"Loaded {len(questions)} questions")

        self._log("Building structured chunks")
        chunk_builder = ExcelChunkBuilder(self.config.retrieval)
        chunks = chunk_builder.build(rows, notes)
        self._log(f"Built {len(chunks)} chunks")
        self._log("Initializing retriever and indexing chunks")
        retriever = HybridRetriever(
            chunks=chunks,
            settings=self.config.retrieval,
            qdrant_path=self.config.runtime.qdrant_path,
        )
        try:
            self._log("Running retrieval for all questions")
            retrievals = {question: retriever.retrieve(question) for question in questions}
            self._log("Retrieval complete")
            responses: list[ModelResponse] = []

            for model_index, spec in enumerate(active_models, start=1):
                self._log(
                    f"Running model {model_index}/{len(active_models)}: "
                    f"{spec.run_name} ({spec.model_name}, rag={spec.rag_enabled})"
                )
                client = create_client(spec, self.config.runtime)
                for question_index, question in enumerate(questions, start=1):
                    self._log(
                        f"  Question {question_index}/{len(questions)} for {spec.run_name}"
                    )
                    question_retrieval = retrievals[question]
                    prompt = (
                        build_rag_prompt(
                            question,
                            format_context(
                                question_retrieval,
                                question=question,
                                max_results=self.config.retrieval.generation_context_result_limit,
                                max_chars=self.config.retrieval.generation_context_char_budget,
                                compact=self.config.retrieval.compact_context_enabled,
                            ),
                        )
                        if spec.rag_enabled
                        else build_non_rag_prompt(question)
                    )
                    answer, latency, success, error = safe_generate(
                        client,
                        prompt,
                        temperature=spec.temperature,
                        max_tokens=spec.max_tokens,
                        system_prompt=SYSTEM_PROMPT if spec.rag_enabled else NON_RAG_SYSTEM_PROMPT,
                    )
                    responses.append(
                        ModelResponse(
                            run_name=spec.run_name,
                            provider=spec.provider,
                            model_name=spec.model_name,
                            rag_enabled=spec.rag_enabled,
                            question=question,
                            answer=answer,
                            latency_seconds=latency,
                            retrieved_chunks=question_retrieval if spec.rag_enabled else [],
                            prompt_tokens_estimate=max(1, len(prompt) // 4),
                            success=success,
                            error_message=error,
                        )
                    )

            metrics_per_run = None
            metrics_summary = None
            references = {question: "" for question in questions}
            reference_sources = {question: "missing" for question in questions}
            golden_path = self._resolve_reference_workbook(golden_workbook)
            if golden_path is not None:
                self._log(f"Loading golden answers workbook: {golden_path}")
                golden_references = load_reference_answers(golden_path, sheet_name=golden_sheet)
                matched_count = 0
                for question in questions:
                    answer = golden_references.get(question, "")
                    if answer:
                        references[question] = answer
                        reference_sources[question] = "golden"
                        matched_count += 1
                self._log(
                    f"Matched {matched_count}/{len(questions)} questions to golden answers"
                )
            if skip_evaluation:
                self._log("Skipping reference generation and evaluation metrics")
            else:
                try:
                    missing_reference_questions = [
                        question for question in questions if not references.get(question)
                    ]
                    if missing_reference_questions:
                        self._log(
                            "Generating fallback reference answers for "
                            f"{len(missing_reference_questions)} questions not covered by golden answers"
                        )
                        judge_spec = ModelSpec(
                            run_name="evaluation_judge",
                            provider=self.config.evaluation.judge_provider,
                            model_name=self.config.evaluation.judge_model,
                            rag_enabled=False,
                        )
                        judge_client = create_client(judge_spec, self.config.runtime)
                        generated_references = build_reference_answers(
                            missing_reference_questions,
                            retrievals,
                            judge_client,
                            context_result_limit=self.config.retrieval.generation_context_result_limit,
                            context_char_budget=self.config.retrieval.generation_context_char_budget,
                            compact_context=self.config.retrieval.compact_context_enabled,
                        )
                        for question, answer in generated_references.items():
                            references[question] = answer
                            reference_sources[question] = "generated"
                    else:
                        self._log("Using golden answers for answer_accuracy evaluation")
                    if write_checkpoint:
                        checkpoint_path = export_results(
                            output_dir=self.config.runtime.output_dir,
                            responses=responses,
                            retrievals=retrievals,
                            references=references,
                            reference_sources=reference_sources,
                            metrics_per_run=None,
                            metrics_summary=None,
                            filename_prefix="comparison_checkpoint",
                            single_sheet_only=single_sheet_only,
                        )
                        self._log(f"Checkpoint workbook written to {checkpoint_path}")
                    self._log("Running evaluation metrics")
                    metrics_per_run, metrics_summary = run_evaluation_metrics(
                        responses=responses,
                        reference_answers=references,
                        judge_provider=self.config.evaluation.judge_provider,
                        judge_model=self.config.evaluation.judge_model,
                        max_retries=self.config.evaluation.max_retries,
                        context_result_limit=self.config.retrieval.evaluation_context_result_limit,
                        context_char_budget=self.config.retrieval.evaluation_context_char_budget,
                        compact_context=self.config.retrieval.compact_context_enabled,
                    )
                except Exception as exc:
                    self._log(
                        "Reference generation or evaluation failed: "
                        f"{exc}. Continuing without metric sheets."
                    )

            if export_response_files and response_output_dir:
                response_dir_path = export_response_sets(
                    output_dir=Path(response_output_dir),
                    responses=responses,
                    references=references,
                    reference_sources=reference_sources,
                    metrics_per_run=metrics_per_run,
                    metrics_summary=metrics_summary,
                )
                self._log(f"Per-run response files written to {response_dir_path}")

            if single_model_report:
                if len(active_models) != 1:
                    raise ValueError(
                        "--single-model-report requires exactly one selected run. "
                        "Use --run-name once."
                    )
                single_model_path = export_single_model_report(
                    output_dir=self.config.runtime.output_dir,
                    responses=responses,
                    references=references,
                    reference_sources=reference_sources,
                    judge_provider=self.config.evaluation.judge_provider,
                    judge_model=self.config.evaluation.judge_model,
                    max_retries=self.config.evaluation.max_retries,
                    context_result_limit=self.config.retrieval.evaluation_context_result_limit,
                    context_char_budget=self.config.retrieval.evaluation_context_char_budget,
                    compact_context=self.config.retrieval.compact_context_enabled,
                    metrics_per_run=metrics_per_run,
                )
                self._log(f"Single-model report written to {single_model_path}")

            self._log("Exporting results workbook")
            output_path = export_results(
                output_dir=self.config.runtime.output_dir,
                responses=responses,
                retrievals=retrievals,
                references=references,
                reference_sources=reference_sources,
                metrics_per_run=metrics_per_run,
                metrics_summary=metrics_summary,
                single_sheet_only=single_sheet_only,
            )
            metrics_path = None
            if not single_sheet_only:
                metrics_path = export_metrics_workbook(
                    output_dir=self.config.runtime.output_dir,
                    metrics_per_run=metrics_per_run,
                    metrics_summary=metrics_summary,
                )
                if metrics_path is not None:
                    self._log(f"Metrics workbook written to {metrics_path}")
            self._log(f"Done. Report written to {output_path}")
            return output_path
        finally:
            retriever.close()

    def _log(self, message: str) -> None:
        print(f"[ev-llm-compare] {message}", flush=True)

    def _select_models(self, selected_run_names: list[str] | None) -> list[ModelSpec]:
        if not selected_run_names:
            return self.config.models

        requested = set(selected_run_names)
        selected = [model for model in self.config.models if model.run_name in requested]
        missing = sorted(requested - {model.run_name for model in selected})
        if missing:
            available = ", ".join(model.run_name for model in self.config.models)
            missing_display = ", ".join(missing)
            raise ValueError(f"Unknown run name(s): {missing_display}. Available runs: {available}")
        return selected

    def _resolve_reference_workbook(self, golden_workbook: str | None) -> Path | None:
        if golden_workbook:
            path = Path(golden_workbook).expanduser().resolve()
            if not path.exists():
                raise FileNotFoundError(f"Golden answers workbook not found: {path}")
            return path

        for candidate in (
            Path("artifacts/Golden_answers_updated.xlsx"),
            Path("artifacts/Golden_answers.xlsx"),
        ):
            if candidate.exists():
                return candidate.resolve()
        return None
