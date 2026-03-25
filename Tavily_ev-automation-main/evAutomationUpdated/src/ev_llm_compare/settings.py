from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args: object, **kwargs: object) -> bool:
        return False


@dataclass(slots=True)
class RetrievalSettings:
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    dense_top_k: int = 18
    final_top_k: int = 8
    batch_size: int = 64
    lexical_weight: float = 0.45
    dense_weight: float = 0.55
    rrf_k: int = 60
    note_chunk_size: int = 1200
    note_chunk_overlap: int = 150
    reranker_enabled: bool = True
    reranker_model: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    reranker_top_k: int = 12
    reranker_weight: float = 0.35
    max_chunks_per_company: int = 2
    structured_summary_limit: int = 8
    structured_exhaustive_limit: int = 150
    compact_context_enabled: bool = True
    generation_context_result_limit: int = 5
    generation_context_char_budget: int = 4200
    evaluation_context_result_limit: int = 4
    evaluation_context_char_budget: int = 2600


@dataclass(slots=True)
class RuntimeSettings:
    ollama_base_url: str = "http://localhost:11434"
    qdrant_path: Path = Path("artifacts/qdrant")
    output_dir: Path = Path("artifacts/results")


@dataclass(slots=True)
class ModelSpec:
    run_name: str
    provider: str
    model_name: str
    rag_enabled: bool
    enabled: bool = True
    temperature: float = 0.1
    max_tokens: int = 1600


@dataclass(slots=True)
class EvaluationSettings:
    judge_provider: str = "ollama"
    judge_model: str = "mistral-small3.2:24b"
    max_retries: int = 2


@dataclass(slots=True)
class AppConfig:
    retrieval: RetrievalSettings = field(default_factory=RetrievalSettings)
    runtime: RuntimeSettings = field(default_factory=RuntimeSettings)
    evaluation: EvaluationSettings = field(default_factory=EvaluationSettings)
    models: list[ModelSpec] = field(default_factory=list)


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_value(*names: str, default: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None:
            return value
    return default


def _build_models() -> list[ModelSpec]:
    return [
        ModelSpec(
            run_name="qwen_rag",
            provider="ollama",
            model_name=os.getenv("QWEN_MODEL", "qwen3:8b"),
            rag_enabled=True,
            enabled=_env_flag("ENABLE_QWEN_RAG", True),
        ),
        ModelSpec(
            run_name="qwen_no_rag",
            provider="ollama",
            model_name=os.getenv("QWEN_MODEL", "qwen3:8b"),
            rag_enabled=False,
            enabled=_env_flag("ENABLE_QWEN_NO_RAG", True),
        ),
        ModelSpec(
            run_name="gemma_rag",
            provider="ollama",
            model_name=os.getenv("GEMMA_MODEL", "gemma3:12b"),
            rag_enabled=True,
            enabled=_env_flag("ENABLE_GEMMA_RAG", True),
        ),
        ModelSpec(
            run_name="gemma_no_rag",
            provider="ollama",
            model_name=os.getenv("GEMMA_MODEL", "gemma3:12b"),
            rag_enabled=False,
            enabled=_env_flag("ENABLE_GEMMA_NO_RAG", True),
        ),
        ModelSpec(
            run_name="gemini_rag",
            provider="gemini",
            model_name=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            rag_enabled=True,
            enabled=_env_flag("ENABLE_GEMINI_RAG", True),
        ),
        ModelSpec(
            run_name="gemini_no_rag",
            provider="gemini",
            model_name=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            rag_enabled=False,
            enabled=_env_flag("ENABLE_GEMINI_NO_RAG", True),
        ),
    ]


def load_config(*, dotenv_enabled: bool = True) -> AppConfig:
    if dotenv_enabled:
        load_dotenv(override=False)

    config = AppConfig()
    config.models = [model for model in _build_models() if model.enabled]
    default_temperature = float(os.getenv("MODEL_TEMPERATURE", "0.1"))
    default_max_tokens = int(os.getenv("MODEL_MAX_TOKENS", "1600"))
    for model in config.models:
        model.temperature = default_temperature
        model.max_tokens = default_max_tokens
    config.retrieval.embedding_model = os.getenv(
        "EMBEDDING_MODEL",
        config.retrieval.embedding_model,
    )
    config.retrieval.reranker_enabled = _env_flag(
        "RERANKER_ENABLED",
        config.retrieval.reranker_enabled,
    )
    config.retrieval.reranker_model = os.getenv(
        "RERANKER_MODEL",
        config.retrieval.reranker_model,
    )
    config.retrieval.reranker_top_k = int(
        os.getenv("RERANKER_TOP_K", str(config.retrieval.reranker_top_k))
    )
    config.retrieval.reranker_weight = float(
        os.getenv("RERANKER_WEIGHT", str(config.retrieval.reranker_weight))
    )
    config.retrieval.max_chunks_per_company = int(
        os.getenv(
            "MAX_CHUNKS_PER_COMPANY",
            str(config.retrieval.max_chunks_per_company),
        )
    )
    config.retrieval.structured_summary_limit = int(
        os.getenv(
            "STRUCTURED_SUMMARY_LIMIT",
            str(config.retrieval.structured_summary_limit),
        )
    )
    config.retrieval.structured_exhaustive_limit = int(
        os.getenv(
            "STRUCTURED_EXHAUSTIVE_LIMIT",
            str(config.retrieval.structured_exhaustive_limit),
        )
    )
    config.retrieval.compact_context_enabled = _env_flag(
        "COMPACT_CONTEXT_ENABLED",
        config.retrieval.compact_context_enabled,
    )
    config.retrieval.generation_context_result_limit = int(
        os.getenv(
            "GENERATION_CONTEXT_RESULT_LIMIT",
            str(config.retrieval.generation_context_result_limit),
        )
    )
    config.retrieval.generation_context_char_budget = int(
        os.getenv(
            "GENERATION_CONTEXT_CHAR_BUDGET",
            str(config.retrieval.generation_context_char_budget),
        )
    )
    config.retrieval.evaluation_context_result_limit = int(
        _env_value(
            "EVALUATION_CONTEXT_RESULT_LIMIT",
            "RAGAS_CONTEXT_RESULT_LIMIT",
            default=str(config.retrieval.evaluation_context_result_limit),
        )
    )
    config.retrieval.evaluation_context_char_budget = int(
        _env_value(
            "EVALUATION_CONTEXT_CHAR_BUDGET",
            "RAGAS_CONTEXT_CHAR_BUDGET",
            default=str(config.retrieval.evaluation_context_char_budget),
        )
    )
    config.runtime.ollama_base_url = os.getenv(
        "OLLAMA_BASE_URL",
        config.runtime.ollama_base_url,
    )
    config.runtime.qdrant_path = Path(
        os.getenv("QDRANT_PATH", str(config.runtime.qdrant_path))
    )
    config.runtime.output_dir = Path(
        os.getenv("OUTPUT_DIR", str(config.runtime.output_dir))
    )
    config.evaluation.judge_provider = _env_value(
        "EVALUATION_JUDGE_PROVIDER",
        "RAGAS_JUDGE_PROVIDER",
        default=config.evaluation.judge_provider,
    )
    config.evaluation.judge_model = _env_value(
        "EVALUATION_JUDGE_MODEL",
        "RAGAS_JUDGE_MODEL",
        default=config.evaluation.judge_model,
    )
    config.evaluation.max_retries = int(
        _env_value(
            "EVALUATION_MAX_RETRIES",
            "RAGAS_MAX_RETRIES",
            default=str(config.evaluation.max_retries),
        )
    )
    return config
