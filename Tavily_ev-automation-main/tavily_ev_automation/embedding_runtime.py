from __future__ import annotations

import hashlib
import math
import time
from dataclasses import dataclass
from typing import Any

import requests


def normalize_space(text: str) -> str:
    return " ".join((text or "").split()).strip()


def cosine_similarity(vector_a: list[float], vector_b: list[float]) -> float:
    if not vector_a or not vector_b or len(vector_a) != len(vector_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vector_a, vector_b))
    norm_a = math.sqrt(sum(a * a for a in vector_a))
    norm_b = math.sqrt(sum(b * b for b in vector_b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return max(0.0, min(1.0, dot / (norm_a * norm_b)))


@dataclass
class EmbeddingConfig:
    provider: str
    model: str
    base_url: str
    api_key: str = ""
    timeout_sec: int = 60
    batch_size: int = 12
    retries: int = 1
    retry_sleep_sec: float = 1.0
    max_text_chars: int = 8000


class EmbeddingRuntime:
    def __init__(self, cfg: EmbeddingConfig):
        self.cfg = cfg
        self.cache: dict[str, list[float]] = {}
        self.dimension: int = 0
        self.last_error: str = ""
        provider = (cfg.provider or "none").strip().lower()
        model = normalize_space(cfg.model)
        self.backend_name = f"{provider}:{model}" if provider != "none" and model else "hashing_bow"

    @property
    def enabled(self) -> bool:
        provider = (self.cfg.provider or "").strip().lower()
        return provider in {"ollama", "openai"} and bool(normalize_space(self.cfg.model))

    def _cache_key(self, text: str) -> str:
        normalized = normalize_space(text)[: self.cfg.max_text_chars]
        return hashlib.sha1(normalized.encode("utf-8")).hexdigest()

    def _normalize_texts(self, texts: list[str]) -> list[str]:
        normalized: list[str] = []
        for text in texts:
            cleaned = normalize_space(text)[: self.cfg.max_text_chars]
            normalized.append(cleaned)
        return normalized

    def warmup(self, texts: list[str]) -> bool:
        if not self.enabled:
            return False
        try:
            self.embed_texts(texts)
            return True
        except Exception as exc:
            self.last_error = f"embedding_warmup_failed:{exc}"
            return False

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        normalized = self._normalize_texts(texts)
        results: list[list[float]] = [[] for _ in normalized]
        if not self.enabled:
            return results

        missing_by_key: dict[str, str] = {}
        for idx, text in enumerate(normalized):
            if not text:
                continue
            key = self._cache_key(text)
            cached = self.cache.get(key)
            if cached:
                results[idx] = cached
            else:
                missing_by_key[key] = text

        if missing_by_key:
            missing_items = list(missing_by_key.items())
            batch_size = max(1, int(self.cfg.batch_size))
            for start in range(0, len(missing_items), batch_size):
                batch = missing_items[start:start + batch_size]
                batch_texts = [text for _, text in batch]
                vectors = self._embed_batch_with_retry(batch_texts)
                if len(vectors) != len(batch_texts):
                    raise RuntimeError(
                        f"Embedding backend returned {len(vectors)} vectors for {len(batch_texts)} texts"
                    )
                for (key, _), vector in zip(batch, vectors):
                    self.cache[key] = vector
                    if vector and not self.dimension:
                        self.dimension = len(vector)

        for idx, text in enumerate(normalized):
            if not text:
                continue
            results[idx] = self.cache.get(self._cache_key(text), [])
        return results

    def score_texts_to_references(
        self,
        texts: list[str],
        references: dict[str, str],
    ) -> list[dict[str, float]]:
        normalized_texts = self._normalize_texts(texts)
        normalized_refs = {
            key: normalize_space(value)[: self.cfg.max_text_chars]
            for key, value in references.items()
            if normalize_space(value)
        }
        if not normalized_refs:
            return [{} for _ in normalized_texts]
        vectors = self.embed_texts(normalized_texts + list(normalized_refs.values()))
        text_vectors = vectors[: len(normalized_texts)]
        ref_vectors = {
            key: vectors[len(normalized_texts) + idx]
            for idx, key in enumerate(normalized_refs.keys())
        }
        scored: list[dict[str, float]] = []
        for vector in text_vectors:
            row_scores: dict[str, float] = {}
            for key, ref_vector in ref_vectors.items():
                row_scores[key] = round(100.0 * cosine_similarity(vector, ref_vector), 2)
            scored.append(row_scores)
        return scored

    def _embed_batch_with_retry(self, texts: list[str]) -> list[list[float]]:
        last_error = "embedding_backend_failed"
        for attempt in range(1, self.cfg.retries + 2):
            try:
                return self._embed_batch(texts)
            except Exception as exc:
                last_error = f"embedding_call_failed:{exc}"
                if attempt < self.cfg.retries + 1:
                    time.sleep(self.cfg.retry_sleep_sec)
        self.last_error = last_error
        raise RuntimeError(last_error)

    def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        provider = (self.cfg.provider or "").strip().lower()
        if provider == "ollama":
            return self._embed_batch_ollama(texts)
        if provider == "openai":
            return self._embed_batch_openai(texts)
        raise RuntimeError(f"unsupported_embedding_provider:{provider}")

    def _embed_batch_ollama(self, texts: list[str]) -> list[list[float]]:
        base_url = self.cfg.base_url.rstrip("/")
        payload = {"model": self.cfg.model, "input": texts}
        try:
            response = requests.post(
                base_url + "/api/embed",
                json=payload,
                timeout=self.cfg.timeout_sec,
            )
            response.raise_for_status()
            data = response.json()
            embeddings = data.get("embeddings") or []
            if isinstance(embeddings, list) and len(embeddings) == len(texts):
                return [self._coerce_vector(vector) for vector in embeddings]
        except Exception as exc:
            self.last_error = f"ollama_native_embed_failed:{exc}"

        try:
            compat_response = requests.post(
                base_url + "/v1/embeddings",
                json={"model": self.cfg.model, "input": texts},
                timeout=self.cfg.timeout_sec,
            )
            compat_response.raise_for_status()
            data = compat_response.json()
            rows = data.get("data") or []
            if isinstance(rows, list) and len(rows) == len(texts):
                return [self._coerce_vector((row or {}).get("embedding")) for row in rows]
        except Exception as exc:
            self.last_error = f"ollama_openai_compat_embed_failed:{exc}"

        legacy_vectors: list[list[float]] = []
        for text in texts:
            response = requests.post(
                base_url + "/api/embeddings",
                json={"model": self.cfg.model, "prompt": text},
                timeout=self.cfg.timeout_sec,
            )
            response.raise_for_status()
            data = response.json()
            legacy_vectors.append(self._coerce_vector(data.get("embedding")))
        return legacy_vectors

    def _embed_batch_openai(self, texts: list[str]) -> list[list[float]]:
        if not self.cfg.api_key:
            raise RuntimeError("missing_openai_embedding_api_key")
        headers = {
            "Authorization": f"Bearer {self.cfg.api_key}",
            "Content-Type": "application/json",
        }
        response = requests.post(
            self.cfg.base_url.rstrip("/") + "/v1/embeddings",
            headers=headers,
            json={"model": self.cfg.model, "input": texts, "encoding_format": "float"},
            timeout=self.cfg.timeout_sec,
        )
        response.raise_for_status()
        data = response.json()
        rows = data.get("data") or []
        if not isinstance(rows, list) or len(rows) != len(texts):
            raise RuntimeError("invalid_openai_embedding_payload")
        return [self._coerce_vector((row or {}).get("embedding")) for row in rows]

    @staticmethod
    def _coerce_vector(value: Any) -> list[float]:
        if not isinstance(value, list):
            return []
        vector: list[float] = []
        for item in value:
            try:
                vector.append(float(item))
            except Exception:
                vector.append(0.0)
        return vector
