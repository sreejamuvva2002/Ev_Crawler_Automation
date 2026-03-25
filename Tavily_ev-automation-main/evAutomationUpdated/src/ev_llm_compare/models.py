from __future__ import annotations

from dataclasses import dataclass
import os
import time
from typing import Any

from .prompts import SYSTEM_PROMPT
from .settings import ModelSpec, RuntimeSettings


@dataclass(slots=True)
class GenerationMetadata:
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cost_usd: float | None = None
    raw_response: Any | None = None


@dataclass(slots=True)
class GenerationResult:
    text: str
    metadata: GenerationMetadata


class LLMClient:
    provider: str
    model_name: str

    def generate_with_metadata(
        self,
        prompt: str,
        temperature: float,
        max_tokens: int,
        system_prompt: str | None = None,
        seed: int | None = None,
    ) -> GenerationResult:
        raise NotImplementedError

    def generate(
        self,
        prompt: str,
        temperature: float,
        max_tokens: int,
        system_prompt: str | None = None,
        seed: int | None = None,
    ) -> str:
        return self.generate_with_metadata(
            prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            system_prompt=system_prompt,
            seed=seed,
        ).text


class OllamaClient(LLMClient):
    def __init__(self, model_name: str, base_url: str):
        import ollama

        self.provider = "ollama"
        self.model_name = model_name
        self.base_url = base_url
        self.client = ollama.Client(host=base_url)

    def generate_with_metadata(
        self,
        prompt: str,
        temperature: float,
        max_tokens: int,
        system_prompt: str | None = None,
        seed: int | None = None,
    ) -> GenerationResult:
        effective_system_prompt = system_prompt or SYSTEM_PROMPT
        options: dict[str, Any] = {
            "temperature": temperature,
            "num_predict": max_tokens,
        }
        if seed is not None:
            options["seed"] = seed
        response = self.client.generate(
            model=self.model_name,
            prompt=prompt,
            system=effective_system_prompt,
            options=options,
        )
        prompt_tokens = response.get("prompt_eval_count")
        completion_tokens = response.get("eval_count")
        total_tokens = None
        if prompt_tokens is not None and completion_tokens is not None:
            total_tokens = int(prompt_tokens) + int(completion_tokens)
        return GenerationResult(
            text=response["response"].strip(),
            metadata=GenerationMetadata(
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
                raw_response=response,
            ),
        )


class GeminiClient(LLMClient):
    def __init__(self, model_name: str):
        from google import genai

        self.provider = "gemini"
        self.model_name = model_name
        api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise RuntimeError("Set GEMINI_API_KEY or GOOGLE_API_KEY before using Gemini.")
        self.client = genai.Client(api_key=api_key)

    def generate_with_metadata(
        self,
        prompt: str,
        temperature: float,
        max_tokens: int,
        system_prompt: str | None = None,
        seed: int | None = None,
    ) -> GenerationResult:
        from google.genai import types

        effective_system_prompt = system_prompt or SYSTEM_PROMPT
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=effective_system_prompt,
                temperature=temperature,
                max_output_tokens=max_tokens,
                seed=seed,
                thinking_config=types.ThinkingConfig(thinking_budget=0),
            ),
        )
        text = _extract_gemini_text(response)
        if text is None:
            raise RuntimeError("Gemini returned no text content.")
        usage = getattr(response, "usage_metadata", None)
        return GenerationResult(
            text=text,
            metadata=GenerationMetadata(
                prompt_tokens=getattr(usage, "prompt_token_count", None),
                completion_tokens=getattr(usage, "candidates_token_count", None),
                total_tokens=getattr(usage, "total_token_count", None),
                raw_response=response,
            ),
        )

def _extract_gemini_text(response: Any) -> str | None:
    if getattr(response, "text", None):
        return response.text.strip()

    parts: list[str] = []
    for candidate in getattr(response, "candidates", []) or []:
        content = getattr(candidate, "content", None)
        for part in getattr(content, "parts", []) or []:
            text = getattr(part, "text", None)
            if text:
                parts.append(text)
    if parts:
        return "\n".join(parts).strip()
    return None


def create_client(spec: ModelSpec, runtime: RuntimeSettings) -> LLMClient:
    if spec.provider == "ollama":
        return OllamaClient(spec.model_name, runtime.ollama_base_url)
    if spec.provider == "gemini":
        return GeminiClient(spec.model_name)
    raise ValueError(f"Unsupported provider: {spec.provider}")


def safe_generate(
    client: LLMClient,
    prompt: str,
    temperature: float,
    max_tokens: int,
    system_prompt: str | None = None,
    seed: int | None = None,
) -> tuple[str, float, bool, str | None]:
    answer, latency, success, error, _ = safe_generate_with_metadata(
        client,
        prompt,
        temperature=temperature,
        max_tokens=max_tokens,
        system_prompt=system_prompt,
        seed=seed,
    )
    return answer, latency, success, error


def safe_generate_with_metadata(
    client: LLMClient,
    prompt: str,
    temperature: float,
    max_tokens: int,
    system_prompt: str | None = None,
    seed: int | None = None,
) -> tuple[str, float, bool, str | None, GenerationMetadata]:
    start = time.perf_counter()
    try:
        result = client.generate_with_metadata(
            prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            system_prompt=system_prompt,
            seed=seed,
        )
        return result.text, round(time.perf_counter() - start, 2), True, None, result.metadata
    except Exception as exc:
        return (
            f"ERROR: {exc}",
            round(time.perf_counter() - start, 2),
            False,
            str(exc),
            GenerationMetadata(),
        )
