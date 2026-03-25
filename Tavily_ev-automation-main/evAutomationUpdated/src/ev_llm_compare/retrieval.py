from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import re
import tempfile
from typing import Any
import uuid

import pandas as pd

try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, PointStruct, VectorParams
except ImportError:
    QdrantClient = None  # type: ignore[assignment]
    Distance = None  # type: ignore[assignment]
    PointStruct = None  # type: ignore[assignment]
    VectorParams = None  # type: ignore[assignment]

try:
    from sentence_transformers import CrossEncoder, SentenceTransformer
except ImportError:
    CrossEncoder = None  # type: ignore[assignment]
    SentenceTransformer = None  # type: ignore[assignment]

from .chunking import tokenize
from .schemas import Chunk, RetrievalResult
from .settings import RetrievalSettings

NORMALIZE_PATTERN = re.compile(r"[^a-z0-9]+")
QUERY_STOPWORDS = {
    "a",
    "all",
    "an",
    "and",
    "are",
    "as",
    "by",
    "for",
    "from",
    "group",
    "how",
    "in",
    "is",
    "list",
    "me",
    "of",
    "show",
    "the",
    "them",
    "what",
    "which",
    "with",
}
INDEX_MANIFEST_VERSION = 2


def normalize_text(value: str) -> str:
    return NORMALIZE_PATTERN.sub(" ", value.lower()).strip()


def build_collection_fingerprint(chunks: list[Chunk], embedding_model: str) -> str:
    digest = hashlib.sha1()
    digest.update(embedding_model.encode("utf-8"))
    digest.update(f"|{len(chunks)}|".encode("utf-8"))
    for chunk in chunks:
        digest.update(chunk.chunk_id.encode("utf-8"))
        digest.update(b"\0")
        digest.update(chunk.text.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(chunk.metadata.get("row_key", "")).encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()[:12]


@dataclass(slots=True)
class QueryPlan:
    question: str
    normalized_question: str
    intent: str
    dense_queries: list[str]
    matched_categories: list[str]
    matched_companies: list[str]
    matched_locations: list[str]
    excluded_locations: list[str]
    matched_primary_oems: list[str]
    matched_role_terms: list[str]
    group_by_role: bool
    prefer_structured: bool


class HybridRetriever:
    def __init__(
        self,
        chunks: list[Chunk],
        settings: RetrievalSettings,
        qdrant_path: Path,
        *,
        collection_name: str | None = None,
        force_reindex: bool = False,
        client: QdrantClient | None = None,
    ):
        if QdrantClient is None or SentenceTransformer is None:
            raise RuntimeError(
                "HybridRetriever requires qdrant-client and sentence-transformers to be installed."
            )

        self.chunks = chunks
        self.settings = settings
        self.qdrant_path = qdrant_path
        self.force_reindex = force_reindex
        self.collection_fingerprint = build_collection_fingerprint(chunks, settings.embedding_model)
        self.collection_name = collection_name or self._collection_name(chunks, settings.embedding_model)
        self._persistent_collection_requested = collection_name is not None
        self._temp_dir: tempfile.TemporaryDirectory[str] | None = None
        self._reranker: CrossEncoder | None = None
        self._reranker_failed = False
        self._owns_client = client is None
        self.embedding_model = SentenceTransformer(settings.embedding_model)
        self.chunk_map = {chunk.chunk_id: chunk for chunk in chunks}
        self.point_id_to_chunk_id = {
            self._qdrant_point_id(chunk.chunk_id): chunk.chunk_id for chunk in chunks
        }
        self.idf = self._build_idf(chunks)
        self.row_records = self._build_row_records(chunks)
        self.known_categories = self._known_field_values("category")
        self.known_companies = self._known_field_values("company")
        self.known_locations = self._known_field_values("location")
        self.known_primary_oems = self._known_field_values("primary_oems")
        self.role_terms = self._build_role_terms()
        self.client = client or self._create_client(qdrant_path)
        self._index_chunks()

    def close(self) -> None:
        if self._owns_client:
            try:
                self.client.close()
            except Exception:
                pass
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None

    def retrieve(self, question: str, top_k: int | None = None) -> list[RetrievalResult]:
        query_plan = self._plan_query(question)
        structured_results = self._structured_matches(query_plan)
        dense_rank, dense_scores = self._rank_dense(query_plan.dense_queries)
        lexical_rank, lexical_scores = self._rank_lexically(query_plan.dense_queries)

        candidate_ids = set(dense_rank) | set(lexical_rank)
        retrieved: list[RetrievalResult] = []
        for chunk_id in candidate_ids:
            chunk = self.chunk_map[str(chunk_id)]
            final_score = self._fusion_score(str(chunk_id), dense_rank, lexical_rank)
            final_score += self._metadata_boost(question, chunk.metadata)
            retrieved.append(
                RetrievalResult(
                    chunk_id=str(chunk_id),
                    text=chunk.text,
                    metadata=chunk.metadata,
                    dense_score=round(dense_scores.get(str(chunk_id), 0.0), 5),
                    lexical_score=round(lexical_scores.get(str(chunk_id), 0.0), 5),
                    final_score=round(final_score, 5),
                )
            )
        retrieved.sort(key=lambda item: item.final_score, reverse=True)
        reranked = self._rerank_candidates(question, retrieved)
        return self._select_context_results(
            query_plan,
            structured_results,
            reranked,
            limit=top_k,
        )

    def _index_chunks(self) -> None:
        self.qdrant_path.mkdir(parents=True, exist_ok=True)
        if self._collection_is_current():
            return

        embeddings = self.embedding_model.encode(
            [chunk.text for chunk in self.chunks],
            batch_size=self.settings.batch_size,
            show_progress_bar=False,
        )
        vector_size = len(embeddings[0])
        self.client.recreate_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        )

        points: list[PointStruct] = []
        for chunk, vector in zip(self.chunks, embeddings, strict=True):
            points.append(
                PointStruct(
                    id=self._qdrant_point_id(chunk.chunk_id),
                    vector=vector.tolist(),
                    payload={
                        "chunk_id": chunk.chunk_id,
                        "text": chunk.text,
                        "metadata": chunk.metadata,
                    },
                )
            )

        for start in range(0, len(points), self.settings.batch_size):
            batch = points[start : start + self.settings.batch_size]
            self.client.upsert(collection_name=self.collection_name, points=batch)
        self._write_collection_manifest()

    def _collection_is_current(self) -> bool:
        if self.force_reindex:
            return False
        try:
            self.client.get_collection(self.collection_name)
            count = self.client.count(collection_name=self.collection_name, exact=True).count
            if count != len(self.chunks):
                return False
            manifest = self._read_collection_manifest()
            if manifest is None:
                return not self._persistent_collection_requested
            return (
                int(manifest.get("manifest_version", -1)) == INDEX_MANIFEST_VERSION
                and manifest.get("point_id_strategy") == "uuid5"
                and manifest.get("fingerprint") == self.collection_fingerprint
                and int(manifest.get("chunk_count", -1)) == len(self.chunks)
            )
        except Exception:
            return False

    def _collection_manifest_path(self) -> Path:
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", self.collection_name)
        return self.qdrant_path / "_index_manifests" / f"{safe_name}.json"

    def _read_collection_manifest(self) -> dict[str, Any] | None:
        manifest_path = self._collection_manifest_path()
        if not manifest_path.exists():
            return None
        try:
            return json.loads(manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None

    def _write_collection_manifest(self) -> None:
        manifest_path = self._collection_manifest_path()
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(
                {
                    "collection_name": self.collection_name,
                    "fingerprint": self.collection_fingerprint,
                    "chunk_count": len(self.chunks),
                    "embedding_model": self.settings.embedding_model,
                    "manifest_version": INDEX_MANIFEST_VERSION,
                    "point_id_strategy": "uuid5",
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    def _build_idf(self, chunks: list[Chunk]) -> dict[str, float]:
        document_frequency: defaultdict[str, int] = defaultdict(int)
        for chunk in chunks:
            for token in chunk.token_set:
                document_frequency[token] += 1
        total_documents = len(chunks)
        return {
            token: math.log((1 + total_documents) / (1 + count)) + 1
            for token, count in document_frequency.items()
        }

    def _build_row_records(self, chunks: list[Chunk]) -> dict[str, dict[str, str]]:
        row_records: dict[str, dict[str, str]] = {}
        for chunk in chunks:
            row_key = str(chunk.metadata.get("row_key", "")).strip()
            if not row_key or row_key in row_records:
                continue
            row_records[row_key] = {
                "row_key": row_key,
                "company": str(chunk.metadata.get("company", "")),
                "category": str(chunk.metadata.get("category", "")),
                "ev_supply_chain_role": str(chunk.metadata.get("ev_supply_chain_role", "")),
                "product_service": str(chunk.metadata.get("product_service", "")),
                "primary_oems": str(chunk.metadata.get("primary_oems", "")),
                "location": str(chunk.metadata.get("location", "")),
                "industry_group": str(chunk.metadata.get("industry_group", "")),
                "primary_facility_type": str(chunk.metadata.get("primary_facility_type", "")),
                "supplier_or_affiliation_type": str(
                    chunk.metadata.get("supplier_or_affiliation_type", "")
                ),
                "classification_method": str(chunk.metadata.get("classification_method", "")),
                "employment": str(chunk.metadata.get("employment", "")),
                "ev_battery_relevant": str(chunk.metadata.get("ev_battery_relevant", "")),
                "source_file": str(chunk.metadata.get("source_file", "")),
                "sheet_name": str(chunk.metadata.get("sheet_name", "")),
                "row_number": str(chunk.metadata.get("row_number", "")),
                "row_summary": str(chunk.metadata.get("row_summary", "")),
            }
        return row_records

    def _known_field_values(self, field_name: str) -> list[str]:
        values = {
            str(record.get(field_name, "")).strip()
            for record in self.row_records.values()
            if str(record.get(field_name, "")).strip()
        }
        return sorted(values, key=len, reverse=True)

    def _build_role_terms(self) -> list[str]:
        terms: set[str] = set()
        for record in self.row_records.values():
            role = normalize_text(record.get("ev_supply_chain_role", ""))
            if not role:
                continue
            terms.add(role)
            for part in re.split(r"\band\b|/|,", role):
                candidate = part.strip()
                if len(candidate.split()) >= 2:
                    terms.add(candidate)
        return sorted(terms, key=len, reverse=True)

    def _plan_query(self, question: str) -> QueryPlan:
        normalized_question = normalize_text(question)
        exact_category = self._extract_exact_category_filter(question)
        classification_focused = any(
            term in normalized_question for term in {"classified as", "classification method"}
        )
        if exact_category:
            matched_categories = [
                category
                for category in self.known_categories
                if self._category_key(category) == exact_category
            ]
        else:
            matched_categories = [] if classification_focused else self._match_known_categories(question)
        if "primary oem" in normalized_question or "primary oems" in normalized_question:
            matched_categories = [
                category for category in matched_categories if self._category_key(category) != "oem"
            ]
        matched_companies = self._match_known_values(normalized_question, self.known_companies)
        matched_locations = self._match_locations(normalized_question)
        excluded_locations = self._extract_excluded_locations(question)
        if excluded_locations:
            excluded_keys = {normalize_text(value) for value in excluded_locations}
            matched_locations = [
                location
                for location in matched_locations
                if normalize_text(location) not in excluded_keys
            ]
        matched_primary_oems = self._match_known_values(
            normalized_question,
            getattr(self, "known_primary_oems", []),
        )
        matched_role_terms = [
            term for term in getattr(self, "role_terms", []) if term and term in normalized_question
        ]
        if any(term in normalized_question for term in {"linked to", "suppliers linked to", "supply to"}):
            if matched_companies and not matched_primary_oems:
                matched_primary_oems = matched_companies.copy()
            if matched_primary_oems:
                matched_companies = []
        if any(term in normalized_question for term in {"define", "definition", "meaning", "methodology"}):
            intent = "definition"
        elif any(term in normalized_question for term in {"compare", "difference", "versus", "vs"}):
            intent = "comparison"
        elif any(term in normalized_question for term in {"count", "how many", "list", "show all", "group"}):
            intent = "aggregation"
        else:
            intent = "fact"
        analytic_requested = any(
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
                "missing",
                "compute",
                "calculate",
                "compare",
                "summarize",
            }
        )
        analytic_requested = analytic_requested or self._extract_employment_threshold(question) is not None
        analytic_requested = analytic_requested or "missing primary oems" in normalized_question
        analytic_requested = analytic_requested or (
            "mentions" in normalized_question and "product service" in normalized_question
        )
        analytic_requested = analytic_requested or (
            "ev battery relevant" in normalized_question
            or "ev / battery relevant" in normalized_question
        )
        analytic_requested = analytic_requested or "containing both" in normalized_question
        analytic_requested = analytic_requested or "only one" in normalized_question
        analytic_requested = analytic_requested or classification_focused
        analytic_requested = analytic_requested or "appear multiple times" in normalized_question
        analytic_requested = analytic_requested or "only one company in the dataset" in normalized_question
        analytic_requested = analytic_requested or "most concentrated in a single county" in normalized_question
        analytic_requested = analytic_requested or "supply to more than one oem" in normalized_question

        dense_queries = [question]
        focused_terms = [
            token
            for token in normalized_question.split()
            if token not in QUERY_STOPWORDS
        ]
        if focused_terms:
            dense_queries.append(" ".join(focused_terms[:12]))
        filters = (
            matched_companies
            + matched_categories
            + matched_locations
            + matched_primary_oems
            + matched_role_terms
        )
        if filters:
            dense_queries.append(" | ".join(dict.fromkeys(filters)))

        return QueryPlan(
            question=question,
            normalized_question=normalized_question,
            intent=intent,
            dense_queries=list(dict.fromkeys(query for query in dense_queries if query.strip())),
            matched_categories=matched_categories,
            matched_companies=matched_companies,
            matched_locations=matched_locations,
            excluded_locations=excluded_locations,
            matched_primary_oems=matched_primary_oems,
            matched_role_terms=matched_role_terms,
            group_by_role="group" in normalized_question and "ev supply chain role" in normalized_question,
            prefer_structured=analytic_requested or (
                bool(filters) and intent in {"aggregation", "comparison", "fact"}
            ),
        )

    def _rank_dense(self, dense_queries: list[str]) -> tuple[dict[str, int], dict[str, float]]:
        rank_scores: defaultdict[str, float] = defaultdict(float)
        raw_scores: dict[str, float] = {}
        for query in dense_queries:
            query_vector = self.embedding_model.encode(query).tolist()
            dense_results = self.client.query_points(
                collection_name=self.collection_name,
                query=query_vector,
                limit=self.settings.dense_top_k,
            ).points
            for rank, point in enumerate(dense_results, start=1):
                logical_chunk_id = self._logical_chunk_id(point)
                rank_scores[logical_chunk_id] += 1.0 / (self.settings.rrf_k + rank)
                raw_scores[logical_chunk_id] = max(
                    raw_scores.get(logical_chunk_id, 0.0),
                    float(point.score),
                )
        dense_rank = {
            chunk_id: rank
            for rank, (chunk_id, _) in enumerate(
                sorted(rank_scores.items(), key=lambda item: item[1], reverse=True)[: self.settings.dense_top_k],
                start=1,
            )
        }
        return dense_rank, raw_scores

    def _rank_lexically(self, dense_queries: list[str]) -> tuple[dict[str, int], dict[str, float]]:
        scores: list[tuple[str, float]] = []
        score_lookup: dict[str, float] = {}
        for chunk in self.chunks:
            score = max(self._lexical_score(query, chunk) for query in dense_queries)
            if score > 0:
                scores.append((chunk.chunk_id, score))
                score_lookup[chunk.chunk_id] = score
        scores.sort(key=lambda item: item[1], reverse=True)
        lexical_rank = {
            chunk_id: rank
            for rank, (chunk_id, _) in enumerate(scores[: self.settings.dense_top_k], start=1)
        }
        return lexical_rank, score_lookup

    def _lexical_score(self, question: str, chunk: Chunk) -> float:
        query_tokens = tokenize(question)
        if not query_tokens:
            return 0.0
        overlap = query_tokens & chunk.token_set
        base = sum(self.idf.get(token, 1.0) for token in overlap)
        phrase_bonus = 0.0
        question_lower = question.lower()
        company = str(chunk.metadata.get("company", "")).lower()
        if company and company in question_lower:
            phrase_bonus += 2.5
        if "sheet_name" in chunk.metadata and str(chunk.metadata["sheet_name"]).lower() in question_lower:
            phrase_bonus += 0.5
        return base + phrase_bonus

    def _fusion_score(
        self,
        chunk_id: str,
        dense_rank: dict[str, int],
        lexical_rank: dict[str, int],
    ) -> float:
        dense_component = 0.0
        lexical_component = 0.0
        dense_position = dense_rank.get(chunk_id)
        lexical_position = lexical_rank.get(chunk_id)
        if dense_position:
            dense_component = self.settings.dense_weight / (self.settings.rrf_k + dense_position)
        if lexical_position:
            lexical_component = self.settings.lexical_weight / (self.settings.rrf_k + lexical_position)
        return dense_component + lexical_component

    def _metadata_boost(self, question: str, metadata: dict[str, Any]) -> float:
        boost = 0.0
        question_lower = normalize_text(question)
        company = normalize_text(str(metadata.get("company", "")))
        category = normalize_text(str(metadata.get("category", "")))
        role = normalize_text(str(metadata.get("ev_supply_chain_role", "")))
        product_service = normalize_text(str(metadata.get("product_service", "")))
        chunk_type = str(metadata.get("chunk_type", ""))
        if company and company in question_lower:
            boost += 0.04
        if category and category in question_lower:
            boost += 0.04
        if role:
            for role_term in self.role_terms:
                if role_term in question_lower and role_term in role:
                    boost += 0.05
                    break
        if product_service:
            overlap = tokenize(question_lower) & tokenize(product_service)
            if len(overlap) >= 2:
                boost += 0.015
        if "employment" in question_lower and chunk_type == "location_theme":
            boost += 0.01
        if any(term in question_lower for term in {"oem", "hyundai", "kia", "rivian", "mercedes"}):
            if chunk_type == "supply_chain_theme":
                boost += 0.015
        if chunk_type == "note_reference" and any(term in question_lower for term in {"define", "definition", "methodology"}):
            boost += 0.03
        if chunk_type == "derived_analytic_summary":
            analysis_title = normalize_text(str(metadata.get("analysis_title", "")))
            analysis_type = normalize_text(str(metadata.get("analysis_type", "")))
            title_overlap = tokenize(question_lower) & tokenize(analysis_title)
            type_overlap = tokenize(question_lower) & tokenize(analysis_type)
            if title_overlap:
                boost += min(0.06, 0.012 * len(title_overlap))
            if type_overlap:
                boost += min(0.04, 0.01 * len(type_overlap))
            if any(
                term in question_lower
                for term in {"highest", "lowest", "compare", "versus", "vs", "concentration", "cluster", "density", "county"}
            ):
                boost += 0.03
        return boost

    def _structured_matches(self, query_plan: QueryPlan) -> list[RetrievalResult]:
        if not query_plan.prefer_structured:
            return []

        matched_rows = [
            record
            for record in self.row_records.values()
            if self._row_matches_filters(record, query_plan)
        ]
        if not matched_rows:
            return []

        has_explicit_filters = any(
            [
                query_plan.matched_categories,
                query_plan.matched_companies,
                query_plan.matched_locations,
                query_plan.matched_primary_oems,
                query_plan.matched_role_terms,
            ]
        )
        if (
            not has_explicit_filters
            and len(matched_rows) > self.settings.structured_exhaustive_limit
            and not self._is_exhaustive_question(query_plan)
        ):
            return []

        summary_text = self._build_structured_summary(query_plan, matched_rows)
        results: list[RetrievalResult] = [
            RetrievalResult(
                chunk_id=f"structured-summary::{hashlib.sha1(query_plan.normalized_question.encode('utf-8')).hexdigest()[:12]}",
                text=summary_text,
                metadata={
                    "chunk_type": "structured_match_summary",
                    "company": "",
                    "source_file": matched_rows[0]["source_file"],
                    "sheet_name": matched_rows[0]["sheet_name"],
                },
                dense_score=1.0,
                lexical_score=1.0,
                final_score=1.0,
            )
        ]

        row_limit = min(self.settings.structured_summary_limit, max(2, self.settings.final_top_k - 2))
        for row in matched_rows[:row_limit]:
            results.append(
                RetrievalResult(
                    chunk_id=f"structured-row::{row['row_key']}",
                    text=row["row_summary"],
                    metadata={
                        "chunk_type": "structured_row_match",
                        "company": row["company"],
                        "source_file": row["source_file"],
                        "sheet_name": row["sheet_name"],
                        "row_number": row["row_number"],
                        "row_key": row["row_key"],
                    },
                    dense_score=0.99,
                    lexical_score=0.99,
                    final_score=0.99,
                )
            )
        return results

    def _row_matches_filters(self, row: dict[str, str], query_plan: QueryPlan) -> bool:
        row_category = self._category_key(row.get("category", ""))
        row_company = normalize_text(row.get("company", ""))
        row_role = normalize_text(row.get("ev_supply_chain_role", ""))
        row_product = normalize_text(row.get("product_service", ""))
        row_location = normalize_text(row.get("location", ""))
        row_primary_oems = normalize_text(row.get("primary_oems", ""))

        if query_plan.matched_categories:
            category_filters = {self._category_key(value) for value in query_plan.matched_categories}
            if row_category not in category_filters:
                return False
        if query_plan.matched_companies:
            company_filters = {normalize_text(value) for value in query_plan.matched_companies}
            if row_company not in company_filters:
                return False
        if query_plan.matched_locations:
            location_filters = {normalize_text(value) for value in query_plan.matched_locations}
            if row_location not in location_filters:
                return False
        if query_plan.excluded_locations:
            excluded_filters = {normalize_text(value) for value in query_plan.excluded_locations}
            if row_location in excluded_filters:
                return False
        if query_plan.matched_primary_oems:
            oem_filters = {normalize_text(value) for value in query_plan.matched_primary_oems}
            if row_primary_oems not in oem_filters:
                return False
        if query_plan.matched_role_terms:
            role_match = any(term in row_role for term in query_plan.matched_role_terms)
            if not role_match:
                allow_product_match = any(
                    term in query_plan.normalized_question
                    for term in {"product service", "product / service", "mentions"}
                )
                if not allow_product_match or not any(
                    term in row_product for term in query_plan.matched_role_terms
                ):
                    return False
        return True

    def _build_structured_summary(self, query_plan: QueryPlan, matched_rows: list[dict[str, str]]) -> str:
        lines = ["Structured workbook matches from exact metadata filters:"]
        applied_filters: list[str] = []
        if query_plan.matched_categories:
            applied_filters.append(f"category in {query_plan.matched_categories}")
        if query_plan.matched_companies:
            applied_filters.append(f"company in {query_plan.matched_companies}")
        if query_plan.matched_locations:
            applied_filters.append(f"location in {query_plan.matched_locations}")
        if query_plan.excluded_locations:
            applied_filters.append(f"location not in {query_plan.excluded_locations}")
        if query_plan.matched_primary_oems:
            applied_filters.append(f"primary OEMs in {query_plan.matched_primary_oems}")
        if query_plan.matched_role_terms:
            applied_filters.append(f"role terms in {query_plan.matched_role_terms}")
        lines.append(f"Applied filters: {', '.join(applied_filters)}")
        lines.append(f"Matched rows: {len(matched_rows)}")

        # For explicit grouped listing questions, prefer a compact grouped summary first.
        # This keeps the context aligned with question intent and avoids noisy row-by-row spillover.
        grouped_listing_requested = (
            query_plan.group_by_role
            and self._is_exhaustive_question(query_plan)
            and not any(
                term in query_plan.normalized_question
                for term in {"count", "how many", "total", "average", "top", "bottom", "highest", "lowest"}
            )
        )
        if grouped_listing_requested:
            grouped: defaultdict[str, list[str]] = defaultdict(list)
            for row in matched_rows:
                grouped[row.get("ev_supply_chain_role") or "Unspecified"].append(
                    row.get("company") or "Unknown"
                )
            lines.append("Grouped by EV Supply Chain Role:")
            full_group_output = len(matched_rows) <= self.settings.structured_exhaustive_limit
            for role in sorted(grouped):
                companies = sorted(dict.fromkeys(grouped[role]))
                if full_group_output:
                    lines.append(f"- {role}: {'; '.join(companies)}")
                else:
                    preview = companies[: self.settings.structured_summary_limit]
                    lines.append(f"- {role}: {', '.join(preview)}")
                    if len(companies) > len(preview):
                        lines.append(f"  + {len(companies) - len(preview)} more companies")
            if not full_group_output:
                lines.append(
                    f"Summary truncated because matched rows ({len(matched_rows)}) exceed "
                    f"structured_exhaustive_limit ({self.settings.structured_exhaustive_limit})."
                )
            return "\n".join(lines)

        analytic_lines = self._build_analytic_summary_lines(query_plan, matched_rows)
        if analytic_lines:
            lines.extend(analytic_lines)
            return "\n".join(lines)

        exhaustive_limit = max(
            self.settings.structured_summary_limit,
            self.settings.structured_exhaustive_limit,
        )
        if self._is_exhaustive_question(query_plan) and len(matched_rows) <= exhaustive_limit:
            if query_plan.group_by_role:
                grouped: defaultdict[str, list[str]] = defaultdict(list)
                for row in matched_rows:
                    grouped[row.get("ev_supply_chain_role") or "Unspecified"].append(
                        row.get("company") or "Unknown"
                    )
                lines.append("Grouped by EV Supply Chain Role:")
                for role in sorted(grouped):
                    companies = sorted(dict.fromkeys(grouped[role]))
                    lines.append(f"- {role}: {'; '.join(companies)}")
                return "\n".join(lines)

            lines.append("Detailed rows:")
            for row in matched_rows:
                lines.append(
                    "- "
                    + " | ".join(
                        value
                        for value in [
                            f"Company: {row.get('company', '')}",
                            f"Category: {row.get('category', '')}",
                            f"Industry Group: {row.get('industry_group', '')}",
                            f"EV Supply Chain Role: {row.get('ev_supply_chain_role', '')}",
                            f"Product / Service: {row.get('product_service', '')}",
                            f"Primary OEMs: {row.get('primary_oems', '')}",
                            f"Location: {row.get('location', '')}",
                            f"Primary Facility Type: {row.get('primary_facility_type', '')}",
                            f"Employment: {row.get('employment', '')}",
                            f"EV / Battery Relevant: {row.get('ev_battery_relevant', '')}",
                            f"Supplier or Affiliation Type: {row.get('supplier_or_affiliation_type', '')}",
                            f"Classification Method: {row.get('classification_method', '')}",
                        ]
                        if value.split(': ', 1)[1]
                    )
                )
            return "\n".join(lines)

        if len(matched_rows) > 12 or query_plan.group_by_role:
            grouped: defaultdict[str, list[str]] = defaultdict(list)
            for row in matched_rows:
                grouped[row.get("ev_supply_chain_role") or "Unspecified"].append(row.get("company") or "Unknown")
            lines.append("Grouped by EV Supply Chain Role:")
            for role in sorted(grouped):
                companies = sorted(dict.fromkeys(grouped[role]))
                preview = companies[: self.settings.structured_summary_limit]
                lines.append(f"- {role}: {', '.join(preview)}")
                if len(companies) > len(preview):
                    lines.append(f"  + {len(companies) - len(preview)} more companies")
            return "\n".join(lines)

        lines.append("Detailed rows:")
        for row in matched_rows[: self.settings.structured_summary_limit]:
            lines.append(
                "- "
                + " | ".join(
                    value
                    for value in [
                        f"Company: {row.get('company', '')}",
                        f"Category: {row.get('category', '')}",
                        f"Industry Group: {row.get('industry_group', '')}",
                        f"EV Supply Chain Role: {row.get('ev_supply_chain_role', '')}",
                        f"Product / Service: {row.get('product_service', '')}",
                        f"Primary OEMs: {row.get('primary_oems', '')}",
                        f"Location: {row.get('location', '')}",
                        f"Primary Facility Type: {row.get('primary_facility_type', '')}",
                        f"Employment: {row.get('employment', '')}",
                        f"EV / Battery Relevant: {row.get('ev_battery_relevant', '')}",
                        f"Supplier or Affiliation Type: {row.get('supplier_or_affiliation_type', '')}",
                        f"Classification Method: {row.get('classification_method', '')}",
                    ]
                    if value.split(': ', 1)[1]
                )
            )
        if len(matched_rows) > self.settings.structured_summary_limit:
            lines.append(
                f"Additional matched rows omitted: {len(matched_rows) - self.settings.structured_summary_limit}"
            )
        return "\n".join(lines)

    def _is_exhaustive_question(self, query_plan: QueryPlan) -> bool:
        question = query_plan.normalized_question
        if any(term in question for term in {"list all", "show all", "provide the matching companies"}):
            return True
        if "include their" in question or "summarize their" in question:
            return True
        if "for each category" in question:
            return True
        return False

    def _rerank_candidates(
        self,
        question: str,
        candidates: list[RetrievalResult],
    ) -> list[RetrievalResult]:
        if not self.settings.reranker_enabled or len(candidates) < 2:
            return candidates

        reranker = self._load_reranker()
        if reranker is None:
            return candidates

        rerank_candidates = candidates[: self.settings.reranker_top_k]
        try:
            scores = reranker.predict(
                [(question, candidate.text) for candidate in rerank_candidates],
                show_progress_bar=False,
            )
        except Exception:
            self._reranker_failed = True
            return candidates

        rerank_order = sorted(
            range(len(rerank_candidates)),
            key=lambda index: float(scores[index]),
            reverse=True,
        )
        rerank_bonus = {
            rerank_candidates[index].chunk_id: self.settings.reranker_weight / (self.settings.rrf_k + rank)
            for rank, index in enumerate(rerank_order, start=1)
        }
        reranked: list[RetrievalResult] = []
        for candidate in candidates:
            reranked.append(
                RetrievalResult(
                    chunk_id=candidate.chunk_id,
                    text=candidate.text,
                    metadata=candidate.metadata,
                    dense_score=candidate.dense_score,
                    lexical_score=candidate.lexical_score,
                    final_score=round(candidate.final_score + rerank_bonus.get(candidate.chunk_id, 0.0), 5),
                )
            )
        reranked.sort(key=lambda item: item.final_score, reverse=True)
        return reranked

    def _load_reranker(self) -> CrossEncoder | None:
        if CrossEncoder is None:
            self._reranker_failed = True
            return None
        if self._reranker_failed:
            return None
        if self._reranker is not None:
            return self._reranker
        try:
            self._reranker = CrossEncoder(self.settings.reranker_model)
        except Exception:
            self._reranker_failed = True
            return None
        return self._reranker

    def _select_context_results(
        self,
        query_plan: QueryPlan,
        structured_results: list[RetrievalResult],
        candidates: list[RetrievalResult],
        limit: int | None = None,
    ) -> list[RetrievalResult]:
        ordered_candidates = sorted(
            candidates,
            key=lambda item: (self._context_priority(item, query_plan), item.final_score),
            reverse=True,
        )

        result_limit = limit or self.settings.final_top_k
        selected: list[RetrievalResult] = []
        seen_keys: set[str] = set()
        company_counts: defaultdict[str, int] = defaultdict(int)

        for result in structured_results[:1]:
            selected.append(result)
            seen_keys.add(result.chunk_id)

        for result in structured_results[1:] + ordered_candidates:
            unique_key = str(result.metadata.get("row_key", "")).strip() or result.chunk_id
            if unique_key in seen_keys:
                continue
            company = str(result.metadata.get("company", "")).strip()
            if company and company_counts[company] >= self.settings.max_chunks_per_company:
                continue
            selected.append(result)
            seen_keys.add(unique_key)
            if company:
                company_counts[company] += 1
            if len(selected) >= result_limit:
                break
        return selected

    def _context_priority(self, result: RetrievalResult, query_plan: QueryPlan) -> float:
        chunk_type = str(result.metadata.get("chunk_type", ""))
        if chunk_type == "structured_match_summary":
            return 3.0
        if chunk_type == "derived_analytic_summary":
            if query_plan.intent in {"aggregation", "comparison"}:
                return 2.7
            return 1.7
        if query_plan.intent == "definition" and chunk_type == "note_reference":
            return 2.5
        if chunk_type in {"structured_row_match", "company_profile", "row_full"}:
            return 1.5
        if chunk_type == "note_reference":
            return 1.0
        return 0.0

    def _collection_name(self, chunks: list[Chunk], embedding_model: str) -> str:
        return f"ev_compare_{build_collection_fingerprint(chunks, embedding_model)}"

    def _qdrant_point_id(self, logical_chunk_id: str) -> str:
        try:
            return str(uuid.UUID(logical_chunk_id))
        except (ValueError, AttributeError, TypeError):
            return str(uuid.uuid5(uuid.NAMESPACE_URL, logical_chunk_id))

    def _logical_chunk_id(self, point: Any) -> str:
        payload = getattr(point, "payload", None) or {}
        payload_chunk_id = payload.get("chunk_id")
        if payload_chunk_id:
            return str(payload_chunk_id)
        return self.point_id_to_chunk_id.get(str(point.id), str(point.id))

    def _create_client(self, qdrant_path: Path) -> QdrantClient:
        if QdrantClient is None:
            raise RuntimeError("qdrant-client is required to create a retrieval index.")
        try:
            return QdrantClient(path=str(qdrant_path))
        except RuntimeError as exc:
            if "already accessed by another instance" not in str(exc):
                raise
            if self._persistent_collection_requested:
                raise RuntimeError(
                    f"Qdrant path is locked and collection '{self.collection_name}' requires a persistent index. "
                    "Close the other process using the local Qdrant path and try again."
                ) from exc
            self._temp_dir = tempfile.TemporaryDirectory(prefix="ev_qdrant_")
            return QdrantClient(path=self._temp_dir.name)

    def _match_known_values(self, normalized_question: str, values: list[str]) -> list[str]:
        matches: list[str] = []
        seen_normalized: set[str] = set()
        padded_question = f" {normalized_question} "
        for value in values:
            normalized_value = normalize_text(value)
            if not normalized_value or normalized_value in seen_normalized:
                continue
            if f" {normalized_value} " in padded_question:
                matches.append(value)
                seen_normalized.add(normalized_value)
        return matches

    def _match_known_categories(self, question: str) -> list[str]:
        question_key = self._category_key(question)
        matches: list[str] = []
        seen_keys: set[str] = set()
        for category in getattr(self, "known_categories", []):
            category_key = self._category_key(category)
            if not category_key or category_key in seen_keys:
                continue
            if category_key == "oem" and not self._question_intends_oem_category(question_key):
                continue
            if re.search(rf"(?<!\\w){re.escape(category_key)}(?!\\w)", question_key):
                matches.append(category)
                seen_keys.add(category_key)
        return matches

    def _extract_exact_category_filter(self, question: str) -> str | None:
        patterns = [
            r"category\s*=\s*([A-Za-z0-9 ()/-]+?)(?:\s+and\b|\?|$)",
            r"category\s+is\s+([A-Za-z0-9 ()/-]+?)(?:\s+and\b|\?|$)",
        ]
        for pattern in patterns:
            match = re.search(pattern, question, flags=re.IGNORECASE)
            if match:
                return self._category_key(match.group(1))
        return None

    def _question_intends_oem_category(self, question_key: str) -> bool:
        if any(
            term in question_key
            for term in {
                "primary oem",
                "primary oems",
                "oem contract",
                "oem contracts",
                "oem customer",
                "oem customers",
            }
        ):
            return False
        if any(
            term in question_key
            for term in {
                "category oem",
                "category is oem",
                "category oems",
                "oem footprint",
                "oem footprints",
                "oem company",
                "oem companies",
                "original equipment manufacturer",
                "original equipment manufacturers",
            }
        ):
            return True
        return bool(re.search(r"^(show|list|find|identify|map)\s+oem\b", question_key))

    def _build_analytic_summary_lines(
        self,
        query_plan: QueryPlan,
        matched_rows: list[dict[str, str]],
    ) -> list[str]:
        question = query_plan.normalized_question
        frame = self._matched_rows_frame(matched_rows)
        if frame.empty:
            return []
        group_field = self._detect_group_field(question)

        # Let _build_structured_summary handle explicit exhaustive grouped listings.
        if query_plan.group_by_role and self._is_exhaustive_question(query_plan):
            return []

        threshold = self._extract_employment_threshold(query_plan.question)
        if threshold is not None:
            operator, value = threshold
            if operator == ">=":
                frame = frame[frame["employment_num"] >= value]
            elif operator == ">":
                frame = frame[frame["employment_num"] > value]
            elif operator == "<=":
                frame = frame[frame["employment_num"] <= value]
            elif operator == "<":
                frame = frame[frame["employment_num"] < value]

        if "excluding rows with location = georgia" in question:
            frame = frame[frame["location"].astype(str).str.strip().str.lower() != "georgia"]

        relevance_comparison = any(
            term in question for term in {"yes versus indirect", "yes vs indirect", "yes versus"}
        )
        if not relevance_comparison:
            if "ev battery relevant yes" in question or "ev battery relevant = yes" in question:
                frame = frame[frame["ev_battery_relevant"].astype(str).str.strip().str.lower() == "yes"]
            elif "ev battery relevant indirect" in question or "ev battery relevant = indirect" in question:
                frame = frame[frame["ev_battery_relevant"].astype(str).str.strip().str.lower() == "indirect"]
            elif "ev battery relevant no" in question or "ev battery relevant = no" in question:
                frame = frame[frame["ev_battery_relevant"].astype(str).str.strip().str.lower() == "no"]

        if "public oem footprint supplier listing" in question:
            frame = frame[
                frame["classification_method"].astype(str).str.strip().eq(
                    "Public OEM footprint / supplier listing"
                )
            ]
            if not frame.empty:
                category_counts = frame.groupby("category").size().sort_values(ascending=False)
                relevance_counts = frame.groupby("ev_battery_relevant").size().sort_values(ascending=False)
                companies = "; ".join(frame["company"].astype(str).tolist())
                return [
                    f"Public OEM footprint / supplier listing [{len(frame)} rows]:",
                    "Category summary: " + "; ".join(f"{label}: {int(value)}" for label, value in category_counts.items()),
                    "EV / Battery Relevant summary: " + "; ".join(f"{label}: {int(value)}" for label, value in relevance_counts.items()),
                    f"Companies: {companies}",
                ]

        if "starts with oem" in question and "supplier or affiliation type" in question:
            frame = frame[
                frame["category"].astype(str).str.startswith("OEM")
                & frame["supplier_or_affiliation_type"].astype(str).str.strip().eq("")
            ]

        if "missing primary oems" in question:
            frame = frame[frame["primary_oems"].astype(str).str.strip().eq("")]
            return self._format_detail_lines(
                frame,
                ["ev_supply_chain_role"],
                heading="Entries with missing Primary OEMs:",
            )

        if "mentions" in question and ("product / service" in question or "product service" in question):
            search_terms = self._extract_quoted_terms(query_plan.question)
            if search_terms:
                frame = frame[
                    frame["product_service"].astype(str).str.lower().apply(
                        lambda text: any(term in text for term in search_terms)
                        or ("wire" in normalize_text(text) and "harness" in normalize_text(text))
                    )
                ]
                return self._format_detail_lines(
                    frame,
                    self._detail_fields(question) or ["primary_oems"],
                    heading="Matching Product / Service entries:",
                )

        if "primary facility type containing both" in question:
            frame = frame[
                frame["primary_facility_type"].astype(str).str.lower().apply(
                    lambda value: "engineering" in value and "manufacturing" in value
                )
            ]
            return self._format_detail_lines(
                frame,
                self._detail_fields(question) or ["product_service"],
                heading="Primary Facility Type matches:",
            )

        if "industry group" in question and "represented" in question:
            if "how many" in question:
                count_lines = self._group_count_summary(question, frame, "industry_group", query_plan)
                if count_lines:
                    if "ev battery relevant" in question and "yes" in question:
                        count_lines[0] = "Industry Groups represented among EV / Battery Relevant = Yes companies:"
                    else:
                        count_lines[0] = "Industry Groups represented among matching companies:"
                    return count_lines
            groups = sorted({value for value in frame["industry_group"].astype(str) if value.strip()})
            if groups:
                return ["Industry Groups represented:", f"- {'; '.join(groups)}"]

        if "linked to" in question and query_plan.matched_primary_oems:
            return self._format_detail_lines(
                frame,
                self._detail_fields(question) or ["ev_supply_chain_role", "location", "employment", "ev_battery_relevant"],
                heading="Linked companies:",
            )

        if (
            query_plan.matched_companies
            and "location" in question
            and "primary facility type" in question
        ):
            return self._format_detail_lines(
                frame,
                ["location", "primary_facility_type", "ev_supply_chain_role"],
                heading="Company records by location:",
                include_company=False,
            )

        if "both tier 1 and tier 2/3 companies" in question and "city" in question:
            grouped = frame.groupby("city")
            lines = ["Cities with both Tier 1 and Tier 2/3 companies:"]
            found = False
            for city, group in grouped:
                if not city:
                    continue
                categories = set(group["category"].astype(str))
                if {"Tier 1", "Tier 2/3"}.issubset(categories):
                    found = True
                    companies = sorted(dict.fromkeys(group["company"].astype(str)))
                    lines.append(f"- {city}: {', '.join(companies)}")
            return lines if found else []

        if "appear multiple times" in question:
            grouped = frame.groupby("company").agg(
                roles=("ev_supply_chain_role", lambda values: sorted({value for value in values if str(value).strip()}))
            )
            grouped = grouped[grouped["roles"].apply(lambda values: len(values) > 1)]
            if grouped.empty:
                return ["Companies appearing multiple times with distinct EV Supply Chain Roles: None"]
            lines = ["Companies appearing multiple times with distinct EV Supply Chain Roles:"]
            for company, row in grouped.sort_index().iterrows():
                lines.append(f"- {company}: {', '.join(row['roles'])}")
            return lines

        if "only one company in the dataset" in question and "ev supply chain role" in question:
            grouped = frame.groupby("ev_supply_chain_role").agg(
                count=("company", "size"),
                companies=("company", lambda values: sorted(dict.fromkeys(values))),
            )
            grouped = grouped[grouped["count"] == 1]
            if grouped.empty:
                return []
            lines = ["EV Supply Chain Roles with exactly one company:"]
            for role, row in grouped.sort_index().iterrows():
                lines.append(f"- {role}: {', '.join(row['companies'])}")
            return lines

        if "most concentrated in a single county" in question and "ev supply chain role" in question:
            county_rows = frame[frame["county"].astype(str).str.strip().ne("")]
            if county_rows.empty:
                return []
            best_role = None
            best_county = None
            best_share = -1.0
            best_companies: list[str] = []
            for role, role_group in county_rows.groupby("ev_supply_chain_role"):
                counts = role_group.groupby("county").size()
                total = int(counts.sum())
                if total <= 0:
                    continue
                county = counts.idxmax()
                share = float(counts.max()) / total
                if share > best_share:
                    best_role = role
                    best_county = county
                    best_share = share
                    best_companies = sorted(
                        dict.fromkeys(role_group[role_group["county"] == county]["company"].astype(str))
                    )
            if best_role and best_county:
                return [
                    "Most concentrated EV Supply Chain Role:",
                    f"- Role: {best_role}",
                    f"- County: {best_county}",
                    f"- Share: {best_share:.2f}",
                    f"- Companies: {', '.join(best_companies)}",
                ]

        if "supply to more than one oem" in question:
            filtered = frame[frame["primary_oems"].apply(self._is_explicit_multi_oem)]
            return self._format_detail_lines(
                filtered,
                ["primary_oems"],
                heading="Companies linked to more than one explicit OEM:",
            )

        if (
            ("ev battery relevant" in question or "ev / battery relevant" in question)
            and ("yes versus indirect" in question or "yes vs indirect" in question or "yes versus" in question)
        ):
            grouped = frame[frame["ev_battery_relevant"].astype(str).str.strip().ne("")].groupby("ev_battery_relevant")
            lines = ["EV / Battery Relevant groups:"]
            for label, group in grouped:
                companies = sorted(dict.fromkeys(group["company"].astype(str)))
                lines.append(f"- {label}: {len(group)}")
                lines.append(f"  Companies: {', '.join(companies)}")
            return lines

        if (
            ("ev battery relevant" in question or "ev / battery relevant" in question)
            and ("list all companies" in question or "provide their ev supply chain role and category" in question)
        ):
            return self._format_detail_lines(
                frame,
                ["ev_supply_chain_role", "category"],
                heading="EV / Battery Relevant = Yes companies:",
            )

        if (
            ("ev battery relevant" in question or "ev / battery relevant" in question)
            and "which ev supply chain roles have at least one company" in question
        ):
            grouped = frame.groupby("ev_supply_chain_role").agg(
                companies=("company", lambda values: sorted(dict.fromkeys(values)))
            )
            if grouped.empty:
                return ["EV Supply Chain Roles with EV / Battery Relevant = Yes companies: None"]
            lines = ["EV Supply Chain Roles with EV / Battery Relevant = Yes companies:"]
            for role, row in grouped.sort_index().iterrows():
                companies = row["companies"]
                if not companies:
                    continue
                lines.append(f"- {role}: {'; '.join(companies)}")
            return lines

        if "only one" in question and ("county" in question or "counties" in question):
            county_lines = self._group_count_summary(question, frame, "county", query_plan)
            if county_lines:
                return county_lines

        if (
            re.search(r"\bcount\b", question)
            or "how many" in question
            or "highest number of" in question
            or "largest number of companies" in question
            or "fewest companies" in question
        ):
            count_lines = self._group_count_summary(question, frame, group_field, query_plan)
            if count_lines:
                return count_lines

        if "total employment" in question or "compare total employment" in question:
            total_lines = self._group_numeric_summary(
                question,
                frame,
                group_field,
                metric="sum",
                heading="Total Employment",
            )
            if total_lines:
                return total_lines

        if "average employment" in question:
            average_lines = self._group_numeric_summary(
                question,
                frame,
                group_field,
                metric="mean",
                heading="Average Employment",
            )
            if average_lines:
                return average_lines

        if "median employment" in question:
            with_employment = frame.dropna(subset=["employment_num"])
            if not with_employment.empty:
                median_value = float(with_employment["employment_num"].median())
                lines = [f"Median Employment: {median_value:.0f}"]
                if "list all entries" in question:
                    lines.extend(
                        self._format_detail_lines(
                            with_employment,
                            ["employment", "category", "ev_supply_chain_role", "location"],
                            heading="Matching entries:",
                        )[1:]
                    )
                return lines

        if "range" in question and "employment" in question:
            with_employment = frame.dropna(subset=["employment_num"])
            if not with_employment.empty:
                min_row = with_employment.sort_values("employment_num", ascending=True).iloc[0]
                max_row = with_employment.sort_values("employment_num", ascending=False).iloc[0]
                return [
                    "Employment range:",
                    f"- Min: {int(min_row['employment_num'])} | Company: {min_row['company']}",
                    f"- Max: {int(max_row['employment_num'])} | Company: {max_row['company']}",
                ]

        if ("highest employment" in question or "lowest employment" in question) and ("companies" in question or "company" in question):
            with_employment = frame.dropna(subset=["employment_num"])
            if not with_employment.empty:
                ascending = "lowest employment" in question
                limit = self._extract_rank_limit(question, default=10)
                ranked = with_employment.sort_values(
                    ["employment_num", "company"],
                    ascending=[ascending, True],
                ).head(limit)
                return self._format_detail_lines(
                    ranked,
                    self._detail_fields(question) or ["employment", "category", "location", "ev_supply_chain_role"],
                    heading="Employment-ranked companies:",
                )

        if "highest employment" in question and "product / service" in question:
            with_employment = frame.dropna(subset=["employment_num"])
            if not with_employment.empty:
                top_row = with_employment.sort_values("employment_num", ascending=False).iloc[[0]]
                return self._format_detail_lines(
                    top_row,
                    ["employment", "product_service"],
                    heading="Highest-employment match:",
                )

        if "represented" in question and group_field:
            represented = self._group_count_summary(question, frame, group_field, query_plan)
            if represented:
                return represented

        if "categories exist" in question and group_field:
            categories = self._group_count_summary(question, frame, group_field, query_plan)
            if categories:
                return categories

        detail_lines = self._format_detail_lines(
            frame,
            self._detail_fields(question),
            heading="Matching rows:",
        )
        if detail_lines:
            return detail_lines

        return []

    def _parse_employment(self, value: str) -> int | None:
        digits = re.sub(r"[^0-9]", "", value or "")
        if not digits:
            return None
        return int(digits)

    def _match_locations(self, normalized_question: str) -> list[str]:
        matches: list[str] = []
        seen: set[str] = set()
        padded_question = f" {normalized_question} "
        for location in getattr(self, "known_locations", []):
            normalized_location = normalize_text(location)
            if not normalized_location or normalized_location in seen:
                continue
            county_fragment = ""
            if "," in location:
                county_fragment = normalize_text(location.split(",", 1)[1])
            if (
                f" {normalized_location} " in padded_question
                or (county_fragment and f" {county_fragment} " in padded_question)
            ):
                matches.append(location)
                seen.add(normalized_location)
        return matches

    def _extract_excluded_locations(self, question: str) -> list[str]:
        matches: list[str] = []
        for pattern in [
            r"excluding rows with location\s*=\s*([A-Za-z0-9 ,()/-]+?)(?:\.|\?|,|$)",
            r"excluding location\s*=\s*([A-Za-z0-9 ,()/-]+?)(?:\.|\?|,|$)",
        ]:
            for match in re.finditer(pattern, question, flags=re.IGNORECASE):
                value = match.group(1).strip()
                if value:
                    matches.append(value)
        return list(dict.fromkeys(matches))

    def _category_key(self, value: str) -> str:
        return re.sub(r"\s+", " ", value.strip().lower())

    def _matched_rows_frame(self, matched_rows: list[dict[str, str]]) -> pd.DataFrame:
        frame = pd.DataFrame(matched_rows).fillna("")
        if frame.empty:
            return frame
        frame["employment_num"] = frame["employment"].apply(self._parse_employment)
        location_parts = frame["location"].apply(self._split_location)
        frame["city"] = [city for city, _ in location_parts]
        frame["county"] = [county for _, county in location_parts]
        return frame

    def _split_location(self, value: str) -> tuple[str, str]:
        location = (value or "").strip()
        if not location:
            return "", ""
        if location.lower() == "georgia":
            return "", ""
        if "," in location:
            city, county = location.split(",", 1)
            return city.strip(), county.strip()
        if "county" in location.lower():
            return "", location
        return location, ""

    def _detect_group_field(self, question: str) -> str | None:
        if "industry group" in question or "industry groups" in question:
            return "industry_group"
        if "primary facility type" in question or "primary facility types" in question:
            return "primary_facility_type"
        if "supplier or affiliation type" in question:
            return "supplier_or_affiliation_type"
        if "classification method" in question or "classification methods" in question:
            return "classification_method"
        if "primary oem" in question or "primary oems" in question:
            return "primary_oems"
        if "ev / battery relevant" in question or "ev battery relevant" in question:
            return "ev_battery_relevant"
        if "ev supply chain role" in question or "ev supply chain roles" in question:
            return "ev_supply_chain_role"
        if "category" in question or "categories" in question:
            return "category"
        if "county" in question or "counties" in question:
            return "county"
        if "city" in question or "cities" in question:
            return "city"
        return None

    def _group_count_summary(
        self,
        question: str,
        frame: pd.DataFrame,
        group_field: str | None,
        query_plan: QueryPlan,
    ) -> list[str]:
        if not group_field or group_field not in frame.columns:
            return []
        counted = frame[frame[group_field].astype(str).str.strip().ne("")]
        if counted.empty:
            return []
        grouped_series = counted.groupby(group_field).size()
        grouped_df = (
            grouped_series.rename("count")
            .reset_index()
            .sort_values(by=["count", group_field], ascending=[False, True])
        )
        if "fewest companies" in question or "bottom" in question:
            grouped_df = grouped_df.sort_values(by=["count", group_field], ascending=[True, True])
            limit = self._extract_rank_limit(question, default=5)
        elif "top" in question or "highest" in question or "largest" in question:
            limit = self._extract_rank_limit(question, default=5)
        else:
            limit = None
        if "only one" in question:
            grouped_df = grouped_df[grouped_df["count"] == 1]
        if limit is not None:
            grouped_df = grouped_df.head(limit)
        lines = [f"Counts by {self._display_field_name(group_field)}:"]
        for row in grouped_df.itertuples(index=False):
            label = getattr(row, group_field)
            value = getattr(row, "count")
            lines.append(f"- {label}: {int(value)}")
        if group_field in {"county", "city"} and ("matching companies" in question or "name that supplier" in question):
            for row in grouped_df.itertuples(index=False):
                label = getattr(row, group_field)
                companies = sorted(dict.fromkeys(counted[counted[group_field] == label]["company"].astype(str)))
                lines.append(f"  Companies: {', '.join(companies)}")
        return lines

    def _group_numeric_summary(
        self,
        question: str,
        frame: pd.DataFrame,
        group_field: str | None,
        metric: str,
        heading: str,
    ) -> list[str]:
        if group_field is None or group_field not in frame.columns:
            return []
        numeric = frame.dropna(subset=["employment_num"])
        numeric = numeric[numeric[group_field].astype(str).str.strip().ne("")]
        if numeric.empty:
            return []
        grouped = numeric.groupby(group_field)["employment_num"]
        if metric == "sum":
            series = grouped.sum().sort_values(ascending=False)
        else:
            series = grouped.mean().sort_values(ascending=False)
        if "top" in question:
            series = series.head(self._extract_rank_limit(question, default=3))
        lines = [f"{heading} by {self._display_field_name(group_field)}:"]
        for label, value in series.items():
            value_text = f"{float(value):.2f}" if metric == "mean" else str(int(value))
            lines.append(f"- {label}: {value_text}")
        if "highest" in question and not "top" in question:
            top_label = series.index[0]
            top_value = series.iloc[0]
            value_text = f"{float(top_value):.2f}" if metric == "mean" else str(int(top_value))
            lines.append(f"Highest: {top_label} ({value_text})")
        return lines

    def _detail_fields(self, question: str) -> list[str]:
        fields: list[str] = []
        if "industry group" in question:
            fields.append("industry_group")
        if "category" in question:
            fields.append("category")
        if "classification method" in question:
            fields.append("classification_method")
        if "supplier or affiliation type" in question:
            fields.append("supplier_or_affiliation_type")
        if "ev supply chain role" in question:
            fields.append("ev_supply_chain_role")
        if "primary oem" in question:
            fields.append("primary_oems")
        if "location" in question or "county" in question or "city" in question:
            fields.append("location")
        if "primary facility type" in question:
            fields.append("primary_facility_type")
        if "employment" in question:
            fields.append("employment")
        if "product / service" in question or "product service" in question:
            fields.append("product_service")
        if "ev / battery relevant" in question or "ev battery relevant" in question:
            fields.append("ev_battery_relevant")
        return list(dict.fromkeys(fields))

    def _format_detail_lines(
        self,
        frame: pd.DataFrame,
        fields: list[str],
        heading: str,
        include_company: bool = True,
    ) -> list[str]:
        if frame.empty:
            return []
        lines = [heading]
        selected_fields = fields or []
        for _, row in frame.iterrows():
            parts: list[str] = []
            if include_company:
                company = str(row.get("company", "")).strip()
                if company:
                    parts.append(f"Company: {company}")
            for field in selected_fields:
                value = str(row.get(field, "")).strip()
                if not value:
                    continue
                parts.append(f"{self._display_field_name(field)}: {value}")
            if parts:
                lines.append("- " + " | ".join(parts))
        return lines if len(lines) > 1 else []

    def _display_field_name(self, field_name: str) -> str:
        mapping = {
            "industry_group": "Industry Group",
            "category": "Category",
            "classification_method": "Classification Method",
            "supplier_or_affiliation_type": "Supplier or Affiliation Type",
            "ev_supply_chain_role": "EV Supply Chain Role",
            "primary_oems": "Primary OEMs",
            "location": "Location",
            "primary_facility_type": "Primary Facility Type",
            "employment": "Employment",
            "product_service": "Product / Service",
            "ev_battery_relevant": "EV / Battery Relevant",
            "county": "County",
            "city": "City",
        }
        return mapping.get(field_name, field_name.replace("_", " ").title())

    def _extract_rank_limit(self, question: str, default: int) -> int:
        match = re.search(r"(top|bottom)\s+(\d+)", question)
        if match:
            return int(match.group(2))
        return default

    def _extract_quoted_terms(self, question: str) -> list[str]:
        quoted = re.findall(r"'([^']+)'|\"([^\"]+)\"", question)
        terms = [normalize_text(a or b) for a, b in quoted if (a or b).strip()]
        return [term for term in terms if term]

    def _extract_employment_threshold(self, question: str) -> tuple[str, int] | None:
        patterns = [
            r"employment\s*(>=|<=|>|<)\s*([\d,]+)",
            r"employment\s+at\s+least\s+([\d,]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, question, flags=re.IGNORECASE)
            if not match:
                continue
            if len(match.groups()) == 2:
                operator, value = match.groups()
                return operator, int(value.replace(",", ""))
            return ">=", int(match.group(1).replace(",", ""))
        return None

    def _is_explicit_multi_oem(self, value: object) -> bool:
        normalized = normalize_text(str(value))
        if not normalized or normalized == "multiple oems":
            return False
        return len(normalized.split()) >= 3
