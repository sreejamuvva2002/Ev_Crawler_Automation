from __future__ import annotations

import csv
from dataclasses import dataclass
import hashlib
from html import unescape
import json
from pathlib import Path
import re
import shutil
import subprocess

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None  # type: ignore[assignment]

try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None  # type: ignore[assignment]

from .chunking import sliding_window_chunks, tokenize
from .schemas import Chunk
from .settings import RetrievalSettings

SUPPORTED_TEXT_SUFFIXES = {".txt", ".md"}
SUPPORTED_HTML_SUFFIXES = {".html", ".htm"}
SUPPORTED_PDF_SUFFIXES = {".pdf"}
SUPPORTED_SUFFIXES = SUPPORTED_TEXT_SUFFIXES | SUPPORTED_HTML_SUFFIXES | SUPPORTED_PDF_SUFFIXES


@dataclass(slots=True)
class DocumentRecord:
    filepath: Path
    canonical_source: str
    source_type: str
    text: str
    title: str | None = None
    url: str | None = None
    retrieved_at: str | None = None


@dataclass(slots=True)
class DocumentLoadIssue:
    filepath: str
    reason: str


@dataclass(slots=True)
class DocumentLoadResult:
    records: list[DocumentRecord]
    issues: list[DocumentLoadIssue]


def resolve_tavily_root(project_root: Path) -> Path:
    candidates = [
        project_root / "data" / "tavily",
        project_root / "data" / "tavily ready documents",
        project_root / "data" / "tavilt ready documents",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate.resolve()
    searched = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"Offline Tavily directory not found. Checked: {searched}")


def load_offline_documents(root: Path, *, source_type: str) -> DocumentLoadResult:
    manifest = _load_manifest(root)
    records: list[DocumentRecord] = []
    issues: list[DocumentLoadIssue] = []

    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in SUPPORTED_SUFFIXES:
            continue

        manifest_entry = manifest.get(path.name, {}) | manifest.get(_relative_posix(path, root), {})
        try:
            text, title, url = _extract_document_content(path)
        except Exception as exc:
            issues.append(DocumentLoadIssue(filepath=_relative_posix(path, root), reason=str(exc)))
            continue

        normalized_text = _normalize_document_text(text)
        if not normalized_text:
            issues.append(
                DocumentLoadIssue(
                    filepath=_relative_posix(path, root),
                    reason="No extractable text was found.",
                )
            )
            continue

        records.append(
            DocumentRecord(
                filepath=path.resolve(),
                canonical_source=_relative_posix(path, root.parent.parent if root.parent.name == "data" else root),
                source_type=source_type,
                text=normalized_text,
                title=title or manifest_entry.get("title") or _humanize_stem(path.stem),
                url=url or manifest_entry.get("url"),
                retrieved_at=manifest_entry.get("retrieved_at"),
            )
        )
    return DocumentLoadResult(records=records, issues=issues)


def build_document_chunks(
    records: list[DocumentRecord],
    settings: RetrievalSettings,
) -> list[Chunk]:
    chunks: list[Chunk] = []
    for record in records:
        for chunk_index, (chunk_start, chunk_text) in enumerate(
            sliding_window_chunks(
                record.text,
                settings.note_chunk_size,
                settings.note_chunk_overlap,
            )
        ):
            chunk_id = hashlib.sha1(
                f"{record.canonical_source}::{chunk_index}".encode("utf-8")
            ).hexdigest()
            chunks.append(
                Chunk(
                    chunk_id=chunk_id,
                    text=chunk_text,
                    metadata={
                        "source_type": record.source_type,
                        "filepath": record.canonical_source,
                        "title": record.title or "",
                        "url": record.url or "",
                        "retrieved_at": record.retrieved_at or "",
                        "chunk_index": chunk_index,
                        "chunk_start": chunk_start,
                        "chunk_type": "document_chunk",
                        "source_file": record.filepath.name,
                    },
                    token_set=tokenize(chunk_text),
                )
            )
    return chunks


def _extract_document_content(path: Path) -> tuple[str, str | None, str | None]:
    suffix = path.suffix.lower()
    if suffix in SUPPORTED_TEXT_SUFFIXES:
        return path.read_text(encoding="utf-8", errors="ignore"), None, None
    if suffix in SUPPORTED_HTML_SUFFIXES:
        return _extract_html_content(path)
    if suffix in SUPPORTED_PDF_SUFFIXES:
        return _extract_pdf_content(path)
    raise ValueError(f"Unsupported document type: {path.suffix}")


def _extract_html_content(path: Path) -> tuple[str, str | None, str | None]:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    if BeautifulSoup is None:
        title = _first_match(raw, [r"<title>(.*?)</title>"])
        url = _first_match(
            raw,
            [
                r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\'](.*?)["\']',
                r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\'](.*?)["\']',
            ],
        )
        stripped = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\\1>", " ", raw)
        stripped = re.sub(r"(?s)<[^>]+>", "\n", stripped)
        return stripped, title, url

    soup = BeautifulSoup(raw, "html.parser")
    title = _extract_html_title(soup)
    url = _extract_html_url(soup)
    for tag_name in ("script", "style", "noscript", "svg", "img", "picture", "source"):
        for tag in soup.find_all(tag_name):
            tag.decompose()
    for tag_name in ("header", "footer", "nav", "aside", "form"):
        for tag in soup.find_all(tag_name):
            tag.decompose()

    content_root = soup.find("article") or soup.find("main") or soup.body or soup
    text = content_root.get_text("\n", strip=True)
    return text, title, url


def _extract_html_title(soup: BeautifulSoup) -> str | None:  # type: ignore[valid-type]
    candidates = [
        soup.find("meta", attrs={"property": "og:title"}),
        soup.find("meta", attrs={"name": "twitter:title"}),
    ]
    for candidate in candidates:
        content = str(candidate.get("content", "")).strip() if candidate else ""
        if content:
            return content
    if soup.title and soup.title.string:
        text = soup.title.string.strip()
        if text:
            return text
    return None


def _extract_html_url(soup: BeautifulSoup) -> str | None:  # type: ignore[valid-type]
    canonical = soup.find("link", attrs={"rel": "canonical"})
    if canonical and canonical.get("href"):
        return str(canonical["href"]).strip()
    og_url = soup.find("meta", attrs={"property": "og:url"})
    if og_url and og_url.get("content"):
        return str(og_url["content"]).strip()
    return None


def _extract_pdf_content(path: Path) -> tuple[str, str | None, str | None]:
    errors: list[str] = []
    title: str | None = None
    if PdfReader is not None:
        try:
            reader = PdfReader(str(path), strict=False)
            metadata = getattr(reader, "metadata", None)
            title = _clean_pdf_title(getattr(metadata, "title", None) if metadata else None)
            parts: list[str] = []
            for page in reader.pages:
                page_text = page.extract_text() or ""
                if page_text.strip():
                    parts.append(page_text)
            if parts:
                return "\n\n".join(parts), title, None
            errors.append("pypdf returned no text")
        except Exception as exc:
            errors.append(f"pypdf failed: {exc}")

    pdftotext_path = shutil.which("pdftotext")
    if pdftotext_path:
        try:
            result = subprocess.run(
                [pdftotext_path, str(path), "-"],
                check=True,
                capture_output=True,
                text=True,
            )
            if result.stdout.strip():
                return result.stdout, title, None
            errors.append("pdftotext returned no text")
        except Exception as exc:
            errors.append(f"pdftotext failed: {exc}")

    raise RuntimeError("; ".join(errors) or "No PDF parser is available.")


def _load_manifest(root: Path) -> dict[str, dict[str, str]]:
    manifest_rows: dict[str, dict[str, str]] = {}
    manifest_candidates = sorted(root.glob("*manifest*.csv"))
    for manifest_path in manifest_candidates:
        try:
            with manifest_path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames:
                    continue
                for raw_row in reader:
                    row = {
                        str(key).strip().lower(): str(value).strip()
                        for key, value in raw_row.items()
                        if key is not None and value is not None and str(value).strip()
                    }
                    if not row:
                        continue
                    key = (
                        row.get("filepath")
                        or row.get("file_path")
                        or row.get("filename")
                        or row.get("local_filename")
                        or row.get("saved_as")
                        or row.get("basename")
                    )
                    if not key:
                        continue
                    manifest_rows[key] = {
                        "title": row.get("title") or row.get("name") or "",
                        "url": row.get("url") or row.get("source_url") or "",
                        "retrieved_at": row.get("retrieved_at") or row.get("downloaded_at") or "",
                    }
        except OSError:
            continue
    return manifest_rows


def _normalize_document_text(text: str) -> str:
    text = unescape(text or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for line in text.split("\n"):
        compact = re.sub(r"\s+", " ", line).strip()
        if compact:
            lines.append(compact)
    return "\n".join(lines).strip()


def _relative_posix(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.name


def _humanize_stem(stem: str) -> str:
    return re.sub(r"[_-]+", " ", stem).strip()


def _first_match(text: str, patterns: list[str]) -> str | None:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            value = re.sub(r"\s+", " ", match.group(1)).strip()
            if value:
                return value
    return None


def _clean_pdf_title(value: object) -> str | None:
    if value is None:
        return None
    title = str(value).strip()
    if not title:
        return None
    try:
        parsed = json.loads(title)
        if isinstance(parsed, str) and parsed.strip():
            return parsed.strip()
    except json.JSONDecodeError:
        pass
    return title
