from __future__ import annotations

import hashlib
import html
import json
import math
import os
import re
import zipfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - optional dependency
    BeautifulSoup = None

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional dependency
    PdfReader = None

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover - optional dependency
    fitz = None


DIGITAL_TWIN_SIGNAL_KEYWORDS: dict[str, list[str]] = {
    "network_structure": [
        "tier 1",
        "tier 2",
        "tier 3",
        "supplier",
        "customer",
        "supply chain map",
        "ecosystem map",
        "value chain",
        "network",
        "oem",
    ],
    "node_attributes": [
        "facility",
        "plant",
        "location",
        "capacity",
        "gwh",
        "mwh",
        "tons per year",
        "ownership",
        "commissioning",
        "timeline",
        "expansion",
        "site",
    ],
    "edge_attributes": [
        "material flow",
        "logistics",
        "route",
        "lead time",
        "import",
        "export",
        "shipping",
        "freight",
        "port",
        "rail",
        "hazmat",
    ],
    "risk_variables": [
        "risk",
        "bottleneck",
        "concentration",
        "single source",
        "trade exposure",
        "regulatory",
        "permitting",
        "disruption",
        "dependency",
        "shortage",
    ],
    "optimization_levers": [
        "localization",
        "gap analysis",
        "incentive",
        "tax credit",
        "workforce",
        "site readiness",
        "site selection",
        "grant",
        "capex",
        "investment",
    ],
    "scenario_modeling": [
        "scenario",
        "what-if",
        "stress test",
        "simulation",
        "resilience score",
        "digital twin",
        "control tower",
        "sensitivity analysis",
    ],
}

VALUE_CHAIN_KEYWORDS = [
    "minerals",
    "mining",
    "refining",
    "cathode",
    "cam",
    "anode",
    "separator",
    "electrolyte",
    "cell",
    "module",
    "pack",
    "oem",
    "recycling",
    "black mass",
    "hydrometallurgy",
]

PORT_TERMS = [
    "Port of Savannah",
    "Savannah port",
    "Garden City Terminal",
    "Port of Brunswick",
    "Brunswick port",
    "Colonel's Island Terminal",
    "Colonels Island Terminal",
]

REGION_KEYWORDS = [
    "georgia",
    "southeast",
    "battery belt",
    "atlanta",
    "savannah",
    "brunswick",
    "tennessee",
    "alabama",
    "south carolina",
    "north carolina",
    "kentucky",
    "mississippi",
]

POLICY_LOGISTICS_KEYWORDS = [
    "ira",
    "inflation reduction act",
    "doe",
    "state incentive",
    "tax credit",
    "permitting",
    "workforce",
    "savannah port",
    "brunswick port",
    "rail",
    "hazmat",
]

DOCUMENT_QUALITY_KEYWORDS = [
    "report",
    "white paper",
    "roadmap",
    "policy brief",
    "analysis",
    "study",
    "annual report",
    "investor presentation",
    "10-k",
    "8-k",
    "fact sheet",
    "directory",
    "dataset",
    "filing",
]

NEGATIVE_KEYWORDS = [
    "buy now",
    "shop now",
    "affiliate",
    "sponsored",
    "press release wire",
    "click here",
    "lifestyle",
    "celebrity",
    "top 10",
    "best ev",
    "review",
    "coupon",
    "deal",
    "sponsored content",
    "native advertising",
    "news roundup",
    "price prediction",
    "crypto",
    "betting",
    "giveaway",
    "chemistry only",
]

MARKETING_KEYWORDS = [
    "request a quote",
    "contact us",
    "learn more",
    "book a demo",
    "schedule a demo",
    "world-class",
    "best-in-class",
    "leading provider",
    "we offer",
    "our solutions",
    "our products",
    "trusted partner",
    "industry-leading",
]

COUNTY_ALIAS_STOPLIST = {
    "bacon",
    "banks",
    "barrow",
    "bleckley",
    "brooks",
    "clay",
    "coffee",
    "cook",
    "early",
    "fannin",
    "hall",
    "lee",
    "long",
    "madison",
    "mitchell",
    "union",
    "warren",
    "white",
}

HTML_BOILERPLATE_HINT_RE = re.compile(
    r"(nav|menu|footer|header|aside|sidebar|breadcrumb|social|share|cookie|banner|subscribe|newsletter|pager)",
    re.I,
)
HTML_CONTENT_HINT_RE = re.compile(
    r"(article|content|main|story|post|entry|body|report|copy|text|rich[-_ ]?text)",
    re.I,
)

RESEARCH_ONLY_KEYWORDS = [
    "abstract",
    "literature review",
    "electrochemical",
    "coulombic efficiency",
    "cycle life",
    "half-cell",
    "coin cell",
    "electrode slurry",
    "xrd",
    "sem analysis",
    "lab-scale",
    "novel chemistry",
    "peer reviewed",
]

GENERIC_NEWS_KEYWORDS = [
    "announced today",
    "according to",
    "reported",
    "news release",
    "press release",
    "breaking",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "weekend",
    "shares rose",
]

BLOCKLIST_SOURCE_DOMAIN_HINTS = {
    "ainvest.com",
}

LOW_TRUST_FINANCE_NEWS_DOMAIN_HINTS = {
    "ainvest.com",
    "benzinga.com",
    "investing.com",
    "investorplace.com",
    "marketbeat.com",
    "marketscreener.com",
    "fool.com",
    "247wallst.com",
    "seekingalpha.com",
    "stocktwits.com",
    "tipranks.com",
    "thestreet.com",
    "zacks.com",
}

DIRECT_USECASE_HINTS = [
    "supplier",
    "tier",
    "facility",
    "plant",
    "county",
    "georgia",
    "capacity",
    "gwh",
    "tons",
    "offtake",
    "localization",
]

KNOWN_DOCUMENT_EXTENSIONS = {".pdf", ".html", ".htm", ".txt", ".md"}

FACET_LABELS = [
    "who makes what where at what scale",
    "supplier and tier relationships",
    "logistics and infrastructure risk",
    "policy, incentives, and localization",
]


@dataclass
class GroundingDictionaries:
    companies: list[str]
    company_aliases: dict[str, list[str]]
    counties: list[str]
    county_aliases: dict[str, list[str]]
    county_regions: dict[str, str]
    oems: list[str]
    oem_aliases: dict[str, list[str]]
    ports: list[str]
    port_aliases: dict[str, list[str]]
    facility_types: list[str]
    supply_chain_roles: list[str]
    product_terms: list[str]
    value_chain_terms: list[str]
    docx_reference: str
    global_reference: str
    facet_texts: dict[str, str]


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def as_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return normalize_space(str(value))


def filename_to_text(name: str) -> str:
    txt = html.unescape(as_text(name))
    txt = re.sub(r"\.[a-z0-9]{2,6}$", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"_DOC_\d+(?:_\d+)?$", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"[_\-]+", " ", txt)
    return normalize_space(txt)


def normalize_key(text: str) -> str:
    text = html.unescape(as_text(text)).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return normalize_space(text)


def canonical_filename_key(name: str) -> str:
    return normalize_key(filename_to_text(name))


def get_domain(url: str) -> str:
    return (urlparse(url).netloc or "").lower()


def clamp_score(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def clamp_unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def keyword_hits(text: str, keywords: list[str]) -> tuple[int, list[str]]:
    txt = text.lower()
    hits = [kw for kw in keywords if kw in txt]
    return len(hits), hits


def category_hits(text: str) -> tuple[int, int, list[str]]:
    txt = text.lower()
    cat_names: list[str] = []
    total_hits = 0
    for cat, kws in DIGITAL_TWIN_SIGNAL_KEYWORDS.items():
        cat_hit = False
        for kw in kws:
            if kw in txt:
                cat_hit = True
                total_hits += 1
        if cat_hit:
            cat_names.append(cat)
    return len(cat_names), total_hits, cat_names


def question_coverage_count(text: str) -> int:
    txt = text.lower()
    groups = [
        ["who", "supplier", "manufacturer", "company", "producer"],
        ["what", "product", "material", "cell", "module", "pack", "cam", "anode"],
        ["where", "georgia", "southeast", "plant", "facility", "location", "county"],
        ["scale", "capacity", "gwh", "mwh", "tons", "mtpa"],
        ["relationship", "customer", "offtake", "agreement", "tier", "flow"],
        ["constraints", "risk", "permitting", "trade", "lead time", "bottleneck"],
        ["localization", "incentive", "workforce", "site readiness", "control tower"],
    ]
    count = 0
    for kws in groups:
        if any(kw in txt for kw in kws):
            count += 1
    return count


def specificity_signal_count(text: str) -> int:
    txt = text.lower()
    rules = [
        r"\b\d+(?:\.\d+)?\s?(gwh|mwh|kwh|mtpa|tons|tonnes|million|billion|%)\b",
        r"\b20\d{2}\b",
        r"\b(inc|llc|corp|corporation|co\.|ltd|plc|ag)\b",
        r"\b(offtake|joint venture|jv|supplier agreement|customer agreement)\b",
        r"\b(plant|facility|site|county|port|rail)\b",
    ]
    return sum(1 for pat in rules if re.search(pat, txt))


def source_credibility_score(domain: str) -> float:
    if not domain:
        return 3.0
    d = domain.lower()
    if any(d == hint or d.endswith(f".{hint}") for hint in BLOCKLIST_SOURCE_DOMAIN_HINTS):
        return 0.5
    if any(d == hint or d.endswith(f".{hint}") for hint in LOW_TRUST_FINANCE_NEWS_DOMAIN_HINTS):
        return 1.5
    if d.endswith(".gov"):
        return 15.0
    if d.endswith(".edu"):
        return 13.0
    if d.endswith(".org"):
        return 10.0
    if any(x in d for x in ["sec.gov", "doe.gov", "energy.gov", "nist.gov", "epa.gov"]):
        return 15.0
    if any(x in d for x in ["reuters", "bloomberg", "wsj", "ft.com"]):
        return 8.0
    if any(x in d for x in ["linkedin", "facebook", "instagram", "x.com", "twitter"]):
        return 2.0
    return 6.0


def source_domain_flags(domain: str) -> dict[str, int]:
    d = normalize_space(domain).lower()
    blocked = int(any(d == hint or d.endswith(f".{hint}") for hint in BLOCKLIST_SOURCE_DOMAIN_HINTS))
    low_trust = int(any(d == hint or d.endswith(f".{hint}") for hint in LOW_TRUST_FINANCE_NEWS_DOMAIN_HINTS))
    return {
        "Source_Blocklist_Flag": blocked,
        "Source_LowTrust_Flag": low_trust,
    }


def read_docx_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        with zipfile.ZipFile(path) as zf:
            xml = zf.read("word/document.xml").decode("utf-8", errors="ignore")
    except Exception:
        return ""
    pieces = re.findall(r"<w:t[^>]*>(.*?)</w:t>", xml)
    text = " ".join(html.unescape(p) for p in pieces)
    return normalize_space(text)


def split_multi_value(text: str) -> list[str]:
    txt = as_text(text)
    if not txt:
        return []
    parts = re.split(r";|/|,| and |\||•", txt, flags=re.IGNORECASE)
    return [normalize_space(p) for p in parts if normalize_space(p)]


def company_aliases(name: str) -> list[str]:
    base = normalize_key(name)
    aliases = {base}
    stripped = re.sub(r"\b(inc|llc|corp|corporation|company|co|ltd|plc|ag)\b", "", base).strip()
    stripped = normalize_space(stripped)
    if stripped and len(stripped) >= 4:
        aliases.add(stripped)
    return sorted(aliases, key=len, reverse=True)


def county_aliases(name: str) -> list[str]:
    base = normalize_key(name)
    aliases = {base}
    stripped = normalize_key(re.sub(r"\bcounty\b", "", name, flags=re.I))
    if stripped and len(stripped) >= 6 and stripped not in COUNTY_ALIAS_STOPLIST:
        aliases.add(stripped)
    return sorted(aliases, key=len, reverse=True)


def path_candidates_from_row(row: dict) -> list[Path]:
    candidates: list[Path] = []
    for field in ["Acquired_File_Path", "Resolved_File_Path", "Existing_Path"]:
        raw = as_text(row.get(field))
        if raw:
            candidates.append(Path(raw))
    base_path = as_text(row.get("Path"))
    filename = as_text(row.get("Filename"))
    if base_path and filename:
        candidates.append(Path(base_path) / filename)
    return candidates


def detect_grounding_file(name_variants: list[str], cwd: Path) -> Path | None:
    for name in name_variants:
        path = cwd / name
        if path.exists():
            return path
    return None


def build_grounding_dictionaries(
    *,
    automotive_xlsx: Path,
    counties_geojson: Path,
    supply_chain_docx: Path,
    golden_summary: str,
) -> GroundingDictionaries:
    companies: list[str] = []
    oems: list[str] = []
    facility_types: list[str] = []
    supply_chain_roles: list[str] = []
    product_terms: list[str] = []
    counties: list[str] = []
    county_regions: dict[str, str] = {}
    ports: list[str] = list(PORT_TERMS)

    if automotive_xlsx.exists():
        data_sheet = "Data"
        try:
            xl = pd.ExcelFile(automotive_xlsx)
            if data_sheet not in xl.sheet_names:
                data_sheet = xl.sheet_names[0]
            df = pd.read_excel(automotive_xlsx, sheet_name=data_sheet)
        except Exception:
            df = pd.DataFrame()
        if not df.empty:
            companies = sorted({as_text(v) for v in df.get("Company", []) if as_text(v)})
            facility_types = sorted({as_text(v) for v in df.get("Primary Facility Type", []) if as_text(v)})
            supply_chain_roles = sorted({as_text(v) for v in df.get("EV Supply Chain Role", []) if as_text(v)})
            product_terms = sorted({as_text(v) for v in df.get("Product / Service", []) if as_text(v)})
            for value in df.get("Primary OEMs", []):
                for item in split_multi_value(as_text(value)):
                    if item and "multiple oem" not in item.lower():
                        oems.append(item)
            for location in df.get("Location", []):
                loc = as_text(location)
                match = re.search(r"([A-Z][A-Za-z\-\s]+ County)\b", loc)
                if match:
                    counties.append(normalize_space(match.group(1)))

    if counties_geojson.exists():
        try:
            data = json.loads(counties_geojson.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        for feature in data.get("features", []):
            props = feature.get("properties") or {}
            county_name = as_text(props.get("NAMELSAD10")) or as_text(props.get("NAME10"))
            if county_name:
                counties.append(county_name)
                county_regions[county_name] = as_text(props.get("Reg_Comm"))

    counties = sorted({normalize_space(c) for c in counties if normalize_space(c)})
    oems = sorted({as_text(v) for v in oems if as_text(v)})
    docx_reference = read_docx_text(supply_chain_docx)[:5000]

    value_chain_terms = sorted({normalize_space(term) for term in VALUE_CHAIN_KEYWORDS + product_terms + supply_chain_roles if normalize_space(term)})

    companies_sample = ", ".join(companies[:20])
    counties_sample = ", ".join(counties[:20])
    oems_sample = ", ".join(oems[:12])
    ports_sample = ", ".join(ports[:8])
    facilities_sample = ", ".join(facility_types[:10])
    roles_sample = ", ".join(supply_chain_roles[:12])
    products_sample = ", ".join(product_terms[:12])

    facet_texts = {
        FACET_LABELS[0]: normalize_space(
            f"""
            Georgia and Southeast EV battery supply-chain evidence about who makes what, where, and at what scale.
            Strong matches mention real companies, facilities, plants, counties, products, services, capacities,
            production volumes, commissioning timelines, and OEM links.
            Grounding examples: companies {companies_sample}. counties {counties_sample}. OEMs {oems_sample}.
            Facility types {facilities_sample}. Products and services {products_sample}.
            """
        ),
        FACET_LABELS[1]: normalize_space(
            f"""
            Documents that explain supplier relationships, OEM relationships, tier structure, offtake agreements,
            joint ventures, customer-supplier dependencies, ecosystem mapping, and value-chain links for EV batteries.
            Grounding roles: {roles_sample}. Look for supplier, tier 1, tier 2, tier 3, customer, offtake, joint venture,
            OEM sourcing, and multi-tier dependencies.
            """
        ),
        FACET_LABELS[2]: normalize_space(
            f"""
            Documents about logistics, infrastructure, and risk affecting the Georgia or Southeast battery supply chain.
            Strong matches mention ports, rail, highways, hazmat, freight, bottlenecks, permitting, disruptions,
            Savannah, Brunswick, and county-level infrastructure constraints or resilience planning.
            Port anchors: {ports_sample}.
            Counties and regions: {counties_sample}.
            """
        ),
        FACET_LABELS[3]: normalize_space(
            f"""
            Documents about policy, incentives, workforce, site readiness, localization, IRA impacts, grants,
            state programs, and strategic opportunities to localize the EV battery ecosystem in Georgia and the Southeast.
            Use the GNEM supply-chain vision as guidance: {docx_reference[:1200]}
            """
        ),
    }

    global_reference = normalize_space(
        f"""
        {golden_summary}
        {docx_reference}
        Georgia automotive and EV grounding entities include companies {companies_sample},
        counties {counties_sample}, OEMs {oems_sample}, ports {ports_sample}, facility types {facilities_sample},
        supply-chain roles {roles_sample}, product terms {products_sample}, and value-chain terms {", ".join(value_chain_terms[:20])}.
        """
    )

    return GroundingDictionaries(
        companies=companies,
        company_aliases={name: company_aliases(name) for name in companies},
        counties=counties,
        county_aliases={name: county_aliases(name) for name in counties},
        county_regions=county_regions,
        oems=oems,
        oem_aliases={name: company_aliases(name) for name in oems},
        ports=ports,
        port_aliases={name: company_aliases(name) for name in ports},
        facility_types=facility_types,
        supply_chain_roles=supply_chain_roles,
        product_terms=product_terms,
        value_chain_terms=value_chain_terms,
        docx_reference=docx_reference,
        global_reference=global_reference,
        facet_texts=facet_texts,
    )


def grounding_summary_payload(grounding: GroundingDictionaries) -> dict[str, Any]:
    return {
        "company_count": len(grounding.companies),
        "county_count": len(grounding.counties),
        "oem_count": len(grounding.oems),
        "port_count": len(grounding.ports),
        "facility_type_count": len(grounding.facility_types),
        "supply_chain_role_count": len(grounding.supply_chain_roles),
        "product_term_count": len(grounding.product_terms),
        "value_chain_term_count": len(grounding.value_chain_terms),
        "company_examples": grounding.companies[:15],
        "county_examples": grounding.counties[:15],
        "oem_examples": grounding.oems[:10],
        "port_examples": grounding.ports[:8],
        "facet_labels": list(grounding.facet_texts.keys()),
    }


def probable_document(row: dict) -> bool:
    url = as_text(row.get("URL"))
    file_type = as_text(row.get("File_Type")).lower()
    file_ext = as_text(row.get("File_Ext_Guess")).lower()
    filename = as_text(row.get("Filename")).lower()
    if file_ext in KNOWN_DOCUMENT_EXTENSIONS:
        return True
    if file_type in {"pdf", "html", "htm", "txt"}:
        return True
    if any(filename.endswith(ext) for ext in KNOWN_DOCUMENT_EXTENSIONS):
        return True
    path = urlparse(url).path.lower()
    if any(path.endswith(ext) for ext in KNOWN_DOCUMENT_EXTENSIONS):
        return True
    joined = normalize_space(
        " ".join(
            [
                as_text(row.get("Title")),
                as_text(row.get("Content_Snippet")),
                as_text(row.get("Description")),
                as_text(row.get("Query")),
                filename_to_text(filename),
            ]
        )
    ).lower()
    hints = DOCUMENT_QUALITY_KEYWORDS + DIRECT_USECASE_HINTS + ["webinar", "presentation", "fact sheet", "case study"]
    return any(hint in joined for hint in hints)


def build_local_document_index(local_dirs: list[Path]) -> dict[str, str]:
    index: dict[str, str] = {}
    for root in local_dirs:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in KNOWN_DOCUMENT_EXTENSIONS:
                continue
            resolved = str(path.resolve())
            keys = {path.name.lower(), canonical_filename_key(path.name), normalize_key(path.stem)}
            for key in keys:
                if key and key not in index:
                    index[key] = resolved
    return index


def build_local_text_index(local_dirs: list[Path]) -> dict[str, str]:
    index: dict[str, str] = {}
    for root in local_dirs:
        if not root.exists():
            continue
        for path in root.rglob("*.txt"):
            resolved = str(path.resolve())
            keys = {path.name.lower(), canonical_filename_key(path.name), normalize_key(path.stem)}
            for key in keys:
                if key and key not in index:
                    index[key] = resolved
    return index


def candidate_local_names(row: dict) -> list[str]:
    names: list[str] = []
    filename = as_text(row.get("Filename"))
    if filename:
        names.append(filename)
    url = as_text(row.get("URL"))
    tail = os.path.basename(urlparse(url).path or "").strip()
    if tail:
        names.append(tail)
    title = as_text(row.get("Title"))
    if title:
        base = re.sub(r"[<>:\"/\\|?*]", "_", title)
        names.extend([base, f"{base}.pdf", f"{base}.html", f"{base}.txt"])
    unique: list[str] = []
    seen: set[str] = set()
    for item in names:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def infer_file_type(row: dict, resolved_path: str = "") -> str:
    file_type = as_text(row.get("File_Type")).upper()
    if file_type:
        return file_type
    path = resolved_path or as_text(row.get("Filename")) or urlparse(as_text(row.get("URL"))).path
    ext = Path(path).suffix.lower()
    if ext == ".pdf":
        return "PDF"
    if ext in {".html", ".htm"}:
        return "HTML"
    if ext == ".txt":
        return "TXT"
    return ext.replace(".", "").upper() if ext else ""


def resolve_document_paths(row: dict, local_doc_index: dict[str, str], local_text_index: dict[str, str]) -> dict[str, str]:
    resolved_file = ""
    for candidate in path_candidates_from_row(row):
        try:
            if candidate.exists() and candidate.is_file():
                resolved_file = str(candidate.resolve())
                break
        except Exception:
            continue

    if not resolved_file:
        for name in candidate_local_names(row):
            for key in [name.lower(), canonical_filename_key(name), normalize_key(Path(name).stem)]:
                hit = local_doc_index.get(key)
                if hit:
                    resolved_file = hit
                    break
            if resolved_file:
                break

    resolved_text = ""
    filename = as_text(row.get("Filename"))
    keys = [
        filename.lower(),
        canonical_filename_key(filename),
        normalize_key(Path(filename).stem if filename else ""),
        canonical_filename_key(Path(resolved_file).name) if resolved_file else "",
        normalize_key(Path(resolved_file).stem) if resolved_file else "",
    ]
    for key in keys:
        if not key:
            continue
        hit = local_text_index.get(key)
        if hit:
            resolved_text = hit
            break

    file_type = infer_file_type(row, resolved_file)
    return {
        "Resolved_File_Path": resolved_file,
        "Resolved_Text_Path": resolved_text,
        "Resolved_File_Type": file_type,
    }


def read_text_file(path: Path, max_chars: int) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        text = path.read_text(encoding="latin-1", errors="ignore")
    return normalize_space(text)[:max_chars]


def dedupe_texts(values: list[str], limit: int) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = normalize_key(value)
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(normalize_space(value))
        if len(unique) >= limit:
            break
    return unique


def html_candidate_score(*, text: str, link_text: str, paragraph_count: int, heading_count: int, list_items: int, tag_name: str, content_hint: bool) -> float:
    text_len = len(text)
    if text_len == 0:
        return 0.0
    link_density = len(link_text) / max(1.0, float(text_len))
    punctuation_hits = len(re.findall(r"[\.\!\?\:]", text))
    score = float(text_len)
    score += paragraph_count * 120.0
    score += heading_count * 60.0
    score += min(200.0, punctuation_hits * 4.0)
    score -= list_items * 8.0
    score -= link_density * 1500.0
    if tag_name in {"article", "main"}:
        score += 400.0
    if content_hint:
        score += 250.0
    return score


def select_best_html_text_with_bs4(soup: BeautifulSoup, max_chars: int) -> str:
    candidates: list[tuple[float, str]] = []
    selectors = [
        "article",
        "main",
        "[role='main']",
        "[id*=content]",
        "[id*=article]",
        "[id*=story]",
        "[class*=content]",
        "[class*=article]",
        "[class*=story]",
        "[class*=post]",
        "[class*=entry]",
        "[class*=body]",
        "[class*=report]",
        "[class*=rich-text]",
    ]
    seen_ids: set[int] = set()

    for selector in selectors:
        try:
            nodes = soup.select(selector)
        except Exception:
            nodes = []
        for node in nodes:
            node_id = id(node)
            if node_id in seen_ids:
                continue
            seen_ids.add(node_id)
            text = normalize_space(node.get_text(" ", strip=True))
            if len(text) < 250:
                continue
            classes = " ".join(node.get("class", []) or [])
            attrs_text = f"{node.get('id', '')} {classes}"
            link_text = normalize_space(" ".join(anchor.get_text(" ", strip=True) for anchor in node.find_all("a")))
            paragraph_count = len(node.find_all("p"))
            heading_count = len(node.find_all(["h1", "h2", "h3"]))
            list_items = len(node.find_all("li"))
            score = html_candidate_score(
                text=text,
                link_text=link_text,
                paragraph_count=paragraph_count,
                heading_count=heading_count,
                list_items=list_items,
                tag_name=getattr(node, "name", "") or "",
                content_hint=bool(HTML_CONTENT_HINT_RE.search(attrs_text)),
            )
            candidates.append((score, text[:max_chars]))

    body = soup.body or soup
    fallback_text = normalize_space(body.get_text(" ", strip=True))
    if len(fallback_text) >= 250:
        fallback_link_text = normalize_space(" ".join(anchor.get_text(" ", strip=True) for anchor in body.find_all("a")))
        candidates.append(
            (
                html_candidate_score(
                    text=fallback_text,
                    link_text=fallback_link_text,
                    paragraph_count=len(body.find_all("p")),
                    heading_count=len(body.find_all(["h1", "h2", "h3"])),
                    list_items=len(body.find_all("li")),
                    tag_name=getattr(body, "name", "") or "",
                    content_hint=True,
                ),
                fallback_text[:max_chars],
            )
        )

    if not candidates:
        return fallback_text[:max_chars]
    best_score, best_text = max(candidates, key=lambda item: item[0])
    if best_score <= 0 and fallback_text:
        return fallback_text[:max_chars]
    return best_text[:max_chars]


def select_best_html_text_without_bs4(raw: str, max_chars: int) -> str:
    stripped = re.sub(r"(?is)<(script|style|noscript|nav|footer|header|aside|form).*?>.*?</\1>", " ", raw)
    stripped = re.sub(
        r'(?is)<([a-z0-9]+)[^>]*(?:class|id)=["\'][^"\']*(nav|menu|footer|header|sidebar|breadcrumb|social|share|cookie|banner|subscribe|newsletter|pager)[^"\']*["\'][^>]*>.*?</\1>',
        " ",
        stripped,
    )
    block_candidates = re.findall(
        r"(?is)<(?:article|main|section|div)[^>]*(?:class|id)=['\"][^'\"]*(?:article|content|main|story|post|entry|body|report|copy|text)[^'\"]*['\"][^>]*>(.*?)</(?:article|main|section|div)>",
        stripped,
    )
    block_candidates.extend(re.findall(r"(?is)<article[^>]*>(.*?)</article>", stripped))
    block_candidates.extend(re.findall(r"(?is)<main[^>]*>(.*?)</main>", stripped))

    def clean_html_fragment(fragment: str) -> str:
        no_comments = re.sub(r"(?s)<!--.*?-->", " ", fragment)
        plain = re.sub(r"(?is)<[^>]+>", " ", no_comments)
        return normalize_space(html.unescape(plain))

    cleaned_candidates = [clean_html_fragment(fragment) for fragment in block_candidates]
    cleaned_candidates = [candidate for candidate in cleaned_candidates if len(candidate) >= 250]

    if cleaned_candidates:
        cleaned_candidates.sort(key=len, reverse=True)
        return cleaned_candidates[0][:max_chars]

    no_comments = re.sub(r"(?s)<!--.*?-->", " ", stripped)
    plain = re.sub(r"(?is)<[^>]+>", " ", no_comments)
    return normalize_space(html.unescape(plain))[:max_chars]


def extract_html_content(path: Path, max_chars: int) -> tuple[str, list[str], str]:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    publication_date = ""
    headings: list[str] = []
    if BeautifulSoup is not None:
        soup = BeautifulSoup(raw, "html.parser")
        for bad in soup(["script", "style", "noscript", "nav", "footer", "header", "aside", "form"]):
            bad.extract()
        for tag in soup.find_all(attrs={"class": HTML_BOILERPLATE_HINT_RE}):
            tag.extract()
        for tag in soup.find_all(attrs={"id": HTML_BOILERPLATE_HINT_RE}):
            tag.extract()
        for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            value = normalize_space(tag.get_text(" ", strip=True))
            if value:
                headings.append(value)
        for selector in [
            {"name": "meta", "attrs": {"property": "article:published_time"}},
            {"name": "meta", "attrs": {"property": "article:modified_time"}},
            {"name": "meta", "attrs": {"name": "pubdate"}},
            {"name": "meta", "attrs": {"name": "publish-date"}},
            {"name": "meta", "attrs": {"name": "date"}},
            {"name": "meta", "attrs": {"name": "article:published_time"}},
            {"name": "meta", "attrs": {"itemprop": "datePublished"}},
        ]:
            tag = soup.find(selector["name"], attrs=selector["attrs"])
            if tag and tag.get("content"):
                publication_date = normalize_space(tag.get("content"))
                break
        text = select_best_html_text_with_bs4(soup, max_chars=max_chars)
        return text[:max_chars], dedupe_texts(headings, 20), publication_date

    heading_hits = re.findall(r"(?is)<h[1-6][^>]*>(.*?)</h[1-6]>", raw)
    headings = [normalize_space(re.sub(r"(?is)<[^>]+>", " ", html.unescape(hit))) for hit in heading_hits]
    meta_match = re.search(
        r'(?is)<meta[^>]+(?:property|name|itemprop)=["\'](?:article:published_time|article:modified_time|pubdate|publish-date|date|datePublished)["\'][^>]+content=["\']([^"\']+)["\']',
        raw,
    )
    if meta_match:
        publication_date = normalize_space(meta_match.group(1))
    text = select_best_html_text_without_bs4(raw, max_chars=max_chars)
    return text[:max_chars], dedupe_texts(headings, 20), publication_date


def infer_headings_from_text(text: str) -> list[str]:
    lines = [normalize_space(line) for line in re.split(r"[\r\n]+", text) if normalize_space(line)]
    headings: list[str] = []
    for line in lines:
        if len(line) < 4 or len(line) > 120:
            continue
        if line.endswith(".") and len(line.split()) > 10:
            continue
        title_like = line.isupper() or bool(re.match(r"^(\d+(\.\d+)*|[IVX]+\.?)\s+", line)) or line.istitle()
        if ":" in line:
            title_like = True
        if title_like:
            headings.append(line)
    return dedupe_texts(headings, 20)


def extract_pdf_content(path: Path, max_chars: int) -> tuple[str, str, list[str], str]:
    pages: list[str] = []
    publication_date = ""
    if PdfReader is not None:
        reader = PdfReader(str(path))
        meta = getattr(reader, "metadata", {}) or {}
        publication_date = as_text(meta.get("/CreationDate") or meta.get("/ModDate"))
        for page in reader.pages:
            pages.append(normalize_space(page.extract_text() or ""))
            if sum(len(p) for p in pages) >= max_chars:
                break
    elif fitz is not None:
        doc = fitz.open(str(path))
        try:
            meta = doc.metadata or {}
            publication_date = as_text(meta.get("creationDate") or meta.get("modDate"))
            for idx in range(doc.page_count):
                pages.append(normalize_space(doc.load_page(idx).get_text("text") or ""))
                if sum(len(p) for p in pages) >= max_chars:
                    break
        finally:
            doc.close()
    full_text = normalize_space(" ".join(pages))[:max_chars]
    first_page = pages[0] if pages else ""
    headings = infer_headings_from_text("\n".join(pages[:3]))
    return full_text, first_page, headings, publication_date


def sample_page_indices(page_count: int) -> list[int]:
    if page_count <= 0:
        return []
    indices = {0}
    if page_count > 1:
        indices.add(1)
    if page_count > 2:
        indices.add(page_count // 2)
        indices.add(page_count - 1)
    return sorted(idx for idx in indices if 0 <= idx < page_count)


def pdf_page_count(path: Path) -> int:
    if PdfReader is not None:
        try:
            return len(PdfReader(str(path)).pages)
        except Exception:
            return 0
    if fitz is not None:
        try:
            doc = fitz.open(str(path))
            try:
                return doc.page_count
            finally:
                doc.close()
        except Exception:
            return 0
    return 0


def extract_pdf_page_entries(path: Path, page_indices: list[int] | None, max_chars: int) -> tuple[list[dict[str, Any]], str, int]:
    entries: list[dict[str, Any]] = []
    publication_date = ""
    char_budget = max(0, int(max_chars))

    def add_entry(page_idx: int, raw_text: str) -> None:
        nonlocal char_budget
        text = normalize_space(raw_text)
        if not text or char_budget <= 0:
            return
        clipped = text[:char_budget]
        char_budget -= len(clipped)
        entries.append({"page_number": page_idx + 1, "text": clipped, "label": f"page {page_idx + 1}"})

    if PdfReader is not None:
        reader = PdfReader(str(path))
        total_pages = len(reader.pages)
        meta = getattr(reader, "metadata", {}) or {}
        publication_date = as_text(meta.get("/CreationDate") or meta.get("/ModDate"))
        selected = list(range(total_pages)) if page_indices is None else sorted({idx for idx in page_indices if 0 <= idx < total_pages})
        for idx in selected:
            try:
                add_entry(idx, reader.pages[idx].extract_text() or "")
            except Exception:
                continue
            if char_budget <= 0:
                break
        return entries, publication_date, total_pages

    if fitz is not None:
        doc = fitz.open(str(path))
        try:
            total_pages = doc.page_count
            meta = doc.metadata or {}
            publication_date = as_text(meta.get("creationDate") or meta.get("modDate"))
            selected = list(range(total_pages)) if page_indices is None else sorted({idx for idx in page_indices if 0 <= idx < total_pages})
            for idx in selected:
                try:
                    add_entry(idx, doc.load_page(idx).get_text("text") or "")
                except Exception:
                    continue
                if char_budget <= 0:
                    break
        finally:
            doc.close()
        return entries, publication_date, total_pages

    return [], "", 0


def sample_text_entries(text: str, *, segment_chars: int = 2200) -> list[dict[str, Any]]:
    normalized = normalize_space(text)
    if not normalized:
        return []
    text_len = len(normalized)
    anchors = [
        ("first", 0),
        ("middle", max(0, (text_len // 2) - (segment_chars // 2))),
        ("late", max(0, text_len - segment_chars)),
    ]
    entries: list[dict[str, Any]] = []
    seen_starts: set[int] = set()
    for label, start in anchors:
        if start in seen_starts:
            continue
        seen_starts.add(start)
        snippet = normalize_space(normalized[start:start + segment_chars])
        if not snippet:
            continue
        entries.append({"page_number": None, "text": snippet, "label": label})
    return entries


def sequential_text_entries(text: str, *, segment_chars: int = 2200, max_segments: int = 12) -> list[dict[str, Any]]:
    normalized = normalize_space(text)
    if not normalized:
        return []
    entries: list[dict[str, Any]] = []
    for idx, start in enumerate(range(0, len(normalized), segment_chars), start=1):
        snippet = normalize_space(normalized[start:start + segment_chars])
        if not snippet:
            continue
        entries.append({"page_number": None, "text": snippet, "label": f"segment {idx}"})
        if len(entries) >= max_segments:
            break
    return entries


def build_document_content_profile(
    row: dict[str, Any],
    *,
    max_text_chars: int,
    card_level: str,
) -> dict[str, Any]:
    resolved_path = as_text(row.get("Resolved_File_Path")) or as_text(row.get("Acquired_File_Path"))
    resolved_text_path = as_text(row.get("Resolved_Text_Path"))
    file_type = infer_file_type(row, resolved_path)
    content_source = ""
    full_text = ""
    first_page_text = ""
    first_two_pages_text = ""
    headings: list[str] = []
    publication_date = ""
    sampled_entries: list[dict[str, Any]] = []
    extracted_entries: list[dict[str, Any]] = []
    total_pages = 0

    prefer_pdf = bool(resolved_path and Path(resolved_path).exists() and Path(resolved_path).suffix.lower() == ".pdf")

    if prefer_pdf:
        path = Path(resolved_path)
        page_count = pdf_page_count(path)
        all_page_indices = None if card_level == "enriched" else sample_page_indices(page_count)
        extracted_entries, publication_date, total_pages = extract_pdf_page_entries(path, all_page_indices, max_text_chars)
        first_page_text = extracted_entries[0]["text"] if extracted_entries else ""
        first_two_pages_text = normalize_space(" ".join(entry["text"] for entry in extracted_entries if int(entry.get("page_number") or 0) <= 2))
        sample_numbers = {idx + 1 for idx in sample_page_indices(total_pages)}
        sampled_entries = [entry for entry in extracted_entries if entry.get("page_number") in sample_numbers]
        headings = infer_headings_from_text("\n".join(entry["text"] for entry in extracted_entries[: min(4, len(extracted_entries))]))
        full_text = normalize_space(" ".join(entry["text"] for entry in extracted_entries))[:max_text_chars]
        content_source = "local_pdf_enriched" if card_level == "enriched" else "local_pdf_sampled"
    if not full_text and resolved_text_path and Path(resolved_text_path).exists():
        full_text = read_text_file(Path(resolved_text_path), max_text_chars)
        first_page_text = full_text[:2500]
        first_two_pages_text = full_text[:5000]
        sampled_entries = sample_text_entries(full_text)
        extracted_entries = sequential_text_entries(full_text) if card_level == "enriched" else list(sampled_entries)
        headings = infer_headings_from_text(full_text[:5000])
        content_source = "local_text"
    elif resolved_path and Path(resolved_path).exists():
        path = Path(resolved_path)
        if path.suffix.lower() in {".html", ".htm"}:
            full_text, headings, publication_date = extract_html_content(path, max_text_chars)
            first_page_text = full_text[:2500]
            first_two_pages_text = full_text[:5000]
            sampled_entries = sample_text_entries(full_text)
            extracted_entries = sequential_text_entries(full_text) if card_level == "enriched" else list(sampled_entries)
            content_source = "local_html"
        elif path.suffix.lower() in {".txt", ".md"}:
            full_text = read_text_file(path, max_text_chars)
            first_page_text = full_text[:2500]
            first_two_pages_text = full_text[:5000]
            sampled_entries = sample_text_entries(full_text)
            extracted_entries = sequential_text_entries(full_text) if card_level == "enriched" else list(sampled_entries)
            headings = infer_headings_from_text(full_text[:5000])
            content_source = "local_text"
    if not full_text:
        full_text = as_text(row.get("Raw_Content_Text"))[:max_text_chars]
        first_page_text = full_text[:2500]
        first_two_pages_text = full_text[:5000]
        sampled_entries = sample_text_entries(full_text)
        extracted_entries = sequential_text_entries(full_text) if card_level == "enriched" else list(sampled_entries)
        content_source = "metadata_raw_content" if full_text else content_source
    if not full_text:
        fallback = normalize_space(
            " ".join(
                [
                    as_text(row.get("Title")),
                    as_text(row.get("Description")),
                    as_text(row.get("Content_Snippet")),
                    as_text(row.get("Query")),
                ]
            )
        )
        full_text = fallback[:max_text_chars]
        first_page_text = fallback[:2500]
        first_two_pages_text = fallback[:5000]
        sampled_entries = sample_text_entries(full_text)
        extracted_entries = sequential_text_entries(full_text) if card_level == "enriched" else list(sampled_entries)
        content_source = "metadata_summary" if full_text else "empty"

    if not sampled_entries:
        sampled_entries = extracted_entries[:]
    return {
        "file_type": file_type,
        "content_source": content_source or "empty",
        "full_text": full_text,
        "first_page_text": first_page_text,
        "first_two_pages_text": first_two_pages_text,
        "headings": dedupe_texts(headings, 20),
        "publication_date": publication_date,
        "sampled_entries": sampled_entries,
        "extracted_entries": extracted_entries,
        "total_pages": total_pages,
    }


def extract_publication_date(row: dict, text: str, fallback: str = "") -> str:
    for field in ["Publication_Date_Metadata", "Date_Created", "Date_Modified", "Date_Accessed"]:
        value = as_text(row.get(field))
        if value:
            return value
    if fallback:
        return fallback
    search_text = text[:2500]
    patterns = [
        r"\b(?:Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,2},\s+20\d{2}\b",
        r"\b20\d{2}-\d{2}-\d{2}\b",
        r"\b\d{1,2}/\d{1,2}/20\d{2}\b",
        r"\b20\d{2}\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, search_text, flags=re.IGNORECASE)
        if match:
            return normalize_space(match.group(0))
    return ""


def build_metadata_summary(row: dict) -> str:
    parts = [
        f"title: {as_text(row.get('Title'))}",
        f"query: {as_text(row.get('Query'))}",
        f"domain: {as_text(row.get('Source_Domain'))}",
        f"description: {as_text(row.get('Description'))}",
        f"content snippet: {as_text(row.get('Content_Snippet'))}",
    ]
    if as_text(row.get("Metadata_Score")):
        parts.append(f"metadata score: {as_text(row.get('Metadata_Score'))}")
    return normalize_space(" ".join(part for part in parts if part and not part.endswith(":")))


def summarize_first_page(text: str, max_sentences: int = 4, max_chars: int = 750) -> str:
    txt = normalize_space(text)
    if not txt:
        return ""
    sentences = re.split(r"(?<=[\.\!\?])\s+", txt)
    if len(sentences) <= max_sentences:
        return txt[:max_chars]
    pool = VALUE_CHAIN_KEYWORDS + REGION_KEYWORDS + POLICY_LOGISTICS_KEYWORDS + [kw for kws in DIGITAL_TWIN_SIGNAL_KEYWORDS.values() for kw in kws]
    scored: list[tuple[int, float, str]] = []
    for idx, sentence in enumerate(sentences):
        lowered = sentence.lower()
        score = sum(1.0 for keyword in pool if keyword in lowered)
        score += 0.6 * len(re.findall(r"\b\d+(?:\.\d+)?\b", lowered))
        score += 0.6 * specificity_signal_count(lowered)
        if "georgia" in lowered or "southeast" in lowered:
            score += 1.0
        scored.append((idx, score, sentence))
    picked = {0}
    for idx, _, _ in sorted(scored, key=lambda item: item[1], reverse=True):
        picked.add(idx)
        if len(picked) >= max_sentences:
            break
    return " ".join(sentences[i] for i in sorted(picked) if i < len(sentences))[:max_chars]


def tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9][a-z0-9\-\+\.]*", text.lower())


def hashed_counter(text: str, dims: int = 512) -> Counter[int]:
    tokens = tokenize(text)
    counter: Counter[int] = Counter()
    grams = list(tokens)
    grams.extend(f"{tokens[i]}__{tokens[i + 1]}" for i in range(len(tokens) - 1))
    for gram in grams:
        digest = hashlib.sha1(gram.encode("utf-8")).hexdigest()
        idx = int(digest[:8], 16) % dims
        counter[idx] += 1
    return counter


def hashed_similarity(text_a: str, text_b: str) -> float:
    ca = hashed_counter(text_a)
    cb = hashed_counter(text_b)
    if not ca or not cb:
        return 0.0
    dot = sum(value * cb.get(key, 0) for key, value in ca.items())
    na = math.sqrt(sum(value * value for value in ca.values()))
    nb = math.sqrt(sum(value * value for value in cb.values()))
    if na == 0 or nb == 0:
        return 0.0
    return max(0.0, min(1.0, dot / (na * nb)))


def similarity_reference_texts(grounding: GroundingDictionaries, query_text: str = "") -> dict[str, str]:
    refs = {"global": grounding.global_reference}
    for label, ref_text in grounding.facet_texts.items():
        refs[f"facet::{label}"] = ref_text
    cleaned_query = normalize_space(query_text)
    if cleaned_query:
        refs["query"] = cleaned_query
    return refs


def score_texts_against_grounding(
    texts: list[str],
    grounding: GroundingDictionaries,
    *,
    query_text: str = "",
    similarity_runtime: Any | None = None,
) -> tuple[list[dict[str, float]], str, int]:
    normalized_texts = [normalize_space(text) for text in texts]
    refs = similarity_reference_texts(grounding, query_text=query_text)
    if similarity_runtime is not None and getattr(similarity_runtime, "enabled", False):
        try:
            scores = similarity_runtime.score_texts_to_references(normalized_texts, refs)
            return scores, as_text(getattr(similarity_runtime, "backend_name", "dense_embeddings")), int(getattr(similarity_runtime, "dimension", 0) or 0)
        except Exception:
            pass

    fallback_rows: list[dict[str, float]] = []
    cleaned_query = normalize_space(query_text)
    for text in normalized_texts:
        global_score = round(hashed_similarity(text, grounding.global_reference) * 100.0, 2)
        row_scores: dict[str, float] = {
            "global": global_score,
            "query": round(hashed_similarity(text, cleaned_query) * 100.0, 2) if cleaned_query else global_score,
        }
        for label, ref_text in grounding.facet_texts.items():
            row_scores[f"facet::{label}"] = round(hashed_similarity(text, ref_text) * 100.0, 2)
        fallback_rows.append(row_scores)
    return fallback_rows, "hashing_bow", 512


def split_text_into_chunks(text: str, chunk_words: int = 180, overlap_words: int = 45, max_chunks: int = 60) -> list[str]:
    words = text.split()
    if not words:
        return []
    chunks: list[str] = []
    step = max(1, chunk_words - overlap_words)
    for start in range(0, len(words), step):
        chunk = " ".join(words[start:start + chunk_words]).strip()
        if chunk:
            chunks.append(chunk)
        if len(chunks) >= max_chunks or start + chunk_words >= len(words):
            break
    return chunks


def build_chunk_candidates(entries_or_text: str | list[dict[str, Any]], *, max_chunks_per_entry: int = 4) -> list[dict[str, Any]]:
    if isinstance(entries_or_text, str):
        entries = [{"text": entries_or_text, "page_number": None, "label": "body"}]
    else:
        entries = entries_or_text

    chunk_candidates: list[dict[str, Any]] = []
    for entry in entries:
        text = as_text(entry.get("text"))
        if not text:
            continue
        chunks = split_text_into_chunks(text, max_chunks=max_chunks_per_entry)
        if not chunks:
            chunks = [text[:1800]]
        for idx, chunk in enumerate(chunks[:max_chunks_per_entry], start=1):
            chunk_candidates.append(
                {
                    "text": chunk,
                    "page_number": entry.get("page_number"),
                    "label": as_text(entry.get("label")),
                    "chunk_index": idx,
                }
            )
    return chunk_candidates


def score_chunks(
    entries_or_text: str | list[dict[str, Any]],
    grounding: GroundingDictionaries,
    *,
    top_k: int = 3,
    max_chunks_per_entry: int = 4,
    query_text: str = "",
    similarity_runtime: Any | None = None,
) -> list[dict[str, Any]]:
    candidates = build_chunk_candidates(entries_or_text, max_chunks_per_entry=max_chunks_per_entry)
    if not candidates:
        return []
    similarity_rows, backend_name, embedding_dimension = score_texts_against_grounding(
        [as_text(candidate.get("text")) for candidate in candidates],
        grounding,
        query_text=query_text,
        similarity_runtime=similarity_runtime,
    )
    scored: list[dict[str, Any]] = []
    for candidate, similarity_row in zip(candidates, similarity_rows):
        chunk = as_text(candidate.get("text"))
        global_similarity = float(similarity_row.get("global", 0.0))
        query_similarity = float(similarity_row.get("query", global_similarity))
        facet_scores = {
            label: float(similarity_row.get(f"facet::{label}", 0.0))
            for label in grounding.facet_texts
        }
        best_facet = max(facet_scores.items(), key=lambda item: item[1]) if facet_scores else ("", 0.0)
        top2 = sorted(facet_scores.values(), reverse=True)[:2]
        top2_avg = sum(top2) / len(top2) if top2 else 0.0
        score = 0.30 * global_similarity + 0.30 * float(best_facet[1]) + 0.25 * query_similarity + 0.15 * top2_avg
        candidate.update(
            {
                "embedding_backend": backend_name,
                "embedding_dimension": embedding_dimension,
                "global_similarity": round(global_similarity, 2),
                "query_similarity": round(query_similarity, 2),
                "best_facet": best_facet[0],
                "best_facet_score": round(float(best_facet[1]), 2),
                "top2_facet_average": round(top2_avg, 2),
                "combined_score": round(score, 2),
            }
        )
        scored.append(candidate)
    scored.sort(key=lambda item: item["combined_score"], reverse=True)
    return scored[:top_k]


def match_aliases(
    text: str,
    alias_map: dict[str, list[str]],
    max_hits: int = 20,
    *,
    alias_kind: str = "generic",
) -> list[str]:
    normalized_text = f" {normalize_key(text)} "
    hits: list[str] = []
    for name, aliases in alias_map.items():
        matched = False
        for alias in aliases:
            if not alias:
                continue
            if alias_kind == "county" and "county" not in alias:
                if f" {alias} county " in normalized_text or f" {alias} georgia " in normalized_text:
                    matched = True
                    break
                continue
            if f" {alias} " in normalized_text:
                matched = True
                break
        if matched:
            hits.append(name)
            if len(hits) >= max_hits:
                break
    return hits


def extract_facilities(text: str) -> list[str]:
    pattern = re.compile(
        r"\b([A-Z][A-Za-z0-9&,\- ]{2,80}?\s(?:plant|facility|site|factory|campus|metaplant|hub|center|centre|port|terminal))\b"
    )
    return dedupe_texts([normalize_space(hit) for hit in pattern.findall(text)], 15)


def extract_capacities(text: str) -> list[str]:
    pattern = re.compile(r"\b\d+(?:\.\d+)?\s?(?:gwh|mwh|kwh|mtpa|tons per year|tons|tonnes|million|billion|%)\b", re.I)
    return dedupe_texts([normalize_space(hit) for hit in pattern.findall(text)], 15)


def extract_dates(text: str) -> list[str]:
    patterns = [
        r"\b(?:Jan|January|Feb|February|Mar|March|Apr|April|May|Jun|June|Jul|July|Aug|August|Sep|Sept|September|Oct|October|Nov|November|Dec|December)\s+\d{1,2},\s+20\d{2}\b",
        r"\b20\d{2}-\d{2}-\d{2}\b",
        r"\b\d{1,2}/\d{1,2}/20\d{2}\b",
        r"\b20\d{2}\b",
    ]
    values: list[str] = []
    for pattern in patterns:
        values.extend(re.findall(pattern, text, flags=re.I))
    return dedupe_texts([normalize_space(v) for v in values], 15)


def extract_entities(text: str, grounding: GroundingDictionaries) -> dict[str, list[str]]:
    companies = match_aliases(text, grounding.company_aliases, max_hits=20)
    counties = match_aliases(text, grounding.county_aliases, max_hits=15, alias_kind="county")
    oems = match_aliases(text, grounding.oem_aliases, max_hits=15)
    ports = match_aliases(text, grounding.port_aliases, max_hits=10)
    facilities = extract_facilities(text)
    capacities = extract_capacities(text)
    dates = extract_dates(text)
    value_chain_terms = dedupe_texts([term for term in grounding.value_chain_terms if term and term.lower() in text.lower()], 20)
    return {
        "companies": companies,
        "counties": counties,
        "oems": oems,
        "ports": ports,
        "facilities": facilities,
        "capacities": capacities,
        "dates": dates,
        "value_chain_terms": value_chain_terms,
    }


def join_top_chunks(chunks: list[dict[str, Any]]) -> str:
    formatted = []
    for idx, chunk in enumerate(chunks, start=1):
        snippet = normalize_space(as_text(chunk.get("text")))[:500]
        page_number = chunk.get("page_number")
        location = f"p.{page_number}" if page_number else as_text(chunk.get("label"))
        prefix = f"[{idx} {location}]" if location else f"[{idx}]"
        formatted.append(f"{prefix} {snippet}")
    return " || ".join(formatted)


def summarize_entry_set(entries: list[dict[str, Any]], *, max_entries: int = 4, max_chars: int = 1400) -> str:
    parts: list[str] = []
    for entry in entries[:max_entries]:
        label = f"p.{entry.get('page_number')}" if entry.get("page_number") else as_text(entry.get("label")).title()
        summary = summarize_first_page(as_text(entry.get("text")), max_sentences=2, max_chars=240)
        if summary:
            parts.append(f"{label}: {summary}" if label else summary)
    return " || ".join(parts)[:max_chars]


def evidence_page_numbers(chunks: list[dict[str, Any]]) -> list[int]:
    pages: list[int] = []
    for chunk in chunks:
        try:
            page_number = int(chunk.get("page_number"))
        except Exception:
            continue
        if page_number not in pages:
            pages.append(page_number)
    return pages


def build_document_card(
    row: dict,
    grounding: GroundingDictionaries,
    max_text_chars: int = 60000,
    *,
    card_level: str = "lightweight",
    similarity_runtime: Any | None = None,
) -> dict[str, Any]:
    profile = build_document_content_profile(row, max_text_chars=max_text_chars, card_level=card_level)
    resolved_path = as_text(row.get("Resolved_File_Path")) or as_text(row.get("Acquired_File_Path"))
    metadata_summary = build_metadata_summary(row)
    first_page_summary = summarize_first_page(profile["first_page_text"])
    first_two_pages_summary = summarize_first_page(profile["first_two_pages_text"], max_sentences=5, max_chars=950)
    sampled_page_summary = summarize_entry_set(profile["sampled_entries"], max_entries=4, max_chars=1500)
    publication_date = extract_publication_date(row, profile["full_text"], profile["publication_date"])
    headings = dedupe_texts(profile["headings"], 20)

    entity_text = normalize_space(
        " ".join(
            [
                as_text(row.get("Title")),
                metadata_summary,
                profile["first_page_text"],
                profile["first_two_pages_text"],
                " ".join(as_text(entry.get("text")) for entry in profile["sampled_entries"]),
                profile["full_text"][:18000],
            ]
        )
    )
    entities = extract_entities(entity_text, grounding)
    top_k = 5 if card_level == "enriched" else 3
    max_chunks_per_entry = 6 if card_level == "enriched" else 3
    chunk_source: str | list[dict[str, Any]]
    if card_level == "enriched" and profile["extracted_entries"]:
        chunk_source = profile["extracted_entries"]
    elif profile["sampled_entries"]:
        chunk_source = profile["sampled_entries"]
    else:
        chunk_source = profile["full_text"] or profile["first_page_text"] or metadata_summary
    top_chunks = score_chunks(
        chunk_source,
        grounding,
        top_k=top_k,
        max_chunks_per_entry=max_chunks_per_entry,
        query_text=as_text(row.get("Query")),
        similarity_runtime=similarity_runtime,
    )
    evidence_snippets = [normalize_space(as_text(chunk.get("text")))[:500] for chunk in top_chunks]
    evidence_pages = evidence_page_numbers(top_chunks)
    sampled_page_numbers = evidence_page_numbers(profile["sampled_entries"])
    late_page_evidence = any(page > 2 for page in evidence_pages) if evidence_pages else False

    card_text = normalize_space(
        " ".join(
            [
                as_text(row.get("Title")),
                metadata_summary,
                first_page_summary,
                first_two_pages_summary,
                sampled_page_summary,
                " ".join(headings[:12]),
                " ".join(evidence_snippets),
                " ".join(entities["companies"]),
                " ".join(entities["counties"]),
                " ".join(entities["oems"]),
                " ".join(entities["ports"]),
                " ".join(entities["facilities"]),
                " ".join(entities["capacities"]),
                " ".join(entities["dates"]),
                " ".join(entities["value_chain_terms"]),
            ]
        )
    )

    document_source = resolved_path or as_text(row.get("URL")) or as_text(row.get("Existing_Path"))
    return {
        "Card_Level": card_level,
        "Title": as_text(row.get("Title")) or filename_to_text(as_text(row.get("Filename"))),
        "Source_Domain": as_text(row.get("Source_Domain")) or get_domain(as_text(row.get("URL"))),
        "Document_Source": document_source,
        "Document_URL": as_text(row.get("URL")),
        "Document_File_Path": resolved_path,
        "File_Type": profile["file_type"],
        "Publication_Date": publication_date,
        "Metadata_Summary": metadata_summary[:2000],
        "FirstPage_Summary": first_page_summary[:1200],
        "FirstTwoPages_Summary": first_two_pages_summary[:1200],
        "Sampled_Page_Summaries": sampled_page_summary,
        "Sampled_Page_Numbers": ", ".join(str(page) for page in sampled_page_numbers),
        "Headings_TOC": " | ".join(headings[:15]),
        "Top_Relevant_Chunks": join_top_chunks(top_chunks),
        "Top_Evidence_Snippets": " || ".join(f"[{idx}] {snippet}" for idx, snippet in enumerate(evidence_snippets, start=1)),
        "Evidence_Page_Numbers": ", ".join(str(page) for page in evidence_pages),
        "Extracted_Companies": "; ".join(entities["companies"]),
        "Extracted_Counties": "; ".join(entities["counties"]),
        "Extracted_OEMs": "; ".join(entities["oems"]),
        "Extracted_Ports": "; ".join(entities["ports"]),
        "Extracted_Facilities": "; ".join(entities["facilities"]),
        "Extracted_Capacities": "; ".join(entities["capacities"]),
        "Extracted_Dates": "; ".join(entities["dates"]),
        "Extracted_Value_Chain_Terms": "; ".join(entities["value_chain_terms"]),
        "Grounded_Company_Count": len(entities["companies"]),
        "Grounded_County_Count": len(entities["counties"]),
        "Grounded_OEM_Count": len(entities["oems"]),
        "Grounded_Port_Count": len(entities["ports"]),
        "Grounded_Facility_Count": len(entities["facilities"]),
        "Grounded_Capacity_Count": len(entities["capacities"]),
        "Grounded_Date_Count": len(entities["dates"]),
        "Grounded_ValueChain_Count": len(entities["value_chain_terms"]),
        "Document_Content_Source": profile["content_source"],
        "Document_Content_Chars": len(profile["full_text"]),
        "Document_Total_Pages": int(profile["total_pages"]),
        "Late_Page_Evidence_Flag": 1 if late_page_evidence else 0,
        "_full_text": profile["full_text"],
        "_first_page_text": profile["first_page_text"],
        "_first_two_pages_text": profile["first_two_pages_text"],
        "_sampled_entries": profile["sampled_entries"],
        "_extracted_entries": profile["extracted_entries"],
        "_card_text": card_text,
        "_top_chunks": top_chunks,
        "_entities": entities,
    }


def lexical_entity_score(card: dict[str, Any], grounding: GroundingDictionaries) -> dict[str, Any]:
    entities = card.get("_entities") or {}
    card_text = as_text(card.get("_card_text"))
    relationship_hits, _ = keyword_hits(card_text, ["supplier", "tier 1", "tier 2", "tier 3", "customer", "offtake", "joint venture"])
    logistics_hits, _ = keyword_hits(card_text, ["logistics", "port", "savannah", "brunswick", "rail", "hazmat", "freight", "intermodal"])
    policy_hits, _ = keyword_hits(card_text, POLICY_LOGISTICS_KEYWORDS + ["localization", "site readiness", "workforce", "grant", "incentive"])

    company_count = len(entities.get("companies", []))
    county_count = len(entities.get("counties", []))
    oem_count = len(entities.get("oems", []))
    port_count = len(entities.get("ports", []))
    facility_count = len(entities.get("facilities", []))
    capacity_count = len(entities.get("capacities", []))
    date_count = len(entities.get("dates", []))
    value_chain_count = len(entities.get("value_chain_terms", []))

    score = (
        min(18.0, 6.0 * company_count)
        + min(12.0, 6.0 * county_count)
        + min(10.0, 5.0 * oem_count)
        + min(8.0, 4.0 * port_count)
        + min(12.0, 4.0 * facility_count)
        + min(16.0, 8.0 * capacity_count)
        + min(8.0, 2.0 * date_count)
        + min(8.0, 2.0 * value_chain_count)
        + min(4.0, 2.0 * relationship_hits)
        + min(4.0, 2.0 * logistics_hits)
        + min(4.0, 2.0 * policy_hits)
    )
    return {
        "Lexical_Entity_Score": round(clamp_score(score), 2),
        "Lexical_Company_Hits": company_count,
        "Lexical_County_Hits": county_count,
        "Lexical_OEM_Hits": oem_count,
        "Lexical_Port_Hits": port_count,
        "Lexical_Facility_Hits": facility_count,
        "Lexical_Capacity_Hits": capacity_count,
        "Lexical_Date_Hits": date_count,
        "Lexical_ValueChain_Hits": value_chain_count,
        "Lexical_Relationship_Hits": relationship_hits,
        "Lexical_Logistics_Hits": logistics_hits,
        "Lexical_Policy_Hits": policy_hits,
    }


def score_document_card(card: dict[str, Any], grounding: GroundingDictionaries, metadata_score: float) -> dict[str, Any]:
    card_text = as_text(card.get("_card_text"))
    query_text = as_text(card.get("Query"))
    similarity_rows, backend_name, embedding_dimension = score_texts_against_grounding(
        [card_text],
        grounding,
        query_text=query_text,
        similarity_runtime=card.get("_similarity_runtime"),
    )
    similarity_row = similarity_rows[0] if similarity_rows else {}
    global_similarity = float(similarity_row.get("global", 0.0))
    query_similarity = float(similarity_row.get("query", global_similarity))
    facet_scores = {
        label: float(similarity_row.get(f"facet::{label}", 0.0))
        for label in grounding.facet_texts
    }
    top2 = sorted(facet_scores.values(), reverse=True)[:2]
    top2_avg = sum(top2) / len(top2) if top2 else 0.0
    facet_coverage = 100.0 * (sum(1 for value in facet_scores.values() if value >= 55.0) / max(1, len(facet_scores)))
    semantic_embedding_score = round(0.30 * global_similarity + 0.25 * top2_avg + 0.20 * facet_coverage + 0.25 * query_similarity, 2)
    lexical_scores = lexical_entity_score(card, grounding)
    hybrid_score = round(0.55 * semantic_embedding_score + 0.45 * float(lexical_scores["Lexical_Entity_Score"]), 2)

    cat_count, signal_hits_total, cat_names = category_hits(card_text)
    question_hits = question_coverage_count(card_text)
    specificity_hits = specificity_signal_count(card_text)
    region_hits, _ = keyword_hits(card_text, REGION_KEYWORDS)
    policy_hits, _ = keyword_hits(card_text, POLICY_LOGISTICS_KEYWORDS)
    doc_quality_hits, _ = keyword_hits(card_text, DOCUMENT_QUALITY_KEYWORDS)
    negative_hits, _ = keyword_hits(card_text, NEGATIVE_KEYWORDS)
    marketing_hits, _ = keyword_hits(card_text, MARKETING_KEYWORDS)
    news_hits, _ = keyword_hits(card_text, GENERIC_NEWS_KEYWORDS)
    research_hits, _ = keyword_hits(card_text, RESEARCH_ONLY_KEYWORDS)

    grounded_entity_score = min(
        100.0,
        18.0 * float(card.get("Grounded_Company_Count", 0))
        + 12.0 * float(card.get("Grounded_County_Count", 0))
        + 14.0 * float(card.get("Grounded_OEM_Count", 0))
        + 10.0 * float(card.get("Grounded_Facility_Count", 0))
        + 18.0 * float(card.get("Grounded_Capacity_Count", 0))
        + 5.0 * min(4.0, float(card.get("Grounded_Date_Count", 0))),
    )
    chunk_scores = [float(chunk.get("combined_score", 0.0)) for chunk in card.get("_top_chunks", [])]
    top_chunk_score = sum(chunk_scores) / len(chunk_scores) if chunk_scores else 0.0
    chunk_query_scores = [float(chunk.get("query_similarity", 0.0)) for chunk in card.get("_top_chunks", [])]
    top_chunk_query_score = sum(chunk_query_scores) / len(chunk_query_scores) if chunk_query_scores else 0.0
    embedding_rerank_score = round(0.45 * query_similarity + 0.35 * semantic_embedding_score + 0.20 * top_chunk_query_score, 2)
    source_pct = min(100.0, source_credibility_score(as_text(card.get("Source_Domain"))) / 15.0 * 100.0)
    content_source = as_text(card.get("Document_Content_Source"))
    source_backed_bonus = 12.0 if content_source.startswith("local_pdf") else (
        10.0 if content_source in {"local_html", "local_text"} else 0.0
    )
    evidence_depth_score = min(
        100.0,
        (5.0 if as_text(card.get("Metadata_Summary")) else 0.0)
        + (18.0 if as_text(card.get("FirstPage_Summary")) else 0.0)
        + (15.0 if as_text(card.get("FirstTwoPages_Summary")) else 0.0)
        + (15.0 if as_text(card.get("Sampled_Page_Summaries")) else 0.0)
        + (10.0 if as_text(card.get("Headings_TOC")) else 0.0)
        + (20.0 if as_text(card.get("Top_Relevant_Chunks")) else 0.0)
        + (15.0 if as_text(card.get("Evidence_Page_Numbers")) else 0.0)
        + source_backed_bonus
        + (5.0 if as_text(card.get("Publication_Date")) else 0.0),
    )
    signal_score = min(100.0, (cat_count / 4.0) * 100.0)
    question_score = min(100.0, (question_hits / 5.0) * 100.0)
    doc_quality_score = min(100.0, (doc_quality_hits / 5.0) * 100.0)
    region_score = 100.0 if region_hits > 0 or int(card.get("Grounded_County_Count", 0)) > 0 else 0.0
    late_page_score = 100.0 if int(card.get("Late_Page_Evidence_Flag", 0)) == 1 else 0.0
    penalty = min(45.0, negative_hits * 5.0 + marketing_hits * 6.0 + news_hits * 4.0 + (6.0 if research_hits > 0 and grounded_entity_score < 25 else 0.0))
    heuristic_score = (
        0.20 * signal_score
        + 0.18 * question_score
        + 0.22 * grounded_entity_score
        + 0.14 * top_chunk_score
        + 0.10 * region_score
        + 0.07 * source_pct
        + 0.03 * evidence_depth_score
        + 0.02 * late_page_score
        + 0.04 * float(metadata_score)
        + 0.04 * doc_quality_score
        - penalty
    )
    heuristic_score = round(clamp_score(heuristic_score), 2)

    return {
        **lexical_scores,
        "Heuristic_Score": heuristic_score,
        "Heuristic_Signal_Count": cat_count,
        "Heuristic_Signal_Hits": signal_hits_total,
        "Heuristic_Signal_Categories": ", ".join(cat_names),
        "Heuristic_Question_Coverage": question_hits,
        "Heuristic_Specificity_Hits": specificity_hits,
        "Heuristic_Grounded_Entity_Score": round(grounded_entity_score, 2),
        "Heuristic_Top_Chunk_Score": round(top_chunk_score, 2),
        "Heuristic_Region_Score": round(region_score, 2),
        "Heuristic_Source_Credibility_Pct": round(source_pct, 2),
        "Heuristic_Evidence_Depth_Score": round(evidence_depth_score, 2),
        "Heuristic_Late_Page_Evidence_Score": round(late_page_score, 2),
        "Heuristic_DocQuality_Hits": doc_quality_hits,
        "Heuristic_Policy_Hits": policy_hits,
        "Heuristic_Negative_Hits": negative_hits,
        "Heuristic_Marketing_Hits": marketing_hits,
        "Heuristic_GenericNews_Hits": news_hits,
        "Heuristic_ResearchOnly_Hits": research_hits,
        "Heuristic_Penalty": round(penalty, 2),
        "Embedding_Backend": backend_name,
        "Embedding_Model": backend_name,
        "Embedding_Dimension": embedding_dimension or 512,
        "Embedding_Global_Similarity": round(global_similarity, 2),
        "Embedding_Query_Similarity": round(query_similarity, 2),
        "Embedding_Top2_Facet_Average": round(top2_avg, 2),
        "Embedding_Facet_Coverage": round(facet_coverage, 2),
        "Embedding_Top_Chunk_Query_Score": round(top_chunk_query_score, 2),
        "Embedding_Rerank_Score": embedding_rerank_score,
        "Semantic_Embedding_Score": semantic_embedding_score,
        "Embedding_Score": semantic_embedding_score,
        "Hybrid_Score": hybrid_score,
        "Embedding_Facet_WhoWhereScale": round(facet_scores.get(FACET_LABELS[0], 0.0), 2),
        "Embedding_Facet_SupplierTier": round(facet_scores.get(FACET_LABELS[1], 0.0), 2),
        "Embedding_Facet_LogisticsRisk": round(facet_scores.get(FACET_LABELS[2], 0.0), 2),
        "Embedding_Facet_PolicyLocalization": round(facet_scores.get(FACET_LABELS[3], 0.0), 2),
    }


def classify_document_card(card: dict[str, Any], score_row: dict[str, Any]) -> dict[str, Any]:
    card_text = as_text(card.get("_card_text"))
    policy_hits, _ = keyword_hits(card_text, POLICY_LOGISTICS_KEYWORDS + ["localization", "site readiness", "workforce", "grant"])
    logistics_hits, _ = keyword_hits(card_text, ["logistics", "port", "rail", "freight", "savannah", "brunswick", "hazmat", "corridor", "highway"])
    relationship_hits, _ = keyword_hits(card_text, ["supplier", "tier 1", "tier 2", "tier 3", "customer", "offtake", "joint venture", "partner"])
    recycling_hits, _ = keyword_hits(card_text, ["recycling", "black mass", "hydrometallurgy", "recovered material", "scrap"])
    research_hits = int(score_row.get("Heuristic_ResearchOnly_Hits", 0))
    news_hits = int(score_row.get("Heuristic_GenericNews_Hits", 0))
    marketing_hits = int(score_row.get("Heuristic_Marketing_Hits", 0))

    grounded_specificity = clamp_unit(float(score_row.get("Heuristic_Grounded_Entity_Score", 0.0)) / 100.0)
    semantic_strength = clamp_unit(float(score_row.get("Semantic_Embedding_Score", score_row.get("Embedding_Score", 0.0))) / 100.0)
    embedding_rerank_strength = clamp_unit(float(score_row.get("Embedding_Rerank_Score", 0.0)) / 100.0)
    hybrid_strength = clamp_unit(float(score_row.get("Hybrid_Score", 0.0)) / 100.0)
    lexical_strength = clamp_unit(float(score_row.get("Lexical_Entity_Score", 0.0)) / 100.0)
    question_strength = clamp_unit(float(score_row.get("Heuristic_Question_Coverage", 0.0)) / 5.0)
    region_strength = 1.0 if float(score_row.get("Heuristic_Region_Score", 0.0)) > 0 else 0.0
    chunk_strength = clamp_unit(float(score_row.get("Heuristic_Top_Chunk_Score", 0.0)) / 100.0)
    policy_strength = clamp_unit(policy_hits / 5.0)
    logistics_strength = clamp_unit(logistics_hits / 5.0)
    relationship_strength = clamp_unit(relationship_hits / 5.0)
    recycling_strength = clamp_unit(recycling_hits / 4.0)
    research_strength = clamp_unit(research_hits / 4.0)
    news_strength = clamp_unit(news_hits / 4.0)
    marketing_strength = clamp_unit(marketing_hits / 4.0)

    direct_usecase_score = clamp_unit(
        0.24 * grounded_specificity
        + 0.18 * hybrid_strength
        + 0.08 * semantic_strength
        + 0.08 * embedding_rerank_strength
        + 0.08 * lexical_strength
        + 0.15 * question_strength
        + 0.10 * relationship_strength
        + 0.10 * logistics_strength
        + 0.10 * policy_strength
        + 0.10 * region_strength
        - 0.12 * news_strength
        - 0.15 * marketing_strength
        - 0.08 * research_strength
    )
    adjacent_background_score = clamp_unit(
        0.28 * policy_strength
        + 0.24 * logistics_strength
        + 0.14 * semantic_strength
        + 0.10 * hybrid_strength
        + 0.08 * embedding_rerank_strength
        + 0.12 * question_strength
        + 0.08 * chunk_strength
        + 0.06 * region_strength
        - 0.10 * marketing_strength
    )
    research_only_score = clamp_unit(
        0.58 * research_strength
        + 0.18 * (1.0 - grounded_specificity)
        + 0.12 * (1.0 - relationship_strength)
        + 0.12 * (1.0 - region_strength)
    )
    generic_news_score = clamp_unit(
        0.55 * news_strength
        + 0.15 * (1.0 - grounded_specificity)
        + 0.15 * (1.0 - question_strength)
        + 0.15 * (1.0 - chunk_strength)
    )
    marketing_noise_score = clamp_unit(
        0.62 * marketing_strength
        + 0.20 * (1.0 - grounded_specificity)
        + 0.18 * (1.0 - clamp_unit(float(score_row.get("Heuristic_Source_Credibility_Pct", 0.0)) / 100.0))
    )

    rerank_score = round(
        0.28 * float(score_row.get("Heuristic_Score", 0.0))
        + 0.28 * float(score_row.get("Hybrid_Score", 0.0))
        + 0.16 * float(score_row.get("Embedding_Rerank_Score", 0.0))
        + 0.06 * float(score_row.get("Semantic_Embedding_Score", score_row.get("Embedding_Score", 0.0)))
        + 20.0 * direct_usecase_score
        - 10.0 * generic_news_score
        - 10.0 * marketing_noise_score,
        2,
    )

    subtopic_scores = {
        "recycling_materials": recycling_strength + 0.20 * semantic_strength,
        "policy_localization": policy_strength + 0.15 * region_strength + 0.10 * hybrid_strength,
        "logistics_risk": logistics_strength + 0.20 * semantic_strength,
        "supplier_tiering": relationship_strength + 0.20 * grounded_specificity + 0.10 * hybrid_strength,
        "who_where_scale": 0.35 * grounded_specificity + 0.25 * lexical_strength + 0.20 * question_strength + 0.20 * semantic_strength,
    }
    primary_subtopic = max(subtopic_scores.items(), key=lambda item: item[1])[0]

    return {
        "Direct_Usecase_Score": round(direct_usecase_score, 4),
        "Adjacent_Background_Score": round(adjacent_background_score, 4),
        "Research_Only_Score": round(research_only_score, 4),
        "Generic_News_Score": round(generic_news_score, 4),
        "Marketing_Noise_Score": round(marketing_noise_score, 4),
        "Rubric_Policy_Hits": policy_hits,
        "Rubric_Logistics_Hits": logistics_hits,
        "Rubric_Relationship_Hits": relationship_hits,
        "Rubric_Recycling_Hits": recycling_hits,
        "Primary_Subtopic": primary_subtopic,
        "Rerank_Score": rerank_score,
    }


def assess_document_credibility(row: dict[str, Any]) -> dict[str, Any]:
    source_flags = source_domain_flags(as_text(row.get("Source_Domain")))
    source_pct = clamp_score(float(row.get("Heuristic_Source_Credibility_Pct", 0.0)))
    entity_pct = clamp_score(
        1.2 * float(row.get("Lexical_Entity_Score", 0.0))
        + 0.3 * float(row.get("Heuristic_Grounded_Entity_Score", 0.0))
    )
    content_source = as_text(row.get("Document_Content_Source"))
    source_backed_bonus = 12.0 if content_source.startswith("local_pdf") else (
        10.0 if content_source in {"local_html", "local_text"} else 0.0
    )
    evidence_pct = min(
        100.0,
        (5.0 if as_text(row.get("Metadata_Summary")) else 0.0)
        + (18.0 if as_text(row.get("FirstPage_Summary")) else 0.0)
        + (15.0 if as_text(row.get("FirstTwoPages_Summary")) else 0.0)
        + (15.0 if as_text(row.get("Sampled_Page_Summaries")) else 0.0)
        + (20.0 if as_text(row.get("Top_Relevant_Chunks")) else 0.0)
        + (15.0 if as_text(row.get("Evidence_Page_Numbers")) else 0.0)
        + (10.0 if as_text(row.get("Headings_TOC")) else 0.0)
        + source_backed_bonus,
    )
    date_pct = 100.0 if as_text(row.get("Publication_Date")) else (60.0 if as_text(row.get("Extracted_Dates")) else 25.0)

    doc_type_text = normalize_space(
        " ".join([as_text(row.get("Title")), as_text(row.get("Top_Relevant_Chunks"))[:400], as_text(row.get("File_Type"))])
    ).lower()
    doc_type_hits = sum(1 for keyword in DOCUMENT_QUALITY_KEYWORDS if keyword in doc_type_text)
    doc_type_pct = min(100.0, 15.0 + 12.0 * doc_type_hits + (8.0 if as_text(row.get("File_Type")).upper() == "PDF" else 0.0))

    llm_quality = 50.0
    if as_text(row.get("LLM_Judge_Status")) == "ok":
        llm_quality = clamp_score(
            8.0 * float(row.get("LLM_Information_Quality", 0.0))
            + 12.0 * float(row.get("LLM_Confidence", 0.0))
            - 4.0 * float(row.get("LLM_Noise_Level", 0.0))
        )

    penalty = min(
        60.0,
        6.0 * float(row.get("Heuristic_GenericNews_Hits", 0.0))
        + 8.0 * float(row.get("Heuristic_Marketing_Hits", 0.0))
        + 3.0 * float(row.get("Marketing_Noise_Score", 0.0)),
    )
    if int(source_flags["Source_Blocklist_Flag"]) == 1:
        penalty += 35.0
    elif int(source_flags["Source_LowTrust_Flag"]) == 1:
        penalty += 18.0
    score = (
        0.18 * source_pct
        + 0.28 * evidence_pct
        + 0.24 * entity_pct
        + 0.10 * date_pct
        + 0.08 * doc_type_pct
        + 0.12 * llm_quality
        - penalty
    )
    credibility_score = round(clamp_score(score), 2)

    reason_parts: list[str] = []
    if source_pct >= 80.0:
        reason_parts.append("credible source domain")
    elif source_pct <= 35.0:
        reason_parts.append("low-authority source domain")
    if int(source_flags["Source_Blocklist_Flag"]) == 1:
        reason_parts.append("blocklisted finance/news aggregator domain")
    elif int(source_flags["Source_LowTrust_Flag"]) == 1:
        reason_parts.append("low-trust finance/news aggregator domain")
    if evidence_pct >= 70.0:
        reason_parts.append("strong evidence depth")
    elif evidence_pct <= 40.0:
        reason_parts.append("thin evidence depth")
    if entity_pct >= 65.0:
        reason_parts.append("specific grounded entities")
    if as_text(row.get("Evidence_Page_Numbers")):
        reason_parts.append("page-linked evidence")
    if not reason_parts:
        reason_parts.append("mixed credibility signals")

    return {
        **source_flags,
        "Credibility_Score": credibility_score,
        "Credibility_Source_Pct": round(source_pct, 2),
        "Credibility_Evidence_Pct": round(evidence_pct, 2),
        "Credibility_Entity_Pct": round(entity_pct, 2),
        "Credibility_Date_Pct": round(date_pct, 2),
        "Credibility_DocType_Pct": round(doc_type_pct, 2),
        "Credibility_LLM_Pct": round(llm_quality, 2),
        "Credibility_Reason": "; ".join(reason_parts)[:250],
    }


def apply_diversity_pass(rows: list[dict[str, Any]], max_share: float = 0.5) -> list[dict[str, Any]]:
    keep_rows = [row for row in rows if as_text(row.get("Final_Decision")) == "keep"]
    keep_topics = {as_text(row.get("Primary_Subtopic")) for row in keep_rows if as_text(row.get("Primary_Subtopic"))}
    if len(keep_rows) < 4 or len(keep_topics) < 2:
        return rows

    cap = max(1, math.ceil(len(keep_rows) * max(0.2, min(1.0, float(max_share)))))
    counts: dict[str, int] = {}
    ranked_keep_rows = sorted(keep_rows, key=lambda item: float(item.get("Final_Rank_Score", item.get("Rerank_Score", 0.0))), reverse=True)
    for row in ranked_keep_rows:
        topic = as_text(row.get("Primary_Subtopic")) or "unknown"
        counts.setdefault(topic, 0)
        if counts[topic] >= cap:
            existing_reason = as_text(row.get("Keep_Review_Discard_Reason"))
            row["Final_Decision"] = "review"
            row["Keep_Review_Discard_Reason"] = normalize_space(
                f"{existing_reason} Diversity pass moved this from keep to review to avoid over-concentration in one subtopic."
            )
            continue
        counts[topic] += 1

    return rows


def final_decision_reason(row: dict, llm_enabled: bool) -> tuple[str, str]:
    shortlist_pass = int(row.get("Shortlist_Pass", 0)) == 1
    llm_pass = int(row.get("LLM_Judge_Pass", 0)) == 1
    source_blocked = int(row.get("Source_Blocklist_Flag", 0)) == 1
    source_low_trust = int(row.get("Source_LowTrust_Flag", 0)) == 1
    source_file_available = int(row.get("Source_File_Available", 0)) == 1
    adjacent = float(row.get("Adjacent_Background_Score", 0.0))
    direct = float(row.get("Direct_Usecase_Score", 0.0))
    hybrid = float(row.get("Hybrid_Score", row.get("Embedding_Score", 0.0)))
    heuristic = float(row.get("Heuristic_Score", 0.0))
    credibility = float(row.get("Credibility_Score", 0.0))
    generic_news = float(row.get("Generic_News_Score", 0.0))
    marketing = float(row.get("Marketing_Noise_Score", 0.0))
    rerank = float(row.get("Rerank_Score", 0.0))

    if not source_file_available:
        return "discard", "Rejected because the source file was not preserved locally for final curation."
    if source_blocked:
        return "discard", "Discarded because the source domain is a blocked low-trust finance/news aggregator."
    if source_low_trust and (generic_news >= 0.20 or marketing >= 0.20 or credibility < 75.0):
        return "discard", "Discarded because the source domain is a low-trust finance/news aggregator and did not clear the elevated credibility bar."
    if shortlist_pass and llm_enabled and llm_pass and credibility >= 60.0:
        return "keep", "Passed heuristic, hybrid, direct-usecase, LLM judge, and credibility thresholds."
    if shortlist_pass and not llm_enabled and credibility >= 60.0:
        return "review", "Passed heuristic, hybrid, direct-usecase, and credibility thresholds; LLM judge was disabled."
    if shortlist_pass and llm_enabled and llm_pass and credibility < 60.0:
        return "review", "Passed shortlist and LLM thresholds, but credibility remains below the keep threshold."
    if shortlist_pass and llm_enabled and not llm_pass:
        return "review", "Passed shortlist thresholds but missed one or more LLM judge thresholds."
    if credibility >= 55.0 and heuristic >= 48.0 and rerank >= 58.0 and (direct >= 0.45 or adjacent >= 0.55):
        return "review", "Strong enough evidence and ranking signals to keep for manual review even without a strict shortlist pass."
    if adjacent >= 0.60 and hybrid >= 55.0 and credibility >= 50.0 and generic_news <= 0.45 and marketing <= 0.45:
        return "review", "Useful adjacent background with enough evidence to keep in human review."
    if direct >= 0.55 and heuristic >= 38.0 and hybrid >= 55.0:
        return "review", "Borderline direct-usecase evidence; worth manual review before discard."
    return "discard", "Insufficient grounded supply-chain evidence or too much generic/news/marketing noise."
