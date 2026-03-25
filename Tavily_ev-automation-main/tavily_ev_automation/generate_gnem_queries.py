"""
Generate 1000 grounded GNEM-focused queries for phase-1 corpus construction.

The topic seeds are built from the local grounding assets:
- GNEM Supply Chain.docx
- GA_Automotive Landscape_All_Companies.xlsx (or the "(1)" variant)
- Counties_Georgia.geojson

Output format:
- 25 grounded topics x 40 query templates = 1000 unique queries.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import zipfile
from collections import Counter
from pathlib import Path

import pandas as pd


DEFAULT_QUERY_OUTPUT = Path("data") / "queries" / "queries_1000.txt"
DEFAULT_GROUNDING_XLSX = Path("data") / "grounding" / "GA_Automotive Landscape_All_Companies (1).xlsx"
LEGACY_GROUNDING_XLSX = Path("GA_Automotive Landscape_All_Companies (1).xlsx")
ALT_LEGACY_GROUNDING_XLSX = Path("GA_Automotive Landscape_All_Companies.xlsx")
DEFAULT_GROUNDING_GEOJSON = Path("data") / "grounding" / "Counties_Georgia.geojson"
LEGACY_GROUNDING_GEOJSON = Path("Counties_Georgia.geojson")
DEFAULT_GROUNDING_DOCX = Path("data") / "grounding" / "GNEM Supply Chain.docx"
LEGACY_GROUNDING_DOCX = Path("GNEM Supply Chain.docx")


PORT_TERMS = [
    "Port of Savannah",
    "Port of Brunswick",
    "Garden City Terminal",
    "Colonel's Island Terminal",
]

SOUTHEAST_STATES = [
    "Georgia",
    "Alabama",
    "South Carolina",
    "North Carolina",
    "Tennessee",
    "Kentucky",
    "Mississippi",
]

VALUE_CHAIN_STAGE_SEEDS = [
    "critical minerals refining and precursor processing",
    "cathode active material and precursor CAM manufacturing",
    "graphite and anode material processing",
    "separator electrolyte binder and copper foil supply",
    "battery cell module and pack manufacturing",
    "battery recycling black mass and hydrometallurgy",
]

BATTERY_SIGNAL_KEYWORDS = [
    "battery",
    "lithium",
    "anode",
    "cathode",
    "graphite",
    "electrolyte",
    "separator",
    "pack",
    "cell",
    "module",
    "recycling",
    "black mass",
    "hydrometallurgy",
    "copper foil",
    "precursor",
]

GNEM_THEME_SEEDS = [
    "resilience control tower battery network",
    "localization gap analysis battery suppliers",
    "ghost node missing battery capability analysis",
    "physical web logistics Savannah Brunswick rail corridors",
    "commercial web supplier customer dependency mapping",
    "innovation web battery capability and technology network",
    "trust layer compliance quality certification supplier screening",
    "site readiness workforce permitting utility constraints",
    "single source risk and bottleneck assessment",
]

TOPIC_TEMPLATES = [
    "{geo} EV battery supply chain",
    "{geo} Battery Belt supplier network",
    "{geo} battery localization and resilience",
    "{geo} battery logistics Savannah Brunswick rail hazmat",
    "{county} Georgia EV battery facilities",
    "{county} Georgia battery supplier network",
    "{company} Georgia battery supply chain",
    "{company} Southeast battery facility capacity",
    "{oem} Georgia Southeast battery sourcing suppliers",
    "{oem} Georgia localization battery suppliers",
    "Georgia {stage}",
    "Southeast {stage}",
    "Georgia multi tier battery supplier relationships",
    "Georgia resilience control tower battery network",
    "Georgia county level battery facility capacity localization",
]

QUERY_TEMPLATES = [
    "{topic} supplier customer tier 1 tier 2 network map report",
    "{topic} who makes what where at what scale capacity timeline",
    "{topic} facility ownership location commissioning timeline tracker",
    "{topic} supplier relationships customer dependencies offtake joint venture",
    "{topic} multi tier supplier relationships and dependency graph",
    "{topic} facility capacity gwh mwh tons mtpa",
    "{topic} county plant facility expansion tracker",
    "{topic} logistics routes Savannah Brunswick ports rail hazmat",
    "{topic} logistics bottleneck intermodal freight corridor analysis",
    "{topic} import export customs tariff trade exposure",
    "{topic} infrastructure risk permitting utility workforce",
    "{topic} risk bottleneck disruption concentration single source assessment",
    "{topic} localization gap analysis and sourcing alternatives",
    "{topic} policy incentives IRA tax credit grant workforce site readiness",
    "{topic} localization decision support OEM state leaders",
    "{topic} battery value chain minerals CAM anode cell pack recycling",
    "{topic} cathode anode separator electrolyte copper foil suppliers",
    "{topic} recycling black mass hydrometallurgy recovery capacity",
    "{topic} supplier directory capabilities tiering and locations",
    "{topic} ecosystem map with suppliers facilities ports and logistics edges",
    "{topic} resilience scorecard what if stress test scenario",
    "{topic} resilience control tower digital twin framework",
    "{topic} official filing 10-k 8-k facility capacity battery operations",
    "{topic} investor presentation battery plant capacity timeline",
    "{topic} annual report battery supply chain dependencies",
    "{topic} state economic development battery facility tracker",
    "{topic} supplier map with OEM or offtake relationships",
    "{topic} county workforce and site readiness",
    "{topic} trade lanes customs and localization impacts",
    "{topic} rail freight port throughput hazmat constraints",
    "{topic} government report site:energy.gov",
    "{topic} government report site:doe.gov",
    "{topic} federal analysis site:commerce.gov",
    "{topic} policy or incentive analysis site:georgia.org",
    "{topic} infrastructure analysis site:dot.ga.gov",
    "{topic} research report site:nrel.gov",
    "{topic} research report site:anl.gov",
    "{topic} company filing site:sec.gov battery facility supplier risk",
    "{topic} Georgia and Southeast manufacturing network",
    "{topic} high quality dataset directory supplier facility capacity",
]


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


def clean_seed_text(text: str) -> str:
    cleaned = re.sub(r"[\*\[\]\{\}\|<>]+", " ", as_text(text))
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def queryable_term(text: str, *, max_words: int = 10, max_chars: int = 80) -> str:
    cleaned = clean_seed_text(text)
    if not cleaned:
        return ""
    words = cleaned.split()
    if len(words) > max_words:
        cleaned = " ".join(words[:max_words])
    return cleaned[:max_chars].strip(" ,;:-")


def read_docx_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        with zipfile.ZipFile(path) as zf:
            xml = zf.read("word/document.xml").decode("utf-8", errors="ignore")
    except Exception:
        return ""
    pieces = re.findall(r"<w:t[^>]*>(.*?)</w:t>", xml)
    return normalize_space(" ".join(html.unescape(piece) for piece in pieces))


def split_multi_value(text: str) -> list[str]:
    raw = clean_seed_text(text)
    if not raw:
        return []
    parts = re.split(r";|/|,| and |\|", raw, flags=re.IGNORECASE)
    return [normalize_space(part) for part in parts if normalize_space(part)]


def detect_existing_path(candidates: list[str | Path]) -> Path:
    attempted: list[str] = []
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        attempted.append(str(path))
        if path.exists():
            return path
    raise FileNotFoundError(f"None of the expected grounding files were found: {attempted}")


def load_grounding_context(xlsx_path: Path, geojson_path: Path, docx_path: Path) -> dict[str, list[str] | str]:
    df = pd.DataFrame()
    if xlsx_path.exists():
        xl = pd.ExcelFile(xlsx_path)
        sheet_name = "Data" if "Data" in xl.sheet_names else xl.sheet_names[0]
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name)

    company_scores: Counter[str] = Counter()
    battery_company_scores: Counter[str] = Counter()
    county_counts: Counter[str] = Counter()
    battery_county_counts: Counter[str] = Counter()
    oem_counts: Counter[str] = Counter()
    facility_type_counts: Counter[str] = Counter()
    role_counts: Counter[str] = Counter()
    product_counts: Counter[str] = Counter()

    if not df.empty:
        for _, row in df.iterrows():
            company = as_text(row.get("Company"))
            location = clean_seed_text(row.get("Location"))
            role = clean_seed_text(row.get("EV Supply Chain Role"))
            product = clean_seed_text(row.get("Product / Service"))
            relevant = clean_seed_text(row.get("EV / Battery Relevant"))
            facility_type = queryable_term(row.get("Primary Facility Type"))
            oems = split_multi_value(row.get("Primary OEMs"))
            company = clean_seed_text(company)

            battery_signal_text = normalize_space(" ".join([role, product, relevant])).lower()
            battery_signal = any(keyword in battery_signal_text for keyword in BATTERY_SIGNAL_KEYWORDS)
            company_score = 1
            if "yes" in relevant.lower():
                company_score += 4
            elif "indirect" in relevant.lower():
                company_score += 2
            if battery_signal:
                company_score += 3
            if any(keyword in role.lower() for keyword in ["battery", "materials", "thermal", "electronics"]):
                company_score += 1
            if company:
                company_scores[company] += company_score
                if battery_signal or "yes" in relevant.lower() or "indirect" in relevant.lower():
                    battery_company_scores[company] += company_score + 2

            match = re.search(r"([A-Z][A-Za-z\-\s]+ County)\b", location)
            if match:
                county_name = normalize_space(match.group(1))
                county_counts[county_name] += 1
                if battery_signal or "yes" in relevant.lower() or "indirect" in relevant.lower():
                    battery_county_counts[county_name] += company_score + 1

            for oem in oems:
                if not oem or "multiple oem" in oem.lower():
                    continue
                oem_counts[oem] += 1
            if battery_signal or "yes" in relevant.lower() or "indirect" in relevant.lower():
                if facility_type:
                    facility_type_counts[facility_type] += company_score
                if role:
                    role_counts[queryable_term(role, max_words=8, max_chars=70)] += company_score
                product_term = queryable_term(product, max_words=8, max_chars=70)
                if product_term and any(keyword in product_term.lower() for keyword in BATTERY_SIGNAL_KEYWORDS):
                    product_counts[product_term] += company_score

    geojson_counties: list[str] = []
    if geojson_path.exists():
        try:
            data = json.loads(geojson_path.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        for feature in data.get("features", []):
            props = feature.get("properties") or {}
            county_name = clean_seed_text(props.get("NAMELSAD10")) or clean_seed_text(props.get("NAME10"))
            if county_name:
                geojson_counties.append(county_name)

    docx_text = read_docx_text(docx_path)
    docx_text_lower = docx_text.lower()
    docx_flags = {
        "battery_belt": "Battery Belt" in docx_text,
        "control_tower": "Control Tower" in docx_text,
        "localization": "localization" in docx_text.lower(),
        "ghost_node": "ghost node" in docx_text_lower,
        "physical_web": "physical web" in docx_text_lower,
        "commercial_web": "commercial web" in docx_text_lower,
        "innovation_web": "innovation web" in docx_text_lower,
        "trust_web": "trust web" in docx_text_lower,
        "site_readiness": "site readiness" in docx_text_lower,
        "single_source": "single source risk" in docx_text_lower,
    }

    counties = [name for name, _ in battery_county_counts.most_common(10)]
    for county, _ in county_counts.most_common(6):
        if county not in counties:
            counties.append(county)
    for county in geojson_counties:
        if county not in counties:
            counties.append(county)
        if len(counties) >= 12:
            break

    companies = [queryable_term(name, max_words=5, max_chars=55) for name, _ in battery_company_scores.most_common(12)]
    for name, _ in company_scores.most_common(8):
        cleaned = queryable_term(name, max_words=5, max_chars=55)
        if cleaned and cleaned not in companies:
            companies.append(cleaned)
        if len(companies) >= 12:
            break
    oems = [clean_seed_text(name) for name, _ in oem_counts.most_common(6)]
    geos = ["Georgia", "Southeast"]
    if docx_flags["battery_belt"]:
        geos.append("Georgia Battery Belt")
    if docx_flags["control_tower"]:
        geos.append("Georgia resilience control tower")

    gnem_themes = []
    for theme in GNEM_THEME_SEEDS:
        theme_lower = theme.lower()
        if (
            ("control tower" in theme_lower and docx_flags["control_tower"])
            or ("ghost node" in theme_lower and docx_flags["ghost_node"])
            or ("physical web" in theme_lower and docx_flags["physical_web"])
            or ("commercial web" in theme_lower and docx_flags["commercial_web"])
            or ("innovation web" in theme_lower and docx_flags["innovation_web"])
            or ("trust layer" in theme_lower and docx_flags["trust_web"])
            or ("site readiness" in theme_lower and docx_flags["site_readiness"])
            or ("single source" in theme_lower and docx_flags["single_source"])
            or ("localization" in theme_lower and docx_flags["localization"])
        ):
            gnem_themes.append(theme)
    if not gnem_themes:
        gnem_themes = GNEM_THEME_SEEDS[:5]

    return {
        "companies": [company for company in companies if company],
        "counties": counties,
        "oems": oems or ["Hyundai", "Kia", "Rivian", "Blue Bird"],
        "geographies": geos,
        "stages": VALUE_CHAIN_STAGE_SEEDS,
        "ports": PORT_TERMS,
        "facility_types": [name for name, _ in facility_type_counts.most_common(6)],
        "roles": [name for name, _ in role_counts.most_common(8)],
        "products": [name for name, _ in product_counts.most_common(8)],
        "gnem_themes": gnem_themes,
        "docx_text": docx_text,
    }


def unique_in_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        key = value.lower()
        if not value or key in seen:
            continue
        seen.add(key)
        ordered.append(value)
    return ordered


def build_topics(context: dict[str, list[str] | str]) -> list[str]:
    geos = list(context["geographies"])
    counties = list(context["counties"])
    companies = list(context["companies"])
    oems = list(context["oems"])
    stages = list(context["stages"])
    ports = list(context.get("ports", []))
    facility_types = list(context.get("facility_types", []))
    roles = list(context.get("roles", []))
    products = list(context.get("products", []))
    gnem_themes = list(context.get("gnem_themes", []))

    topic_candidates: list[str] = []
    topic_candidates.extend(
        [
            "Georgia EV battery supplier and customer network",
            "Georgia county level battery facility capacity and commissioning",
            "Georgia battery localization resilience and control tower",
            "Georgia Battery Belt supplier network and bottlenecks",
            "Georgia battery site readiness workforce permitting and utilities",
            "Georgia battery trade logistics Savannah Brunswick rail hazmat",
        ]
    )
    for theme in gnem_themes[:4]:
        topic_candidates.append(f"Georgia {theme}")
    for port in ports[:2]:
        port_short = queryable_term(port, max_words=4, max_chars=40)
        topic_candidates.extend(
            [
                f"{port_short} Georgia battery logistics and supplier network",
                f"{port_short} Georgia battery materials freight and trade exposure",
            ]
        )
    for county in counties[:4]:
        county_short = queryable_term(county, max_words=4, max_chars=40)
        role_hint = roles[len(topic_candidates) % len(roles)] if roles else "battery supply chain"
        topic_candidates.extend(
            [
                f"{county_short} Georgia battery facilities and supplier network",
                f"{county_short} Georgia {queryable_term(role_hint, max_words=6, max_chars=50)}",
            ]
        )
    for company in companies[:5]:
        topic_candidates.extend(
            [
                f"{company} Georgia battery facility capacity and timeline",
                f"{company} Georgia supplier role and OEM relationships",
            ]
        )
    for oem in oems[:4]:
        topic_candidates.extend(
            [
                f"{oem} Georgia battery sourcing suppliers and localization",
                f"{oem} Georgia Southeast supplier network and battery capacity",
            ]
        )
    for stage in stages[:3]:
        topic_candidates.append(f"Georgia {queryable_term(stage, max_words=8, max_chars=65)}")
    for role in roles[:3]:
        topic_candidates.append(f"Georgia {queryable_term(role, max_words=8, max_chars=60)}")
    for product in products[:2]:
        topic_candidates.append(f"Georgia {queryable_term(product, max_words=8, max_chars=60)} supply chain")
    for facility_type in facility_types[:2]:
        topic_candidates.append(f"Georgia {queryable_term(facility_type, max_words=6, max_chars=50)} battery suppliers")

    topics = unique_in_order(topic_candidates)
    fallback_topics = [
        "Georgia EV battery manufacturing network",
        "Southeast EV battery manufacturing network",
        "Georgia battery facility capacity and localization",
        "Georgia battery infrastructure and logistics risk",
        "Georgia battery supplier customer mapping",
    ]
    for topic in fallback_topics:
        if len(topics) >= 25:
            break
        if topic not in topics:
            topics.append(topic)

    if len(topics) < 25:
        raise ValueError(f"Expected at least 25 grounded topics, got {len(topics)}")
    return topics[:25]


def generate_queries(context: dict[str, list[str] | str]) -> list[str]:
    topics = build_topics(context)
    queries: list[str] = []
    for topic in topics:
        for template in QUERY_TEMPLATES:
            queries.append(template.format(topic=topic))
    queries = unique_in_order(queries)
    if len(queries) != 1000:
        raise ValueError(f"Expected exactly 1000 queries, got {len(queries)}")
    return queries


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate GNEM-focused grounded queries_1000.txt")
    parser.add_argument("--output", default=str(DEFAULT_QUERY_OUTPUT), help="Output query file path")
    parser.add_argument(
        "--grounding-xlsx",
        default=str(DEFAULT_GROUNDING_XLSX),
        help="Grounding workbook path or preferred filename",
    )
    parser.add_argument(
        "--grounding-counties-geojson",
        default=str(DEFAULT_GROUNDING_GEOJSON),
        help="Georgia counties geojson path",
    )
    parser.add_argument(
        "--grounding-docx",
        default=str(DEFAULT_GROUNDING_DOCX),
        help="GNEM supply-chain vision docx path",
    )
    args = parser.parse_args()

    xlsx_path = detect_existing_path(
        [
            args.grounding_xlsx,
            LEGACY_GROUNDING_XLSX,
            ALT_LEGACY_GROUNDING_XLSX,
        ]
    )
    geojson_path = detect_existing_path([args.grounding_counties_geojson, LEGACY_GROUNDING_GEOJSON])
    docx_path = detect_existing_path([args.grounding_docx, LEGACY_GROUNDING_DOCX])

    context = load_grounding_context(xlsx_path, geojson_path, docx_path)
    queries = generate_queries(context)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(queries) + "\n", encoding="utf-8")
    print(f"Wrote {len(queries)} queries -> {output}")
    print(f"Grounded companies: {', '.join(context['companies'][:6])}")
    print(f"Grounded counties: {', '.join(context['counties'][:6])}")
    print(f"Grounded OEMs: {', '.join(context['oems'][:4])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
