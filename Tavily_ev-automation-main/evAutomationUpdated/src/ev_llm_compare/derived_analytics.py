from __future__ import annotations

from collections import Counter
from pathlib import Path
import re
import uuid

import pandas as pd

from .chunking import tokenize
from .schemas import Chunk, TableRow

ATLANTA_CORE_COUNTIES = {
    "Cherokee County",
    "Clayton County",
    "Cobb County",
    "DeKalb County",
    "Douglas County",
    "Fayette County",
    "Forsyth County",
    "Fulton County",
    "Gwinnett County",
    "Henry County",
    "Paulding County",
    "Rockdale County",
}

CAPABILITY_RULES: dict[str, tuple[str, ...]] = {
    "battery modules": ("battery module", "battery modules", "module", "modules"),
    "complete battery packs": ("battery pack", "battery packs", "complete pack", "pack systems"),
    "power electronics": (
        "power electronics",
        "inverter",
        "converter",
        "dc-dc",
        "on-board charger",
        "onboard charger",
        "electronics systems",
    ),
    "high-voltage connectors": (
        "high-voltage connector",
        "high voltage connector",
        "connector",
        "connectors",
        "terminal",
        "wire harness",
        "wiring",
    ),
    "dc fast charging hardware": (
        "dc fast",
        "fast charger",
        "charging hardware",
        "ev charging",
        "charging solutions",
        "charger",
        "charging ports",
    ),
    "autonomous vehicle sensor systems": (
        "sensor",
        "sensors",
        "adas",
        "radar",
        "lidar",
        "camera",
        "autonomous",
    ),
}

BATTERY_ECOSYSTEM_RULES: dict[str, tuple[str, ...]] = {
    "battery manufacturing": (
        "battery",
        "cell",
        "cells",
        "module",
        "modules",
        "pack",
        "packs",
        "separator",
    ),
    "battery materials": (
        "cathode",
        "anode",
        "graphite",
        "lithium",
        "cobalt",
        "nickel",
        "electrolyte",
        "foil",
        "copper",
    ),
    "charging hardware": ("charger", "charging", "dc fast", "ev charging"),
    "recycling and recovery": ("recycling", "recover", "recovery", "scrap", "circular"),
    "power electronics and sensors": ("power electronics", "inverter", "converter", "sensor", "adas"),
}


def build_derived_summary_chunks(rows: list[TableRow]) -> list[Chunk]:
    if not rows:
        return []

    frame = _rows_to_frame(rows)
    workbook_name = rows[0].workbook_path.name
    summary_specs = [
        (
            "county_cluster_summary",
            "Derived analytic summary table: county supplier density, employment, EV relevance, and non-metro cluster candidates.",
            _county_cluster_summary(frame),
        ),
        (
            "role_category_concentration",
            "Derived analytic summary table: EV supply chain role and category concentration proxies.",
            _role_category_concentration_summary(frame),
        ),
        (
            "capability_keyword_index",
            "Derived analytic summary table: capability keyword index for battery modules, complete packs, power electronics, high-voltage connectors, DC fast charging hardware, and autonomous vehicle sensor systems.",
            _capability_keyword_summary(frame),
        ),
        (
            "oem_supplier_linkage",
            "Derived analytic summary table: supplier linkage by Primary OEM, category, and EV supply chain role.",
            _oem_supplier_linkage_summary(frame),
        ),
        (
            "battery_ecosystem_summary",
            "Derived analytic summary table: battery ecosystem, materials, charging, recycling, and circular economy signals from workbook rows.",
            _battery_ecosystem_summary(frame),
        ),
    ]

    chunks: list[Chunk] = []
    for analysis_type, title, body in summary_specs:
        text = f"{title}\n\n{body}".strip()
        chunk_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{workbook_name}:{analysis_type}"))
        chunks.append(
            Chunk(
                chunk_id=chunk_id,
                text=text,
                metadata={
                    "source_file": workbook_name,
                    "sheet_name": "DerivedAnalytics",
                    "chunk_type": "derived_analytic_summary",
                    "analysis_type": analysis_type,
                    "analysis_title": title,
                },
                token_set=tokenize(text),
            )
        )
    return chunks


def _rows_to_frame(rows: list[TableRow]) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for row in rows:
        values = row.values
        location = str(values.get("Location", "")).strip()
        records.append(
            {
                "company": str(values.get("Company", "")).strip(),
                "category": str(values.get("Category", "")).strip(),
                "industry_group": str(values.get("Industry Group", "")).strip(),
                "location": location,
                "county": _extract_county(location),
                "primary_facility_type": str(values.get("Primary Facility Type", "")).strip(),
                "ev_supply_chain_role": str(values.get("EV Supply Chain Role", "")).strip(),
                "primary_oems": str(values.get("Primary OEMs", "")).strip(),
                "supplier_or_affiliation_type": str(values.get("Supplier or Affiliation Type", "")).strip(),
                "employment": str(values.get("Employment", "")).strip(),
                "employment_num": _parse_employment(values.get("Employment", "")),
                "product_service": str(values.get("Product / Service", "")).strip(),
                "ev_battery_relevant": str(values.get("EV / Battery Relevant", "")).strip(),
                "classification_method": str(values.get("Classification Method", "")).strip(),
                "row_number": row.row_number,
            }
        )
    return pd.DataFrame(records)


def _extract_county(location: str) -> str:
    if not location:
        return "Unknown"
    matches = re.findall(r"([A-Za-z .'-]+ County)", location)
    if matches:
        return matches[-1].strip()
    value = location.strip()
    if value.lower() in {"georgia", "alabama", "tennessee", "south carolina", "north carolina"}:
        return "Statewide / Unspecified"
    return value


def _parse_employment(value: object) -> int:
    digits = re.sub(r"[^0-9]", "", str(value))
    return int(digits) if digits else 0


def _format_counter(counter: Counter[str], limit: int = 4) -> str:
    if not counter:
        return "n/a"
    parts = [f"{label}: {count}" for label, count in counter.most_common(limit)]
    return "; ".join(parts)


def _sample_companies(frame: pd.DataFrame, limit: int = 4) -> str:
    if frame.empty:
        return "n/a"
    parts: list[str] = []
    for _, row in frame.head(limit).iterrows():
        company = str(row["company"]).strip() or "Unknown"
        location = str(row["location"]).strip()
        category = str(row["category"]).strip()
        bits = [company]
        if location:
            bits.append(location)
        if category:
            bits.append(f"Category={category}")
        parts.append(" | ".join(bits))
    return "; ".join(parts)


def _county_cluster_summary(frame: pd.DataFrame) -> str:
    county_frame = (
        frame[frame["county"].astype(str).ne("Unknown")]
        .groupby("county", dropna=False)
        .agg(
            company_count=("company", "count"),
            employment_total=("employment_num", "sum"),
            ev_yes=("ev_battery_relevant", lambda values: sum(str(value).strip().lower() == "yes" for value in values)),
            ev_indirect=("ev_battery_relevant", lambda values: sum(str(value).strip().lower() == "indirect" for value in values)),
        )
        .reset_index()
        .sort_values(["company_count", "employment_total", "county"], ascending=[False, False, True])
    )
    top_counties = county_frame.head(15)
    lines = [
        "County | Company count | Employment total | EV/Battery Yes | EV/Battery Indirect",
        "--- | ---:| ---:| ---:| ---:",
    ]
    for _, row in top_counties.iterrows():
        lines.append(
            f"{row['county']} | {int(row['company_count'])} | {int(row['employment_total'])} | {int(row['ev_yes'])} | {int(row['ev_indirect'])}"
        )

    non_metro = county_frame[~county_frame["county"].isin(sorted(ATLANTA_CORE_COUNTIES))].head(10)
    lines.append("")
    lines.append("Non-metro cluster candidates (Atlanta-core county heuristic excluded):")
    if non_metro.empty:
        lines.append("- No non-metro counties detected from the workbook rows.")
    else:
        for _, row in non_metro.iterrows():
            lines.append(
                f"- {row['county']}: {int(row['company_count'])} companies, employment_total={int(row['employment_total'])}, EV_yes={int(row['ev_yes'])}, EV_indirect={int(row['ev_indirect'])}"
            )
    return "\n".join(lines)


def _role_category_concentration_summary(frame: pd.DataFrame) -> str:
    lines = [
        "Role | Company count | Distinct counties | Top county | Top county share | Category mix",
        "--- | ---:| ---:| --- | ---:| ---",
    ]
    grouped = frame.groupby("ev_supply_chain_role", dropna=False)
    summary_rows: list[tuple[str, int, int, str, float, str]] = []
    for role, group in grouped:
        role_name = str(role).strip() or "Unspecified"
        county_counts = group["county"].value_counts()
        top_county = county_counts.index[0] if not county_counts.empty else "Unknown"
        share = float(county_counts.iloc[0] / len(group)) if len(group) else 0.0
        category_mix = _format_counter(Counter(group["category"].astype(str)), limit=3)
        summary_rows.append((role_name, len(group), group["county"].nunique(), top_county, share, category_mix))
    summary_rows.sort(key=lambda item: (item[1], item[4]), reverse=True)
    for role_name, company_count, county_count, top_county, share, category_mix in summary_rows[:15]:
        lines.append(
            f"{role_name} | {company_count} | {county_count} | {top_county} | {share:.2f} | {category_mix}"
        )
    lines.append("")
    lines.append("Interpretation note: top_county_share is a simple concentration proxy built from workbook company counts, not a full supplier-risk model.")
    return "\n".join(lines)


def _capability_keyword_summary(frame: pd.DataFrame) -> str:
    searchable = (
        frame["company"].astype(str)
        + " | "
        + frame["product_service"].astype(str)
        + " | "
        + frame["ev_supply_chain_role"].astype(str)
        + " | "
        + frame["industry_group"].astype(str)
    ).str.lower()
    lines = [
        "Capability | Matching companies | Category mix | Illustrative workbook examples",
        "--- | ---:| --- | ---",
    ]
    for capability, patterns in CAPABILITY_RULES.items():
        mask = searchable.apply(lambda text: any(pattern in text for pattern in patterns))
        matched = frame[mask].copy().sort_values(["category", "company"])
        category_mix = _format_counter(Counter(matched["category"].astype(str)), limit=3)
        lines.append(
            f"{capability} | {len(matched)} | {category_mix} | {_sample_companies(matched, limit=4)}"
        )
    lines.append("")
    lines.append("Detailed notes:")
    lines.append("- This capability table is keyword-derived from Product / Service, EV Supply Chain Role, Industry Group, and Company fields.")
    lines.append("- Matches should be treated as evidence candidates that still require answer-level citations.")
    return "\n".join(lines)


def _oem_supplier_linkage_summary(frame: pd.DataFrame) -> str:
    linkage_frame = frame.copy()
    linkage_frame["primary_oems"] = linkage_frame["primary_oems"].replace({"": "Unspecified OEM"})
    lines = [
        "Primary OEM | Supplier count | Category mix | Top roles | Example suppliers",
        "--- | ---:| --- | --- | ---",
    ]
    summary_rows: list[tuple[str, int, str, str, str]] = []
    for oem, group in linkage_frame.groupby("primary_oems", dropna=False):
        oem_name = str(oem).strip() or "Unspecified OEM"
        category_mix = _format_counter(Counter(group["category"].astype(str)), limit=3)
        top_roles = _format_counter(Counter(group["ev_supply_chain_role"].astype(str)), limit=3)
        summary_rows.append(
            (
                oem_name,
                len(group),
                category_mix,
                top_roles,
                _sample_companies(group.sort_values("company"), limit=3),
            )
        )
    summary_rows.sort(key=lambda item: item[1], reverse=True)
    for oem_name, supplier_count, category_mix, top_roles, examples in summary_rows[:15]:
        lines.append(f"{oem_name} | {supplier_count} | {category_mix} | {top_roles} | {examples}")
    return "\n".join(lines)


def _battery_ecosystem_summary(frame: pd.DataFrame) -> str:
    searchable = (
        frame["company"].astype(str)
        + " | "
        + frame["product_service"].astype(str)
        + " | "
        + frame["ev_supply_chain_role"].astype(str)
        + " | "
        + frame["industry_group"].astype(str)
    ).str.lower()
    lines = [
        "Theme | Matching companies | Top counties | Illustrative workbook examples",
        "--- | ---:| --- | ---",
    ]
    for theme, patterns in BATTERY_ECOSYSTEM_RULES.items():
        mask = searchable.apply(lambda text: any(pattern in text for pattern in patterns))
        matched = frame[mask | frame["ev_battery_relevant"].astype(str).str.lower().eq("yes")].copy()
        county_mix = _format_counter(Counter(matched["county"].astype(str)), limit=3)
        lines.append(
            f"{theme} | {len(matched)} | {county_mix} | {_sample_companies(matched.sort_values('company'), limit=4)}"
        )
    lines.append("")
    lines.append("This table is useful for questions about battery ecosystem coverage, materials, recycling, charging, and adjacent EV infrastructure.")
    return "\n".join(lines)
