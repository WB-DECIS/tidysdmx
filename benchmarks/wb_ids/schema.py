"""The DS_IDS target schema as committed JSON: ``python -m benchmarks.wb_ids.schema``.

The pipeline's last two steps need the dissemination schema, which lives on the
production FMR. A pickled pysdmx ``Schema`` would be 2.7 MB and break when
pysdmx changes its classes, so ``capture`` keeps only what ``standardize_output``
and ``validate_dataset_local`` read, plus what pysdmx needs to rebuild it: the
context, and each component's id, role, ``required`` flag, data type, attachment
level and code ids. ``load_schema`` rebuilds a ``Schema`` from that with pysdmx
constructors.

Codelists are trimmed to the codes this pipeline can write — the countries and
counterparts in the profile, and the template's target codes and fixed values —
and each keeps its real size as ``codes_total``. Validation therefore reports
exactly what it would against the full codelists, since membership is unchanged
for every code that can occur, while the file stays small (DS_IDS shares
codelists such as an 11,743-code indicator list with other datasets). A codelist
none of whose codes can occur keeps its first code, so it stays a codelist.
"""

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Literal, TypedDict

import pandas as pd
from pysdmx.model import (
    Code,
    Codelist,
    Component,
    Components,
    Concept,
    DataType,
    Hierarchy,
    Role,
    Schema,
)

from benchmarks.wb_ids import PROFILE_JSON, SCHEMA_JSON, TEMPLATE_XLSX
from tidysdmx import parse_mapping_template_wb

FMR_URL: str = "https://fmr.worldbank.org/FMR/sdmx/v2"
AGENCY: str = "WB.DEC.DG"
STRUCTURE_ID: str = "DS_IDS"
VERSION: str = "1.0.0"
ARTEFACT_ID: str = f"{AGENCY}:{STRUCTURE_ID}({VERSION})"

Context = Literal["dataflow", "datastructure", "provisionagreement"]


class ComponentSpec(TypedDict):
    """One schema component, reduced to what the pipeline reads."""

    id: str
    role: str
    required: bool
    dtype: str
    attachment_level: str | None
    codelist: str | None
    codes: list[str] | None
    codes_total: int | None


class SchemaSpec(TypedDict):
    """A schema, reduced to what the pipeline reads."""

    context: Context
    agency: str
    id: str
    version: str
    source: str
    components: list[ComponentSpec]


def to_spec(
    schema: Schema, source: str, keep: Mapping[str, set[str]] | None = None
) -> SchemaSpec:
    """Reduce a pysdmx schema to its committed JSON form.

    Args:
        schema: A schema, as ``RegistryClient.get_schema`` returns it.
        source: Where the schema came from, recorded for the reader.
        keep: Per component id, the codes to keep; components not listed keep
            all of theirs. ``None`` keeps everything.

    Returns:
        The context, identity and components, with each coded component's
        code ids (every level of a hierarchy, de-duplicated, in FMR's order)
        and how many it had before trimming.
    """
    components: list[ComponentSpec] = []
    for component in schema.components:
        enumeration = component.enumeration
        codes: list[str] | None = None
        codelist: str | None = None
        if isinstance(enumeration, Hierarchy):
            codes = list(dict.fromkeys(code.id for code in enumeration.all_codes()))
            codelist = enumeration.short_urn
        elif enumeration is not None:
            codes = [code.id for code in enumeration]
            codelist = enumeration.short_urn
        components.append(
            {
                "id": component.id,
                "role": component.role.name,
                "required": component.required,
                "dtype": component.dtype.name,
                "attachment_level": component.attachment_level,
                "codelist": codelist,
                "codes": _trim(codes, (keep or {}).get(component.id)),
                "codes_total": None if codes is None else len(codes),
            }
        )
    return {
        "context": schema.context,
        "agency": _agency_id(schema.agency),
        "id": schema.id,
        "version": schema.version,
        "source": source,
        "components": components,
    }


def from_spec(spec: SchemaSpec) -> Schema:
    """Rebuild a pysdmx schema from its committed JSON form.

    Args:
        spec: A spec written by ``to_spec``.

    Returns:
        A schema whose components carry the recorded roles, flags, data types
        and codes, each coded one as a local codelist.
    """
    components = [
        Component(
            id=item["id"],
            required=item["required"],
            role=Role[item["role"]],
            concept=Concept(item["id"]),
            local_dtype=DataType[item["dtype"]],
            local_codes=_codelist(item),
            attachment_level=item["attachment_level"],
        )
        for item in spec["components"]
    ]
    return Schema(
        context=spec["context"],
        agency=spec["agency"],
        id=spec["id"],
        components=Components(components),
        version=spec["version"],
    )


def load_schema(path: Path = SCHEMA_JSON) -> Schema:
    """Load the committed DS_IDS schema.

    Args:
        path: A JSON file written by ``capture``.

    Returns:
        The rebuilt schema.
    """
    return from_spec(json.loads(path.read_text(encoding="utf-8")))


def reachable_codes(
    profile: Mapping[str, Any], mappings: Mapping[str, pd.DataFrame]
) -> dict[str, set[str]]:
    """Return, per target component, every code the pipeline can write to it.

    Args:
        profile: A profile from ``benchmarks.wb_ids.profile``, for the codes of
            columns copied implicitly (countries, counterparts).
        mappings: The mapping workbook, as ``parse_mapping_template_wb`` reads it.

    Returns:
        Codes per target: a ``fixed:`` rule's value, an implicit copy's source
        codes, or a ``representation`` rule's REP_MAPPING target column.
    """
    by_column = {
        "Country_Code": set(profile["countries"]),
        "Counterpart_Area_Code": set(profile["counterparts"]),
        "Series": set(profile["series"]),
        "Year": set(profile["years"]),
    }
    rep = mappings.get("REP_MAPPING")
    reachable: dict[str, set[str]] = {}
    for _, row in mappings["COMP_MAPPING"].iterrows():
        target, source, rule = (
            row.get("TARGET"),
            row.get("SOURCE"),
            row.get("MAPPING_RULES"),
        )
        if pd.isna(target) or pd.isna(rule):
            continue
        rule = str(rule)
        if rule.startswith("fixed:"):
            reachable[target] = {rule.removeprefix("fixed:").strip()}
        elif rule == "implicit" and source in by_column:
            reachable[target] = by_column[source]
        elif rule == "representation" and rep is not None and f"T:{target}" in rep:
            reachable[target] = set(rep[f"T:{target}"].dropna().astype(str))
    return reachable


def capture(
    out: Path, url: str = FMR_URL, keep: Mapping[str, set[str]] | None = None
) -> SchemaSpec:
    """Fetch DS_IDS from FMR and write its JSON form.

    Needs network access to ``url``; the production FMR serves reads anonymously.

    Args:
        out: Where to write the JSON.
        url: The FMR SDMX REST endpoint.
        keep: Codes to keep per component, as ``reachable_codes`` returns them.

    Returns:
        The spec written.
    """
    from pysdmx.api.fmr import RegistryClient

    schema = RegistryClient(url).get_schema(
        "datastructure", agency=AGENCY, id=STRUCTURE_ID, version=VERSION
    )
    spec = to_spec(schema, source=f"{url} datastructure {ARTEFACT_ID}", keep=keep)
    # indent=1 puts one code id per line, so a codelist change diffs readably.
    text = json.dumps(spec, indent=1) + "\n"
    out.write_text(text, encoding="utf-8", newline="\n")
    return spec


def _trim(codes: list[str] | None, keep: set[str] | None) -> list[str] | None:
    if codes is None or keep is None:
        return codes
    # pysdmx treats an empty codelist as no codelist, which would stop validation
    # checking the component at all; so one real code always stays.
    return [code for code in codes if code in keep] or codes[:1]


def _codelist(item: ComponentSpec) -> Codelist | None:
    if item["codes"] is None:
        return None
    reference = item["codelist"] or f"Codelist=WB:CL_{item['id']}(1.0)"
    agency, rest = reference.split("=", 1)[1].split(":", 1)
    codelist_id, version = rest.rstrip(")").split("(", 1)
    return Codelist(
        id=codelist_id,
        agency=agency,
        version=version,
        items=[Code(id=code) for code in item["codes"]],
    )


def _agency_id(agency: object) -> str:
    return agency if isinstance(agency, str) else str(getattr(agency, "id", agency))


def main(argv: Sequence[str] | None = None) -> int:
    """Capture DS_IDS from FMR into ``--out``.

    Args:
        argv: Command-line arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        0 on success.
    """
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.wb_ids.schema",
        description="Capture the DS_IDS schema from FMR as committed JSON.",
    )
    parser.add_argument("--out", type=Path, default=SCHEMA_JSON)
    parser.add_argument("--url", default=FMR_URL)
    parser.add_argument("--profile", type=Path, default=PROFILE_JSON)
    parser.add_argument("--template", type=Path, default=TEMPLATE_XLSX)
    args = parser.parse_args(argv)
    profile = json.loads(args.profile.read_text(encoding="utf-8"))
    keep = reachable_codes(profile, parse_mapping_template_wb(args.template))
    spec = capture(args.out, args.url, keep)
    coded = sum(1 for item in spec["components"] if item["codes"] is not None)
    sys.stderr.write(
        f"Wrote {args.out}: {len(spec['components'])} components, {coded} coded\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
