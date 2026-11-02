"""PII indicators that follow column lineage along every path (task 1.6).

Ported from data-capsule's ``services/compliance.py`` (commit 5e57fcc) and rewritten over
the graph. The server traced only the first upstream edge of a column (review finding
F12) and reported exposure only for columns that were themselves tagged. Here:

- A column is a **direct** indicator when dbt meta or tags declare it (``tag``) or its
  name matches a PII pattern (``pattern``).
- PII flows along ``derives`` edges, and across a physical table from a seed or model to
  the source that reads it, along **every** path. A column it reaches is an
  **inherited** indicator (``lineage``) that names every PII origin upstream of it.
- A masked column (hashed, encrypted, ... by name or meta) stops the flow.
- Where a downstream model's column lineage is unknown, the flow cannot be followed, so
  the model is listed as unknown, never as clean.

These are **indicators**, not verified exposure: names, tags and parsed SQL suggest PII,
and nothing here reads the data. Every result says how it was found and how sure it is.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Enum

from lineagepilot_core.analysis.traversal import (
    Direction,
    Granularity,
    Traversal,
    steps,
    traverse,
)
from lineagepilot_core.graph.ids import split_column_id
from lineagepilot_core.graph.model import (
    Column,
    Confidence,
    LineageState,
    MetadataGraph,
    NodeKind,
)

_MASKING_INDICATORS = (
    "_hash", "_hashed", "_masked", "_encrypted", "_redacted",
    "_anonymized", "_tokenized", "_obfuscated",
    "hash_", "masked_", "encrypted_",
)  # fmt: skip

# Layers where PII is consumed, as in data-capsule
CONSUMPTION_LAYERS = frozenset({"gold", "marts", "mart", "consumption", "reporting", "analytics"})

_SQL_KINDS = frozenset({NodeKind.MODEL, NodeKind.SNAPSHOT})


class PIIBasis(str, Enum):
    TAG = "tag"  # declared in dbt meta or tags
    PATTERN = "pattern"  # the column name matches a PII pattern
    LINEAGE = "lineage"  # derived from a PII column upstream


class PIISeverity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


# As in data-capsule; unknown types are MEDIUM
PII_SEVERITY = {
    "ssn": PIISeverity.CRITICAL,
    "national_id": PIISeverity.CRITICAL,
    "passport": PIISeverity.CRITICAL,
    "drivers_license": PIISeverity.CRITICAL,
    "credit_card": PIISeverity.CRITICAL,
    "bank_account": PIISeverity.CRITICAL,
    "biometric": PIISeverity.CRITICAL,
    "health": PIISeverity.CRITICAL,
    "email": PIISeverity.HIGH,
    "phone": PIISeverity.HIGH,
    "address": PIISeverity.HIGH,
    "name": PIISeverity.HIGH,
    "first_name": PIISeverity.HIGH,
    "last_name": PIISeverity.HIGH,
    "date_of_birth": PIISeverity.HIGH,
    "ip_address": PIISeverity.MEDIUM,
    "device_id": PIISeverity.MEDIUM,
    "location": PIISeverity.MEDIUM,
    "age": PIISeverity.MEDIUM,
    "gender": PIISeverity.LOW,
}
_SEVERITY_ORDER = (PIISeverity.CRITICAL, PIISeverity.HIGH, PIISeverity.MEDIUM, PIISeverity.LOW)


def is_masked(column: Column) -> bool:
    """True if the column's name or meta says it is masked, hashed, encrypted, ..."""
    name = column.name.lower()
    if any(indicator in name for indicator in _MASKING_INDICATORS):
        return True
    meta = column.meta or {}
    if meta.get("masked") or meta.get("hashed") or meta.get("encrypted"):
        return True
    return meta.get("transformation") in ("hash", "mask", "encrypt", "redact")


def severity_of(pii_types: Iterable[str]) -> PIISeverity:
    levels = [PII_SEVERITY.get(t.lower(), PIISeverity.MEDIUM) for t in pii_types]
    return min(levels, key=_SEVERITY_ORDER.index) if levels else PIISeverity.MEDIUM


@dataclass(frozen=True, slots=True)
class PIIIndicator:
    column: str
    node_id: str
    pii_types: tuple[str, ...]
    basis: PIIBasis
    confidence: Confidence
    # The direct PII columns upstream that flow into this one, along any path
    origins: tuple[str, ...] = ()
    masked: bool = False

    @property
    def severity(self) -> PIISeverity:
        return severity_of(self.pii_types)


@dataclass
class PIIIndex:
    """Every PII indicator in the graph, and the models PII may reach unseen."""

    indicators: dict[str, PIIIndicator] = field(default_factory=dict)
    # Models downstream of PII whose column lineage is unknown: node ID -> reason
    unknown: dict[str, str] = field(default_factory=dict)

    def for_node(self, node_id: str) -> list[PIIIndicator]:
        prefix = f"{node_id}#"
        return [i for c, i in self.indicators.items() if c.startswith(prefix)]

    def nodes(self) -> set[str]:
        """Nodes with an unmasked PII indicator, for the CodeLens "PII" badge."""
        return {i.node_id for i in self.indicators.values() if not i.masked}


def pii_index(graph: MetadataGraph) -> PIIIndex:
    """Direct indicators, then the flow of PII along every lineage path."""
    index = PIIIndex()
    direct = {cid: c for cid, c in graph.columns.items() if c.pii_type}
    # column -> the direct, unmasked PII origins that reach it
    origins: dict[str, set[str]] = {}
    queue: deque[str] = deque()
    for cid, column in direct.items():
        if not is_masked(column):
            origins[cid] = {cid}
            queue.append(cid)

    # Worklist to a fixed point, so every path contributes its origins (F12)
    while queue:
        cid = queue.popleft()
        carried = origins[cid]
        for step in steps(graph, cid, Direction.DOWNSTREAM, Granularity.COLUMN):
            target = graph.columns[step.ref]
            if is_masked(target):
                origins.setdefault(step.ref, set())  # reached, but the flow stops here
                continue
            known = origins.setdefault(step.ref, set())
            if not carried <= known:
                known |= carried
                queue.append(step.ref)

    for cid, found in origins.items():
        column = graph.columns[cid]
        upstream = tuple(sorted(found - {cid}))
        if cid in direct:
            basis = PIIBasis.TAG if column.pii_detected_by == "tag" else PIIBasis.PATTERN
            confidence = Confidence.DECLARED if basis is PIIBasis.TAG else Confidence.HEURISTIC
            types = tuple(sorted({column.pii_type, *_types(direct, upstream)}))
        elif upstream:
            basis = PIIBasis.LINEAGE
            tagged = any(direct[o].pii_detected_by == "tag" for o in upstream)
            confidence = Confidence.INFERRED if tagged else Confidence.HEURISTIC
            types = tuple(sorted(_types(direct, upstream)))
        else:
            continue  # a masked column the flow reached; it carries no PII on
        index.indicators[cid] = PIIIndicator(
            column=cid,
            node_id=column.node_id,
            pii_types=types,
            basis=basis,
            confidence=confidence,
            origins=upstream,
            masked=is_masked(column),
        )
    # Masked direct columns the flow never started from are still indicators
    for cid, column in direct.items():
        if cid not in index.indicators:
            basis = PIIBasis.TAG if column.pii_detected_by == "tag" else PIIBasis.PATTERN
            index.indicators[cid] = PIIIndicator(
                column=cid,
                node_id=column.node_id,
                pii_types=(column.pii_type,),
                basis=basis,
                confidence=Confidence.DECLARED if basis is PIIBasis.TAG else Confidence.HEURISTIC,
                masked=True,
            )

    index.indicators = dict(sorted(index.indicators.items()))
    index.unknown = _unknown_downstream(graph, index.nodes())
    return index


def _types(direct: dict[str, Column], origins: Iterable[str]) -> set[str]:
    return {direct[o].pii_type for o in origins if direct[o].pii_type}


def _unknown_downstream(graph: MetadataGraph, pii_nodes: set[str]) -> dict[str, str]:
    unknown: dict[str, str] = {}
    for node_id in sorted(pii_nodes):
        for step in steps(graph, node_id, Direction.DOWNSTREAM, Granularity.MODEL):
            downstream = graph.nodes[step.ref]
            if downstream.kind not in _SQL_KINDS or step.ref in unknown:
                continue
            status = graph.column_lineage.get(step.ref)
            if status is None:
                unknown[step.ref] = "column lineage not computed"
            elif status.state is LineageState.UNKNOWN:
                unknown[step.ref] = f"column lineage unknown: {status.reason}"
    return dict(sorted(unknown.items()))


@dataclass(frozen=True, slots=True)
class PIIExposure:
    """An unmasked PII indicator in a consumption layer. An indicator, not a verdict."""

    indicator: PIIIndicator
    layer: str
    severity: PIISeverity
    reason: str
    recommendation: str


@dataclass
class PIIExposureReport:
    exposures: list[PIIExposure] = field(default_factory=list)
    # Exposures (dbt) and Airflow tasks that use a node with unmasked PII
    consumers: dict[str, list[str]] = field(default_factory=dict)
    # From the index: models PII may reach where column lineage is unknown
    unknown: dict[str, str] = field(default_factory=dict)

    def severity_breakdown(self) -> dict[str, int]:
        counts = dict.fromkeys((s.value for s in _SEVERITY_ORDER), 0)
        for exposure in self.exposures:
            counts[exposure.severity.value] += 1
        return counts


def pii_exposure(
    graph: MetadataGraph, index: PIIIndex | None = None, layers: Iterable[str] | None = None
) -> PIIExposureReport:
    """Unmasked PII indicators in consumption layers, and what consumes them."""
    index = index or pii_index(graph)
    wanted = {layer.lower() for layer in layers} if layers else CONSUMPTION_LAYERS
    report = PIIExposureReport(unknown=dict(index.unknown))
    for indicator in index.indicators.values():
        node = graph.nodes[indicator.node_id]
        layer = (node.layer or "").lower()
        if indicator.masked or layer not in wanted:
            continue
        how = {
            PIIBasis.TAG: "declared",
            PIIBasis.PATTERN: "suggested by the column name",
            PIIBasis.LINEAGE: f"derived from {', '.join(indicator.origins)}",
        }[indicator.basis]
        column_name = split_column_id(indicator.column)[1]
        report.exposures.append(
            PIIExposure(
                indicator=indicator,
                layer=layer,
                severity=indicator.severity,
                reason=f"unmasked {', '.join(indicator.pii_types)} PII indicator in the "
                f"{layer} layer ({how})",
                recommendation=f"Mask, hash or encrypt {column_name}, or tag it if it is not PII",
            )
        )
    for node_id in sorted(index.nodes()):
        users = [
            s.ref
            for s in steps(graph, node_id, Direction.DOWNSTREAM, Granularity.MODEL)
            if graph.nodes[s.ref].kind in (NodeKind.EXPOSURE, NodeKind.AIRFLOW_TASK)
        ]
        if users:
            report.consumers[node_id] = sorted(users)
    return report


@dataclass
class PIITrace:
    """Where one column's PII comes from and where it goes, along every path."""

    column: str
    # Direct PII columns upstream, along any path
    origins: list[str]
    # The upstream column traversal; every way into each column is in ``reached[...].via``
    upstream: Traversal
    # Columns the PII flows to, unmasked
    downstream: list[str]
    # Columns where masking stops it
    masked_at: list[str]
    # Columns it flows to that feed nothing further
    terminals: list[str]
    # Downstream models where the flow cannot be followed
    unknown: dict[str, str]


def trace_pii(graph: MetadataGraph, column: str) -> PIITrace:
    if column not in graph.columns:
        raise KeyError(f"unknown column: {column}")
    upstream = traverse(graph, [column], Direction.UPSTREAM, Granularity.COLUMN)
    origins = sorted(
        ref for ref in upstream.reached if ref != column and graph.columns[ref].pii_type
    )

    flowing: list[str] = []
    masked: list[str] = []
    terminals: list[str] = []
    seen = {column}
    queue: deque[str] = deque([column])
    while queue:
        ref = queue.popleft()
        onward = [s.ref for s in steps(graph, ref, Direction.DOWNSTREAM, Granularity.COLUMN)]
        if ref != column and not onward:
            terminals.append(ref)
        for target in sorted(onward):
            if target in seen:
                continue
            seen.add(target)
            if is_masked(graph.columns[target]):
                masked.append(target)
                continue
            flowing.append(target)
            queue.append(target)

    reached_nodes = {split_column_id(c)[0] for c in [column, *flowing]}
    return PIITrace(
        column=column,
        origins=origins,
        upstream=upstream,
        downstream=flowing,
        masked_at=masked,
        terminals=sorted(terminals),
        unknown=_unknown_downstream(graph, reached_nodes),
    )
