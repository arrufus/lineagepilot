"""Rules, results and scores (design section 5.3).

Every rule produces one of five results for each subject. ``unsupported`` and ``unknown``
are kept out of the score's denominator, and coverage is reported beside the score, so a
check that could not be made never counts as a pass (review finding F11).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

from lineagepilot_core.graph.model import (
    Column,
    Confidence,
    Evidence,
    MetadataGraph,
    Node,
    NodeKind,
)


class Result(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    NOT_APPLICABLE = "not_applicable"
    UNSUPPORTED = "unsupported"  # LineagePilot cannot make this check
    UNKNOWN = "unknown"  # the facts needed are missing


class Severity(str, Enum):
    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


SEVERITY_WEIGHTS = {Severity.CRITICAL: 10, Severity.ERROR: 5, Severity.WARNING: 2, Severity.INFO: 1}


class Category(str, Enum):
    NAMING = "naming"
    LINEAGE = "lineage"
    PII = "pii"
    DOCUMENTATION = "documentation"
    TESTING = "testing"
    STRUCTURAL = "structural"
    QUALITY = "quality"
    SEMANTIC = "semantic"
    CUSTOM = "custom"


class Scope(str, Enum):
    NODE = "node"
    COLUMN = "column"


@dataclass(frozen=True, slots=True)
class Outcome:
    result: Result
    message: str | None = None
    confidence: Confidence = Confidence.DECLARED


PASS = Outcome(Result.PASS)
NOT_APPLICABLE = Outcome(Result.NOT_APPLICABLE)

# (graph, node, column or None) -> outcome
Check = Callable[[MetadataGraph, Node, "Column | None"], Outcome]


@dataclass(frozen=True)
class Rule:
    id: str
    name: str
    description: str
    severity: Severity
    category: Category
    scope: Scope
    check: Check
    resource_types: frozenset[NodeKind] = frozenset({NodeKind.MODEL})
    rule_set: str = "custom"
    remediation: str | None = None
    # The data-capsule rule this one remaps to dbt (design section 12)
    replaces: str | None = None
    # Findings point at the YAML that documents a node, when it has one
    prefer_yaml: bool = False
    # "builtin", or the rule pack file that defined it
    source: str = "builtin"


@dataclass(frozen=True, slots=True)
class Finding:
    """A non-passing result: fail, unknown or unsupported."""

    rule_id: str
    subject: str  # node or column ID
    result: Result
    severity: Severity
    message: str
    evidence: Evidence
    remediation: str | None = None


@dataclass(frozen=True, slots=True)
class Score:
    passed: int
    failed: int
    not_applicable: int
    unsupported: int
    unknown: int
    # pass / (pass + fail) * 100; None when nothing was decided
    score: float | None
    # The same, weighted by severity
    weighted_score: float | None
    # (pass + fail) / (pass + fail + unsupported + unknown) * 100
    coverage: float | None


@dataclass
class Evaluation:
    rules: dict[str, Rule]
    findings: list[Finding] = field(default_factory=list)
    # rule ID -> result -> count, including passes and not_applicable
    counts: dict[str, dict[Result, int]] = field(default_factory=dict)
    # Problems with the rule pack, which never stop the built-in rules
    problems: list[str] = field(default_factory=list)

    def score(self, rule_ids: set[str] | None = None) -> Score:
        totals = dict.fromkeys(Result, 0)
        weighted_pass = weighted_total = 0.0
        for rule_id, counts in self.counts.items():
            if rule_ids is not None and rule_id not in rule_ids:
                continue
            for result, count in counts.items():
                totals[result] += count
            weight = SEVERITY_WEIGHTS[self.rules[rule_id].severity]
            weighted_pass += counts.get(Result.PASS, 0) * weight
            weighted_total += (counts.get(Result.PASS, 0) + counts.get(Result.FAIL, 0)) * weight

        decided = totals[Result.PASS] + totals[Result.FAIL]
        attempted = decided + totals[Result.UNSUPPORTED] + totals[Result.UNKNOWN]
        return Score(
            passed=totals[Result.PASS],
            failed=totals[Result.FAIL],
            not_applicable=totals[Result.NOT_APPLICABLE],
            unsupported=totals[Result.UNSUPPORTED],
            unknown=totals[Result.UNKNOWN],
            score=_percent(totals[Result.PASS], decided),
            weighted_score=_percent(weighted_pass, weighted_total),
            coverage=_percent(decided, attempted),
        )

    def findings_for(self, subject: str) -> list[Finding]:
        return [f for f in self.findings if f.subject == subject]


def _percent(part: float, whole: float) -> float | None:
    return round(part / whole * 100, 2) if whole else None
