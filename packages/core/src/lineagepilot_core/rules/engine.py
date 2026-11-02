"""Evaluate rules over the metadata graph."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import replace

from lineagepilot_core.graph.model import Evidence, Location, MetadataGraph, Node, Origin
from lineagepilot_core.rules.builtin import builtin_rules
from lineagepilot_core.rules.model import (
    Evaluation,
    Finding,
    Outcome,
    Result,
    Rule,
    Scope,
)
from lineagepilot_core.rules.pack import RulePack

logger = logging.getLogger(__name__)

_RECORDED = frozenset({Result.FAIL, Result.UNKNOWN, Result.UNSUPPORTED})


def active_rules(pack: RulePack | None = None, rules: Iterable[Rule] | None = None) -> list[Rule]:
    """Built-in (or given) rules plus the pack's, less disabled ones, with severity overrides."""
    chosen = list(rules) if rules is not None else builtin_rules()
    if pack is None:
        return chosen
    chosen += pack.rules
    return [
        replace(rule, severity=pack.severities[rule.id]) if rule.id in pack.severities else rule
        for rule in chosen
        if rule.id not in pack.disabled
    ]


def evaluate(
    graph: MetadataGraph,
    pack: RulePack | None = None,
    rules: Iterable[Rule] | None = None,
) -> Evaluation:
    """Run every active rule on every subject it applies to.

    Findings are kept for ``fail``, ``unknown`` and ``unsupported`` results; every result
    is counted, for the score and its coverage. A check that raises is ``unknown`` for
    that subject, never a crash and never a pass.
    """
    active = active_rules(pack, rules)
    evaluation = Evaluation(rules={rule.id: rule for rule in active})
    if pack is not None:
        evaluation.problems = list(pack.problems)
        known = set(evaluation.rules) | pack.disabled
        for rule_id in sorted(set(pack.severities) - known):
            evaluation.problems.append(f"{pack.path}: severity for unknown rule {rule_id}")

    nodes = sorted(graph.nodes.values(), key=lambda n: n.id)
    for rule in active:
        counts = evaluation.counts.setdefault(rule.id, {})
        for node in nodes:
            if node.kind not in rule.resource_types:
                continue
            subjects = graph.columns_of(node.id) if rule.scope is Scope.COLUMN else [None]
            for column in subjects:
                outcome = _run(rule, graph, node, column)
                counts[outcome.result] = counts.get(outcome.result, 0) + 1
                if outcome.result in _RECORDED:
                    evaluation.findings.append(_finding(rule, graph, node, column, outcome))

    evaluation.findings.sort(key=lambda f: (f.subject, f.rule_id))
    return evaluation


def _run(rule: Rule, graph: MetadataGraph, node: Node, column) -> Outcome:
    try:
        return rule.check(graph, node, column)
    except Exception as e:  # a rule bug, or data it did not expect
        logger.exception("Rule %s failed on %s", rule.id, node.id)
        return Outcome(Result.UNKNOWN, f"the rule could not be evaluated: {type(e).__name__}")


def _finding(rule: Rule, graph: MetadataGraph, node: Node, column, outcome: Outcome) -> Finding:
    path = (
        (node.patch_path or node.file_path)
        if rule.prefer_yaml
        else (node.file_path or node.patch_path)
    )
    return Finding(
        rule_id=rule.id,
        subject=column.id if column is not None else node.id,
        result=outcome.result,
        severity=rule.severity,
        message=outcome.message or rule.description,
        evidence=Evidence(
            origin=Origin.MANIFEST,
            confidence=outcome.confidence,
            artifact=graph.manifest,
            location=Location(path) if path else None,
        ),
        remediation=rule.remediation,
    )
