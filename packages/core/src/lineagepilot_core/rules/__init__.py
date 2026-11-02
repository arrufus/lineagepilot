"""Conformance rules with five result states, a coverage-aware score and rule packs."""

from lineagepilot_core.rules.builtin import builtin_rules
from lineagepilot_core.rules.engine import active_rules, evaluate
from lineagepilot_core.rules.model import (
    Category,
    Evaluation,
    Finding,
    Outcome,
    Result,
    Rule,
    Scope,
    Score,
    Severity,
)
from lineagepilot_core.rules.pack import DEFAULT_PATH, RulePack, load_rule_pack, parse_rule_pack
from lineagepilot_core.rules.safe_regex import SafeRegex, UnsafePattern

__all__ = [
    "DEFAULT_PATH",
    "Category",
    "Evaluation",
    "Finding",
    "Outcome",
    "Result",
    "Rule",
    "RulePack",
    "SafeRegex",
    "Scope",
    "Score",
    "Severity",
    "UnsafePattern",
    "active_rules",
    "builtin_rules",
    "evaluate",
    "load_rule_pack",
    "parse_rule_pack",
]
