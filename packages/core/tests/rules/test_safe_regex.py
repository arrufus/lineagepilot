"""Rule-pack regexes cannot be used for ReDoS."""

from __future__ import annotations

import time

import pytest

import lineagepilot_core.rules.safe_regex as safe_regex
from lineagepilot_core.rules.safe_regex import MAX_PATTERN, MAX_SUBJECT, SafeRegex, UnsafePattern


@pytest.mark.parametrize(
    "pattern",
    [r"^(dim|fct)_", r"^[a-z][a-z0-9_]*$", r"x{2,5}y", r"(?=a+)b", r"(ab)?c", r"(a|b)c", r"^fin_"],
)
def test_safe_patterns_are_accepted(pattern):
    SafeRegex(pattern)


@pytest.mark.parametrize(
    ("pattern", "reason"),
    [
        (r"(a+)+$", "a repeat inside a repeat"),
        (r"(a*)*", "a repeat inside a repeat"),
        (r"(\w+\s?)*$", "a repeat inside a repeat"),
        (r"(.*a){12}", "a repeat inside a repeat"),
        (r"(?=(a+)+)b", "a repeat inside a repeat"),
        (r"(a|aa)*$", "alternation inside a repeat"),
        (r"(x(a|aa))+", "alternation inside a repeat"),
        (r"(\d+)\1", "a backreference"),
        (r"(a)?(?(1)b|c)", "a backreference"),
        (r"[", "not a valid regular expression"),
        ("a" * (MAX_PATTERN + 1), "longer than"),
        (7, "not a string"),
    ],
)
def test_dangerous_or_invalid_patterns_are_refused(pattern, reason):
    with pytest.raises(UnsafePattern, match=reason):
        SafeRegex(pattern)


def test_matching_is_a_case_insensitive_search():
    regex = SafeRegex(r"^fin_")
    assert regex.search("FIN_orders") is True
    assert regex.search("orders_fin_") is False


def test_long_subjects_are_not_matched():
    assert SafeRegex("a").search("a" * (MAX_SUBJECT + 1)) is None


def test_a_slow_pattern_is_switched_off(monkeypatch):
    regex = SafeRegex("a")
    ticks = iter([0.0, 1.0])
    monkeypatch.setattr(safe_regex.time, "perf_counter", lambda: next(ticks))
    assert regex.search("a") is True  # the slow match still answers
    assert regex.disabled
    assert regex.search("a") is None  # but the pattern is not run again


def test_refused_shapes_really_are_slow_with_plain_re():
    """Guard the reason for the checks: the classic shape is exponential in re."""
    import re

    started = time.perf_counter()
    re.match(r"(a+)+$", "a" * 22 + "!")
    assert time.perf_counter() - started > 0.05
