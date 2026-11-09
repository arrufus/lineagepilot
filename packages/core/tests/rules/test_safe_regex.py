"""Rule-pack regexes cannot be used for ReDoS."""

from __future__ import annotations

import re
import sys
import time

import pytest

import lineagepilot_core.rules.builtin as builtin
import lineagepilot_core.rules.safe_regex as safe_regex
from lineagepilot_core.rules.safe_regex import MAX_PATTERN, MAX_SUBJECT, SafeRegex, UnsafePattern

NEEDS_311 = pytest.mark.skipif(sys.version_info < (3, 11), reason="atomic and possessive: 3.11")


@pytest.mark.parametrize(
    "pattern",
    [
        r"^(dim|fct)_",
        r"^[a-z][a-z0-9_]*$",
        r"x{2,5}y",
        r"(?=a+)b",
        r"(ab)?c",
        r"(a|b)c",
        r"^fin_",
        # From the rule-pack docs, tests and demo
        r"^fin_(dim|fct)_",
        r"^models/finance/",
        r"amount$",
        r"^models/marts/",
        r"^mart_",
        # .airflowignore lines seen in real repositories and the Airflow docs
        r"^regexp_.*",
        r"nested/*.py",
        r"ignored/",
        r"whirl.setup.d",
        r"tenant_[\d]",
        r".*_test\.py",
        r"^.*\.sql$",
        # Repeats kept apart by a character neither can take
        r"[^/]*/[^/]*\.sql$",
        r"^[a-z]+_[0-9]+$",
        r"^[a-z_]+_v[0-9]+$",
        r"\d{4}-\d{2}-\d{2}",
        r"^(dim|fct)_[a-z]+(_v\d+)?$",
        # A repeat that ends the pattern: nothing after it can fail
        r".*/tests/.*",
        r".*finance.*",
        r"^models/.*/staging/",
        # Alternatives told apart by a character, or of different fixed lengths
        r"^(stg|staging)_",
        r"^models/(marts|staging)/.*\.sql$",
        r"(test|tests)/",
        r"(^|/)tests?/",
        r"^(foo|foobar)$",
        r"^(?!tmp_)\w+$",
        # IGNORECASE matches nothing outside ASCII with [a-z] but four letters, not these
        r"k*[\u4e00-\u9fff]*$",
        r"[a-z]*[^a-z]*$",
    ],
)
def test_safe_patterns_are_accepted(pattern):
    SafeRegex(pattern)


@pytest.mark.parametrize(
    "pattern",
    [
        # Told apart by their first characters, though neither is a fixed text
        r"(?:a?b|c?d)e",
        r"(?:(?:ab)*c|d)",
        r"(?:(?:ab|cd)e|f)g",
        # By a character inside groups, or a fixed repeat
        r"(?:(dim)|(fct))_",
        r"(?:a{3}|b{3})c",
        r"(?:a{40}|b)",
        # Fixed texts of two lengths, which nothing after can take
        r"(?:x{3}|x{4})y",
        # A repeat before them that no branch lets through
        r"[ab]*(?:cd|ef)[a-f]+$",
        # One that some branch lets through, with nothing after it to bring it back into step
        r".*(?:ab|cd)x",
    ],
)
def test_alternatives_told_apart_are_accepted(pattern):
    SafeRegex(pattern)


@NEEDS_311
@pytest.mark.parametrize("pattern", [r"(?>a+)b", r"a*+b", r"^(?>[a-z]+)_\d++$", r"(?:(?>a+)b|c)"])
def test_atomic_groups_and_possessive_repeats_without_a_nested_repeat_are_accepted(pattern):
    SafeRegex(pattern)


def test_the_built_in_name_patterns_would_be_accepted(monkeypatch):
    """They are compiled directly, but a team may copy one into a rule pack."""
    seen = []
    name_matches = builtin._name_matches

    def recording(pattern, flags=re.IGNORECASE):
        seen.append(pattern)
        return name_matches(pattern, flags)

    monkeypatch.setattr(builtin, "_name_matches", recording)
    builtin.builtin_rules()
    assert len(seen) >= 4
    for pattern in seen:
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
        (r"(a)(?:\1){2}", "a backreference"),
        (r"[", "not a valid regular expression"),
        ("a" * (MAX_PATTERN + 1), "longer than"),
        (7, "not a string"),
    ],
)
def test_dangerous_or_invalid_patterns_are_refused(pattern, reason):
    with pytest.raises(UnsafePattern, match=reason):
        SafeRegex(pattern)


IN_A_ROW = "repeats or alternatives in a row that can match the same text"


@pytest.mark.parametrize(
    "pattern",
    [
        # LP-SEC-06: polynomial, each extra repeat multiplying the work by the subject's length
        r"\d+\d+$",
        r"a*a*a*b",
        r".*.*x",
        r".*a.*a.*b",
        r"a{0,250}a{0,250}b",
        # Apart only by something that can be empty, or that both can take
        r"\w+\s?\w+$",
        r"[a-z]+_?[a-z0-9]*$",
        r".*_\d+$",  # safe, but refused: write _\d+$, as matching is a search
        # Not at the end, as $ can fail
        r".*x.*$",
        # A lookahead is tried at every step of the repeat before it
        r".*(?=.*x)",
        # A long fixed repeat costs its length at every step of the repeat before it
        r".*[a-z]{250}\d",
        r"[a-z]*(?=[a-z]{250}\d)",
        # No repeat without a bound, but exponential in the pattern's length
        "a?" * 28 + "a" * 28,
        # Alternatives of different lengths whose difference what follows can take
        r"(?:a|ab)[bc](?:c|cc)d",
        # The Kelvin sign is k, ignoring case, and in this range
        r"k*[\u2100-\u214f]*$",
        # A branch the try behind can start, then the other branch's characters: acdd is
        # a|cd|d and |ac|dd
        r"[ab]*(?:ac|cd)[cd]+$",
        r"(?:x{3}|x{4})x*$",
    ],
)
def test_repeats_that_can_take_the_same_text_in_a_row_are_refused(pattern):
    with pytest.raises(UnsafePattern, match=IN_A_ROW):
        SafeRegex(pattern)


@pytest.mark.parametrize(
    "pattern",
    [r"(a|a)" * 30 + "$", r"(?:ab|\wb)" * 19 + "$", r"x(?:ab|\wb)y", r"(?:a?b|b?d)"],
)
def test_alternatives_that_can_match_the_same_text_are_refused(pattern):
    """Each doubles the work of what follows: exponential in a row, with no repeat at all."""
    with pytest.raises(UnsafePattern, match="alternatives that can match the same text"):
        SafeRegex(pattern)


@NEEDS_311
@pytest.mark.parametrize(
    ("pattern", "reason"),
    [
        # LP-SEC-06: atomic and possessive bodies were not looked into
        (r"(?>(a+)+b)", "a repeat inside a repeat"),
        (r"(?:(a+)+b)*+", "a repeat inside a repeat"),
        (r"(a+)*+$", "a repeat inside a repeat"),
        (r"(?>a+)+b", "a repeat inside a repeat"),  # safe, but refused like any other
        (r"(?:a|aa)++", "alternation inside a repeat"),
        (r"\d++\d+$", IN_A_ROW),
    ],
)
def test_atomic_groups_and_possessive_repeats_are_looked_into(pattern, reason):
    with pytest.raises(UnsafePattern, match=reason):
        SafeRegex(pattern)


def test_only_four_characters_outside_ascii_match_ascii_ignoring_case():
    """The check tells characters apart one by one only in ASCII and these four."""
    ascii_class = re.compile("[\x00-\x7f]", re.IGNORECASE)
    found = {chr(c) for c in range(128, 0x110000) if ascii_class.match(chr(c))}
    assert found == set(safe_regex._PROBE[128:])


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


def fastest(compiled, subject, runs=5):
    best = float("inf")
    for _ in range(runs):
        started = time.perf_counter()
        compiled.search(subject)
        best = min(best, time.perf_counter() - started)
    return best


def test_refused_shapes_really_are_slow_with_plain_re():
    """Guard the reason for the checks: the classic shape is exponential in re."""
    started = time.perf_counter()
    re.match(r"(a+)+$", "a" * 22 + "!")
    assert time.perf_counter() - started > 0.05


@pytest.mark.parametrize(
    ("pattern", "subject"),
    [(r"\d+\d+$", lambda n: "1" * n + "x"), (r".*.*x", lambda n: "a" * n)],
)
def test_two_repeats_in_a_row_really_are_polynomial_with_plain_re(pattern, subject):
    """Four times the subject, about 64 times the work: a search is cubic."""
    compiled = re.compile(pattern, re.IGNORECASE)
    assert fastest(compiled, subject(400)) > 10 * fastest(compiled, subject(100))


# Accepted shapes, each with the subject that makes it work hardest
ACCEPTED_AT_WORK = [
    (r"[a-z][a-z0-9_]*$", lambda n: "a" * (n - 1) + "!"),
    (r".*\.sql$", lambda n: (".sq" * n)[: n - 1] + "!"),
    (r"[^/]*/[^/]*\.sql$", lambda n: ("a/" * n)[:n]),
    (r".*finance.*", lambda n: ("financ" * n)[:n]),
    (r"(?!tmp_)\w+$", lambda n: "a" * (n - 1) + "!"),
    (r"(?=[a-z]+\d)[a-z]\w*$", lambda n: "a" * (n - 1) + "!"),
    (r"models/(marts|staging)/.*\.sql$", lambda n: ("models/marts/" + ".sq" * n)[:n]),
]


@pytest.mark.parametrize(("pattern", "subject"), ACCEPTED_AT_WORK)
def test_accepted_shapes_do_linear_work_from_each_start(pattern, subject):
    """Anchored, ten times the subject is about ten times the work, not a hundred. A search
    starts again at each character, which is why subjects are capped."""
    compiled = re.compile("^" + pattern, re.IGNORECASE)
    assert fastest(compiled, subject(100_000)) < 30 * fastest(compiled, subject(10_000))


@pytest.mark.parametrize(("pattern", "subject"), ACCEPTED_AT_WORK)
def test_accepted_shapes_finish_well_within_the_budget(pattern, subject):
    regex = SafeRegex(pattern)
    for text in (subject(MAX_SUBJECT), subject(MAX_SUBJECT - 1) + "!"):
        assert regex.search(text) is not None
    assert not regex.disabled
