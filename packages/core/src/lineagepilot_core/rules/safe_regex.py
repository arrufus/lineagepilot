"""Regular expressions from rule packs, which are untrusted workspace input.

Closes the ReDoS class (design section 10, data-capsule DCS-61r). Python cannot stop a
running ``re`` match, and the bundle cannot ship re2 (it is compiled code), so instead:

1. Patterns are at most ``MAX_PATTERN`` characters.
2. Patterns that can backtrack exponentially are refused before use: a repeat inside a
   repeat (``(a+)+``), alternation inside a repeat (``(a|aa)*``), and backreferences.
3. Subjects are at most ``MAX_SUBJECT`` characters; longer ones are not matched.
4. Each match is timed. A pattern that ever takes longer than ``BUDGET_SECONDS`` is
   switched off for the rest of the run, so one slow pattern costs one slow match.

The repeat checks are conservative: ``(a|b)*`` is refused although it is safe; write
``[ab]*`` instead.
"""

from __future__ import annotations

import re
import time
from typing import Any

try:  # Python 3.11+
    from re import _constants as sre_constants
    from re import _parser as sre_parse
except ImportError:  # pragma: no cover - Python 3.10
    import sre_constants  # type: ignore[no-redef]
    import sre_parse  # type: ignore[no-redef]

MAX_PATTERN = 200
MAX_SUBJECT = 512
BUDGET_SECONDS = 0.05

_REPEATS = {sre_constants.MAX_REPEAT, sre_constants.MIN_REPEAT}


class UnsafePattern(ValueError):
    """A pattern that is too long, invalid, or able to backtrack exponentially."""


class SafeRegex:
    """A vetted pattern that matches with ``search``, case-insensitively."""

    def __init__(self, pattern: str) -> None:
        if not isinstance(pattern, str):
            raise UnsafePattern("the pattern is not a string")
        if len(pattern) > MAX_PATTERN:
            raise UnsafePattern(f"the pattern is longer than {MAX_PATTERN} characters")
        try:
            parsed = sre_parse.parse(pattern, re.IGNORECASE)
        except (re.error, RecursionError) as e:
            raise UnsafePattern(f"the pattern is not a valid regular expression: {e}") from None
        problem = _backtracking_risk(parsed, inside_repeat=False)
        if problem:
            raise UnsafePattern(f"the pattern could backtrack exponentially: {problem}")
        self.pattern = pattern
        self._compiled = re.compile(pattern, re.IGNORECASE)
        self.disabled = False

    def search(self, subject: str) -> bool | None:
        """True or False, or None if the match could not be run safely."""
        if self.disabled or len(subject) > MAX_SUBJECT:
            return None
        started = time.perf_counter()
        found = self._compiled.search(subject) is not None
        if time.perf_counter() - started > BUDGET_SECONDS:
            self.disabled = True
        return found


def _backtracking_risk(parsed: Any, inside_repeat: bool) -> str | None:
    for op, value in parsed:
        if op in _REPEATS:
            low, high, body = value
            if inside_repeat and high != 0:
                return "a repeat inside a repeat"
            unbounded = high == sre_constants.MAXREPEAT or high > 1
            problem = _backtracking_risk(body, inside_repeat or unbounded)
            if problem:
                return problem
        elif op is sre_constants.BRANCH:
            if inside_repeat:
                return "alternation inside a repeat"
            for branch in value[1]:
                problem = _backtracking_risk(branch, inside_repeat)
                if problem:
                    return problem
        elif op is sre_constants.SUBPATTERN:
            problem = _backtracking_risk(value[-1], inside_repeat)
            if problem:
                return problem
        elif op in (sre_constants.ASSERT, sre_constants.ASSERT_NOT):
            problem = _backtracking_risk(value[1], inside_repeat)
            if problem:
                return problem
        elif op is sre_constants.GROUPREF or op is sre_constants.GROUPREF_EXISTS:
            return "a backreference"
    return None
