"""Regular expressions from rule packs and ``.airflowignore``, which are untrusted workspace input.

Closes the ReDoS class (design section 10, data-capsule DCS-61r, LP-SEC-06). Python cannot stop a
running ``re`` match, and the bundle cannot ship re2 (it is compiled code), so each match is
bounded before it starts, by the length of the subject and the shape of the pattern:

1. Patterns are at most ``MAX_PATTERN`` characters. Subjects are at most ``MAX_SUBJECT``;
   longer ones are not matched.
2. A pattern is refused if its backtracking could grow faster than the subject:
   - a repeat inside a repeat (``(a+)+``) or alternatives inside a repeat (``(a|aa)*``),
     atomic groups and possessive repeats included: exponential;
   - two repeats in a row that can take the same text (``\\d+\\d+$``, ``.*a.*b``, ``a?a?aa``):
     each one multiplies the work by the subject's length. Only a character that neither can
     take keeps them apart, so ``[a-z]+_\\d+`` is accepted. A repeat that ends the pattern is
     accepted too, since nothing after it can fail;
   - alternatives that can match the same text (``(ab|\\wb)``), exponential in a row;
   - backreferences.
   What is left backtracks at most a fixed number of times per character of the subject at
   each place a search starts: a few milliseconds at ``MAX_SUBJECT`` (the tests measure it).
3. As a backstop, a pattern whose match ever takes longer than ``BUDGET_SECONDS`` is switched
   off for the rest of the run.

The checks are conservative and say why. ``(ab|cd)*`` and ``.*_\\d+$`` are refused although
they are safe; the second is ``_\\d+$`` written longer, since matching is a search.
"""

from __future__ import annotations

import functools
import itertools
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
# A repeat costs what it can take each time it is tried. Up to this many characters, that is
# no more than a few characters of the pattern written out.
_NARROW = 16

_C = sre_constants
_CHARACTERS = {_C.LITERAL, _C.NOT_LITERAL, _C.ANY, _C.IN}
# Atomic groups and possessive repeats are new in Python 3.11
_ATOMIC = getattr(_C, "ATOMIC_GROUP", None)
_REPEATS = {_C.MAX_REPEAT, _C.MIN_REPEAT, getattr(_C, "POSSESSIVE_REPEAT", None)} - {None}
_LOOKAROUNDS = {_C.ASSERT, _C.ASSERT_NOT}
_ZERO_WIDTH = {_C.AT, _C.FAILURE}
_BACKREFERENCES = {_C.GROUPREF, _C.GROUPREF_EXISTS}

# Character sets are bit sets: one bit for each ASCII character and for each of the four
# others that IGNORECASE matches with an ASCII letter, and one for every other character,
# so that any two sets holding one of those are taken to overlap.
_PROBE = "".join(map(chr, range(128))) + "\u0130\u0131\u017f\u212a"
_OTHER = 1 << len(_PROBE)
_ALL = (_OTHER << 1) - 1
_CATEGORIES = {
    _C.CATEGORY_DIGIT: r"\d",
    _C.CATEGORY_NOT_DIGIT: r"\D",
    _C.CATEGORY_SPACE: r"\s",
    _C.CATEGORY_NOT_SPACE: r"\S",
    _C.CATEGORY_WORD: r"\w",
    _C.CATEGORY_NOT_WORD: r"\W",
}


class UnsafePattern(ValueError):
    """A pattern that is too long, invalid, or able to backtrack too much."""


class SafeRegex:
    """A vetted pattern that matches with ``search``, case-insensitively."""

    def __init__(self, pattern: str) -> None:
        if not isinstance(pattern, str):
            raise UnsafePattern("the pattern is not a string")
        if len(pattern) > MAX_PATTERN:
            raise UnsafePattern(f"the pattern is longer than {MAX_PATTERN} characters")
        try:
            parsed = sre_parse.parse(pattern, re.IGNORECASE)
        except (re.error, RecursionError, OverflowError) as e:
            raise UnsafePattern(f"the pattern is not a valid regular expression: {e}") from None
        try:
            _Walk().sequence(parsed, {}, in_repeat=False, tail=True)
        except _Refused as e:
            raise UnsafePattern(f"the pattern could backtrack too much: {e}") from None
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


class _Refused(Exception):
    pass


Live = dict[int, int]


class _Walk:
    """Follows the pattern in order, keeping track of the choices still live.

    A choice (a repeat whose count can vary, or alternatives of different lengths) lets two
    tries of the match fall out of step. The try behind must then take, with what follows,
    characters that the try ahead has taken: the choice is live while what follows can take
    them, and the characters it may have to take grow with everything the try ahead can take
    meanwhile. A second choice that can take one of them can bring the tries back into step in
    many ways, which multiplies the work, so it is refused. A long fixed repeat is refused
    there too, as it costs its length at every step of the first choice.
    """

    def __init__(self) -> None:
        self._ids = itertools.count()

    def sequence(self, items: Any, live: Live, in_repeat: bool, tail: bool) -> Live:
        last = len(items) - 1
        for index, (op, av) in enumerate(items):
            live = self.node(op, av, live, in_repeat, tail and index == last)
        return live

    def node(self, op: Any, av: Any, live: Live, in_repeat: bool, tail: bool) -> Live:
        if op in _CHARACTERS:
            bits = _characters(op, av)
            # The try behind cannot take a character this cannot match: it fails here
            return {key: seen | bits for key, seen in live.items() if seen & bits}
        if op is _C.SUBPATTERN:
            return self.sequence(av[-1], live, in_repeat, tail)
        if op is _ATOMIC:
            return self.sequence(av, live, in_repeat, tail)
        if op in _LOOKAROUNDS:
            # Tried at every step of a live choice, as if written in place; takes nothing
            self.sequence(av[1], live, in_repeat, tail=False)
            return live
        if op in _REPEATS:
            return self.repeat(*av, live, in_repeat, tail)
        if op is _C.BRANCH:
            return self.branch(av[1], live, in_repeat, tail)
        if op in _BACKREFERENCES:
            raise _Refused("a backreference")
        if op in _ZERO_WIDTH:
            return live
        raise _Refused(f"{op} is not checked")

    def repeat(
        self, low: int, high: int, body: Any, live: Live, in_repeat: bool, tail: bool
    ) -> Live:
        if in_repeat and high != 0:
            raise _Refused("a repeat inside a repeat")
        bits = _chars(body)
        varies = low != high
        # At the end, a repeat ends the match once it has its minimum, which must be cheap
        if (varies and not tail) or low * _width(body) > _NARROW:
            _refuse_overlap(live, _first(body))
        after = dict(self.sequence(body, live, in_repeat or high > 1, tail and high <= 1))
        if low == 0:
            # The try ahead may take it and the try behind skip it
            after.update({key: seen | bits for key, seen in live.items()})
        if varies:
            after[next(self._ids)] = bits
        return after

    def branch(self, branches: Any, live: Live, in_repeat: bool, tail: bool) -> Live:
        if in_repeat:
            raise _Refused("alternation inside a repeat")
        varies = _alternatives(branches)
        bits = start = 0
        for branch in branches:
            bits |= _chars(branch)
            start |= _first(branch)
        if varies and not tail:
            _refuse_overlap(live, start)
        after: Live = {}
        for branch in branches:
            # The try behind starts a branch on a character it has to take. After that, the
            # try ahead may have taken any branch, and the try behind may have to take that.
            empty, first = _nullable(branch), _first(branch)
            entering = {key: seen | bits for key, seen in live.items() if empty or seen & first}
            for key, seen in self.sequence(branch, entering, in_repeat, tail).items():
                after[key] = after.get(key, 0) | seen
        if varies:
            after[next(self._ids)] = bits
        return after


def _refuse_overlap(live: Live, bits: int) -> None:
    if any(seen & bits for seen in live.values()):
        raise _Refused("repeats or alternatives in a row that can match the same text")


def _alternatives(branches: Any) -> bool:
    """Whether the alternatives are a choice: texts of fixed, different lengths, one of which
    can start like another, as ``test(|s)``. Alternatives that a character tells apart are no
    choice; any others could match the same text, and are refused."""
    varies = False
    seen: list[tuple[list[int], bool, int, bool]] = []
    for branch in branches:
        prefix, whole = _prefix(branch)
        first, empty = _first(branch), _nullable(branch)
        for prefix2, whole2, first2, empty2 in seen:
            # Told apart at a place where both must have a character
            if any(not a & b for a, b in zip(prefix, prefix2, strict=False)):
                continue
            if not empty and not empty2 and not first & first2:
                continue
            if whole and whole2 and len(prefix) != len(prefix2):
                varies = True
                continue
            raise _Refused("alternatives that can match the same text")
        seen.append((prefix, whole, first, empty))
    return varies


def _characters(op: Any, av: Any) -> int:
    """The bit set of what one LITERAL, NOT_LITERAL, ANY or IN node matches."""
    if op is _C.LITERAL:
        return _bits(f"[\\U{av:08x}]", av >= 128)
    if op is _C.NOT_LITERAL:
        return _bits(f"[^\\U{av:08x}]", True)
    if op is _C.ANY:
        return _ALL
    parts, other = [], False
    for kind, value in av:
        if kind is _C.NEGATE:
            parts.insert(0, "^")
            other = True
        elif kind is _C.LITERAL:
            parts.append(f"\\U{value:08x}")
            other = other or value >= 128
        elif kind is _C.RANGE:
            parts.append(f"\\U{value[0]:08x}-\\U{value[1]:08x}")
            other = other or value[1] >= 128
        elif kind is _C.CATEGORY and value in _CATEGORIES:
            parts.append(_CATEGORIES[value])
            other = True
        else:
            return _ALL
    return _bits(f"[{''.join(parts)}]", other)


@functools.lru_cache(maxsize=1024)
def _bits(character_class: str, other: bool) -> int:
    """Asks ``re`` itself, so case-insensitive matching is exactly what the match will do."""
    bits = _OTHER if other else 0
    for found in re.finditer(character_class, _PROBE, re.IGNORECASE):
        bits |= 1 << found.start()
    return bits


def _chars(items: Any) -> int:
    """Every character a match of these items can take."""
    bits = 0
    for op, av in items:
        if op in _CHARACTERS:
            bits |= _characters(op, av)
        elif op is _C.SUBPATTERN:
            bits |= _chars(av[-1])
        elif op is _ATOMIC:
            bits |= _chars(av)
        elif op in _REPEATS and av[1]:
            bits |= _chars(av[2])
        elif op is _C.BRANCH:
            for branch in av[1]:
                bits |= _chars(branch)
    return bits


def _nullable(items: Any) -> bool:
    for op, av in items:
        if op in _CHARACTERS:
            return False
        if op is _C.SUBPATTERN and not _nullable(av[-1]):
            return False
        if op is _ATOMIC and not _nullable(av):
            return False
        if op in _REPEATS and av[0] and not _nullable(av[2]):
            return False
        if op is _C.BRANCH and not any(_nullable(branch) for branch in av[1]):
            return False
    return True


def _first(items: Any) -> int:
    """The characters a match of these items can start with."""
    bits = 0
    for op, av in items:
        if op in _CHARACTERS:
            return bits | _characters(op, av)
        if op is _C.SUBPATTERN or op is _ATOMIC:
            body = av[-1] if op is _C.SUBPATTERN else av
            bits |= _first(body)
            if not _nullable(body):
                return bits
        elif op in _REPEATS:
            bits |= _first(av[2]) if av[1] else 0
            if av[0] and not _nullable(av[2]):
                return bits
        elif op is _C.BRANCH:
            for branch in av[1]:
                bits |= _first(branch)
            if not any(_nullable(branch) for branch in av[1]):
                return bits
    return bits


def _prefix(items: Any, limit: int = 32) -> tuple[list[int], bool]:
    """What a match must start with, one bit set per character, and whether that is all of it:
    a text of fixed length."""
    sets: list[int] = []
    for op, av in items:
        if len(sets) > limit:
            break
        if op in _CHARACTERS:
            sets.append(_characters(op, av))
        elif op in _ZERO_WIDTH or op in _LOOKAROUNDS:
            continue
        elif op is _C.SUBPATTERN or op is _ATOMIC:
            more, whole = _prefix(av[-1] if op is _C.SUBPATTERN else av, limit)
            sets += more
            if not whole:
                return sets[:limit], False
        elif op in _REPEATS:
            low, high, body = av
            more, whole = _prefix(body, limit)
            if whole:
                sets += more * min(low, limit + 1)
                if low == high:
                    continue
            elif low:
                sets += more
            return sets[:limit], False
        else:
            return sets[:limit], False
    if len(sets) > limit:
        return sets[:limit], False
    return sets, True


def _width(items: Any) -> int:
    """The most characters a match of these items can take; anything over the longest
    subject counts the same."""
    total = 0
    for op, av in items:
        if op in _CHARACTERS:
            total += 1
        elif op is _C.SUBPATTERN:
            total += _width(av[-1])
        elif op is _ATOMIC:
            total += _width(av)
        elif op in _REPEATS:
            total += min(av[1], MAX_SUBJECT + 1) * _width(av[2])
        elif op is _C.BRANCH:
            total += max(_width(branch) for branch in av[1])
        elif op in _BACKREFERENCES:
            total += MAX_SUBJECT + 1
    return min(total, MAX_SUBJECT + 1)
