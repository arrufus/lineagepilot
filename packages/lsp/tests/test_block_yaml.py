"""The quick YAML reader (task 7.10): it must give exactly the positions PyYAML gives, or step
aside so PyYAML reads the file."""

from __future__ import annotations

import random
import string
import time

import pytest

from lineagepilot_lsp import block_yaml
from lineagepilot_lsp.locate import _composed_entries, _plain_entries, yaml_entries

SECTIONS = ["models", "sources", "seeds", "exposures", "semantic_models", "metrics", "unrelated"]
QQ = "''"  # a quote inside single quotes, doubled


def name(r: random.Random) -> str:
    return r.choice(string.ascii_lowercase) + "".join(
        r.choice(string.ascii_lowercase + string.digits + "_") for _ in range(r.randint(0, 10))
    )


def plain(r: random.Random) -> str:
    words = [name(r) for _ in range(r.randint(1, 5))]
    return " ".join(words) + r.choice(["", ".", " (x)", " = 1", "/y"])


def items(r: random.Random, depth: int) -> list[str]:
    """The lines of a list of named items, each a mapping, with what dbt puts beside a name."""
    lines = []
    for _ in range(r.randint(0, 4)):
        value = name(r)
        style = r.choice(["plain", "single", "double", "doubled"])
        shown = {
            "plain": value,
            "single": f"'{value}'",
            "double": f'"{value}"',
            "doubled": f"'{value}''s'",  # a quote inside single quotes
        }[style]
        keys = [f"name: {shown}" + r.choice(["", "  # its name"])]
        for _ in range(r.randint(0, 3)):
            kind = r.choice(
                ["plain", "quoted", "flow", "block", "empty", "url", "filter", "wrapped", "escaped"]
            )
            if kind == "plain":
                keys.append(f"description: {plain(r)}")
            elif kind == "quoted":
                keys.append(f"label: '{plain(r)}'")
            elif kind == "flow":
                keys.append(f"tags: [{name(r)}, {name(r)}]")
            elif kind == "url":
                keys.append("url: https://example.com/x#y")
            elif kind == "empty":
                keys.append("config:")
            elif kind == "filter":
                # A Jinja filter in single quotes, its own quotes doubled, as dbt writes them
                dimension = f"{name(r)}__{name(r)}"
                keys.append(
                    "filter: '{{ Dimension(" + QQ + dimension + QQ + ") }} = " + QQ + "x" + QQ + "'"
                )
            elif kind == "wrapped":
                # Long strings wrapped over lines, as tools write them: plain, and quoted with a
                # line inside that looks like a comment, or like an entry
                keys.append(
                    r.choice(
                        [
                            f"description: {plain(r)}\n  {plain(r)}\n  {plain(r)}",
                            f"description: '{plain(r)}\n  # {plain(r)}\n  - name: no'",
                            f'description: "{plain(r)}\n  {plain(r)}"',
                        ]
                    )
                )
            elif kind == "escaped":
                keys.append(f'label: "{name(r)}\\t{name(r)} \\"q\\""')
            else:
                # A block scalar whose text looks like an entry: it must not count
                header = r.choice(["|", ">", ">-", "|+"])
                keys.append(f"notes: {header}\n  - name: fake\n  text")
        if r.random() < 0.5:  # the name need not come first
            keys.append(keys.pop(0))
        if depth < 2 and r.random() < 0.5:
            nested = items(r, depth + 1)
            child = "columns" if depth == 0 else "tables"
            body = "\n".join("  " + line for line in nested)
            keys.append(f"{child}:" + ("\n" + body if nested else " []"))
        width = r.choice([2, 4])
        first, *rest = "\n".join(keys).split("\n")
        lines.append("- " + " " * (width - 2) + first)
        lines += [" " * width + line if line else "" for line in rest]
    return lines


def document(r: random.Random) -> str:
    lines = ["version: 2"] if r.random() < 0.5 else []
    for _ in range(r.randint(1, 3)):
        body = items(r, 0)
        lines.append(f"{r.choice(SECTIONS)}:" + ("" if body else " []"))
        indent = r.choice(["", "  "])  # an indentless sequence, or not
        lines += [indent + line if line else "" for line in body]
        if r.random() < 0.5:
            lines += ["", "# a comment between sections"]
    return "\n".join(lines) + "\n"


@pytest.mark.parametrize("seed", range(600))
def test_the_quick_reader_gives_pyyaml_s_positions(seed):
    text = document(random.Random(seed))
    try:
        plain_entries = _plain_entries(block_yaml.read(text))
    except block_yaml.NotPlain as e:
        pytest.fail(f"dbt's ordinary YAML should be read quickly ({e}):\n{text}")
    assert plain_entries.spans == _composed_entries(text).spans


@pytest.mark.parametrize(
    "text",
    [
        "models:\n  - &m\n    name: a\n  - *m\n",  # an anchor and an alias
        "models:\n  - name: !!str a\n",  # a tag
        "models:\n\t- name: a\n",  # a tab
        "models:\n  - name: a\n    description: one # c\n      two\n",  # a comment, then more
        "models:\n  - name: a\n    description: 'open\n",  # a quote never closed
        'models:\n  - name: "a\\tb"\n',  # a name with an escape
        "models:\n  - name: 'two\n      lines'\n",  # a quoted name over lines
        "models:\n  - name: two\n      lines\n",  # a plain name over lines
        "models:\n  - name:\n",  # an empty name
        "models:\n  - name: a\n    tags: [x,\n      y]\n",  # flow over lines
        "---\nmodels: []\n---\nseeds: []\n",  # two documents
        "%YAML 1.2\n---\nmodels:\n  - name: a\n",  # a directive
        '"models":\n  - name: a\n',  # a quoted key
        "models:\n  -\n    name: a\n",  # an item on the next line
        "models:\n  - name: a\n    notes: |2\n      x\n",  # a block scalar indent
        "models:\n  - - name: a\n",  # a sequence in a sequence
        "  models:\n    - name: a\n",  # an indented top level
        "models: x\n  - name: a\n",  # an item under a scalar
        "models:\n  - name: a: b\n",  # a mapping value in a plain scalar
    ],
)
def test_what_the_quick_reader_does_not_read_is_composed(text):
    with pytest.raises(block_yaml.NotPlain):
        _plain_entries(block_yaml.read(text))
    assert yaml_entries(text).spans == _composed_entries(text).spans


def test_a_large_semantic_file_is_read_quickly_and_the_same():
    semantic = []
    for n in range(300):
        semantic.append(
            f"  - name: model_{n}\n"
            "    description: >\n      A semantic model.\n      - name: not_an_entry\n"
            f"    model: ref('model_{n}')\n"
            "    entities:\n      - name: key\n        type: primary\n        expr: id\n"
            "    dimensions:\n      - name: status\n        type: categorical\n"
            "    measures:\n      - name: total\n        agg: sum\n        expr: amount\n"
        )
    metrics = "".join(
        f"  - name: metric_{n}\n    type: simple\n    type_params:\n      measure: total\n"
        f"    filter: \"{{{{ Dimension('key__status') }}}} = 'x'\"\n"
        for n in range(900)
    )
    text = "version: 2\nsemantic_models:\n" + "".join(semantic) + "metrics:\n" + metrics
    started = time.perf_counter()
    plain = _plain_entries(block_yaml.read(text))
    quick = time.perf_counter() - started
    started = time.perf_counter()
    composed = _composed_entries(text)
    slow = time.perf_counter() - started
    assert plain.spans == composed.spans and len(plain.spans) == 1200
    assert quick * 3 < slow, (quick, slow)
