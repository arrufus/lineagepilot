"""Generate the protocol validators from the shared JSON Schema (task 2.3, design section 9).

    python protocol/generate.py           write both files
    python protocol/generate.py --check   fail if either file is out of date (CI)

One schema, two outputs:

- ``extension/src/protocol.ts``: zod schemas and their TypeScript types.
- ``packages/lsp/src/lineagepilot_lsp/protocol.py``: frozen dataclasses whose
  ``from_json`` validates against the schema, which the module carries as data.

Only the JSON Schema keywords both outputs implement are accepted; anything else is an
error here rather than a check one side silently skips. Standard library only.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = ROOT / "protocol" / "lineage-protocol.schema.json"
TS_OUT = ROOT / "extension" / "src" / "protocol.ts"
PY_OUT = ROOT / "packages" / "lsp" / "src" / "lineagepilot_lsp" / "protocol.py"

KEYWORDS = {
    "type", "enum", "$ref", "anyOf", "properties", "required", "additionalProperties",
    "items", "minItems", "maxItems", "minLength", "maxLength", "minimum", "maximum",
    "description",
}  # fmt: skip
TYPES = {"string", "integer", "number", "boolean", "null", "object", "array"}
HEADER = "Generated from protocol/lineage-protocol.schema.json by protocol/generate.py. Do not edit."


class SchemaError(ValueError):
    pass


def load() -> dict[str, Any]:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    defs = schema["$defs"]
    for name, node in defs.items():
        check_node(node, name, defs, top=True)
    for method, pair in schema["x-requests"].items():
        for role in ("params", "result"):
            if pair[role] not in defs:
                raise SchemaError(f"{method} {role}: no $defs entry {pair[role]}")
    for method, name in schema["x-notifications"].items():
        if name not in defs:
            raise SchemaError(f"{method}: no $defs entry {name}")
    return schema


def check_node(node: Any, path: str, defs: dict[str, Any], top: bool = False) -> None:
    if not isinstance(node, dict):
        raise SchemaError(f"{path}: not a schema object")
    unknown = set(node) - KEYWORDS
    if unknown:
        raise SchemaError(f"{path}: unsupported keywords {sorted(unknown)}")
    if "$ref" in node:
        if set(node) - {"$ref", "description"}:
            raise SchemaError(f"{path}: $ref cannot have siblings other than description")
        ref_name(node["$ref"], defs, path)
        return
    if "anyOf" in node:
        for i, branch in enumerate(node["anyOf"]):
            check_node(branch, f"{path}.anyOf[{i}]", defs)
        return
    if "enum" in node:
        if not all(isinstance(v, str) for v in node["enum"]):
            raise SchemaError(f"{path}: only string enums are supported")
        return
    kind = node.get("type")
    if kind not in TYPES:
        raise SchemaError(f"{path}: type must be one of {sorted(TYPES)}")
    if kind == "object":
        props = node.get("properties")
        extra = node.get("additionalProperties", True)
        if props is None:
            if not isinstance(extra, dict):
                raise SchemaError(f"{path}: a map needs an additionalProperties schema")
            check_node(extra, f"{path}.additionalProperties", defs)
            return
        if not top and props:
            raise SchemaError(f"{path}: objects with properties must be named in $defs")
        if extra is not False:
            raise SchemaError(f"{path}: records must set additionalProperties to false")
        for key, value in props.items():
            check_node(value, f"{path}.{key}", defs)
        missing = set(node.get("required", [])) - set(props)
        if missing:
            raise SchemaError(f"{path}: required names unknown properties {sorted(missing)}")
    elif kind == "array":
        check_node(node["items"], f"{path}.items", defs)


def ref_name(ref: str, defs: dict[str, Any], path: str = "") -> str:
    match = re.fullmatch(r"#/\$defs/(\w+)", ref)
    if not match or match.group(1) not in defs:
        raise SchemaError(f"{path}: bad $ref {ref}")
    return match.group(1)


def is_record(node: dict[str, Any]) -> bool:
    return node.get("type") == "object" and "properties" in node


def references(node: Any) -> set[str]:
    if isinstance(node, dict):
        found = {node["$ref"].rsplit("/", 1)[1]} if "$ref" in node else set()
        for value in node.values():
            found |= references(value)
        return found
    if isinstance(node, list):
        return set().union(*(references(v) for v in node)) if node else set()
    return set()


def ordered(defs: dict[str, Any]) -> list[str]:
    """Definitions with every reference before its use (zod needs that order)."""
    done: list[str] = []
    visiting: set[str] = set()

    def visit(name: str) -> None:
        if name in done:
            return
        if name in visiting:
            raise SchemaError(f"recursive definition: {name}")
        visiting.add(name)
        for dep in sorted(references(defs[name])):
            visit(dep)
        visiting.discard(name)
        done.append(name)

    for name in defs:
        visit(name)
    return done


def snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def const_name(name: str) -> str:
    return snake(name).upper()


# TypeScript


def zod(node: dict[str, Any]) -> str:
    if "$ref" in node:
        return node["$ref"].rsplit("/", 1)[1]
    if "anyOf" in node:
        branches = node["anyOf"]
        nulls = [b for b in branches if b.get("type") == "null"]
        rest = [b for b in branches if b.get("type") != "null"]
        if len(rest) == 1:
            inner = zod(rest[0])
            return f"{inner}.nullable()" if nulls else inner
        return f"z.union([{', '.join(zod(b) for b in branches)}])"
    if "enum" in node:
        return f"z.enum([{', '.join(json.dumps(v) for v in node['enum'])}])"
    kind = node["type"]
    if kind == "string":
        return "z.string()" + _bounds(node, "minLength", "maxLength")
    if kind == "integer":
        return "z.int()" + _bounds(node, "minimum", "maximum")
    if kind == "number":
        return "z.number()" + _bounds(node, "minimum", "maximum")
    if kind == "boolean":
        return "z.boolean()"
    if kind == "null":
        return "z.null()"
    if kind == "array":
        return f"z.array({zod(node['items'])})" + _bounds(node, "minItems", "maxItems")
    if not is_record(node):
        return f"z.record(z.string(), {zod(node['additionalProperties'])})"
    required = set(node.get("required", []))
    fields = [
        f"  {json.dumps(key) if not key.isidentifier() else key}: {zod(value)}"
        + ("" if key in required else ".optional()")
        + ","
        for key, value in node["properties"].items()
    ]
    return "z.strictObject({\n" + "\n".join(fields) + "\n})" if fields else "z.strictObject({})"


def _bounds(node: dict[str, Any], low: str, high: str) -> str:
    out = ""
    if low in node:
        out += f".min({node[low]})"
    if high in node:
        out += f".max({node[high]})"
    return out


def typescript(schema: dict[str, Any]) -> str:
    defs = schema["$defs"]
    lines = [
        f"// {HEADER}",
        "// Validates everything the language server sends to the extension (design section 9).",
        "import * as z from 'zod';",
        "",
        f"export const PROTOCOL_VERSION = {schema['x-protocol-version']};",
        "",
        "export const ErrorCode = {",
        *(f"  {name}: {code}," for name, code in schema["x-error-codes"].items()),
        "} as const;",
        "",
    ]
    for name in ordered(defs):
        node = defs[name]
        if "description" in node:
            lines.append(f"/** {node['description']} */")
        lines.append(f"export const {name} = {zod(node)};")
        lines.append(f"export type {name} = z.infer<typeof {name}>;")
        lines.append("")
    lines.append("export const requests = {")
    for method, pair in schema["x-requests"].items():
        lines.append(f"  '{method}': {{ params: {pair['params']}, result: {pair['result']} }},")
    lines += ["} as const;", "export type RequestMethod = keyof typeof requests;", ""]
    lines.append("export const notifications = {")
    for method, name in schema["x-notifications"].items():
        lines.append(f"  '{method}': {name},")
    lines += ["} as const;", "export type NotificationMethod = keyof typeof notifications;", ""]
    return "\n".join(lines)


# Python


PY_RUNTIME = '''
class ValidationError(ValueError):
    """A message that does not match the protocol schema."""

    def __init__(self, path: str, message: str) -> None:
        super().__init__(f"{path}: {message}")
        self.path = path


def validate(type_name: str, value: Any) -> None:
    """Raise ValidationError unless ``value`` matches the named definition."""
    _check(DEFS[type_name], value, type_name)


def _check(node: dict[str, Any], value: Any, path: str) -> None:
    if "$ref" in node:
        _check(DEFS[node["$ref"].rsplit("/", 1)[1]], value, path)
        return
    if "anyOf" in node:
        errors = []
        for branch in node["anyOf"]:
            try:
                _check(branch, value, path)
                return
            except ValidationError as e:
                errors.append(str(e))
        raise ValidationError(path, "matches none of: " + "; ".join(errors))
    if "enum" in node:
        if not isinstance(value, str) or value not in node["enum"]:
            raise ValidationError(path, f"must be one of {node['enum']}, not {_show(value)}")
        return
    kind = node["type"]
    if not _TYPE_CHECKS[kind](value):
        raise ValidationError(path, f"must be {kind}, not {_show(value)}")
    if kind == "string":
        _within(len(value), node, "minLength", "maxLength", path, "characters")
    elif kind in ("integer", "number"):
        _within(value, node, "minimum", "maximum", path, "")
    elif kind == "array":
        _within(len(value), node, "minItems", "maxItems", path, "items")
        for i, item in enumerate(value):
            _check(node["items"], item, f"{path}[{i}]")
    elif kind == "object":
        props = node.get("properties")
        if props is None:
            for key, item in value.items():
                _check(node["additionalProperties"], item, f"{path}.{key}")
            return
        for key in node.get("required", []):
            if key not in value:
                raise ValidationError(path, f"missing {key!r}")
        for key, item in value.items():
            if key not in props:
                raise ValidationError(path, f"unexpected {key!r}")
            _check(props[key], item, f"{path}.{key}")


def _within(size: float, node: dict[str, Any], low: str, high: str, path: str, unit: str) -> None:
    if low in node and size < node[low]:
        raise ValidationError(path, f"must be at least {node[low]} {unit}".rstrip())
    if high in node and size > node[high]:
        raise ValidationError(path, f"must be at most {node[high]} {unit}".rstrip())


def _show(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= 40 else text[:37] + "..."


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return (_is_int(value) or isinstance(value, float)) and value == value and abs(value) != float("inf")


_TYPE_CHECKS = {
    "string": lambda v: isinstance(v, str),
    "integer": _is_int,
    "number": _is_number,
    "boolean": lambda v: isinstance(v, bool),
    "null": lambda v: v is None,
    "object": lambda v: isinstance(v, dict) and all(isinstance(k, str) for k in v),
    "array": lambda v: isinstance(v, list),
}


def _build(node: dict[str, Any], value: Any) -> Any:
    """A validated value, with records as their dataclasses."""
    if "$ref" in node:
        name = node["$ref"].rsplit("/", 1)[1]
        record = RECORDS.get(name)
        return record._build(value) if record else _build(DEFS[name], value)
    if "anyOf" in node:
        for branch in node["anyOf"]:
            try:
                _check(branch, value, "")
            except ValidationError:
                continue
            return _build(branch, value)
    if isinstance(value, list):
        return [_build(node["items"], item) for item in value]
    if isinstance(value, dict) and "additionalProperties" in node:
        return {key: _build(node["additionalProperties"], item) for key, item in value.items()}
    return value


def _unbuild(value: Any) -> Any:
    if hasattr(value, "to_json"):
        return value.to_json()
    if isinstance(value, list):
        return [_unbuild(item) for item in value]
    if isinstance(value, dict):
        return {key: _unbuild(item) for key, item in value.items()}
    return value


class _Record:
    """Base of the generated dataclasses."""

    _NAME: ClassVar[str]
    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]]  # (attribute, JSON key, required)

    @classmethod
    def from_json(cls, data: Any) -> Any:
        """Validate a decoded JSON message and build the dataclass."""
        validate(cls._NAME, data)
        return cls._build(data)

    @classmethod
    def _build(cls, data: dict[str, Any]) -> Any:
        props = DEFS[cls._NAME]["properties"]
        return cls(**{attr: _build(props[key], data[key]) for attr, key, _ in cls._FIELDS if key in data})

    def to_json(self) -> dict[str, Any]:
        out = {}
        for attr, key, required in self._FIELDS:
            value = getattr(self, attr)
            if required or value is not None:
                out[key] = _unbuild(value)
        return out
'''


def py_type(node: dict[str, Any], defs: dict[str, Any]) -> str:
    if "$ref" in node:
        return node["$ref"].rsplit("/", 1)[1]
    if "anyOf" in node:
        return " | ".join(dict.fromkeys(py_type(b, defs) for b in node["anyOf"]))
    if "enum" in node:
        return f"Literal[{', '.join(json.dumps(v) for v in node['enum'])}]"
    kind = node["type"]
    if kind == "array":
        return f"list[{py_type(node['items'], defs)}]"
    if kind == "object":
        return f"dict[str, {py_type(node['additionalProperties'], defs)}]" if not is_record(node) else "dict[str, Any]"
    return {"string": "str", "integer": "int", "number": "float", "boolean": "bool", "null": "None"}[kind]


def python(schema: dict[str, Any]) -> str:
    defs = schema["$defs"]
    lines = [
        f'"""{HEADER}',
        "",
        "The lineage/* protocol (design section 9). The server validates every request it",
        "receives with ``<Params>.from_json``; tests validate what it sends with ``validate``.",
        '"""',
        "",
        "# ruff: noqa: E501",
        "from __future__ import annotations",
        "",
        "from dataclasses import dataclass",
        "from typing import Any, ClassVar, Literal",
        "",
        f"PROTOCOL_VERSION = {schema['x-protocol-version']}",
        "",
        "# method -> (params definition, result definition)",
        f"REQUESTS = {json.dumps({m: [p['params'], p['result']] for m, p in schema['x-requests'].items()}, indent=4)}",
        "",
        "# method -> params definition",
        f"NOTIFICATIONS = {json.dumps(schema['x-notifications'], indent=4)}",
        "",
        "",
        "class ErrorCode:",
        *(f"    {const_name(name)} = {code}" for name, code in schema["x-error-codes"].items()),
        "",
        "",
        "# The schema's definitions, as data for the validator",
        f"DEFS: dict[str, Any] = {_py_literal(defs)}",
        PY_RUNTIME,
    ]
    for name in ordered(defs):
        node = defs[name]
        if is_record(node):
            continue
        lines += ["", f"{name} = {py_type(node, defs)}"]
    records = [name for name in ordered(defs) if is_record(defs[name])]
    for name in records:
        node = defs[name]
        required = node.get("required", [])
        props = node["properties"]
        lines += ["", "", "@dataclass(frozen=True)", f"class {name}(_Record):"]
        if "description" in node:
            lines.append(f'    """{node["description"]}"""')
            lines.append("")
        lines.append(f'    _NAME: ClassVar[str] = "{name}"')
        fields = [(snake(key), key, key in required) for key in props]
        lines.append(f"    _FIELDS: ClassVar[tuple[tuple[str, str, bool], ...]] = {tuple(fields)!r}")
        if props:
            lines.append("")
        # Required fields first, so the optional ones can default to None
        for attr, key, req in sorted(fields, key=lambda f: not f[2]):
            annotation = py_type(props[key], defs)
            lines.append(f"    {attr}: {annotation}" if req else f"    {attr}: {annotation} | None = None")
    lines += ["", "", "RECORDS: dict[str, type[_Record]] = {", *(f'    "{n}": {n},' for n in records), "}", ""]
    return "\n".join(lines)


def _py_literal(value: Any) -> str:
    """A Python literal for JSON data (true/false/null spelled as Python)."""
    return repr(value)


def outputs(schema: dict[str, Any]) -> dict[Path, str]:
    return {TS_OUT: typescript(schema), PY_OUT: python(schema)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="fail if a generated file is stale")
    args = parser.parse_args(argv)
    try:
        files = outputs(load())
    except SchemaError as e:
        sys.stderr.write(f"schema error: {e}\n")
        return 2
    stale = []
    for path, text in files.items():
        current = path.read_text(encoding="utf-8") if path.is_file() else None
        if current == text:
            continue
        if args.check:
            stale.append(path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8", newline="\n")
            sys.stdout.write(f"wrote {path.relative_to(ROOT)}\n")
    if stale:
        names = ", ".join(str(p.relative_to(ROOT)) for p in stale)
        sys.stderr.write(f"out of date: {names}; run python protocol/generate.py\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
