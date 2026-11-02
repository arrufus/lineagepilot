"""Task 2.2 end to end: CodeLens, hover and diagnostics on the jaffle-shop fixture."""

from __future__ import annotations

import asyncio
import json
import re
import time

from lsprotocol import types

J = "model.jaffle_shop"
DOCS = "https://github.com/arrufus/lineagepilot/blob/main/docs/rules.md"


async def ready(start, **kwargs):
    client, _ = await start(**kwargs)
    status = await client.ready()
    return client, status


async def lenses(client, path) -> list[types.CodeLens]:
    return await client.text_document_code_lens_async(
        types.CodeLensParams(text_document=types.TextDocumentIdentifier(uri=path.as_uri()))
    )


def open_document(client, path, text: str | None = None) -> None:
    client.text_document_did_open(
        types.DidOpenTextDocumentParams(
            text_document=types.TextDocumentItem(
                uri=path.as_uri(),
                language_id="sql" if path.suffix == ".sql" else "yaml",
                version=1,
                text=path.read_text() if text is None else text,
            )
        )
    )


async def hover(client, path, line: int, text: str, offset: int = 2) -> types.Hover | None:
    """Hover ``offset`` characters into the first ``text`` on ``line`` (0-based)."""
    character = path.read_text().splitlines()[line].index(text) + offset
    return await client.text_document_hover_async(
        types.HoverParams(
            text_document=types.TextDocumentIdentifier(uri=path.as_uri()),
            position=types.Position(line=line, character=character),
        )
    )


def line_of(path, text: str) -> int:
    return next(i for i, line in enumerate(path.read_text().splitlines()) if line.strip() == text)


async def until(predicate, timeout: float = 30.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        await asyncio.sleep(0.05)


# CodeLens


async def test_code_lens_on_a_model_file(start, workspace):
    client, _ = await ready(start)
    [lens] = await lenses(client, workspace / "models" / "marts" / "orders.sql")
    assert lens.range.start == types.Position(line=0, character=0)
    assert lens.command.title == "↓ 1 model"
    assert lens.command.command == "lineagePilot.showImpact"
    assert list(lens.command.arguments) == [f"{J}.orders", str(workspace.resolve())]
    [lens] = await lenses(client, workspace / "models" / "marts" / "customers.sql")
    assert lens.command.title == "↓ nothing downstream · PII"


async def test_code_lens_on_yaml_entries(start, workspace):
    client, _ = await ready(start)
    orders_yml = workspace / "models" / "marts" / "orders.yml"
    [lens] = await lenses(client, orders_yml)
    assert lens.range.start.line == line_of(orders_yml, "- name: orders")
    sources = workspace / "models" / "staging" / "__sources.yml"
    found = await lenses(client, sources)
    assert len(found) == 6
    by_line = {lens.range.start.line: lens.command for lens in found}
    raw_orders = by_line[line_of(sources, "- name: raw_orders")]
    assert list(raw_orders.arguments)[0] == "source.jaffle_shop.ecom.raw_orders"
    assert raw_orders.title == "↓ 4 models"


async def test_code_lens_follows_unsaved_yaml(start, workspace):
    client, _ = await ready(start)
    orders_yml = workspace / "models" / "marts" / "orders.yml"
    open_document(client, orders_yml, "\n\n\n" + orders_yml.read_text())
    [lens] = await lenses(client, orders_yml)
    assert lens.range.start.line == line_of(orders_yml, "- name: orders") + 3


async def test_no_code_lens_without_a_graph_or_outside_the_project(start, workspace):
    client, _ = await ready(start)
    assert not await lenses(client, workspace / "macros" / "cents_to_dollars.sql")
    (workspace / "target" / "manifest.json").unlink()
    other, _ = await ready(start)
    assert not await lenses(other, workspace / "models" / "marts" / "orders.sql")


async def test_code_lenses_are_refreshed_when_a_graph_loads(start):
    client, _ = await ready(start)
    assert client.lens_refreshes >= 1


# Hover


async def test_hover_on_a_ref(start, workspace):
    client, _ = await ready(start)
    orders = workspace / "models" / "marts" / "orders.sql"
    found = await hover(client, orders, 4, "ref('stg_orders')", offset=6)
    value = found.contents.value
    assert value.startswith("### stg\\_orders")
    assert "**Upstream** (1 direct)" in value and "**raw\\_orders** (source)" in value
    assert "**Downstream** (2 direct)" in value and "**orders** (model)" in value
    assert "ref · declared, manifest" in value
    assert "In all downstream: 3 models" in value
    assert "Column lineage: resolved" in value
    text = orders.read_text().splitlines()[4]
    assert found.range.start.character == text.index("ref(")
    assert found.range.end.character == text.index("')") + 2


async def test_hover_on_a_source_crosses_to_the_seed(start, workspace):
    client, _ = await ready(start)
    found = await hover(client, workspace / "models" / "staging" / "stg_orders.sql", 4, "source(")
    value = found.contents.value
    assert value.startswith("### raw\\_orders") and "source · source\\.jaffle\\_shop" in value
    assert "**raw\\_orders** (seed) · " in value and "via table relation:" in value


async def test_hover_on_a_column(start, workspace):
    client, _ = await ready(start)
    customers = workspace / "models" / "marts" / "customers.sql"
    found = await hover(client, customers, 25, "lifetime_spend")
    value = found.contents.value
    assert value.startswith("### customers.lifetime\\_spend")
    assert "**Derived from** (1)" in value
    assert "**orders.order\\_total** · aggregate · inferred, compiled sql" in value
    assert "**Feeds** (0)" in value


async def test_hover_elsewhere(start, workspace):
    client, _ = await ready(start)
    customers = workspace / "models" / "marts" / "customers.sql"
    assert await hover(client, customers, 4, "select") is None  # not a column
    assert await hover(client, customers, 0, "with") is None
    orders_yml = workspace / "models" / "marts" / "orders.yml"
    assert await hover(client, orders_yml, 1, "name") is None  # YAML has no hovers yet
    broken = workspace / "models" / "marts" / "orders.sql"
    open_document(client, broken, "select * from {{ ref('nowhere') }}\n")
    missing = await client.text_document_hover_async(
        types.HoverParams(
            text_document=types.TextDocumentIdentifier(uri=broken.as_uri()),
            position=types.Position(line=0, character=20),
        )
    )
    assert missing.contents.value.startswith("**Not in the lineage graph:** ref\\(")
    assert (
        await client.text_document_hover_async(
            types.HoverParams(
                text_document=types.TextDocumentIdentifier(uri=broken.as_uri()),
                position=types.Position(line=5, character=0),
            )
        )
        is None
    )


async def test_hover_escapes_markdown_from_the_manifest(start, workspace):
    manifest_path = workspace / "target" / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["nodes"][f"{J}.stg_orders"]["description"] = (
        "![x](https://evil.example/x.png) [run](command:workbench.action.terminal.new) <img src=x>"
    )
    manifest_path.write_text(json.dumps(manifest))
    client, _ = await ready(start)
    found = await hover(
        client, workspace / "models" / "marts" / "orders.sql", 4, "ref('stg_orders')"
    )
    value = found.contents.value
    # Every Markdown and HTML opener from the manifest is escaped
    assert not re.search(r"(?<!\\)\]\(", value) and not re.search(r"(?<!\\)<", value)
    assert "\\!\\[x\\]\\(https://evil\\.example/x\\.png\\)" in value


# Diagnostics


async def test_rule_findings_are_published(start, workspace):
    client, _ = await ready(start)
    customers_sql = (workspace / "models" / "marts" / "customers.sql").as_uri()
    customers_yml = workspace / "models" / "marts" / "customers.yml"
    await until(lambda: customers_sql in client.diagnostics)

    by_code = {d.code: d for d in client.diagnostics[customers_sql]}
    gold = by_code["MED_001"]
    assert gold.severity == types.DiagnosticSeverity.Error and gold.source == "LineagePilot"
    assert gold.code_description.href == f"{DOCS}#med_001"
    assert gold.range.start == types.Position(line=0, character=0)
    assert "violates the layer hierarchy" in gold.message
    assert by_code["MED_005"].severity == types.DiagnosticSeverity.Warning

    yml = client.diagnostics[customers_yml.as_uri()]
    pii = next(d for d in yml if d.code == "PII_001")
    assert pii.message.startswith("PII indicator: PII column 'customer_name'")
    assert pii.range.start.line == line_of(customers_yml, "- name: customer_name")
    owner = next(d for d in yml if d.code == "DBT_009")
    assert owner.range.start.line == line_of(customers_yml, "- name: customers")
    assert owner.severity == types.DiagnosticSeverity.Warning


async def test_a_rule_pack_changes_the_diagnostics(start, workspace):
    client, status = await ready(start)
    assert status["rules"] == {
        "pack": None,
        "packLoaded": False,
        "restricted": False,
        "problems": [],
        "findings": 126,
        "score": 40.0,
        "coverage": 100.0,
    }
    pack = workspace / ".lineagepilot" / "rules.yml"
    pack.parent.mkdir()
    pack.write_text(
        "version: 1\n"
        "disable: [DBT_009]\n"
        "rules:\n"
        "  - id: ACME_001\n"
        "    name: Marts have a mart_ prefix\n"
        "    when: {path_matches: '^models/marts/'}\n"
        "    require: {name_matches: '^mart_'}\n"
    )
    client.file_changed(pack, types.FileChangeType.Created)
    status = await client.wait_for_update(lambda u: u["rules"]["packLoaded"])
    assert status["rules"]["pack"] == str(pack)
    # Custom rules report on the YAML that documents a model, when it has one
    orders_yml = (workspace / "models" / "marts" / "orders.yml").as_uri()
    customers_yml = (workspace / "models" / "marts" / "customers.yml").as_uri()
    await until(lambda: any(d.code == "ACME_001" for d in client.diagnostics.get(orders_yml, [])))
    acme = next(d for d in client.diagnostics[orders_yml] if d.code == "ACME_001")
    assert acme.code_description is None  # custom rules have no page in docs/rules.md
    assert not any(d.code == "DBT_009" for d in client.diagnostics[customers_yml])


async def test_rule_pack_problems_show_on_the_pack(start, workspace):
    pack = workspace / ".lineagepilot" / "rules.yml"
    pack.parent.mkdir()
    pack.write_text("version: 1\nrules:\n  - id: lower_case\n    name: bad id\n")
    client, status = await ready(start)
    assert status["rules"]["problems"]
    await until(lambda: pack.as_uri() in client.diagnostics)
    [problem] = client.diagnostics[pack.as_uri()]
    assert problem.code == "rule-pack" and problem.severity == types.DiagnosticSeverity.Warning


async def test_restricted_mode_runs_built_in_rules_only(start, workspace):
    pack = workspace / ".lineagepilot" / "rules.yml"
    pack.parent.mkdir()
    pack.write_text("version: 1\ndisable: [DBT_009]\n")
    client, status = await ready(start, options={"trusted": False})
    assert status["trusted"] is False
    assert status["rules"]["restricted"] and not status["rules"]["packLoaded"]
    customers_yml = (workspace / "models" / "marts" / "customers.yml").as_uri()
    await until(lambda: pack.as_uri() in client.diagnostics and customers_yml in client.diagnostics)
    assert any(d.code == "DBT_009" for d in client.diagnostics[customers_yml])
    [note] = client.diagnostics[pack.as_uri()]
    assert note.code == "restricted-mode" and note.severity == types.DiagnosticSeverity.Information


async def test_diagnostics_are_cleared_when_the_graph_is_dropped(start, workspace):
    client, _ = await ready(start)
    customers_sql = (workspace / "models" / "marts" / "customers.sql").as_uri()
    await until(lambda: client.diagnostics.get(customers_sql))
    manifest = workspace / "target" / "manifest.json"
    manifest.write_text("{")
    client.file_changed(manifest)
    await client.ready(2)
    await until(
        lambda: (
            client.diagnostics.get(customers_sql) is not None
            and not client.diagnostics[customers_sql]
        )
    )
