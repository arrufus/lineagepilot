# Demo script

How to demonstrate LineagePilot from a checkout of this repository: what to prepare, what to click, what the audience should see, and what to say.

**What the build contains.** Phases 0–4 are complete: the `lineagepilot_core` library and its CLI (Phase 1), the language server and the VS Code extension (Phase 2), the lineage graph (Phase 3) and the Change Impact Review (Phase 4). Part 4 below covers the graph and Part 6 the review; skip them to show only what Phase 2 delivered. Airflow linkage (Phase 5) is not built yet: the empty **Airflow** view says so, and so does the review.

**The project you demo on** is `fixtures/jaffle-shop`: dbt Labs' sample project with the `manifest.json` and `catalog.json` that dbt 1.12 built from it. It has 13 models, 6 sources and 6 seeds, column lineage for every model, and no exposures. dbt does not need to be installed. Part 6 uses `fixtures/impact-shop`, a small project of our own with an exposure, owners and a model that cannot be parsed, and the artifacts real dbt wrote before and after a column rename.

| Part | What the audience sees | Phase | Time |
|---|---|---|---|
| 1 | The core from the command line: index and impact, model and column | 1 | 4 min |
| 2 | In the editor: status bar, CodeLens, hovers, Problems, the Lineage view | 2 | 8 min |
| 3 | Rule packs, the staleness warning, exports | 2 | 5 min |
| 4 | The lineage graph | 3 | 6 min |
| 5 | A cloned repository that is not trusted | 2 | 4 min |
| 6 | The Change Impact Review: rename a column, see what it reaches, paste it into a pull request | 4 | 6 min |

About 30 minutes in all. Parts 3 and 5 can be cut for a short demo.

---

## Prepare (once, about 10 minutes)

### What you need

- macOS or Linux. The commands below are for a POSIX shell; on Windows they need translating.
- [uv](https://docs.astral.sh/uv/), Node.js 22 or later (CI uses 22), git.
- VS Code 1.93 or later (CI tests 1.144). The `code` command does not have to be on `PATH`.
- A network connection for the preparation only. The demo itself runs offline.

**Use a standalone terminal** (Terminal, iTerm, a Linux terminal), not VS Code's integrated terminal. VS Code's terminal passes variables such as `ELECTRON_RUN_AS_NODE` to anything started from it, and `code` then misbehaves.

Set these in that terminal, and again in any new terminal you use. `REPO` is your checkout. Keep `DEMO` short: macOS limits the length of the socket path VS Code creates inside it.

```bash
export UV_PYTHON_PREFERENCE=only-managed   # uv's own Pythons, never Homebrew's or the system's
REPO=~/lineagepilot
DEMO=~/lp-demo
CODE=code   # on macOS without `code` on PATH:
# CODE="/Applications/Visual Studio Code.app/Contents/Resources/app/bin/code"
uv python install 3.13 && PY="$(uv python find 3.13)"   # a no-op once installed
```

### 1. Build the extension

```bash
cd "$REPO/extension"
npm ci                                       # its audit count includes dev tools; see below
"$PY" scripts/bundle_libs.py                 # the server's pure-Python libraries, from the hashed lock
npm run package                              # dist/lineagepilot.vsix
"$PY" scripts/check_vsix.py dist/lineagepilot.vsix

cd "$REPO/packages/core" && uv sync --locked # the CLI for Part 1
cd "$REPO/packages/lsp"  && uv sync --locked # only for the check in step 4
```

The last line of the VSIX check should be `VSIX check passed: universal, pure Python, expected contents, within budget`. Its warning that there is no licence is expected (decision D-5). `npm ci` reports advisories across all packages, including test and packaging tools that never ship. For what ships, `npm audit --omit=dev` should list only minimatch through vscode-languageclient (LP-dy1).

### 2. Make the demo workspaces

Work on copies, never on `fixtures/`. The `touch` matters: without it, a copied source file can look newer than the manifest and the status bar starts with a staleness warning.

```bash
mkdir -p "$DEMO"
cp -R "$REPO/fixtures/jaffle-shop" "$DEMO/jaffle-shop"
touch "$DEMO/jaffle-shop/target/manifest.json"
```

For Part 5, make a second copy that behaves like a hostile clone. Its `.vscode/settings.json` points LineagePilot's interpreter and dbt at scripts inside the repository, and those scripts leave a file behind if anything ever runs them. It also has a rule pack.

```bash
CLONE="$DEMO/untrusted-clone"
cp -R "$REPO/fixtures/jaffle-shop" "$CLONE"
touch "$CLONE/target/manifest.json"
mkdir -p "$CLONE/.venv/bin" "$CLONE/.vscode" "$CLONE/.lineagepilot"
for tool in python dbt; do
  printf '#!/bin/sh\necho "planted %s ran: $*" >> "%s/PLANTED-RAN.txt"\n' "$tool" "$DEMO" > "$CLONE/.venv/bin/$tool"
  chmod +x "$CLONE/.venv/bin/$tool"
done
cat > "$CLONE/.vscode/settings.json" <<EOF
{
  "lineagePilot.python.interpreterPath": "$CLONE/.venv/bin/python",
  "lineagePilot.dbt.executablePath": "$CLONE/.venv/bin/dbt"
}
EOF
printf 'version: 1\ndisable: [DBT_009]\n' > "$CLONE/.lineagepilot/rules.yml"
```

For Part 6, make the review's project a repository of its own, with the project as deployed as its only commit:

```bash
REVIEW="$DEMO/impact-shop"
cp -R "$REPO/fixtures/impact-shop" "$REVIEW"
printf 'target/\nchanges/\nmake_fixture.py\nPROVENANCE.json\n' > "$REVIEW/.gitignore"
git -C "$REVIEW" init -q -b main
git -C "$REVIEW" add -A
git -C "$REVIEW" -c user.name=demo -c user.email=demo@example.com commit -qm "The project as deployed"
touch "$REVIEW/target/manifest.json"
```

### 3. Make a clean VS Code profile and install the VSIX

A separate profile keeps your own extensions and settings out of the demo, and the demo out of your VS Code. The profile's user settings pin the server's interpreter to the Python built in step 1.

```bash
mkdir -p "$DEMO/profile/User"
cat > "$DEMO/profile/User/settings.json" <<EOF
{
  "lineagePilot.python.interpreterPath": "$PY",
  "workbench.startupEditor": "none",
  "telemetry.telemetryLevel": "off"
}
EOF
"$CODE" --user-data-dir "$DEMO/profile" --extensions-dir "$DEMO/extensions" \
  --install-extension "$REPO/extension/dist/lineagepilot.vsix"
```

You should see `Extension 'lineagepilot.vsix' was successfully installed.`

To show the interpreter search instead (setting, then the Python extension, then `python3` on `PATH`, then uv), leave `lineagePilot.python.interpreterPath` out. The server then runs on whatever `python3` your login shell finds, which must be 3.10 or later.

### 4. Check it works (optional, 1 minute)

The language server's end-to-end tests, run against the server inside the VSIX, on an interpreter with nothing installed, as CI runs them:

```bash
cd "$REPO/packages/lsp"
LP_TEST_SERVER=../../extension/bundled/tool/lsp_server.py LP_TEST_PYTHONS="$PY" \
  uv run --locked pytest --no-cov -p no:cacheprovider \
  tests/test_indexing_e2e.py tests/test_methods_e2e.py tests/test_features_e2e.py
```

Expect `56 passed`. For a full rehearsal in a real VS Code, run the extension's integration tests. Each label opens a test window for about a minute.

```bash
cd "$REPO/extension" && npm run compile-tests
# macOS; elsewhere, point LP_VSCODE_PATH at your VS Code executable, or leave it out to download one
export LP_VSCODE_PATH="/Applications/Visual Studio Code.app/Contents/MacOS/Code"
LP_TEST_PYTHON="$PY" npx vscode-test --label trusted      # 27 passing
LP_TEST_PYTHON="$PY" npx vscode-test --label restricted   # 28 passing
```

---

## Run the demo

Before you start, open the standalone terminal with the variables above set, and make the CLI short to type:

```bash
alias lineagepilot="$REPO/packages/core/.venv/bin/lineagepilot"
cd "$DEMO/jaffle-shop"
```

### Part 1: the core, from the command line (Phase 1)

> **Say:** Everything in the editor runs on one Python library, which is also a command-line tool for CI later. It reads the artifacts dbt already writes. It needs no server, no account and no warehouse connection.

1. Index the project.

   ```bash
   lineagepilot index
   ```

   ```
   Indexed jaffle_shop (dbt 1.12.6, manifest schema v12)
     44 nodes: 13 model, 19 relation, 6 seed, 6 source
     138 columns; 129 edges: 17 depends_on, 87 derives, 25 materialises
     0 unresolved references
     Column lineage: 13 resolved
   ```

   > **Say:** 19 relations are the physical tables. Sources are linked to the seeds that build their tables, so lineage crosses from dbt's logical graph to the warehouse and back. 87 `derives` edges are column lineage, traced through the compiled SQL.

2. Run it again. The first line now ends `(from the snapshot cache)`, and the run is several times faster. Snapshots are keyed by the artifacts' sha256, so a changed manifest is never served from the cache.

3. What does a change to `stg_orders` reach?

   ```bash
   lineagepilot impact stg_orders
   ```

   ```
   Impact of model.jaffle_shop.stg_orders
     3 impacted: 3 model
       1  model        model.jaffle_shop.order_items  declared  model
       1  model        model.jaffle_shop.orders       declared  model
       2  model        model.jaffle_shop.customers    declared  model
   ```

4. Now only one column:

   ```bash
   lineagepilot impact 'stg_orders#order_total'
   ```

   ```
   Impact of model.jaffle_shop.stg_orders#order_total
     2 impacted: 2 model
       1  model        model.jaffle_shop.orders     inferred  column  [order_total]
       2  model        model.jaffle_shop.customers  inferred  column  [lifetime_spend]
   ```

   > **Say:** At column level, `order_items` drops out: it doesn't use `order_total`. We also name the columns that are hit: `orders.order_total`, then `customers.lifetime_spend`. And it says *inferred*, because this comes from parsing SQL, not from something dbt declared.

5. The same question without column lineage:

   ```bash
   lineagepilot impact 'stg_orders#order_total' --no-column-lineage --no-cache
   ```

   All three models come back at model level and marked `inferred`, with the reason `column lineage not computed`.

   > **Say:** When we can't be precise, we widen the answer and say why. We never narrow it by guessing.

6. Optional extras: `lineagepilot impact models/staging/stg_supplies.sql` takes a file and reports 4 models. `lineagepilot impact nope` exits with code 1 and `nope: no node, model or file by that name`. Add `--json` to any command for machine-readable output.

### Part 2: in the editor (Phase 2)

Open the project in the demo profile:

```bash
"$CODE" --user-data-dir "$DEMO/profile" --extensions-dir "$DEMO/extensions" "$DEMO/jaffle-shop"
```

When VS Code asks whether you trust the authors, choose **Yes, I trust the authors**. Leave the box that trusts the parent folder **unticked**, or Part 5's clone will be trusted too.

1. **Status bar** (bottom left). It shows `Lineage: indexing…` briefly, then `Lineage: 44 nodes · manifest 2 days old`. The age counts from the manifest's own `generated_at`, 1 November 2026, so it reads in hours on 3 November and grows after that. Click it: **Show Status** lists the project, `ready`, `44 nodes · Python 3.13.11 · rules built-in and pack`, and **Show Output**.

   > **Say:** The language server is Python, bundled in the extension. It runs on an interpreter you already have, isolated with `python -I`, so nothing in your project can shadow its libraries.

2. **CodeLens.** Open `models/staging/stg_orders.sql`. Line 1 reads `↓ 3 models`: the same answer as the CLI. Then show:
   - `models/staging/stg_customers.sql`: `↓ 1 model · PII`.
   - `models/marts/customers.sql`: `↓ nothing downstream · PII`.
   - `models/staging/__sources.yml`: a lens on each source table, for example `↓ 4 models` on `raw_orders` and `↓ 5 models` on `raw_products`. Model YAML files get them too, on each model's entry.

   Click the lens in `stg_orders.sql`. The **Lineage** view opens in the LineagePilot activity bar, pinned to `stg_orders`, with its downstream expanded.

3. **Hovers.**
   - In `models/marts/orders.sql`, hover `ref('stg_orders')` (line 5). The card names `stg_orders` with its description, **Upstream** (1 direct): `raw_orders (source) · declared, manifest`, **Downstream** (2 direct), `In all downstream: 3 models` and `Column lineage: resolved`.
   - In `models/staging/stg_orders.sql`, hover `source('ecom', 'raw_orders')` (line 5). Upstream is the `raw_orders` **seed**, `via table relation:jaffle.raw.raw_orders`: the source and the seed meet at the physical table.
   - In `models/marts/customers.sql`, hover `lifetime_spend` on line 26. It is **Derived from** `orders.order_total · aggregate · inferred, compiled sql`, and **Feeds** (0).

   > **Say:** Every link says how we know it: declared in the manifest, or inferred from compiled SQL. That is the evidence a reviewer needs before trusting an impact count.

4. **Problems** (View → Problems). LineagePilot reports 126 findings on this project, from 18 built-in conformance and PII rules. Type `customers` in the Problems filter:
   - `customers.sql`: **MED_001** (error), `customers (gold) sources from orders (gold), which violates the layer hierarchy`, and **MED_005**, a naming-convention warning.
   - `customers.yml`, on `customer_name`: **PII_001**, `PII indicator: PII column 'customer_name' (name) in gold model 'customers' is not masked`, and **PII_002**, `looks like name PII but is not tagged`.
   - **DBT_009**: `model 'customers' has no meta.owner and no group`. Every model in this project gets it.

   Each code links to its entry in [rules.md](rules.md). The link goes to the GitHub repository, so viewers need access to it.

   > **Say:** PII findings are *indicators*, from tags, column names and lineage, never claims of verified exposure. A check we can't make shows as "Not checked", never as a pass. Here every check could be made.

5. **Lineage view.** Click **Follow the Active Editor** (the pin icon in the view's title) to unpin it, then switch between files: the view follows the editor. Expand **Upstream** and **Downstream** one level at a time. Each node is labelled with its kind, its link's confidence and a PII flag, and clicking it opens its file. The **Airflow** view below is empty on purpose: Airflow linkage is Phase 5.

### Part 3: rule packs, staleness and exports (Phase 2)

1. **A team's own rules.** Create `.lineagepilot/rules.yml` in the project (New File, or `mkdir .lineagepilot` in the terminal first), paste this and save:

   ```yaml
   version: 1
   disable: [DBT_004]            # column descriptions: not a priority for this team
   severity: {DBT_009: error}    # every model needs an owner
   rules:
     - id: ACME_001
       name: Marts have a mart_ prefix
       when: {path_matches: '^models/marts/'}
       require: {name_matches: '^mart_'}
   ```

   The Problems panel updates without a restart. The findings drop from 126 to 70: the DBT_004 entries are gone, DBT_009 is now an error, and ACME_001 flags the 7 marts. Change `ACME_001` to `acme_1` and save. The pack file itself gets a warning, `'id' must be upper case letters, digits and _, starting with a letter`, and the rest of the pack still applies. Change it back afterwards.

   > **Say:** Packs are size-capped and validated, and their regular expressions are checked for catastrophic-backtracking shapes before use. They load only in a trusted workspace. Part 5 shows that.

2. **Staleness.** In `models/staging/stg_orders.sql`, add a comment line and save. The status bar turns to a warning: `Lineage: 44 nodes · 1 file newer than the manifest`.

   > **Say:** Lineage comes from dbt's artifacts, so we tell you when your edits have moved past them. Normally you run **LineagePilot: Refresh dbt Artifacts**, which runs `dbt parse`.

   This demo has no dbt profile. Remove the comment and save (the warning stays: the file is still newer than the manifest), then stand in for `dbt parse` from the terminal:

   ```bash
   touch "$DEMO/jaffle-shop/target/manifest.json"
   ```

   The server sees the manifest change and re-indexes (from the cache, since the content is unchanged), and the warning clears.

3. **Exports.** Run **LineagePilot: Export Lineage Graph**, choose **Mermaid**, and save `jaffle_shop-lineage.mmd`. The notification says `Exported 25 nodes to jaffle_shop-lineage.mmd`: the models, seeds and sources, without the physical tables. Click **Open**: the models are grouped by layer. JSON, Graphviz DOT, GraphML and Cypher work the same way. Then run **LineagePilot: Export PII Inventory**, choose **Markdown**, open `jaffle_shop-pii-inventory.md`, and preview it (⇧⌘V, or Ctrl+Shift+V).

   > **Say:** Every value is escaped for its format, and property tests re-parse each format to check that a model name can't inject anything.

### Part 4: the lineage graph (Phase 3)

1. With `models/staging/stg_orders.sql` open, run **LineagePilot: Show Lineage Graph**. You can also use the graph icon on a node in the Lineage view. The graph opens with `stg_orders` in focus, depth 2, both directions: the `raw_orders` source and seed upstream, and `order_items`, `orders` and `customers` downstream. Solid edges are declared in the manifest; dashed edges are inferred from SQL.

2. Click `orders`. The details panel shows its kind, layer, materialisation, direct upstream and downstream counts, its columns and its links, and VS Code previews `orders.sql` beside the graph. Double-click a node, or press **Open**, to open its file.

3. In the details panel, press **Impact**. The impact panel lists everything a change to `orders` could reach. Each entry is a link back into the graph.

4. **Columns.** In the details panel, click the column `order_total` (or press **Columns** in the toolbar and pick it). The graph switches to column lineage. Upstream are `stg_orders.order_total`, then `raw_orders.order_total`; downstream is `customers.lifetime_spend`. Every edge is dashed, because column lineage is inferred from the compiled SQL. Press **Models** to go back.

5. **The controls.** Set **Depth** to 1, then press **+** on a node at the edge to load one more level. Try **Direction** (Upstream or Downstream only), **Layout** (top to bottom), the **Show** checkboxes that filter node kinds, and **Highlight…**: type `stg` to highlight the staging models. The search does not move the graph.

6. **Theme and keyboard.** Switch the colour theme (⌘K ⌘T, or Ctrl+K Ctrl+T) between light and dark; the graph follows it. Then click the graph and press Tab to reach a node. The arrow keys follow its edges, Enter shows details, **O** opens the file, and **+** loads more.

7. **Optional: scale.** One of Phase 3's exit criteria is a 500-node graph staying interactive. A harness runs the production view in a browser against a synthetic 500-node graph. In a second terminal, with the variables set:

   ```bash
   cd "$REPO/extension" && npx esbuild --servedir=. --serve=127.0.0.1:8123
   ```

   Then open <http://127.0.0.1:8123/webview/perf/?nodes=500>. The page draws the graph, zooms, selects, searches and presses an arrow key, then shows its timings in the bottom-left corner. Compare them with the figures recorded on 4 November 2026 in [design §11](design.md#11-performance-budgets): selection, search and the arrow keys within 86 ms, zooming at 57–59 fps on a 60 Hz display, and no CSP violations. Other machines give other numbers. Press Ctrl+C in that terminal afterwards.

### Part 5: a cloned repository that is not trusted (Phase 2)

> **Say:** A cloned repository can contain anything, including a `.vscode/settings.json` that points tools at scripts of its own. Here is one.

1. Open the clone in a new window of the same profile:

   ```bash
   "$CODE" --user-data-dir "$DEMO/profile" --extensions-dir "$DEMO/extensions" "$DEMO/untrusted-clone"
   ```

   This time choose **No, I don't trust the authors**. VS Code opens it in Restricted Mode.

2. Open `.vscode/settings.json` and show that it sets `lineagePilot.python.interpreterPath` and `lineagePilot.dbt.executablePath` to scripts in `.venv/bin`.

3. The lineage still works: the status bar reads `Lineage: 44 nodes · …`, and CodeLens and hovers appear as before. **LineagePilot: Show Output** shows `Using Python 3.13.11 at … (setting)`: the user's interpreter, not the repository's.

4. Open `.lineagepilot/rules.yml`. It carries an information diagnostic: `Custom rules are not loaded in Restricted Mode; trust the workspace to use them.` The pack disables DBT_009, yet DBT_009 is still in the Problems panel.

5. Open the Command Palette and type `LineagePilot`. **Refresh dbt Artifacts** is not listed.

6. In the terminal, check that nothing planted ran:

   ```bash
   ls "$DEMO/PLANTED-RAN.txt"   # No such file or directory
   ```

   > **Say:** In Restricted Mode we only read artifacts. We never run dbt, never run an interpreter from inside the workspace (not even to check its version), and use only the built-in rules. The interpreter and dbt settings are machine-scoped, so a workspace can't set them even once it is trusted.

### Part 6: the Change Impact Review (Phase 4)

1. Open `$DEMO/impact-shop` in a new window of the same profile, as in Part 5, step 1. Run **LineagePilot: Review Changes**, or press the compare icon on the Lineage view. The review opens and says `Nothing differs from this base`.

2. Rename a column, as a developer would, and recompile. In the terminal:

   ```bash
   cp -R "$REVIEW/changes/rename-order-total/models/." "$REVIEW/models/"
   cp "$REVIEW/changes/rename-order-total/target/manifest.json" "$REVIEW/target/manifest.json"
   ```

   This puts in the files from a rename of `orders.order_total` to `total_usd`, and the manifest real `dbt compile` wrote from them. `customer_revenue` still reads the old name: that is the breakage to find. The review updates by itself within a few seconds.

3. Read it from the top. **Changed** shows `orders` with `−order_total` and `+total_usd`, compared with `catalog.json`.

   > **Say:** This is the diff of what the model selects, not of its text: compiled SQL against what is deployed.

4. **Downstream impact.** `customer_revenue` and `revenue_tiers` are in bold and reached through a named column, `lifetime_value` and then `tier`, with the column that led there. `order_counts` is listed too, at model level: it reads `orders`, but not the column that changed.

   > **Say:** Column lineage says what breaks. Everything else below a changed model is still listed, labelled, because comparing names can't rule out a change in how a column is computed. The line beside each row is the same one the graph uses: dashed means parsing inferred it.

5. `revenue_dashboard` is the exposure, with its owner. **Owners** groups the same list by who to tell, from `meta.owner` or dbt `group`; `revenue_tiers` is under **No owner**.

6. **Unknowns.** `broken_report` is there because its SQL cannot be parsed. It is also in the models table, at model level.

   > **Say:** A model we can't read is never counted as safe. And the Airflow section says we didn't look, rather than that nothing is affected, until the Airflow linkage is in.

7. **Make the model stale.** Add a comment to `models/marts/orders.sql` and save. Within a few seconds the columns turn unknown and every row becomes model level, with **Run dbt compile** offered. Undo the edit and save: the review recovers, because the manifest's source text matches the file again.

8. **Copy as Markdown.** Press it, and paste into a scratch file or a pull request. Then produce the same text from the command line:

   ```bash
   cd "$REPO/packages/core" && uv run lineagepilot review --project-dir "$REVIEW" --base main
   ```

   > **Say:** The panel and the command line produce the same bytes from the same inputs, so CI can post this later. Everything from the repository is escaped, so a model called `@org/everyone` mentions nobody.

---

## Reset between runs

Close the demo windows, then:

```bash
rm -rf "$DEMO"
```

Repeat preparation steps 2 and 3; the build from step 1 can be reused. Deleting `$DEMO` also deletes the profile, so VS Code asks about trust again. Your normal VS Code profile was never touched. To remove the Python that step 1 installed: `uv python uninstall 3.13`.

## If something goes wrong

| Symptom | Fix |
|---|---|
| `code` opens the folder in your usual VS Code, or ignores `--user-data-dir` | You ran it from VS Code's integrated terminal. Use a standalone terminal. |
| Status bar: `Lineage: no Python` | Check `lineagePilot.python.interpreterPath` in the profile's settings. **LineagePilot: Show Output** lists every interpreter tried and why each was skipped. |
| Status bar warns `N files newer than the manifest` right after opening | The copy's files are newer than its manifest: `touch target/manifest.json` in the project. |
| Status bar: `Lineage: lineage unknown: …` | The manifest is missing or unreadable. Make the copy again from `fixtures/jaffle-shop`. |
| No CodeLens | Check that the editor's `editor.codeLens` setting is on and the file is under `models/`. |
| Something else | **LineagePilot: Show Output** has the server's log. Set `lineagePilot.trace.server` to `messages` to log the protocol, and use **LineagePilot: Restart Language Server**. |

## If you are asked

- **What about my pull request?** That is Part 6: **Copy as Markdown**, or `lineagepilot review` for the same text. A GitHub Action that posts it is on the backlog.
- **What about a changed macro?** The review lists it under Unknowns: which models call it is not worked out yet (LP-a04).
- **And Airflow?** Phase 5 links Cosmos `DbtDag`/`DbtTaskGroup` and `BashOperator` dbt tasks to models, from the DAG files' syntax. DAG files are parsed, never imported or run.
- **How big can a project be?** On a synthetic 5,000-model project: a model-level cold index takes 2.5 s, a warm start 0.5 s, and an impact query 3–5 ms ([core README](../packages/core/README.md#performance-budgets)). The server is still 15% over its 500 MB budget after a cold index of 5,000 models (LP-dtt).
- **Can I install it?** Not yet from a marketplace: publishing waits for the licence decision (D-5). The VSIX from step 1 installs with `code --install-extension`.
- **Before showing this outside the team:** the jaffle-shop fixture has no licence, and LP-0tf replaces it before the repository goes public.
