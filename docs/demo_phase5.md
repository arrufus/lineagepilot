# Demo script: Airflow linkage (Phase 5)

How to demonstrate what Phase 5 added: what to prepare, what to click, what the audience should see, and what to say. It stands alone: you do not need [demo.md](demo.md), which covers Phases 1 to 4.

**The one-line version.** Rename a column in a dbt model, and LineagePilot tells you which Airflow tasks run that model, which run after them and who owns them. It reads your DAG files as syntax, and needs no Airflow, no connection and no account.

> **Read this before you present.** This demo runs on a fixture written for it, so every feature works. On 17 real repositories the scanner had never seen, it got **17 of 106 links** right. It was almost never wrong, but it usually could not tell. Part 6 says so plainly. Keep it in for anyone deciding whether to adopt this: the rest of the demo, alone, gives the wrong impression.

| Part | What the audience sees | Time |
|---|---|---|
| 1 | What the DAG files run, from the command line, with no Airflow installed | 3 min |
| 2 | In the editor: CodeLens, a hover, the Airflow view, the lineage graph | 5 min |
| 3 | Rename a column: the review lists the tasks, their owners, and how sure it is | 5 min |
| 4 | What cannot be read, and how a team answers it | 5 min |
| 5 | A DAG file that would do harm if it ran, and does not | 3 min |
| 6 | How well it does on repositories it had never seen | 3 min |

About 25 minutes in all. **For 10 minutes:** Part 3, Part 4 steps 1 and 2, and Part 6 step 1.

**The project** is `fixtures/impact-shop`, a small dbt project of our own with real dbt 1.12 artifacts from before and after a column rename. Its `dags` folder holds:

| DAG file | What it shows |
|---|---|
| `dags/daily_marts.py` | A Cosmos `DbtTaskGroup` called `marts` over `orders+`, owned through `default_args`, then a shell task that runs after it |
| `dags/operations.py` | `dbt run` in a `BashOperator`, and a SQL task that reads the mart |
| `dags/backfill.py` | A task created in a loop, so what it runs cannot be known |

---

## Prepare (once, about 10 minutes)

### What you need

- macOS or Linux. The commands below are for a POSIX shell.
- [uv](https://docs.astral.sh/uv/), Node.js 22 or later, git.
- VS Code 1.93 or later. The `code` command does not have to be on `PATH`.
- A network connection for the preparation only. The demo itself runs offline. Neither dbt nor Airflow needs to be installed.

**Use a standalone terminal**, not VS Code's integrated one: it passes variables to anything started from it, and `code` then misbehaves.

Set these in that terminal, and again in any new terminal you use. Keep `DEMO` short: macOS limits the length of the socket path VS Code creates inside it.

```bash
export UV_PYTHON_PREFERENCE=only-managed   # uv's own Pythons, never Homebrew's or the system's
REPO=~/lineagepilot
DEMO=~/lp-demo
SHOP="$DEMO/airflow-shop"
CLONE="$DEMO/airflow-clone"
CODE=code   # on macOS without `code` on PATH:
# CODE="/Applications/Visual Studio Code.app/Contents/Resources/app/bin/code"
uv python install 3.13 && PY="$(uv python find 3.13)"   # a no-op once installed
```

### 1. Build the extension

Rebuild even if you prepared [demo.md](demo.md) before: a VSIX from before Phase 5 has an empty Airflow view.

```bash
cd "$REPO/extension"
npm ci
"$PY" scripts/bundle_libs.py
npm run package
"$PY" scripts/check_vsix.py dist/lineagepilot.vsix

cd "$REPO/packages/core" && uv sync --locked   # the CLI
cd "$REPO/packages/lsp"  && uv sync --locked   # only for the check in step 4
```

The last line of the VSIX check should be `VSIX check passed: universal, pure Python, expected contents, within budget`. Its warning that there is no licence is expected (decision D-5).

### 2. Make the demo workspaces

Work on copies, never on `fixtures/`. The project is a repository of its own, with the project as deployed as its only commit. The `touch` matters: without it, a copied file can look newer than the manifest, and the status bar starts with a staleness warning.

```bash
mkdir -p "$DEMO"
cp -R "$REPO/fixtures/impact-shop" "$SHOP"
printf 'target/\nchanges/\nmake_fixture.py\nPROVENANCE.json\n' > "$SHOP/.gitignore"
git -C "$SHOP" init -q -b main
git -C "$SHOP" add -A
git -C "$SHOP" -c user.name=demo -c user.email=demo@example.com commit -qm "The project as deployed"
touch "$SHOP/target/manifest.json"
```

For Part 5, make a second copy with a DAG file that leaves a file behind if anything ever runs it:

```bash
cp -R "$REPO/fixtures/impact-shop" "$CLONE"
touch "$CLONE/target/manifest.json"
cat > "$CLONE/dags/planted_dag.py" <<EOF
from pathlib import Path

Path("$DEMO/PLANTED-DAG-RAN.txt").write_text("This DAG file was run.\n")

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("planted", schedule=None, default_args={"owner": "mallory"}) as dag:
    BashOperator(task_id="run_orders", bash_command="dbt run --select orders")
EOF
```

### 3. Make a clean VS Code profile and install the VSIX

A separate profile keeps your own extensions and settings out of the demo, and the demo out of your VS Code. `--force` replaces a build you installed earlier.

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
  --install-extension "$REPO/extension/dist/lineagepilot.vsix" --force
```

### 4. Check it works (optional, 1 minute)

The Airflow end-to-end tests, against the server inside the VSIX, on an interpreter with nothing installed:

```bash
cd "$REPO/packages/lsp"
LP_TEST_SERVER=../../extension/bundled/tool/lsp_server.py LP_TEST_PYTHONS="$PY" \
  uv run --locked pytest --no-cov -p no:cacheprovider tests/test_airflow_e2e.py
```

Expect `18 passed`. For a full rehearsal in a real VS Code, run the acceptance scenario. It opens a test window for about a minute, so leave the keyboard alone:

```bash
cd "$REPO/extension" && npm run compile-tests
# macOS; elsewhere, point LP_VSCODE_PATH at your VS Code executable, or leave it out to download one
export LP_VSCODE_PATH="/Applications/Visual Studio Code.app/Contents/MacOS/Code"
LP_TEST_PYTHON="$PY" npx vscode-test --label review   # 15 passing
```

---

## Run the demo

Before you start, open the standalone terminal with the variables above set, and make the CLI short to type:

```bash
alias lineagepilot="$REPO/packages/core/.venv/bin/lineagepilot"
cd "$SHOP"
```

### Part 1: what the DAG files run, with no Airflow

> **Say:** A dbt lineage tool stops at dbt. But what breaks when you rename a column is often an Airflow task. So we read the DAG files as well. We read them as syntax: nothing is imported and nothing runs, so you don't need Airflow installed, a connection, or an account.

1. Show that Airflow really is not there. The traceback ends with:

   ```bash
   "$REPO/packages/core/.venv/bin/python" -c "import airflow"
   ```

   ```
   ModuleNotFoundError: No module named 'airflow'
   ```

2. What do the DAG files run?

   ```bash
   lineagepilot airflow --project-dir .
   ```

   ```
   3 DAG files in dags: 3 DAGs, 2 tasks linked to dbt, 0 unknown
     daily_marts  marts  cosmos_render  4 models  inferred  .
     operations  refresh_counts  run  1 models  heuristic  .
   Unknowns (1):
     dags/backfill.py:12  loop or factory: inside a for-loop (line 11): ...
   ```

   > **Say:** The Cosmos task group selects `orders+`, and we worked out that this means 4 models, using the same selection rules as dbt. We checked those rules against real `dbt ls`, on 44 expressions, on dbt 1.9 and 1.12. The third DAG builds a task in a loop, so we don't guess: it's listed as unknown, at its line.

3. The impact query you already know now crosses into Airflow:

   ```bash
   lineagepilot impact orders
   ```

   ```
   Impact of model.impact_shop.orders
     9 impacted: 4 airflow_task, 1 exposure, 4 model
       1  airflow_task airflow:daily_marts.marts               inferred  model
       1  airflow_task airflow:operations.export_orders        heuristic model
       1  model        model.impact_shop.broken_report         declared  model
       1  model        model.impact_shop.customer_revenue      declared  model
       1  model        model.impact_shop.order_counts          declared  model
       2  airflow_task airflow:daily_marts.notify_finance      inferred  model
       2  airflow_task airflow:operations.refresh_counts       heuristic model
       2  exposure     exposure.impact_shop.revenue_dashboard  declared  model
       2  model        model.impact_shop.revenue_tiers         declared  model
   Unknowns (2):
     airflow:backfill: loop or factory: dags/backfill.py:12: ...
     model.impact_shop.broken_report: column lineage: SQL could not be parsed: ...
   ```

   > **Say:** `export_orders` isn't a dbt task at all. It's a SQL operator, and we parsed its SQL and saw it reads this table. `notify_finance` doesn't touch `orders`, but it runs after the task that does, so if that fails, it doesn't run.

### Part 2: in the editor

Open the project in the demo profile:

```bash
"$CODE" --user-data-dir "$DEMO/profile" --extensions-dir "$DEMO/extensions" "$SHOP"
```

When VS Code asks whether you trust the authors, choose **Yes, I trust the authors**. Leave the box that trusts the parent folder **unticked**, or Part 5's clone will be trusted too.

1. **Status bar** (bottom left): `Lineage: 28 nodes · manifest N days old`. That is 21 from dbt and 7 from the DAG files, which are read a moment after the lineage is ready.

2. **CodeLens.** Open each of these, and read line 1:
   - `models/marts/orders.sql`: `↓ 4 models · 1 exposure · 4 Airflow tasks`
   - `models/marts/customer_revenue.sql`: `↓ 1 model · 1 exposure · 2 Airflow tasks · PII`
   - `models/marts/order_counts.sql`: `↓ 4 Airflow tasks`

   > **Say:** `order_counts` has nothing downstream in dbt, so a dbt-only tool says it's safe to change. Here, two tasks run it and two more run after them.

3. **Hover.** In `models/marts/customer_revenue.sql`, hover `ref('stg_customers')` on line 5. Near the bottom of the card: `In all downstream: 2 models · 1 exposure · 2 Airflow tasks`.

4. **The Airflow view.** Open the LineagePilot activity bar, with `models/marts/order_counts.sql` still open. The **Airflow** view shows what runs this model:

   ```
   daily_marts               data-platform
     marts                   dbt, through Cosmos · inferred
   operations
     refresh_counts          dbt run · heuristic
   Could not be read (1)
     backfill                loop or factory
   ```

   - Hover `marts`. The tooltip names `DbtTaskGroup`, the owner `data-platform`, and the task Cosmos should render for this model: `marts.order_counts_run`.
   - Hover `refresh_counts`. It says why the link is only heuristic: `no project_dir: dbt uses its working directory; assumed the only dbt project in the workspace`.
   - Click `marts`. `dags/daily_marts.py` opens at line 22, the `DbtTaskGroup` call.
   - Open `models/marts/orders.sql`. The view follows: `marts` is still there, now with `marts.orders.run` and `marts.orders.test` in its tooltip, because `orders` has tests. And `export_orders` appears, `reads · heuristic`.

   > **Say:** Cosmos creates its tasks when Airflow parses the file. The file itself says one thing: this group runs these models. That's the link we store. The per-model task names are worked out from the manifest, and we label them as derived.

5. **The lineage graph.** With `models/marts/orders.sql` open, run **LineagePilot: Show Lineage Graph**. Downstream, the tasks sit beside the models: `marts` and `export_orders`, then `notify_finance` and `refresh_counts`. Dashed edges are inferred, and dotted ones heuristic. Click `marts`: its details open, and its DAG file previews beside the graph. Untick **Airflow task** under **Show** to see the dbt-only view, then tick it again.

### Part 3: rename a column

This is the moment the product exists for.

1. Run **LineagePilot: Review Changes**. The review opens and says `Nothing differs from this base`.

2. Rename a column, as a developer would, and recompile. In the terminal:

   ```bash
   cp -R "$SHOP/changes/rename-order-total/models/." "$SHOP/models/"
   cp "$SHOP/changes/rename-order-total/target/manifest.json" "$SHOP/target/manifest.json"
   ```

   This puts in the files from renaming `orders.order_total` to `total_usd`, and the manifest real `dbt compile` wrote from them. The review updates by itself within a few seconds.

3. **Changed** shows `orders`, with `−order_total` and `+total_usd`. **Models** and **Exposures** list what is downstream, as in Phase 4. Scroll to **Airflow tasks (4 tasks in 2 DAGs)**:

   | DAG | Task | What it has to do with this | Confidence | Owner |
   |---|---|---|---|---|
   | `daily_marts` | `marts`, with the tasks Cosmos should render | dbt, through Cosmos: `customer_revenue`, `orders`, `revenue_tiers` | inferred | `data-platform` |
   | `daily_marts` | `notify_finance` | Runs after `marts` | inferred | `data-platform` |
   | `operations` | `export_orders` | reads: `orders` | heuristic | `operations-eng` |
   | `operations` | `refresh_counts` | dbt run: `order_counts` | heuristic | `operations-eng` |

   > **Say:** Before this, you'd rename the column, `dbt compile` would pass, and you might find out from a failed task the next morning. Now the pull request says which tasks are affected and who to tell.

4. **Heuristic links.** Point at the **Evidence** column of `refresh_counts`: it says why it is only heuristic.

   > **Say:** Most real links are heuristic, because a path in a DAG file is usually a path in a container, or depends on an environment variable's default. We show them anyway, with the reason. Hiding them would hide most of what's real. What you see is how sure we are, and why.

5. **Owners** now includes the tasks' owners: `data-platform` from the DAG's `default_args`, and `operations-eng` from `owner=` on each task.

6. **Open a task.** Click `marts` in the table. `dags/daily_marts.py` opens at the task group, beside the review.

7. **In a pull request.** Press **Copy as Markdown**, and paste it somewhere. Then produce the same text from the command line. This prints only the Airflow section:

   ```bash
   lineagepilot review --base main | sed -n '/#### Airflow tasks/,/#### Owners/p' | sed '$d'
   ```

   > **Say:** The panel and the command line produce the same bytes from the same inputs, so CI can post this later. An end-to-end test holds them to that.

### Part 4: what cannot be read, and how a team answers it

1. **The unknown.** Under **Unknowns**: `backfill`, `loop or factory`, at `dags/backfill.py:12`. Open that file and show the loop over `Variable.get("backfill_models")`.

   > **Say:** Which models this runs depends on a value in Airflow's database at run time. We won't guess, and we list it in every review, because it could run the model you just changed.

2. **Answer it.** In the terminal:

   ```bash
   mkdir -p "$SHOP/.lineagepilot"
   cat > "$SHOP/.lineagepilot/airflow.yml" <<'EOF'
   acknowledged:
     - file: dags/backfill.py
   pipelines:
     - pipeline_id: backfill
       owner: data-platform
       tasks:
         - task_id: backfill_all
           consumes: [model.impact_shop.orders]
   EOF
   ```

   Within about 3 seconds, the review updates by itself. The unknown has gone, and `backfill_all` is in the table: `consumes: orders`, with an annotation as its evidence.

   > **Say:** Whatever the scanner can't see, the team can declare, next to the code. It's data, like the manifest, so it can't run anything.

   If someone asks why it says `inferred` when a person declared it: every row not traced through a column is capped at `inferred` in the review, exposures included. The **Airflow** view shows the link itself, as `declared`.

3. **A path in a container.** A DAG file usually names the project by where it is in the image. Add one that does:

   ```bash
   cat > "$SHOP/dags/container_path.py" <<'EOF'
   from datetime import datetime

   from airflow import DAG
   from airflow.operators.bash import BashOperator

   with DAG("nightly", start_date=datetime(2026, 1, 1), schedule="@daily") as dag:
       BashOperator(
           task_id="run_marts",
           bash_command="dbt run --select orders+ --project-dir /opt/warehouse/transform",
           owner="data-platform",
       )
   EOF
   ```

   Under **Unknowns**: `nightly.run_marts`, `no dbt project`, `project path '/opt/warehouse/transform' matches no dbt project here`. Now say where that is:

   ```bash
   cat >> "$SHOP/.lineagepilot/airflow.yml" <<'EOF'
   path_map:
     /opt/warehouse/transform: .
   EOF
   ```

   The unknown goes, and `run_marts` joins the table as `inferred`, with `mapped by the path map entry` as its evidence.

   > **Say:** If the path's tail matches a project, we find it ourselves, and label it heuristic. If it doesn't, one line tells us. And it can't point outside the workspace.

4. **A selector left behind.** In `dags/operations.py`, change `--select order_counts` to `--select order_count` and save. Within about 3 seconds `refresh_counts` leaves the table, and **Unknowns** gains `selects nothing`: `order_count matches no node in this manifest: was a model renamed?`

   > **Say:** That's the mistake a rename leaves behind: a DAG still selecting a model that's gone. dbt would run nothing and call it a success.

   Change it back and save. The file stays under **Other changed files**: a changed DAG file is listed, not followed yet (LP-o59).

### Part 5: a DAG file that would do harm if it ran

> **Say:** A cloned repository can contain anything, and a DAG file is Python. Tools that import DAG files to read them run that code. Here's a file that writes to disk the moment it's run.

1. Show the file, and prove that it works:

   ```bash
   cat "$CLONE/dags/planted_dag.py"
   "$PY" -I "$CLONE/dags/planted_dag.py"     # fails on the airflow import, after the line that writes
   cat "$DEMO/PLANTED-DAG-RAN.txt"           # This DAG file was run.
   rm "$DEMO/PLANTED-DAG-RAN.txt"
   ```

2. Open the clone in a new window of the same profile:

   ```bash
   "$CODE" --user-data-dir "$DEMO/profile" --extensions-dir "$DEMO/extensions" "$CLONE"
   ```

   This time choose **No, I don't trust the authors**. VS Code opens it in Restricted Mode.

3. Open `models/marts/orders.sql`. Among the DAGs in the **Airflow** view is `planted`, owned by `mallory`, with `run_orders`, `dbt run · heuristic`. So the file was read.

4. Check that it did not run:

   ```bash
   lineagepilot airflow --project-dir "$CLONE" | grep planted
   ls "$DEMO/PLANTED-DAG-RAN.txt"      # No such file or directory
   ```

   > **Say:** We read that file, in a workspace we don't trust, and worked out what it runs. It never ran. Every integration test configuration has a file like this planted in it. A crafted file can't do much to the scanner either: strings it builds are capped, each file has a budget of steps, and a symlink to a device can't hang it.

Do not open **Review Changes** in this window. VS Code turns its Git extension off in Restricted Mode, so the review can only say it cannot tell what changed.

### Part 6: how well it does on repositories it had never seen

> **Say:** Everything so far ran on a project we wrote. Here's what happens on repositories we hadn't seen.

1. **The result.** Open `$REPO/corpus/README.md` and preview it (⇧⌘V, or Ctrl+Shift+V). An agent that never saw the scanner labelled 106 links in 17 public repositories. We froze the scanner, recorded its hash and scored it once:

   | What the scanner did | Links |
   |---|---|
   | Correct | 17 |
   | Said "unknown", at the line that caused it | 78 |
   | Said nothing | 9 |
   | Wrong | 2 |
   | Invented a link that doesn't exist | 0 |

   > **Say:** That's an 84% miss rate. It's not what you'd want, and it's the honest number. Our first spike measured 0%, but we'd built the scanner while reading those same repositories. What this tells us: it almost never misleads you, but on repositories like these, the Airflow section would mostly be a list of tasks it couldn't read. The 9 silent ones are fixed: they now say unknown. The rest we deliberately haven't tuned, because this corpus has now been read, so a better score on it would prove nothing.

2. **Why.** Three general gaps make up 62 of the 89 misses:
   - 35: `os.environ["AIRFLOW_HOME"]` with no default (LP-0o1)
   - 16: a helper imported from another file (LP-z2x)
   - 11: a container path that matches no project (LP-c05)

   Show the biggest one live, back in the `airflow-shop` window:

   ```bash
   cat > "$SHOP/dags/astro_style.py" <<'EOF'
   import os

   from airflow import DAG
   from cosmos import DbtTaskGroup, ProjectConfig

   with DAG("astro_style", schedule="@daily") as dag:
       DbtTaskGroup(
           group_id="transform",
           project_config=ProjectConfig(f"{os.environ['AIRFLOW_HOME']}/dags/dbt/impact_shop"),
       )
   EOF
   ```

   Under **Unknowns**: `astro_style.transform`, `value not known`, because the environment variable has no default. A person can see that the path's tail names the project. The scanner cannot yet. Then remove it:

   ```bash
   rm "$SHOP/dags/astro_style.py"
   ```

   > **Say:** That's one line of a very common DAG, and it's fixable. The plan is to close these gaps, then measure again on a new set of repositories, labelled the same way.

**What not to claim:**
- That it finds the Airflow tasks in any repository. On 17 unseen ones it linked 17 of 106. Your audience's may be better or worse.
- That the miss rate is 0%, or that it has improved. 84.0% is the only out-of-sample figure.
- That Phase 5 is finished. As of 6 November 2026 the epic (LP-56q) is open, because CI stopped starting jobs (LP-kf5). See [If you are asked](#if-you-are-asked).

---

## Reset between runs

Close the demo windows, then make the workspaces again (Prepare step 2):

```bash
rm -rf "$SHOP" "$CLONE" "$DEMO/PLANTED-DAG-RAN.txt"
```

To remove everything, including the build and the profile, see `docs/demo_cleanup.sh --help`.

## If something goes wrong

| Symptom | Fix |
|---|---|
| The **Airflow** view says `Not analysed: no DAG files were found in **/dags` | You opened another folder, or `dags` is missing. Open `$SHOP`. |
| The review says `Airflow DAGs were not analysed: the DAG files are still being read` | Wait a few seconds: it updates by itself. |
| The review says `Airflow DAGs were not analysed in this version` | The VSIX is from before Phase 5. Repeat Prepare steps 1 and 3. |
| An edit to a DAG file changes nothing | A module it imports from outside the DAG folders is not watched. Run **LineagePilot: Re-index Lineage**. |
| The numbers differ from this script | A leftover `.lineagepilot/airflow.yml` or DAG file from an earlier run. See [Reset between runs](#reset-between-runs). |
| Status bar warns `N files newer than the manifest` right after opening | `touch target/manifest.json` in the project. |
| `code` opens your usual VS Code | You ran it from VS Code's integrated terminal. Use a standalone terminal. |
| Something else | **LineagePilot: Show Output** has the server's log. **LineagePilot: Restart Language Server** starts it again. |

## If you are asked

- **Does it run our DAG code?** Never. Files are parsed with Python's `ast` module, in any workspace, trusted or not. Part 5 shows this, and the integration tests hold it in every configuration.
- **Which operators does it read?** Cosmos `DbtDag`, `DbtTaskGroup` and its single operators; dbt commands in `BashOperator`, `KubernetesPodOperator`, `DockerOperator` and `SSHOperator`; and SQL operators. Task order comes from `>>`, `<<`, `set_upstream`, `set_downstream` and `chain()`.
- **What can't it read?** Tasks from loops and factories, from a `manifest.json` read when Airflow parses the file, or from wrappers and imported helpers; repo-local operator subclasses; selectors that need dbt's state; Jinja and shell variables; remote manifests; a command inside a container image. All of these are listed as unknowns, not dropped.
- **Would it work on our repository?** Try it: `lineagepilot airflow --project-dir path/to/dbt/project`. It works without a manifest, on an approximate one built from the project's files. Expect unknowns: Part 6 is the fair guide. Don't promise a number.
- **Where does it look for DAG files?** Any folder called `dags`, up to six levels down. Elsewhere, set `lineagePilot.airflow.dagFolders`.
- **What does a `dbt test` task link to?** The selected models that have at least one test.
- **How sure are the Cosmos task names?** They are derived from the manifest, by a mapping read from Cosmos's source and checked against its tests, for Cosmos 1.11 and later. They were not checked against a running Airflow.
- **Doesn't `marts` also run `order_counts`?** Yes, and the row should say so. A known bug: when a task is reached both through a changed column and at model level, its row lists only what the column traversal found (LP-60p). The task itself is listed correctly.
- **What if I change a DAG file?** It is read again within seconds, and the review shows the new links. The file itself is only listed under **Other changed files**: mapping it to its tasks is LP-o59.
- **How big can it go?** At 5,000 models with 2,000 tasks, a warm start has its scan linked in about 2 to 3 seconds, and a review of 100 changed models takes about 100 ms. Peak server memory on Linux is 379 MB, within the 500 MB budget. On macOS it reads 511 to 608 MB, over budget (LP-dtt).
- **Has this been tested on Windows?** As of 6 November 2026, partly. The core passed on Linux, macOS and Windows at the frozen commit, and the acceptance scenario's Airflow checks passed on all three on an earlier commit. On the final code it passes in a Linux container, on Python 3.10 and 3.13. The rest waits for CI to start again (LP-kf5).
- **Can I install it?** Not yet from a marketplace: publishing waits for the licence decision (D-5). The VSIX from step 1 installs with `code --install-extension`.
