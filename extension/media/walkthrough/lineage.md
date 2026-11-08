# See what a model reaches as you edit

Open a model or a properties file.

- **CodeLens** above each model counts everything downstream, for example `↓ 4 models · 1 exposure · 4 Airflow tasks`. Click it to list them in the Lineage view.
- **Hovers** on `ref()` and `source()`, and on the columns of the model you are editing, show the direct upstream and downstream, and how each link is known: declared in the manifest, or inferred from compiled SQL.
- The **Lineage view** in the activity bar follows the open file, one level at a time.
- The **lineage graph** shows a model or one of its columns, with everything around it to the depth you choose. Solid edges are declared, dashed ones inferred. Click a node for its details and **Impact** for everything a change to it could reach; double-click to open its file.
- The **Problems** panel shows conformance rules and PII indicators. A rule LineagePilot could not check says *Not checked*: it is never counted as a pass.
