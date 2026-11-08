# Review a change before you push

**Review Changes** compares your branch and working tree with the merge-base of the default branch, or a base you choose. It shows:

1. each changed model, with its columns as a diff;
2. what that reaches: models, exposures, Airflow tasks and their owners, each with how sure the link is and what says so;
3. **Unknowns**: everything that could not be checked, rather than leaving it out.

**Copy as Markdown** puts the review on the clipboard for your pull request. `lineagepilot review` on the command line prints the same text for the same inputs.

The review compares what the compiled SQL selects, so run `dbt compile` after an edit. Set `lineagePilot.baseline.manifestPath` to your production manifest, the one you use for dbt's `--state`, for the most accurate baseline.

## In the sample

1. Open **Source Control**, choose **Initialize Repository**, and commit everything. The sample's `.gitignore` keeps `target/` out.
2. **Apply the Sample Change** renames `order_total` to `total_usd` in `models/marts/orders.sql` and puts the manifest `dbt compile` would write in `target/`.
3. **Review Changes**. The rename reaches four models, the dashboard and four Airflow tasks, with their owners. The unparseable model and the DAG that builds tasks in a loop are under Unknowns.
