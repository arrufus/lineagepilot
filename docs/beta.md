# The closed beta and Gate G1

How to run the four-week closed beta (task 6.3, LP-7er.3), and how its result decides Gate G1 (LP-7er.4). It is written for the product owner, who recruits the teams. Spike S4 was waived, so there is no pool of interviewees to start from.

**Gate G1**, four weeks into the beta, carries on only if all three of these hold:
1. at least half of the active beta users open the review weekly;
2. at least 3 teams have pasted reviews into real pull requests;
3. at least one team reports that a review changed a decision.

Otherwise pivot, for example to a CI-only product on the same core, or stop ([implementation plan](implementation_plan.md#phase-6-beta-and-release-2-weeks)).

---

## Decisions this rests on (8 November 2026)

| Question | Decision |
|---|---|
| How beta teams get it | A VSIX attached to a GitHub release of this private repository. Teams are invited as read-only collaborators. Nothing goes to the Marketplace or Open VSX before G1 ([release.md](release.md)). |
| Licence (D-5) | MIT, for the extension, `lineagepilot_core` and `lineagepilot_lsp`. |
| Telemetry (D-7) | None. Opt-in **usage counts**, kept on the user's machine and shared by hand in the weekly check-in. LineagePilot still makes no network connections. |
| Feedback | GitHub issues, through three forms: bug report, feedback, and the weekly check-in. |

---

## Before the first invitation

- [ ] GitHub Actions runs again, and CI is green on `main` (LP-kf5). That also closes Phase 5 (LP-56q).
- [ ] The labels and the tag ruleset in [release.md](release.md#before-the-first-beta-release) exist.
- [ ] `v0.1.0` is tagged and its release has the VSIX ([release.md](release.md#cutting-a-release)).
- [ ] You have installed that VSIX yourself on a clean profile, and walked through **Get Started with LineagePilot** and the sample project.
- [ ] **Decide what beta teams may read.** Read access to this repository is the whole repository: the source, the design and plan documents, the corpus, and `fixtures/jaffle-shop`, which has no licence (LP-0tf). If that is too much, release to a separate private repository that holds only releases and issues instead. The release workflow would then need a token that can write there.
- [ ] **Settle how criterion 1 is read** (below), before any check-in arrives.

## Recruiting

- **Who**: teams that run dbt and orchestrate it with Airflow, Cosmos or a `dbt` command in a task, and review dbt changes in pull requests. The cross-tool review is what is being tested, so a dbt-only team tells G1 little.
- **How many**: criterion 2 needs 3 teams, so invite 5 to 8. Some will not start, and some will stop.
- **What to tell them**:
  - it is a closed beta, for four weeks, from a named start date;
  - it runs locally: no account, no server, no warehouse connection, and no network connections at all;
  - you will ask for a three-minute check-in once a week, and for bugs and ideas as issues;
  - what it does not do yet. The Airflow scanner is honest but often cannot tell: on repositories it had never seen, it linked 17 of 106 tasks and listed most of the rest as unknown ([corpus/README.md](../corpus/README.md)). Say this up front. A team that expects every task linked will be disappointed in week one.

## Inviting a team

1. Add each person as a collaborator with the **Read** role: **Settings › Collaborators**, or

   ```bash
   gh api -X PUT repos/arrufus/lineagepilot/collaborators/<their-github-login> -f permission=pull
   ```

   Read access is enough to download a release, open issues and read the code.
2. Send them the link to the newest release, and the extension's README section [Install the beta](../extension/README.md#install-the-beta).
3. Ask each person to turn on **usage counts** if they are willing: `lineagePilot.usageCounts.enabled` in their user settings. They are optional, and the check-in works without them.

## Each week

- **Monday**: ask each team for its check-in, the **Weekly beta check-in** issue form. One per team.
- **Triage** bug reports and feedback. A fix ships as a new tagged release. Tell teams when there is one: the beta does not update itself.
- **Keep a tally** of the check-ins, as in the table below, rather than working it out at the end.

## Usage counts

With counting on, LineagePilot records three numbers a week on the user's machine: the days a dbt project was open with it running, how many times the review was opened, and how many times it was copied as Markdown. The record holds no names, paths or contents. **LineagePilot: Copy Usage Counts** puts them on the clipboard, for the user to read and paste into the check-in. Turning counting off deletes them, at once if LineagePilot is running, or else when it next starts. Only the user's own settings can turn it on: a workspace cannot ([design §10](design.md#10-security-model), D-7).

Counts are optional, so the gate cannot depend on them. They are there to check the check-in's own answers: if a team says four people opened the review, and their pasted counts show it opened once, ask.

## Scoring Gate G1

Score from the check-ins, week by week. One row per team per week:

| Week | Team | Used it (`active`) | Opened the review (`reviewed`) | Pull requests with a pasted review | Changed a decision |
|---|---|---|---|---|---|

**Criterion 1, the review opened weekly.** Proposed reading, to settle before the beta starts: in each of the four weeks, add up `reviewed` and `active` across all teams; the criterion holds if `reviewed` is at least half of `active` in at least three of the four weeks. A week counts only the teams that sent a check-in. A team that stops sending them has stopped being active, and that is a finding in itself. Record which reading you chose, here, before the first check-in.

**Criterion 2, pasted into real pull requests.** Count the teams that gave at least one pull request link over the four weeks. A real pull request is one in a repository the team works in, not a test repository. Three teams are needed.

**Criterion 3, a decision changed.** At least one check-in answers **Yes**, with a description of what the review showed and what the team did differently.

Write the result in LP-7er.4 with the tally and a link to every check-in, then decide.

---

## After G1

- **Continue**: the public release, through [release.md](release.md#before-the-first-public-release-after-gate-g1). Then the backlog in the implementation plan, ordered by what the beta asked for. Archive data-capsule's legacy server (DCS-4sz.3).
- **Pivot**: the plan names a CI-only product on the same core, the backlog's GitHub Action, as the likely one. The check-ins say what the pivot should be.
- **Stop**: tell the beta teams, and leave the last release where it is.

Either way, remove the beta collaborators when the beta ends, unless they are staying for what comes next.
