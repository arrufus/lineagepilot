# Releasing LineagePilot

How a version of the extension reaches users (task 6.2, LP-7er.2). Releases are built and published by CI only. A VSIX built anywhere else is for testing and never reaches a user.

**After Gate G1**, which passed on 13 November 2026, a release goes to the VS Code Marketplace and Open VSX as well as to GitHub releases. The first was 0.2.0, on both registries since 15 November 2026 ([Before the first public release](#before-the-first-public-release-after-gate-g1)). During the closed beta a release was only a `.vsix` attached to a GitHub release ([beta.md](beta.md)).

---

## Cutting a release

1. **Bump the version** in `extension/`, on a branch:

   ```bash
   cd extension && npm version 0.1.1 --no-git-tag-version   # updates package.json and package-lock.json
   ```

   Versions are `major.minor.patch`, as both registries require: no `-beta.1` suffix. No release is a GitHub pre-release, even at `0.x`: GitHub never makes a pre-release the latest release, and the latest release's link is the one the READMEs give.

2. **Add its section to `extension/CHANGELOG.md`**, headed `## 0.1.1`. Its text becomes the release notes, and the release fails without it.

3. **Merge to `main`, with CI green**, through a pull request:

   ```bash
   git add extension/package.json extension/package-lock.json extension/CHANGELOG.md
   git commit -m "Release 0.1.1"
   git push -u origin HEAD
   gh pr create --base main --fill
   gh pr checks --watch --fail-fast && gh pr merge --merge --delete-branch
   ```

   The `&&` is what makes it "CI green": `gh pr checks` fails if any check failed, and then nothing is merged. GitHub enforces only part of it: the `main` ruleset requires a pull request and the secret scan, but not the core, server and extension checks, which run only when their paths change (checklist item 7 below). Auto-merge is off. Merge with `--merge`, not `--squash` or `--rebase`, so that step 4 has a merge commit to tag: the ruleset allows no other method. If `gh pr checks` says no checks were reported, the workflows have not registered yet: wait a few seconds and run that line again.

4. **Tag the merge commit on `main`, and push the tag**:

   ```bash
   git switch main && git pull
   git tag -a v0.1.1 -m "LineagePilot 0.1.1"
   git push origin v0.1.1
   ```

5. **Check the release.** The run takes about 11 minutes. Watch it, then check what it made:

   ```bash
   gh run watch --exit-status "$(gh run list --workflow lineagepilot-release.yml --branch v0.1.1 --limit 1 --json databaseId --jq '.[0].databaseId')"
   python extension/scripts/verify_release.py v0.1.1
   ```

   If `gh run watch` says `failed to get run: HTTP 404: Not Found`, the run has not registered yet: wait a few seconds and run that line again. With the registries on, the run stops before `marketplace` and `open-vsx` until you approve the `registries` deployment on the run's page, and `gh run watch` waits with it. `verify_release.py` prints one line per check, then **The release is complete.** or what is wrong ([Checking a release](#checking-a-release)).

6. **Check that it reached users.** The Marketplace checks a new version before it lists it, which can take some minutes. Then it is at <https://marketplace.visualstudio.com/items?itemName=lineagepilot.lineagepilot> and <https://open-vsx.org/extension/lineagepilot/lineagepilot>, and editors offer it as an update. The VSIX is always at <https://github.com/arrufus/lineagepilot/releases/latest>.

The tag starts [`lineagepilot-release`](../.github/workflows/lineagepilot-release.yml):

| Job | What it does |
|---|---|
| `verify` | The tagged commit is on `main`. The tag, `package.json` and `CHANGELOG.md` agree (`extension/scripts/release_check.py`). |
| `core`, `lsp`, `extension` | The three CI workflows, on the tagged commit. `extension` builds the one universal VSIX, without npm install scripts, and normalises it so its bytes depend only on its files and the commit's time. Then it tests it on Linux, macOS and Windows with Python 3.10 and 3.13, trusted and in Restricted Mode. |
| `secret-scan` | gitleaks over the whole git history ([`lineagepilot-secrets`](../.github/workflows/lineagepilot-secrets.yml), allowlist in [`.gitleaks.toml`](../.gitleaks.toml)). |
| `github-release` | Checks that VSIX against both registries' rules, licence included (`check_vsix.py --release`), and that it is normalised for the tagged commit. Attests its build provenance and that of `SHA256SUMS` (public repository only, see below). Attaches it as `lineagepilot-<version>.vsix`, with `SHA256SUMS`, to a GitHub release, which becomes the latest release. Its notes are the changelog's, ending with the VSIX's SHA-256 and a link to the run. |
| `marketplace`, `open-vsx` | Publish the same file, after checking its SHA-256. These run only when the repository variable `LP_PUBLISH_REGISTRIES` is `true`, and only after a reviewer approves the `registries` environment. `marketplace` signs in with Trusted Publishing, so no Marketplace token is stored (checklist item 5). `open-vsx` uses the environment's `OVSX_PAT` secret. |

Nothing is published unless every test passes. The VSIX that is released is the one that was tested: it is built once, in the `extension` job, and never rebuilt.

### If a release fails

- **`verify` fails**: fix the version or the changelog on `main`, delete the tag (`git push origin :v0.1.1 && git tag -d v0.1.1`), and tag again.
- **A test fails**: no release was made. Fix it on `main`, then move the tag the same way.
- **A publish job fails** after the GitHub release: re-run that job from the Actions page. It publishes the VSIX already released. Never publish a different file under the same version. If `marketplace` fails at the OIDC token exchange, the Trusted Publishing policy does not match this run: compare it with checklist item 5, correct it on the Marketplace and re-run the job. The tag does not change.
- **`verify_release.py` fails** on a release the workflow made: it says which check failed. A pre-release is fixed by the command it prints. For anything else, do not hand the release out: fix the cause on `main` and release a new version. Never replace a released file.

### Rolling back to an earlier release

0.2.0 is the release to fall back on while the next phase is built ([implementation_plan_next_phase.md](implementation_plan_next_phase.md)). Four things keep that possible, and none needs doing now:

- **The source.** `v0.2.0` is the merge commit `7e211cb`. The `Release tags` ruleset forbids creating, moving or deleting any `v*` tag and has no bypass, so it cannot move. Work after it reaches `main` only through pull requests, which the `main` ruleset requires; merged with `--merge`, as step 3 of [Cutting a release](#cutting-a-release) says, each one can be reverted as a whole.
- **The build.** `lineagepilot-0.2.0.vsix` and `SHA256SUMS` are on the GitHub release, with build provenance attestations. `python extension/scripts/verify_release.py v0.2.0` checks them end to end; it passed on 16 November 2026.
- **The cache.** The server's snapshots are keyed by `SNAPSHOT_FORMAT` and the core's version, and one that does not match is ignored and deleted, so an older server never loads a newer one's cache. The core's version does not change between releases, so the format is the guard: `tests/test_snapshot_format.py` fails when the graph model changes until the format is bumped.
- **Settings and files.** An editor ignores a setting the installed version does not declare. The plan has later releases read `.lineagepilot/lineage.yml` and still read `airflow.yml` (task 7.4), so a workspace that keeps `airflow.yml` keeps its Airflow annotations on 0.2.0. One that has moved them to `lineage.yml` loses them until it rolls forward.

**One person rolls back** by installing the older version. In VS Code: the extension's gear › **Install Specific Version…** › `0.2.0`, then check that **Auto Update** is off for it in the same menu, or the newer version comes back. Or from the release, after checking it as in [Checking a release](#checking-a-release):

```bash
gh release download v0.2.0 --repo arrufus/lineagepilot --pattern 'lineagepilot-0.2.0.vsix' --pattern SHA256SUMS
sha256sum --check SHA256SUMS && code --install-extension lineagepilot-0.2.0.vsix
```

**Everyone rolls back** with a new version. Neither registry can make an older version the latest again, and a released version is never replaced. So release the old code under the next number:

1. On a branch from `main`, revert each pull request merged since the version to return to, newest first: `git revert -m 1 <merge commit>`. `git log --merges --oneline v0.2.0..main` lists them. Keep the release tooling: `.github/workflows/`, `extension/scripts/` and the `@vscode/vsce` version in `extension/package.json` changed after 0.2.0 for Trusted Publishing, and reverting them breaks publishing. `git diff v0.2.0 main -- .github extension/scripts extension/package.json` shows what they are.
2. Check that the product is 0.2.0's again: `git diff v0.2.0 -- packages protocol extension/src extension/media fixtures` shows nothing.
3. Bump the version past the newest release, write its changelog section (what was rolled back, and why), and cut it as in [Cutting a release](#cutting-a-release).

### Checking a release

One command checks a release, from a clone of this repository, with the GitHub CLI signed in to an account that can read it:

```bash
python extension/scripts/verify_release.py v0.1.1
```

With no tag, it checks the release of the version in `extension/package.json`, the one just cut. It needs Python 3.10 or later; `uv run --no-project extension/scripts/verify_release.py` runs it with uv's own Python instead of the system's.

It downloads the release into a temporary directory, so files from an earlier download do not get in its way, and checks that:
- the release is published, not as a draft or a pre-release, and is the latest release;
- the release workflow made it: its author is `github-actions[bot]`, rather than someone who uploaded a file;
- it holds `lineagepilot-0.1.1.vsix` and `SHA256SUMS`, fully uploaded;
- the VSIX downloaded whole: its SHA-256 is the one in `SHA256SUMS` and the one GitHub recorded on upload, and its `package.json` says 0.1.1;
- the release run for the tag succeeded on the tagged commit, every job in it passed, and the release job logged the same SHA-256 as it named the file;
- each file has a build provenance attestation, once the repository is public.

It ends by saying who can download the release. It exits 0 if the release is complete, 1 if a check failed, and 2 if it could not ask GitHub, for example because `gh` is not signed in.

**What this shows.** `SHA256SUMS` and GitHub's recorded digest show that a download is complete, but they come from the same release, so they cannot show who made the file. **While the repository is private, as in the closed beta, a release has no attestation.** GitHub makes attestations only in public repositories on this account's plan (private ones need GitHub Enterprise Cloud), so the release workflow skips that step, and `gh attestation verify` fails with `HTTP 404: Not Found`. What ties the file to the workflow instead is the release's author and the hash in the release run's log. Nothing there is signed: it rests on GitHub's records and on who has write access to the repository, so it is weaker than an attestation.

**Once the repository is public, each release has a build provenance attestation**, which `verify_release.py` checks. It is signed with a short-lived Sigstore certificate issued to the release workflow's run, and it names the repository, the workflow, the tag and the commit. Without a clone, check it with `gh` alone:

```bash
gh release download v0.1.1 --repo arrufus/lineagepilot --dir lineagepilot-0.1.1 && cd lineagepilot-0.1.1
sha256sum --check SHA256SUMS                               # shasum -a 256 --check, where there is no sha256sum
gh attestation verify lineagepilot-0.1.1.vsix --repo arrufus/lineagepilot \
  --signer-workflow arrufus/lineagepilot/.github/workflows/lineagepilot-release.yml \
  --source-ref refs/tags/v0.1.1 --deny-self-hosted-runners
```

`gh attestation verify` fails unless an attestation for exactly these bytes was made by that workflow, run for that tag. `SHA256SUMS` is attested the same way. The first release after the repository goes public is the first one attested.

**Rebuilding a release.** The VSIX's bytes depend only on its files and the tagged commit's time. `scripts/bundle_libs.py` leaves out the paths pip records on the build machine, and `scripts/normalise_vsix.py` sorts the entries, sets every timestamp to `SOURCE_DATE_EPOCH` and fixes the file modes, copying each file's compressed bytes unchanged. Two builds on one platform with the same Node and Python versions give the same SHA-256 (checked on macOS arm64, with different directories, umasks and time zones). To rebuild a release, use Linux x64 as the release job does, with the Node 22 and Python 3.10 patch versions in the `extension` job's log (the Python one is also in `bundled/libs/BUNDLE.json`):

```bash
git switch --detach v0.1.1
cd extension
npm ci --ignore-scripts && npm rebuild esbuild
python scripts/bundle_libs.py
export SOURCE_DATE_EPOCH=$(git log -1 --pretty=%ct)
npm run package
python scripts/normalise_vsix.py dist/lineagepilot.vsix
sha256sum dist/lineagepilot.vsix
```

On another platform the files are the same, but a different zlib can compress them differently. Compare each file's size and CRC-32 instead:

```bash
diff <(unzip -v dist/lineagepilot.vsix | awk 'NR > 3 {print $1, $7, $8}') \
     <(unzip -v lineagepilot-0.1.1.vsix | awk 'NR > 3 {print $1, $7, $8}')
```

---

## One-time setup

### Before the first beta release

This was for the closed beta, in the repository that is now the private archive. The public repository follows the next list instead.

- [x] **GitHub Actions runs again** (LP-kf5): billing was fixed on 8 November 2026, under the account's **Billing and plans**. Without it no workflow starts, this one included.
- [ ] **Know who has write access.** A tag on `main` releases, and anyone who can push to this repository can push one. GitHub cannot restrict who creates tags here: rulesets and branch protection need GitHub Pro or a public repository. Beta testers are collaborators, and on a repository owned by a personal account a collaborator always has write access: there is no Read role ([beta.md](beta.md#inviting-a-team)). So invite only people you trust to release. The release workflow refuses a tag that is not on `main`, which stops a stray tag, not a collaborator who pushes to `main` first. For read-only testers, the repository, or a releases-only one, must belong to an organization. If you move to GitHub Pro, add the rulesets in the public-release list below now.
- [ ] **Create the labels the beta's issue forms use.** Never done, and no longer needed: the beta's labels and its weekly check-in form were removed at the public release, which creates the one label it needs.

That is all a GitHub release needs: it uses the workflow's own token.

### Before the first public release, after Gate G1

Gate G1 passed on 13 November 2026. The source goes public as a **new** repository, `arrufus/lineagepilot`, pushed from a copy of this history with the jaffle-shop fixtures filtered out, and this repository becomes a private archive. Replacing the fixtures in a new commit is not enough: the history would still hold them, and so would the refs GitHub keeps for pull request #1, which cannot be deleted. The Marketplace and Open VSX are public too: anyone can find and install what is published there.

Work through these in order:

1. [x] **Replace the jaffle-shop fixtures** (LP-0tf) with a project of our own, with CI green. Done on 13 November 2026 (pull request #2 in the archive): `fixtures/pilot-shop`, written for this repository, with its artifacts built by `make_fixture.py` on dbt 1.12.6 and Fusion 2.0.9. The older copies stay in the archive's history, which is why the source moved ([Publishing the source](#publishing-the-source)).
2. [ ] **Trademark search for "LineagePilot"** (decision D-4), by someone qualified to do one. Keep "dbt" out of the name and the publisher, per dbt Labs' trademark guidelines.
3. [ ] **Keep your personal email address out of what you publish.** Set git's identity to your GitHub noreply address, in this clone and globally, before any commit or tag for the new repository (an annotated tag records its tagger's address too):

   ```bash
   git config user.email 30685928+arrufus@users.noreply.github.com
   git config --global user.email 30685928+arrufus@users.noreply.github.com
   ```

   Then, under GitHub's **Settings › Emails**, turn on **Keep my email addresses private** and **Block command line pushes that expose my email**. The git identity was set on 13 November 2026; the two GitHub settings are still to do.
4. [x] **Move the source to the new repository**. Done on 13 November 2026: 80 commits, with a tree identical to the archive's `main`, public the same afternoon after a last check of every ref for personal addresses, jaffle-shop paths and secrets. Sub-step 4 started the core, server, extension and S1 workflows by hand (`gh workflow run <file> --repo arrufus/lineagepilot --ref main`): their path filters did not match the first push to an empty repository, and only the secret scan started by itself.
   1. Rename this one: `gh repo rename lineagepilot-private --repo arrufus/lineagepilot`. GitHub redirects the old name to it until another repository takes the name, which the next step does. So point this clone at the new name straight away: `git remote set-url origin https://github.com/arrufus/lineagepilot-private.git` ([Publishing the source](#publishing-the-source), step 1).
   2. Create the new one, private for now: `gh repo create arrufus/lineagepilot --private`.
   3. Push the filtered history to it, as in [Publishing the source](#publishing-the-source).
   4. Let CI run there, and check that every workflow passes on `main`. Secrets, variables, environments and settings do not move with the history: the new repository starts with none.
   5. Create the label the feedback form uses. `bug` exists already in a new repository:

      ```bash
      gh label create feedback --repo arrufus/lineagepilot --color c5def5 --description "An idea, a question or something that could be better"
      ```

   6. Make it public: `gh repo edit arrufus/lineagepilot --visibility public --accept-visibility-change-consequences`. The READMEs' links then work for everyone, releases get build provenance attestations, and the rulesets and environment below can be enforced.
   7. Turn on private vulnerability reporting, which `SECURITY.md` points to: **Settings › Code security › Private vulnerability reporting**, or `gh api -X PUT repos/arrufus/lineagepilot/private-vulnerability-reporting`. GitHub offers it to public repositories only: on the private repository the API answers 404 (checked 13 November 2026).
5. [ ] **Marketplace publisher** `lineagepilot`, the `publisher` in `extension/package.json`, created at <https://marketplace.visualstudio.com/manage> with a Microsoft account that has two-factor authentication on. Done, except the sign-in check below. 0.2.0 was published with an Azure DevOps personal access token for **All accessible organizations**, with the **Marketplace › Manage** scope, made for that release alone. Azure DevOps stops supporting such global tokens on 1 December 2026. Every later release uses **Trusted Publishing** instead. The publisher's **Trusted Publishing** page holds this policy, which shows as **Active** (to make it again, **Create** one with these fields):
   - **Policy name**: `Release workflow`
   - **Repository owner**: `arrufus`
   - **Repository**: `lineagepilot`
   - **Workflow**: `lineagepilot-release.yml`
   - **Environment**: `registries`

   The Marketplace stores the owner and the repository by their numeric IDs (30685928 and 1456595272), so a repository deleted and made again under the same name would not match. The `marketplace` job trades its GitHub OIDC token for a Marketplace credential that lasts 15 minutes (`vsce publish --oidc`). That takes vsce 4.1.0 or later: 4.0.0 had the option, but its exchange did not yet match the Marketplace's. No Marketplace token is stored anywhere: the `VSCE_PAT` secret is gone from the `registries` environment, and the token was revoked in Azure DevOps on 16 November 2026.

   **Checking the sign-in without releasing.** The release workflow's `marketplace-sign-in` job signs in with the policy and publishes nothing. It offers the latest release's VSIX, which the Marketplace already has, so vsce signs in, sees the version is there and stops before uploading. A step before it stops the job if the Marketplace lacks that version. Run it after creating or changing the policy, renaming the workflow, or changing the `registries` environment. It runs on GitHub, so any machine with a browser will do; the `gh` commands are alternatives, from an account that can administer the repository.
   1. **Let the environment admit `main`, for this check only.** **Settings › Environments › registries › Deployment branches and tags › Add deployment branch or tag rule**: ref type **Branch**, name pattern `main`. Or: `gh api -X POST repos/arrufus/lineagepilot/environments/registries/deployment-branch-policies -f name=main -f type=branch`.
   2. **Start it.** **Actions › lineagepilot-release › Run workflow**, branch `main`. Or: `gh workflow run lineagepilot-release.yml --repo arrufus/lineagepilot --ref main`. Every release job is skipped; only `marketplace-sign-in` runs.
   3. **Approve it.** The run waits at **Review deployments**: tick `registries`, then **Approve and deploy**.
   4. **Read the result.** The job passes only if its last step's log shows `Version 0.2.0 is already published. Skipping publish.` (the latest release's version), which vsce prints after signing in. On the Marketplace, the policy's **Last used** should no longer say **Never**.
   5. **Take `main` out again**, so that only `v*` tags reach the environment: delete the rule from step 1. Or: `gh api repos/arrufus/lineagepilot/environments/registries/deployment-branch-policies --jq '.branch_policies[] | select(.name == "main") | .id'` gives its ID, and `gh api -X DELETE repos/arrufus/lineagepilot/environments/registries/deployment-branch-policies/<ID>` deletes it.

   If it fails: a refusal to deploy `main` to `registries` means step 1 is missing. `The Marketplace does not have …` means the job stopped itself before signing in. `Marketplace OIDC token exchange failed with 401` (or 403) means the policy does not match this run: compare it with the fields above. Nothing is published in any of these cases.
6. [x] **Open VSX namespace** `lineagepilot`. Done on 14 and 15 November 2026:
   - Register an Eclipse Foundation account at <https://accounts.eclipse.org/user/register>, with its **GitHub Username** set to the GitHub account you sign in to <https://open-vsx.org> with. Sign in there, then under **Settings › Profile** choose **Log in with Eclipse**, **Show Publisher Agreement** and **Agree**.
   - Generate a token under **Settings › Access Tokens**. It is shown once and does not expire. Create the namespace with it: `npx ovsx create-namespace lineagepilot`, with the token in `OVSX_PAT`. That makes you a contributor, and the namespace stays unverified until it is claimed.
   - Claim it with the namespace template at <https://github.com/EclipseFdn/open-vsx.org/issues/new/choose>. Because `lineagepilot` is not your GitHub username, say why it is yours, and show that you own the Marketplace publisher of the same name. Granted in [EclipseFdn/open-vsx.org#15712](https://github.com/EclipseFdn/open-vsx.org/issues/15712), before the first publish, so 0.2.0 shows as verified.
7. [x] **Protect the new, public repository** (audit H8 and H9, LP-qy0.9). Set up on 13 November 2026, with the secrets added on 14 and 15 November:
   - **A GitHub environment called `registries`**, under **Settings › Environments**: add yourself as a required reviewer, allow only tags `v*` to deploy, and add the secret `OVSX_PAT`. The token lives only there, so only an approved release job can read it. The Marketplace needs no secret (item 5), and the Trusted Publishing policy names this environment, so only an approved release job can publish there either. Check that the required reviewer and the tag rule show as saved before you add a secret.
   - **Set the repository variable** `LP_PUBLISH_REGISTRIES` to `true`, under **Settings › Secrets and variables › Actions › Variables**.
   - **Rulesets**, under **Settings › Rules › Rulesets**:
     - a tag ruleset for `v*` (**Release tags**) that only you can create, update or delete: it blocks all three, and the repository admin role bypasses it;
     - a branch ruleset for `main` that blocks deletion and force pushes, requires a pull request merged with a merge commit, and requires the secret scan's `gitleaks` check, from GitHub Actions, to pass. It does not require the core, server and extension checks. Their workflows run only when their paths change, and GitHub would hold a pull request that touches none of them forever, waiting for checks that never come. Their job names (`test (ubuntu-latest, 3.13)`, `lint-and-audit`) are also shared between workflows, so a required name could be met by the wrong one. Requiring them needs one job that always runs and reports their result: until then, step 3's `&&` is what holds them.
   - **Workflows from forks**: under **Settings › Actions › General**, require approval for workflows from fork pull requests by all outside contributors, so a stranger's pull request runs nothing until you have read it.
8. [ ] **Confirm that the leaked Snowflake key was rotated** (DCS-hov, in data-capsule's tracker), as the security audit asks before a public release (LP-qy0.9).
9. [ ] **Remove the beta collaborators** from the private archive ([beta.md](beta.md#after-g1)). None are added to the new repository.

Then **cut 0.2.0** in the new repository, as in [Cutting a release](#cutting-a-release), and approve the `registries` deployment when the run asks. `verify_release.py v0.2.0` then checks the attestations too. Done: tagged on 13 November 2026, and on both registries since 15 November. The files the Marketplace and Open VSX serve are byte-for-byte the VSIX on the GitHub release.

**Marketplace pre-releases are not used** (decided 13 November 2026). Every version is a normal release on both registries, and `vsce publish --pre-release` is not set up. The Marketplace shows LineagePilot as a Preview instead, from `"preview": true` in `extension/package.json`.

**Long-lived tokens** (security audit H4). The Marketplace job holds none (item 5). Open VSX still uses `OVSX_PAT`, which does not expire and can publish to the namespace from anywhere until it is deleted. Keep it only in the `registries` environment, and replace it at once if it may have leaked. ovsx 1.3.0 also has `--oidc`, for Open VSX's own trusted publishing: check Open VSX's documentation before switching to it.

### Publishing the source

The new repository gets `main` only, with every commit rewritten so that:
- the five folders that held copies of dbt-labs/jaffle-shop are gone from all of history, not just from the latest commit (LP-0tf);
- every author and committer address is the GitHub noreply address.

The `v0.1.1` tag and its release stay in the private archive. The first release in the new repository is 0.2.0.

1. **Point this clone at the archive first.** Once the new repository takes the name `arrufus/lineagepilot`, a clone whose `origin` still says that name would push to it, and this clone's history holds the jaffle-shop files. So, straight after renaming:

   ```bash
   git remote set-url origin https://github.com/arrufus/lineagepilot-private.git
   ```

   Work on the public repository from a fresh clone of it, never from this one.

2. **Make the filtered copy**, from a clone of `main` alone in an empty folder. The mailmap is built from the history itself, so no address is written here:

   ```bash
   WORK=$(mktemp -d) && cd "$WORK"
   git clone --no-local --single-branch --branch main --no-tags \
     https://github.com/arrufus/lineagepilot-private.git lineagepilot
   cd lineagepilot
   git log --format='%ae%n%ce' | sort -u \
     | grep -v -e '@users\.noreply\.github\.com$' -e '^noreply@github\.com$' \
     | sed 's/.*/Rotimi Ademola <30685928+arrufus@users.noreply.github.com> <&>/' > ../mailmap
   uvx --from git-filter-repo git-filter-repo --force --mailmap ../mailmap --invert-paths \
     --path fixtures/jaffle-shop/ \
     --path packages/core/tests/fixtures/dbt/jaffle-1.12/ \
     --path packages/core/tests/fixtures/dbt/jaffle-fusion-2.0/ \
     --path spikes/s2-parse-perf/tests/fixtures/jaffle-1.12/ \
     --path spikes/s2-parse-perf/tests/fixtures/jaffle-fusion-2.0/
   ```

   `noreply@github.com` is left alone: it is GitHub's own address, as committer of commits made on github.com.

3. **Check it before pushing.** Each of these must print what its comment says:

   ```bash
   git log --all --format= --name-only | grep -c -i -E 'jaffle-shop/|jaffle-1\.12/|jaffle-fusion-2\.0/'   # 0
   git log --all --format='%ae%n%ce' | sort -u                     # only noreply addresses
   git log --all -p --format='%an %ae %cn %ce %B' \
     | grep -c -F -f <(sed -E 's/.*<([^>]*)>$/\1/' ../mailmap)     # 0: no old address in any file or message
   git tag | wc -l                                                 # 0
   git status --short && git log --oneline -1                      # clean, at the tip of main
   ```

4. **Push `main`, and nothing else.** Pushing a `v*` tag starts the release workflow, so no tag goes until the repository is ready for 0.2.0:

   ```bash
   git remote add origin https://github.com/arrufus/lineagepilot.git
   git push -u origin main
   ```

5. **Work from a fresh clone** of `arrufus/lineagepilot` from now on, with the noreply address set in it (step 3 of the checklist above).

This was rehearsed on 13 November 2026 on a scratch copy: all 75 commits survived, no jaffle-shop path and no personal address remained, and `v0.1.1`'s tagger was mapped too (the steps above leave tags out altogether).

---

## Building a VSIX to test locally

```bash
cd extension
npm ci --ignore-scripts && npm rebuild esbuild   # as CI: esbuild's install script only
python scripts/bundle_libs.py          # the pure-Python libraries, from requirements.lock with hashes
npm run package                        # also builds the sample project and THIRD_PARTY_NOTICES.md
SOURCE_DATE_EPOCH=$(git log -1 --pretty=%ct) python scripts/normalise_vsix.py dist/lineagepilot.vsix
python scripts/check_vsix.py dist/lineagepilot.vsix --release
code --install-extension dist/lineagepilot.vsix
```

What the VSIX holds:
- the bundled extension and its two views;
- the language server and its pure-Python libraries;
- the walkthrough's pages;
- the sample project, built from `fixtures/impact-shop` by `scripts/build_sample.py`;
- `LICENSE`, the changelog, and `THIRD_PARTY_NOTICES.md`, with the licence of every npm and Python package it carries code from.

`scripts/third_party_notices.py` fails the build if a package's licence is not on its allowed list, so shipping a new licence is a deliberate decision. `scripts/check_vsix.py` fails if the VSIX carries a pip `direct_url.json`, which records the build machine's paths.

CI audits the npm dependencies in two steps. `npm audit --omit=dev --audit-level=high` covers what ships. `scripts/npm_audit_gate.py` covers the rest of the tree: it fails on a high advisory in the tools that build and publish the VSIX (vsce, ovsx, esbuild), and on a critical one in the test tools. Moderate and low advisories are listed in the log, not gated.
