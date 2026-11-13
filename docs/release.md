# Releasing LineagePilot

How a version of the extension reaches users (task 6.2, LP-7er.2). Releases are built and published by CI only. A VSIX built anywhere else is for testing and never reaches a user.

**After Gate G1**, which passed on 13 November 2026, a release goes to the VS Code Marketplace and Open VSX as well as to GitHub releases, once the checklist in [Before the first public release](#before-the-first-public-release-after-gate-g1) is done. Until then the Marketplace and Open VSX jobs stay off, and a release is only a `.vsix` attached to a GitHub release, as it was during the closed beta ([beta.md](beta.md)).

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

   The `&&` is what makes it "CI green": `gh pr checks` fails if any check failed, and then nothing is merged. GitHub does not enforce it here. On its current plan this private repository can have no branch protection or rulesets, and auto-merge is off. Merge with `--merge`, not `--squash` or `--rebase`, so that step 4 has a merge commit to tag. If `gh pr checks` says no checks were reported, the workflows have not registered yet: wait a few seconds and run that line again.

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

6. **Check that it reached users.** The Marketplace checks a new version before it lists it, which can take some minutes. Then it is at <https://marketplace.visualstudio.com/items?itemName=lineagepilot.lineagepilot> and <https://open-vsx.org/extension/lineagepilot/lineagepilot>, and editors offer it as an update. The VSIX is always at <https://github.com/arrufus/lineagepilot/releases/latest>. While a repository is private, only you and the collaborators you invite can open its releases, and anyone else gets GitHub's "Page not found": that is how the closed beta worked ([beta.md](beta.md#inviting-a-team)).

The tag starts [`lineagepilot-release`](../.github/workflows/lineagepilot-release.yml):

| Job | What it does |
|---|---|
| `verify` | The tagged commit is on `main`. The tag, `package.json` and `CHANGELOG.md` agree (`extension/scripts/release_check.py`). |
| `core`, `lsp`, `extension` | The three CI workflows, on the tagged commit. `extension` builds the one universal VSIX, without npm install scripts, and normalises it so its bytes depend only on its files and the commit's time. Then it tests it on Linux, macOS and Windows with Python 3.10 and 3.13, trusted and in Restricted Mode. |
| `secret-scan` | gitleaks over the whole git history ([`lineagepilot-secrets`](../.github/workflows/lineagepilot-secrets.yml), allowlist in [`.gitleaks.toml`](../.gitleaks.toml)). |
| `github-release` | Checks that VSIX against both registries' rules, licence included (`check_vsix.py --release`), and that it is normalised for the tagged commit. Attests its build provenance and that of `SHA256SUMS` (public repository only, see below). Attaches it as `lineagepilot-<version>.vsix`, with `SHA256SUMS`, to a GitHub release, which becomes the latest release. Its notes are the changelog's, ending with the VSIX's SHA-256 and a link to the run. |
| `marketplace`, `open-vsx` | Publish the same file, after checking its SHA-256. These run only when the repository variable `LP_PUBLISH_REGISTRIES` is `true`, and only after a reviewer approves the `registries` environment. |

Nothing is published unless every test passes. The VSIX that is released is the one that was tested: it is built once, in the `extension` job, and never rebuilt.

### If a release fails

- **`verify` fails**: fix the version or the changelog on `main`, delete the tag (`git push origin :v0.1.1 && git tag -d v0.1.1`), and tag again.
- **A test fails**: no release was made. Fix it on `main`, then move the tag the same way.
- **A publish job fails** after the GitHub release: re-run that job from the Actions page. It publishes the VSIX already released. Never publish a different file under the same version.
- **`verify_release.py` fails** on a release the workflow made: it says which check failed. A pre-release is fixed by the command it prints. For anything else, do not hand the release out: fix the cause on `main` and release a new version. Never replace a released file.

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

1. [ ] **Replace the jaffle-shop fixtures** (LP-0tf) with a project of our own, on `main` here, with CI green. History filter: see 'Publishing the source' below.
2. [ ] **Trademark search for "LineagePilot"** (decision D-4), by someone qualified to do one. Keep "dbt" out of the name and the publisher, per dbt Labs' trademark guidelines.
3. [ ] **Keep your personal email address out of what you publish.** Set git's identity to your GitHub noreply address, in this clone and globally, before any commit or tag for the new repository (an annotated tag records its tagger's address too):

   ```bash
   git config user.email 30685928+arrufus@users.noreply.github.com
   git config --global user.email 30685928+arrufus@users.noreply.github.com
   ```

   Then, under GitHub's **Settings › Emails**, turn on **Keep my email addresses private** and **Block command line pushes that expose my email**.
4. [ ] **Move the source to the new repository**:
   1. Rename this one: `gh repo rename lineagepilot-private --repo arrufus/lineagepilot`. GitHub redirects the old name to it until another repository takes the name, which the next step does.
   2. Create the new one, private for now: `gh repo create arrufus/lineagepilot --private`.
   3. Push the filtered history to it, as in [Publishing the source](#publishing-the-source).
   4. Let CI run there, and check that every workflow passes on `main`. Secrets, variables, environments and settings do not move with the history: the new repository starts with none.
   5. Turn on private vulnerability reporting, which `SECURITY.md` points to: **Settings › Code security › Private vulnerability reporting**, or `gh api -X PUT repos/arrufus/lineagepilot/private-vulnerability-reporting`.
   6. Create the label the feedback form uses. `bug` exists already in a new repository:

      ```bash
      gh label create feedback --repo arrufus/lineagepilot --color c5def5 --description "An idea, a question or something that could be better"
      ```

   7. Make it public: `gh repo edit arrufus/lineagepilot --visibility public --accept-visibility-change-consequences`. The READMEs' links then work for everyone, releases get build provenance attestations, and the rulesets and environment below can be enforced.
5. [ ] **Marketplace publisher** `lineagepilot`, the `publisher` in `extension/package.json`. Create it at <https://marketplace.visualstudio.com/manage> with a Microsoft account that has two-factor authentication on. Then create an Azure DevOps personal access token with the **Marketplace › Manage** scope, for **All accessible organizations**. It lasts a year at most: note when it expires.
6. [ ] **Open VSX namespace** `lineagepilot`: sign in to <https://open-vsx.org> with GitHub (two-factor on), sign the Eclipse publisher agreement, create the namespace (`npx ovsx create-namespace lineagepilot -p <token>`), and claim it. The access token is on the same settings page.
7. [ ] **Protect the new, public repository** (audit H8 and H9, LP-qy0.9):
   - **A GitHub environment called `registries`**, under **Settings › Environments**: add yourself as a required reviewer, allow only tags `v*` to deploy, and add the secrets `VSCE_PAT` and `OVSX_PAT`. The tokens live only there, so only an approved release job can read them. Check that the required reviewer and the tag rule show as saved before you add the secrets.
   - **Set the repository variable** `LP_PUBLISH_REGISTRIES` to `true`, under **Settings › Secrets and variables › Actions › Variables**.
   - **Rulesets**, under **Settings › Rules › Rulesets**:
     - a tag ruleset for `v*` that only you can create, update or delete;
     - a branch ruleset for `main` that requires a pull request and the core, server, extension and secret-scan checks to pass. Step 3's `&&` is then enforced by GitHub as well.
   - **Workflows from forks**: under **Settings › Actions › General**, require approval for workflows from fork pull requests by all outside contributors, so a stranger's pull request runs nothing until you have read it.
8. [ ] **Confirm that the leaked Snowflake key was rotated** (DCS-hov, in data-capsule's tracker), as the security audit asks before a public release (LP-qy0.9).
9. [ ] **Remove the beta collaborators** from the private archive ([beta.md](beta.md#after-g1)). None are added to the new repository.

Then **cut 0.2.0** in the new repository, as in [Cutting a release](#cutting-a-release), and approve the `registries` deployment when the run asks. `verify_release.py v0.2.0` then checks the attestations too.

**Marketplace pre-releases are not used** (decided 13 November 2026). Every version is a normal release on both registries, and `vsce publish --pre-release` is not set up. The Marketplace shows LineagePilot as a Preview instead, from `"preview": true` in `extension/package.json`.

**Recommended follow-up: publish without long-lived tokens.** `VSCE_PAT` lasts up to a year and can publish any version from anywhere until it expires. vsce can instead authenticate with Microsoft Entra ID (`vsce publish --azure-credential`, after `azure/login` with a federated credential bound to this repository and the `registries` environment), so the job holds only a short-lived OIDC token. vsce 4.0.0 and ovsx 1.3.0 also carry OpenID Connect trusted publishing (`--oidc`; hidden from vsce's help). Check each registry's documentation for its status before switching. Publishing is unchanged for now (security audit H4).

### Publishing the source

TODO(parent): filter-repo steps

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
