# Releasing LineagePilot

How a version of the extension reaches users (task 6.2, LP-7er.2). Releases are built and published by CI only. A VSIX built anywhere else is for testing and never reaches a user.

**During the closed beta** a release is a `.vsix` attached to a GitHub release of this private repository, and beta teams download it ([beta.md](beta.md)). The Marketplace and Open VSX jobs exist, but stay off until the public release, after Gate G1.

---

## Cutting a release

1. **Bump the version** in `extension/`, on a branch:

   ```bash
   cd extension && npm version 0.1.1 --no-git-tag-version   # updates package.json and package-lock.json
   ```

   Versions are `major.minor.patch`, as both registries require: no `-beta.1` suffix. While the version is `0.x`, the GitHub release is marked as a pre-release.

2. **Add its section to `extension/CHANGELOG.md`**, headed `## 0.1.1`. Its text becomes the release notes, and the release fails without it.

3. **Merge to `main`**, with CI green.

4. **Tag the merge commit on `main`, and push the tag**:

   ```bash
   git switch main && git pull
   git tag -a v0.1.1 -m "LineagePilot 0.1.1"
   git push origin v0.1.1
   ```

The tag starts [`lineagepilot-release`](../.github/workflows/lineagepilot-release.yml):

| Job | What it does |
|---|---|
| `verify` | The tagged commit is on `main`. The tag, `package.json` and `CHANGELOG.md` agree (`extension/scripts/release_check.py`). |
| `core`, `lsp`, `extension` | The three CI workflows, on the tagged commit. `extension` builds the one universal VSIX, without npm install scripts, and normalises it so its bytes depend only on its files and the commit's time. Then it tests it on Linux, macOS and Windows with Python 3.10 and 3.13, trusted and in Restricted Mode. |
| `secret-scan` | gitleaks over the whole git history ([`lineagepilot-secrets`](../.github/workflows/lineagepilot-secrets.yml), allowlist in [`.gitleaks.toml`](../.gitleaks.toml)). |
| `github-release` | Checks that VSIX against both registries' rules, licence included (`check_vsix.py --release`), and that it is normalised for the tagged commit. Attests its build provenance and that of `SHA256SUMS` (public repository only, see below). Attaches it as `lineagepilot-<version>.vsix`, with `SHA256SUMS`, to a GitHub release with the changelog's notes. |
| `marketplace`, `open-vsx` | Publish the same file, after checking its SHA-256. These run only when the repository variable `LP_PUBLISH_REGISTRIES` is `true`, and only after a reviewer approves the `registries` environment. |

Nothing is published unless every test passes. The VSIX that is released is the one that was tested: it is built once, in the `extension` job, and never rebuilt.

### If a release fails

- **`verify` fails**: fix the version or the changelog on `main`, delete the tag (`git push origin :v0.1.1 && git tag -d v0.1.1`), and tag again.
- **A test fails**: no release was made. Fix it on `main`, then move the tag the same way.
- **A publish job fails** after the GitHub release: re-run that job from the Actions page. It publishes the VSIX already released. Never publish a different file under the same version.

### Checking a release

`SHA256SUMS` shows that a download is complete, but it comes from the same GitHub release, so it cannot show who made the file. The build provenance attestation can: it is signed with a short-lived Sigstore certificate issued to the release workflow's run, and it names the repository, the workflow, the tag and the commit.

```bash
gh release download v0.1.1 --repo arrufus/lineagepilot     # the VSIX and SHA256SUMS
sha256sum --check SHA256SUMS
gh attestation verify lineagepilot-0.1.1.vsix --repo arrufus/lineagepilot \
  --signer-workflow arrufus/lineagepilot/.github/workflows/lineagepilot-release.yml \
  --source-ref refs/tags/v0.1.1 --deny-self-hosted-runners
```

`gh attestation verify` fails unless an attestation for exactly these bytes was made by that workflow, run for that tag. `SHA256SUMS` is attested the same way.

GitHub makes attestations only in public repositories on this account's plan (private ones need GitHub Enterprise Cloud), so the step is skipped while the repository is private: the closed beta's releases have no attestation, and `gh attestation verify` finds none for them. The first release after the repository goes public is the first one attested.

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

- [ ] **GitHub Actions runs again** (LP-kf5): billing, under the account's **Billing and plans**. Until then, no workflow starts, this one included.
- [ ] **Protect release tags**: in the repository's **Settings › Rules › Rulesets**, add a tag ruleset for `v*` that only you can create, update or delete. A tag is what releases.
- [ ] **Create the labels the beta's issue forms use**:

  ```bash
  gh label create beta --color 1d76db --description "From the closed beta"
  gh label create beta-checkin --color 0e8a16 --description "A team's weekly check-in"
  gh label create feedback --color c5def5 --description "An idea, a question or something that could be better"
  ```

  `bug` exists already in a new repository.

That is all a GitHub release needs: it uses the workflow's own token.

### Before the first public release, after Gate G1

The Marketplace and Open VSX are public: anyone can find and install what is published there.

- [ ] **Trademark search for "LineagePilot"** (decision D-4), by someone qualified to do one. Keep "dbt" out of the name and the publisher, per dbt Labs' trademark guidelines.
- [ ] **Marketplace publisher** `lineagepilot`, the `publisher` in `extension/package.json`. Create it at <https://marketplace.visualstudio.com/manage> with a Microsoft account that has two-factor authentication on. Then create an Azure DevOps personal access token with the **Marketplace › Manage** scope, for **All accessible organizations**. It lasts a year at most: note when it expires.
- [ ] **Open VSX namespace** `lineagepilot`: sign in to <https://open-vsx.org> with GitHub (two-factor on), sign the Eclipse publisher agreement, create the namespace (`npx ovsx create-namespace lineagepilot -p <token>`), and claim it. The access token is on the same settings page.
- [ ] **A GitHub environment called `registries`**, under **Settings › Environments**: add yourself as a required reviewer, allow only tags `v*` to deploy, and add the secrets `VSCE_PAT` and `OVSX_PAT`. The tokens live only there, so only an approved release job can read them.
- [ ] **Set the repository variable** `LP_PUBLISH_REGISTRIES` to `true`, under **Settings › Secrets and variables › Actions › Variables**.
- [ ] **Replace the jaffle-shop fixtures** (LP-0tf) and make the repository public, so that the README's links work for everyone.

Marketplace pre-releases (`vsce publish --pre-release`, odd minor versions by convention) are not set up. Decide whether to use them before the first public release.

**Recommended follow-up: publish without long-lived tokens.** `VSCE_PAT` lasts up to a year and can publish any version from anywhere until it expires. vsce can instead authenticate with Microsoft Entra ID (`vsce publish --azure-credential`, after `azure/login` with a federated credential bound to this repository and the `registries` environment), so the job holds only a short-lived OIDC token. vsce 4.0.0 and ovsx 1.3.0 also carry OpenID Connect trusted publishing (`--oidc`; hidden from vsce's help). Check each registry's documentation for its status before switching. Publishing is unchanged for now (security audit H4).

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
