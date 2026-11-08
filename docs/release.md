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
| `core`, `lsp`, `extension` | The three CI workflows, on the tagged commit. `extension` builds the one universal VSIX, then tests it on Linux, macOS and Windows with Python 3.10 and 3.13, trusted and in Restricted Mode. |
| `github-release` | Checks that VSIX against both registries' rules, licence included (`check_vsix.py --release`). Attaches it as `lineagepilot-<version>.vsix`, with `SHA256SUMS`, to a GitHub release with the changelog's notes. |
| `marketplace`, `open-vsx` | Publish the same file, after checking its SHA-256. These run only when the repository variable `LP_PUBLISH_REGISTRIES` is `true`, and only after a reviewer approves the `registries` environment. |

Nothing is published unless every test passes. The VSIX that is released is the one that was tested: it is built once, in the `extension` job, and never rebuilt.

### If a release fails

- **`verify` fails**: fix the version or the changelog on `main`, delete the tag (`git push origin :v0.1.1 && git tag -d v0.1.1`), and tag again.
- **A test fails**: no release was made. Fix it on `main`, then move the tag the same way.
- **A publish job fails** after the GitHub release: re-run that job from the Actions page. It publishes the VSIX already released. Never publish a different file under the same version.

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

---

## Building a VSIX to test locally

```bash
cd extension
npm ci
python scripts/bundle_libs.py          # the pure-Python libraries, from requirements.lock with hashes
npm run package                        # also builds the sample project and THIRD_PARTY_NOTICES.md
python scripts/check_vsix.py dist/lineagepilot.vsix --release
code --install-extension dist/lineagepilot.vsix
```

What the VSIX holds:
- the bundled extension and its two views;
- the language server and its pure-Python libraries;
- the walkthrough's pages;
- the sample project, built from `fixtures/impact-shop` by `scripts/build_sample.py`;
- `LICENSE`, the changelog, and `THIRD_PARTY_NOTICES.md`, with the licence of every npm and Python package it carries code from.

`scripts/third_party_notices.py` fails the build if a package's licence is not on its allowed list, so shipping a new licence is a deliberate decision.
