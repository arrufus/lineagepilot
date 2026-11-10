"""Check that a GitHub release of LineagePilot is complete, and say who can download it (task 6.2).

Usage, from a clone of the repository, with the GitHub CLI signed in to an account that can read it:

    python extension/scripts/verify_release.py v0.1.1 [--repo arrufus/lineagepilot]

It downloads the release into a temporary directory, so an earlier download gets in nobody's way,
and checks that:
- the release is published, not a draft or a pre-release, and is the repository's latest release;
- the release workflow made it, rather than someone uploading a file;
- it holds lineagepilot-<version>.vsix and SHA256SUMS, fully uploaded;
- the VSIX downloaded whole: its SHA-256 is the one in SHA256SUMS and the one GitHub recorded on
  upload, and its package.json is the version the tag names;
- the release workflow's run for the tag succeeded on the tagged commit, every job in it passed,
  and its release job logged the same SHA-256 as it named the file;
- each file has a build provenance attestation, once the repository is public.

It ends by saying who can download the release. It exits 1 if a check failed, and 2 if it could not
ask GitHub.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

from release_check import TAG

REPO = "arrufus/lineagepilot"
WORKFLOW = "lineagepilot-release.yml"
BOT = "github-actions[bot]"
SUMS = "SHA256SUMS"
# Jobs that must be in the release run: the tests of every part, the secret scan and the release
REQUIRED_JOBS = ("verify", "core", "lsp", "extension", "secret-scan", "github-release")
# Jobs that run only when LP_PUBLISH_REGISTRIES is 'true', so may be skipped
REGISTRY_JOBS = ("marketplace", "open-vsx")
DOCS = "docs/release.md"


class GhError(Exception):
    """The GitHub CLI failed. `missing` is true when GitHub answered 404 or 410."""

    def __init__(self, args: tuple[str, ...], stderr: str):
        super().__init__(f"gh {' '.join(args)}: {stderr.strip()}")
        self.missing = "HTTP 404" in stderr or "HTTP 410" in stderr


class GitHub:
    """What the check reads from GitHub, through the gh CLI and its sign-in."""

    def __init__(self, repo: str):
        self.repo = repo

    def gh(self, *args: str) -> str:
        try:
            done = subprocess.run(["gh", *args], capture_output=True, text=True, encoding="utf-8",
                                  errors="replace", check=False)
        except FileNotFoundError:
            raise GhError(args, "the GitHub CLI is not installed: https://cli.github.com") from None
        if done.returncode != 0:
            raise GhError(args, done.stderr)
        return done.stdout

    def api(self, path: str) -> dict:
        return json.loads(self.gh("api", path))

    def api_or_none(self, path: str) -> dict | None:
        try:
            return self.api(path)
        except GhError as error:
            if error.missing:
                return None
            raise

    def repository(self) -> dict:
        return self.api(f"repos/{self.repo}")

    def release(self, tag: str) -> dict | None:
        return self.api_or_none(f"repos/{self.repo}/releases/tags/{tag}")

    def latest(self) -> dict | None:
        return self.api_or_none(f"repos/{self.repo}/releases/latest")

    def commit(self, tag: str) -> str:
        return self.api(f"repos/{self.repo}/commits/{tag}")["sha"]

    def runs(self, tag: str) -> list[dict]:
        """The release workflow's runs for `tag`, newest first."""
        return self.api(f"repos/{self.repo}/actions/workflows/{WORKFLOW}/runs?branch={tag}&event=push")["workflow_runs"]

    def jobs(self, run_id: int) -> list[dict]:
        return self.api(f"repos/{self.repo}/actions/runs/{run_id}/jobs?per_page=100")["jobs"]

    def job_log(self, job_id: int) -> str | None:
        """The job's log, or None once GitHub has deleted it."""
        try:
            return self.gh("api", f"repos/{self.repo}/actions/jobs/{job_id}/logs")
        except GhError as error:
            if error.missing:
                return None
            raise

    def download(self, tag: str, directory: Path) -> None:
        self.gh("release", "download", tag, "--repo", self.repo, "--dir", str(directory))

    def attestation(self, path: Path, tag: str) -> str | None:
        """None if `path` has a build provenance attestation from the release workflow, run for
        `tag`; otherwise what gh said."""
        try:
            self.gh("attestation", "verify", str(path), "--repo", self.repo,
                    "--signer-workflow", f"{self.repo}/.github/workflows/{WORKFLOW}",
                    "--source-ref", f"refs/tags/{tag}", "--deny-self-hosted-runners")
        except GhError as error:
            return str(error)
        return None


@dataclass
class Check:
    ok: bool | None  # None: noted, or could not be checked
    text: str


@dataclass
class Report:
    title: str
    checks: list[Check] = field(default_factory=list)
    audience: str = ""

    def add(self, ok: bool | None, text: str) -> None:
        self.checks.append(Check(ok, text))

    @property
    def failed(self) -> list[Check]:
        return [c for c in self.checks if c.ok is False]

    def render(self) -> str:
        marks = {True: "ok  ", False: "FAIL", None: "--  "}
        lines = [self.title, *(f"  {marks[c.ok]}  {c.text}" for c in self.checks)]
        if self.audience:
            lines += ["", self.audience]
        failed = len(self.failed)
        lines += ["", "The release is complete." if not failed else
                  f"The release is NOT complete: {failed} check{'s' if failed > 1 else ''} failed. See '{DOCS}'."]
        return "\n".join(lines)


def listed_hash(sums: str, name: str) -> str | None:
    """The SHA-256 that a sha256sum file gives `name`, or None if it gives none or several."""
    found = {m.group(1) for line in sums.splitlines()
             if (m := re.fullmatch(r"([0-9a-f]{64}) [ *](.+?)\s*", line)) and m.group(2) == name}
    return found.pop() if len(found) == 1 else None


def packaged_version(path: Path) -> str | None:
    """The version in the VSIX's package.json, or None if it is not a LineagePilot VSIX."""
    try:
        with zipfile.ZipFile(path) as vsix:
            package = json.loads(vsix.read("extension/package.json"))
    except (OSError, zipfile.BadZipFile, KeyError, ValueError):
        return None
    return package.get("version") if package.get("name") == "lineagepilot" else None


def logged_hashes(log: str, name: str) -> set[str]:
    """The SHA-256 values a job's log printed for `name`, as sha256sum prints them."""
    return set(re.findall(rf"\b([0-9a-f]{{64}})  {re.escape(name)}\s*$", log, re.MULTILINE))


def job_problems(jobs: list[dict]) -> list[str]:
    """Why the release run's jobs do not show a tested release, if they do not."""
    names = [job["name"] for job in jobs]
    problems = [f"no {required} job" for required in REQUIRED_JOBS
                if not any(n == required or n.startswith(required + " / ") for n in names)]
    for job in jobs:
        if job["conclusion"] == "success" or (job["name"] in REGISTRY_JOBS and job["conclusion"] == "skipped"):
            continue
        problems.append(f"{job['name']}: {job['conclusion'] or job['status']}")
    return problems


def audience(repository: dict, url: str) -> str:
    if repository.get("private"):
        return (f"Who can download it: only people with access to the private repository {repository['full_name']}. "
                "It is not public, by design during the closed beta. Invite a beta team with the Read role "
                "(docs/beta.md, 'Inviting a team'); the link works for them once they accept. "
                "Releases become public when the repository does, after Gate G1.")
    return f"Who can download it: anyone, from {url}"


def assess(github: GitHub, tag: str, workdir: Path) -> Report:
    """Check the release of `tag`, downloading its files into `workdir`."""
    report = Report(f"LineagePilot {tag}, in {github.repo}")
    match = TAG.match(tag)
    if not match:
        report.add(False, f"{tag!r} is not a release tag: v<major>.<minor>.<patch>")
        return report
    vsix = f"lineagepilot-{match.group(1)}.vsix"
    repository = github.repository()
    release = github.release(tag)
    if release is None:
        report.add(False, f"there is no published release for {tag}: see 'If a release fails' in {DOCS}")
        report.audience = audience(repository, f"https://github.com/{github.repo}/releases")
        return report
    report.title += f": {release['html_url']}"
    report.audience = audience(repository, release["html_url"])
    check_release(report, github, tag, release)
    sha256 = check_files(report, github, tag, release, match.group(1), workdir)
    check_run(report, github, tag, vsix, sha256)
    if repository.get("private"):
        report.add(None, "no attestation to check: GitHub makes them only in public repositories on this plan")
    elif sha256:
        for name in (vsix, SUMS):
            problem = github.attestation(workdir / name, tag)
            if problem is None:
                report.add(True, f"{name} has a build provenance attestation from the release workflow for {tag}")
            else:
                report.add(False, f"{name} has no valid attestation (a release made while the repository was "
                                  f"private has none): {problem}")
    return report


def check_release(report: Report, github: GitHub, tag: str, release: dict) -> None:
    """The release is published, is the latest, and the release workflow made it."""
    if release.get("draft"):
        report.add(False, "it is still a draft")
    elif release.get("prerelease"):
        report.add(False, "it is marked as a pre-release, which GitHub never makes the latest release. "
                          f"Fix: gh release edit {tag} --repo {github.repo} --prerelease=false --latest")
    else:
        report.add(True, f"published on {release.get('published_at')}, not as a draft or a pre-release")
    latest = github.latest()
    if latest is None:
        report.add(False, f"the repository has no latest release, so https://github.com/{github.repo}/releases/latest "
                          "is not found, and `gh release download` without a tag fails")
    elif latest["tag_name"] == tag:
        report.add(True, f"it is the latest release: https://github.com/{github.repo}/releases/latest")
    else:
        report.add(None, f"it is not the latest release: {latest['tag_name']} is")
    author = (release.get("author") or {}).get("login")
    if author == BOT:
        report.add(True, f"the release workflow made it: its author is {BOT}")
    else:
        report.add(False, f"its author is {author}, not the release workflow ({BOT}): do not hand it out")


def check_files(report: Report, github: GitHub, tag: str, release: dict, version: str, workdir: Path) -> str | None:
    """The release holds the VSIX and SHA256SUMS, and they download whole. The VSIX's SHA-256, if
    it could be downloaded."""
    vsix = f"lineagepilot-{version}.vsix"
    assets = {asset["name"]: asset for asset in release.get("assets", [])}
    problems = []
    if missing := sorted({vsix, SUMS} - set(assets)):
        problems.append(f"missing {', '.join(missing)}")
    if extra := sorted(set(assets) - {vsix, SUMS}):
        problems.append(f"unexpected {', '.join(extra)}")
    if partial := sorted(n for n in {vsix, SUMS} & set(assets) if assets[n].get("state") != "uploaded"):
        problems.append(f"not fully uploaded: {', '.join(partial)}")
    if problems:
        report.add(False, "its files are wrong: " + "; ".join(problems))
    else:
        report.add(True, f"it holds {vsix} and {SUMS}, fully uploaded")
    if missing:
        return None

    github.download(tag, workdir)
    sha256 = hashlib.sha256((workdir / vsix).read_bytes()).hexdigest()
    listed = listed_hash((workdir / SUMS).read_text(encoding="utf-8", errors="replace"), vsix)
    recorded = (assets[vsix].get("digest") or "").removeprefix("sha256:")
    problems = []
    if listed != sha256:
        problems.append(f"{SUMS} gives {listed or 'no single SHA-256'}")
    if recorded and recorded != sha256:
        problems.append(f"GitHub recorded {recorded} on upload")
    if problems:
        report.add(False, f"{vsix} has SHA-256 {sha256}, but " + "; ".join(problems))
    else:
        report.add(True, f"{vsix} downloaded whole: SHA-256 {sha256}, as in {SUMS}"
                         + (" and as GitHub recorded on upload" if recorded else ""))
    packaged = packaged_version(workdir / vsix)
    if packaged == version:
        report.add(True, f"the VSIX is LineagePilot {version}")
    else:
        report.add(False, f"the VSIX is not LineagePilot {version}: its package.json says {packaged or 'nothing readable'}")
    return sha256


def check_run(report: Report, github: GitHub, tag: str, vsix: str, sha256: str | None) -> None:
    """The release workflow's run for the tagged commit passed, and logged the VSIX's SHA-256."""
    commit = github.commit(tag)
    runs = [run for run in github.runs(tag) if run.get("head_sha") == commit]
    if not runs:
        report.add(False, f"there is no release run for {tag} at {commit[:7]}")
        return
    run = runs[0]
    if run["status"] != "completed":
        report.add(False, f"the release run is still {run['status'].replace('_', ' ')}: "
                          f"gh run watch {run['id']} --repo {github.repo} --exit-status")
    elif run["conclusion"] != "success":
        report.add(False, f"the release run ended in {run['conclusion']}: {run['html_url']}")
    else:
        report.add(True, f"the release run for {tag} at {commit[:7]} succeeded: {run['html_url']}")
    jobs = github.jobs(run["id"])
    if problems := job_problems(jobs):
        report.add(False, "the release run's jobs did not all pass: " + "; ".join(problems))
    else:
        report.add(True, f"all {len(jobs)} of its jobs passed or were not due: the core, server and extension "
                         "tests on every platform, the secret scan and the release")
    if skipped := [job["name"] for job in jobs if job["name"] in REGISTRY_JOBS and job["conclusion"] == "skipped"]:
        report.add(None, f"not published to the Marketplace or Open VSX: {' and '.join(skipped)} stay off "
                         "until the public release")
    release_job = next((job for job in jobs if job["name"] == "github-release"), None)
    if release_job is None or sha256 is None:
        return
    log = github.job_log(release_job["id"])
    if log is None:
        report.add(None, "the release job's log has been deleted, so the SHA-256 it logged cannot be compared")
    elif (logged := logged_hashes(log, vsix)) == {sha256}:
        report.add(True, "the release job logged the same SHA-256 when it named the file")
    else:
        report.add(False, f"the release job logged {', '.join(sorted(logged)) or 'no SHA-256'} for {vsix}, not {sha256}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("tag", help="the release's tag, for example v0.1.1")
    parser.add_argument("--repo", default=REPO, help=f"OWNER/REPO (default {REPO})")
    args = parser.parse_args(argv)
    with tempfile.TemporaryDirectory(prefix="lineagepilot-release-") as directory:
        try:
            report = assess(GitHub(args.repo), args.tag, Path(directory))
        except GhError as error:
            print(f"verify_release: could not ask GitHub: {error}", file=sys.stderr)
            return 2
    print(report.render())
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())
