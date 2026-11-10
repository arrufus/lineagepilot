"""verify_release.py, the one-command check that a GitHub release is complete (task 6.2, LP-fzx).
GitHub is replaced by a fake that answers as the real API does. Run with the other script tests:

    uv run --project ../packages/lsp --locked pytest --no-cov -p no:cacheprovider scripts/tests
"""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import release_check  # noqa: E402
import verify_release as vr  # noqa: E402

TAG, REPO, COMMIT, RUN = "v0.1.1", "arrufus/lineagepilot", "95546ecdca17d902feaa851e172e4759cc91f34e", 42700988466
VSIX = "lineagepilot-0.1.1.vsix"


def vsix_bytes(package: dict | None = None) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as vsix:
        vsix.writestr("extension/package.json", json.dumps(package or {"name": "lineagepilot", "version": "0.1.1"}))
    return buffer.getvalue()


def log_line(sha256: str, name: str = VSIX) -> str:
    return f"2026-11-09T21:27:31.5236609Z {sha256}  {name}\r\n"


class FakeGitHub(vr.GitHub):
    """A complete release of v0.1.1 from a private repository, as GitHub reports it. Tests break
    one part at a time."""

    def __init__(self, vsix: bytes | None = None):
        super().__init__(REPO)
        self.files = {VSIX: vsix if vsix is not None else vsix_bytes()}
        sha256 = hashlib.sha256(self.files[VSIX]).hexdigest()
        self.files["SHA256SUMS"] = f"{sha256}  {VSIX}\n".encode()
        self.sha256 = sha256
        self.repo_info = {"full_name": REPO, "private": True}
        self.release_info: dict | None = {
            "tag_name": TAG, "draft": False, "prerelease": False, "published_at": "2026-11-09T21:27:35Z",
            "html_url": f"https://github.com/{REPO}/releases/tag/{TAG}",
            "author": {"login": "github-actions[bot]"},
            "assets": [{"name": VSIX, "state": "uploaded", "digest": f"sha256:{sha256}"},
                       {"name": "SHA256SUMS", "state": "uploaded", "digest": "sha256:" + "0" * 64}],
        }
        self.latest_info: dict | None = {"tag_name": TAG}
        self.run_list = [{"id": RUN, "head_sha": COMMIT, "status": "completed", "conclusion": "success",
                          "html_url": f"https://github.com/{REPO}/actions/runs/{RUN}"}]
        names = ["verify", "core / test (ubuntu-latest, 3.10)", "lsp / test (macos-latest, 3.13)",
                 "extension / build", "extension / test (windows-latest, 3.10)", "secret-scan / gitleaks",
                 "github-release"]
        self.job_list = [{"id": i, "name": n, "status": "completed", "conclusion": "success"} for i, n in enumerate(names)]
        self.job_list += [{"id": 90, "name": "marketplace", "status": "completed", "conclusion": "skipped"},
                          {"id": 91, "name": "open-vsx", "status": "completed", "conclusion": "skipped"}]
        self.log: str | None = "##[group]Run mkdir -p release\r\n" + log_line(sha256)
        self.attested: list[str] = []
        self.attest_problem: str | None = None
        self.downloads = 0

    def repository(self):
        return self.repo_info

    def release(self, tag):
        return self.release_info

    def latest(self):
        return self.latest_info

    def commit(self, tag):
        return COMMIT

    def runs(self, tag):
        return self.run_list

    def jobs(self, run_id):
        assert run_id == RUN
        return self.job_list

    def job_log(self, job_id):
        assert job_id == next(j["id"] for j in self.job_list if j["name"] == "github-release")
        return self.log

    def download(self, tag, directory):
        self.downloads += 1
        for name, data in self.files.items():
            (directory / name).write_bytes(data)

    def attestation(self, path, tag):
        self.attested.append(path.name)
        return self.attest_problem


def run(github: FakeGitHub, tmp_path: Path) -> vr.Report:
    return vr.assess(github, TAG, tmp_path)


def failures(report: vr.Report) -> list[str]:
    return [c.text for c in report.failed]


def notes(report: vr.Report) -> list[str]:
    return [c.text for c in report.checks if c.ok is None]


# A complete release


def test_a_complete_release_from_the_private_repository_passes(tmp_path):
    github = FakeGitHub()
    report = run(github, tmp_path)
    assert failures(report) == []
    passed = " | ".join(c.text for c in report.checks if c.ok)
    for expected in ("not as a draft or a pre-release", "it is the latest release", "its author is github-actions[bot]",
                     f"it holds {VSIX} and SHA256SUMS", "as in SHA256SUMS and as GitHub recorded on upload",
                     "the VSIX is LineagePilot 0.1.1", f"at {COMMIT[:7]} succeeded", "all 9 of its jobs passed",
                     "logged the same SHA-256"):
        assert expected in passed, expected
    assert notes(report) == [
        "not published to the Marketplace or Open VSX: marketplace and open-vsx stay off until the public release",
        "no attestation to check: GitHub makes them only in public repositories on this plan",
    ]
    assert github.attested == []
    text = report.render()
    assert text.startswith(f"LineagePilot {TAG}, in {REPO}: https://github.com/{REPO}/releases/tag/{TAG}\n  ok    ")
    assert "only people with access to the private repository arrufus/lineagepilot" in text
    assert text.endswith("The release is complete.")


def test_a_public_release_is_checked_against_its_attestations(tmp_path):
    github = FakeGitHub()
    github.repo_info["private"] = False
    report = run(github, tmp_path)
    assert failures(report) == [] and github.attested == [VSIX, "SHA256SUMS"]
    assert f"Who can download it: anyone, from https://github.com/{REPO}/releases/tag/{TAG}" in report.render()
    github = FakeGitHub()
    github.repo_info["private"] = False
    github.attest_problem = "gh attestation verify: HTTP 404"
    assert failures(run(github, tmp_path)) == [
        f"{name} has no valid attestation (a release made while the repository was private has none): "
        "gh attestation verify: HTTP 404" for name in (VSIX, "SHA256SUMS")]


def test_the_registries_published_it_when_they_were_turned_on(tmp_path):
    github = FakeGitHub()
    for job in github.job_list[-2:]:
        job["conclusion"] = "success"
    report = run(github, tmp_path)
    assert failures(report) == [] and not any("Marketplace" in n for n in notes(report))


# What LP-fzx found: a pre-release is never the latest release


def test_a_pre_release_fails_with_its_fix_and_so_does_having_no_latest_release(tmp_path):
    github = FakeGitHub()
    github.release_info["prerelease"] = True
    github.latest_info = None
    assert failures(run(github, tmp_path)) == [
        ("it is marked as a pre-release, which GitHub never makes the latest release. "
         f"Fix: gh release edit {TAG} --repo {REPO} --prerelease=false --latest"),
        (f"the repository has no latest release, so https://github.com/{REPO}/releases/latest is not found, "
         "and `gh release download` without a tag fails"),
    ]


def test_an_older_release_is_noted_as_not_the_latest_without_failing(tmp_path):
    github = FakeGitHub()
    github.latest_info = {"tag_name": "v0.2.0"}
    report = run(github, tmp_path)
    assert failures(report) == [] and "it is not the latest release: v0.2.0 is" in notes(report)


def test_a_draft_fails(tmp_path):
    github = FakeGitHub()
    github.release_info["draft"] = True
    assert failures(run(github, tmp_path)) == ["it is still a draft"]


def test_a_release_someone_made_by_hand_fails(tmp_path):
    github = FakeGitHub()
    github.release_info["author"] = {"login": "rademola"}
    assert failures(run(github, tmp_path)) == [
        "its author is rademola, not the release workflow (github-actions[bot]): do not hand it out"]


def test_no_release_for_the_tag_fails_and_still_says_who_could_see_one(tmp_path):
    github = FakeGitHub()
    github.release_info = None
    report = run(github, tmp_path)
    assert failures(report) == [f"there is no published release for {TAG}: see 'If a release fails' in docs/release.md"]
    assert "private repository" in report.render() and github.downloads == 0


def test_a_tag_that_is_not_a_version_fails_before_asking_github(tmp_path, monkeypatch):
    monkeypatch.setattr(vr.subprocess, "run", lambda *a, **k: pytest.fail("asked GitHub"))
    report = vr.assess(vr.GitHub(REPO), "v0.1.1-beta.1", tmp_path)
    assert failures(report) == ["'v0.1.1-beta.1' is not a release tag: v<major>.<minor>.<patch>"]
    assert report.render().endswith("The release is NOT complete: 1 check failed. See 'docs/release.md'.")


# Its files


def test_missing_extra_and_half_uploaded_files_fail(tmp_path):
    github = FakeGitHub()
    github.release_info["assets"] = [{"name": "SHA256SUMS", "state": "starter"},
                                     {"name": "notes.txt", "state": "uploaded"}]
    report = run(github, tmp_path)
    assert failures(report) == [
        f"its files are wrong: missing {VSIX}; unexpected notes.txt; not fully uploaded: SHA256SUMS"]
    assert github.downloads == 0
    # Without the VSIX there is no SHA-256 to compare with the release job's log
    assert not any("logged" in c.text for c in report.checks)


def test_a_vsix_that_does_not_match_either_hash_fails(tmp_path):
    github = FakeGitHub()
    github.files["SHA256SUMS"] = f"{'a' * 64}  {VSIX}\n".encode()
    github.release_info["assets"][0]["digest"] = "sha256:" + "b" * 64
    assert failures(run(github, tmp_path)) == [
        f"{VSIX} has SHA-256 {github.sha256}, but SHA256SUMS gives {'a' * 64}; GitHub recorded {'b' * 64} on upload"]


def test_a_release_from_before_github_recorded_digests_is_checked_against_sha256sums_alone(tmp_path):
    github = FakeGitHub()
    github.release_info["assets"][0]["digest"] = None
    report = run(github, tmp_path)
    assert failures(report) == []
    assert any(c.text.endswith("as in SHA256SUMS") for c in report.checks)


@pytest.mark.parametrize("vsix, says", [
    (vsix_bytes({"name": "lineagepilot", "version": "0.1.0"}), "0.1.0"),
    (vsix_bytes({"name": "other", "version": "0.1.1"}), "nothing readable"),
    (b"not a zip", "nothing readable"),
])
def test_a_vsix_that_is_not_this_version_fails(tmp_path, vsix, says):
    github = FakeGitHub(vsix)
    assert failures(run(github, tmp_path)) == [f"the VSIX is not LineagePilot 0.1.1: its package.json says {says}"]


# The run that made it


def test_no_run_for_the_tagged_commit_fails(tmp_path):
    github = FakeGitHub()
    github.run_list[0]["head_sha"] = "0" * 40  # the run of a tag that was moved since
    report = run(github, tmp_path)
    assert failures(report) == [f"there is no release run for {TAG} at {COMMIT[:7]}"]


@pytest.mark.parametrize("status, conclusion, expected", [
    ("in_progress", None, f"the release run is still in progress: gh run watch {RUN} --repo {REPO} --exit-status"),
    ("completed", "failure", f"the release run ended in failure: https://github.com/{REPO}/actions/runs/{RUN}"),
])
def test_a_run_that_has_not_succeeded_fails(tmp_path, status, conclusion, expected):
    github = FakeGitHub()
    github.run_list[0].update(status=status, conclusion=conclusion)
    assert failures(run(github, tmp_path)) == [expected]


def test_the_newest_run_for_the_tagged_commit_is_the_one_checked(tmp_path):
    github = FakeGitHub()
    github.run_list.append({**github.run_list[0], "id": 1, "conclusion": "failure"})
    assert failures(run(github, tmp_path)) == []


def test_failed_and_missing_jobs_fail(tmp_path):
    github = FakeGitHub()
    github.job_list = [j for j in github.job_list if not j["name"].startswith("secret-scan")]
    github.job_list[1]["conclusion"] = "failure"
    github.job_list[-1]["conclusion"] = "failure"  # open-vsx was turned on, and failed
    assert failures(run(github, tmp_path)) == [
        ("the release run's jobs did not all pass: no secret-scan job; "
         "core / test (ubuntu-latest, 3.10): failure; open-vsx: failure")]


def test_a_test_job_that_was_skipped_is_not_a_pass():
    jobs = [{"name": n, "status": "completed", "conclusion": "success"} for n in vr.REQUIRED_JOBS]
    assert vr.job_problems(jobs) == []
    jobs[2]["conclusion"] = "skipped"
    jobs.append({"name": "extension / test (macos-latest, 3.13)", "status": "queued", "conclusion": None})
    assert vr.job_problems(jobs) == ["lsp: skipped", "extension / test (macos-latest, 3.13): queued"]
    # A required name is a job, or a prefix before " / ": a longer name is another job
    assert "no core job" in vr.job_problems([{"name": "corex", "status": "completed", "conclusion": "success"}])


def test_a_logged_hash_that_differs_fails_and_a_deleted_log_is_noted(tmp_path):
    github = FakeGitHub()
    github.log = log_line("c" * 64)
    assert failures(run(github, tmp_path)) == [f"the release job logged {'c' * 64} for {VSIX}, not {github.sha256}"]
    github.log = "nothing here\n"
    assert failures(run(github, tmp_path)) == [f"the release job logged no SHA-256 for {VSIX}, not {github.sha256}"]
    github.log = None
    report = run(github, tmp_path)
    assert failures(report) == []
    assert "the release job's log has been deleted, so the SHA-256 it logged cannot be compared" in notes(report)


# The helpers


def test_sha256sums_lines_in_text_and_binary_mode():
    sha = "d" * 64
    assert vr.listed_hash(f"{sha}  {VSIX}\n", VSIX) == sha
    assert vr.listed_hash(f"{sha} *{VSIX}\r\n", VSIX) == sha
    assert vr.listed_hash(f"{sha}  other.vsix\n", VSIX) is None
    assert vr.listed_hash(f"{sha}  {VSIX}\n{'e' * 64}  {VSIX}\n", VSIX) is None  # two answers are none
    assert vr.listed_hash(f"{sha}  {VSIX}\n{sha}  {VSIX}\n", VSIX) == sha
    assert vr.listed_hash("xyz  " + VSIX, VSIX) is None


def test_logged_hashes_ignore_the_script_and_other_names():
    sha = "f" * 64
    log = ('Run cd release && sha256sum "lineagepilot-$VERSION.vsix" > SHA256SUMS\r\n'
           + log_line(sha) + log_line("1" * 64, "lineagepilot-0.1.10.vsix"))
    assert vr.logged_hashes(log, VSIX) == {sha}


# The gh wrapper


def test_gh_errors_say_what_failed_and_404_means_missing(monkeypatch):
    calls = []

    def fake_run(args, **kwargs):
        calls.append(args)
        if args[-1].endswith("/releases/latest"):
            return subprocess.CompletedProcess(args, 1, "", "gh: Not Found (HTTP 404)\n")
        if args[-1].endswith("/logs"):
            return subprocess.CompletedProcess(args, 1, "", "gh: Gone (HTTP 410)\n")
        if args[-1] == f"repos/{REPO}":
            return subprocess.CompletedProcess(args, 0, '{"private": true}', "")
        return subprocess.CompletedProcess(args, 1, "", "HTTP 401: Bad credentials\n")

    monkeypatch.setattr(vr.subprocess, "run", fake_run)
    github = vr.GitHub(REPO)
    assert github.repository() == {"private": True}
    assert github.latest() is None
    assert github.job_log(7) is None
    with pytest.raises(vr.GhError, match="Bad credentials") as error:
        github.release(TAG)
    assert not error.value.missing
    assert github.attestation(Path("x.vsix"), TAG) == f"gh attestation verify x.vsix --repo {REPO} " \
        f"--signer-workflow {REPO}/.github/workflows/lineagepilot-release.yml --source-ref refs/tags/{TAG} " \
        "--deny-self-hosted-runners: HTTP 401: Bad credentials"
    assert calls[0] == ["gh", "api", f"repos/{REPO}"]


def test_gh_wrapper_commands(monkeypatch, tmp_path):
    answers = {
        f"repos/{REPO}/commits/{TAG}": {"sha": COMMIT},
        f"repos/{REPO}/actions/workflows/lineagepilot-release.yml/runs?branch={TAG}&event=push": {"workflow_runs": [1]},
        f"repos/{REPO}/actions/runs/{RUN}/jobs?per_page=100": {"jobs": [2]},
        f"repos/{REPO}/releases/tags/{TAG}": {"tag_name": TAG},
    }
    calls = []

    def fake_run(args, **kwargs):
        calls.append(args)
        if args[1] == "api":
            body = answers.get(args[2])
            return subprocess.CompletedProcess(args, 0, "log text" if body is None else json.dumps(body), "")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(vr.subprocess, "run", fake_run)
    github = vr.GitHub(REPO)
    assert github.commit(TAG) == COMMIT
    assert github.runs(TAG) == [1] and github.jobs(RUN) == [2] and github.release(TAG) == {"tag_name": TAG}
    assert github.job_log(5) == "log text"
    github.download(TAG, tmp_path)
    assert calls[-1] == ["gh", "release", "download", TAG, "--repo", REPO, "--dir", str(tmp_path)]
    assert github.attestation(tmp_path / VSIX, TAG) is None


def test_no_gh_installed_is_an_error_not_a_crash(monkeypatch):
    def missing(args, **kwargs):
        raise FileNotFoundError("gh")

    monkeypatch.setattr(vr.subprocess, "run", missing)
    with pytest.raises(vr.GhError, match="GitHub CLI is not installed"):
        vr.GitHub(REPO).repository()


# The command


def test_main_prints_the_report_and_exits_by_its_result(monkeypatch, capsys):
    made = []

    def fake(repo):
        made.append(repo)
        return github

    monkeypatch.setattr(vr, "GitHub", fake)
    github = FakeGitHub()
    assert vr.main([TAG]) == 0 and made == [REPO]
    assert capsys.readouterr().out.rstrip().endswith("The release is complete.")
    github = FakeGitHub()
    github.release_info["prerelease"] = True
    assert vr.main([TAG, "--repo", "someone/fork"]) == 1 and made[-1] == "someone/fork"
    assert "The release is NOT complete: 1 check failed." in capsys.readouterr().out


def test_main_with_no_tag_checks_the_version_in_package_json(monkeypatch, capsys):
    checked = []

    def fake_assess(github, tag, workdir):
        checked.append(tag)
        return vr.Report(f"LineagePilot {tag}")

    monkeypatch.setattr(vr, "assess", fake_assess)
    version = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))["version"]
    assert vr.main([]) == 0 and checked == [f"v{version}"]
    assert vr.main(["v0.0.9"]) == 0 and checked[-1] == "v0.0.9"


def test_main_exits_2_when_it_cannot_ask_github(monkeypatch, capsys):
    class Broken(FakeGitHub):
        def repository(self):
            raise vr.GhError(("api", f"repos/{REPO}"), "To get started with GitHub CLI, please run:  gh auth login")

    monkeypatch.setattr(vr, "GitHub", lambda repo: Broken())
    assert vr.main([TAG]) == 2
    assert "could not ask GitHub: gh api repos/arrufus/lineagepilot: To get started" in capsys.readouterr().err


# The release workflow no longer marks a release as a pre-release (LP-fzx)


def test_no_release_is_a_pre_release():
    _, facts = release_check.check("v0.1.0", {"version": "0.1.0"}, "## 0.1.0\n\nFirst.\n")
    assert "prerelease" not in facts
    workflow = (ROOT.parent / ".github" / "workflows" / "lineagepilot-release.yml").read_text(encoding="utf-8")
    assert "prerelease" not in workflow
