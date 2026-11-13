# Security policy

## Reporting a vulnerability

Report a vulnerability privately, through GitHub's private vulnerability reporting: on <https://github.com/arrufus/lineagepilot>, open the **Security** tab and choose **Report a vulnerability**. Only you and the maintainers can read the report and the conversation that follows.

Please do not report it in a public issue, a pull request or a discussion. Those are public as soon as they are posted.

## What to include

- the LineagePilot version, from the Extensions view or **LineagePilot: Show Status**, and your editor, operating system and Python versions;
- what an attacker controls, and what they gain: for example, a file in a cloned repository that makes LineagePilot read outside the workspace, run a program, or put something in the review's Markdown that renders when pasted;
- the smallest workspace or input that shows it, and the steps to reproduce it;
- whether it needs a trusted workspace, or also works in Restricted Mode.

Leave out anything from your own projects that you cannot share.

## Supported versions

Only the latest release gets security fixes. A fix ships as a new release on the VS Code Marketplace, Open VSX and GitHub releases.

## What LineagePilot treats as untrusted

The workspace itself: a cloned repository can hold a crafted manifest, SQL, DAG files, annotation files, rule packs, `.vscode/settings.json` or a `.git` folder. LineagePilot reads these as data. It never imports or runs DAG files, and in Restricted Mode it runs neither dbt nor an interpreter from the workspace. LineagePilot makes no network connections. The [design's security model](docs/design.md#10-security-model) says what each control does, and anything that gets past one is a vulnerability worth reporting.
