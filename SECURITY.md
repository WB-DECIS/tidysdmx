# Security policy

## Reporting a vulnerability

**Do not open a public issue for a security vulnerability.**

Report it privately through GitHub's
[private vulnerability reporting](https://github.com/WB-DECIS/tidysdmx/security/advisories/new)
— Security → Advisories → *Report a vulnerability*. If that is not available to
you, email tonyfujs@gmail.com instead.

Please include:

- what the vulnerability allows an attacker to do,
- the affected version(s),
- a minimal reproduction,
- any known mitigation or workaround.

You can expect an acknowledgement within a few working days. We will keep you
updated as we investigate, agree a disclosure timeline with you, and credit you
in the advisory unless you would rather stay anonymous.

## Supported versions

Only the latest released version receives security fixes. There are no
long-term-support branches.

## What this project does to protect the supply chain

- **Tokenless publishing.** Releases go to PyPI via Trusted Publishing (OIDC), so
  no long-lived API token exists to leak. Each distribution carries a PEP 740
  Sigstore attestation binding it to the workflow run that built it.
- **Pinned CI.** Every GitHub Action is pinned to a full commit SHA, not a
  mutable tag, and Dependabot keeps those pins current.
- **Least privilege.** Workflows default to `permissions: contents: read` and
  escalate only per job. Checkouts do not persist credentials, except the release
  checkout that must push the version commit.
- **Automated auditing.** `pip-audit` runs against the resolved lockfile on every
  pull request and weekly on a schedule, so a CVE published against an unchanged
  dependency still surfaces.
- **Workflow linting.** [zizmor](https://docs.zizmor.sh/) checks the workflows
  for template injection, credential persistence and excessive permissions.
- **Static analysis.** ruff's `S` (flake8-bandit) rules run on every commit.
- **Reproducible installs.** `uv.lock` is committed and CI installs with
  `uv sync --locked`, so a build never silently resolves a different dependency.

## Suppressing an advisory

`make audit` audits **every** dependency group — the runtime dependencies users
install, and the dev, docs, release, security and notebooks tooling — because a
compromised build tool is a supply-chain problem too. The cost is that an
advisory against a tool you cannot upgrade turns the audit red: a common case is
a package in one of those groups that caps the vulnerable dependency below the
patched version, which no bump in this repository can resolve.

For that case, and only that case, list the advisory ID in `PIP_AUDIT_IGNORE`
in the `Makefile`:

```make
# PYSEC-2026-0001: click < 8.2 is capped by python-semantic-release; re-check at
# each PSR release. Added 2026-09-01.
PIP_AUDIT_IGNORE ?= PYSEC-2026-0001
```

Each ID becomes a `--ignore-vuln` flag, locally and in `security.yml` alike.
Write down why and when next to every entry, and remove it as soon as the
upstream cap moves — a suppression that outlives its reason is a silent hole.
To try a suppression without committing it:
`make audit PIP_AUDIT_IGNORE="PYSEC-2026-0001 GHSA-xxxx-xxxx-xxxx"`.

## Releasing a dependency fix

Routine Dependabot bumps land as `build:`/`chore:` commits and deliberately cut
no release — the lockfile only affects development and CI, not what users
install. When a vulnerability forces a change users must receive (raising a
version floor in `dependencies`), title that change `fix(deps): ...` (for a
squash-merge, the PR title) so a patch release ships immediately.

## Scope

Vulnerabilities in this package's own code are in scope. Vulnerabilities in
dependencies should be reported upstream — though please do tell us if we are
shipping an affected version, so we can bump it.
