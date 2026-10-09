# Skill: Intake

Operating instructions for the Intake Agent — the first stage of the ADF → Microsoft
Fabric migration system.

## Purpose

Establish the source of truth for a migration project by obtaining the source repository
and recording where it lives. Intake answers a single question:

> "Where is the source, and can we obtain it?"

## Responsibilities

- Accept project/repository information (project name, repository URL, branch, provider).
- Identify the repository provider (GitHub today).
- Authenticate securely using a token supplied via the environment.
- Validate repository accessibility (URL, provider, authentication, requested branch).
- Fetch / clone the source repository.
- Persist the source repository locally as a canonical copy at `projects/<id>/source/`.
- Capture minimal repository metadata (provider, url, branch, commit SHA, fetched_at,
  local source path).
- Reuse an already-fetched source repository when it matches the request, instead of
  re-cloning.
- Never expose or persist credentials.
- Stop after Intake.

## Boundaries

```text
INTAKE DOES NOT INSPECT THE CONTENTS OF THE SOURCE REPOSITORY.
The repository is treated as an opaque source artifact.
Discovery is responsible for understanding the contents later.
```

Intake must **not**: read files, count or classify files, parse ADF JSON, inspect
pipelines / datasets / linked services / triggers / integration runtimes / data flows,
build an inventory or dependency graph, understand the repository, or perform any
migration analysis. All of that belongs to Discovery or later stages.

## Deterministic, no LLM

Intake is orchestration/infrastructure. Authentication, URL validation, git operations,
branch selection, commit SHA capture, metadata persistence, and error handling are all
deterministic code. No LLM reasoning is used or required at this stage.

## Success criterion (hard stop)

```text
Authenticate → Validate → Clone / Fetch → Persist local source
             → Capture metadata → HARD STOP
```

Once metadata is recorded, stop. Do not continue into Discovery or any later stage.

## Providers

Only GitHub is implemented. GitHub-specific behavior is documented in
[`github-repository.md`](github-repository.md). Azure DevOps, GitLab, and Bitbucket are
future providers and are not implemented.
