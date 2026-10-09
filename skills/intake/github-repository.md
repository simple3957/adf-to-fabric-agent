# Intake: GitHub repository handling

GitHub-specific behavior for the Intake Agent. GitHub is the only provider implemented
right now.

## Authentication

- The token is read from the `GITHUB_TOKEN` environment variable (configurable via
  `config/config.yaml`).
- It is never hardcoded, never committed, and never written to project files or logs.
- For the git operation the token is embedded into an HTTPS clone URL using GitHub's
  documented token username:

  ```text
  https://x-access-token:<TOKEN>@github.com/<owner>/<repo>.git
  ```

## Token handling and credential safety

- The authenticated URL exists only in memory for the duration of the git command.
- After cloning, the local remote is reset to the token-free URL:

  ```text
  authenticated remote URL → scrub credentials → token-free local .git/config
  ```

- The stored metadata records only the **clean** (token-free) repository URL.
- Any git output surfaced in an error message is redacted so an embedded token cannot
  leak.

Never:

- write the token into `project.yaml`
- write the token into metadata
- write the token into logs
- write the token into skill files
- commit the token
- leave the token in `.git/config`

## URL handling

Accepts common GitHub URL forms and normalizes them to a canonical
`https://github.com/<owner>/<repo>.git`:

- `https://github.com/<owner>/<repo>`
- `https://github.com/<owner>/<repo>.git`
- `github.com/<owner>/<repo>`

Non-GitHub URLs are rejected before any network call.

## Branch handling

- The requested branch (default `main`) is validated to exist on the remote before
  cloning, using `git ls-remote --heads <url> <branch>`.
- The clone is single-branch (`--branch <branch> --single-branch`).

## Clone / fetch and commit SHA

- The repository is cloned into `projects/<id>/source/`.
- The HEAD commit SHA of the fetched branch is captured with `git rev-parse HEAD` and
  recorded in metadata.

## Reuse of an existing local repository

Before cloning, Intake checks whether `projects/<id>/source/.git` already exists:

- If the existing `origin` remote URL and current branch match the request → **reuse**
  the local copy (no clone, no authentication needed) and refresh the recorded commit
  SHA from the existing checkout.
- If they differ, or a refresh is explicitly requested → replace the local copy with a
  fresh clone.

This keeps authentication to once per source and lets later stages consume the local
copy without re-cloning.
