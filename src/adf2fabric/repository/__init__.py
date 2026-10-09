"""Repository access layer for Intake.

Deterministic, provider-aware fetching of a source repository:

- ``authentication`` - resolve a token from the environment, redact it from text
- ``providers``      - provider abstraction (GitHub implemented; others future)
- ``git``            - thin subprocess wrappers around the ``git`` CLI
- ``client``         - orchestrates validate -> clone -> capture SHA -> scrub
"""
