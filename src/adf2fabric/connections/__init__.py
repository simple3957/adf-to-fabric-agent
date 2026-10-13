"""Connections: how the accelerator signs in to sources and destinations, discovers resources, configures
per-linked-service credentials, analyses integration runtimes and TESTS what it claims.

Layers (no web framework here; ``adf2fabric.api`` wraps these):

* ``providers/`` - one adapter per way in: Azure CLI, Fabric CLI, Git repository, uploaded ZIP.
* ``catalog``    - what each connector supports (auth methods, fields, validation, Fabric compatibility).
* ``resolution`` - linked service -> Fabric connection plan, including integration runtime analysis.
* ``service``    - orchestration: apply choices, test them, write the connection migration report.

Account authentication (CLI sessions) is kept separate from per-connector credentials. Secrets live only in the
in-memory ``SecretVault`` and are never logged, returned, or written to disk.
"""
