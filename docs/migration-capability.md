# Migration capability (authoritative scope)

This document is the single source of truth for **what the migrator can do today**.
Other docs and skills reference it; they must not contradict it.

## Current capability

The current/initial capability is **Azure Data Factory → Microsoft Fabric,
like-for-like (parity) migration**, for an engagement whose source estate is:

```text
ETL / source integration : Azure Data Factory
Source warehouse         : SQL Server
Storage / sink           : Azure Data Lake Storage Gen2
Target platform          : Microsoft Fabric
Migration mode           : Like-for-like
```

Like-for-like means:

- Preserve source functionality/behavior and artifact intent.
- **Preserve naming** unless a supported migration rule requires otherwise.
- Produce **executable / deployable** Fabric artifacts.
- The destination contains the *equivalent* migrated representation of the source.

```text
ADF source object → equivalent Fabric representation → executable/deployable target
```

The exact Fabric representation may differ where the target platform requires it,
but the intent is **parity, not redesign**.

## Future capabilities (NOT supported today)

The following are future scope. They must **not** be implemented now and must
**not** be treated as currently supported migration modes:

- Major architectural redesign
- Functional transformation / business-logic changes
- Advanced modernization
- Optimization-driven redesign
- Complex source-to-target restructuring
- Automatic naming-transformation strategies
- Alternative target architectures
- Any user-requested redesign beyond like-for-like

The architecture leaves room for these later (see the connector registry and the
`MigrationMode` enum), but a request for any of them is **captured and explicitly
flagged as unsupported** — never silently reinterpreted as like-for-like, and
never executed. Later Assessment/Planning phases decide how such requests are
handled.

## Artifact-level mapping is a later concern

Consultation records *what the user wants*. It does not map artifact names to
targets. Discovery establishes *what exists*; Engineering/Generation performs the
deterministic source→target mapping. If like-for-like yields `Aple → Aple`, that
is a consequence of the migration rules on the discovered artifact — not a name
invented by Consultation.
