"""Warehouse migration framework: source warehouse -> existing Microsoft Fabric Warehouse (metadata-driven, read-only by default).

Flow: config -> source adapter (discover) -> neutral catalog -> ADF dependency map -> manifest + DDL (dry run)
-> target inventory -> validation -> report. Source platforms plug in through ``adapter.SourceAdapter``.
"""
