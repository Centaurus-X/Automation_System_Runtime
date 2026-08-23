# Changelog

All notable public releases are documented here. Historical engineering detail
remains in the version-specific release-note files.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow semantic versioning where applicable.

## [Unreleased]

### Added

- Two default-collapsed Mermaid architecture views for the internal component
  and event flow and the external Master, MQTT/I/O-broker and industrial
  gateway boundaries, including standalone `.mmd` sources and a source-anchored
  component map.

### Planned

- Profile-aware locked optional dependencies and SBOM generation.
- Reproducible target-system latency and capacity baselines.
- Additional Linux/free-threaded CI qualification.

## [0.101.0] - 2026-07-27

### Added

- Node-local Authority V5 with explicit transfer and quarantined restore.
- Canonical Node V9 and Cross V8 storage contracts.
- Durable SQLite authority heads, journals, mirror outbox and registry view.
- Worker-local file-WAL commits through SEM and safety policy.
- Cross-owner execution fencing and owner-side actuator readback.
- mTLS MQTT identity validation, transport hardening and command deduplication.
- Release archive hardening, LF-only shell validation and SPDX 2.3 SBOM.

### Fixed

- Preserved valid `execution_epoch=0` in mirror records and SQLite round trips.
- Canonicalized grouped runtime snapshots before journal, mirror and transport.
- Routed safe `processes/process_location` proposals while continuing to reject
  credential-path proposals.
- Prevented stale replica-journal state from silently becoming node truth.

### Evidence

- 54/54 executable test files recorded as passing in the Linux release run.
- Clean-room CPython 3.14.6t, mTLS/MQTT and Modbus-simulator validation.
- See `RELEASE_NOTES_V0101_DE.md` and `RELEASE_VALIDATION_V0101.md`.

[Unreleased]: https://github.com/Centaurus-X/Automation_System_Runtime/compare/v0.101.0...HEAD
[0.101.0]: https://github.com/Centaurus-X/Automation_System_Runtime/releases/tag/v0.101.0
