# Tiefendiagnose Authority V5 – Worker v0101

## Ausgangsdiagnose

Der Stand v0096 enthielt bereits belastbare Worker-, SEM-, MQTT-, Cross- und
Hardwarepfade, erfüllte den Zielvertrag aus Spezifikation V005 aber noch nicht
durchgängig:

- Contract- und Type-Logik existierte zusätzlich im alten Paket
  `automation_contracts`; damit waren mehrere Wahrheiten möglich.
- `master_global`-Annahmen wirkten bis in Pfade, die im Zielmodus
  `node_local` ausschließlich der Worker entscheiden darf.
- Config-Proposals konnten als Full-/Path-/Selector-Eingaben bis an verteilte
  Grenzen gelangen. Das widersprach dem einzigen kanonischen entity-keyed
  Format `config_scope_operations_v1`.
- Authority-, Mirror-, Restore- und Cross-Owner-Zustände benötigten eine
  gemeinsame monotone Fencing-Semantik.

## Implementierter Zielzustand

Der Worker ist im Standardmodus die finale Authority seines Node-Shards.
Master-Dienste erweitern diesen Zustand nur um Registry, Relay, Mirror und
Audit. Ein lokaler Commit durchläuft immer dieselbe Kette:

1. unveränderlicher autoritativer Snapshot,
2. kanonische entity-keyed Operations,
3. SEM- und Safety-Entscheidung,
4. atomarer Authority-Commit,
5. persistenter Journal- und Mirror-Outbox-Eintrag,
6. Runtime-Aktivierung und erst danach Hardware-Freigabe.

Die persistierte Fencing-Identität ist:

`(authority_epoch, scope_revision, execution_epoch, fingerprint)`

Ein veralteter Wert wird fail-closed abgewiesen. Die eindeutigen Konfliktcodes
sind `stale_revision`, `authority_epoch_stale`, `execution_epoch_stale`,
`principal_denied`, `path_denied` und `unsupported_operation_format`.

## Persistenz und Recovery

- SQLite hält Scope-Heads, Snapshots, Journal, Inbox/Outbox, Mirror-Records,
  Transfer- und Restore-Zustände crash-sicher.
- Ein kontrollierter Authority-Transfer wird erst nach dem exakten Ziel-ACK
  aktiviert.
- Disaster Restore bleibt in Quarantäne, bis Snapshot, Proofs und Preflight
  vollständig verifiziert sind.
- Ein älterer Master-Mirror darf lokale Wahrheit nur über einen expliziten,
  protokollierten Override ersetzen.
- Hardware-Schreibrechte werden mit Authority- und Execution-Epoch gefenced.

## Cross-Scope

Cross-Konfiguration bleibt Cross V8. Owner-Accept bedeutet: Der Registry-Pfad
bestimmt den Owner und die nächste `execution_epoch`; der neue Owner validiert,
klassifiziert und akzeptiert die kanonischen Operations selbst. Ohne
`old_owner_frozen_ack`, persistierte Registry-Entscheidung und passenden
Owner-Beweis erfolgt keine Aktivierung.

## Contract- und Type-Authority

Es existiert nur noch:

- `src/library/_generic_type_library.py` – Type Schema `2.0.0`,
- `src/library/_generic_system_contract_library/` – Contract `2.0`,
- Node Storage V9, Cross Storage V8 und ausschließlich expliziter V5-Import.

Das alte Verzeichnis `automation_contracts` und ein paralleles
`src/libraries/` sind im Release verboten. Worker, Master und Proxy verwenden
byte-identische Authority-Dateien.

## Verifikation

- 53 von 53 ausführbaren Worker-Testdateien bestanden.
- Authority-Transfer, Restore, Mirror, Cross-Owner, SEM, File-WAL, MQTT,
  Hardware-Fencing, Shutdown und Revision-Churn wurden regressiv geprüft.
- Der Linux-Clean-Room-Lauf bestand mit CPython 3.14.6t, POSIX-Rechten,
  OpenSSL/mTLS, MQTT und dem realen Modbus-Labpfad.
- Der Worker-Head `node:fn-01` konvergierte nach Auto-Admission und Neustart
  mit identischem Owner, Epoch-Tupel, Contract/Type und Fingerprint in den
  Master-Mirror.
- Die Live-Diagnose fand ein fehlendes Top-Level-`execution_epoch` im
  kanonischen Mirror-Record. Der byte-identische Contract-Fix und die neue
  SQLite-Outbox-Regression schließen diesen Fehler dauerhaft.
## Späte Artefakt-E2E-Diagnose

Der erste vollständige Artefakt-Rollout zeigte zusätzlich, dass ein
Worker-Mirror zwar mit dem normativen Fingerprint versehen war, sein
eingebetteter Config-Snapshot aber noch die gruppierte Runtime-Darstellung
enthalten konnte. Das widersprach dem geforderten unveränderlichen,
kanonischen Transport-Snapshot.

`make_scope_snapshot()` delegiert die Projektion nun immer an
`_generic_type_library.canonicalize_config_for_fingerprint()`. Damit werden
Node V9 und Cross V8 vor Journal, Outbox und Mirror-Transport exakt in ihre
versionierte Storage-Darstellung überführt. Der Fingerprint bleibt über
Runtime- und Storage-Darstellung identisch. Die Regression startet bewusst
mit gruppierten Runtime-Daten und beweist flache V9-Speicherung, unveränderten
Fingerprint sowie den SQLite-Outbox-Roundtrip.

## Proposal-Safety und Journal-Recovery

Der reale WSS-/Relay-Lauf erreichte den Worker dauerhaft, wurde dort aber mit
`path_denied` für die Section `processes` abgeschlossen. Ursache war eine zu
enge Default-Allowlist, obwohl Process-Selector und kanonischer Proposal-
Vertrag diese Node-lokale Änderung bereits vorsehen. Die Basisrichtlinie führt
`processes` nun explizit; sie erweitert weder Principal noch Operationstypen.
Die Regression akzeptiert ausschließlich den sicheren Prozesspfad und weist
einen Credential-Pfad weiterhin ab.

Eine zweite Diagnose bestätigte das vorgesehene Fencing nach Journalverlust:
War der persistierte Master-Mirror bereits bei Revision 6 und wurde das lokale
Worker-Journal auf Revision 0 zurückgesetzt, blieb der Worker `DEGRADED` und
der Master antwortete `stale_revision`. Gleicher Fingerprint legitimiert keine
Revisionserhöhung. Zulässig sind nur:

1. Journal-Erhalt bei Restart/Reuse,
2. der explizite, auditierte Restore-/Authority-Transition-Pfad oder
3. ein gemeinsamer Fresh-Rollout von Worker-Baseline und Registry.

Damit wird eine lokale Datenbankmanipulation ausdrücklich ausgeschlossen und
die Worker-Authority auch im Recovery-Fall nicht implizit überschrieben.
