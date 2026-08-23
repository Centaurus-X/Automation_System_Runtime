# Release Notes – automation_system_v0101

`automation_system_v0101` ist der Worker-Zielstand für Authority V5 und bleibt
ein eigenständiges Projekt mit eigenständigem Release-Archiv.

## Architektur

- `node_local` ist die einheitliche Authority-Semantik für Standalone und
  Distributed; der Worker entscheidet final über seinen Node-Shard.
- Contract Authority `2.0` und Type Schema `2.0.0` liegen ausschließlich unter
  `src/library/`; das frühere Paket `automation_contracts` ist entfernt.
- Das einzige Proposal-Format ist `config_scope_operations_v1` mit
  `replace_field`, `upsert_entity` und `delete_entity`.
- SQLite persistiert Authority-Heads, Journal, Mirror-Outbox, Registry-View,
  Transfer und quarantänisierten Restore.
- Authority-, Scope- und Execution-Epoch bilden das monotone Hardware- und
  Cross-Owner-Fencing.
- V5 bleibt ein expliziter Importpfad; aktive Speicherung ist Node V9 oder
  Cross V8.

## Sicherheit und Kompatibilität

Relay, Journal, Mirror und SEM verarbeiten ausschließlich kanonische
entity-keyed Operations. Nicht eindeutige Legacy-Eingaben werden fail-closed
abgewiesen. Produktive Abnahme erfolgt auf Linux mit Python 3.14t; die lokale
Regression verwendet zusätzlich Python 3.12.

## Qualifizierte Korrekturen

- Mirror-Records führen `execution_epoch` auch für den gültigen Wert `0`
  explizit auf oberster Ebene; SQLite-Outbox und Master-Mirror verlieren das
  Fencing-Feld nicht mehr.
- Die neue Regression deckt Record-Erzeugung, Persistenz und erneutes Laden
  des Mirror-Outbox-Eintrags gemeinsam ab.
- Der Live-Rollout bestätigt Auto-Admission, dauerhafte Heartbeats, exakte
  Mirror-Konvergenz sowie reale Modbus-Sensor- und Aktorpfade.
- Installationslogik bewahrt eine abweichende nutzerverwaltete Config und
  ersetzt ausschließlich die verifizierte Paketbaseline.
## Nachtrag: kanonische Scope-Snapshots

Scope-Snapshots werden nicht mehr in einer zufällig vom Heap gelieferten
Runtime-Darstellung persistiert. Die gemeinsame Persistence Contract Authority
projiziert jeden Node-V9- und Cross-V8-Snapshot vor Journal, Mirror-Outbox und
Transport über die absolute Type Authority in die kanonische Storage-Form.
Eine Regression beweist die Darstellungsinvarianz des Fingerprints.

## Abschlusskorrekturen des realen Proposal-Pfads

- Die externe SEM-/Safety-Basisrichtlinie erlaubt nun gezielt die kanonische
  Section `processes`. Damit kann der Master-Relay einen sicheren
  `replace_field`-Vorschlag für `process_location` an die finale Worker-
  Authority weiterleiten. Nicht freigegebene Sections wie `credentials`
  bleiben unverändert fail-closed mit `path_denied`.
- `test_worker_simulation_profile.py` prüft beide Seiten derselben Grenze:
  den erlaubten Prozess-Vorschlag und den verweigerten Credential-Vorschlag.
- Der Data-Center-Integrationstest verwendet ein eigenes, automatisch
  bereinigtes Temp-Verzeichnis. Eine wiederverwendete Betriebssystem-PID kann
  deshalb nicht mehr mit einem alten gültigen Test-Snapshot kollidieren.
- Ein Replica-Journal-Reset ist ausschließlich Teil eines gemeinsamen Fresh-
  Rollouts. Bei einer laufenden `node_local`-Registry muss das Journal erhalten
  bleiben oder über den expliziten Restore-Pfad wiederhergestellt werden; eine
  ältere Announce wird korrekt mit `stale_revision` abgewiesen.

## Abschluss: Worker-lokaler File-WAL-Commit

- Im Standardmodus `node_local` werden Runtime-, Policy-Fallback- und
  strukturelle Dateiänderungen als kanonische Scope-Operations durch SEM
  entschieden und unmittelbar im Worker committed.
- Der lokale Commit wartet weder auf einen Master-Commit noch auf einen
  Mirror-ACK. Journal und Mirror-Outbox sichern die nachgelagerte Konvergenz.
- Der File-WAL-Beleg bindet die exakte Baseline, den semantischen Diff, die
  monotone Zielrevision, den Authority-Journal-Eintrag und die Mirror-ID.
- Recovery führt einen Commit niemals doppelt aus und akzeptiert keine
  Rückmaterialisierung eines Master-Mirrors als lokale Wahrheit.

## Abschlusskorrektur: portable Service-Skripte

- `_tools/worker_service_manager.sh` wird ausschließlich mit LF-Zeilenenden
  ausgeliefert und ist damit auf dem Linux-Zielsystem direkt ausführbar.
- Der Release-Builder verwirft jedes ausgelieferte Shellskript mit einem
  Carriage-Return fail-closed, bevor Manifest oder Archiv entstehen.
- `test_release_archive_builder.py` beweist diese Packaging-Grenze mit einem
  absichtlich erzeugten CRLF-Shellskript.
