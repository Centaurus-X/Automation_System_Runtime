# Release Notes v0083

## Ergebnis

v0083 behält die in v0082 eingeführte Trennung von Node-lokaler Regelung und
Cross-Verarbeitung bei. Der Master
führt keine Automation mehr aus. Jeder Worker bleibt im Distributed-Modus die
lokale, untergeordnete Authority und regelt mit seinem letzten konsistenten
Node-Shard weiter, wenn Master oder MQTT ausfallen.

Zusätzlich beseitigt v0083 die beim Neustart und bei lokalen Dateiänderungen
gefundenen Inkonsistenzen, ohne den bestehenden Master-Commit-Pfad zu ersetzen:

- Sensorsequenzen verwenden einen crashsicheren High-Water-Allocator und
  übernehmen beim Registrierungs-ACK einen höheren Master-Floor.
- Jeder Start besitzt weiterhin eine neue Runtime-Kennung; Registration und
  Heartbeat melden jetzt den tatsächlichen Live-Fingerprint der aktiven Config.
- Gültige Änderungen an der Worker-JSON-Datei werden stabil gelesen,
  semantisch differenziert und dauerhaft journalisiert.
- Interne SEM-Persistierungen schreiben einen crashsicheren
  Fingerprint-Beleg. Dadurch erzeugen zeitlich überholte Runtime-Snapshots
  keine falschen File-Proposals und echte Operator-Edits werden weiterhin
  ohne zeitbasiertes Unterdrückungsfenster erkannt.
- Die Worker-Persistenz erhält die vom Master autoritative Listenreihenfolge.
  Ein zusätzlicher Identity-Guard vergleicht Live- und Render-Fingerprint vor
  jedem Dateizugriff und weist formatterbedingte Identitätsänderungen
  fail-closed ab. Damit bleibt derselbe Shard auch über einen Neustart
  fingerprint-identisch.
- `run_worker.sh` deaktiviert Bytecode-Schreibzugriffe bereits vor dem ersten
  Python-Import und setzt zusätzlich einen Cache-Präfix unter `/tmp`. Der
  Preflight validiert diese Interpreterwerte gegen die verbindliche
  `runtime.python_cache`-Policy der YAML; Projekt-`__pycache__` sind damit auf
  allen unterstützten Startpfaden ausgeschlossen.
- Ein bereits vom Master konvergierter File-Proposal schließt sein lokales WAL
  auch dann korrekt ab, wenn unabhängige Runtime-Werte danach weitergelaufen
  sind; materialisiert wird stets der tatsächliche aktuelle Live-Snapshot.
- Bleibt ein `PENDING`-File-WAL über einen Fresh-Rollout erhalten, während das
  getrennte Replica-/Outbox-Journal bewusst zurückgesetzt wird, rekonstruiert
  der Worker den Proposal mit derselben dauerhaften Operation-ID und lässt ihn
  erneut durch die Master-Autorität entscheiden.
- `value_id`- und Parameteränderungen laufen local-first durch den vorhandenen
  SEM-Single-Writer und werden anschließend dem Master vorgeschlagen.
- Topologie-, Ownership-, Controller-, Sensor-, Aktor-, Rule- und
  Process-State-Strukturänderungen bleiben Master-first und werden erst nach
  dessen validiertem Commit aktiviert.
- Ein fehlgeschlagener Runtime-Rollout wird durch denselben SEM-Pfad auf die
  zuvor aktive Gesamtconfig kompensiert; Regeln, FSM und Controller bleiben
  dadurch revisionskonsistent.
- Die selbstverriegelnde Bedingung der lokalen Heizregel wurde entfernt. Der
  Automatikmodus bleibt von der eigentlichen Zustandsaktivierung getrennt.

## Durchgeführte Änderungen

### Deterministisches Config-Sharding

- `build_local_execution_shard()` projiziert nur Hardware und
  Single-Controller-Logik des Ziel-Nodes.
- Mirror-/Standby-Routen starten keine Hardware-Threads.
- Reine Hardware-Nodes ohne Process-State erhalten einen gültigen,
  selbstständigen `local_node`-Datensatz.
- Node- und Cross-Shards werden separat gerollt, bestätigt und über eigene
  Fingerprints verglichen.
- Hardwarekoordinaten sind im Cross-Shard verboten; dort existieren nur
  logische UUID-/I/O-Referenzen.

### Isolierte Cross-Runtime

- Neue Module:
  - `src/library/_cross_scope_library.py`
  - `src/core/_cross_worker_thread.py`
  - `src/adapter/_cross_io_adapter.py`
  - `src/library/_cross_snapshot_store.py`
- Eigener Thread, eigene Queue, eigene RuleEngine-, FSM- und Automation-Zustände.
- Rechenjobs nutzen den vorhandenen generischen ThreadPool; Ergebnisse werden
  nachrichtenbasiert an den Cross-Owner zurückgeleitet.
- Lokale Ziele gehen an den normalen Controller-Thread; Remote-Ziele nur an den
  journalisierten Coordinator-Pfad.
- UUID-Intents enthalten keine Adresse/Funktion. Erst der Hardware-Owner löst
  diese Felder aus seinem lokalen Shard auf und überschreibt fremde Angaben.
- Fakten werden nach Owner, Quality, Timestamp, Sequenz und Alter validiert.
- Atomarer Cross-Snapshot stellt Config, Fakten, Engine-/FSM-Zustand und
  Automation-State nach Restart wieder her.
- Stabile Task-IDs und Fact-Sequenzen verhindern erneute Ausführung desselben
  MQTT-Replays; konkurrierende Werte für dasselbe Aktorziel werden fail-closed
  verworfen.

### Master als reiner Orchestrator

- Die lokale Automation-Pipeline ist für `role=coordinator` vollständig gesperrt.
- Automatisch vom Master erzeugte Aktorbefehle werden am Worker abgewiesen.
- Worker-Cross-Aktionen dürfen kontrolliert über den Master geroutet werden.
- Remote Sensor-Fakten dienen am Master nur Monitoring und werden dort nie in
  RuleEngine/Automation eingespeist.
- Cross-Shards werden nur an Worker verteilt, die eine aktive Cross-Capability
  registriert haben.

### Safety, no-GIL und Release-Hygiene

- Mutable Cross-Daten und Statistiken sind durch `RLock` geschützt.
- Start/Stop, Queue-Produzenten, FSM-Reihenfolge und Snapshot-Commit sind
  free-threaded-sicher serialisiert.
- Cross-Fehler degradieren ausschließlich die Cross-Komponente; lokale Threads
  laufen weiter.
- Private Schlüssel, Zertifikate, alte Quellduplikate, Logs und Python-Caches
  wurden aus dem Release entfernt.
- Der Zertifikatsgenerator besitzt kein festes PKCS#12-Default-Passwort mehr;
  ohne injiziertes Secret erzeugt er kryptographisch zufällige Zugangsdaten.
- Das getrennte Profil `worker_fn02` bindet nur den fn-02-Node-Shard, dessen
  mTLS-Identität, Controller-Allowlist und drei lokale Safety-Limits.

## Ergebnis der V7-Graphanalyse

Die gelieferte unsplit fn-01-Konfiguration enthält:

- lokal ausführbar: Rules `1, 2, 4, 10, 11`
- Cross-ausführbar: Rules `3, 6`
- migrationspflichtige Mixed-Writer-Rules: `5, 7, 8, 9`
- unzulässige Cross-Safety-Rules: `13, 15, 17`

Deshalb enthält der ausgelieferte Cross-Shard ausschließlich Rules `3` und `6`
sowie deren exklusiv benötigte States `14` und `15`. Unklare oder
sicherheitskritische Regeln werden nicht still ausgeführt. Für neue
`master_global`-Konfigurationen blockiert die Partition-Validierung einen
unsicheren Rollout.

## Bewusst nicht automatisch aktiviert

`cross_runtime.enabled` bleibt im Simulatorprofil `false`. Die vorhandenen
Cross-Regeln adressieren mehrere reale fn-01-Controller, während das
Simulatorprofil absichtlich nur einen Controller öffnet. Aktivierung erfolgt
erst nach Kommissionierung von Controller-Allowlist, MQTT-Routing und
Safety-Limits.
