# Release-Validierung v0083

## Freigabeergebnis

Der Teststand ist vollständig grün. Die v0083 trennt die unverändert
autonome Node-Regelung von der optionalen Cross-Runtime und sperrt jede
Automation-Ausführung in der Coordinator-Rolle. Ein Cross-, MQTT- oder
Master-Ausfall kann die lokale Regelung nicht stoppen.

## Reproduzierbarer Hauptlauf

```bash
cd automation_system_v0083
./test_all.sh
```

Ergebnis vom 21. Juli 2026:

```text
PASS complete Worker validation: 39 executable tests
```

Der Lauf umfasst Shell-Syntax, `compileall` für Runtime, Tests und Tools, die
Preflights `unit_test` und `standalone` sowie sämtliche ausführbaren Tests
unter `test/test_*.py`.

## Abgedeckte Invarianten

| Bereich | Nachweis |
|---|---|
| Lokale Authority | Lokale RuleEngine, FSM, Automation und Aktorpfad bleiben im Distributed-Worker aktiv |
| Master-Grenze | Coordinator verarbeitet keine Sensorfakten in Automation und erzeugt keine Aktorbefehle |
| Config-Sharding | Node- und Cross-Shards sind disjunkt, owner-spezifisch und getrennt versioniert |
| UUID-Grenze | Cross-Config/-Tasks enthalten keine Modbus-Adresse, Funktion, Unit oder Hostdaten |
| Hardware-Ownership | Ausschließlich der Ziel-Worker löst UUID und Aktor-ID auf lokale Busdaten auf |
| Cross-End-to-End | Remote-Fakt löst Cross-Rule aus; Remote-Aktion läuft fenced, dedupliziert und mit Readback |
| Isolation | Cross-Start-/Snapshot-/Jobfehler degradieren nur Cross; lokale Threads bleiben aktiv |
| Persistenz | Cross-Snapshot ist atomar und Sensorwert-Persistenz ist nicht an MQTT gekoppelt |
| Replay-Sicherheit | stabile Ereignis-/Task-Identität, Sequenzprüfung und Operationsjournal verhindern Doppelwrites |
| Restart-Sequenz | crashsicher reservierter High-Water-Wert plus Master-Floor verhindern stille Telemetrie nach Neustart |
| Datei-Hot-Apply | stabile JSON-Erkennung, Proposal-WAL, local-first Runtime-Werte, Master-first Strukturänderungen und fingerprint-basierte Herkunftserkennung interner Writes |
| File-WAL-Recovery | ein Master-konvergierter Proposal wird trotz später veränderter Runtime-Werte beendet; ein nach Replica-Reset verwaister PENDING-Eintrag wird identitätsgleich über den Master neu eingereicht |
| Persistenzidentität | Master-Reihenfolge bleibt beim Pretty-Print erhalten; jeder Save wird vor dem Schreiben gegen den Live-Fingerprint geprüft und Identitätsdrift fail-closed abgewiesen |
| Python-Cache-Hygiene | Launcher setzt die Umgebung vor dem Interpreterstart, YAML und Preflight erzwingen deaktivierte Bytecode-Writes sowie einen Präfix unter `/tmp`; ein Launcher-Preflight erzeugt keine Projekt-Caches |
| Atomarer Apply | ein Rolloutfehler stellt Config-Heap, Regeln, FSM und Controller auf die vorherige Revision zurück |
| Free-threaded Vorbereitung | explizite Locks und Single-Writer-Queues schützen mutablen Zustand; kein Rückgriff auf den GIL |

## Zusätzliche Stressläufe

- GenericThreadPool: 100 Testwiederholungen mit jeweils 1000 Shutdown-Zyklen,
  insgesamt 100000 Start-/Shutdown-Zyklen, 100/100 erfolgreich.
- StateShardManager: 100/100 Wiederholungen erfolgreich.
- Cross-I/O, Cross-Worker und Cross-Recovery: 200/200 Wiederholungen
  erfolgreich.
- GenericDataCenter: 41/41 interne Prüfungen erfolgreich.

## Konfigurationsprüfung

Folgende Dateien bestehen die kanonische GenericType-Prüfung ohne Fehler oder
Warnungen:

- `_config/config_data/_legacy/device_state_data_v5_multi_worker_norm.json`
  (Legacy-Kompatibilität)
- `_config/config_data/_legacy/device_state_data_v7_node_fn-01.json`
- `_config/config_data/_legacy/device_state_data_v7_node_fn-02.json`
- `_config/config_data/_legacy/device_state_data_v7_cross_fn-01.json`

Der Cross-Shard enthält bewusst ausschließlich die graph-sicher
partitionierbaren Rules 3 und 6 sowie States 14 und 15. Mixed-Writer- und
Safety-Cross-Regeln werden fail-closed nicht ausgerollt.

## Zielsystem-Abnahme

Die vollständige Suite wird zusätzlich auf dem Zielsystem mit Python
3.14t/free-threaded ausgeführt. Der Control-Plane-E2E prüft Mosquitto/mTLS,
Master-Commit, Worker-Datei-Proposal, WSS-Events, Neustart-Sequenz und realen
Modbus-Readback. Die Locks ersetzen den deaktivierten GIL nicht, sondern
schützen den gemeinsam veränderlichen Zustand explizit.
