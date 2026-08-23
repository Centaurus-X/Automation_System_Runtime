# Architektur der Cross-Runtime v0083

## Verantwortlichkeiten

| Ebene | Besitzt | Führt Automation aus | Darf Hardware schreiben |
|---|---|---:|---:|
| Master | globale Config, Registry, Audit, Shard-Verteilung | nein | nein |
| lokaler Worker | eigener Node-Shard und lokale Runtime | ja | nur eigene Controller |
| Cross-Worker | genau einen Cross-Shard | ja, isoliert | nie direkt |
| Controller-Thread | einen lokalen Hardware-Controller | nein | ja, nach Safety/Ownership |

## Lokale Pipeline

```text
Sensor -> EventRouter -> RuleEngine -> lokaler Single-Writer/FSM
       -> Automation -> Safety -> Controller-Thread -> Feldbus -> Readback
```

Dieser Pfad ist in Standalone und Distributed identisch. MQTT ist kein
Synchronisationspunkt der Feldregelung und blockiert die lokale Pipeline nie.

## Cross-Pipeline

```text
lokaler Fakt --------------------+
                                 v
Remote-Fakt -> MQTT -> Fakten-Cache -> Cross-Thread -> ThreadPool
                                      RuleEngine/FSM/Automation
                                                |
                          +---------------------+--------------------+
                          v                                          v
                 lokales UUID-Ziel                         Remote-UUID-Ziel
                 normaler Controller-Thread               durable actuator.route
                 Owner löst Busdaten auf                  Owner-Worker schreibt
```

Der Cross-Thread besitzt keinen Connection-Adapter. Cross-Config und Tasks
enthalten deshalb keine Modbus-Adresse, Funktion, Unit oder Hostdaten. Der
Ziel-Worker löst `controller_uuid + actuator_id` ausschließlich aus seinem
eigenen Node-Shard auf.

## Config-Verteilung

1. Der Master validiert die globale Config und bestimmt den Graph-Scope.
2. Für jeden Node entsteht ein `node_local`-Shard mit nur lokaler Hardware und
   Single-Controller-Logik.
3. Cross-Regeln werden in einen separaten `cross`-Shard mit genau einem
   `owner_node_id` projiziert.
4. Beide Shards besitzen getrennte Fingerprints und Rollout-Nachrichten:
   `distributed.config.apply` und `distributed.cross.apply`.
5. Worker bestätigen Revision und Fingerprint unabhängig. Ein Heartbeat löst
   nur bei echter Abweichung einen erneuten Rollout aus.

## Fakten und Zeitverhalten

- Ein Sensor wird ausschließlich von seinem Hardware-Owner gepollt.
- Lokale Fakten gelangen direkt vom EventRouter zum Cross-Thread.
- Remote-Fakten werden via MQTT gespiegelt und gegen die registrierte
  Controller-Ownership geprüft.
- `max_signal_age_s` entfernt veraltete Fakten aus der RuleEngine-Sicht.
- Quality, Timestamp, Future-Skew und Sequenz werden fail-closed geprüft.
- Cross ist ausschließlich für Koordination im Sekundenbereich vorgesehen.
  Safety- und zeitkritische Loops bleiben lokal oder hartverdrahtet.

## Zustands- und Replay-Sicherheit

Der Cross-Thread ist der einzige Writer seines mutablen Cross-Zustands.
ThreadPool-Jobs arbeiten nur auf Kopien und liefern Resultate per Queue zurück.
Nach erfolgreichem Commit wird ein eigener Snapshot atomar per `fsync` und
`os.replace` gespeichert. Er enthält Authority-Config, Runtime-State,
RuleEngine-/FSM-Zustand, Automation-Slots und Fact-Cache. Eine Prüfsumme erkennt
unvollständige oder veränderte Records.

Task-UUIDs werden aus Cross-Domain, Revision, stabiler Sensor-Event-Identität
und Aktorziel erzeugt. Empfangszeitpunkte beeinflussen die UUID nicht. Remote
Writes werden zusätzlich im SQLite-Operationsjournal dedupliziert und erst nach
Hardware-Readback als `CONFIRMED` gemeldet.

## Fehlerisolation

- Cross-Configfehler, Snapshotkorruption oder ein gestoppter Cross-Thread setzen
  die Cross-Komponente auf `degraded`.
- ThreadManagement, lokale Controller und Sensor-Polling bleiben aktiv.
- Broker-/Masterausfall stoppt nur neue Remote-Fakten und Cross-Remote-Aktionen.
- Stale Fakten altern aus; lokale Regeln laufen mit dem letzten gültigen
  Node-Shard weiter.
