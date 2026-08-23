# Distributed Runtime v0092

## Authority-Modell

Der Worker ist die lokale Authority und der einzige Feldbus-Writer seiner
Controller. Der Master ist globale Config-Authority, Registry, Audit-Instanz
und Nachrichtenrelay. Er führt weder RuleEngine/FSM/Automation noch Hardware-I/O
aus.

Im Distributed-Modus erzwingt der Worker:

```yaml
distributed_runtime:
  role: worker
  local_automation_enabled: true
  config_sync_mode: local_first
  conflict_policy: master_wins
```

Der Ausfall von Master oder Broker beeinträchtigt nur Synchronisation und
Cross-Remote-Flows. Lokale Steuerung nutzt weiter den letzten konsistenten
Node-Shard.

## Getrennte Shards

- `distributed.config.apply`: Node-lokale Hardware und Single-Controller-Logik
- `distributed.cross.apply`: isolierte Cross-Logik eines Owner-Workers

Authority-Fingerprint, Node-Shard-Fingerprint und Cross-Fingerprint sind
verschiedene Werte. Registrierung und Heartbeat übertragen alle drei, damit
nur echte Abweichungen einen Rollout auslösen.

Node-Scope-Revision und Node-Shard-Fingerprint werden dabei immer als das
zuletzt gemeinsam bestätigte, persistierte Replica-Paar gemeldet. Bei einem
lokal vorab angewendeten `local_first`-Commit zeigt ausschließlich
`live_config_fingerprint` den abweichenden Laufzeitstand; das Replica-Paar
wechselt erst nach erfolgreichem Master-Apply gemeinsam auf das Ziel.

Ein Worker ohne aktive `cross_runtime`-Capability erhält keinen Cross-Shard und
abonniert keine fremden Sensor-Fakten.

## Bidirektionale Config-Synchronisation

Worker-lokale Änderungen committen zuerst über den lokalen Single-Writer und
werden danach im SQLite-Outbox-Journal als `config.propose` gespeichert.
Reihenfolge:

```text
LOCAL_COMMITTED -> PENDING/AWAITING_ACK -> MASTER_ACKED -> CONVERGED
```

Stale Proposals werden sichtbar rebased. Die Master-Replikation wird
asynchron geprüft und ausgerollt. Kein lokaler Regelzyklus wartet auf ein
Master-ACK.

## Sensor- und Aktorpfad

Sensorwerte werden am Owner gelesen, lokal persistiert und per MQTT gespiegelt.
Der Master speichert sie nur für Monitoring. Nur ein aktivierter Cross-Worker
konsumiert fremde Fakten für Cross-Regeln.

Remote-Aktoren durchlaufen:

```text
RECEIVED -> DISPATCHED -> CONFIRMED
                       -> WRITE_UNVERIFIED / FAILED / UNCERTAIN
```

`CONFIRMED` verlangt Ownership, gültigen Fencing-Term, Safety-Freigabe,
Feldbus-Write und passenden Readback. Ein Cross-Task enthält nur UUID und lokale
Aktor-ID; Buskoordinaten werden am Owner kanonisiert.

Automatisch vom Coordinator erzeugte Aktortasks werden abgewiesen. Zulässig
sind manuelle/kommissionierte Befehle und weitergeleitete Cross-Aktionen eines
registrierten Worker-Nodes.

Weitere Details: `../ARCHITEKTUR_CROSS_RUNTIME_V0083_DE.md` und
`../ARCHITEKTUR_CONFIG_V0092_DE.md`.
