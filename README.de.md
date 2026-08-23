# Automation System Runtime

**Eine lokal autonome, verteilbare Industrial-Automation-Runtime für
deterministische Steuerung, explizite Authority-Grenzen und robuste
Feldoperation.**

[English README](README.md) · [Architektur](docs/ARCHITECTURE.md) ·
[Deployment](docs/DEPLOYMENT.md) ·
[Technische Diagnose](docs/TECHNICAL_ASSESSMENT.md) · [Security](SECURITY.md)

## Warum dieses System existiert

Eine lokale Feldregelung sollte nicht anhalten, nur weil Broker, Master oder
Cross-Koordination vorübergehend ausfallen. Deshalb bleiben Sensorik,
RuleEngine, Zustandsautomat, Automation, Safety und Hardware-I/O beim Worker.
Die verteilte Ebene ergänzt Registry, Relay, Mirror und Audit, übernimmt aber
nicht die finale Authority über den Node-Shard.

Dieses Release bündelt mehrere Jahre iterativer Entwicklungsarbeit. Der
entscheidende, bewusst konservative Grundsatz lautet:

> **Der Node, der die Hardware besitzt, bleibt die finale Authority über seine
> lokale Konfiguration und Ausführung.**

## Für wen ist das Projekt gedacht?

- OT- und Automatisierungsingenieure mit Bedarf an lokaler Autonomie.
- Systemintegratoren für Modbus, S7/TIA, OPC UA und weitere Feldprotokolle.
- Plattformteams, die Worker-/Control-Plane-Grenzen robust entwerfen.
- Security Engineers, die mTLS, Fencing, Fail-Closed-Verhalten und nachvollziehbare Zustandsübergänge prüfen.
- Entwickler, die Free-Threaded Python für parallele Control-Workloads evaluieren.

## Wesentliche Ergebnisse

- Lokale Automation läuft auch bei Master- oder Broker-Ausfall weiter.
- Externe und dateibasierte Änderungen enden an derselben atomaren, Safety-autorisierten Commit-Grenze.
- Cross-Regeln koordinieren Nodes, erhalten aber niemals direkten Hardwarezugriff.
- Authority-Heads, Journale, Mirror-Outbox und Laufzeitwerte überstehen Neustarts.
- Remote-Aktorabsichten werden zum Hardware-Owner geroutet und erst nach passendem Readback bestätigt.
- Versionierte Contracts und Regressionstests machen Änderungen prüfbar.

## Reifegrad

**Pre-Production / technisch validierter Release-Snapshot.** Das System ist
nicht unabhängig Safety-, Echtzeit- oder IEC-62443-zertifiziert und beansprucht
keine pauschale NIS2-/KRITIS-Konformität. Vor dem Einsatz an realer Hardware
sind eigene Hazard-, Timing-, Security- und Integrationsprüfungen zwingend.

## Schnellstart

```bash
git clone https://github.com/Centaurus-X/Automation_System_Runtime.git
cd Automation_System_Runtime

python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

chmod 0755 run_worker.sh install_mqtt_identity.sh test_all.sh
./test_all.sh

AUTOMATION_PROFILE=standalone ./run_worker.sh --preflight
```

> **Achtung:** Vor dem tatsächlichen Start eines hardwarefähigen Profils müssen
> `_config/application/app_config.yaml` und die aktive Node-Konfiguration
> geprüft werden. Die ausgelieferten Profile `standalone` und
> `worker_simulation` erlauben den Simulator unter `192.168.0.3:5020`.

## Technischer Kern

| Bereich | Umsetzung |
|---|---|
| Authority | Node-local Authority V5 mit Authority-, Scope- und Execution-Epoch |
| Konfiguration | Node V9, Cross V8, expliziter V5-Legacy-Import |
| Commit | SEM Single Writer, Safety Policy, File-WAL, atomare Aktivierung |
| Verteilung | MQTT/MQTTS, Registry, Relay, Mirror, ACK und durable Outbox |
| Cross | hardwarefreie Cross-Runtime, UUID-only Aktorintents |
| Persistenz | SQLite-Journal, Authority-Heads, Runtime-Value-Store |
| Parallelität | isolierte Controller-Owner, Event Router, Thread Pool |
| Feldprotokolle | Adaptergrenzen für Modbus, S7/TIA, OPC UA, EtherNet/IP, KNX/IP, PROFINET und PROFIBUS-Gateway |

## Nachweis

Der Original-Release dokumentiert 54/54 bestandene ausführbare Testdateien auf
Linux, einen CPython-3.14.6t-Clean-Room-Lauf, mTLS/MQTT, reale
Modbus-Simulatorpfade und exakte Mirror-Konvergenz. Vor der Veröffentlichung
wurden zusätzlich der äußere ZIP-Hash sowie 191/191 Einträge des internen
`SHA256SUMS` verifiziert. Das originale ZIP wird bytegenau als Release-Artefakt
ausgeliefert.

Diese Belege sind projektinterne Engineering-Evidence und keine unabhängige
Zertifizierung. Details: [Release-Validierung](RELEASE_VALIDATION_V0101.md) und
[Publikationsdiagnose](docs/TECHNICAL_ASSESSMENT.md).

## Performance

Der lokale Regelpfad wartet nicht auf Master oder MQTT, Controller-Owner sind
isoliert und Buszugriffe können gebündelt werden. Universelle Latenz-, Jitter-
oder Durchsatzwerte wären ohne Zielhardware unseriös; deshalb enthält
[PERFORMANCE.md](docs/PERFORMANCE.md) ein Messmodell statt unbelegter Zahlen.

## Lizenz und kommerzielle Nutzung

Die Community-Ausgabe steht unter [GNU GPL v3.0 only](LICENSE). Abweichende
kommerzielle Lizenzbedingungen sowie Architekturreview, Integration,
Hardening, Deployment und Validierung können separat vereinbart werden. Siehe
[Commercial Licensing](COMMERCIAL_LICENSE.md).

Für eine erste Anfrage kann eine
[GitHub Discussion](https://github.com/Centaurus-X/Automation_System_Runtime/discussions)
ohne vertrauliche Details eröffnet werden.
