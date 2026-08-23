# Deployment v0083

## 1. Archiv entpacken und prüfen

```bash
unzip automation_system_v0083.zip
cd automation_system_v0083
chmod 0755 run_worker.sh install_mqtt_identity.sh test_all.sh
sha256sum -c SHA256SUMS
./test_all.sh
```

Wichtig: Start und Preflight immer über `run_worker.sh` ausführen. Nur dieser
Launcher kann `PYTHONDONTWRITEBYTECODE` und `PYTHONPYCACHEPREFIX` setzen, bevor
Python Projektmodule importiert. Der YAML-Block `runtime.python_cache` wird im
Preflight geprüft; ein direkter `python -m ...`-Start ist nicht freigegeben.

## 2. Standalone-Simulator testen

```bash
AUTOMATION_PROFILE=standalone ./run_worker.sh --preflight
AUTOMATION_PROFILE=standalone ./run_worker.sh
```

Das Profil pollt den ausgewählten Simulator-Controller alle 10 Sekunden und
registriert dessen fünf Aktoren durch einen einmaligen, wertneutralen
Readback/No-op-Write.

## 3. mTLS-Identität für Distributed installieren

Auf dem Quellsystem Zertifikate zunächst in ein temporäres Verzeichnis des
Workers übertragen. Danach auf dem Worker:

```bash
./install_mqtt_identity.sh \
  --source-dir "$HOME/.automation-cert-import-v0010" \
  --site-id site-main \
  --node-id fn-01

AUTOMATION_PROFILE=worker_simulation ./run_worker.sh --preflight
```

Der Installer setzt den Private-Key auf Modus `0600`. Das Release selbst
enthält keine Zertifikate oder Schlüssel.

## 4. Distributed starten

```bash
AUTOMATION_PROFILE=worker_simulation ./run_worker.sh
```

Erwartete Kernaussagen im Log:

```text
Worker preflight passed: profile=worker_simulation mode=distributed
Local automation pipeline: ENABLED distributed=True role=worker
```

Bei Brokerausfall darf MQTT `degraded` melden; Sensoren, lokale RuleEngine,
FSM, Automation und Aktoren müssen weiterlaufen. Sensorwerte werden unabhängig
davon nach `_runtime/sensor_values.json` persistiert.

Interne Aktualisierungen der sichtbaren Worker-Konfiguration werden zusätzlich
unter `_runtime/config_persistence_<node-id>.json` mit PREPARED/COMMITTED-
Fingerprint belegt. Diese Datei sowie das File-Proposal-WAL müssen bei Upgrade
und Neustart erhalten bleiben; sie enthalten keine Zugangsdaten.

Für den separat geshardeten Hardware-Owner fn-02 wird stattdessen dessen
eigenes Profil verwendet:

```bash
./install_mqtt_identity.sh \
  --source-dir "$HOME/.automation-cert-import-v0010" \
  --site-id site-main \
  --node-id fn-02

AUTOMATION_PROFILE=worker_fn02 ./run_worker.sh --preflight
AUTOMATION_PROFILE=worker_fn02 ./run_worker.sh
```

`worker_fn02` lädt ausschließlich
`_config/config_data/_legacy/device_state_data_v7_node_fn-02.json`,
öffnet nur Controller-UUID `06ab4528-f8a3-5ec7-a91f-eea79d60761e` und löst
eingehende Cross-Aktionen erst auf diesem Owner aus UUID plus `actuator_id` in
lokale Buskoordinaten auf. Sein Sensor-Snapshot liegt getrennt unter
`_runtime/sensor_values_fn-02.json`.

## 5. Cross-Runtime gezielt aktivieren

Vor Aktivierung müssen alle im Cross-Shard referenzierten Controller auf ihren
Owner-Nodes kommissioniert sein. In `_config/application/app_config.yaml`:

```yaml
cross_runtime:
  enabled: true
  node_id: "fn-01"
  domain_id: "cross-fn-01-default"
  config_path: "_config/config_data/_legacy/device_state_data_v7_cross_fn-01.json"
  snapshot_path: "_runtime/cross_snapshot_{node_id}_{domain_id}.json"
  max_signal_age_s: 15.0
```

Zusätzlich müssen die lokalen Ziel-Controller in
`thread_management.controller_allowlist` und alle Zielaktoren in der
Safety-Allowlist enthalten sein. Remote-Ziel-Worker benötigen korrekte
Broker-ACLs für `distributed.actuator.execute` und Result-Topics.

Danach erneut:

```bash
./run_worker.sh --preflight
./run_worker.sh
```

Ein Cross-Fehler erscheint als degradierte Cross-Komponente, darf aber die
lokale Pipeline nicht stoppen.
