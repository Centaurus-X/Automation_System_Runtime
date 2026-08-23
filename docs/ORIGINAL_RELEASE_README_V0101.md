# Automation System v0101

Eigenständiger Worker für autonome Feldsteuerung mit Authority V5. Der Worker
bleibt im Standalone- und Distributed-Modus die finale Config-Authority seines
Node-Shards. Ein Ausfall von Master, Broker oder Cross-Koordination darf die
lokale Regelung nicht anhalten.

## Zielarchitektur

- Sensorik, RuleEngine, FSM, Automation, Safety und Hardware-I/O laufen lokal.
- `node_local` ist der Standard in Standalone und Distributed.
- Der Master ergänzt Registry, Relay, Mirror und Audit, entscheidet aber nicht über Node-Werte.
- SEM autorisiert jeden externen oder dateibasierten Commit vor der atomaren Aktivierung.
- Scope-Heads werden als monotones Tupel aus Authority-Epoch, Scope-Revision, Execution-Epoch und Fingerprint persistiert.
- Cross-Ausführung ist hardwarefrei isoliert; nur der Hardware-Owner schreibt und bestätigt per Readback.

## Contract- und Config-Normen

| Vertrag | Version |
|---|---|
| Contract Authority | `2.0` |
| Type Schema | `2.0.0` |
| Node Storage | V9 |
| Cross Storage | V8 |
| Legacy-Import | ausschließlich explizit V5 |
| Proposal-Format | `config_scope_operations_v1` |

Die einzigen Authority-Quellen liegen unter `src/library/`. Das alte Paket
`automation_contracts` und ein paralleles `src/libraries/` sind verboten.

## Profile und Start

```sh
chmod 0755 run_worker.sh install_mqtt_identity.sh test_all.sh
AUTOMATION_PROFILE=standalone ./run_worker.sh --preflight
AUTOMATION_PROFILE=standalone ./run_worker.sh
```

Für Distributed-Betrieb muss zuvor die node-spezifische mTLS-Identität über
`install_mqtt_identity.sh` installiert werden. Secrets und private Schlüssel
gehören nie in Config, Releasebaum oder Archiv.

Der Linux-Produktionsstart erfolgt ausschließlich über `run_worker.sh`. Der
Launcher setzt die in `runtime.python_cache` definierte Cache-Policy vor dem
Interpreterstart; der Preflight weist abweichende Umgebungen fail-closed ab.

## Wichtige Pfade

- `_config/application/app_config.yaml`: Profile, Authority- und Runtime-Policy.
- `_config/config_data/device_state_data_v9_node_fn-01.json`: aktiver Node-V9-Contract für `fn-01`.
- `_config/config_data/device_state_data_v9_node_fn-02.json`: aktiver Node-V9-Contract für `fn-02`.
- `_config/config_data/device_state_data_v8_cross_fn-01.json`: separater Cross-V8-Contract.
- `_config/config_data/_legacy/`: ausschließlich explizite Import- und Regressionsfixtures.
- `src/core/authority_transition_service.py`: Transfer und Restore.
- `src/core/distributed_coordinator.py`: Proposal-, Mirror- und Cross-Protokoll.
- `src/core/config_file_sync.py`: crash-sicherer File-WAL-/SEM-Pfad.

## Test und Archiv

```sh
./test_all.sh
python3 _tools/build_release_archive.py
```

Der Archiv-Builder erzeugt außerhalb des Worker-Verzeichnisses:

- `automation_system_v0101.zip`
- `automation_system_v0101.zip.sha256`

Das interne `SHA256SUMS` deckt den Releasebaum exakt ab. Caches, Logs,
Laufzeitdaten, `.verification_deps` und private Schlüssel sind unzulässig.

## Dokumentation

- `ARCHITEKTUR_CONFIG_V0101_DE.md`
- `DIAGNOSE_AUTHORITY_V5_V0101_DE.md`
- `RELEASE_NOTES_V0101_DE.md`
- `RELEASE_VALIDATION_V0101.md`
- `DEPLOYMENT_V0092_DE.md` für die weiterhin gültige Identitätsinstallation
- historische v009x-Diagnosen als Auditspur

---

This file preserves the README contained in the byte-exact v0101 release
archive. The repository-facing README was expanded for publication without
changing the runtime source, configuration or tests.
