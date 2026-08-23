# Deployment v0092

## Voraussetzungen

- Linux mit Python 3.14t/free-threaded für die Produktionsabnahme
- PyYAML und die in `requirements.txt` festgelegten Abhängigkeiten
- node-spezifische MQTT-mTLS-Identität
- erreichbarer Feldbus bzw. freigegebener Simulator

## Prüfung und Start

```bash
./test_all.sh
AUTOMATION_PROFILE=worker_simulation ./run_worker.sh --preflight
AUTOMATION_PROFILE=worker_simulation ./run_worker.sh
```

Die Standardpfade der fn-01-/fn-02-Worker sind
`_config/config_data/device_state_data_v9_node_fn-01.json` beziehungsweise
`_config/config_data/device_state_data_v9_node_fn-02.json`; Legacy-Import ist
in beiden Hardware-Owner-Profilen deaktiviert. Zertifikate und
Runtime-Journale werden außerhalb des
unveränderlichen Release-Manifests installiert bzw. erhalten.

Configs mit `meta.version < 8` sind unter `_config/config_data/_legacy/`
isoliert. Sie dürfen nur über eine explizite Import-/Regression-Konfiguration
mit `allow_legacy_import: true` und einem `_legacy`-Pfad geladen werden; aktive
Standardprofile und Config-Discovery bleiben auf V9 Node beziehungsweise V8
Cross begrenzt.

Ein Upgrade erfolgt über das verifizierte Control-Plane-Rollout. Archivhash,
internes `SHA256SUMS`, Zielpfad und installierte Version werden vor dem Start
fail-closed geprüft.
