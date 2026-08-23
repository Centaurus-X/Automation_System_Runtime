# Directory Structure Runtime v0094

```text
automation_system_v0094/
├── _config/
│   ├── application/app_config.yaml
│   ├── certs/README.md
│   └── config_data/
│       ├── device_state_data_v9_node_fn-01.json
│       ├── device_state_data_v9_node_fn-02.json
│       ├── device_state_data_v8_cross_fn-01.json
│       └── _legacy/                    # nur expliziter Import, meta.version < 8
│           ├── device_state_data_v5_multi_worker_norm.json
│           ├── device_state_data_v7_cross_fn-01.json
│           ├── device_state_data_v7_node_fn-01.json
│           └── device_state_data_v7_node_fn-02.json
├── _documentation/
├── src/
│   ├── adapter/
│   │   ├── _cross_io_adapter.py
│   │   └── fieldbus/                 # ausschließlich lokale Hardware
│   ├── core/
│   │   ├── _worker_thread.py         # lokale Legacy-Pipeline
│   │   ├── _cross_worker_thread.py   # isolierte Cross-Pipeline
│   │   ├── distributed_coordinator.py
│   │   └── thread_management.py
│   ├── interfaces/mqtt_interface.py
│   ├── library/
│   │   ├── _cross_scope_library.py
│   │   ├── _cross_snapshot_store.py
│   │   ├── _generic_thread_pool.py
│   │   ├── _generic_type_library.py
│   │   └── weitere generische FSM-/Rule-/State-Bibliotheken
│   └── system/
│       ├── bootstrap_runtime_interface.py
│       ├── bootstrap_system_interface.py
│       └── worker_preflight.py
├── test/
│   ├── fixtures/                     # unveränderliche Graph-Fixtures
│   └── test_*.py
├── ARCHITEKTUR_CONFIG_V0092_DE.md
├── ARCHITEKTUR_CROSS_RUNTIME_V0083_DE.md
├── DEPLOYMENT_V0092_DE.md
├── RELEASE_NOTES_V0092_DE.md
├── RELEASE_VALIDATION_V0092.md
├── DIAGNOSE_HOT_RELOAD_PID_V0093_DE.md
|-- DIAGNOSE_CONFIG_PRECEDENCE_V0094_DE.md
├── RELEASE_NOTES_V0094_DE.md
├── RELEASE_VALIDATION_V0094.md
├── SHA256SUMS
└── test_all.sh
```

Logs, Sensorwert-Snapshots, Operationsjournale, Cross-Snapshots und
Python-Caches sind Laufzeitdaten. Sie werden erst unter `logs/`, `_runtime/`
beziehungsweise im temporären Cache des Zielsystems erzeugt und sind nicht im
Release-Inhalt vorbefüllt.
