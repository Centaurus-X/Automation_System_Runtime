# Release-Validierung v0096

## Verbindliche Gates

1. `VERSION` und Verzeichnisname sind `automation_system_v0096`.
2. Keine Klassen, Decorators, Funktionsannotationen oder anonymen Funktionen in
   Python-Quellen.
3. Alle Python-Quellen lassen sich kompilieren.
4. Bestehende V8-/V9- und Cross-Scope-Konfigurationen validieren.
5. Alle Worker-Tests bestehen.
6. Der Snap7-Emulatortest darf im Release-Gate nicht übersprungen werden.
7. Keine privaten Schlüssel, Runtime-Dateien, Logs oder Bytecode-Dateien im
   Release-Baum.
8. `SHA256SUMS` deckt den Release-Baum exakt ab.
9. ZIP-Inhalt, Dateimodi, CRC und Sidecar-Hash werden unabhängig geprüft.
10. Control Plane r4 verankert exakt den finalen v0096-ZIP- und Manifest-Hash.
11. Fresh-Rollout und E2E-Tests auf der Testumgebung bestehen.

## Lokaler Worker-Gate

```bash
python -m pip install --requirement requirements-plc-s7.txt
REQUIRE_S7_EMULATOR=1 ./test_all.sh
python _tools/build_release_archive.py
```

## Zusätzliche fokussierte Tests

```bash
python test/test_tia_plc_adapter.py
REQUIRE_S7_EMULATOR=1 python test/test_tia_plc_adapter_emulator.py
python test/test_tia_connection_integration.py
python test/test_tia_openness_converter.py
python test/test_generic_thread_pool_advanced.py
```

## Erwartete Nachweise

- Batchgrenzen `20/20/rest` bleiben stabil.
- CPU-`RUN` und CPU-`STOP` werden sichtbar.
- Inline-Secrets, optimierter DB-Zugriff und fehlender TLS-Provider scheitern.
- Timeout oder Abbruch blockiert keinen Pool-Worker dauerhaft.
- Höhere Priorität gewinnt; gleiche Priorität bleibt FIFO.
- Manager-Snapshot meldet `manager_polling=false`.
- Modbus-Connection-Key und vorhandene Config-Verträge driften nicht.

Die finalen Hashes und Remote-E2E-Ergebnisse werden erst nach vollständig
grünem Release-Gate in `TEST_RESULTS.txt` eingetragen.
