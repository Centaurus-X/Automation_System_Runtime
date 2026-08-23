# Release-Validierung v0092

Verbindliche Gates:

- `./test_all.sh` auf Linux mit allen Worker-Tests und beiden Preflights
- V9 Storage↔Runtime↔Storage mit identischem Fingerprint
- vollständiges `automation_contracts`-Paket bytegleich zu Master und Proxy;
  interne Worker-Diffs deklarieren `representation="runtime"` explizit
- fn-01 und fn-02 als flache Node-V9-Standardprofile, Cross V8
  flat/hardwarefrei, V5 nur mit explizitem Opt-in
- aktives Config-Verzeichnis enthält nur Verträge ab V8; `<8` ausschließlich
  unter `_legacy/`, ohne implizite oder rekursive Runtime-Discovery
- native MQTT/Master-Diffs und lokale File-WAL-Diffs getrennt nachgewiesen
- `local_first` meldet vor dem Master-Apply das persistierte
  Replica-Paar aus Basisrevision/-fingerprint und den abweichenden Live-
  Fingerprint separat; nach Full-Apply zeigt das Paar gemeinsam auf das Ziel
- interner State-Persistenz-Lost-Update-Guard gegen parallel sichtbare V9-Dateiedits
- Controller-/Sensor-/Aktuator-/Rule-/Process-/PID-Laufzeitmatrix mit Restore
- dynamischer V9-Aktor des freigegebenen Simulator-Controllers innerhalb des
  controllergebundenen Limits erlaubt; Over-Limit, stale Revision und fremder
  Controller fail-closed abgewiesen
- Client↔Proxy↔Master↔Worker sowie realer Modbus-Readback
- Restart-, Idempotenz-, Stale-/Hash- und Recovery-Negativfälle
- Release-Manifest, Archivhash, Cache- und Secret-Scan

Die endgültigen Laufresultate stehen in `TEST_RESULTS.txt`. Der exakte
Archivhash steht ausschließlich in der externen `.zip.sha256`-Sidecar-Datei;
das interne `SHA256SUMS` deckt den vollständigen Releasebaum ohne sich selbst
ab und wird vom Builder verifiziert.
