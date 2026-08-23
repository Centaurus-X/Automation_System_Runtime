# Release Notes v0096

`automation_system_v0096` ist die direkte, additive Migration des eingefrorenen
v0095-Stands.

## Neu

- Vollständig integrierter `plc_s7`-Adapter für Siemens TIA Portal über
  ISO-on-TCP/python-snap7.
- Striktes, typisiertes DB-/M-/I-/Q-Adressmodell.
- Big-Endian-Codecs für skalare S7-Typen, Strings, WStrings und Bytefelder.
- Batch-Lesen mit höchstens 20 Variablen je Roundtrip.
- Health-Status mit Verbindung und CPU-Zustand.
- Fail-closed Security-Modi ohne Secrets in der V9-Fachkonfiguration.
- TIA-Openness-XML-Konverter für deterministische V9-Sensor-/Aktorfragmente.
- `plc_s7`-Timingprofil mit `cycle=0.1 s`, `min_gap=0.005 s` und
  `deterministic=false`.

## Thread-Pool

- Stabile Jobprioritäten.
- Queue- und Laufzeit-Timeouts.
- Abbruch vor und während der Ausführung.
- Optional kooperative Cancellation per Event.
- Condition-Wakeup statt 0,25-Sekunden-Manager-Polling.
- Begrenzte Isolation nicht kooperativer Alt-Jobs.
- Explizite Prioritäten für FSM, Automation, Cross und Rule Engine.

## Kompatibilität

- Modbus- und OPC-UA-Adapterverträge bleiben erhalten.
- Vorhandene Connection-Keys bleiben unverändert; nur S7 ergänzt `rack/slot`.
- Bestehende V8-/V9-Konfigurationen validieren unverändert.
- Cross-Scope-I/O-Zeilen bleiben hardwarefrei; die neue S7-Union wird im
  Cross-Profil korrekt rekursiv entspannt.
- Die Control Plane ist wegen Contract-, Artefakt- und Rollout-Pins als
  `automation_control_plane_release_v0020_r4` versioniert.

## Abhängigkeit

Der klassische S7-Pfad verwendet die separat gepinnte optionale Abhängigkeit:

```text
python-snap7==3.1.0
```

Installation:

```bash
python -m pip install --requirement requirements-plc-s7.txt
```

## Wichtige Grenze

Absolute DB-Adressen benötigen in TIA Portal deaktivierten optimierten
Bausteinzugriff. Sichere S7CommPlus/TLS-Kommunikation benötigt einen gesondert
qualifizierten Backend-Provider und fällt ohne ihn geschlossen aus.
