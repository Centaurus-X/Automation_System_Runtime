# Release Notes v0094

`automation_system_v0094` ist der additive, unver�nderliche Nachfolger von
`automation_system_v0093`. v0093 und alle �lteren Versionen bleiben
unver�ndert.

## v0094-Praezedenz-Delta

- Ein konkurrierender Master-Patch gewinnt deterministisch gegen ein lokales
  JSON-Proposal derselben Baseline.
- Der Worker wartet ohne Log-Schleife auf den autoritativen Master-Shard,
  materialisiert diesen atomar und verwirft den lokalen Kandidaten genau einmal.
- Dieser erwartete Konflikt erzeugt exakt ein WARNING und keinen Rollback.
- Stabile unerwartete Fehler werden weiterhin gezaehlt, aber pro Ursache auf
  einen Logeintrag je 30 Sekunden mit Suppression-Zusammenfassung begrenzt.
- Der State-21-Hot-Reload 800 -> 1800 ist als exakte Regression abgedeckt.
- Alle fachlichen V9-Abschnitte koennen lokal als Master-validiertes Proposal
  geaendert werden; `meta` und Secret-/TLS-Material bleiben Systemgrenzen.
- Der Materialisierungsfehler enthaelt jetzt Status, Ursache und vorhandenen
  externen Fingerprint statt der ursachenlosen Sammelmeldung.
- Keine neue externe Laufzeitabhaengigkeit wurde eingefuehrt.


## Geerbte v0093-Basis

- Der lokale File-Sync bewertet Konflikte anhand des tats�chlich von seinem
  Action-Plan besessenen State-/Parameter-Overlays statt anhand des gesamten
  Live-Config-Fingerprints.
- Ein bereits angewendeter Plan mit `NO_CHANGE` wird bei nachgewiesener
  Zielkonvergenz durable nachgef�hrt.
- Die JSON-Materialisierung akzeptiert genau den erwarteten, bereits
  journalisierten File-WAL-Kandidaten, ohne den Lost-Update-Schutz f�r fremde
  �nderungen zu lockern.
- PID-/ONOFF-Controller besitzen eine deterministische Konfigurationssignatur.
  Relevante Laufzeit�nderungen bauen nur den betroffenen Controller neu auf;
  reine Setpoint-�nderungen erhalten seinen dynamischen Speicher.
- Sensor 5 des Controllers
  `62671112-766c-59ed-b0ff-30a49d48f747` verwendet Profil 1 und damit dieselbe
  Temperatureinheit wie der zugeh�rige State-21-Sollwert.
- Der All-Actuator-Test synchronisiert zus�tzlich auf die asynchrone
  Ausgabecapture und ist dadurch deterministisch.

## Unver�ndert

V9-Schema, Storage-/Runtime-Projektion, Master-/MQTT-Diff-Commit,
File-WAL-Protokoll, Modbus-Adressen, Aktorprofil 2, Safety-Limits, PKI,
Topic-/ACL-Vertr�ge und die Worker-Ownership-Architektur bleiben unver�ndert.

Der State-21-Sollwert und die Wahl zwischen PID 1 und PID 2 werden nicht
automatisch ver�ndert, weil die gelieferten Config-Varianten unterschiedliche
fachliche Werte enthalten. v0094 stellt sicher, dass eine explizite
Laufzeit�nderung dieser Werte korrekt aktiv und persistent wird.

## Regressionsschutz

- lokaler Start trotz unabh�ngiger Live-�nderung;
- `NO_CHANGE`-Race mit durablem Abschluss;
- bekannter Kandidat nach fortgeschrittenem Heap;
- weiterhin blockierte fremde Dateiedits;
- sofortige PID-Gain-/Profil-Rekonfiguration;
- speicherschonender Setpoint-Update;
- vollst�ndige lokale Aktor-Pipeline.

Die vollst�ndige Ursachen- und Kurvenanalyse steht in
`DIAGNOSE_CONFIG_PRECEDENCE_V0094_DE.md`. Die ausgef�hrten Release-Gates stehen in
`RELEASE_VALIDATION_V0094.md` und `TEST_RESULTS.txt`.
