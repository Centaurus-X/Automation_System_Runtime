# Diagnose: Laufzeit-Config, Master-Prioritaet und CLI-Error-Sturm v0094

## Kurzbefund

Die beobachteten Meldungen stammen vom noch laufenden Worker v0092. Der
File-Watcher erkannte die gueltige lokale JSON-Aenderung, journalisierte sie
und erhielt anschliessend beim Persistieren einen Lost-Update-Schutzfehler.
Da der WAL-Eintrag pending blieb, wurde derselbe unveraenderte Fehler in jedem
0,5-Sekunden-Poll erneut verarbeitet und als ERROR ausgegeben.

Die Parametergrenze 800 oder 1800 ist nicht die Ursache. Beide Werte sind
gueltige fachliche Laufzeitwerte. Ursache war die Kombination aus altem
Materialisierungs-Fingerprint und einer ungedrosselten Retry-Logschleife.

## Deterministische Prioritaet

Die Authority-Reihenfolge lautet:

1. Ein vom Master akzeptierter und revisionierter Patch ist autoritativ.
2. Eine lokale V9-Dateiaenderung ist ein Proposal, keine zweite Authority.
3. Treffen beide auf derselben Baseline zusammen, gewinnt der Master.
4. Der Worker wartet, bis der autoritative Master-Shard live ist.
5. Danach materialisiert er exakt diesen Stand atomar in die lokale JSON-Datei.
6. Der lokale Kandidat wird genau einmal als verworfen abgeschlossen.

Der Worker fuehrt bei diesem Rennen keinen Baseline-Rollback aus. Dadurch kann
eine alte lokale Baseline einen neueren Master-Stand weder ueberschreiben noch
kurzzeitig wieder aktivieren.

## Logging-Semantik

Ein erwarteter Master-vs.-Local-Konflikt erzeugt genau ein WARNING:

`Local config proposal ... was not written because a concurrent Master patch has priority`

Solange der neue Master-Shard noch nicht live ist, wartet der Worker ohne
ERROR- oder WARNING-Loop. Unerwartete stabile technische Fehler werden weiter
gezaehlt, aber pro Fehlerursache hoechstens einmal je 30 Sekunden ausgegeben.
Der naechste Eintrag nennt die Anzahl unterdrueckter identischer Wiederholungen.

## Laufzeitveraenderbare V9-Daten

Alle fachlichen Top-Level-Abschnitte der kanonischen V9-Config sind ueber beide
Commit-Urspruenge zugelassen:

- Client -> Proxy -> Master -> Worker;
- lokale Worker-JSON-Datei -> File-WAL -> Master -> Worker.

Dies umfasst insbesondere `pid_configs`, Sensor-/Aktortypen, Scaling-Profile,
Modbus-Funktionen, Prozessmodi/-werte, Control-Method-Kataloge, Filter, Timer,
Rule-Engine, Sites, Nodes, Controller, Sensoren, Aktoren, Prozesse, States und
Rules. `meta` bleibt die systemeigene Vertrags-/Identitaetshuelle. Secrets,
Credentials, Zertifikate und TLS-Material bleiben default-deny und gehoeren
nicht in die fachliche V9-Laufzeitkonfiguration.

## Regressionsnachweise

`test/test_config_file_sync.py` prueft konkret:

- State 21 nacheinander mit 800 und 1800 waehrend laufender Runtime;
- lokales Proposal und konkurrierenden `REBASE_REQUIRED`-Master-Patch;
- Wartephase ohne Log, danach Master-Materialisierung;
- genau ein Warning, null Rollbacks und einen IDLE-WAL-Abschluss;
- 20 identische technische Fehler mit genau einem sofortigen Logeintrag und
  einer spaeteren zusammengefassten Meldung.

Der Control-Plane-E2E `config_commit_path_equivalence_e2e.py` startet diesen
Praezedenzfall gegen den exakt gepinnten Worker-Release und liefert
`master_wins=true`, `warning_count=1`, `rollback_count=0` sowie
`stable_error_stream_coalesced=true` im maschinenlesbaren Ergebnis.

## Bibliotheksentscheidung

Es wurde keine neue externe Laufzeitbibliothek aufgenommen. Die vorhandene
kanonische `scope_diff`-Bibliothek, SHA-256-Fingerprints, File-WAL und atomare
fsync-Persistenz bilden bereits die richtige Abstraktion. Eine weitere
Abhaengigkeit wuerde Python-3.14-no-GIL-Kompatibilitaet, Supply Chain und
Fehlerflaeche vergroessern, ohne die Commit-Semantik zu verbessern.
