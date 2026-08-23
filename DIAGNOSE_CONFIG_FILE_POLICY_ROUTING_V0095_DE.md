# Tiefendiagnose: Config-File-Parameter und Master-Routing v0095

## Befund

Die lokale Änderung von `process_states[21].parameters.target_temp_c` von
`200.0` auf `400.0` ist syntaktisch und fachlich gültig. Der Fehler in v0094
lag im Übergang zwischen dem lokalen File-WAL und der Runtime-Safety-Policy:

1. Die Datei wurde korrekt geladen und als runtime-fähiger Action-Plan erkannt.
2. Der aktive RuleEngine-Vertrag verlangte für automatische lokale Aktionen
   `required_mode_id = 2`, während State 21 noch `mode_id = 1` verwendete.
3. Die Runtime lehnte deshalb den lokalen Sofortpfad erwartungsgemäß mit
   `required_mode` ab.
4. `ConfigFileSync` interpretierte den fehlenden lokalen Journalbeleg jedoch
   als technischen Fehler, rollte die Datei zurück und meldete
   `file proposal was not durably journaled`.

Die Änderung der `pid_config_id` funktionierte, weil sie bereits als
struktureller Master-first-Patch klassifiziert und dauerhaft im
Distributed-Journal verankert wurde.

## Korrektur

Eine lokale JSON-Datei ist eine Proposal-Quelle, keine zweite Authority.
Wenn ein gültiger Datei-Action-Plan ausschließlich an einer
Authority-/Aktionsdomänen-Grenze scheitert, wird derselbe vorbereitete
Scope-Diff nun über den vorhandenen Master-first-Pfad journalisiert:

- `required_mode`
- `writable_fields`
- `engine_missing`

Der Master bleibt die einzige Autorität. Der Worker übernimmt den bestätigten
Master-Stand erst nach dauerhaftem Outbox-/Journalbeleg.

Echte Safety- und Vertragsverletzungen werden ausdrücklich nicht umgeleitet:

- `parameter_limit`
- `interlock`
- unbekannte Ziele oder Werte
- technische Persistenz-/Transportfehler

Für die erfolgreiche Umleitung entsteht genau ein verständliches WARNING.
Fehlgeschlagene Vorgänge enthalten nun den ursprünglichen Fehlercode und die
ursprüngliche Meldung, statt nur den ungenauen Journal-Sammelfehler zu zeigen.

## Präzedenz und Determinismus

Trifft während eines lokalen Proposals ein neuer Master-Patch ein, gewinnt
weiterhin der Master. Der lokale Kandidat wird nicht über den neueren
Authority-Stand geschrieben. Die bestehende Master-Präzedenz-, WAL- und
Log-Koaleszierungslogik bleibt unverändert.

## Verifikation

Der neue reale SEM-Regressionsfall beweist:

- `200.0 -> 400.0`: `required_mode`, Master-first, ein WARNING,
  null Rollbacks, anschließend konvergent.
- `400.0 -> 1800.0`: `parameter_limit`, kein Master-Fallback,
  präzise Ablehnung und sicherer Restore.

Der Control-Plane-E2E führt denselben `200.0 -> 400.0 -> 200.0`-Ablauf über
lokale V9-Datei, File-WAL, Master-Outbox und Worker-Replikation aus und
verlangt die exakte Route `policy_fallback`.
