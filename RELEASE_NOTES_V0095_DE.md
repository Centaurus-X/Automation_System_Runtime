# Release Notes v0095

`automation_system_v0095` ist der additive Nachfolger von v0094. Das Release
ändert ausschließlich die Behandlung gültiger lokaler V9-Datei-Proposals,
die an einer lokalen Authority-/Aktionsdomänen-Grenze abgelehnt werden.

## Produktivdelta

- `ConfigFileSync` extrahiert den ursprünglichen Staging-Code und die
  ursprüngliche Meldung.
- `required_mode`, `writable_fields` und `engine_missing` wechseln
  deterministisch auf den vorhandenen Master-first-Scope-Pfad.
- Das Master-Journal muss eine dauerhafte `message_id` bestätigen, bevor der
  Vorgang als `PENDING` gilt.
- Der erfolgreiche Authority-Wechsel erzeugt genau ein WARNING.
- `parameter_limit`, `interlock`, ungültige Ziele und technische Fehler
  bleiben fail-closed und werden nicht zum Master umgeleitet.
- Master-vs.-Local-Präzedenz, atomare Persistenz, PID-Signaturen und die
  V9-Verträge aus v0094 bleiben unverändert.

## Regressionen

- Echter State/Event-Management-Test für `target_temp_c: 200 -> 400`.
- Negativtest für `target_temp_c: 1800` und `parameter_limit`.
- Control-Plane-E2E mit exakter File-WAL-/Outbox-Korrelation für
  `200 -> 400 -> 200`.
- Fail-Closed-Verifier gegen eine falsch deklarierte Policy-Route.

Details stehen in `DIAGNOSE_CONFIG_FILE_POLICY_ROUTING_V0095_DE.md`.
