# Release Notes v0092

`automation_system_v0092` ist der additive, unveränderliche Nachfolger von
`automation_system_v0091`. v0091 und alle älteren Versionen bleiben
unverändert.

## Eng begrenztes Delta

- V9 bleibt das primäre, flache Config-Speicherformat.
- Persistierte V9-Dateien werden mit `sort_mode: fingerprint_safe` geschrieben.
  Dadurch erhalten Tabellen und Objektfelder wieder die kanonische lesbare
  Reihenfolge (IDs zuerst, `parameters` am Ende), ohne die semantisch relevante
  Reihenfolge der Entity-Arrays zu verändern.
- Der Config-Fingerprint und die Config-Identität werden vor und nach jedem
  Rendern geprüft. Eine identitätsverändernde Formatierung bleibt fail-closed.
- Ein bereits veralteter lokaler Rule-`action_plan` wird nach einem
  `stale_revision`-ACK terminal als `REJECTED` behandelt. Er wird nicht mit
  derselben idempotenten Operationsidentität endlos rebased und kann dadurch
  nachfolgende JSON/File-WAL-Proposals nicht mehr blockieren.
- Echte differenzielle `scope_operations` behalten unverändert den vorhandenen
  `REBASE_REQUIRED`-Pfad.

## Unverändert

Master-/MQTT-Diff-Commit, lokaler JSON/File-WAL-Commit, V9
Storage↔Runtime-Projektion, Rule Engine, FSM, Controller, Modbus, PID-Parameter,
PKI und bestehende Safety-Grenzen bleiben funktional unverändert.

## Regressionen

- Master-artige Feldreihenfolge → Worker-Persistenz → kanonische
  fingerprint-sichere V9-Darstellung.
- Veralteter Rule-Action-Plan vor einem File-WAL-Proposal → terminale
  Freigabe der FIFO; das File-Proposal wird anschließend zustellbar.
