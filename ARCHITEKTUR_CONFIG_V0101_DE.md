# Config-Architektur v0101 – Authority V5

V9 ist der primäre kanonische Node- und Persistenzvertrag. Sensoren und
Aktuatoren liegen im JSON flach als vollständige Entities vor. Ausschließlich
an der zentralen Storage↔Runtime-Grenze werden sie in das gruppierte
Runtime-Modell überführt. RuleEngine, FSM, Automation und Controller kennen
deshalb keine zweite Speicherrepräsentation.

Cross-Konfiguration ist ein eigener V8-Vertrag. Sie bleibt flach,
hardwarefrei und besitzt getrennte Authority-, Scope- und Execution-Epochen
sowie eigene Fingerprints. Config-Verträge mit `meta.version < 8` liegen nur
unter `_config/config_data/_legacy/` und werden ausschließlich durch einen
expliziten Legacy-Import verwendet. Die aktiven Node-Pfade für `fn-01` und
`fn-02` sind V9; ihr Legacy-Import ist deaktiviert.

## Einheitliche Commit-Grenze

Zwei differenzielle Eingänge enden in derselben Worker-lokalen Authority:

1. Client → WSS-Proxy → Master-Relay → `distributed.config.propose` →
   Worker-Inbox → kanonische Operations → SEM/Safety → Authority-Commit.
2. Lokale JSON-Datei → File-WAL → stabiler Selector-Diff → kanonische
   Operations → SEM/Safety → derselbe Authority-Commit.

Das einzige verteilte Operationsformat ist
`config_scope_operations_v1`. Proxy und Master authentifizieren, prüfen und
relayen die Operations unverändert. Sie mappen keine Operations, vergeben
keine Node-Revision und committen keinen Node-Shard. Legacy Apply-/Patch-
Ingress wird nur im expliziten Migrationsmodus Worker-seitig adaptiert.

Jeder Commit bindet
`(authority_epoch, base_scope_revision, base_fingerprint)` und erzeugt exakt
die nächste Scope-Revision sowie den kanonischen Zielfingerprint. Journal,
Scope-Head, Snapshot und Mirror-Outbox werden vor der Runtime-Aktivierung
dauerhaft geschrieben. Ein Duplicate verwendet das persistierte Inbox-
Ergebnis und führt keinen zweiten Commit aus.

## Mirror und Ausfallverhalten

Der Worker publiziert seinen lokalen Head lazy über das eigenständige
`distributed.mirror.*`-Protokoll. Erst ein exaktes ACK für Authority-Epoch,
Scope-Revision, Execution-Epoch und Fingerprint erlaubt die Kompaktierung der
restartfesten Mirror-Outbox. Der Master ist dabei ausschließlich RRMA:
Registry, Relay, Mirror und Audit.

Ein Master- oder Broker-Ausfall blockiert keinen lokalen Commit. Nach
Reconnect konvergiert ausschließlich Worker→Master; ein Master-Mirror wird
niemals als implizite Node-Baseline zurückmaterialisiert. Ein älterer Mirror
kann nur über den expliziten, auditierten Authority-Transfer- oder
Disaster-Restore-Vertrag aktiviert werden.

## File-WAL und Profile

Interne State-Persistenz arbeitet mit einer Fingerprint-Precondition. Ein
bereits sichtbarer fremder V9-Stand wird nicht überschrieben, sondern dem
File-WAL zur differenziellen Verarbeitung überlassen. Der terminale
`last_operation`-Beleg bindet exakten Diff, Route, Commit, Journal und
Mirror-ID.

`config_file_sync.enabled` ist im Basis-/Produktionsprofil `false`. Nur
ausdrückliche Laborprofile wie `worker_simulation` dürfen den Datei-Ingest
aktivieren. Auch dort bleibt der Worker die finale Authority; es existiert
kein Master-first-Dateipfad.

## Normative Fencing-Identität

Die persistierte Identität jedes Node-/Cross-Scope lautet:

`(authority_epoch, scope_revision, execution_epoch, fingerprint)`

Veraltete Authority-, Scope- oder Execution-Werte werden fail-closed
abgewiesen. Fingerprint-Gleichheit allein legitimiert weder eine Revision noch
einen Owner-Wechsel.
