# Config-Architektur v0092

V9 ist der primäre kanonische Node- und Persistenzvertrag. Sensoren und
Aktuatoren liegen im JSON flach als vollständige Entities vor. Erst an der
zentralen Storage↔Runtime-Grenze werden sie in das bewährte gruppierte
Runtime-Modell überführt; RuleEngine, FSM, Automation und Controller müssen
das Speicherformat nicht kennen.

Cross-Konfiguration ist ein eigener V8-Vertrag. Sie bleibt flach,
hardwarefrei und besitzt eigene Revisionen und Fingerprints. Config-Verträge
mit tatsächlichem `meta.version < 8` liegen ausschließlich unter
`_config/config_data/_legacy/` und werden nur in ausdrücklich aktivierten
Legacy-Profilen bzw. Regressionstests importiert. Nicht rekursive aktive
Config-Pfade sehen damit ausschließlich V9 Node und V8 Cross. Das gilt für
beide Hardware-Owner-Profile `fn-01` und `fn-02`; Legacy-Import ist in beiden
deaktiviert.

Zwei voneinander unterscheidbare differenzielle Commit-Wege bleiben erhalten:

1. Native Kette: Client → WSS-Proxy → MQTT → Master →
   `distributed.config.patch` → Worker-Inbox → atomarer Runtime-Commit.
2. Lokale JSON-Datei: File-WAL → Stable-Selector-Diff → Master-Proposal →
   Entscheidung → atomarer Worker-Commit bzw. Restore.

Interne State-Persistenz arbeitet gegen eine Fingerprint-Precondition: Ein bereits
sichtbarer fremder V9-Stand wird nicht überschrieben, sondern dem File-WAL zur
differenziellen Verarbeitung überlassen.

Der native Patch prüft Base-Scope-Revision und -Fingerprint, jeden
Operation-Hash, Zielrevision und Zielfingerprint. Authority-/Replica-Metadaten
werden erst nach erfolgreichem Runtime-Commit fortgeschrieben. Duplicates
verwenden die persistierte Inbox-Antwort und führen keinen zweiten Commit aus.

Registrierung und Heartbeat behandeln `node_scope_revision` und
`node_shard_fingerprint` als ein persistiertes Replica-Paar. Bei einem
`local_first` vorab angewendeten lokalen Commit bleibt dieses Paar bis zum
bestätigten Master-Apply auf der letzten autoritativen Basis; nur
`live_config_fingerprint` zeigt den bereits laufenden Zielstand. Ist bei der
Erstinitialisierung noch kein Node-Shard-Fingerprint gespeichert, wird der
aktuelle V9-Live-Fingerprint einmalig mit der bestehenden Scope-Revision als
Ausgangspaar persistiert.
