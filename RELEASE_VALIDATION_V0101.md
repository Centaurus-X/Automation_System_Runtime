# Release-Validierung – automation_system_v0101

## Identität

- eigenständiges Verzeichnis und Archiv: `automation_system_v0101`
- Worker-Semver im Control-Plane-Manifest: `0.101.0`
- Contract Authority: `2.0`
- Type Schema: `2.0.0`
- aktive Config-Normen: Node V9 und Cross V8
- Altformat V5: ausschließlich expliziter Import

## Lokaler Nachweis

| Gate | Ergebnis |
|---|---:|
| ausführbare Worker-Tests | 54/54 PASS |
| normativer V005-Standards-Gate | PASS |
| Architektur-Hard-Rules | PASS |
| Authority-Transfer/Restore | PASS |
| Node-local Mirror/Registry/Outbox | PASS |
| Cross-Owner- und Hardware-Epoch-Fencing | PASS |
| SEM, File-WAL und Recovery | PASS |
| JSON, YAML, Python-AST und Shell-Syntax | PASS |
| aktive UTF-8-Prüfung | 0 Encoding-Befunde |
| funktionaler AST-Style-Gate | 0 Klassen, Decorators, Lambdas oder Type Hints |

## Linux- und Live-Nachweis

Der Clean-Room-Lauf `20260726T230950Z-3358171` entfernte die bekannten alten
Control-Plane-Ressourcen und baute alle Images ohne Cache. Der Worker lief auf
dem Testhost unter CPython 3.14.6t (`--disable-gil`) mit frisch installierter
mTLS-Identität. POSIX-, OpenSSL-, MQTT- und Runtime-Gates bestanden.

Der produktive Worker `fn-01` wurde automatisch zugelassen und blieb
`APPROVED/ACTIVE`. Reale Sensorzyklen und readback-bestätigte Aktorwrites liefen
gegen den Modbus-Simulator `192.168.0.3:5020`; Sensor-, Aktor- und
Heartbeat-Telemetrie wurden auf den `site-main`-Topics beobachtet.

Der node-lokale Head konvergierte exakt in den Master-Mirror:

- Scope/Owner/Quelle: `node:fn-01 / fn-01 / fn-01`,
- Authority-/Execution-Epoch: `0 / 0`,
- Type/Contract: `2.0.0 / 2.0`,
- Fingerprint-Präfix: `0767595845b1`.

Ein im Live-Lauf entdeckter Contract-Fehler ließ bei `execution_epoch=0` das
Top-Level-Feld eines Mirror-Records aus. Der Record-Builder wurde in Worker,
Master und Proxy byte-identisch korrigiert. Die Regression
`test_mirror_outbox_execution_epoch.py` beweist den kanonischen Record und den
SQLite-Outbox-Roundtrip einschließlich des gültigen Nullwerts.

Eine vorhandene, vom Release abweichende fachliche Worker-Konfiguration wurde
beim Installieren bewusst weder überschrieben noch gelöscht. Paketbaseline,
Runtimezustand und nutzerverwaltete Config bleiben getrennte Zuständigkeiten.

## Archivfreigabe

Der bereinigte Releasebaum, das interne `SHA256SUMS`, ZIP und Sidecar werden
vom Release-Builder fail-closed abgeglichen. Die Control Plane pinnt exakt den
finalen Worker-ZIP-Hash und den Hash seines internen Manifests.
## Nachqualifikation des Artefakt-E2E

Die Artefakt-E2E-Nachprüfung deckte eine Repräsentationsabweichung auf: Ein
gruppierter Runtime-Snapshot konnte unverändert in den Mirror gelangen. Der
zentrale Persistenzvertrag projiziert deshalb jeden Scope-Snapshot über die
absolute Type Authority in die flache, versionierte Storage-Form. Nach der
Korrektur bestanden die vollständige Worker-Suite mit 54 Testdateien, der
Worker-Hard-Rule-Gate mit null Fehlern und die neue Runtime-zu-Storage-
Regression erneut.

## Abschlussgates für den Proposal- und Recovery-Pfad

Der erneute vollständige lokale Lauf bestand mit 54 von 54 ausführbaren
Testdateien, sechs Shell-Syntaxprüfungen und beiden Runtime-Preflights
(`unit_test`, `standalone`). Der zusätzliche Policy-Test beweist mit der realen
`authorize_external_commit()`-Funktion:

- `processes/process_location` ist für den Principal `master_relay` im
  kanonischen Format `config_scope_operations_v1` zulässig,
- derselbe Principal erhält für `credentials` weiterhin `path_denied`.

Der Testzustand wird nicht paketiert: erzeugte Sensor-Sequenzen und Logs werden
vor dem deterministischen Build entfernt, das interne `SHA256SUMS` wird danach
über den exakten bereinigten Baum neu gebildet. Ein Journal-Reset gegen eine
nicht frische Registry ist kein zulässiger Wiederanlauf; der beobachtete
`stale_revision`-Fence bestätigt diese Schutzwirkung.

## Portabilitäts- und Packaging-Nachweis

Der abschließende Neuaufbau prüft jedes ausgelieferte `*.sh` bytegenau. Ein
Carriage-Return führt bereits vor Manifest- und ZIP-Erzeugung zum Abbruch. Die
vollständige Worker-Suite umfasst deshalb nun 54 von 54 ausführbaren Tests;
der zusätzliche Regressionstest beweist die Ablehnung eines CRLF-Shellskripts.
Alle Shellskripte des freigegebenen Quellbaums sind LF-only.
