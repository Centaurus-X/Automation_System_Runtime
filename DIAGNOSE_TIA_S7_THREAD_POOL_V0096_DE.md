# Tiefendiagnose TIA/S7 und Generic Thread Pool – v0096

## Ergebnis

`automation_system_v0096` erweitert den eingefrorenen Stand v0095 an zwei
klar begrenzten Architekturstellen:

1. `plc_s7` ist ein gleichwertiger Hardware-Transport neben `plc_modbus` und
   `plc_opcua`.
2. Der generische Thread-Pool besitzt stabile Prioritäten, Queue- und
   Laufzeit-Timeouts, explizite Cancellation sowie ereignisgesteuertes
   Manager-Wakeup.

Die bestehende lokale Regelpipeline, Safety-Logik, V9-Konfigurationsautorität,
Connection Registry und der Controller-Thread bleiben die kanonischen
Ausführungspfade. Es existiert kein zweiter S7-Sonderpfad.

## 1. Architekturdiagnose

### 1.1 Integrationskante

Der Adaptervertrag bleibt vollständig erhalten:

- `connect`
- `disconnect`
- `execute`
- `health_check`
- `capabilities`

`src/adapter/_adapter_interface.py` kennt `plc_s7` als Protokoll und akzeptiert
für diesen Transport typisierte Stringadressen. Die Factory wird über
`src/adapter/_connection_registry.py` registriert. Der Connection-Thread baut
den vorhandenen Request-/Response-Kanal auf; der Controller-Thread verwendet
ihn unverändert.

Die S7-spezifische Transportidentität erweitert den Connection-Key nur um
`rack` und `slot`. Modbus- und OPC-UA-Keys bleiben bytegenau in ihrer bisherigen
Form. Damit können mehrere logische S7-Endpunkte auf derselben IP und demselben
Port kollisionsfrei betrieben werden.

### 1.2 Timing

`PROTOCOL_TIMING_DEFAULTS` enthält:

| Feld | Wert |
|---|---:|
| Protokoll | `plc_s7` |
| Zyklus | `0.1 s` |
| Mindestabstand | `0.005 s` |
| Deterministisch | `false` |

Diese Werte sind konservative Startwerte. Sie sind keine Zusicherung einer
PLC-Zykluszeit. Die reale Last muss gegen CPU, Netz, PDU-Größe und Anzahl der
Variablen kommissioniert werden.

## 2. Adressmodell

### 2.1 Unterstützte Bereiche

- Datenbaustein: `DB<n>.<byte>:<type>`
- Merker: `M<byte>:<type>`
- Eingang: `I<byte>:<type>` beziehungsweise Siemens-Alias `E`
- Ausgang: `Q<byte>:<type>` beziehungsweise Siemens-Alias `A`
- Bit: zusätzlicher Bitindex `0..7`, Typ zwingend `BOOL`

Akzeptierte Beispiele:

```text
DB10.4:REAL
DB10.DBD4:REAL
DB10.DBX8.2:BOOL
M20:WORD
MX10.5:BOOL
I4:INT
Q12:DINT
DB20.0:STRING[40]
DB20.64:WSTRING[16]
DB20.128:BYTES[32]
```

Der Parser normalisiert jede Adresse auf eine kanonische Repräsentation.
Marker und Datentyp müssen in ihrer Größe zusammenpassen. Unvollständige,
mehrdeutige oder außerhalb der S7-Grenzen liegende Adressen werden vor dem
Netzwerkzugriff abgewiesen.

### 2.2 Datentypen

Implementiert sind:

- `BOOL`, `BYTE`, `USINT`, `SINT`, `CHAR`
- `WORD`, `UINT`, `INT`
- `DWORD`, `UDINT`, `DINT`, `REAL`, `TIME`, `TOD`
- `DATE`
- `LWORD`, `ULINT`, `LINT`, `LREAL`
- `STRING[n]`, `WSTRING[n]`, `BYTES[n]`

Numerische S7-Werte werden im Siemens-Netzwerkformat Big Endian kodiert.
`STRING` und `WSTRING` berücksichtigen ihre jeweiligen S7-Längenheader.
Nicht implementierte Typen, insbesondere das historische acht Byte große
`DATE_AND_TIME`/`DT`, werden fail-closed abgewiesen und nicht auf einen
ähnlich benannten Typ verkürzt.

### 2.3 Schreibgrenzen

- Der Eingangsbereich `I` ist schreibgeschützt.
- Ein `BOOL`-Write ist Read-Modify-Write und bewahrt die übrigen Bits.
- Werte außerhalb des kodierbaren Typbereichs erzeugen einen Fehler.
- Block- und Einzelzugriffe benutzen dieselben Parser- und Codec-Funktionen.

## 3. Kommunikation und Batch-I/O

Der stabile Backend-Pfad verwendet `python-snap7==3.1.0` über ISO-on-TCP
(RFC 1006, Standardport 102). `rack`, `slot`, `port` und optionale
python-snap7-Verbindungsparameter werden aus dem Controller-`comm`-Block
abgeleitet.

Sensoren eines S7-Controllers werden im Controller-Thread zu Batch-Anfragen
zusammengefasst. Der Adapter begrenzt `read_multi_vars` hart auf 20 Variablen
pro Roundtrip. Größere Mengen werden stabil in mehrere Batches zerlegt. Wenn
der Backend-Batch fehlschlägt und
`s7_batch_fallback_sequential` nicht deaktiviert ist, erfolgt ein kontrollierter
Einzelzugriff; der Fehler bleibt in den Adaptermetriken sichtbar.

Batching verändert weder Sensorreihenfolge noch Zuordnung. Jedes Ergebnis
enthält weiterhin die kanonische Adresse und den dekodierten Wert.

## 4. Health und Betriebszustand

`health_check` kombiniert:

- lokalen Adapter-Verbindungszustand,
- Backend-Verbindungszustand,
- `get_cpu_state`,
- Backend- und Adressierungsmetadaten,
- kumulierte Fehler- und Timingmetriken.

`RUN` und `STOP` sind erreichbare CPU-Zustände. `STOP` bedeutet nicht
automatisch Verbindungsverlust, bleibt aber für Monitoring und Freigabelogik
explizit sichtbar. Ein nicht erreichbarer oder inkonsistenter Client liefert
einen negativen Health-Status.

## 5. TIA- und Sicherheitsgrenzen

### 5.1 Optimierter Bausteinzugriff

Absolute DB-Adressen benötigen in TIA Portal nicht optimierte Datenbausteine.
`optimized_block_access: true` wird für den absoluten S7-Pfad abgewiesen.
Optimierte DBs benötigen OPC UA oder einen nachweislich symbolischen,
S7CommPlus-fähigen Backend-Pfad.

### 5.2 Zugangsdaten

Inline-Secrets werden rekursiv erkannt und abgewiesen. Dazu gehören unter
anderem `password`, `private_key`, `key_data`, `token` und `secret`.

Zulässige Referenzen sind:

- `security.password_env`
- `security.credential_ref` mit injiziertem Credential-Provider
- Dateipfade für CA, Client-Zertifikat und privaten Schlüssel

Die V9-Fachkonfiguration enthält niemals geheime Werte.

### 5.3 Security-Modi

- `classic`: stabiler python-snap7/S7comm-Pfad.
- `legacy_password`: klassischer Pfad mit extern aufgelöstem Session-Passwort,
  sofern der konkrete Client dies unterstützt.
- `secure`: expliziter S7CommPlus/TLS-Backend-Pfad. Peer-Verifikation ist
  standardmäßig aktiv. Fehlender Backend-Provider, fehlende CA-Datei oder
  unvollständiges Client-Zertifikatspaar blockieren die Verbindung.

Damit wird keine TLS-Fähigkeit von python-snap7 vorgetäuscht. Die sichere
Variante besitzt eine klare Provider-Grenze und fällt ohne geeignete
Implementierung geschlossen aus.

## 6. TIA-Openness-Konverter

`_tools/convert_tia_openness_xml.py` verarbeitet einen einmaligen TIA-
Openness-XML-Tag-Export.

Eigenschaften:

- maximale Eingabegröße,
- Ablehnung von DTD- und Entity-Deklarationen,
- Übersetzung unterstützter TIA-Skalartypen und Byte-Arrays,
- Richtungsableitung für `I`/`E` und `Q`/`A`,
- konfigurierbare Namensmuster für Sensoren und Aktoren,
- fail-closed-Verhalten für mehrdeutige DB-/M-Tags,
- kanonische Adressvalidierung über denselben Runtime-Parser,
- deterministische V9-Fragmentausgabe,
- optionaler Merge in eine vollständige Basis-V9-Konfiguration,
- vollständige Type-Library-Validierung nach dem Merge,
- Quell-Hash und Warnhinweis für optimierten Bausteinzugriff,
- atomarer Output.

Der Konverter erfindet keine Hardware-Richtung für DB- oder Merker-Tags.
Solche Tags benötigen ein eindeutiges Kommentar-/Namensmuster oder
`--default-direction`.

## 7. Thread-Pool-Diagnose

### 7.1 Vorherige Grenzen

Der v0095-Pool hatte drei betriebliche Lücken:

- Ein dauerhaft blockierender Job belegte einen Worker bis zum Shutdown.
- Der Manager prüfte zyklisch im Abstand von `0.25 s`.
- Jobs besaßen keine Priorität.

### 7.2 Neue Queue-Semantik

Die interne Queue ist eine stabile Priority Queue. Der Sortierschlüssel ist:

```text
(-priority, monotone_sequence, job)
```

Eine höhere Zahl bedeutet höhere Priorität. Gleiche Prioritäten behalten
FIFO-Reihenfolge.

Standardprioritäten:

| Pipeline | Priorität |
|---|---:|
| FSM | 300 |
| Automation | 200 |
| Cross-I/O/FSM | 150/250 |
| Rule Engine | 100 |

Prioritäten verändern keine Safety-Prüfung und keine fachliche Reihenfolge
innerhalb derselben Prioritätsklasse.

### 7.3 Wakeup

Der Manager wartet auf einer `threading.Condition`. Submit, Completion,
Cancellation und Shutdown signalisieren die Condition direkt. Damit entfällt
die feste Polling-Latenz; Status-Snapshots weisen
`manager_polling: false` aus.

### 7.4 Timeouts

Es existieren zwei unabhängige Grenzen:

- `queue_timeout_s`: maximale Wartezeit bis zum Start.
- `timeout_s`: maximale Laufzeit ab Jobstart.

Defaults in der Worker-Integration:

| Feld | Wert |
|---|---:|
| Job-Timeout | `30 s` |
| Queue-Timeout | `10 s` |
| maximal isolierte Alt-Jobs | `32` |

Ein Laufzeit-Timeout beendet nicht gewaltsam fremden Python- oder
Bibliothekscode. Der Pool isoliert den alten Runner als daemonisierten,
beobachteten Alt-Job, veröffentlicht ein terminales Timeout-Ergebnis und gibt
den Worker für nachfolgende Jobs frei. Die Anzahl gleichzeitig isolierter
Alt-Jobs ist begrenzt; danach werden weitere riskante Starts abgewiesen.

### 7.5 Cancellation

- Vor dem Start: Job wird terminal als abgebrochen markiert.
- Während der Ausführung: Cancellation ist sofort für den Pool sichtbar.
- Kooperativ: Ein Funktionsparameter aus `cancel_kwarg` erhält ein Event.
- Nicht kooperativ: Der Runner wird wie beim Timeout isoliert.
- Ein expliziter Abbruch gewinnt auch dann, wenn die Funktion zeitgleich
  regulär zurückkehrt.

Die öffentliche Job-Registry liefert Status und terminale Resultate ohne
zusätzliche Objektmodelle.

### 7.6 Shutdown

Shutdown signalisiert Condition, Worker und aktive Jobs, wartet nur bis zur
konfigurierten Deadline und bleibt idempotent. Ein fehlgeschlagener Cross-Job
wird weiterhin an die Cross-Pipeline zurückgemeldet, damit fachliche Locks und
In-Flight-Markierungen sicher freigegeben werden.

## 8. Verifikation

Neue Regressionstests:

- `test_tia_plc_adapter.py`
- `test_tia_plc_adapter_emulator.py`
- `test_tia_connection_integration.py`
- `test_tia_openness_converter.py`
- `test_generic_thread_pool_advanced.py`

Der Snap7-Emulatortest verwendet einen echten TCP-Server und prüft DB, M, I,
Q, Batch-Lesen, Schreiben, Readback und Health. Der funktionale Fake-Test
ergänzt CPU-`STOP`, Security-Failures und kontrollierte Batch-Fehler.

Der vollständige Release-Gate umfasst zusätzlich:

- AST- und Compile-Prüfung aller Python-Dateien,
- vollständige Worker-Regressionssuite,
- synchronisierte Contract-Hashes,
- Control-Plane-Static-, Unit- und E2E-Gates,
- deterministische ZIP-/Manifest-Verifikation,
- Fresh-Rollout auf der Testumgebung,
- Remote-Worker- und Cross-E2E.

## 9. Bewusst offene Betriebsgrenzen

- Die sichere S7CommPlus/TLS-Verbindung benötigt einen separat qualifizierten
  Backend-Provider; python-snap7 ist der klassische S7comm-Pfad.
- Optimierte DBs sind nicht absolut adressierbar.
- Ein nicht kooperativer nativer Fremdaufruf kann nicht sicher zwangsweise
  beendet werden; er wird bounded isoliert.
- Produktionsadressen, DB-Layouts, Zugriffslevel und Zertifikate müssen pro
  Anlage kommissioniert werden.

Diese Grenzen sind explizit und fail-closed. Sie werden nicht durch
stillschweigende Fallbacks verdeckt.
