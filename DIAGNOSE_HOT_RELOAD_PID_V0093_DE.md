# Tiefendiagnose Hot-Reload und PID-Ausgaben v0093

## Ergebnis

Die beobachteten Fehler besitzen vier voneinander getrennte, aber aufeinander
wirkende Ursachen:

1. Der lokale Config-Sync verglich beim Start den Fingerprint der gesamten
   Live-Konfiguration mit dem lokalen Dateikandidaten. Bereits normale,
   sachfremde Laufzeit„nderungen wurden deshalb als Konflikt der lokalen
   nderung interpretiert.
2. Der Persistenzschutz kannte den Fingerprint des vom File-WAL bereits
   akzeptierten Kandidaten nicht. Wenn der Live-Heap nach dem Apply schon eine
   h”here Revision besaá, wurde die anschlieáende Materialisierung f„lschlich
   als Lost Update abgewiesen.
3. Zwischen der Konvergenzprfung und `request_action_plan` existierte ein
   kleines Zeitfenster. Wurde der Plan dort bereits angewendet, kam
   `NO_CHANGE` ohne Message-ID zurck. Der File-Sync wertete dies als Fehler,
   obwohl die Zielwerte schon aktiv waren.
4. Ein einmal aufgebauter PID-Controller wurde nur ber State-ID und Typ
   wiederverwendet. nderungen an `pid_config_id`, `kp`, `ki`, `kd`, Limits,
   Sample-Zeit oder Hysterese „nderten die Config, aber nicht das bereits
   gecachte Controller-Objekt. Der alte Regler rechnete deshalb bis zum
   Neustart weiter.

v0093 behebt alle vier Codeursachen minimal-invasiv. Zus„tzlich erh„lt nur
Sensor 5 des Controllers `62671112-766c-59ed-b0ff-30a49d48f747` das in der
Vergleichskonfiguration vorhandene, im Release v0092 jedoch fehlende
`profile_id: 1`.

## Analysierte Belege

- vollst„ndiger Worker `automation_system_v0092`;
- vollst„ndige Control Plane `automation_control_plane_release_v0019_r31`;
- Spezifikation `CODE_AND_ARCHITECTURE_SPEZIFICATION.md`;
- die drei Logauszge `ProblemAnalyse_Output_1.md`,
  `ProblemAnalyse_Output_2.md` und
  `ProblemAnalyse_Output_3_config_„ndert sich_automatisch.md`;
- alle sieben Screenshots aus `_Problem_Analyse_` sowie die vier direkt
  gelieferten Dashboard-Kurven;
- Release-, Test- und Runtime-Konfigurationen einschlieálich der beiden
  separat gelieferten JSON-Varianten.

Die Logklassifikation ergab:

- 2033 Vorkommen von
  `local-first startup proposal conflicts with live config`;
- 318 Vorkommen von
  `live configuration materialization failed`;
- ein tats„chliches Ereignis
  `file proposal was not durably journaled`, das in den zusammengefhrten
  Auszgen zweimal enthalten ist.

Das Muster ist damit kein Parserfehler und kein zuf„lliger Dateisystemfehler.
Es entspricht exakt der Abfolge Konfliktprfung, Commit und nachgelagerter
Materialisierung.

## Ursache 1: falsche Konfliktdom„ne beim lokalen Start

`src/core/config_file_sync.py` verwendete einen Fingerprint der gesamten
Konfiguration, um zu entscheiden, ob der lokale Dateikandidat noch auf seiner
Basis steht. Die Config enth„lt jedoch viele unabh„ngige Tabellen und
Laufzeitwerte. Ein lokaler Plan „ndert nur die in seinen Actions genannten
State-Felder und Parameter.

Folge: Eine unabh„ngige Revision, ein anderer State-Wert oder eine bereits
materialisierte Master-nderung lieá den vollst„ndigen Fingerprint abweichen.
Der lokale Kandidat wurde trotz unver„nderter eigener Basis blockiert. Jeder
erneute Datei-Scan erzeugte denselben Logfehler, bis zuf„llig wieder ein
akzeptierter Zustand beziehungsweise eine neue Baseline entstand.

Patch:

- `_runtime_state_overlay_matches` vergleicht nur den von einer Action
  besessenen `value_id`- und Parameter-Overlay;
- `_runtime_overlay_matches` prft alle Actions eines Plans;
- `_stage_prepared` entscheidet lokale Basis, Zielkonvergenz und Konflikt nur
  noch in dieser fachlich richtigen Dom„ne.

Sachfremde Live-nderungen bleiben damit erhalten und verhindern keinen
zul„ssigen lokalen Hot-Reload.

## Ursache 2: falscher Lost-Update-Alarm bei der Materialisierung

Der vorhandene Guard in `src/library/_generic_data_center.py` akzeptierte als
extern sichtbare Datei nur den zuletzt quittierten oder den gerade aus dem Heap
gerenderten Fingerprint. Im File-WAL-Pfad ist jedoch ein dritter Fingerprint
legitim: der exakt vorbereitete und journalisierte lokale Dateikandidat.

Nach einem erfolgreichen Apply konnte der Heap bereits weitergelaufen sein.
Beim anschlieáenden Schreiben sah der Guard den noch auf Disk vorhandenen
Kandidaten als unbekannte externe nderung und brach fail-closed ab. Das ist
die direkte Quelle von `live configuration materialization failed`.

Patch:

- `_external_config_fingerprint` akzeptiert optional genau einen expliziten
  `expected_external_fingerprint`;
- `save_heap_to_json` prft diesen Wert vor und nach dem atomaren Rendern;
- alle File-WAL-Materialisierungspfade bergeben ausschlieálich den
  Fingerprint ihres eigenen Kandidaten.

Die Sicherheit wird nicht aufgeweicht: Jede andere, sp„ter eingebrachte
Datei„nderung bleibt ein Lost Update und wird weiterhin abgewiesen.

## Ursache 3: `NO_CHANGE`-Zeitfenster

Zwischen Vorprfung und Action-Anforderung kann ein paralleler Pfad denselben
idempotenten Plan bereits committen. Die API antwortet dann korrekt mit
`NO_CHANGE`, aber ohne neue Message-ID. v0092 rollte in diesem Fall zurck und
meldete, der Vorschlag sei nicht dauerhaft journalisiert, obwohl das Ziel im
Live-Heap bereits erreicht war.

Patch:

- nach einer Antwort ohne Message-ID wird die aktuelle Live-Projektion erneut
  gegen den Action-Overlay geprft;
- stimmt sie exakt berein, wird derselbe Plan einmalig ber
  `stage_applied_action_plan` durable nachgefhrt;
- nur wenn auch diese Konvergenz nicht vorliegt, bleibt der bisherige
  fail-closed Rollback aktiv.

## Ursache 4: veralteter PID-Controller-Cache

`ensure_controller_for_state` erkannte bisher nicht, ob sich die
Controller-Konfiguration eines bestehenden States ge„ndert hatte. Der Heap
enthielt neue PID-Werte, der aktive Controller aber weiterhin die alten Gains
und Limits. Das erkl„rt, warum mehrfaches Speichern oder ein Neustart scheinbar
notwendig war und warum ein erfolgreich geladener Parameter nicht sofort die
erwartete Kurve erzeugte.

Patch:

- `_controller_configuration_signature` bildet eine deterministische Signatur
  aus kompletter `control_strategy`, normalisierter referenzierter PID-Config,
  Hystereseparametern und relevanten Aktor-Funktionstypen;
- Setpoints sind bewusst nicht Teil der Signatur und werden weiterhin ohne
  Regler-Neubau dynamisch bernommen;
- bei gleicher Signatur wird der Controller einschlieálich Integralspeicher
  wiederverwendet;
- bei ge„nderter Signatur werden typgebundene Altwerte entfernt und der
  Controller mit den neuen Parametern sauber neu aufgebaut;
- `sync_controller_from_shared` bernimmt dynamischen PID-Speicher nur bei
  identischem Typ und identischer, nichtleerer Signatur.

Damit wirken nderungen an `kp`, `ki`, `kd`, `pid_config_id`, Limits und
Zeitparametern innerhalb der Laufzeit. Gleichzeitig bleibt ein normaler
Setpoint-Update bumpless und verliert nicht unn”tig den Integralspeicher.

## Vergleich Register 524 und 546

Die beiden Kurven sind in v0092 nicht gleich konfiguriert. Sie k”nnen daher
auch bei „hnlicher Beschreibung nicht dieselbe Form besitzen.

### Register 524

- Aktor 3, State 1, Sensor 1;
- Sensorprofil 1: Rohwert `0..65535` wird in `0..1000 øC` skaliert;
- Filter 1: Median 5, Moving Average 5, Lowpass `alpha=0.2`, anschlieáend
  Sensorprofil-Skalierung;
- PID 1: `kp=0.01`, `ki=0.001`, `kd=0.0`;
- Sollwert 554;
- Aktorprofil 2: Reglerausgang `0..10 V` wird auf Register `0..65535`
  abgebildet.

Die geringe Verst„rkung, fehlende D-Komponente und Filterkette erkl„ren die
glatte, langsame Kurve.

### Register 546

- Aktor 5, State 21, Sensor 5;
- im v0092-Release fehlte am Sensor das `profile_id`;
- PID 2: `kp=0.6`, `ki=0.3`, `kd=0.15`;
- Sollwert 200;
- dasselbe Aktorprofil 2 wie bei Register 524.

Ohne Sensorprofil gelangte der zuf„llige Modbus-Rohwert effektiv in einer
anderen Gr”áenordnung in den Regler. Gleichzeitig reagiert PID 2 um ein
Vielfaches aggressiver und besitzt eine D-Komponente. Bei unabh„ngigen,
zuf„lligen Sprngen erzeugt die Ableitung starke Wechsel und h„ufige
S„ttigung. Das sichtbare Plateau bei ungef„hr `65.5` entspricht dem
Register-Maximum `65535`, das das Dashboard in Tausendern darstellt. Die
Screenshots sind damit konsistent mit den Worker-Writes und kein reiner
Darstellungsfehler.

Die Abbildungen lauten:

```text
Sensorprofil 1: engineering = raw / 65535 * 1000
Aktorprofil 2:  register    = control_output / 10 * 65535
Dashboard:      plotted     = register / 1000
```

Ein Threshold von 800 gl„ttet diese Kurve nicht. Der Threshold entscheidet
ber Regel-/State-Aktivierung; die Kurvenform entsteht aus Eingangssignal,
Skalierung, Filterung, PID-Gains, Sample-Zeit, S„ttigung und Aktorprofil.

Auch nach der Profilkorrektur werden 524 und 546 bei einem Simulator mit
unabh„ngigen Zufallswerten nicht identisch. Fr einen echten A/B-Vergleich
mssen beide Regler denselben deterministischen Sensor-Trace, dieselbe
Filterkette, dieselbe PID-Config, denselben Sollwert und denselben Startzustand
erhalten. Erst dann ist eine Kurvendifferenz ein Controllerfehler.

## Bewusst nicht automatisch ge„ndert

Die gelieferten Quellen widersprechen sich beim State-21-Sollwert:

- Release v0092: `target_temp_c=200`, PID 2;
- beigefgte Haupt-JSON: `target_temp_c=200`, PID 2;
- beigefgte Test-JSON: `target_temp_c=600`, PID 1;
- Problembeschreibung: Threshold beziehungsweise Zielwert 800.

v0093 erfindet deshalb keinen fachlichen Sollwert und tauscht PID 2 nicht
stillschweigend gegen PID 1. Diese Werte k”nnen nun zuverl„ssig zur Laufzeit
gesetzt werden. Das fehlende Sensorprofil ist dagegen eindeutig und wurde
minimal korrigiert.

## Regressionen

Die neuen Tests decken genau die vier Fehlerklassen ab:

- Start mit sachfremdem Live-Drift akzeptiert einen gltigen lokalen Overlay;
- ein `NO_CHANGE`-Race wird durable nachgefhrt;
- ein exakt bekannter File-WAL-Kandidat darf nach fortgeschrittenem Heap
  materialisiert werden;
- eine danach auftretende fremde Datei„nderung bleibt fail-closed;
- Setpoint-nderung erh„lt Controller und Integralspeicher;
- Gain-/Profil„nderung baut den PID-Controller neu auf;
- Migration mit gleicher Signatur darf dynamischen PID-Speicher bernehmen;
- der All-Actuator-Test wartet deterministisch auf alle asynchronen Captures.

Die Architekturregeln bleiben eingehalten: rein funktionaler Aufbau, keine
Klassen, keine Decorators und keine Lambdas. Die Anzahl heuristischer
Code-Standard-Funde in den drei ge„nderten Produktionsdateien ist gegenber
v0092 unver„ndert; die neuen Helfer und Tests erzeugen keine zus„tzlichen
Funde.

## Betriebswirkung

Nach dem Rollout von v0093 gilt:

1. Lokale State-/Parameter„nderungen k”nnen w„hrend des Betriebs bernommen
   und persistent materialisiert werden.
2. Parallel laufende, sachfremde Config-nderungen werden nicht berschrieben.
3. PID- und Sensor-/Aktor-Konfigurations„nderungen invalidieren den betroffenen
   Controller deterministisch, ohne andere States neu aufzubauen.
4. Ein Setpoint-Update bleibt ein leichter Laufzeitupdate.
5. Externe unbekannte Dateiedits und echte Revision-Konflikte bleiben
   fail-closed.

Fr den Produktionsbetrieb sollte der Worker weiterhin ausschlieálich ber
`run_worker.sh` gestartet werden. Vor dem Umschalten ist die aktive JSON-Datei
zu sichern; nach dem Test sind Config-Fingerprint, File-WAL-Status und die
Register 524/546 gemeinsam zu prfen.
