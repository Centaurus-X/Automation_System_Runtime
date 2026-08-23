# Release-Validierung v0094

## v0094-Zusatzabnahme

Der additive v0094-Baum wurde isoliert unter dem auf `.80` allein
freigegebenen v0092-Pfad getestet; der produktive v0092-Baum wurde nicht
ueberschrieben. Verwendet wurde `/usr/local/bin/python`, das in der vom Nutzer
vorgegebenen Aktivierung auf Python 3.14.5t zeigt. `sys._is_gil_enabled()` war
`False`.

- offizielle `test_all.sh`: 42/42 bestanden;
- Architektur-Hard-Gate: keine Klassen, Decorators, Lambdas oder Type-Hints;
- State 21 mit 800 und 1800 waehrend laufender Runtime;
- Master-vs.-Local: Master gewinnt, genau ein Warning, null Rollbacks;
- stabile Error-Ursache: 30-Sekunden-Coalescing mit Suppression-Zaehler;
- vollstaendige fachliche V9-Datei-Proposal-Policy;
- GenericDataCenter: 41/41;
- OpenSSL-/mTLS-, MQTT-, Recovery-, Cross- und Shutdown-Gates bestanden.

Der Simulator auf `.03` wurde exakt mit dem vorhandenen `(modbus)`-Environment
aus `/home/philo/modbus_server/modbus_sim_server_` durch
`python modbus_sim_server.py` gestartet. Der echte v0094-Modbus-Adapter auf
`.80` schrieb und las Register 524/546 mit den Paaren 8192/65535,
16384/32768 und 32768/8192. Alle Readbacks waren exakt, die Adaptermetrik
meldete 18 Operationen und 0 Fehler. Anschliessend wurden beide Register auf
ihren Ausgangswert 0 restauriert.

Die geerbte v0093-Basisabnahme folgt unveraendert als Kontext.


## Freigabestatus

Worker-Code, V9-Konfiguration, Hot-Reload-/PID-Regressionen und der
reproduzierbare Paketnachweis sind auf Windows und auf dem Linux-Referenzhost
bestanden. Der vollst�ndige 42er-Lauf wurde direkt aus dem extrahierten ZIP
wiederholt; Archiv- und Manifest-Hash stehen in den erzeugten Sidecars.

## Verifiziertes Delta

- Action-begrenzte Overlay-Konfliktpr�fung f�r lokale File-Proposals;
- exakter, WAL-gebundener Kandidatenfingerprint bei der JSON-Materialisierung;
- durable Nachf�hrung eines bereits konvergierten `NO_CHANGE`-Races;
- deterministische Controller-Konfigurationssignatur und Cache-Invalidierung;
- dynamischer Setpoint-Update ohne unn�tigen Verlust des PID-Speichers;
- Signatur-Fence f�r die Migration dynamischer PID-Zust�nde;
- Sensor 5 am Simulator-Controller mit Temperaturprofil 1;
- adressunabh�ngige PID-/Aktorprofil-Ausgabe f�r Register 524 und 546;
- deterministisches Warten auf alle asynchronen Aktor-Captures.

## Lokale Pr�fung

Umgebung:

- Windows;
- geb�ndeltes Python 3.12.13;
- `PYTHONDONTWRITEBYTECODE=1` und isolierte tempor�re PyYAML-Abh�ngigkeit.

Bestanden:

- `test_config_file_sync.py`;
- `test_config_persistence_receipt.py`;
- `test_runtime_controller_reconfiguration.py`;
- `test_worker_all_local_actuators.py`;
- `test_v9_worker_contract.py` mit 9 V9-Unterpr�fungen;
- `test_architecture_hard_rules.py`;
- alle weiteren plattformunabh�ngigen Worker-Tests.

Die drei unter Windows nicht aussagekr�ftigen Gates wurden nicht umgangen,
sondern verbindlich auf Linux ausgef�hrt: POSIX-Modus `0600`, OpenSSL/mTLS und
Launcher-/Cache-Preflight.

## Linux-Referenzlauf

Host und isolierter Pfad:

```text
philo@192.168.0.80
~/python-skripte_22/__NEW_FILES__/automation_system_v0092/_validation_v0094
```

Runtime:

- Python 3.14.5 no-GIL;
- Aktivierung �ber `~/activate_python314_nogil.sh`;
- kanonischer Runner `bash ./test_all.sh`;
- 42 von 42 ausf�hrbaren Tests bestanden;
- GenericDataCenter: 41 von 41 Unterpr�fungen bestanden;
- Lightweight MQTT mTLS mit realem OpenSSL bestanden;
- Cross-Runtime-POSIX-Recovery bestanden;
- Worker-Identity-, Preflight- und Cache-Policy bestanden;
- V9-Vertrag einschlie�lich Sensor-5-Profil bestanden;
- Controller-Rekonfiguration und adressunabh�ngige Ausgabe bestanden.

Der erste Transfer von Windows verlor erwartungsgem�� Unix-Execute-Bits. Nach
Setzen des Release-Modus `0755` ausschlie�lich im isolierten Testbaum bestand
der kanonische Runner vollst�ndig. Der finale ZIP-Test pr�ft dieselben Modi aus
dem vom Builder erzeugten Archiv erneut.

## Isolierter Live-Modbus-Nachweis

Der konfigurierte Simulator `.03:5020` war zun�chst nicht erreichbar. Das auf
`.03` vorhandene freigegebene Skript konnte dort nicht gestartet werden, weil
kein Virtualenv und systemweit kein `aiohttp` vorhanden war. Entsprechend der
Read/Execute-Policy wurde `.03` nicht ver�ndert und es wurden keine Pakete
installiert.

F�r den Netzwerk-I/O-Nachweis wurde der Simulator schreibgesch�tzt aus seinem
freigegebenen `.03`-Ordner in einen tempor�ren Unterordner des erlaubten
`.80`-Validierungsbaums kopiert. Dort lief er mit der bereits vorhandenen
Python-3.14-no-GIL-Umgebung auf einem isolierten Testport. Danach wurde die
Kopie wieder vollst�ndig entfernt.

Der echte v0094-Modbus-Adapter best�tigte:

- sechs erfolgreiche Sensor-/Aktor-Readbacks;
- Sensor 1 und Sensor 5 lieferten unabh�ngige Zufallswerte �ber den
  `0..65535`-Rohbereich;
- Register 524 und 546 waren ohne Worker zun�chst `0`;
- `2.5 V` wurde als `16384` geschrieben und gelesen, Dashboardwert `16.384`;
- `10.0 V` wurde als `65535` geschrieben und gelesen, Dashboardwert `65.535`;
- `5.0 V` wurde als `32768` geschrieben und gelesen;
- `1.25 V` wurde als `8192` geschrieben und gelesen;
- der `finally`-Pfad restaurierte Register 524 und 546 auf `[0, 0]`.

Damit ist die Skalierungsanalyse der Screenshots live best�tigt. Die Adresse
beeinflusst den Reglerausgang nicht; identische PID-, Messwert- und
Profilbedingungen erzeugen bitgleich identische Registerfolgen. Die realen
Defaultpfade sind dagegen wegen unabh�ngiger Zufallssensoren, unterschiedlicher
PID-Gains, Filterung und der in v0092 fehlenden Sensor-5-Skalierung nicht
gleichartig.

## Architektur- und Qualit�tsgates

- keine Klassen und kein OOP im Produktdelta;
- keine Decorators;
- keine Lambdas;
- keine Type-Hints;
- Kommentare im neuen Testcode ausschlie�lich Englisch;
- keine zus�tzlichen heuristischen Standardsfunde in den ge�nderten
  Produktionsdateien gegen�ber v0092;
- keine Private Keys, PKCS#12-Dateien, Caches, Logs oder Laufzeitjournale im
  Releasebaum;
- v0092 bleibt unver�ndert.

## Paketnachweis

Zwei lokale Builds erzeugten bytegleiches ZIP und Manifest. Sidecar,
ZIP-CRC, interne SHA256SUMS, Pfade und Unix-Modi wurden auf Linux gepr�ft; die
vollst�ndige 42er-Suite bestand aus dem extrahierten Archiv. Die additive
Control Plane v0020-r2 bindet exakt den finalen v0094-ZIP-/Manifest-Hash.
