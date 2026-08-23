# Release-Validierung v0095

## Abnahmevertrag

Das Release ist nur gültig, wenn:

- alle ausführbaren Worker-Tests auf Python 3.14 no-GIL bestehen,
- der echte SEM-Policy-Routing-Test besteht,
- die Code-/Architektur-Gates keine Klassen, Decorators, Lambdas oder
  Typannotationen finden,
- das ZIP aus zwei aufeinanderfolgenden Builds byteidentisch entsteht,
- das interne SHA256-Manifest vollständig stimmt,
- der Live-Rollout auf `.80` mit der Control Plane r3 auf `.82` konvergiert,
- der Modbus-Simulator auf `.03` unverändert erreichbar bleibt,
- der Dateiablauf `200 -> 400 -> 200` ohne ERROR und ohne Rollback besteht.

Die finalen Prüfergebnisse und Hashwerte werden in `TEST_RESULTS.txt` sowie im
Control-Plane-r3-Validierungsnachweis festgehalten.
