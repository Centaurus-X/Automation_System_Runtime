# Legacy config fixtures

Dieses Verzeichnis isoliert Config-Verträge mit tatsächlichem
`meta.version < 8` vom aktiven Config-Verzeichnis.

V9 Node und V8 Cross bleiben direkt unter `_config/config_data/` die einzigen
aktiven Formate. Dateien aus `_legacy/` dürfen nur über einen expliziten Pfad
und `allow_legacy_import: true` geladen werden. Standardprofile und normale
Config-Auswahl durchsuchen dieses Verzeichnis nicht rekursiv.
