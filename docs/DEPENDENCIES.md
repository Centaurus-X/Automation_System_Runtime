# Dependency model

## Core runtime

| Dependency | Version | Purpose |
|---|---:|---|
| CPython | 3.12 regression / 3.14t target | Runtime and parallel execution platform |
| PyYAML | 6.0.3 | Application profile loading |

`requirements.txt` is the canonical pinned core dependency file.

## Optional integrations

The adapter modules import their backends only when selected. The v0101
snapshot does not provide a complete lock file for these optional packages.

| Integration | Import/package family | Status in v0101 |
|---|---|---|
| Siemens S7/TIA classic | `python-snap7` | Pinned separately to 3.1.0 |
| Siemens secure/S7CommPlus | `s7commplus` | Optional backend; version not pinned |
| Modbus TCP | `pymodbus` | Optional backend; version not pinned |
| OPC UA | `asyncua` | Optional backend; version not pinned |
| EtherNet/IP | `pycomm3` | Optional backend; version not pinned |
| KNX/IP | `xknx` | Optional backend; version not pinned |
| Certificate generation | `cryptography` or OpenSSL CLI | Optional backend; version not pinned |
| Runtime metrics | `psutil` | Optional enhancement; version not pinned |
| Complexity metrics | `radon` | Optional development metric; version not pinned |

Before enabling an adapter in production:

1. Select one exact backend and version.
2. Verify Python/interpreter compatibility.
3. Review the dependency's security and license metadata.
4. Add it to a profile-specific lock file and SBOM.
5. Run protocol, failure, timing and hardware-in-the-loop tests.
6. Record the resulting evidence with the release.

The root SPDX document describes the core published runtime. It does not claim
that every optional backend above is installed or qualified.
