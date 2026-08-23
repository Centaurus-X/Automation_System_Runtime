# Publication technical assessment

Assessment date: 2026-08-23
Release identity: `automation_system_v0101` / `0.101.0-v0101`

## Executive diagnosis

The release is a substantial, coherent worker runtime rather than a minimal
demo. Its strongest engineering quality is the consistency of its ownership
model: node-local authority, canonical storage contracts, durable convergence,
hardware-free cross execution and owner-side readback all reinforce the same
failure philosophy.

The appropriate public maturity label is **pre-production, technically
validated**. The release contains meaningful test and live-integration
evidence, but it does not contain independent certification, complete optional
dependency pinning, reproducible quantitative performance baselines or a
turnkey production platform.

## Integrity results

| Check | Result |
|---|---:|
| Supplied ZIP SHA-256 | `c1e62ead807817930cd8323852e0ba421d6d70baa336b37f89d86692f1939467` |
| ZIP sidecar match | PASS |
| Supplied marketing playbook SHA-256 | `9fbe8bf009dc77bf44607ac16e6205ed0cf9d73f84c9cd7f7adc85500075d92f` |
| Playbook sidecar match | PASS |
| Unsafe ZIP paths | 0 |
| Extracted runtime files | 192 |
| Extracted bytes | 3,040,635 |
| Internal manifest entries | 191 |
| Internal manifest verified before publication | 191/191 PASS |
| Python parse/compile scan | 129 files, 0 syntax errors |

The original ZIP and sidecar are published unchanged as GitHub release assets.
Repository-facing documentation was added around the source tree; runtime
source, configuration and tests were not rewritten for marketing.

## Codebase profile

| Area | Python files | Lines |
|---|---:|---:|
| Runtime source (`src`) | 65 | 57,827 |
| Regression tests (`test`) | 56 | 11,377 |
| Project tools (`_tools`) | 8 | 4,624 |
| **Total** | **129** | **73,828** |

Additional material includes 54 executable `test_*.py` files, six shell
scripts, seven active/legacy configuration JSON files, the YAML profile system,
historical diagnoses, release evidence and an SPDX 2.3 SBOM.

## Architecture findings

### Strengths

1. **Authority is explicit.** The persisted fencing tuple combines authority,
   scope and execution epochs with a fingerprint, reducing ambiguous rollback
   and stale-owner behavior.
2. **The local control path is independent.** Master and MQTT failures do not
   automatically stop local rule/FSM/automation execution.
3. **Configuration has a single mutation boundary.** MQTT proposals and file
   edits both become canonical operations authorized by SEM before activation.
4. **Convergence is durable.** SQLite journals and mirror outboxes preserve
   asynchronous retry and evidence across restarts.
5. **Cross execution respects ownership.** Cross workers cannot carry raw bus
   addresses or write hardware directly.
6. **Security controls are concrete.** mTLS URI identity, key-mode checks,
   HMAC, expiry, dedupe, packet limits and retained-command rejection are
   implemented and tested.
7. **Release engineering is visible.** Contracts, release notes, validation,
   checksums, archive builder and SBOM provide a useful audit trail.

### Maintainability observations

- Several core modules are large: the distributed coordinator exceeds 4,300
  lines, the automation engine 3,900 and the generic type library 3,200. This
  concentrates review risk even though functional boundaries are documented.
- The bundled generic standards checker reports 893 warning/advisory findings:
  627 missing broad `try/except` blocks under its configured heuristic, 101
  branch-count warnings, 65 function-length warnings, 54 non-English-comment
  advisories and 46 nesting-depth warnings. These are maintainability signals,
  not syntax errors or proof of runtime defects.
- The release-specific architecture hard-rule test passes and separately
  enforces the project's intentional ban on classes, decorators, lambdas and
  type annotations.
- Historical documents are valuable as an audit trail but make current versus
  superseded guidance harder to identify. The publication README now routes
  readers to current v0101 documents first.

## Validation results

### Release evidence supplied with the artifact

- 54/54 executable Worker test files recorded as passing.
- Linux clean-room rollout on CPython 3.14.6t.
- POSIX, OpenSSL/mTLS and runtime gates recorded as passing.
- MQTT admission, heartbeat and mirror convergence recorded.
- Real Modbus simulator sensor and readback-confirmed actuator paths recorded.

### Publication-host verification

- External ZIP and playbook hashes: PASS.
- Internal 191-entry manifest: PASS before documentation changes.
- Python compilation with an external cache: PASS for all 129 Python files.
- Unit/contract tests: the large majority passed directly on Windows.
- `unit_test` and `standalone` preflight: PASS when the documented cache
  environment is established before interpreter startup.
- TLS smoke test could not execute locally because the Windows host lacked an
  `openssl` executable.
- Optional S7 integration tests correctly reported `SKIP` without
  `python-snap7`.
- Two runtime-heavy tests did not terminate in the non-target Windows sandbox;
  Linux CI is the authoritative publication gate.

The Windows limitations are recorded rather than presented as successful
Linux validation. GitHub Actions runs the repository's native `test_all.sh` on
Ubuntu after publication.

## Dependency and SBOM diagnosis

The core requirements file pins only `PyYAML==6.0.3`; the S7 supplement pins
`python-snap7==3.1.0`. Adapter code can additionally load `pymodbus`, `asyncua`,
`pycomm3`, `xknx` and an optional `s7commplus` backend. Certificate tooling can
use `cryptography`, while metrics can optionally use `psutil` and `radon`.

Those optional packages are not all pinned and are not all represented in the
supplied SBOM. The SBOM also declares the runtime package license as
`NOASSERTION`. The repository license resolves the public project terms, but a
future release should generate a profile-aware SBOM containing every selected
backend and exact version.

## Performance diagnosis

The design has strong qualitative performance properties: local critical-path
independence, connection ownership, batching, event priorities and configurable
worker pools. It does not provide reproducible p95/p99 latency, jitter,
throughput, queue high-water or resource-capacity results. The included
free-threading benchmark is a CPU microbenchmark and cannot substantiate
field-control performance.

Before production use, benchmark the exact node profile, interpreter, adapter,
field device, network and persistence settings. See `docs/PERFORMANCE.md`.

## Deployment diagnosis

### Ready

- Fail-closed worker preflight.
- Linux launcher with external bytecode cache.
- Atomic mTLS identity installer.
- systemd service manager with controlled takeover and shutdown.
- Immutable artifact/hash workflow and explicit runtime-state separation.

### Requires deployment-specific work

- Replace release-era service paths and user names.
- Commission broker ACLs, certificates and environmental secrets.
- Pin and qualify every selected protocol backend.
- Define queue/backpressure capacity and log retention.
- Provide real monitoring, alerting, backup and restore procedures.
- Complete plant-specific safety analysis and interlocks.
- Measure timing and recovery on target hardware.
- Integrate or deploy the separate master/control plane.

## Security diagnosis

The runtime demonstrates a sound defense-in-depth direction, especially at the
authority, transport and hardware-owner boundaries. No claim is made that this
publication constitutes a full penetration test or formal threat model.

Particular residual risks are operational: unsafe profile selection, incorrect
plant limits, optional backend vulnerabilities, unbounded queues under overload,
certificate lifecycle mistakes and treating cross-node coordination as a
hard-real-time safety loop.

## Publication decision

**Publishable with explicit maturity and limitation labels.** The source and
release evidence support a professional public engineering presentation. The
publication should not use `production-ready`, `enterprise-grade`, `real-time`
or compliance claims until independent, target-specific evidence exists.

Recommended next engineering milestones:

1. Run the full suite on every release in Linux CI and on CPython 3.14t where a
   runner is available.
2. Produce profile-specific locked dependencies and SBOMs.
3. Establish reproducible end-to-end performance baselines.
4. Split the largest modules along existing responsibility boundaries without
   changing authority semantics.
5. Add a documented threat model and independent security review.
6. Add hardware-in-the-loop qualification for each supported adapter profile.
