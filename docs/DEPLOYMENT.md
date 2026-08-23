# Deployment guide

## Supported boundary

The documented deployment target is Linux. The release evidence records a
clean-room run on free-threaded CPython 3.14.6t. CPython 3.12 is useful for the
portable regression suite and CI, but is not the recorded production
qualification interpreter.

This repository contains the worker runtime. It does not include a turnkey
master/control-plane deployment, container image or Kubernetes package.

## 1. Prepare the host

Required tools:

- Linux user with a dedicated runtime directory.
- Python 3.12+ for tests; CPython 3.14t for the qualified target path.
- `openssl`, POSIX shell tools and `systemd` when installing a service.
- Network access only to the explicitly required broker and field endpoints.

```bash
git clone https://github.com/Centaurus-X/Automation_System_Runtime.git
cd Automation_System_Runtime

python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
chmod 0755 run_worker.sh install_mqtt_identity.sh test_all.sh
```

Install optional protocol backends only for adapters selected by your node
configuration. Their versions must be qualified and recorded by the deploying
organization; the v0101 snapshot pins only core PyYAML and optional
`python-snap7`.

## 2. Verify the source and tests

```bash
./test_all.sh
```

The test runner performs shell syntax checks, Python compilation, unit-test and
standalone preflight checks, all 54 executable regression files and a private
key scan.

The byte-exact release artifact can be checked separately:

```bash
sha256sum --check automation_system_v0101.zip.sha256
```

Expected archive digest:

```text
c1e62ead807817930cd8323852e0ba421d6d70baa336b37f89d86692f1939467
```

## 3. Select and review a profile

Never start a hardware-enabled profile before reviewing:

- `_config/application/app_config.yaml`
- the selected node V9 JSON file
- the selected cross V8 JSON file, if enabled
- controller ownership and allowlists
- field endpoints and credentials
- safety limits and interlocks
- persistence paths and permissions

For a side-effect-free environment check:

```bash
AUTOMATION_PROFILE=standalone ./run_worker.sh --preflight
```

The packaged `standalone` and `worker_simulation` profiles target the
allowlisted Modbus simulator at `192.168.0.3:5020`. Change or isolate this
configuration before any real deployment.

## 4. Install the MQTT mTLS identity

Distributed operation requires a node-specific client identity. Private key
material must come from a secure directory outside the repository.

```bash
./install_mqtt_identity.sh \
  --source-dir /secure/provisioning/fn-01 \
  --site-id site-main \
  --node-id fn-01
```

The installer verifies:

- certificate and private-key parseability;
- certificate validity and CA chain;
- public/private key match;
- exact URI SAN `urn:automation:<site>:worker:<node>`;
- atomic installation and private-key mode `0600`.

Then run:

```bash
AUTOMATION_PROFILE=worker_simulation ./run_worker.sh --preflight
```

Use a broker ACL that grants each node read access only to its command topics
and write access only to its status, event and ACK topics.

## 5. Start manually

```bash
AUTOMATION_PROFILE=standalone ./run_worker.sh
```

or, after mTLS and broker configuration:

```bash
AUTOMATION_PROFILE=worker_simulation ./run_worker.sh
```

`run_worker.sh` must remain the Linux entry point. It configures the external
Python bytecode-cache policy before Python starts and runs the fail-closed
preflight.

## 6. Install as a systemd service

The supplied service manager contains release-era defaults. Always override
`WORKER_DIR`, service user, state path and profile for the target host.

```bash
chmod 0700 _tools/worker_service_manager.sh

SERVICE_USER=automation \
WORKER_DIR=/opt/automation/Automation_System_Runtime \
STATE_DIR=/var/lib/automation-runtime/site-main/fn-01 \
AUTOMATION_PROFILE=worker_simulation \
./_tools/worker_service_manager.sh --install
```

Review the generated unit before enabling it, then:

```bash
SERVICE_USER=automation \
WORKER_DIR=/opt/automation/Automation_System_Runtime \
STATE_DIR=/var/lib/automation-runtime/site-main/fn-01 \
AUTOMATION_PROFILE=worker_simulation \
./_tools/worker_service_manager.sh --enable-now
```

The service uses `Restart=on-failure`, `KillMode=control-group` and a staged
preflight. Production operators should integrate logs, retention, monitoring
and backup with their platform standards.

## Upgrade and rollback

1. Download the exact release artifact and verify its sidecar hash.
2. Extract to a new immutable versioned directory.
3. Preserve user-managed configuration, identity and runtime journals.
4. Run tests and profile preflight.
5. Stop the old worker cleanly.
6. Switch the service to the new version and start it.
7. Verify node admission, heartbeat, controller ownership and mirror convergence.
8. Retain the previous version for rollback, but never combine an old binary
   with unreviewed new contracts or reset a journal against a live registry.

A master mirror is not an implicit rollback source. Older state requires the
explicit authority-transfer or disaster-restore path.

## Production checklist

- [ ] Exact archive and manifest hashes verified.
- [ ] Target interpreter and optional backend versions recorded.
- [ ] All tests and selected-profile preflight pass.
- [ ] Node/cross contracts and ownership reviewed.
- [ ] No example endpoints can reach unintended equipment.
- [ ] Safety limits and interlocks commissioned for the plant.
- [ ] mTLS identity, key permissions and broker ACL verified.
- [ ] Runtime/state directories have least-privilege ownership.
- [ ] Queue, CPU, memory, disk and timing capacity measured.
- [ ] Failure, reconnect, readback, shutdown and rollback drills completed.
- [ ] Monitoring, backup and incident-response ownership assigned.
- [ ] Independent safety/security assessment completed where required.
