# Security policy

Automation System Runtime interacts with industrial control paths. Please use
coordinated disclosure and do not publish exploit details, credentials or plant
information in a public issue.

## Supported versions

| Version | Security support |
|---|---|
| `0.101.x` | Current public pre-production line |
| Older snapshots | Historical evidence only; no active security support |

Support means best-effort assessment and remediation. It is not an SLA,
certification or warranty.

## Report a vulnerability

Use the repository's **Security → Report a vulnerability** form (GitHub private
vulnerability reporting). Include, where possible:

- affected version, commit and profile;
- affected node/cross contract versions;
- threat scenario and required attacker access;
- safe reproduction steps using a lab or simulator;
- impact on confidentiality, integrity, availability, authority or safety;
- relevant logs with secrets and plant identifiers removed;
- suggested mitigation, if known.

If private vulnerability reporting is temporarily unavailable, contact the
maintainer through the Centaurus-X GitHub profile without sending sensitive
details publicly. Agree on a private channel before sharing the report.

We aim to acknowledge complete reports within seven business days, but this is
a best-effort target and not a contractual response time.

## Scope priorities

High-priority examples include:

- bypass of SEM, safety policy or controller ownership;
- acceptance of stale authority/scope/execution epochs;
- replay or duplicate execution leading to repeated hardware writes;
- cross-runtime access to raw hardware coordinates;
- mTLS identity, HMAC, ACL or command-envelope bypass;
- path traversal, unsafe restore or journal corruption leading to authority loss;
- secret/private-key disclosure from repository or logs;
- unauthenticated remote configuration or actuator execution.

## Safe research rules

- Test only systems you own or are explicitly authorized to assess.
- Use isolated simulators; never test against live production equipment without
  a written safety plan and authorization.
- Do not cause physical movement, environmental change or service interruption.
- Do not retain or disclose personal, customer or plant data.
- Stop immediately if testing could affect real equipment or human safety.

## Deployment security baseline

- Keep private keys and credentials outside the repository.
- Enforce key mode `0600`, certificate expiry monitoring and node-specific URI SANs.
- Use broker ACLs scoped to each node's topics.
- Keep unsafe plaintext MQTT overrides disabled.
- Commission actuator limits and interlocks for the actual plant.
- Pin and monitor every enabled optional protocol dependency.
- Restrict service users, filesystem paths and outbound network destinations.
- Preserve authority journals and follow explicit transfer/restore procedures.
- Monitor rejected commands, stale epochs, queue growth and readback failures.

This policy does not claim that the project has undergone an independent
penetration test or formal safety certification.
