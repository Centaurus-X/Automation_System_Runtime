# Contributing

Thank you for helping improve Automation System Runtime. Changes in an
industrial control runtime can affect authority, safety and hardware ownership,
so small, reviewable and evidence-backed contributions are preferred.

## Before opening a pull request

- Use an issue or Discussion to describe substantial architectural changes.
- Never include credentials, private keys, customer data, plant topology or
  non-public controller information.
- Keep changes within the existing authority and ownership model.
- Add or update executable regression coverage for behavioral changes.
- Update current documentation and the changelog when user-visible behavior
  changes.

## Development setup

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
chmod 0755 run_worker.sh install_mqtt_identity.sh test_all.sh
./test_all.sh
```

Optional adapter dependencies should be installed only for the integration
being tested. Record exact versions and explain why the dependency is needed.

## Project code rules

The functional style is an intentional project contract:

- Do not add classes or object-oriented state containers.
- Do not add decorators.
- Do not add lambdas; use `functools.partial()` for callable binding.
- Do not add type annotations.
- Keep mutable ownership and lock boundaries explicit.
- Use canonical Node V9 / Cross V8 storage contracts.
- Preserve SEM as the single mutation boundary.
- Do not allow cross execution to carry raw hardware addressing.
- Do not turn MQTT or the master into a synchronous local-control dependency.
- Failure behavior must be explicit and fail closed at trust boundaries.

Run the architecture gate directly when changing Python:

```bash
python3 test/test_architecture_hard_rules.py
```

## Pull request checklist

- [ ] Scope is focused and explained.
- [ ] Runtime behavior and failure behavior are documented.
- [ ] Tests cover success, rejection and recovery paths.
- [ ] `./test_all.sh` passes on Linux.
- [ ] No generated caches, logs, runtime databases or secrets are included.
- [ ] Shell files use LF line endings.
- [ ] Documentation and `CHANGELOG.md` are current.
- [ ] Performance claims include reproducible context and measurements.
- [ ] Security/compliance wording avoids unsupported claims.

## Commit and review guidance

Use concise imperative commit subjects and explain the reason, invariant and
validation in the body. Reviewers should be able to answer:

1. Who owns the affected state?
2. What happens when the broker, master, storage or controller fails?
3. Can a stale revision, epoch or task be accepted?
4. Is a commit or hardware write repeatable exactly once?
5. What test proves the intended boundary?

## Licensing of contributions

The public project is distributed under GPL-3.0-only and also offers separate
commercial licensing. No general contributor license agreement is currently
published. Discuss non-trivial contributions with the maintainer before
investing substantial work; an additional written contribution agreement may
be required to preserve the dual-licensing model.

By submitting a pull request you confirm that you have the right to submit the
work and that the contribution may be distributed under the repository's
public GPL-3.0-only terms. This statement is not a copyright assignment.
