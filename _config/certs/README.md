# MQTT certificate directory

Place private-CA and optional mutual-TLS material here and reference it from
`_config/application/app_config.yaml`.

Recommended files:

- `mqtt_ca.crt`: broker CA certificate or CA chain.
- `worker_fn-01.crt`: certificate for this node (mTLS).
- `worker_fn-01.key`: private key for this node (mTLS).

No certificate or private key is shipped in the release ZIP. Restrict the
injected `worker_fn-01.key` to the runtime user
(for example mode `0600`) and inject broker/HMAC secrets through environment
variables or a secret manager.

Do not copy these files manually into a new release. Use the validating,
idempotent installer from the project root:

```sh
./install_mqtt_identity.sh \
  --source-dir "$HOME/.automation-cert-import" \
  --site-id site-main \
  --node-id fn-01

./run_worker.sh --preflight
```

The source directory may contain the Control Plane names `ca.crt`, `mqtt.crt`
and `mqtt.key`, or the installed Worker names listed above. The installer
checks certificate purpose, CA chain, key match, URI identity and permissions
before replacing the current identity.
