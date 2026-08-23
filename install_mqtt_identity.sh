#!/bin/sh

# Install and validate the Worker MQTT mutual-TLS identity atomically.

set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
DESTINATION="$PROJECT_ROOT/_config/certs"
SITE_ID="${AUTOMATION_SITE_ID:-site-main}"
NODE_ID="${AUTOMATION_NODE_ID:-fn-01}"
SOURCE_DIRECTORY=""
CA_SOURCE=""
CERT_SOURCE=""
KEY_SOURCE=""
LOCK_DIRECTORY=""
WORK_DIRECTORY=""

usage() {
  printf '%s\n' \
    "Usage:" \
    "  ./install_mqtt_identity.sh --source-dir DIRECTORY [--site-id SITE] [--node-id NODE]" \
    "  ./install_mqtt_identity.sh --ca FILE --cert FILE --key FILE [--site-id SITE] [--node-id NODE]" \
    "" \
    "The source directory may contain either:" \
    "  mqtt_ca.crt, worker_fn-01.crt, worker_fn-01.key" \
    "or:" \
    "  ca.crt, mqtt.crt, mqtt.key"
}

require_argument() {
  option=$1
  value=${2:-}
  if [ -z "$value" ]; then
    printf 'Missing value for %s\n' "$option" >&2
    usage >&2
    exit 2
  fi
}

select_source_file() {
  first=$1
  second=$2
  if [ -f "$first" ]; then
    printf '%s\n' "$first"
    return
  fi
  printf '%s\n' "$second"
}

cleanup() {
  if [ -n "$WORK_DIRECTORY" ] && [ -d "$WORK_DIRECTORY" ]; then
    rm -rf -- "$WORK_DIRECTORY"
  fi
  if [ -n "$LOCK_DIRECTORY" ] && [ -d "$LOCK_DIRECTORY" ]; then
    rmdir -- "$LOCK_DIRECTORY" 2>/dev/null || true
  fi
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --source-dir)
      require_argument "$1" "${2:-}"
      SOURCE_DIRECTORY=$2
      shift 2
      ;;
    --ca)
      require_argument "$1" "${2:-}"
      CA_SOURCE=$2
      shift 2
      ;;
    --cert)
      require_argument "$1" "${2:-}"
      CERT_SOURCE=$2
      shift 2
      ;;
    --key)
      require_argument "$1" "${2:-}"
      KEY_SOURCE=$2
      shift 2
      ;;
    --site-id)
      require_argument "$1" "${2:-}"
      SITE_ID=$2
      shift 2
      ;;
    --node-id)
      require_argument "$1" "${2:-}"
      NODE_ID=$2
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$SITE_ID" in
  *[!A-Za-z0-9._-]*|'')
    printf 'Invalid site ID: %s\n' "$SITE_ID" >&2
    exit 2
    ;;
esac

case "$NODE_ID" in
  *[!A-Za-z0-9._-]*|'')
    printf 'Invalid node ID: %s\n' "$NODE_ID" >&2
    exit 2
    ;;
esac

if [ -n "$SOURCE_DIRECTORY" ]; then
  test -d "$SOURCE_DIRECTORY" || {
    printf 'Source directory does not exist: %s\n' "$SOURCE_DIRECTORY" >&2
    exit 2
  }
  CA_SOURCE=$(select_source_file \
    "$SOURCE_DIRECTORY/mqtt_ca.crt" "$SOURCE_DIRECTORY/ca.crt")
  CERT_SOURCE=$(select_source_file \
    "$SOURCE_DIRECTORY/worker_fn-01.crt" "$SOURCE_DIRECTORY/mqtt.crt")
  KEY_SOURCE=$(select_source_file \
    "$SOURCE_DIRECTORY/worker_fn-01.key" "$SOURCE_DIRECTORY/mqtt.key")
fi

for source_file in "$CA_SOURCE" "$CERT_SOURCE" "$KEY_SOURCE"; do
  test -s "$source_file" || {
    printf 'Required identity file is missing or empty: %s\n' "$source_file" >&2
    exit 2
  }
done

command -v openssl >/dev/null 2>&1 || {
  printf 'openssl is required to validate the MQTT identity\n' >&2
  exit 2
}

EXPECTED_IDENTITY="urn:automation:${SITE_ID}:worker:${NODE_ID}"

openssl x509 -in "$CA_SOURCE" -noout >/dev/null
openssl x509 -in "$CERT_SOURCE" -noout -checkend 0 >/dev/null
openssl pkey -in "$KEY_SOURCE" -noout -check >/dev/null
openssl verify -purpose sslclient -CAfile "$CA_SOURCE" "$CERT_SOURCE" >/dev/null

certificate_public_key=$(
  openssl x509 -in "$CERT_SOURCE" -pubkey -noout |
    openssl sha256
)
private_public_key=$(
  openssl pkey -in "$KEY_SOURCE" -pubout |
    openssl sha256
)

if [ "$certificate_public_key" != "$private_public_key" ]; then
  printf 'MQTT certificate and private key do not match\n' >&2
  exit 2
fi

if ! openssl x509 -in "$CERT_SOURCE" -noout -ext subjectAltName 2>/dev/null |
  tr ',' '\n' |
  sed 's/^[[:space:]]*//' |
  grep -Fx "URI:${EXPECTED_IDENTITY}" >/dev/null; then
  printf 'MQTT certificate URI identity mismatch; expected %s\n' \
    "$EXPECTED_IDENTITY" >&2
  exit 2
fi

umask 077
install -d -m 0700 "$DESTINATION"
LOCK_DIRECTORY="$DESTINATION/.identity-install.lock"
if ! mkdir "$LOCK_DIRECTORY" 2>/dev/null; then
  printf 'Another MQTT identity installation is already active\n' >&2
  exit 3
fi
trap cleanup EXIT HUP INT TERM

WORK_DIRECTORY=$(mktemp -d "${TMPDIR:-/tmp}/automation-worker-identity.XXXXXX")
install -m 0644 "$CA_SOURCE" "$WORK_DIRECTORY/mqtt_ca.crt"
install -m 0644 "$CERT_SOURCE" "$WORK_DIRECTORY/worker_fn-01.crt"
install -m 0600 "$KEY_SOURCE" "$WORK_DIRECTORY/worker_fn-01.key"

if [ -f "$DESTINATION/mqtt_ca.crt" ] \
  && [ -f "$DESTINATION/worker_fn-01.crt" ] \
  && [ -f "$DESTINATION/worker_fn-01.key" ] \
  && cmp -s "$WORK_DIRECTORY/mqtt_ca.crt" "$DESTINATION/mqtt_ca.crt" \
  && cmp -s "$WORK_DIRECTORY/worker_fn-01.crt" "$DESTINATION/worker_fn-01.crt" \
  && cmp -s "$WORK_DIRECTORY/worker_fn-01.key" "$DESTINATION/worker_fn-01.key"; then
  chmod 0644 "$DESTINATION/mqtt_ca.crt" "$DESTINATION/worker_fn-01.crt"
  chmod 0600 "$DESTINATION/worker_fn-01.key"
  printf 'Worker MQTT identity is already installed and valid.\n'
  exit 0
fi

mv -f "$WORK_DIRECTORY/mqtt_ca.crt" "$DESTINATION/mqtt_ca.crt"
mv -f "$WORK_DIRECTORY/worker_fn-01.crt" "$DESTINATION/worker_fn-01.crt"
mv -f "$WORK_DIRECTORY/worker_fn-01.key" "$DESTINATION/worker_fn-01.key"

chmod 0644 "$DESTINATION/mqtt_ca.crt" "$DESTINATION/worker_fn-01.crt"
chmod 0600 "$DESTINATION/worker_fn-01.key"

printf 'Worker MQTT identity installed: %s\n' "$EXPECTED_IDENTITY"
printf 'Run ./run_worker.sh --preflight before starting the Worker.\n'
