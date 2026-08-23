# -*- coding: utf-8 -*-

# src/library/_generate_ca_signed_certs.py


"""
 =============================================================================
 _GENERATE_CA_SIGNED_CERTS
 =============================================================================

 What it is:
   A unified TLS/PKI library for WebSocket servers.  It consolidates the
   responsibilities of the former files
   ``ws_tls_common.py``, ``generate_ssl_certs.py`` and
   ``generate_self_signed_websocket_cert.py`` and implements the newer
   CA‑signed workflow from ``generate_ca_signed_websocket_certs_mtls.py``.
   The module can be used both as a library and as a CLI utility.

 Why it is needed:
   • Provides a single source of truth for certificate generation.
   • Supports a local root CA, a CA‑signed WebSocket server certificate,
     and optional mTLS client certificates (including PKCS#12 bundles).
   • Performs automatic SAN discovery for:
       - Local hostnames / FQDNs
       - Local interface IP addresses
       - Route‑probe IP
       - Reverse‑DNS for non‑loopback local IPs
   • Supplies an idempotent runtime API that:
       - Validates existing material,
       - Repairs missing side‑cars,
       - Regenerates only when necessary (expiration, host‑mismatch,
         broken chain).
   • Guarantees path resolution relative to the project root and backs up
     existing files before rotation.

 How it works:
   1. Determine the SAN entries required for the server certificate.
   2. Validate any existing certificates, keys, and auxiliary files against
      the desired configuration.
   3. If validation fails, (re)create the root CA, the server certificate,
      and optionally client certificates, persisting them under
      ``DEFAULT_OUT_DIR``.
   4. Export a PKCS#12 archive for each client when requested.
   5. Return a dictionary containing the paths to the generated material
      (``certfile``, ``keyfile``, ``cafile``, ``client_p12`` …) together with
      status information.

 Integration note for ``app_config.yaml``:
   The library is intended to be invoked from a higher‑level interface.
   The recommended call pattern is shown below:

   ```python
   from src.library._generate_ca_signed_certs import (
       DEFAULT_CLIENT_P12_PASSWORD,
       DEFAULT_CLIENT_PREFIX,
       DEFAULT_OUT_DIR,
       DEFAULT_P12_PASSWORD_LENGTH,
       DEFAULT_ROOT_CN,
       DEFAULT_ROOT_PREFIX,
       DEFAULT_SERVER_PREFIX,
       ensure_websocket_server_ssl_material,
   )

   tls_result = ensure_websocket_server_ssl_material(
       enabled=ssl_cfg.get("enabled", False),
       certfile=ssl_cfg.get(
           "certfile",
           "config/ssl/certs/pki/websocket_server.cert.pem",
       ),
       keyfile=ssl_cfg.get(
           "keyfile",
           "config/ssl/certs/pki/websocket_server.key.pem",
       ),
       host=websocket_cfg.get("host", "0.0.0.0"),
       root_cn=ssl_cfg.get("root_cn", DEFAULT_ROOT_CN),
       root_prefix=ssl_cfg.get("root_prefix", DEFAULT_ROOT_PREFIX),
       client_prefix=ssl_cfg.get("client_prefix", DEFAULT_CLIENT_PREFIX),
       client_p12_password=ssl_cfg.get(
           "client_p12_password",
           DEFAULT_CLIENT_P12_PASSWORD,
       ),
       client_p12_password_length=ssl_cfg.get(
           "client_p12_password_length",
           DEFAULT_P12_PASSWORD_LENGTH,
       ),
       extra_dns=ssl_cfg.get("dns") or [],
       extra_ip=ssl_cfg.get("ip") or [],
       logger_obj=logger,
   )
 =============================================================================
"""


# =============================================================================
# MODULE IMPORTS
# =============================================================================

import argparse
import datetime
import ipaddress
import json
import logging
import os
import secrets
import shutil
import socket
import ssl
import subprocess
from functools import partial
from pathlib import Path
from urllib.parse import urlsplit

try:
    from cryptography import x509
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec, ed25519, ed448, padding, rsa
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from cryptography.hazmat.primitives.serialization.pkcs12 import serialize_key_and_certificates
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
    _CRYPTOGRAPHY_IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover - nur Fallbackpfad
    x509 = None
    InvalidSignature = Exception
    hashes = None
    serialization = None
    ec = None
    ed25519 = None
    ed448 = None
    padding = None
    rsa = None
    load_pem_private_key = None
    serialize_key_and_certificates = None
    NameOID = None
    ExtendedKeyUsageOID = None
    _CRYPTOGRAPHY_IMPORT_ERROR = exc


DEFAULT_ROOT_CN = "NIEData Local Root CA"
DEFAULT_ROOT_PREFIX = "root-ca"
DEFAULT_SERVER_PREFIX = "websocket_server"
DEFAULT_OUT_DIR = "config/ssl/certs/pki"
DEFAULT_CLIENT_PREFIX = "ws_client"
DEFAULT_P12_PASSWORD_LENGTH = 24
# An empty default intentionally triggers a cryptographically random password
# in _resolve_client_p12_password().  Deployments may still inject an explicit
# secret, but the release never reuses a built-in credential.
DEFAULT_CLIENT_P12_PASSWORD = None

DEFAULT_COUNTRY = "DE"
DEFAULT_ORGANIZATION = "Local WebSocket"
DEFAULT_ORGANIZATIONAL_UNIT = "Engineering"

DEFAULT_ROOT_DAYS = 3650
DEFAULT_SERVER_DAYS = 825
DEFAULT_CLIENT_DAYS = 825
DEFAULT_ROOT_KEY_SIZE = 4096
DEFAULT_SERVER_KEY_SIZE = 2048
DEFAULT_CLIENT_KEY_SIZE = 2048

WILDCARD_HOSTS = {"", "0.0.0.0", "::", "*"}
logger = logging.getLogger(__name__)


def require_cryptography():
    if x509 is None:
        raise RuntimeError(
            "Die Bibliothek 'cryptography' ist für den CA-signierten Standard erforderlich. "
            f"Importfehler: {_CRYPTOGRAPHY_IMPORT_ERROR}"
        )


def safe_str(value, default=""):
    try:
        if value is None:
            return str(default)
        return str(value)
    except Exception:
        return str(default)


def safe_int(value, default=0):
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def safe_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "on", "enabled"):
            return True
        if normalized in ("0", "false", "no", "off", "disabled"):
            return False
    return bool(default)


def ensure_directory(path_value):
    path_obj = Path(path_value)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj


def write_text_file(path_value, text):
    path_obj = Path(path_value)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    path_obj.write_text(safe_str(text, ""), encoding="utf-8", newline="\n")
    return path_obj


def write_binary_file(path_value, data):
    path_obj = Path(path_value)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    path_obj.write_bytes(data)
    return path_obj


def chmod_private(path_value):
    try:
        os.chmod(path_value, 0o600)
    except OSError:
        pass


def normalize_multi_value(values):
    result = []
    seen = set()

    for item in values or []:
        text = safe_str(item, "").strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)

    return result


def is_ip_address(value):
    text = safe_str(value, "").strip()
    if not text:
        return False

    try:
        ipaddress.ip_address(text)
        return True
    except ValueError:
        return False


def is_wildcard_host(value):
    return safe_str(value, "").strip().lower() in WILDCARD_HOSTS


def normalize_ip_text(value):
    text = safe_str(value, "").strip()
    if not text:
        return ""

    if is_wildcard_host(text):
        return ""

    try:
        return str(ipaddress.ip_address(text))
    except ValueError:
        return ""


def normalize_ip_values(values):
    result = []
    seen = set()

    for item in values or []:
        ip_text = normalize_ip_text(item)
        if not ip_text:
            continue
        if ip_text in seen:
            continue
        seen.add(ip_text)
        result.append(ip_text)

    return result


def looks_like_useful_dns_name(name_value):
    text = safe_str(name_value, "").strip()
    if not text:
        return False

    if is_ip_address(text):
        return False

    lowered = text.lower()
    if lowered in {"localhost", "localhost.localdomain"}:
        return True

    if " " in text:
        return False

    return True


def normalize_dns_name(value):
    text = safe_str(value, "").strip().rstrip(".")
    if not looks_like_useful_dns_name(text):
        return ""
    return text


def normalize_dns_values(values):
    result = []
    seen = set()

    for item in values or []:
        text = normalize_dns_name(item)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)

    return result


def slugify_token(value):
    text = safe_str(value, "").strip().lower()
    if not text:
        return "client"

    result = []
    last_was_sep = False

    for char in text:
        if char.isalnum():
            result.append(char)
            last_was_sep = False
            continue

        if not last_was_sep:
            result.append("-")
            last_was_sep = True

    slug = "".join(result).strip("-")
    return slug or "client"


def parse_ws_uri(uri):
    parts = urlsplit(safe_str(uri, ""))
    scheme = safe_str(parts.scheme, "").strip().lower()
    host = parts.hostname
    port = parts.port

    if not scheme:
        raise ValueError("URI ohne Schema ist ungültig.")

    if host is None:
        raise ValueError("URI ohne Host ist ungültig.")

    if port is None:
        if scheme == "wss":
            port = 443
        elif scheme == "ws":
            port = 80
        else:
            raise ValueError("Nur ws:// oder wss:// werden unterstützt.")

    return scheme, safe_str(host, "").strip(), int(port)


def get_route_probe_ip(family):
    if family == socket.AF_INET:
        target = ("192.0.2.1", 9)
    elif family == socket.AF_INET6:
        target = ("2001:db8::1", 9, 0, 0)
    else:
        return None

    sock = None

    try:
        sock = socket.socket(family, socket.SOCK_DGRAM)
        sock.connect(target)
        local_address = sock.getsockname()[0]
        return safe_str(local_address, "").strip() or None
    except Exception:
        return None
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass


def get_local_ips_from_ip_command():
    ip_binary = shutil.which("ip")
    if ip_binary is None:
        return []

    commands = [
        [ip_binary, "-json", "addr", "show"],
        [ip_binary, "-j", "addr", "show"],
    ]

    for cmd in commands:
        try:
            result = subprocess.run(cmd, check=False, text=True, capture_output=True)
        except Exception:
            continue

        if result.returncode != 0 or not safe_str(result.stdout, "").strip():
            continue

        try:
            payload = json.loads(result.stdout)
        except Exception:
            continue

        collected = []
        for interface in payload or []:
            for addr_info in interface.get("addr_info", []) or []:
                local_value = safe_str(addr_info.get("local"), "").strip()
                if local_value:
                    collected.append(local_value)

        normalized = normalize_ip_values(collected)
        if normalized:
            return normalized

    return []


def resolve_name_to_ips(name_value):
    name_text = normalize_dns_name(name_value)
    if not name_text:
        return []

    collected = []

    try:
        infos = socket.getaddrinfo(name_text, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except Exception:
        return []

    for info in infos or []:
        sockaddr = info[4]
        if not sockaddr:
            continue
        host_text = safe_str(sockaddr[0], "").strip()
        if host_text:
            collected.append(host_text)

    return normalize_ip_values(collected)


def reverse_lookup_names(ip_value):
    ip_text = normalize_ip_text(ip_value)
    if not ip_text:
        return []

    try:
        hostname, aliaslist, _ = socket.gethostbyaddr(ip_text)
    except Exception:
        return []

    values = [hostname]
    values.extend(aliaslist or [])
    return normalize_dns_values(values)


def _append_unique_dns(target, value):
    normalized = normalize_dns_name(value)
    if not normalized:
        return
    if normalized.lower() not in {item.lower() for item in target}:
        target.append(normalized)


def _append_unique_ip(target, value):
    normalized = normalize_ip_text(value)
    if not normalized:
        return
    if normalized not in target:
        target.append(normalized)


def _pick_preferred_primary_ip(ip_values):
    normalized = normalize_ip_values(ip_values)

    preferred_ipv4 = []
    preferred_ipv6 = []
    fallback_loopback = []

    for ip_text in normalized:
        try:
            ip_obj = ipaddress.ip_address(ip_text)
        except ValueError:
            continue

        if ip_obj.is_loopback:
            fallback_loopback.append(ip_text)
            continue

        if ip_obj.version == 4:
            preferred_ipv4.append(ip_text)
        else:
            preferred_ipv6.append(ip_text)

    if preferred_ipv4:
        return preferred_ipv4[0]
    if preferred_ipv6:
        return preferred_ipv6[0]
    if fallback_loopback:
        return fallback_loopback[0]
    return ""


def discover_local_identity(bind_host=None, include_loopback=True, enable_reverse_dns=True):
    dns_values = []
    ip_values = []
    reverse_dns_map = {}

    add_dns_many = partial(_append_unique_dns, dns_values)
    add_ip_many = partial(_append_unique_ip, ip_values)

    hostname = safe_str(socket.gethostname(), "").strip()
    fqdn = safe_str(socket.getfqdn(), "").strip()
    bind_text = safe_str(bind_host, "").strip()

    env_hostnames = [
        os.environ.get("HOSTNAME"),
        os.environ.get("COMPUTERNAME"),
    ]

    for item in ["localhost", hostname, fqdn]:
        add_dns_many(item)

    for item in env_hostnames:
        add_dns_many(item)

    if include_loopback:
        for item in ["127.0.0.1", "::1"]:
            add_ip_many(item)

    for item in get_local_ips_from_ip_command():
        add_ip_many(item)

    for item in [get_route_probe_ip(socket.AF_INET), get_route_probe_ip(socket.AF_INET6)]:
        add_ip_many(item)

    for item in resolve_name_to_ips(hostname):
        add_ip_many(item)

    for item in resolve_name_to_ips(fqdn):
        add_ip_many(item)

    for item in resolve_name_to_ips("localhost"):
        add_ip_many(item)

    if bind_text:
        if is_ip_address(bind_text):
            if not is_wildcard_host(bind_text):
                add_ip_many(bind_text)
            if enable_reverse_dns and not is_wildcard_host(bind_text):
                reverse_names = reverse_lookup_names(bind_text)
                reverse_dns_map[normalize_ip_text(bind_text)] = reverse_names
                for item in reverse_names:
                    add_dns_many(item)
        else:
            add_dns_many(bind_text)
            for item in resolve_name_to_ips(bind_text):
                add_ip_many(item)

    if enable_reverse_dns:
        for ip_text in list(ip_values):
            try:
                ip_obj = ipaddress.ip_address(ip_text)
            except ValueError:
                continue

            if ip_obj.is_loopback:
                continue

            reverse_names = reverse_lookup_names(ip_text)
            if reverse_names:
                reverse_dns_map[ip_text] = reverse_names

            for item in reverse_names:
                add_dns_many(item)

    normalized_dns = normalize_dns_values(dns_values)
    normalized_ip = normalize_ip_values(ip_values)

    primary_ip = _pick_preferred_primary_ip(normalized_ip)
    non_loopback_ips = []

    for ip_text in normalized_ip:
        try:
            ip_obj = ipaddress.ip_address(ip_text)
        except ValueError:
            continue
        if not ip_obj.is_loopback:
            non_loopback_ips.append(ip_text)

    recommended_dns = []
    if primary_ip and reverse_dns_map.get(primary_ip):
        recommended_dns.extend(reverse_dns_map.get(primary_ip) or [])

    if fqdn and fqdn.lower() != "localhost":
        recommended_dns.append(fqdn)

    if hostname and hostname.lower() != "localhost":
        recommended_dns.append(hostname)

    recommended_dns = normalize_dns_values(recommended_dns)

    effective_bind_host = bind_text
    if is_wildcard_host(effective_bind_host):
        effective_bind_host = primary_ip or (recommended_dns[0] if recommended_dns else "") or hostname or "localhost"

    return {
        "hostname": hostname,
        "fqdn": fqdn,
        "bind_host_input": bind_text,
        "effective_bind_host": safe_str(effective_bind_host, "").strip(),
        "primary_ip": primary_ip,
        "non_loopback_ip_values": normalize_ip_values(non_loopback_ips),
        "dns_values": normalized_dns,
        "ip_values": normalized_ip,
        "reverse_dns_map": reverse_dns_map,
        "recommended_dns": recommended_dns,
    }


def choose_common_name(explicit_common_name, bind_host, dns_values):
    explicit = safe_str(explicit_common_name, "").strip()
    if explicit:
        return explicit

    bind_text = safe_str(bind_host, "").strip()
    if bind_text and not is_ip_address(bind_text) and not is_wildcard_host(bind_text):
        return bind_text

    for candidate in normalize_dns_values(dns_values):
        lowered = candidate.lower()
        if lowered not in {"localhost", "localhost.localdomain"} and "." in candidate:
            return candidate

    for candidate in normalize_dns_values(dns_values):
        lowered = candidate.lower()
        if lowered not in {"localhost", "localhost.localdomain"}:
            return candidate

    bind_ip = normalize_ip_text(bind_text)
    if bind_ip:
        return bind_ip

    return "localhost"


def _namespace_value(namespace_or_dict, key, default=None):
    if isinstance(namespace_or_dict, dict):
        return namespace_or_dict.get(key, default)
    return getattr(namespace_or_dict, key, default)


def build_subject_kwargs(args_or_dict, common_name, defaults=None):
    defaults_map = dict(defaults or {})

    country_name = safe_str(
        _namespace_value(args_or_dict, "country", defaults_map.get("country", DEFAULT_COUNTRY)),
        DEFAULT_COUNTRY,
    ).strip() or DEFAULT_COUNTRY

    state_or_province_name = safe_str(
        _namespace_value(args_or_dict, "state", defaults_map.get("state", "")),
        "",
    ).strip()

    locality_name = safe_str(
        _namespace_value(args_or_dict, "locality", defaults_map.get("locality", "")),
        "",
    ).strip()

    organization_name = safe_str(
        _namespace_value(
            args_or_dict,
            "organization",
            defaults_map.get("organization", DEFAULT_ORGANIZATION),
        ),
        DEFAULT_ORGANIZATION,
    ).strip() or DEFAULT_ORGANIZATION

    organizational_unit_name = safe_str(
        _namespace_value(
            args_or_dict,
            "organizational_unit",
            defaults_map.get("organizational_unit", DEFAULT_ORGANIZATIONAL_UNIT),
        ),
        DEFAULT_ORGANIZATIONAL_UNIT,
    ).strip() or DEFAULT_ORGANIZATIONAL_UNIT

    common_name_text = safe_str(common_name, "localhost").strip() or "localhost"

    return {
        "country_name": country_name,
        "state_or_province_name": state_or_province_name,
        "locality_name": locality_name,
        "organization_name": organization_name,
        "organizational_unit_name": organizational_unit_name,
        "common_name": common_name_text,
    }


def build_name(subject_kwargs):
    require_cryptography()

    name_attributes = []

    country_text = safe_str(subject_kwargs.get("country_name"), "").strip()
    state_text = safe_str(subject_kwargs.get("state_or_province_name"), "").strip()
    locality_text = safe_str(subject_kwargs.get("locality_name"), "").strip()
    organization_text = safe_str(subject_kwargs.get("organization_name"), "").strip()
    unit_text = safe_str(subject_kwargs.get("organizational_unit_name"), "").strip()
    common_name_text = safe_str(subject_kwargs.get("common_name"), "").strip()

    if country_text:
        name_attributes.append(x509.NameAttribute(NameOID.COUNTRY_NAME, country_text))

    if state_text:
        name_attributes.append(x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, state_text))

    if locality_text:
        name_attributes.append(x509.NameAttribute(NameOID.LOCALITY_NAME, locality_text))

    if organization_text:
        name_attributes.append(x509.NameAttribute(NameOID.ORGANIZATION_NAME, organization_text))

    if unit_text:
        name_attributes.append(x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, unit_text))

    if common_name_text:
        name_attributes.append(x509.NameAttribute(NameOID.COMMON_NAME, common_name_text))

    if not name_attributes:
        name_attributes.append(x509.NameAttribute(NameOID.COMMON_NAME, "localhost"))

    return x509.Name(name_attributes)


def select_backend(requested_backend):
    backend = safe_str(requested_backend, "auto").strip().lower()
    if backend in {"", "auto", "cryptography"}:
        require_cryptography()
        return "cryptography"
    if backend == "openssl":
        raise ValueError(
            "Der neue CA-signierte Standard unterstützt keine OpenSSL-Generierung mehr. "
            "Bitte 'cryptography' verwenden."
        )
    raise ValueError(f"Unbekannter Backend-Wert: {requested_backend}")


def _derive_prefix_from_cert_path(certfile_path):
    cert_path = Path(certfile_path)
    name_text = cert_path.name

    suffixes = (
        ".cert.pem",
        ".crt.pem",
        ".pem",
    )

    for suffix in suffixes:
        if name_text.endswith(suffix):
            trimmed = name_text[: -len(suffix)].strip()
            if trimmed:
                return trimmed

    return cert_path.stem


def build_output_paths(out_dir, file_prefix, root_prefix=DEFAULT_ROOT_PREFIX):
    base_dir = ensure_directory(out_dir)
    build_path = partial(Path, base_dir)

    return {
        "base_dir": base_dir,
        "root_key": build_path(f"{root_prefix}.key.pem"),
        "root_cert": build_path(f"{root_prefix}.cert.pem"),
        "key": build_path(f"{file_prefix}.key.pem"),
        "cert": build_path(f"{file_prefix}.cert.pem"),
        "chain": build_path(f"{file_prefix}.chain.pem"),
        "combined": build_path(f"{file_prefix}.combined.pem"),
        "trust": build_path(f"{file_prefix}.trust.pem"),
        "manifest": build_path(f"{file_prefix}.manifest.json"),
        "file_prefix": file_prefix,
        "root_prefix": root_prefix,
    }


def build_output_paths_from_server_paths(certfile_path, keyfile_path=None, root_prefix=DEFAULT_ROOT_PREFIX):
    cert_path = Path(certfile_path).resolve()
    key_path = Path(keyfile_path).resolve() if safe_str(keyfile_path, "").strip() else None
    prefix = _derive_prefix_from_cert_path(cert_path)
    base_dir = cert_path.parent

    if key_path is None:
        key_path = base_dir / (prefix + ".key.pem")

    output_paths = build_output_paths(base_dir, prefix, root_prefix=root_prefix)
    output_paths["cert"] = cert_path
    output_paths["key"] = key_path
    output_paths["chain"] = base_dir / (prefix + ".chain.pem")
    output_paths["combined"] = base_dir / (prefix + ".combined.pem")
    output_paths["trust"] = base_dir / (prefix + ".trust.pem")
    output_paths["manifest"] = base_dir / (prefix + ".manifest.json")
    return output_paths


def build_client_output_paths(out_dir, client_prefix):
    base_dir = Path(out_dir)
    return {
        "key": base_dir / (client_prefix + ".key.pem"),
        "cert": base_dir / (client_prefix + ".cert.pem"),
        "chain": base_dir / (client_prefix + ".chain.pem"),
        "combined": base_dir / (client_prefix + ".combined.pem"),
        "p12": base_dir / (client_prefix + ".p12"),
        "password": base_dir / (client_prefix + ".p12.password.txt"),
    }


def serialize_private_key(private_key):
    require_cryptography()
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )


def serialize_certificate(certificate):
    require_cryptography()
    return certificate.public_bytes(serialization.Encoding.PEM)


def serialize_public_key(public_key):
    require_cryptography()
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def generate_private_key(key_size):
    require_cryptography()
    return rsa.generate_private_key(public_exponent=65537, key_size=int(key_size))


def _now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def _normalize_positive_days(value, default_value):
    number = safe_int(value, default_value)
    if number <= 0:
        return int(default_value)
    return int(number)


def _normalize_key_size(value, default_value):
    number = safe_int(value, default_value)
    if number < 2048:
        return int(default_value)
    return int(number)


def build_final_san_lists(args_or_dict, discovery):
    requested_dns = normalize_dns_values(_namespace_value(args_or_dict, "dns", []) or [])
    requested_ip = normalize_ip_values(_namespace_value(args_or_dict, "ip", []) or [])

    bind_host_input = safe_str(_namespace_value(args_or_dict, "bind_host", ""), "").strip()
    effective_bind_host = safe_str(discovery.get("effective_bind_host"), "").strip() or bind_host_input

    dns_values = []
    ip_values = []

    for item in discovery.get("recommended_dns") or []:
        _append_unique_dns(dns_values, item)

    for item in discovery.get("dns_values") or []:
        _append_unique_dns(dns_values, item)

    for item in requested_dns:
        _append_unique_dns(dns_values, item)

    for item in discovery.get("ip_values") or []:
        _append_unique_ip(ip_values, item)

    for item in requested_ip:
        _append_unique_ip(ip_values, item)

    if effective_bind_host:
        if is_ip_address(effective_bind_host):
            _append_unique_ip(ip_values, effective_bind_host)

            reverse_dns_map = discovery.get("reverse_dns_map") or {}
            for item in reverse_dns_map.get(normalize_ip_text(effective_bind_host), []) or []:
                _append_unique_dns(dns_values, item)
        else:
            _append_unique_dns(dns_values, effective_bind_host)
            for item in resolve_name_to_ips(effective_bind_host):
                _append_unique_ip(ip_values, item)

    common_name = choose_common_name(
        _namespace_value(args_or_dict, "common_name", ""),
        effective_bind_host,
        dns_values,
    )

    if common_name and not is_ip_address(common_name):
        if common_name.lower() not in {item.lower() for item in dns_values}:
            dns_values.insert(0, common_name)
            dns_values = normalize_dns_values(dns_values)

    return {
        "common_name": common_name,
        "dns_names": normalize_dns_values(dns_values),
        "ip_values": normalize_ip_values(ip_values),
        "requested_dns": requested_dns,
        "requested_ip": requested_ip,
        "effective_bind_host": effective_bind_host,
    }


def build_server_subject_alt_names(bind_host, extra_dns, extra_ips, include_loopback=True, enable_reverse_dns=True):
    discovery = discover_local_identity(
        bind_host=bind_host,
        include_loopback=bool(include_loopback),
        enable_reverse_dns=bool(enable_reverse_dns),
    )

    san_data = build_final_san_lists(
        {
            "bind_host": bind_host,
            "dns": list(extra_dns or []),
            "ip": list(extra_ips or []),
            "common_name": "",
        },
        discovery,
    )

    san_items = []

    require_cryptography()

    for dns_name in san_data["dns_names"]:
        san_items.append(x509.DNSName(dns_name))

    for ip_text in san_data["ip_values"]:
        san_items.append(x509.IPAddress(ipaddress.ip_address(ip_text)))

    return {
        "discovery": discovery,
        "common_name": san_data["common_name"],
        "dns_names": san_data["dns_names"],
        "ip_values": san_data["ip_values"],
        "requested_dns": san_data["requested_dns"],
        "requested_ip": san_data["requested_ip"],
        "effective_bind_host": san_data["effective_bind_host"],
        "san_items": san_items,
    }


def build_client_subject_alt_names(client_name, client_dns, client_ips):
    require_cryptography()

    san_items = []
    dns_names = []
    ip_values = []
    uri_values = []

    add_dns = partial(_append_unique_dns, dns_names)
    add_ip = partial(_append_unique_ip, ip_values)

    for item in client_dns or []:
        add_dns(item)

    for item in client_ips or []:
        add_ip(item)

    name_text = safe_str(client_name, "").strip()

    if "://" in name_text:
        uri_values.append(name_text)
    elif "@" in name_text and " " not in name_text:
        uri_values.append("urn:niedata:client:" + slugify_token(name_text))
    elif "." in name_text and " " not in name_text and not is_ip_address(name_text):
        add_dns(name_text)
    elif is_ip_address(name_text):
        add_ip(name_text)
    else:
        uri_values.append("urn:niedata:client:" + slugify_token(name_text))

    for dns_name in normalize_dns_values(dns_names):
        san_items.append(x509.DNSName(dns_name))

    for ip_text in normalize_ip_values(ip_values):
        san_items.append(x509.IPAddress(ipaddress.ip_address(ip_text)))

    for uri_text in normalize_multi_value(uri_values):
        san_items.append(x509.UniformResourceIdentifier(uri_text))

    return {
        "client_name": name_text,
        "client_cn": name_text or "client",
        "dns_names": normalize_dns_values(dns_names),
        "ip_values": normalize_ip_values(ip_values),
        "uri_values": normalize_multi_value(uri_values),
        "san_items": san_items,
    }


def build_root_ca_certificate(root_private_key, subject_kwargs, valid_days):
    require_cryptography()

    now = _now_utc()
    subject = build_name(subject_kwargs)

    builder = x509.CertificateBuilder()
    builder = builder.subject_name(subject)
    builder = builder.issuer_name(subject)
    builder = builder.public_key(root_private_key.public_key())
    builder = builder.serial_number(x509.random_serial_number())
    builder = builder.not_valid_before(now - datetime.timedelta(minutes=5))
    builder = builder.not_valid_after(now + datetime.timedelta(days=int(valid_days)))
    builder = builder.add_extension(
        x509.BasicConstraints(ca=True, path_length=0),
        critical=True,
    )
    builder = builder.add_extension(
        x509.KeyUsage(
            digital_signature=False,
            content_commitment=False,
            key_encipherment=False,
            data_encipherment=False,
            key_agreement=False,
            key_cert_sign=True,
            crl_sign=True,
            encipher_only=False,
            decipher_only=False,
        ),
        critical=True,
    )
    builder = builder.add_extension(
        x509.SubjectKeyIdentifier.from_public_key(root_private_key.public_key()),
        critical=False,
    )
    builder = builder.add_extension(
        x509.AuthorityKeyIdentifier.from_issuer_public_key(root_private_key.public_key()),
        critical=False,
    )

    return builder.sign(private_key=root_private_key, algorithm=hashes.SHA256())


def build_server_certificate(
    server_private_key,
    root_private_key,
    root_certificate,
    subject_kwargs,
    san_items,
    valid_days,
):
    require_cryptography()

    now = _now_utc()

    builder = x509.CertificateBuilder()
    builder = builder.subject_name(build_name(subject_kwargs))
    builder = builder.issuer_name(root_certificate.subject)
    builder = builder.public_key(server_private_key.public_key())
    builder = builder.serial_number(x509.random_serial_number())
    builder = builder.not_valid_before(now - datetime.timedelta(minutes=5))
    builder = builder.not_valid_after(now + datetime.timedelta(days=int(valid_days)))
    builder = builder.add_extension(
        x509.BasicConstraints(ca=False, path_length=None),
        critical=True,
    )
    builder = builder.add_extension(
        x509.KeyUsage(
            digital_signature=True,
            content_commitment=False,
            key_encipherment=True,
            data_encipherment=False,
            key_agreement=False,
            key_cert_sign=False,
            crl_sign=False,
            encipher_only=False,
            decipher_only=False,
        ),
        critical=True,
    )
    builder = builder.add_extension(
        x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
        critical=False,
    )

    if san_items:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(san_items),
            critical=False,
        )

    builder = builder.add_extension(
        x509.SubjectKeyIdentifier.from_public_key(server_private_key.public_key()),
        critical=False,
    )
    builder = builder.add_extension(
        x509.AuthorityKeyIdentifier.from_issuer_public_key(root_private_key.public_key()),
        critical=False,
    )

    return builder.sign(private_key=root_private_key, algorithm=hashes.SHA256())


def build_client_certificate(
    client_private_key,
    root_private_key,
    root_certificate,
    subject_kwargs,
    san_items,
    valid_days,
):
    require_cryptography()

    now = _now_utc()

    builder = x509.CertificateBuilder()
    builder = builder.subject_name(build_name(subject_kwargs))
    builder = builder.issuer_name(root_certificate.subject)
    builder = builder.public_key(client_private_key.public_key())
    builder = builder.serial_number(x509.random_serial_number())
    builder = builder.not_valid_before(now - datetime.timedelta(minutes=5))
    builder = builder.not_valid_after(now + datetime.timedelta(days=int(valid_days)))
    builder = builder.add_extension(
        x509.BasicConstraints(ca=False, path_length=None),
        critical=True,
    )
    builder = builder.add_extension(
        x509.KeyUsage(
            digital_signature=True,
            content_commitment=False,
            key_encipherment=True,
            data_encipherment=False,
            key_agreement=False,
            key_cert_sign=False,
            crl_sign=False,
            encipher_only=False,
            decipher_only=False,
        ),
        critical=True,
    )
    builder = builder.add_extension(
        x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
        critical=False,
    )

    if san_items:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(san_items),
            critical=False,
        )

    builder = builder.add_extension(
        x509.SubjectKeyIdentifier.from_public_key(client_private_key.public_key()),
        critical=False,
    )
    builder = builder.add_extension(
        x509.AuthorityKeyIdentifier.from_issuer_public_key(root_private_key.public_key()),
        critical=False,
    )

    return builder.sign(private_key=root_private_key, algorithm=hashes.SHA256())


def _read_certificate_datetime(cert_obj, attribute_name):
    value = getattr(cert_obj, attribute_name, None)
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)
    return None


def load_pem_certificate(certfile):
    require_cryptography()
    cert_path = Path(certfile)
    cert_data = cert_path.read_bytes()
    return x509.load_pem_x509_certificate(cert_data)


def load_pem_private_key_file(keyfile, password=None):
    require_cryptography()
    key_path = Path(keyfile)
    password_bytes = None
    if password is not None:
        password_bytes = safe_str(password, "").encode("utf-8")
    return load_pem_private_key(key_path.read_bytes(), password=password_bytes)


def _get_name_values(name_obj, oid):
    try:
        return [safe_str(item.value, "") for item in name_obj.get_attributes_for_oid(oid)]
    except Exception:
        return []


def _extension_value(cert_obj, extension_class):
    try:
        return cert_obj.extensions.get_extension_for_class(extension_class).value
    except Exception:
        return None


def _get_key_usage_summary(cert_obj):
    key_usage = _extension_value(cert_obj, x509.KeyUsage)
    if key_usage is None:
        return {}

    encipher_only = False
    decipher_only = False

    try:
        if key_usage.key_agreement:
            encipher_only = bool(key_usage.encipher_only)
            decipher_only = bool(key_usage.decipher_only)
    except Exception:
        encipher_only = False
        decipher_only = False

    return {
        "digital_signature": bool(key_usage.digital_signature),
        "content_commitment": bool(key_usage.content_commitment),
        "key_encipherment": bool(key_usage.key_encipherment),
        "data_encipherment": bool(key_usage.data_encipherment),
        "key_agreement": bool(key_usage.key_agreement),
        "key_cert_sign": bool(key_usage.key_cert_sign),
        "crl_sign": bool(key_usage.crl_sign),
        "encipher_only": encipher_only,
        "decipher_only": decipher_only,
    }


def _get_extended_key_usage_summary(cert_obj):
    eku = _extension_value(cert_obj, x509.ExtendedKeyUsage)
    if eku is None:
        return []

    result = []
    for item in eku:
        dotted = safe_str(getattr(item, "dotted_string", ""), "").strip()
        if item == ExtendedKeyUsageOID.SERVER_AUTH:
            result.append("serverAuth")
        elif item == ExtendedKeyUsageOID.CLIENT_AUTH:
            result.append("clientAuth")
        elif dotted:
            result.append(dotted)

    return normalize_multi_value(result)


def load_pem_certificate_info(certfile):
    require_cryptography()

    cert_obj = load_pem_certificate(certfile)
    dns_san = []
    ip_san = []
    uri_san = []

    san_extension = _extension_value(cert_obj, x509.SubjectAlternativeName)
    if san_extension is not None:
        try:
            dns_san = [safe_str(item, "") for item in san_extension.get_values_for_type(x509.DNSName)]
        except Exception:
            dns_san = []
        try:
            ip_san = [str(item) for item in san_extension.get_values_for_type(x509.IPAddress)]
        except Exception:
            ip_san = []
        try:
            uri_san = [safe_str(item, "") for item in san_extension.get_values_for_type(x509.UniformResourceIdentifier)]
        except Exception:
            uri_san = []

    basic_constraints = _extension_value(cert_obj, x509.BasicConstraints)
    subject_key_identifier = _extension_value(cert_obj, x509.SubjectKeyIdentifier)
    authority_key_identifier = _extension_value(cert_obj, x509.AuthorityKeyIdentifier)

    info = {
        "subject_cn": normalize_multi_value(_get_name_values(cert_obj.subject, NameOID.COMMON_NAME)),
        "issuer_cn": normalize_multi_value(_get_name_values(cert_obj.issuer, NameOID.COMMON_NAME)),
        "subject": cert_obj.subject.rfc4514_string(),
        "issuer": cert_obj.issuer.rfc4514_string(),
        "dns_san": normalize_dns_values(dns_san),
        "ip_san": normalize_ip_values(ip_san),
        "uri_san": normalize_multi_value(uri_san),
        "not_before": safe_str(_read_certificate_datetime(cert_obj, "not_valid_before_utc") or _read_certificate_datetime(cert_obj, "not_valid_before"), ""),
        "not_after": safe_str(_read_certificate_datetime(cert_obj, "not_valid_after_utc") or _read_certificate_datetime(cert_obj, "not_valid_after"), ""),
        "serial_number": safe_str(cert_obj.serial_number, ""),
        "is_ca": bool(getattr(basic_constraints, "ca", False)) if basic_constraints is not None else False,
        "path_length": getattr(basic_constraints, "path_length", None) if basic_constraints is not None else None,
        "key_usage": _get_key_usage_summary(cert_obj),
        "extended_key_usage": _get_extended_key_usage_summary(cert_obj),
        "subject_key_identifier": safe_str(getattr(subject_key_identifier, "digest", b"").hex() if subject_key_identifier is not None else "", ""),
        "authority_key_identifier": safe_str(
            getattr(authority_key_identifier, "key_identifier", b"").hex()
            if authority_key_identifier is not None and getattr(authority_key_identifier, "key_identifier", None) is not None
            else "",
            "",
        ),
        "backend": "cryptography",
    }
    return info


def _parse_certificate_datetime(value):
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)

    text = safe_str(value, "").strip()
    if not text:
        return None

    parse_candidates = [
        partial(datetime.datetime.strptime, text, "%b %d %H:%M:%S %Y GMT"),
        partial(datetime.datetime.strptime, text, "%Y-%m-%d %H:%M:%S%z"),
        partial(datetime.datetime.strptime, text, "%Y-%m-%d %H:%M:%S.%f%z"),
        partial(datetime.datetime.strptime, text, "%Y-%m-%d %H:%M:%S"),
        partial(datetime.datetime.strptime, text, "%Y-%m-%dT%H:%M:%S%z"),
        partial(datetime.datetime.strptime, text, "%Y-%m-%dT%H:%M:%S.%f%z"),
    ]

    for fn in parse_candidates:
        try:
            parsed = fn()
        except Exception:
            continue

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.astimezone(datetime.timezone.utc)

    try:
        parsed = datetime.datetime.fromisoformat(text)
    except Exception:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)


def certificate_expired(cert_info, renew_before_days=0):
    not_after = _parse_certificate_datetime(cert_info.get("not_after"))
    if not_after is None:
        return True, None

    now_utc = _now_utc()
    renew_before_delta = datetime.timedelta(days=int(max(0, safe_int(renew_before_days, 0))))
    expired = now_utc >= (not_after - renew_before_delta)
    return expired, not_after


def _certificate_matches_private_key(certificate_obj, private_key_obj):
    try:
        cert_public = serialize_public_key(certificate_obj.public_key())
        key_public = serialize_public_key(private_key_obj.public_key())
        return cert_public == key_public
    except Exception:
        return False


def _verify_certificate_signature(certificate_obj, issuer_certificate_obj):
    issuer_public_key = issuer_certificate_obj.public_key()

    try:
        if isinstance(issuer_public_key, rsa.RSAPublicKey):
            issuer_public_key.verify(
                certificate_obj.signature,
                certificate_obj.tbs_certificate_bytes,
                padding.PKCS1v15(),
                certificate_obj.signature_hash_algorithm,
            )
            return True

        if isinstance(issuer_public_key, ec.EllipticCurvePublicKey):
            issuer_public_key.verify(
                certificate_obj.signature,
                certificate_obj.tbs_certificate_bytes,
                ec.ECDSA(certificate_obj.signature_hash_algorithm),
            )
            return True

        if isinstance(issuer_public_key, ed25519.Ed25519PublicKey):
            issuer_public_key.verify(
                certificate_obj.signature,
                certificate_obj.tbs_certificate_bytes,
            )
            return True

        if isinstance(issuer_public_key, ed448.Ed448PublicKey):
            issuer_public_key.verify(
                certificate_obj.signature,
                certificate_obj.tbs_certificate_bytes,
            )
            return True
    except InvalidSignature:
        return False
    except Exception:
        return False

    return False


def _certificate_has_extended_key_usage(certificate_obj, eku_oid):
    eku_extension = _extension_value(certificate_obj, x509.ExtendedKeyUsage)
    if eku_extension is None:
        return False
    return eku_oid in list(eku_extension)


def _certificate_is_ca(certificate_obj):
    basic_constraints = _extension_value(certificate_obj, x509.BasicConstraints)
    if basic_constraints is None:
        return False
    return bool(getattr(basic_constraints, "ca", False))


def _load_root_material(output_paths):
    root_cert_path = Path(output_paths["root_cert"])
    root_key_path = Path(output_paths["root_key"])

    if not root_cert_path.is_file():
        return None, None

    root_cert_obj = load_pem_certificate(root_cert_path)

    if root_key_path.is_file():
        try:
            root_key_obj = load_pem_private_key_file(root_key_path)
        except Exception:
            root_key_obj = None
    else:
        root_key_obj = None

    return root_cert_obj, root_key_obj


def _load_server_material(output_paths):
    cert_path = Path(output_paths["cert"])
    key_path = Path(output_paths["key"])

    if not cert_path.is_file():
        return None, None

    cert_obj = load_pem_certificate(cert_path)

    if key_path.is_file():
        try:
            key_obj = load_pem_private_key_file(key_path)
        except Exception:
            key_obj = None
    else:
        key_obj = None

    return cert_obj, key_obj


def _root_material_status(output_paths, renew_before_days=0):
    status = {
        "exists": False,
        "key_exists": Path(output_paths["root_key"]).is_file(),
        "cert_exists": Path(output_paths["root_cert"]).is_file(),
        "valid": False,
        "expired": True,
        "self_signed": False,
        "is_ca": False,
        "key_matches": False,
        "cert_info": {},
        "reasons": [],
    }

    if not status["cert_exists"]:
        status["reasons"].append("root_cert_missing")
        return status

    root_cert_obj, root_key_obj = _load_root_material(output_paths)
    if root_cert_obj is None:
        status["reasons"].append("root_cert_unreadable")
        return status

    status["exists"] = True
    cert_info = load_pem_certificate_info(output_paths["root_cert"])
    status["cert_info"] = cert_info

    expired, _ = certificate_expired(cert_info, renew_before_days=renew_before_days)
    status["expired"] = bool(expired)

    status["is_ca"] = _certificate_is_ca(root_cert_obj)
    if not status["is_ca"]:
        status["reasons"].append("root_not_ca")

    if root_cert_obj.issuer == root_cert_obj.subject and _verify_certificate_signature(root_cert_obj, root_cert_obj):
        status["self_signed"] = True
    else:
        status["reasons"].append("root_not_self_signed")

    if not status["key_exists"]:
        status["reasons"].append("root_key_missing")
    elif root_key_obj is None:
        status["reasons"].append("root_key_unreadable")
    else:
        status["key_matches"] = _certificate_matches_private_key(root_cert_obj, root_key_obj)
        if not status["key_matches"]:
            status["reasons"].append("root_key_mismatch")

    if status["expired"]:
        status["reasons"].append("root_expired_or_within_renew_window")

    status["valid"] = bool(
        status["exists"]
        and status["is_ca"]
        and status["self_signed"]
        and not status["expired"]
    )

    return status


def _build_identity_expectations(bind_host, requested_dns, requested_ip, discovery):
    expected_dns = normalize_dns_values(requested_dns)
    expected_ip = normalize_ip_values(requested_ip)

    host_text = safe_str(bind_host, "").strip()
    if host_text and not is_wildcard_host(host_text):
        if is_ip_address(host_text):
            ip_text = normalize_ip_text(host_text)
            if ip_text:
                expected_ip = normalize_ip_values(list(expected_ip) + [ip_text])
        else:
            expected_dns = normalize_dns_values(list(expected_dns) + [host_text])

    if not expected_dns and not expected_ip:
        recommended_dns = normalize_dns_values(discovery.get("recommended_dns") or [])
        primary_ip = normalize_ip_text(discovery.get("primary_ip"))
        if recommended_dns:
            expected_dns = recommended_dns
        if primary_ip:
            expected_ip = normalize_ip_values([primary_ip])

    return expected_dns, expected_ip


def _certificate_matches_expected_server_identity(cert_info, bind_host, requested_dns, requested_ip, discovery):
    cert_dns = {item.lower() for item in normalize_dns_values(cert_info.get("dns_san") or [])}
    cert_ip = set(normalize_ip_values(cert_info.get("ip_san") or []))

    expected_dns, expected_ip = _build_identity_expectations(
        bind_host=bind_host,
        requested_dns=requested_dns,
        requested_ip=requested_ip,
        discovery=discovery,
    )

    missing_dns = []
    missing_ip = []

    for item in expected_dns:
        if item.lower() not in cert_dns:
            missing_dns.append(item)

    for item in expected_ip:
        if item not in cert_ip:
            missing_ip.append(item)

    if missing_dns or missing_ip:
        return False, {
            "missing_dns": missing_dns,
            "missing_ip": missing_ip,
            "expected_dns": expected_dns,
            "expected_ip": expected_ip,
        }

    if not cert_dns and not cert_ip:
        return False, {
            "missing_dns": expected_dns,
            "missing_ip": expected_ip,
            "expected_dns": expected_dns,
            "expected_ip": expected_ip,
        }

    return True, {
        "missing_dns": [],
        "missing_ip": [],
        "expected_dns": expected_dns,
        "expected_ip": expected_ip,
    }


def _server_material_status(
    output_paths,
    root_status,
    bind_host,
    requested_dns,
    requested_ip,
    discovery,
    renew_before_days=0,
):
    status = {
        "exists": False,
        "cert_exists": Path(output_paths["cert"]).is_file(),
        "key_exists": Path(output_paths["key"]).is_file(),
        "valid": False,
        "expired": True,
        "key_matches": False,
        "signed_by_root": False,
        "is_ca": False,
        "server_auth": False,
        "identity_ok": False,
        "identity_details": {},
        "cert_info": {},
        "reasons": [],
    }

    if not status["cert_exists"]:
        status["reasons"].append("server_cert_missing")
        return status

    cert_obj, key_obj = _load_server_material(output_paths)
    if cert_obj is None:
        status["reasons"].append("server_cert_unreadable")
        return status

    status["exists"] = True
    cert_info = load_pem_certificate_info(output_paths["cert"])
    status["cert_info"] = cert_info

    expired, _ = certificate_expired(cert_info, renew_before_days=renew_before_days)
    status["expired"] = bool(expired)
    if status["expired"]:
        status["reasons"].append("server_expired_or_within_renew_window")

    status["is_ca"] = _certificate_is_ca(cert_obj)
    if status["is_ca"]:
        status["reasons"].append("server_marked_as_ca")

    status["server_auth"] = _certificate_has_extended_key_usage(cert_obj, ExtendedKeyUsageOID.SERVER_AUTH)
    if not status["server_auth"]:
        status["reasons"].append("server_missing_serverauth_eku")

    if not status["key_exists"]:
        status["reasons"].append("server_key_missing")
    elif key_obj is None:
        status["reasons"].append("server_key_unreadable")
    else:
        status["key_matches"] = _certificate_matches_private_key(cert_obj, key_obj)
        if not status["key_matches"]:
            status["reasons"].append("server_key_mismatch")

    if root_status.get("cert_exists"):
        root_cert_obj, _ = _load_root_material(output_paths)
        if root_cert_obj is not None and cert_obj.issuer == root_cert_obj.subject and _verify_certificate_signature(cert_obj, root_cert_obj):
            status["signed_by_root"] = True
        else:
            status["reasons"].append("server_not_signed_by_root")
    else:
        status["reasons"].append("root_cert_missing_for_chain_validation")

    identity_ok, identity_details = _certificate_matches_expected_server_identity(
        cert_info=cert_info,
        bind_host=bind_host,
        requested_dns=requested_dns,
        requested_ip=requested_ip,
        discovery=discovery,
    )
    status["identity_ok"] = bool(identity_ok)
    status["identity_details"] = identity_details
    if not status["identity_ok"]:
        status["reasons"].append("server_identity_mismatch")

    status["valid"] = bool(
        status["exists"]
        and not status["expired"]
        and not status["is_ca"]
        and status["server_auth"]
        and status["key_matches"]
        and status["signed_by_root"]
        and status["identity_ok"]
    )

    return status


def _client_certificate_status(client_paths, root_status, renew_before_days=0):
    status = {
        "exists": Path(client_paths["cert"]).is_file(),
        "key_exists": Path(client_paths["key"]).is_file(),
        "cert_exists": Path(client_paths["cert"]).is_file(),
        "chain_exists": Path(client_paths["chain"]).is_file(),
        "combined_exists": Path(client_paths["combined"]).is_file(),
        "p12_exists": Path(client_paths["p12"]).is_file(),
        "password_exists": Path(client_paths["password"]).is_file(),
        "valid": False,
        "expired": True,
        "key_matches": False,
        "client_auth": False,
        "signed_by_root": False,
        "cert_info": {},
        "reasons": [],
    }

    if not status["cert_exists"]:
        status["reasons"].append("client_cert_missing")
        return status

    cert_obj = load_pem_certificate(client_paths["cert"])
    cert_info = load_pem_certificate_info(client_paths["cert"])
    status["cert_info"] = cert_info

    expired, _ = certificate_expired(cert_info, renew_before_days=renew_before_days)
    status["expired"] = bool(expired)
    if status["expired"]:
        status["reasons"].append("client_expired_or_within_renew_window")

    if status["key_exists"]:
        try:
            key_obj = load_pem_private_key_file(client_paths["key"])
        except Exception:
            key_obj = None
            status["reasons"].append("client_key_unreadable")
        if key_obj is not None:
            status["key_matches"] = _certificate_matches_private_key(cert_obj, key_obj)
            if not status["key_matches"]:
                status["reasons"].append("client_key_mismatch")
    else:
        status["reasons"].append("client_key_missing")

    status["client_auth"] = _certificate_has_extended_key_usage(cert_obj, ExtendedKeyUsageOID.CLIENT_AUTH)
    if not status["client_auth"]:
        status["reasons"].append("client_missing_clientauth_eku")

    if root_status.get("cert_exists"):
        root_cert_obj, _ = _load_root_material({
            "root_cert": root_status.get("root_cert_path") or "",
            "root_key": root_status.get("root_key_path") or "",
        })
        if root_cert_obj is None:
            try:
                root_cert_obj = load_pem_certificate(root_status["root_cert_path"])
            except Exception:
                root_cert_obj = None
        if root_cert_obj is not None and cert_obj.issuer == root_cert_obj.subject and _verify_certificate_signature(cert_obj, root_cert_obj):
            status["signed_by_root"] = True
        else:
            status["reasons"].append("client_not_signed_by_root")
    else:
        status["reasons"].append("root_cert_missing_for_client_validation")

    if not status["chain_exists"]:
        status["reasons"].append("client_chain_missing")
    if not status["combined_exists"]:
        status["reasons"].append("client_combined_missing")
    if not status["p12_exists"]:
        status["reasons"].append("client_p12_missing")

    status["valid"] = bool(
        status["exists"]
        and not status["expired"]
        and status["key_matches"]
        and status["client_auth"]
        and status["signed_by_root"]
        and status["chain_exists"]
        and status["combined_exists"]
        and status["p12_exists"]
    )
    return status


def build_manifest(subject_kwargs, output_paths, discovery, dns_names, ip_values, info, bind_host, requested_dns, requested_ip):
    return {
        "created_at": _now_utc().isoformat(),
        "subject": subject_kwargs,
        "bind_host": safe_str(bind_host, ""),
        "requested_dns": normalize_dns_values(requested_dns),
        "requested_ip": normalize_ip_values(requested_ip),
        "discovery": discovery,
        "san": {
            "dns": normalize_dns_values(dns_names),
            "ip": normalize_ip_values(ip_values),
        },
        "paths": {key: str(value) for key, value in output_paths.items()},
        "certificate": info,
    }


def build_ca_signed_manifest(
    *,
    output_paths,
    discovery,
    bind_host_input,
    effective_bind_host,
    requested_dns,
    requested_ip,
    root_subject,
    server_subject,
    server_dns_names,
    server_ip_values,
    root_info,
    server_info,
    client_items,
    defaults,
    action,
    reasons,
):
    return {
        "created_at": _now_utc().isoformat(),
        "managed_by": "_generate_ca_signed_certs.py",
        "mode": "ca_signed",
        "action": safe_str(action, ""),
        "reasons": normalize_multi_value(reasons),
        "defaults": defaults,
        "bind_host_input": safe_str(bind_host_input, ""),
        "effective_bind_host": safe_str(effective_bind_host, ""),
        "requested_dns": normalize_dns_values(requested_dns),
        "requested_ip": normalize_ip_values(requested_ip),
        "discovery": discovery,
        "paths": {key: str(value) for key, value in output_paths.items()},
        "root": {
            "subject": root_subject,
            "certificate": root_info,
        },
        "server": {
            "subject": server_subject,
            "dns_san": normalize_dns_values(server_dns_names),
            "ip_san": normalize_ip_values(server_ip_values),
            "certificate": server_info,
        },
        "clients": client_items,
        "compatibility": {
            "trustfile_alias": str(output_paths["trust"]),
            "trustfile_source": str(output_paths["root_cert"]),
            "server_chainfile": str(output_paths["chain"]),
        },
    }


def save_manifest(path_value, payload):
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    write_text_file(path_value, text)
    return path_value


def generate_password(length):
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789-_@#%+="
    normalized_length = max(12, safe_int(length, DEFAULT_P12_PASSWORD_LENGTH))
    return "".join(secrets.choice(alphabet) for _ in range(normalized_length))


def _resolve_client_p12_password(configured_password, password_length):
    password_text = safe_str(configured_password, "").strip()
    if password_text:
        return password_text, False
    return generate_password(password_length), True


def write_client_material(
    *,
    out_dir,
    root_certificate,
    client_name,
    client_prefix,
    client_private_key,
    client_certificate,
    p12_password,
    write_password_file,
):
    require_cryptography()

    output_paths = build_client_output_paths(out_dir, client_prefix)

    client_key_pem = serialize_private_key(client_private_key)
    client_cert_pem = serialize_certificate(client_certificate)
    root_cert_pem = serialize_certificate(root_certificate)
    client_chain_pem = client_cert_pem + root_cert_pem
    client_combined_pem = client_cert_pem + client_key_pem

    p12_bytes = serialize_key_and_certificates(
        name=safe_str(client_name, client_prefix).encode("utf-8"),
        key=client_private_key,
        cert=client_certificate,
        cas=[root_certificate],
        encryption_algorithm=serialization.BestAvailableEncryption(p12_password.encode("utf-8")),
    )

    write_binary_file(output_paths["key"], client_key_pem)
    chmod_private(output_paths["key"])
    write_binary_file(output_paths["cert"], client_cert_pem)
    write_binary_file(output_paths["chain"], client_chain_pem)
    write_binary_file(output_paths["combined"], client_combined_pem)
    write_binary_file(output_paths["p12"], p12_bytes)
    chmod_private(output_paths["p12"])

    if write_password_file:
        write_text_file(output_paths["password"], p12_password + "\n")
        chmod_private(output_paths["password"])
    else:
        try:
            Path(output_paths["password"]).unlink(missing_ok=True)
        except Exception:
            pass

    return output_paths


def _write_server_sidecars(output_paths, server_cert_pem, server_key_pem, root_cert_pem):
    write_binary_file(output_paths["cert"], server_cert_pem)
    write_binary_file(output_paths["key"], server_key_pem)
    chmod_private(output_paths["key"])
    write_binary_file(output_paths["chain"], server_cert_pem + root_cert_pem)
    write_binary_file(output_paths["combined"], server_cert_pem + server_key_pem)
    write_binary_file(output_paths["trust"], root_cert_pem)


def _repair_server_sidecars_from_existing_material(output_paths):
    cert_bytes = Path(output_paths["cert"]).read_bytes()
    key_bytes = Path(output_paths["key"]).read_bytes()
    root_bytes = Path(output_paths["root_cert"]).read_bytes()

    write_binary_file(output_paths["chain"], cert_bytes + root_bytes)
    write_binary_file(output_paths["combined"], cert_bytes + key_bytes)
    write_binary_file(output_paths["trust"], root_bytes)


def _collect_missing_server_sidecars(output_paths):
    missing = []
    for key_name in ("chain", "combined", "trust", "manifest"):
        path_obj = Path(output_paths[key_name])
        if not path_obj.is_file():
            missing.append(key_name)
    return missing


def _backup_existing_files(path_values, logger_obj=None):
    active_logger = logger_obj or logger
    timestamp = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    backups = []

    for item in path_values:
        if not item:
            continue
        path_obj = Path(item)
        if not path_obj.exists():
            continue

        backup_path = path_obj.with_name(path_obj.name + ".bak." + timestamp)
        shutil.copy2(path_obj, backup_path)
        backups.append(str(backup_path))

    if backups:
        active_logger.info("TLS/PKI-Backups erstellt: %s", ", ".join(backups))

    return backups


def _project_root_from_this_file():
    return Path(__file__).resolve().parents[2]


def _resolve_runtime_path(path_value, project_root=None):
    text = safe_str(path_value, "").strip()
    if not text:
        return ""

    expanded = os.path.expanduser(os.path.expandvars(text))
    if os.path.isabs(expanded):
        return str(Path(expanded).resolve())

    if project_root is None:
        project_root = _project_root_from_this_file()

    return str((Path(project_root) / expanded).resolve())


def _build_generation_defaults(
    *,
    root_cn,
    root_prefix,
    file_prefix,
    client_prefix,
    client_p12_password_length,
):
    return {
        "root_cn": safe_str(root_cn, DEFAULT_ROOT_CN),
        "root_prefix": safe_str(root_prefix, DEFAULT_ROOT_PREFIX),
        "server_prefix": safe_str(file_prefix, DEFAULT_SERVER_PREFIX),
        "client_prefix": safe_str(client_prefix, DEFAULT_CLIENT_PREFIX),
        "client_p12_password_length": safe_int(client_p12_password_length, DEFAULT_P12_PASSWORD_LENGTH),
        "default_client_p12_password": DEFAULT_CLIENT_P12_PASSWORD,
        "default_out_dir": DEFAULT_OUT_DIR,
    }


def _generate_root_and_server_material(
    *,
    output_paths,
    discovery,
    bind_host,
    requested_dns,
    requested_ip,
    common_name,
    root_subject_kwargs,
    server_subject_kwargs,
    root_days,
    server_days,
    root_key_size,
    server_key_size,
    root_private_key=None,
    root_certificate=None,
):
    require_cryptography()

    san_items = []

    for dns_name in normalize_dns_values(discovery.get("server_dns_names") or []):
        san_items.append(x509.DNSName(dns_name))

    for ip_text in normalize_ip_values(discovery.get("server_ip_values") or []):
        san_items.append(x509.IPAddress(ipaddress.ip_address(ip_text)))

    local_root_private_key = root_private_key
    local_root_certificate = root_certificate

    if local_root_private_key is None or local_root_certificate is None:
        local_root_private_key = generate_private_key(root_key_size)
        local_root_certificate = build_root_ca_certificate(
            root_private_key=local_root_private_key,
            subject_kwargs=root_subject_kwargs,
            valid_days=root_days,
        )

    server_private_key = generate_private_key(server_key_size)
    server_certificate = build_server_certificate(
        server_private_key=server_private_key,
        root_private_key=local_root_private_key,
        root_certificate=local_root_certificate,
        subject_kwargs=server_subject_kwargs,
        san_items=san_items,
        valid_days=server_days,
    )

    root_key_pem = serialize_private_key(local_root_private_key)
    root_cert_pem = serialize_certificate(local_root_certificate)
    server_key_pem = serialize_private_key(server_private_key)
    server_cert_pem = serialize_certificate(server_certificate)

    return {
        "root_private_key": local_root_private_key,
        "root_certificate": local_root_certificate,
        "server_private_key": server_private_key,
        "server_certificate": server_certificate,
        "root_key_pem": root_key_pem,
        "root_cert_pem": root_cert_pem,
        "server_key_pem": server_key_pem,
        "server_cert_pem": server_cert_pem,
        "bind_host": bind_host,
        "requested_dns": requested_dns,
        "requested_ip": requested_ip,
        "common_name": common_name,
    }


def _write_root_material(output_paths, root_key_pem, root_cert_pem):
    write_binary_file(output_paths["root_key"], root_key_pem)
    chmod_private(output_paths["root_key"])
    write_binary_file(output_paths["root_cert"], root_cert_pem)


def _prepare_server_generation_context(
    *,
    bind_host,
    explicit_common_name,
    extra_dns,
    extra_ip,
    include_loopback,
    enable_reverse_dns,
):
    discovery = discover_local_identity(
        bind_host=bind_host,
        include_loopback=include_loopback,
        enable_reverse_dns=enable_reverse_dns,
    )

    san_data = build_final_san_lists(
        {
            "bind_host": bind_host,
            "common_name": explicit_common_name,
            "dns": list(extra_dns or []),
            "ip": list(extra_ip or []),
        },
        discovery,
    )

    discovery["server_dns_names"] = list(san_data["dns_names"])
    discovery["server_ip_values"] = list(san_data["ip_values"])

    return {
        "discovery": discovery,
        "common_name": san_data["common_name"],
        "effective_bind_host": san_data["effective_bind_host"],
        "requested_dns": san_data["requested_dns"],
        "requested_ip": san_data["requested_ip"],
        "dns_names": san_data["dns_names"],
        "ip_values": san_data["ip_values"],
    }


def _gather_client_output_paths(out_dir, client_names, client_prefix):
    items = []
    for client_name in client_names or []:
        client_text = safe_str(client_name, "").strip()
        if not client_text:
            continue
        client_slug = slugify_token(client_text)
        effective_prefix = safe_str(client_prefix, DEFAULT_CLIENT_PREFIX).strip() or DEFAULT_CLIENT_PREFIX
        final_prefix = f"{effective_prefix}-{client_slug}"
        items.append({
            "client_name": client_text,
            "client_prefix": final_prefix,
            "paths": build_client_output_paths(out_dir, final_prefix),
        })
    return items


def _build_client_manifest_item(
    *,
    client_name,
    client_prefix,
    client_san_data,
    client_paths,
    password_file_written,
):
    return {
        "client_name": client_name,
        "client_cn": client_san_data["client_cn"],
        "client_prefix": client_prefix,
        "dns_sans": client_san_data["dns_names"],
        "ip_sans": client_san_data["ip_values"],
        "uri_sans": client_san_data["uri_values"],
        "certfile": str(client_paths["cert"]),
        "keyfile": str(client_paths["key"]),
        "chainfile": str(client_paths["chain"]),
        "combinedfile": str(client_paths["combined"]),
        "pkcs12": str(client_paths["p12"]),
        "pkcs12_password_file": str(client_paths["password"]) if password_file_written else "",
    }


def _backup_relevant_paths(output_paths, client_output_items, logger_obj=None):
    all_paths = [
        output_paths["root_key"],
        output_paths["root_cert"],
        output_paths["cert"],
        output_paths["key"],
        output_paths["chain"],
        output_paths["combined"],
        output_paths["trust"],
        output_paths["manifest"],
    ]

    for item in client_output_items or []:
        client_paths = item["paths"]
        all_paths.extend(client_paths.values())

    return _backup_existing_files(all_paths, logger_obj=logger_obj)


def _generate_or_update_clients(
    *,
    out_dir,
    root_private_key,
    root_certificate,
    client_output_items,
    client_days,
    client_key_size,
    country,
    state,
    locality,
    organization,
    organizational_unit,
    client_dns,
    client_ip,
    client_p12_password,
    client_p12_password_length,
    write_client_password_file,
    root_rotated,
    force,
    renew_before_days,
    root_prefix,
):
    client_manifest_items = []
    client_actions = []

    effective_root_prefix = safe_str(root_prefix, DEFAULT_ROOT_PREFIX).strip() or DEFAULT_ROOT_PREFIX

    root_status_for_clients = {
        "cert_exists": True,
        "root_cert_path": str(Path(out_dir) / (effective_root_prefix + ".cert.pem")),
        "root_key_path": str(Path(out_dir) / (effective_root_prefix + ".key.pem")),
    }

    for item in client_output_items or []:
        client_name = item["client_name"]
        client_prefix = item["client_prefix"]
        client_paths = item["paths"]
        client_san_data = build_client_subject_alt_names(
            client_name=client_name,
            client_dns=client_dns,
            client_ips=client_ip,
        )

        client_subject_kwargs = build_subject_kwargs(
            {
                "country": country,
                "state": state,
                "locality": locality,
                "organization": organization,
                "organizational_unit": organizational_unit,
            },
            client_san_data["client_cn"],
        )

        client_status = _client_certificate_status(
            client_paths=client_paths,
            root_status=root_status_for_clients,
            renew_before_days=renew_before_days,
        )

        missing_files = [
            key_name
            for key_name in ("cert", "key", "chain", "combined", "p12")
            if not Path(client_paths[key_name]).is_file()
        ]

        need_generate_client = bool(force or root_rotated or not client_status["valid"] or missing_files)

        if need_generate_client:
            password_value, was_generated_password = _resolve_client_p12_password(
                configured_password=client_p12_password,
                password_length=client_p12_password_length,
            )

            client_private_key = generate_private_key(client_key_size)
            client_certificate = build_client_certificate(
                client_private_key=client_private_key,
                root_private_key=root_private_key,
                root_certificate=root_certificate,
                subject_kwargs=client_subject_kwargs,
                san_items=client_san_data["san_items"],
                valid_days=client_days,
            )

            written_paths = write_client_material(
                out_dir=out_dir,
                root_certificate=root_certificate,
                client_name=client_name,
                client_prefix=client_prefix,
                client_private_key=client_private_key,
                client_certificate=client_certificate,
                p12_password=password_value,
                write_password_file=write_client_password_file,
            )

            client_manifest_items.append(
                _build_client_manifest_item(
                    client_name=client_name,
                    client_prefix=client_prefix,
                    client_san_data=client_san_data,
                    client_paths=written_paths,
                    password_file_written=write_client_password_file,
                )
            )

            action_reason = "generated"
            if root_rotated:
                action_reason = "regenerated_after_root_rotation"
            elif client_status["exists"]:
                action_reason = "regenerated"

            if was_generated_password:
                action_reason = action_reason + "_with_generated_password"

            client_actions.append({
                "client_name": client_name,
                "client_prefix": client_prefix,
                "action": action_reason,
                "reasons": client_status["reasons"] or missing_files,
            })
            continue

        if write_client_password_file and not Path(client_paths["password"]).is_file():
            password_value, _ = _resolve_client_p12_password(
                configured_password=client_p12_password,
                password_length=client_p12_password_length,
            )

            existing_client_key = load_pem_private_key_file(client_paths["key"])
            existing_client_cert = load_pem_certificate(client_paths["cert"])

            write_client_material(
                out_dir=out_dir,
                root_certificate=root_certificate,
                client_name=client_name,
                client_prefix=client_prefix,
                client_private_key=existing_client_key,
                client_certificate=existing_client_cert,
                p12_password=password_value,
                write_password_file=write_client_password_file,
            )
            client_actions.append({
                "client_name": client_name,
                "client_prefix": client_prefix,
                "action": "repaired_password_or_p12",
                "reasons": ["client_password_or_p12_missing"],
            })

        client_manifest_items.append(
            _build_client_manifest_item(
                client_name=client_name,
                client_prefix=client_prefix,
                client_san_data=client_san_data,
                client_paths=client_paths,
                password_file_written=write_client_password_file and Path(client_paths["password"]).is_file(),
            )
        )
        client_actions.append({
            "client_name": client_name,
            "client_prefix": client_prefix,
            "action": "validated",
            "reasons": [],
        })

    return client_manifest_items, client_actions


def _calculate_generation_plan(
    *,
    output_paths,
    root_status,
    server_status,
    requested_client_items,
    force,
):
    plan = {
        "rotate_root": False,
        "regenerate_server": False,
        "repair_sidecars": False,
        "client_root_rotated": False,
        "reasons": [],
    }

    if force:
        plan["rotate_root"] = True
        plan["regenerate_server"] = True
        plan["reasons"].append("force")
        return plan

    if not root_status["valid"]:
        if not root_status.get("key_exists") and server_status.get("valid"):
            plan["reasons"].append("root_key_missing_but_server_still_valid")
        else:
            plan["rotate_root"] = True
            plan["regenerate_server"] = True
            plan["reasons"].extend(root_status["reasons"])

    if not server_status["valid"]:
        plan["regenerate_server"] = True
        plan["reasons"].extend(server_status["reasons"])

        if not root_status.get("key_exists") or "root_key_mismatch" in root_status.get("reasons", []):
            plan["rotate_root"] = True
            plan["reasons"].append("root_not_usable_for_server_reissue")

    missing_sidecars = _collect_missing_server_sidecars(output_paths)
    if missing_sidecars and not plan["regenerate_server"] and not plan["rotate_root"]:
        plan["repair_sidecars"] = True
        plan["reasons"].append("missing_sidecars:" + ",".join(missing_sidecars))

    if requested_client_items:
        for item in requested_client_items:
            client_paths = item["paths"]
            mandatory_paths = [client_paths["cert"], client_paths["key"], client_paths["chain"], client_paths["combined"], client_paths["p12"]]
            if any(not Path(path_obj).is_file() for path_obj in mandatory_paths):
                plan["reasons"].append("client_material_missing:" + item["client_prefix"])
                break

    plan["reasons"] = normalize_multi_value(plan["reasons"])
    return plan


def bootstrap_server_certificate(connect_host, port, target_pem_path, timeout):
    pem_text = ssl.get_server_certificate((connect_host, int(port)), timeout=float(timeout))
    path_obj = write_text_file(target_pem_path, pem_text)
    return str(path_obj.resolve())


def build_client_ssl_context(cafile=None, insecure=False, minimum_tls_version=None):
    if insecure:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    if minimum_tls_version:
        minimum_map = {
            "TLSv1": ssl.TLSVersion.TLSv1,
            "TLSv1_1": ssl.TLSVersion.TLSv1_1,
            "TLSv1_2": ssl.TLSVersion.TLSv1_2,
            "TLSv1_3": ssl.TLSVersion.TLSv1_3,
        }
        version_name = safe_str(minimum_tls_version, "").strip()
        if version_name:
            if version_name not in minimum_map:
                raise ValueError(f"Unbekannte TLS-Version: {version_name}")
            context.minimum_version = minimum_map[version_name]

    if cafile:
        context.load_verify_locations(cafile=str(cafile))
    else:
        context.load_default_certs()

    return context


def perform_tls_handshake(connect_host, port, server_hostname, timeout, ssl_context):
    with socket.create_connection((connect_host, int(port)), timeout=float(timeout)) as raw_socket:
        with ssl_context.wrap_socket(raw_socket, server_hostname=server_hostname) as tls_socket:
            peer_cert = tls_socket.getpeercert()
            binary_cert = tls_socket.getpeercert(binary_form=True)

            return {
                "tls_version": tls_socket.version(),
                "cipher": tls_socket.cipher(),
                "peer_cert": peer_cert,
                "peer_cert_der": binary_cert,
            }


def extract_peer_certificate_summary(peer_cert):
    subject_cn = []
    issuer_cn = []
    dns_san = []
    ip_san = []

    for rdn in peer_cert.get("subject", ()):
        for key, value in rdn:
            if safe_str(key, "") == "commonName":
                subject_cn.append(safe_str(value, ""))

    for rdn in peer_cert.get("issuer", ()):
        for key, value in rdn:
            if safe_str(key, "") == "commonName":
                issuer_cn.append(safe_str(value, ""))

    for key, value in peer_cert.get("subjectAltName", ()):
        key_text = safe_str(key, "")
        value_text = safe_str(value, "")
        if key_text == "DNS":
            dns_san.append(value_text)
        elif key_text == "IP Address":
            ip_san.append(value_text)

    return {
        "subject_cn": normalize_multi_value(subject_cn),
        "issuer_cn": normalize_multi_value(issuer_cn),
        "dns_san": normalize_dns_values(dns_san),
        "ip_san": normalize_ip_values(ip_san),
        "not_before": safe_str(peer_cert.get("notBefore"), ""),
        "not_after": safe_str(peer_cert.get("notAfter"), ""),
    }


def ensure_websocket_server_ssl_material(
    *,
    enabled=False,
    certfile=None,
    keyfile=None,
    host=None,
    bind_host=None,
    common_name="",
    backend="auto",
    root_cn=DEFAULT_ROOT_CN,
    root_prefix=DEFAULT_ROOT_PREFIX,
    out_dir=None,
    file_prefix="",
    days=None,
    server_days=DEFAULT_SERVER_DAYS,
    root_days=DEFAULT_ROOT_DAYS,
    key_size=None,
    server_key_size=DEFAULT_SERVER_KEY_SIZE,
    root_key_size=DEFAULT_ROOT_KEY_SIZE,
    country=DEFAULT_COUNTRY,
    state="",
    locality="",
    organization=DEFAULT_ORGANIZATION,
    organizational_unit=DEFAULT_ORGANIZATIONAL_UNIT,
    extra_dns=None,
    extra_ip=None,
    include_loopback=True,
    enable_reverse_dns=True,
    renew_before_days=0,
    client_names=None,
    client_prefix=DEFAULT_CLIENT_PREFIX,
    client_days=DEFAULT_CLIENT_DAYS,
    client_key_size=DEFAULT_CLIENT_KEY_SIZE,
    client_dns=None,
    client_ip=None,
    client_p12_password=DEFAULT_CLIENT_P12_PASSWORD,
    client_p12_password_length=DEFAULT_P12_PASSWORD_LENGTH,
    write_client_password_file=True,
    force=False,
    logger_obj=None,
):
    active_logger = logger_obj or logger

    if not safe_bool(enabled, False):
        return {
            "enabled": False,
            "changed": False,
            "generated": False,
            "repaired": False,
            "action": "skipped_disabled",
            "certfile": safe_str(certfile, "").strip(),
            "keyfile": safe_str(keyfile, "").strip(),
            "chainfile": "",
            "trustfile": "",
            "cafile": "",
            "root_certfile": "",
            "root_keyfile": "",
            "manifest": "",
            "reasons": [],
            "clients": [],
        }

    backend_name = select_backend(backend)
    if backend_name != "cryptography":
        raise RuntimeError("Der CA-signierte Standard erfordert 'cryptography'.")

    normalized_server_days = _normalize_positive_days(days if days is not None else server_days, DEFAULT_SERVER_DAYS)
    normalized_root_days = _normalize_positive_days(root_days, DEFAULT_ROOT_DAYS)
    normalized_server_key_size = _normalize_key_size(key_size if key_size is not None else server_key_size, DEFAULT_SERVER_KEY_SIZE)
    normalized_root_key_size = _normalize_key_size(root_key_size, DEFAULT_ROOT_KEY_SIZE)
    normalized_client_days = _normalize_positive_days(client_days, DEFAULT_CLIENT_DAYS)
    normalized_client_key_size = _normalize_key_size(client_key_size, DEFAULT_CLIENT_KEY_SIZE)

    project_root = _project_root_from_this_file()

    resolved_certfile = _resolve_runtime_path(certfile, project_root) if safe_str(certfile, "").strip() else ""
    resolved_keyfile = _resolve_runtime_path(keyfile, project_root) if safe_str(keyfile, "").strip() else ""

    if resolved_certfile:
        output_paths = build_output_paths_from_server_paths(
            certfile_path=resolved_certfile,
            keyfile_path=resolved_keyfile or None,
            root_prefix=root_prefix,
        )
    else:
        resolved_out_dir = _resolve_runtime_path(out_dir or DEFAULT_OUT_DIR, project_root)
        effective_file_prefix = safe_str(file_prefix, DEFAULT_SERVER_PREFIX).strip() or DEFAULT_SERVER_PREFIX
        output_paths = build_output_paths(
            out_dir=resolved_out_dir,
            file_prefix=effective_file_prefix,
            root_prefix=root_prefix,
        )

    ensure_directory(output_paths["base_dir"])

    effective_bind_host_input = safe_str(bind_host, "").strip() or safe_str(host, "").strip()
    if is_wildcard_host(effective_bind_host_input):
        effective_bind_host_input = ""

    server_context = _prepare_server_generation_context(
        bind_host=effective_bind_host_input or safe_str(host, "").strip(),
        explicit_common_name=common_name,
        extra_dns=extra_dns or [],
        extra_ip=extra_ip or [],
        include_loopback=safe_bool(include_loopback, True),
        enable_reverse_dns=safe_bool(enable_reverse_dns, True),
    )

    effective_bind_host = safe_str(server_context["effective_bind_host"], "").strip()

    root_status = _root_material_status(
        output_paths=output_paths,
        renew_before_days=renew_before_days,
    )
    root_status["root_cert_path"] = str(output_paths["root_cert"])
    root_status["root_key_path"] = str(output_paths["root_key"])

    server_status = _server_material_status(
        output_paths=output_paths,
        root_status=root_status,
        bind_host=effective_bind_host,
        requested_dns=server_context["requested_dns"],
        requested_ip=server_context["requested_ip"],
        discovery=server_context["discovery"],
        renew_before_days=renew_before_days,
    )

    requested_client_items = _gather_client_output_paths(
        out_dir=output_paths["base_dir"],
        client_names=client_names or [],
        client_prefix=client_prefix,
    )

    plan = _calculate_generation_plan(
        output_paths=output_paths,
        root_status=root_status,
        server_status=server_status,
        requested_client_items=requested_client_items,
        force=safe_bool(force, False),
    )

    changed = False
    generated = False
    repaired = False
    root_rotated = False
    client_manifest_items = []
    client_actions = []
    action = "validated"

    root_subject_kwargs = build_subject_kwargs(
        {
            "country": country,
            "state": state,
            "locality": locality,
            "organization": organization,
            "organizational_unit": organizational_unit,
        },
        safe_str(root_cn, DEFAULT_ROOT_CN).strip() or DEFAULT_ROOT_CN,
    )

    server_subject_kwargs = build_subject_kwargs(
        {
            "country": country,
            "state": state,
            "locality": locality,
            "organization": organization,
            "organizational_unit": organizational_unit,
        },
        server_context["common_name"],
    )

    if plan["rotate_root"] or plan["regenerate_server"]:
        _backup_relevant_paths(output_paths, requested_client_items, logger_obj=active_logger)

        existing_root_cert, existing_root_key = _load_root_material(output_paths)

        generation_result = _generate_root_and_server_material(
            output_paths=output_paths,
            discovery={
                "server_dns_names": server_context["dns_names"],
                "server_ip_values": server_context["ip_values"],
            },
            bind_host=effective_bind_host,
            requested_dns=server_context["requested_dns"],
            requested_ip=server_context["requested_ip"],
            common_name=server_context["common_name"],
            root_subject_kwargs=root_subject_kwargs,
            server_subject_kwargs=server_subject_kwargs,
            root_days=normalized_root_days,
            server_days=normalized_server_days,
            root_key_size=normalized_root_key_size,
            server_key_size=normalized_server_key_size,
            root_private_key=None if plan["rotate_root"] or existing_root_key is None or existing_root_cert is None else existing_root_key,
            root_certificate=None if plan["rotate_root"] or existing_root_key is None or existing_root_cert is None else existing_root_cert,
        )

        if plan["rotate_root"] or existing_root_key is None or existing_root_cert is None:
            _write_root_material(
                output_paths=output_paths,
                root_key_pem=generation_result["root_key_pem"],
                root_cert_pem=generation_result["root_cert_pem"],
            )
            root_rotated = True

        _write_server_sidecars(
            output_paths=output_paths,
            server_cert_pem=generation_result["server_cert_pem"],
            server_key_pem=generation_result["server_key_pem"],
            root_cert_pem=generation_result["root_cert_pem"],
        )

        changed = True
        generated = True

        if root_rotated:
            action = "regenerated_root_and_server"
        elif plan["regenerate_server"]:
            if server_status["exists"]:
                action = "regenerated_server"
            else:
                action = "generated_server"

    elif plan["repair_sidecars"]:
        _repair_server_sidecars_from_existing_material(output_paths)
        changed = True
        repaired = True
        action = "repaired_sidecars"

    current_root_cert_obj, current_root_key_obj = _load_root_material(output_paths)
    if current_root_cert_obj is None:
        raise RuntimeError("Root-CA-Zertifikat konnte nach der Verarbeitung nicht geladen werden.")

    if current_root_key_obj is None and requested_client_items:
        raise RuntimeError(
            "Client-Zertifikate wurden angefordert, aber root-ca.key.pem ist nicht verfügbar. "
            "Ohne Root-Schlüssel können keine neuen Client-Zertifikate ausgestellt werden."
        )

    if requested_client_items:
        current_root_key = current_root_key_obj
        if current_root_key is None:
            raise RuntimeError("Root-CA-Schlüssel ist für Client-Zertifikate erforderlich.")

        client_manifest_items, client_actions = _generate_or_update_clients(
            out_dir=output_paths["base_dir"],
            root_private_key=current_root_key,
            root_certificate=current_root_cert_obj,
            client_output_items=requested_client_items,
            client_days=normalized_client_days,
            client_key_size=normalized_client_key_size,
            country=country,
            state=state,
            locality=locality,
            organization=organization,
            organizational_unit=organizational_unit,
            client_dns=client_dns or [],
            client_ip=client_ip or [],
            client_p12_password=client_p12_password,
            client_p12_password_length=client_p12_password_length,
            write_client_password_file=safe_bool(write_client_password_file, True),
            root_rotated=root_rotated,
            force=safe_bool(force, False),
            renew_before_days=renew_before_days,
            root_prefix=root_prefix,
        )

        if any(item["action"] != "validated" for item in client_actions):
            changed = True
            generated = True if any(item["action"].startswith("generated") or "regenerated" in item["action"] for item in client_actions) else generated

    root_info = load_pem_certificate_info(output_paths["root_cert"])
    server_info = load_pem_certificate_info(output_paths["cert"])

    defaults_info = _build_generation_defaults(
        root_cn=root_cn,
        root_prefix=root_prefix,
        file_prefix=output_paths["file_prefix"],
        client_prefix=client_prefix,
        client_p12_password_length=client_p12_password_length,
    )

    manifest_payload = build_ca_signed_manifest(
        output_paths=output_paths,
        discovery=server_context["discovery"],
        bind_host_input=safe_str(bind_host or host, ""),
        effective_bind_host=effective_bind_host,
        requested_dns=server_context["requested_dns"],
        requested_ip=server_context["requested_ip"],
        root_subject=root_subject_kwargs,
        server_subject=server_subject_kwargs,
        server_dns_names=server_context["dns_names"],
        server_ip_values=server_context["ip_values"],
        root_info=root_info,
        server_info=server_info,
        client_items=client_manifest_items,
        defaults=defaults_info,
        action=action,
        reasons=plan["reasons"],
    )
    save_manifest(output_paths["manifest"], manifest_payload)

    expired_after_action, expires_at_after_action = certificate_expired(
        server_info,
        renew_before_days=renew_before_days,
    )

    result = {
        "enabled": True,
        "changed": changed,
        "generated": generated,
        "repaired": repaired,
        "action": action,
        "backend": backend_name,
        "certfile": str(output_paths["cert"]),
        "keyfile": str(output_paths["key"]),
        "chainfile": str(output_paths["chain"]),
        "combinedfile": str(output_paths["combined"]),
        "trustfile": str(output_paths["trust"]),
        "cafile": str(output_paths["root_cert"]),
        "root_certfile": str(output_paths["root_cert"]),
        "root_keyfile": str(output_paths["root_key"]),
        "manifest": str(output_paths["manifest"]),
        "host": effective_bind_host,
        "expires_at": safe_str(expires_at_after_action, ""),
        "expired": bool(expired_after_action),
        "server_dns_san": list(server_context["dns_names"]),
        "server_ip_san": list(server_context["ip_values"]),
        "server_cert_info": server_info,
        "root_cert_info": root_info,
        "clients": client_manifest_items,
        "client_actions": client_actions,
        "reasons": plan["reasons"],
    }

    active_logger.info(
        "CA-TLS-Status: action=%s changed=%s generated=%s repaired=%s certfile=%s keyfile=%s cafile=%s host=%s reasons=%s expires_at=%s",
        result["action"],
        result["changed"],
        result["generated"],
        result["repaired"],
        result["certfile"],
        result["keyfile"],
        result["cafile"],
        result["host"] or "<leer>",
        ",".join(result["reasons"]) if result["reasons"] else "<none>",
        result["expires_at"] or "<unknown>",
    )

    return result


def generate_certificate_material(args_or_namespace):
    args = args_or_namespace

    result = ensure_websocket_server_ssl_material(
        enabled=True,
        certfile=str(Path(_resolve_runtime_path(_namespace_value(args, "out_dir", DEFAULT_OUT_DIR), _project_root_from_this_file())) / ((safe_str(_namespace_value(args, "file_prefix", DEFAULT_SERVER_PREFIX), DEFAULT_SERVER_PREFIX).strip() or DEFAULT_SERVER_PREFIX) + ".cert.pem")),
        keyfile=str(Path(_resolve_runtime_path(_namespace_value(args, "out_dir", DEFAULT_OUT_DIR), _project_root_from_this_file())) / ((safe_str(_namespace_value(args, "file_prefix", DEFAULT_SERVER_PREFIX), DEFAULT_SERVER_PREFIX).strip() or DEFAULT_SERVER_PREFIX) + ".key.pem")),
        host=_namespace_value(args, "bind_host", ""),
        bind_host=_namespace_value(args, "bind_host", ""),
        backend=_namespace_value(args, "backend", "auto"),
        common_name=_namespace_value(args, "common_name", ""),
        root_cn=_namespace_value(args, "root_cn", DEFAULT_ROOT_CN),
        root_prefix=_namespace_value(args, "root_prefix", DEFAULT_ROOT_PREFIX),
        out_dir=_namespace_value(args, "out_dir", DEFAULT_OUT_DIR),
        file_prefix=_namespace_value(args, "file_prefix", DEFAULT_SERVER_PREFIX),
        days=_namespace_value(args, "days", None),
        server_days=_namespace_value(args, "server_days", DEFAULT_SERVER_DAYS),
        root_days=_namespace_value(args, "root_days", DEFAULT_ROOT_DAYS),
        key_size=_namespace_value(args, "key_size", None),
        server_key_size=_namespace_value(args, "server_key_size", DEFAULT_SERVER_KEY_SIZE),
        root_key_size=_namespace_value(args, "root_key_size", DEFAULT_ROOT_KEY_SIZE),
        country=_namespace_value(args, "country", DEFAULT_COUNTRY),
        state=_namespace_value(args, "state", ""),
        locality=_namespace_value(args, "locality", ""),
        organization=_namespace_value(args, "organization", DEFAULT_ORGANIZATION),
        organizational_unit=_namespace_value(args, "organizational_unit", DEFAULT_ORGANIZATIONAL_UNIT),
        extra_dns=_namespace_value(args, "dns", []) or [],
        extra_ip=_namespace_value(args, "ip", []) or [],
        include_loopback=not safe_bool(_namespace_value(args, "no_loopback", False), False),
        enable_reverse_dns=not safe_bool(_namespace_value(args, "no_reverse_dns", False), False),
        renew_before_days=_namespace_value(args, "renew_before_days", 0),
        client_names=_namespace_value(args, "client_name", []) or [],
        client_prefix=_namespace_value(args, "client_prefix", DEFAULT_CLIENT_PREFIX),
        client_days=_namespace_value(args, "client_days", DEFAULT_CLIENT_DAYS),
        client_key_size=_namespace_value(args, "client_key_size", DEFAULT_CLIENT_KEY_SIZE),
        client_dns=_namespace_value(args, "client_dns", []) or [],
        client_ip=_namespace_value(args, "client_ip", []) or [],
        client_p12_password=_namespace_value(args, "client_p12_password", DEFAULT_CLIENT_P12_PASSWORD),
        client_p12_password_length=_namespace_value(args, "client_p12_password_length", DEFAULT_P12_PASSWORD_LENGTH),
        write_client_password_file=not safe_bool(_namespace_value(args, "no_client_password_file", False), False),
        force=safe_bool(_namespace_value(args, "force", False), False),
    )
    return result


def print_summary(result):
    print("Root-CA, signiertes WebSocket-Serverzertifikat und optionale mTLS-Client-Zertifikate sind bereit.\n")
    print("WICHTIG:")
    print("  - root-ca.cert.pem ist die Trust-Datei für Browser/OS und für serverseitige Client-Zertifikatsprüfung.")
    print("  - root-ca.key.pem bleibt lokal und geheim.")
    print("  - websocket_server.cert.pem + websocket_server.key.pem gehören auf den WebSocket-Server.")
    print("  - websocket_server.trust.pem ist ein Kompatibilitäts-Alias und enthält ebenfalls root-ca.cert.pem.")
    print("  - *.key.pem, *.combined.pem und *.p12 niemals unkontrolliert verteilen.\n")

    print("Server-Dateien:")
    print("  Root-CA Zertifikat : " + result["root_certfile"])
    print("  Root-CA Schlüssel  : " + result["root_keyfile"])
    print("  Server Zertifikat  : " + result["certfile"])
    print("  Server Schlüssel   : " + result["keyfile"])
    print("  Server Kette       : " + result["chainfile"])
    print("  Server Combined    : " + result["combinedfile"])
    print("  Trust Alias        : " + result["trustfile"])
    print("  Manifest           : " + result["manifest"] + "\n")

    print("Server-Identität:")
    print("  Effektiver Host    : " + safe_str(result.get("host"), "-"))
    print("  DNS SANs           : " + (", ".join(result.get("server_dns_san") or []) if result.get("server_dns_san") else "-"))
    print("  IP SANs            : " + (", ".join(result.get("server_ip_san") or []) if result.get("server_ip_san") else "-"))
    print("  Aktion             : " + safe_str(result.get("action"), "-"))
    print("  Ablauf             : " + safe_str(result.get("expires_at"), "-") + "\n")

    if result.get("clients"):
        print("mTLS-Clients:")
        for item in result["clients"]:
            print("  Client             : " + item["client_name"])
            print("    Prefix           : " + item["client_prefix"])
            print("    Zertifikat       : " + item["certfile"])
            print("    Schlüssel        : " + item["keyfile"])
            print("    PKCS#12          : " + item["pkcs12"])
            if item.get("pkcs12_password_file"):
                print("    P12 Passwort     : " + item["pkcs12_password_file"])
            if item.get("dns_sans"):
                print("    DNS SANs         : " + ", ".join(item["dns_sans"]))
            if item.get("ip_sans"):
                print("    IP SANs          : " + ", ".join(item["ip_sans"]))
            if item.get("uri_sans"):
                print("    URI SANs         : " + ", ".join(item["uri_sans"]))
        print()

    print("Empfohlene Server-YAML:")
    print("ssl:")
    print("  enabled: true")
    print('  certfile: "' + result["certfile"] + '"')
    print('  keyfile: "' + result["keyfile"] + '"')
    print('  cafile: "' + result["cafile"] + '"')
    print("  require_client_cert: false")
    print("  optional_client_cert: false")
    print()
    print("Hinweis für die spätere app_config-Integration:")
    print("  - root_cn, client_prefix, client_p12_password, out_dir und weitere Defaults sollen")
    print("    im aufrufenden Interface aus app_config.yaml gelesen und an ensure_websocket_server_ssl_material() übergeben werden.")
    print("  - Für Host=0.0.0.0 oder :: wird automatisch ein lokaler Nicht-Loopback-Host ermittelt.")
    print("  - Reverse-DNS wird genutzt, wenn verfügbar; sonst FQDN/Hostname.")


def build_argument_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Erzeugt und validiert eine lokale Root-CA, ein CA-signiertes "
            "WebSocket-Serverzertifikat und optional mTLS-Client-Zertifikate."
        )
    )

    parser.add_argument("--backend", choices=["auto", "cryptography"], default="auto")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--bind-host", default="")
    parser.add_argument("--common-name", default="")
    parser.add_argument("--dns", action="append", default=[])
    parser.add_argument("--ip", action="append", default=[])

    parser.add_argument("--country", default=DEFAULT_COUNTRY)
    parser.add_argument("--state", default="")
    parser.add_argument("--locality", default="")
    parser.add_argument("--organization", default=DEFAULT_ORGANIZATION)
    parser.add_argument("--organizational-unit", default=DEFAULT_ORGANIZATIONAL_UNIT)

    parser.add_argument("--root-cn", default=DEFAULT_ROOT_CN)
    parser.add_argument("--root-prefix", default=DEFAULT_ROOT_PREFIX)
    parser.add_argument("--file-prefix", default=DEFAULT_SERVER_PREFIX)

    parser.add_argument("--root-days", type=int, default=DEFAULT_ROOT_DAYS)
    parser.add_argument("--server-days", type=int, default=DEFAULT_SERVER_DAYS)
    parser.add_argument("--days", type=int, default=None, help="Alias für --server-days")
    parser.add_argument("--root-key-size", type=int, default=DEFAULT_ROOT_KEY_SIZE)
    parser.add_argument("--server-key-size", type=int, default=DEFAULT_SERVER_KEY_SIZE)
    parser.add_argument("--key-size", type=int, default=None, help="Alias für --server-key-size")

    parser.add_argument("--client-name", action="append", default=[])
    parser.add_argument("--client-prefix", default=DEFAULT_CLIENT_PREFIX)
    parser.add_argument("--client-days", type=int, default=DEFAULT_CLIENT_DAYS)
    parser.add_argument("--client-key-size", type=int, default=DEFAULT_CLIENT_KEY_SIZE)
    parser.add_argument("--client-dns", action="append", default=[])
    parser.add_argument("--client-ip", action="append", default=[])
    parser.add_argument("--client-p12-password", default=DEFAULT_CLIENT_P12_PASSWORD)
    parser.add_argument("--client-p12-password-length", type=int, default=DEFAULT_P12_PASSWORD_LENGTH)
    parser.add_argument("--no-client-password-file", action="store_true")

    parser.add_argument("--renew-before-days", type=int, default=0)
    parser.add_argument("--no-loopback", action="store_true")
    parser.add_argument("--no-reverse-dns", action="store_true")
    parser.add_argument("--force", action="store_true")

    return parser


def main(argv=None):
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    result = generate_certificate_material(args)
    print_summary(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
