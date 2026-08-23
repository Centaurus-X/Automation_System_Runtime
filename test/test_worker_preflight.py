# -*- coding: utf-8 -*-

"""Worker identity installer and startup preflight integration test."""

import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile

import yaml


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.system.worker_preflight import validate_worker_environment
from src.library._generate_ca_signed_certs import (
    DEFAULT_CLIENT_P12_PASSWORD,
    _resolve_client_p12_password,
)


DEVICE_CONFIG = os.path.join(
    PROJECT_ROOT, "_config", "config_data",
    "device_state_data_v9_node_fn-01.json",
)
INSTALLER = os.path.join(PROJECT_ROOT, "install_mqtt_identity.sh")
WORKER_LAUNCHER = os.path.join(PROJECT_ROOT, "run_worker.sh")


def _shell_path(path):
    if os.name != "nt":
        return str(path)
    executable = shutil.which("cygpath")
    if not executable:
        raise RuntimeError("cygpath is required for Windows shell tests")
    result = subprocess.run(
        [executable, "-u", str(path)],
        check=True, capture_output=True, text=True,
    )
    return result.stdout.strip()


def _shell_command(script, arguments=None):
    values = []
    for value in arguments or ():
        values.append(_shell_path(value) if os.path.isabs(str(value)) else str(value))
    command = [_shell_path(script)] + values
    if os.name != "nt":
        return command
    executable = shutil.which("sh")
    if not executable:
        raise RuntimeError("sh is required for Windows shell tests")
    return [executable] + command




def _openssl(executable, arguments):
    subprocess.run(
        [executable] + arguments,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _generate_identity(directory):
    executable = shutil.which("openssl")
    if not executable:
        raise RuntimeError("openssl is required for Worker identity test")
    ca_cert = os.path.join(directory, "ca.crt")
    ca_key = os.path.join(directory, "ca.key")
    client_cert = os.path.join(directory, "mqtt.crt")
    client_key = os.path.join(directory, "mqtt.key")
    client_csr = os.path.join(directory, "mqtt.csr")
    _openssl(executable, [
        "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", ca_key, "-out", ca_cert, "-days", "1",
        "-subj", "/CN=Worker Test CA",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign",
    ])
    _openssl(executable, [
        "req", "-new", "-newkey", "rsa:2048", "-nodes",
        "-keyout", client_key, "-out", client_csr,
        "-subj", "/CN=urn:automation:site-main:worker:fn-01",
        "-addext", "subjectAltName=URI:urn:automation:site-main:worker:fn-01",
        "-addext", "extendedKeyUsage=clientAuth",
    ])
    _openssl(executable, [
        "x509", "-req", "-in", client_csr,
        "-CA", ca_cert, "-CAkey", ca_key, "-CAcreateserial",
        "-out", client_cert, "-days", "1", "-copy_extensions", "copy",
    ])


def _install_identity(directory):
    release_root = os.path.join(directory, "worker-release")
    source_root = os.path.join(directory, "source")
    os.makedirs(release_root)
    os.makedirs(source_root)
    shutil.copy2(INSTALLER, os.path.join(release_root, "install_mqtt_identity.sh"))
    _generate_identity(source_root)
    command = _shell_command(
        os.path.join(release_root, "install_mqtt_identity.sh"),
        (
            "--source-dir", source_root,
            "--site-id", "site-main",
            "--node-id", "fn-01",
        ),
    )
    first = subprocess.run(command, check=True, capture_output=True, text=True)
    second = subprocess.run(command, check=True, capture_output=True, text=True)
    assert "identity installed" in first.stdout
    assert "already installed and valid" in second.stdout
    destination = os.path.join(release_root, "_config", "certs")
    key_file = os.path.join(destination, "worker_fn-01.key")
    assert stat.S_IMODE(os.stat(key_file).st_mode) == 0o600
    return destination


def _write_config(
        path, identity_directory, site_id="site-main",
        cache_root="/tmp/automation_system_pycache_{uid}"):
    base = {
        "app": {"site_id": site_id, "node_id": "fn-01"},
        "runtime": {
            "data_center": {"json_path": DEVICE_CONFIG},
            "python_cache": {
                "bytecode_enabled": False,
                "project_cache_forbidden": True,
                "root_template": cache_root,
            },
        },
        "thread_management": {
            "controllers_enabled": False,
            "connections_enabled": False,
        },
        "mqtt_interface": {
            "enabled": True,
            "required": True,
            "startup_connection_required": False,
            "transport": {
                "tls": {
                    "enabled": True,
                    "ca_file": os.path.join(identity_directory, "mqtt_ca.crt"),
                    "cert_file": os.path.join(
                        identity_directory, "worker_fn-01.crt"),
                    "key_file": os.path.join(
                        identity_directory, "worker_fn-01.key"),
                    "require_ca_file": True,
                    "require_client_certificate": True,
                    "require_identity_uri": True,
                    "enforce_private_key_permissions": True,
                },
            },
        },
        "distributed_runtime": {"enabled": True, "role": "worker"},
    }
    with open(path, "w", encoding="utf-8") as handle:
        yaml.safe_dump({"active_profile": "base", "base": base}, handle)


def _run_test():
    assert DEFAULT_CLIENT_P12_PASSWORD is None
    generated_password, was_generated = _resolve_client_p12_password(None, 24)
    assert was_generated is True
    assert len(generated_password) >= 24
    standalone = validate_worker_environment(active_profile="standalone")
    assert standalone["ok"], standalone
    assert standalone["python_bytecode_enabled"] is False
    temporary_root = os.path.realpath("/tmp")
    assert os.path.commonpath((
        standalone["python_cache_root"], temporary_root,
    )) == temporary_root
    before = tuple(sorted(
        os.path.join(root, name)
        for root, directories, files in os.walk(PROJECT_ROOT)
        for name in files
        if os.path.basename(root) == "__pycache__" and name.endswith(".pyc")
    ))
    launcher_environment = dict(os.environ)
    launcher_environment["AUTOMATION_PROFILE"] = "standalone"
    launcher_environment["PYTHON_BIN"] = _shell_path(sys.executable)
    if os.name == "nt":
        launcher_environment["AUTOMATION_CACHE_USER_ID"] = "user"
        launcher_environment["MSYS2_ENV_CONV_EXCL"] = (
            "PYTHONPATH;PYTHONPYCACHEPREFIX"
        )
    launched = subprocess.run(
        _shell_command(WORKER_LAUNCHER, ("--preflight",)),
        cwd=PROJECT_ROOT,
        env=launcher_environment,
        check=True,
        capture_output=True,
        text=True,
    )
    launch_report = json.loads(launched.stdout)
    assert launch_report["ok"] is True
    assert launch_report["python_bytecode_enabled"] is False
    after = tuple(sorted(
        os.path.join(root, name)
        for root, directories, files in os.walk(PROJECT_ROOT)
        for name in files
        if os.path.basename(root) == "__pycache__" and name.endswith(".pyc")
    ))
    assert after == before
    assert standalone["mqtt_enabled"] is False
    assert standalone["distributed_enabled"] is False
    if os.name == "nt":
        print("SKIP POSIX identity installer permission test on Windows")
        return
    with tempfile.TemporaryDirectory(prefix="worker-preflight-") as directory:
        identity = _install_identity(directory)
        valid_config = os.path.join(directory, "valid.yaml")
        _write_config(valid_config, identity)
        valid = validate_worker_environment(valid_config, "base")
        assert valid["ok"], valid
        mismatch_config = os.path.join(directory, "mismatch.yaml")
        _write_config(mismatch_config, identity, site_id="another_site")
        mismatch = validate_worker_environment(mismatch_config, "base")
        assert mismatch["ok"] is False
        assert "URI identity mismatch" in mismatch["message"]
        unsafe_config = os.path.join(directory, "unsafe-cache.yaml")
        _write_config(
            unsafe_config,
            identity,
            cache_root=os.path.join(PROJECT_ROOT, "__pycache__"),
        )
        unsafe = validate_worker_environment(unsafe_config, "base")
        assert unsafe["ok"] is False
        assert "must be below /tmp" in unsafe["message"]


if __name__ == "__main__":
    _run_test()
    print("PASS Worker identity install, preflight and identity binding")
