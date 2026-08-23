# -*- coding: utf-8 -*-

"""Real TLS socket smoke test for the standard-library MQTT client."""

import os
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time

from functools import partial


PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.library._lightweight_mqtt_client import (
    _fixed_packet,
    build_publish_packet,
    create_mqtt_client,
    parse_publish_packet,
    read_mqtt_packet,
)


def _run_openssl(openssl, arguments):
    subprocess.run([openssl] + arguments, check=True, capture_output=True)


def _generate_certificates(directory):
    openssl = shutil.which("openssl")
    if not openssl:
        raise RuntimeError("openssl executable is required for TLS smoke test")
    ca_cert = os.path.join(directory, "ca.crt")
    ca_key = os.path.join(directory, "ca.key")
    server_cert = os.path.join(directory, "server.crt")
    server_key = os.path.join(directory, "server.key")
    server_csr = os.path.join(directory, "server.csr")
    client_cert = os.path.join(directory, "client.crt")
    client_key = os.path.join(directory, "client.key")
    client_csr = os.path.join(directory, "client.csr")
    _run_openssl(openssl, [
        "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", ca_key, "-out", ca_cert, "-days", "1",
        "-subj", "/CN=MQTT Test CA",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign",
    ])
    _run_openssl(openssl, [
        "req", "-new", "-newkey", "rsa:2048", "-nodes",
        "-keyout", server_key, "-out", server_csr, "-subj", "/CN=localhost",
        "-addext", "subjectAltName=DNS:localhost",
        "-addext", "extendedKeyUsage=serverAuth",
    ])
    _run_openssl(openssl, [
        "x509", "-req", "-in", server_csr, "-CA", ca_cert, "-CAkey", ca_key,
        "-CAcreateserial", "-out", server_cert, "-days", "1",
        "-copy_extensions", "copy",
    ])
    _run_openssl(openssl, [
        "req", "-new", "-newkey", "rsa:2048", "-nodes",
        "-keyout", client_key, "-out", client_csr,
        "-subj", "/CN=automation-test-node",
        "-addext", "extendedKeyUsage=clientAuth",
    ])
    _run_openssl(openssl, [
        "x509", "-req", "-in", client_csr, "-CA", ca_cert, "-CAkey", ca_key,
        "-CAserial", os.path.join(directory, "ca.srl"),
        "-out", client_cert, "-days", "1", "-copy_extensions", "copy",
    ])
    return ca_cert, server_cert, server_key, client_cert, client_key


def _broker_packet(connection, state):
    first_byte, payload = read_mqtt_packet(connection, 1024 * 1024)
    packet_type = first_byte >> 4
    if packet_type == 8:
        packet_id = struct.unpack("!H", payload[:2])[0]
        connection.sendall(_fixed_packet(0x90, struct.pack("!HB", packet_id, 1)))
        incoming = build_publish_packet(
            "incoming/test", b"from-broker", qos=1, packet_id=77)
        connection.sendall(incoming)
    elif packet_type == 3:
        message = parse_publish_packet(first_byte, payload)
        state["outbound"] = message
        if message["qos"] == 1:
            connection.sendall(_fixed_packet(
                0x40, struct.pack("!H", message["packet_id"])))
    elif packet_type == 4 and len(payload) == 2:
        state["incoming_acked"] = struct.unpack("!H", payload)[0] == 77
    elif packet_type == 12:
        connection.sendall(b"\xd0\x00")
    return packet_type != 14


def _broker_session(connection, state):
    connection.settimeout(0.2)
    first_byte, _payload = read_mqtt_packet(connection, 1024 * 1024)
    assert first_byte >> 4 == 1
    connection.sendall(b"\x20\x02\x00\x00")
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            if not _broker_packet(connection, state):
                break
        except socket.timeout:
            if _broker_complete(state):
                break


def _broker_loop(server, ca_file, cert_file, key_file, state):
    try:
        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_context.minimum_version = ssl.TLSVersion.TLSv1_2
        tls_context.load_cert_chain(cert_file, key_file)
        tls_context.load_verify_locations(cafile=ca_file)
        tls_context.verify_mode = ssl.CERT_REQUIRED
        raw, _address = server.accept()
        with tls_context.wrap_socket(raw, server_side=True) as connection:
            state["client_cert"] = connection.getpeercert()
            _broker_session(connection, state)
        state["ok"] = True
    except Exception as exc:
        if not _broker_complete(state):
            state["error"] = repr(exc)
    finally:
        server.close()


def _capture_message(bucket, message):
    bucket.append(message)


def _wait_for(predicate, timeout_s=5.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _broker_complete(state):
    return bool(state.get("outbound") and state.get("incoming_acked"))


def _run_test():
    with tempfile.TemporaryDirectory(prefix="mqtt_tls_") as directory:
        certificates = _generate_certificates(directory)
        ca_file, cert_file, key_file, client_cert, client_key = certificates
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        state = {}
        thread = threading.Thread(
            target=partial(
                _broker_loop, server, ca_file, cert_file, key_file, state),
            name="TestMQTTTLSBroker",
            daemon=False,
        )
        thread.start()
        messages = []
        client = create_mqtt_client(
            host="127.0.0.1",
            port=port,
            client_id="tls-smoke-client",
            keepalive_s=2,
            reconnect_min_s=0.1,
            reconnect_max_s=0.2,
            subscriptions=[{"topic": "incoming/test", "qos": 1}],
            on_message=partial(_capture_message, messages),
            tls={
                "enabled": True,
                "ca_file": ca_file,
                "cert_file": client_cert,
                "key_file": client_key,
                "server_hostname": "localhost",
            },
        )
        client["start"]()
        assert client["wait_connected"](3.0)
        assert client["publish"]("outgoing/test", b"from-client", qos=1)
        assert _wait_for(partial(_broker_complete, state), 5.0)
        assert _wait_for(partial(bool, messages), 2.0)
        client["stop"](join_timeout_s=3.0)
        thread.join(timeout=3.0)
        assert not thread.is_alive()
        assert not state.get("error"), state.get("error")
        assert state.get("client_cert")
        assert state["outbound"]["payload"] == b"from-client"
        assert messages[0]["payload"] == b"from-broker"
        assert client["snapshot"]()["pending_qos1"] == 0


if __name__ == "__main__":
    _run_test()
    print("PASS lightweight MQTT mTLS, subscribe and bidirectional QoS 1")
