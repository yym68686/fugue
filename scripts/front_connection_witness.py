"""A held, non-reconnecting TLS connection and its exact Front runtime fact."""
import datetime
import http.client
import ipaddress
import secrets
import socket
import ssl
import re
import shutil
import subprocess
import sys
import json
from pathlib import Path

try:
    from . import observe_front_candidate as front
    from . import read_front_conntrack as conntrack
except ImportError:
    import observe_front_candidate as front
    import read_front_conntrack as conntrack


class HeldTLS:
    def __init__(self, address, host, port=443):
        ipaddress.ip_address(address)
        self.host = host
        self.target_address, self.target_port = address, port
        context = ssl.create_default_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        raw = socket.create_connection((address, port), timeout=5)
        try:
            self.socket = context.wrap_socket(raw, server_hostname=host)
        except BaseException:
            raw.close()
            raise
        self.certificate_until = datetime.datetime.fromtimestamp(ssl.cert_time_to_seconds(self.socket.getpeercert()["notAfter"]), datetime.timezone.utc)
        self.client_address, self.client_port = self.socket.getsockname()[:2]
        self.fd = self.socket.fileno()
        self.requests = 0
        self.closed = False

    def proof(self, path):
        if self.closed or self.socket.fileno() != self.fd or self.fd < 0 or not path.startswith("/") or any(c in path for c in "\r\n"):
            raise ValueError("held Front connection is unavailable")
        nonce = secrets.token_hex(16)
        request = "HEAD " + path + " HTTP/1.1\r\nHost: " + self.host + "\r\nX-Fugue-Route-Probe: 1\r\nX-Fugue-Route-Probe-Nonce: " + nonce + "\r\nConnection: keep-alive\r\n\r\n"
        self.socket.sendall(request.encode("ascii"))
        response = http.client.HTTPResponse(self.socket)
        response.begin()
        try:
            proof = front.parse_proof(response, nonce, self.certificate_until)
            if response.will_close or response.read(1):
                raise ValueError("held Front connection was closed or returned a nonempty proof body")
            if self.socket.fileno() != self.fd:
                raise ValueError("held Front connection was replaced")
            self.requests += 1
            return proof
        finally:
            response.close()

    def close(self):
        self.closed = True
        self.socket.close()


def translated_tuple(raw, held, pod_ip):
    if len(raw) > 4 << 20:
        raise ValueError("connection tracking observation exceeds bound")
    matches = []
    for line in raw.splitlines():
        tokens = line.split()
        if "tcp" not in tokens or "ESTABLISHED" not in tokens or "[UNREPLIED]" in tokens or any(t.startswith("zone=") and t != "zone=0" for t in tokens):
            continue
        fields = {key: re.findall(r"(?:^|\s)"+key+r"=([^\s]+)", line) for key in ["src", "dst", "sport", "dport"]}
        if any(len(values) != 2 for values in fields.values()):
            continue
        try:
            original = (str(ipaddress.ip_address(fields["src"][0])), str(ipaddress.ip_address(fields["dst"][0])), int(fields["sport"][0]), int(fields["dport"][0]))
            reply = (str(ipaddress.ip_address(fields["src"][1])), str(ipaddress.ip_address(fields["dst"][1])), int(fields["sport"][1]), int(fields["dport"][1]))
        except ValueError:
            continue
        if original == (held.client_address, held.target_address, held.client_port, held.target_port) and reply[0] == pod_ip and reply[2] == 443 and 1 <= reply[3] <= 65535:
            matches.append((reply[1], reply[3]))
    if len(matches) != 1:
        raise ValueError("held connection lacks one exact original/reply kernel mapping")
    return matches[0]


def observe_translation(held, pod_ip):
    if not getattr(held, "target_address", None) or not getattr(held, "target_port", None):
        raise ValueError("held connection target identity is unavailable")
    try:
        return conntrack.read(held, pod_ip)
    except OSError as error:
        # Some runners keep Kubernetes administration separate from the local
        # root identity. The fixed helper has only the single CT_GET operation;
        # no shell, table dump, flush or connection mutation is exposed.
        if error.errno in [1, 13] and shutil.which("sudo"):
            helper = str(Path(conntrack.__file__).resolve())
            result = subprocess.run(["sudo", "-n", sys.executable, helper, "--source", held.client_address, "--destination", held.target_address, "--source-port", str(held.client_port), "--destination-port", str(held.target_port), "--pod", pod_ip], capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and len(result.stdout) <= 4096:
                value = json.loads(result.stdout)
                if set(value) != {"address", "port"} or type(value["port"]) is not int or not 1 <= value["port"] <= 65535:
                    raise ValueError("privileged exact conntrack observation is invalid")
                return str(ipaddress.ip_address(value["address"])), value["port"]
    binary = shutil.which("conntrack")
    if binary:
        result = subprocess.run([binary, "-L", "-p", "tcp", "--orig-src", held.client_address, "--orig-dst", held.target_address, "--orig-port-src", str(held.client_port), "--orig-port-dst", str(held.target_port), "-o", "extended"], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            return translated_tuple(result.stdout, held, pod_ip)
    for path in ["/proc/net/nf_conntrack", "/proc/net/ip_conntrack"]:
        try:
            with Path(path).open() as stream:
                raw = stream.read((4 << 20)+1)
        except OSError:
            continue
        return translated_tuple(raw, held, pod_ip)
    raise ValueError("kernel connection tracking is unavailable for exact NAT attribution")


def fact(profile, pod, held):
    namespace, name = profile["namespace"], pod["metadata"]["name"]
    value = front.read("get", "--raw", "/api/v1/namespaces/" + namespace + "/pods/" + name + ":7831/proxy/edge/tcp-connections")
    active = value.get("active")
    if not isinstance(active, list) or type(value.get("count")) is not int or value["count"] != len(active) or len(active) > 16384:
        raise ValueError("Front connection observation is incomplete")
    def select(expected, expected_port):
        matches = []
        for item in active:
            host, separator, port = item.get("downstream_remote", "").rpartition(":")
            if not separator or not port.isdigit():
                continue
            if str(ipaddress.ip_address(host.strip("[]"))) == expected and int(port) == expected_port and item.get("protocol") == "https":
                matches.append(item)
        return matches
    matches = select(str(ipaddress.ip_address(held.client_address)), held.client_port)
    source_evidence = "socket"
    if not matches and pod.get("status", {}).get("podIP"):
        # Node-originated Service flows may be SNATed. Do not guess the node's
        # address: require the exact kernel original/reply tuple for this held
        # socket, destination and candidate Pod before matching the Front fact.
        address, port = observe_translation(held, pod["status"]["podIP"])
        matches = select(address, port)
        source_evidence = "kernel_conntrack"
    if len(matches) != 1 or not matches[0].get("id"):
        raise ValueError("held connection cannot be attributed to exactly one Front executor")
    item = matches[0]
    return {"pod_uid": pod["metadata"]["uid"], "connection_id": item["id"], "started_at": item["started_at"], "slot": item["slot"], "target": item["target"], "source_evidence": source_evidence}


def observe_pair(profile, address, port):
    import time
    old = front.pod(profile, profile["legacySelector"], False)
    candidate = front.pod(profile, profile["candidateSelector"], True)
    target = profile["probes"][0]
    before = front.state(profile, old)
    if before != front.state(profile, candidate):
        raise ValueError("held-connection observation has different activation authority")
    current, next_connection = None, None
    try:
        current = HeldTLS(address, target["host"])
        next_connection = HeldTLS(address, target["host"], port)
        observations = []
        original, next_original = None, None
        for index in range(3):
            previous_proof, next_proof = current.proof(target["path"]), next_connection.proof(target["path"])
            if previous_proof != next_proof or next_proof["edge"] != profile["node"] or next_proof["group"] != profile["group"]:
                raise ValueError("held Front connections do not serve equivalent authority")
            previous_fact, next_fact = fact(profile, old, current), fact(profile, candidate, next_connection)
            if index == 0:
                original, next_original = previous_fact, next_fact
            elif original != previous_fact or next_original != next_fact:
                raise ValueError("Front replaced a held connection during observation")
            observations.append({"at": front.now().isoformat(), "old_connection": previous_fact, "candidate_connection": next_fact, "proof": previous_proof})
            if index < 2:
                time.sleep(5)
        if front.state(profile, old) != before or front.state(profile, candidate) != before:
            raise ValueError("activation changed across held connections")
        return {"authorizes_traffic": False, "observations": observations, "old_requests": current.requests, "candidate_requests": next_connection.requests}
    finally:
        if current is not None:
            current.close()
        if next_connection is not None:
            next_connection.close()


def main():
    import argparse
    import json
    from pathlib import Path
    try:
        from . import reconcile_front_probe_transport as transport
    except ImportError:
        import reconcile_front_probe_transport as transport
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    config = transport.validate(json.loads(Path(args.config).read_text()))
    observations = []
    for listener in config["listeners"]:
        profile = front.validate(json.loads(Path(listener["observation"]).read_text()))
        observations.append({"listener": listener["name"], "profile_digest": transport.digest(profile), "witness": observe_pair(profile, listener["address"], listener["port"])})
    result = {"schema": "fugue.front-held-connection-evidence/v1", "authorizes_traffic": False, "declaration_digest": transport.digest(config), "observations": observations}
    Path(args.evidence).write_text(front.canonical(result) + "\n")
    print(front.canonical({"held_connections_verified": True, "authorizes_traffic": False, "listeners": len(observations)}))


if __name__ == "__main__":
    main()
