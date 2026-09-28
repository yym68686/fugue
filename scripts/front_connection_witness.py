"""A held, non-reconnecting TLS connection and its exact Front runtime fact."""
import datetime
import http.client
import ipaddress
import secrets
import socket
import ssl

try:
    from . import observe_front_candidate as front
except ImportError:
    import observe_front_candidate as front


class HeldTLS:
    def __init__(self, address, host, port=443):
        ipaddress.ip_address(address)
        self.host = host
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


def fact(profile, pod, held):
    namespace, name = profile["namespace"], pod["metadata"]["name"]
    value = front.read("get", "--raw", "/api/v1/namespaces/" + namespace + "/pods/" + name + ":7831/proxy/edge/tcp-connections")
    active = value.get("active")
    if not isinstance(active, list) or type(value.get("count")) is not int or value["count"] != len(active) or len(active) > 16384:
        raise ValueError("Front connection observation is incomplete")
    expected = str(ipaddress.ip_address(held.client_address))
    matches = []
    for item in active:
        remote = item.get("downstream_remote", "")
        host, separator, port = remote.rpartition(":")
        if not separator or not port.isdigit():
            continue
        host = host.strip("[]")
        if str(ipaddress.ip_address(host)) == expected and int(port) == held.client_port and item.get("protocol") == "https":
            matches.append(item)
    if len(matches) != 1 or not matches[0].get("id"):
        raise ValueError("held connection cannot be attributed to exactly one Front executor")
    item = matches[0]
    return {"pod_uid": pod["metadata"]["uid"], "connection_id": item["id"], "started_at": item["started_at"], "slot": item["slot"], "target": item["target"]}


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
