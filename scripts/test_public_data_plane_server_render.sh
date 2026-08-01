#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
SERVER_PID=""
cleanup() {
  [[ -z "${SERVER_PID}" ]] || kill "${SERVER_PID}" 2>/dev/null || true
  [[ -z "${SERVER_PID}" ]] || wait "${SERVER_PID}" 2>/dev/null || true
  rm -rf "${TMP}"
}
trap cleanup EXIT

command -v helm >/dev/null

cat >"${TMP}/server.py" <<'PY'
import http.server
import json
import sys
import urllib.parse

port_file, request_log, mode_file = sys.argv[1:]
secrets = {
    "fugue-fugue-config": {
        "FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY": "d29ya2xvYWQ=",
        "FUGUE_BUNDLE_SIGNING_KEY": "YnVuZGxl",
        "FUGUE_EDGE_TLS_ASK_TOKEN": "ZWRnZQ==",
        "POSTGRES_PASSWORD": "cGc=",
    },
    "fugue-fugue-control-plane-postgres-app": {"username": "ZnVndWU=", "password": "cGc="},
    "fugue-fugue-platform-component-identity": {
        "FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY": "cGxhdA==",
        "FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID": "aWQ=",
    },
}
groups = {
    "apps": ["Deployment", "DaemonSet", "StatefulSet", "ReplicaSet"],
    "batch": ["CronJob", "Job"],
    "scheduling.k8s.io": ["PriorityClass"],
    "policy": ["PodDisruptionBudget"],
    "storage.k8s.io": ["StorageClass", "CSIDriver", "VolumeAttachment"],
    "apiextensions.k8s.io": ["CustomResourceDefinition"],
    "rbac.authorization.k8s.io": ["ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding"],
    "admissionregistration.k8s.io": ["MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"],
    "postgresql.cnpg.io": ["Cluster", "Backup", "ScheduledBackup"],
    "coordination.k8s.io": ["Lease"],
}
cluster_kinds = {
    "PriorityClass", "StorageClass", "CSIDriver", "CustomResourceDefinition", "ClusterRole",
    "ClusterRoleBinding", "MutatingWebhookConfiguration", "ValidatingWebhookConfiguration",
}

class Handler(http.server.BaseHTTPRequestHandler):
    def reply(self, value, status=200):
        encoded = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        with open(request_log, "a", encoding="utf-8") as target:
            target.write("GET " + self.path + "\n")
        path = urllib.parse.urlparse(self.path).path
        if path == "/version":
            return self.reply({"major": "1", "minor": "31", "gitVersion": "v1.31.0"})
        if path == "/api":
            return self.reply({"kind": "APIVersions", "apiVersion": "v1", "versions": ["v1"], "serverAddressByClientCIDRs": []})
        if path == "/apis":
            return self.reply({
                "kind": "APIGroupList", "apiVersion": "v1",
                "groups": [{
                    "name": group,
                    "versions": [{"groupVersion": group + "/v1", "version": "v1"}],
                    "preferredVersion": {"groupVersion": group + "/v1", "version": "v1"},
                } for group in groups],
            })
        if path == "/api/v1":
            resources = [
                ("serviceaccounts", "ServiceAccount"), ("configmaps", "ConfigMap"),
                ("persistentvolumeclaims", "PersistentVolumeClaim"), ("secrets", "Secret"),
                ("services", "Service"),
            ]
            return self.reply({
                "kind": "APIResourceList", "apiVersion": "v1", "groupVersion": "v1",
                "resources": [{"name": name, "singularName": "", "namespaced": True, "kind": kind, "verbs": ["get", "list"]} for name, kind in resources],
            })
        if path.startswith("/apis/"):
            parts = path.split("/")
            group = parts[2]
            return self.reply({
                "kind": "APIResourceList", "apiVersion": "v1", "groupVersion": group + "/v1",
                "resources": [{
                    "name": kind.lower() + "s", "singularName": kind.lower(),
                    "namespaced": kind not in cluster_kinds, "kind": kind, "verbs": ["get", "list"],
                } for kind in groups.get(group, [])],
            })
        prefix = "/api/v1/namespaces/fugue-system/secrets/"
        if path.startswith(prefix):
            name = path[len(prefix):]
            mode = open(mode_file, encoding="utf-8").read().strip()
            if mode == "missing-postgres" and name == "fugue-fugue-control-plane-postgres-app":
                return self.reply({"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "NotFound", "code": 404}, 404)
            if name in secrets:
                payload = dict(secrets[name])
                if mode == "missing-postgres" and name == "fugue-fugue-config":
                    payload.pop("POSTGRES_PASSWORD", None)
                return self.reply({
                    "apiVersion": "v1", "kind": "Secret",
                    "metadata": {"name": name, "namespace": "fugue-system", "uid": name + "-uid", "resourceVersion": "7"},
                    "data": payload,
                })
        return self.reply({"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": "NotFound", "code": 404}, 404)

    def mutation(self):
        with open(request_log, "a", encoding="utf-8") as target:
            target.write(self.command + " " + self.path + "\n")
        self.reply({"kind": "Status", "status": "Failure", "reason": "Forbidden", "code": 403}, 403)

    do_POST = mutation
    do_PUT = mutation
    do_PATCH = mutation
    do_DELETE = mutation
    def log_message(self, *_):
        pass

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
with open(port_file, "w", encoding="utf-8") as target:
    target.write(str(server.server_address[1]))
server.serve_forever()
PY

: >"${TMP}/requests.log"
printf 'ready\n' >"${TMP}/mode"
python3 "${TMP}/server.py" "${TMP}/port" "${TMP}/requests.log" "${TMP}/mode" &
SERVER_PID=$!
for _ in $(seq 1 100); do
  [[ -s "${TMP}/port" ]] && break
  sleep 0.02
done
[[ -s "${TMP}/port" ]]
PORT="$(cat "${TMP}/port")"

cat >"${TMP}/kubeconfig" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: fixture
  cluster:
    server: http://127.0.0.1:${PORT}
contexts:
- name: fixture
  context: {cluster: fixture, user: fixture, namespace: fugue-system}
current-context: fixture
users:
- name: fixture
  user: {}
EOF

render=(helm template fugue "${ROOT}/deploy/helm/fugue"
  --namespace fugue-system --is-upgrade --no-hooks --dry-run=server
  --disable-openapi-validation --kubeconfig "${TMP}/kubeconfig"
  --set controlPlanePostgres.enabled=true --set controlPlanePostgres.useForAPI=true)

"${render[@]}" >"${TMP}/rendered.yaml" 2>"${TMP}/rendered.stderr"
grep -q 'postgres://fugue:pg@fugue-fugue-control-plane-postgres-rw' "${TMP}/rendered.yaml"
if grep -Ev '^GET ' "${TMP}/requests.log" | grep -q .; then
  printf 'server render attempted a mutating Kubernetes request\n' >&2
  exit 1
fi

printf 'missing-postgres\n' >"${TMP}/mode"
if "${render[@]}" >"${TMP}/missing.yaml" 2>"${TMP}/missing.stderr"; then
  printf 'server render accepted a missing live postgres Secret\n' >&2
  exit 1
fi
grep -q 'requires an existing control-plane postgres secret' "${TMP}/missing.stderr"
if grep -Ev '^GET ' "${TMP}/requests.log" | grep -q .; then
  printf 'failed server render attempted a mutating Kubernetes request\n' >&2
  exit 1
fi

printf 'public data-plane real Helm server lookup render passed\n'
