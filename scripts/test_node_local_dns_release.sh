#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

export MOCK_LOG="${TMP_DIR}/kubectl.log"
export MOCK_APPLY="${TMP_DIR}/applied.json"
export MOCK_DS_STATE="${TMP_DIR}/daemonset.exists"
MOCK_KUBECTL="${TMP_DIR}/kubectl"

cat >"${MOCK_KUBECTL}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${MOCK_LOG}"
args="$*"

if [[ "${args}" == *"apply -f -"* ]]; then
  payload="$(cat)"
  printf '%s' "${payload}" >"${MOCK_APPLY}"
  if [[ "${MOCK_USE_DS_STATE:-false}" == "true" && "${payload}" == *'"kind":"DaemonSet"'* ]]; then
    : >"${MOCK_DS_STATE}"
  fi
  exit 0
fi
if [[ "${args}" == *"get pod "*"jsonpath={.status.phase}"* ]]; then
  if [[ -n "${MOCK_PROBE_FAIL_PURPOSE:-}" && "${args}" == *"${MOCK_PROBE_FAIL_PURPOSE}"* ]]; then
    printf 'Failed'
  else
    printf 'Succeeded'
  fi
  exit 0
fi
if [[ "${args}" == *" logs "* || "${args}" == *" delete pod "* ]]; then
  exit 0
fi
if [[ "${args}" == *"get service kube-dns -o json"* ]]; then
  printf '{"spec":{"clusterIP":"10.43.0.10","selector":{"k8s-app":"kube-dns"}}}'
  exit 0
fi
if [[ "${args}" == *"get endpoints kube-dns -o json"* ]]; then
  printf '{"subsets":[{"addresses":[{"ip":"10.42.0.2"}]}]}'
  exit 0
fi
if [[ "${args}" == *"get endpoints fugue-fugue-dns-upstream -o json"* ]]; then
  printf '{"subsets":[{"addresses":[{"ip":"10.42.0.2"}]}]}'
  exit 0
fi
if [[ "${args}" == *"get configmap coredns"* ]]; then
  printf '.:53 { kubernetes cluster.local in-addr.arpa ip6.arpa }'
  exit 0
fi
if [[ "${args}" == *"get configmap fugue-fugue-node-local-dns"* ]]; then
  cat <<'EOF_COREFILE'
cluster.local:53 {
  errors
  cache {
    success 9984 30
    denial 9984 5
  }
  reload
  loop
  bind 169.254.20.10
  forward . __PILLAR__CLUSTER__DNS__ {
    force_tcp
  }
  prometheus :9253
  health 169.254.20.10:8080
}
in-addr.arpa:53 {
  errors
  cache 30
  reload
  loop
  bind 169.254.20.10
  forward . __PILLAR__CLUSTER__DNS__ {
    force_tcp
  }
  prometheus :9253
}
ip6.arpa:53 {
  errors
  cache 30
  reload
  loop
  bind 169.254.20.10
  forward . __PILLAR__CLUSTER__DNS__ {
    force_tcp
  }
  prometheus :9253
}
.:53 {
  errors
  cache 30
  reload
  loop
  bind 169.254.20.10
  forward . __PILLAR__CLUSTER__DNS__ {
    force_tcp
  }
  prometheus :9253
}
EOF_COREFILE
  exit 0
fi
if [[ "${args}" == *"get nodes -l kubernetes.io/os=linux --no-headers"* ]]; then
  printf 'vps-84c8f0a9 Ready control-plane 1d v1.35.4\n'
  exit 0
fi
if [[ "${args}" == *"get pods --all-namespaces --field-selector spec.nodeName=vps-84c8f0a9 -o json"* ]]; then
  if [[ "${MOCK_HOSTPORT_CONFLICT:-false}" == "true" ]]; then
    printf '{"items":[{"metadata":{"namespace":"fugue-system","name":"fugue-fugue-dns-country-de-test"},"status":{"phase":"Running"},"spec":{"hostNetwork":false,"containers":[{"name":"dns","ports":[{"containerPort":53,"hostPort":53,"protocol":"UDP"},{"containerPort":53,"hostPort":53,"protocol":"TCP"}]}]}}]}'
  else
    printf '{"items":[]}'
  fi
  exit 0
fi
if [[ "${args}" == *"get node vps-84c8f0a9"*"status.conditions"* ]]; then
  printf 'True'
  exit 0
fi
if [[ "${args}" == *"get node vps-84c8f0a9"*"kubernetes\\.io/os"* ]]; then
  printf 'linux'
  exit 0
fi
if [[ "${args}" == *"get node vps-84c8f0a9"*"kubernetes\\.io/arch"* ]]; then
  printf 'amd64'
  exit 0
fi
if [[ "${args}" == *"get daemonset fugue-fugue-node-local-dns"* ]]; then
  [[ "${MOCK_DS_GET_ERROR:-false}" != "true" ]] || exit 1
  if [[ "${MOCK_USE_DS_STATE:-false}" == "true" ]]; then
    [[ -e "${MOCK_DS_STATE}" ]] || {
      [[ "${args}" == *"--ignore-not-found"* ]] && exit 0
      exit 1
    }
  else
    [[ -n "${MOCK_LIVE_MODE:-}" ]] || {
      [[ "${args}" == *"--ignore-not-found"* ]] && exit 0
      exit 1
    }
  fi
  if [[ "${args}" == *"--ignore-not-found -o name"* ]]; then
    printf 'daemonset.apps/fugue-fugue-node-local-dns'
  elif [[ "${args}" == *" -o json" ]]; then
    printf '{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"fugue-fugue-node-local-dns","namespace":"kube-system","labels":{"app.kubernetes.io/name":"fugue","app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"node-local-dns"}},"spec":{"selector":{"matchLabels":{"app.kubernetes.io/name":"fugue","app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"node-local-dns"}},"template":{"metadata":{"labels":{"app.kubernetes.io/name":"fugue","app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"node-local-dns","fugue.io/node-local-dns-mode":"shadow"}},"spec":{"nodeSelector":{"kubernetes.io/os":"linux","kubernetes.io/hostname":"vps-84c8f0a9"},"containers":[{"name":"node-cache","image":"registry.k8s.io/dns/k8s-dns-node-cache@sha256:bc6e64e2c85956af2fcc0aa720086410d41b4f31f378c9a92646fecc85cd4739","args":["-localip","169.254.20.10","-conf","/etc/Corefile","-upstreamsvc","fugue-fugue-dns-upstream"]}]}}}}'
  elif [[ "${args}" == *"spec.updateStrategy.type"* ]]; then
    printf 'RollingUpdate'
  elif [[ "${args}" == *"node-local-dns-mode"* ]]; then
    printf '%s' "${MOCK_LIVE_MODE}"
  elif [[ "${args}" == *"kubernetes\\.io/hostname"* ]]; then
    printf '%s' "${MOCK_LIVE_NODE:-vps-84c8f0a9}"
  fi
  exit 0
fi
if [[ "${args}" == *"delete daemonset fugue-fugue-node-local-dns"* ]]; then
  [[ "${MOCK_DS_DELETE_ERROR:-false}" != "true" ]] || exit 1
  rm -f "${MOCK_DS_STATE}"
  exit 0
fi
if [[ "${args}" == *"rollout status ds/fugue-fugue-node-local-dns"* ]]; then
  exit 0
fi
if [[ "${args}" == *"get pods -l app.kubernetes.io/component=node-local-dns"*"jsonpath="* ]]; then
  printf 'fugue-fugue-node-local-dns-test\tvps-84c8f0a9\n'
  exit 0
fi
if [[ "${args}" == *"get pods -l app.kubernetes.io/name=fugue,app.kubernetes.io/instance=fugue,app.kubernetes.io/component=node-local-dns --no-headers"* ]]; then
  if [[ "${MOCK_USE_DS_STATE:-false}" == "true" && -e "${MOCK_DS_STATE}" ]]; then
    printf 'fugue-fugue-node-local-dns-test 1/1 Running 0 1m\n'
  fi
  exit 0
fi
if [[ "${args}" == *" exec fugue-fugue-node-local-dns-test "* ]]; then
  exit 0
fi
exit 1
EOF
chmod +x "${MOCK_KUBECTL}"

export FUGUE_UPGRADE_LIB_ONLY=true
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"

if command -v docker >/dev/null 2>&1; then
  docker run --rm --platform linux/amd64 --entrypoint /bin/sh \
    registry.k8s.io/dns/k8s-dns-node-cache@sha256:bc7c80faba5261a740a9f878ab8f7403e72444b0a2fa0a9a42ed26577a48290a \
    -ec 'command -v sh >/dev/null; command -v grep >/dev/null; command -v iptables >/dev/null; command -v iptables-save >/dev/null'
fi

set_common_values() {
  KUBECTL="${MOCK_KUBECTL}"
  FUGUE_RELEASE_NAME=fugue
  FUGUE_RELEASE_FULLNAME="fugue-fugue"
  FUGUE_COREDNS_NAMESPACE=kube-system
  FUGUE_NODE_LOCAL_DNS_ENABLED=true
  FUGUE_NODE_LOCAL_DNS_MODE=shadow
  FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=false
  FUGUE_NODE_LOCAL_DNS_NODE_NAME=vps-84c8f0a9
  FUGUE_NODE_LOCAL_DNS_NAMESPACE=kube-system
  FUGUE_NODE_LOCAL_DNS_LOCAL_IP=169.254.20.10
  FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN=cluster.local
  FUGUE_NODE_LOCAL_DNS_KUBE_DNS_SERVICE_NAME=kube-dns
  FUGUE_NODE_LOCAL_DNS_COREDNS_CONFIGMAP_NAME=coredns
  FUGUE_NODE_LOCAL_DNS_EXPECTED_KUBE_DNS_SERVICE_IP=10.43.0.10
  FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME=api.example.test
  FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY=registry.k8s.io/dns/k8s-dns-node-cache
  FUGUE_NODE_LOCAL_DNS_IMAGE_TAG=1.26.8
  FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST=sha256:bc6e64e2c85956af2fcc0aa720086410d41b4f31f378c9a92646fecc85cd4739
  FUGUE_NODE_LOCAL_DNS_IMAGE_PULL_POLICY=IfNotPresent
  FUGUE_NODE_LOCAL_DNS_PROBE_IMAGE=docker.io/library/busybox@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028
  FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS=30
  FUGUE_ROLLOUT_TIMEOUT=30s
  NODE_LOCAL_DNS_PREVIOUS_ENABLED=false
  NODE_LOCAL_DNS_PREVIOUS_MODE=""
  NODE_LOCAL_DNS_TARGET_NODES=""
  NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP=""
  NODE_LOCAL_DNS_HELM_SET_ARGS=()
  MOCK_LIVE_MODE=""
  MOCK_LIVE_NODE=""
  MOCK_USE_DS_STATE=false
  MOCK_DS_GET_ERROR=false
  MOCK_DS_DELETE_ERROR=false
  MOCK_PROBE_FAIL_PURPOSE=""
  MOCK_HOSTPORT_CONFLICT=false
  rm -f "${MOCK_DS_STATE}"
  export MOCK_LIVE_MODE MOCK_LIVE_NODE MOCK_USE_DS_STATE MOCK_DS_GET_ERROR MOCK_DS_DELETE_ERROR MOCK_PROBE_FAIL_PURPOSE MOCK_HOSTPORT_CONFLICT
}

set_common_values
prepare_node_local_dns_helm_args
helm_args="${NODE_LOCAL_DNS_HELM_SET_ARGS[*]}"
grep -Fq 'nodeLocalDNS.kubeDNSServiceIP=10.43.0.10' <<<"${helm_args}"
grep -Fq 'nodeLocalDNS.upstreamSelector={"k8s-app":"kube-dns"}' <<<"${helm_args}"
grep -Fq 'nodeLocalDNS.nodeSelector={"kubernetes.io/hostname":"vps-84c8f0a9","kubernetes.io/os":"linux"}' <<<"${helm_args}"
[[ "${NODE_LOCAL_DNS_TARGET_NODES}" == "vps-84c8f0a9" ]]

node_local_dns_shadow_host_preflight "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"

MOCK_HOSTPORT_CONFLICT=true
export MOCK_HOSTPORT_CONFLICT
if (prepare_node_local_dns_helm_args) >/dev/null 2>&1; then
  echo "release preparation must reject an existing authoritative DNS host-port owner in every NodeLocal mode" >&2
  exit 1
fi
MOCK_HOSTPORT_CONFLICT=false
export MOCK_HOSTPORT_CONFLICT

if (
  set_common_values
  FUGUE_NODE_LOCAL_DNS_MODE=iptables
  prepare_node_local_dns_helm_args
) >/dev/null 2>&1; then
  echo "iptables mode must require a preceding shadow release" >&2
  exit 1
fi

set_common_values
FUGUE_NODE_LOCAL_DNS_MODE=iptables
MOCK_LIVE_MODE=shadow
MOCK_LIVE_NODE=vps-84c8f0a9
export MOCK_LIVE_MODE MOCK_LIVE_NODE
prepare_node_local_dns_helm_args

set_common_values
node_local_dns_run_probe_pod vps-84c8f0a9 unit-test \
  registry.k8s.io/dns/k8s-dns-node-cache@sha256:bc6e64e2c85956af2fcc0aa720086410d41b4f31f378c9a92646fecc85cd4739 \
  true true 'iptables-save >/dev/null'
python3 - "${MOCK_APPLY}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    pod = json.load(handle)
assert pod["metadata"]["namespace"] == "kube-system"
assert pod["spec"]["nodeName"] == "vps-84c8f0a9"
assert pod["spec"]["hostNetwork"] is True
assert pod["spec"]["automountServiceAccountToken"] is False
assert pod["spec"]["containers"][0]["securityContext"]["capabilities"]["add"] == ["NET_ADMIN"]
PY

set_common_values
MOCK_LIVE_MODE=shadow
MOCK_USE_DS_STATE=true
NODE_LOCAL_DNS_TARGET_NODES=vps-84c8f0a9
NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP=10.43.0.10
: >"${MOCK_DS_STATE}"
export MOCK_LIVE_MODE MOCK_USE_DS_STATE
node_local_dns_delete_daemonset_safely "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"
[[ ! -e "${MOCK_DS_STATE}" ]]
grep -Fq 'delete daemonset fugue-fugue-node-local-dns --cascade=foreground --wait=true' "${MOCK_LOG}"

set_common_values
MOCK_LIVE_MODE=shadow
MOCK_USE_DS_STATE=true
MOCK_DS_GET_ERROR=true
NODE_LOCAL_DNS_TARGET_NODES=vps-84c8f0a9
NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP=10.43.0.10
: >"${MOCK_DS_STATE}"
export MOCK_LIVE_MODE MOCK_USE_DS_STATE MOCK_DS_GET_ERROR
if (node_local_dns_delete_daemonset_safely "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}") >/dev/null 2>&1; then
  echo "DaemonSet API errors must not be treated as NotFound" >&2
  exit 1
fi
[[ -e "${MOCK_DS_STATE}" ]]

set_common_values
FUGUE_NODE_LOCAL_DNS_ENABLED=false
MOCK_LIVE_MODE=shadow
MOCK_USE_DS_STATE=true
MOCK_DS_GET_ERROR=true
: >"${MOCK_DS_STATE}"
export MOCK_LIVE_MODE MOCK_USE_DS_STATE MOCK_DS_GET_ERROR
if (prepare_node_local_dns_helm_args) >/dev/null 2>&1; then
  echo "release preparation must not treat a DaemonSet API error as NotFound" >&2
  exit 1
fi
[[ -e "${MOCK_DS_STATE}" ]]

set_common_values
MOCK_LIVE_MODE=shadow
MOCK_USE_DS_STATE=true
MOCK_PROBE_FAIL_PURPOSE=fallback
NODE_LOCAL_DNS_TARGET_NODES=vps-84c8f0a9
NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP=10.43.0.10
: >"${MOCK_DS_STATE}"
export MOCK_LIVE_MODE MOCK_USE_DS_STATE MOCK_PROBE_FAIL_PURPOSE
if (node_local_dns_delete_daemonset_safely "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}") >/dev/null 2>&1; then
  echo "failed post-removal fallback must fail the removal transaction" >&2
  exit 1
fi
[[ -e "${MOCK_DS_STATE}" ]]

echo "[test_node_local_dns_release] ok"
