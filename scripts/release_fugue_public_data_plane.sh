#!/usr/bin/env bash

set -euo pipefail

log() {
  printf '[fugue-public-data-plane] %s\n' "$*"
}

fail() {
  printf '[fugue-public-data-plane] ERROR: %s\n' "$*" >&2
  exit 1
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

trim_field() {
  local value="${1:-}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

KUBECTL_CMD=()

detect_kubectl() {
  if [[ -n "${KUBECTL:-}" ]]; then
    # shellcheck disable=SC2206
    KUBECTL_CMD=(${KUBECTL})
  elif command_exists kubectl; then
    KUBECTL_CMD=(kubectl)
  elif command_exists k3s; then
    KUBECTL_CMD=(k3s kubectl)
  else
    fail "kubectl or k3s kubectl is required"
  fi
}

kubectl_cmd() {
  "${KUBECTL_CMD[@]}" "$@"
}

compact_json_object() {
  local label="$1"
  local value="$2"
  python3 -c '
import json
import sys

label = sys.argv[1]
try:
    value = json.loads(sys.argv[2])
except Exception as exc:
    raise SystemExit(f"{label} must be valid JSON: {exc}")
if not isinstance(value, dict):
    raise SystemExit(f"{label} must be a JSON object")
print(json.dumps(value, separators=(",", ":")))
' "${label}" "${value}"
}

edge_daemonset_names() {
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get ds -o json | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
names = []
for item in doc.get("items", []):
    labels = item.get("metadata", {}).get("labels") or {}
    component = (labels.get("app.kubernetes.io/component") or "").strip()
    subsystem = (labels.get("fugue.io/rollout-subsystem") or "").strip()
    if subsystem != "public-data-plane":
        continue
    if component == "edge" or component.startswith("edge-"):
        names.append(item.get("metadata", {}).get("name", ""))
for name in sorted(n for n in names if n):
    print(name)
'
}

bluegreen_worker_bases() {
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get ds -o json | python3 -c '
import json
import re
import sys

doc = json.load(sys.stdin)
bases = {}
for item in doc.get("items", []):
    meta = item.get("metadata", {})
    labels = meta.get("labels") or {}
    if labels.get("fugue.io/rollout-subsystem") != "public-data-plane":
        continue
    if labels.get("fugue.io/rollout-mode") != "node-local-blue-green-worker":
        continue
    name = meta.get("name", "")
    match = re.match(r"^(.*)-worker-([ab])$", name)
    if not match:
        continue
    base, slot = match.groups()
    bases.setdefault(base, set()).add(slot)
for base, slots in sorted(bases.items()):
    if {"a", "b"}.issubset(slots):
        print(base)
'
}

bluegreen_front_daemonset_name() {
  local base="$1"
  printf '%s-front' "${base}"
}

bluegreen_worker_daemonset_name() {
  local base="$1"
  local slot="$2"
  printf '%s-worker-%s' "${base}" "${slot}"
}

pod_names_for_daemonset() {
  local daemonset_name="$1"
  local selector
  selector="$(daemonset_selector "${daemonset_name}")"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -l "${selector}" -o json | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
for pod in sorted(doc.get("items", []), key=lambda item: item.get("metadata", {}).get("name", "")):
    print(pod.get("metadata", {}).get("name", ""))
'
}

ready_pods_for_daemonset() {
  local daemonset_name="$1"
  local selector
  selector="$(daemonset_selector "${daemonset_name}")"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -l "${selector}" -o json | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
for pod in sorted(doc.get("items", []), key=lambda item: item.get("metadata", {}).get("name", "")):
    status = pod.get("status", {})
    if status.get("phase") != "Running":
        continue
    ready = False
    for condition in status.get("conditions") or []:
        if condition.get("type") == "Ready" and condition.get("status") == "True":
            ready = True
            break
    if ready:
        print(pod.get("metadata", {}).get("name", ""))
'
}

node_ips_for_daemonset() {
  local daemonset_name="$1"
  local selector
  selector="$(daemonset_selector "${daemonset_name}")"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -l "${selector}" -o json | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
seen = set()
for pod in sorted(doc.get("items", []), key=lambda item: item.get("metadata", {}).get("name", "")):
    status = pod.get("status", {})
    host_ip = status.get("hostIP", "")
    if host_ip and host_ip not in seen:
        seen.add(host_ip)
        print(host_ip)
'
}

daemonset_selector() {
  local daemonset_name="$1"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
labels = doc.get("spec", {}).get("selector", {}).get("matchLabels") or {}
if not labels:
    raise SystemExit("daemonset selector is empty")
print(",".join(f"{key}={value}" for key, value in sorted(labels.items())))
'
}

capture_daemonset_pods() {
  local daemonset_name selector
  for daemonset_name in "$@"; do
    selector="$(daemonset_selector "${daemonset_name}")"
    kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -l "${selector}" -o json | python3 -c '
import json
import sys

daemonset_name = sys.argv[1]
doc = json.load(sys.stdin)
rows = []
for pod in doc.get("items", []):
    meta = pod.get("metadata", {})
    status = pod.get("status", {})
    statuses = status.get("containerStatuses") or []
    restart_parts = []
    for item in sorted(statuses, key=lambda item: item.get("name", "")):
        restart_parts.append("%s:%s" % (item.get("name", ""), item.get("restartCount", 0)))
    restarts = ",".join(restart_parts)
    rows.append("|".join([
        daemonset_name,
        meta.get("name", ""),
        meta.get("uid", ""),
        meta.get("creationTimestamp", ""),
        status.get("phase", ""),
        restarts,
    ]))
for row in sorted(rows):
    print(row)
' "${daemonset_name}"
  done
}

wait_daemonset_observed() {
  local daemonset_name="$1"
  local timeout_seconds="${FUGUE_PUBLIC_DATA_PLANE_OBSERVED_TIMEOUT_SECONDS:-120}"
  local started_at generation observed

  started_at="$(date +%s)"
  while true; do
    generation="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.metadata.generation}')"
    observed="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.status.observedGeneration}')"
    if [[ -n "${generation}" && "${generation}" == "${observed}" ]]; then
      return 0
    fi
    if (( $(date +%s) - started_at >= timeout_seconds )); then
      fail "daemonset ${daemonset_name} did not observe generation ${generation}; observed=${observed}"
    fi
    sleep 2
  done
}

patch_daemonset_ondelete() {
  local daemonset_name="$1"
  log "setting ${daemonset_name} updateStrategy=OnDelete before template patch"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" patch "ds/${daemonset_name}" --type=merge -p \
    '{"spec":{"updateStrategy":{"type":"OnDelete","rollingUpdate":null}}}' >/dev/null
  wait_daemonset_observed "${daemonset_name}"
}

resource_patch_for_daemonset() {
  local daemonset_name="$1"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json | python3 -c '
import json
import os
import sys

doc = json.load(sys.stdin)
release_id = os.environ["FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID"]
edge_resources = json.loads(os.environ["FUGUE_EDGE_RESOURCES_JSON"])
caddy_resources = json.loads(os.environ["FUGUE_EDGE_CADDY_RESOURCES_JSON"])
containers = []
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    name = container.get("name")
    if name == "edge":
        containers.append({"name": "edge", "resources": edge_resources})
    elif name == "caddy":
        containers.append({"name": "caddy", "resources": caddy_resources})
if not containers:
    raise SystemExit(0)
patch = {
    "spec": {
        "template": {
            "metadata": {
                "annotations": {
                    "fugue.io/public-data-plane-release-id": release_id,
                    "fugue.io/public-data-plane-release-mode": "edge-template-ondelete",
                },
            },
            "spec": {
                "containers": containers,
            },
        },
    },
}
print(json.dumps(patch, separators=(",", ":")))
'
}

patch_daemonset_resources() {
  local daemonset_name="$1"
  local patch

  patch="$(resource_patch_for_daemonset "${daemonset_name}")"
  if [[ -z "$(trim_field "${patch}")" ]]; then
    log "skipping ${daemonset_name}; no edge/caddy containers found"
    return 0
  fi
  log "patching ${daemonset_name} edge/caddy template resources without deleting pods"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    printf '%s\n' "${patch}"
    return 0
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" patch "ds/${daemonset_name}" --type=strategic -p "${patch}" >/dev/null
  wait_daemonset_observed "${daemonset_name}"
}

helm_cmd() {
  if [[ -n "${HELM:-}" ]]; then
    # shellcheck disable=SC2206
    local helm_parts=(${HELM})
    "${helm_parts[@]}" "$@"
  else
    helm "$@"
  fi
}

enable_bluegreen_chart_mode() {
  local args=()

  [[ "${FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN}" == "true" ]] || return 0
  command_exists helm || [[ -n "${HELM:-}" ]] || fail "helm is required when FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN=true"

  args=(
    upgrade "${FUGUE_RELEASE_NAME}" "${FUGUE_HELM_CHART_PATH}"
    -n "${FUGUE_NAMESPACE}"
    --reuse-values
    --history-max 20
    --timeout "${FUGUE_HELM_TIMEOUT}"
    --set edge.blueGreen.enabled=true
    --set edge.caddy.enabled=true
    --set edge.caddy.publicHostPorts.enabled=false
  )
  if [[ -n "$(trim_field "${FUGUE_EDGE_IMAGE_REPOSITORY:-}")" ]]; then
    args+=(--set-string "edge.image.repository=${FUGUE_EDGE_IMAGE_REPOSITORY}")
    args+=(--set-string "edge.blueGreen.front.image.repository=${FUGUE_EDGE_IMAGE_REPOSITORY}")
  fi
  if [[ -n "$(trim_field "${FUGUE_EDGE_IMAGE_TAG:-}")" ]]; then
    args+=(--set-string "edge.image.tag=${FUGUE_EDGE_IMAGE_TAG}")
    args+=(--set-string "edge.blueGreen.front.image.tag=${FUGUE_EDGE_IMAGE_TAG}")
  fi
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    log "dry-run: would enable edge.blueGreen.enabled=true with isolated Helm upgrade"
    printf 'helm'
    printf ' %q' "${args[@]}"
    printf '\n'
    return 0
  fi
  log "enabling edge.blueGreen.enabled=true through isolated public data-plane release"
  helm_cmd "${args[@]}"
}

container_patch_for_worker() {
  local daemonset_name="$1"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json | python3 -c '
import json
import os
import sys

doc = json.load(sys.stdin)
release_id = os.environ["FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID"]
edge_resources = json.loads(os.environ["FUGUE_EDGE_RESOURCES_JSON"])
caddy_resources = json.loads(os.environ["FUGUE_EDGE_CADDY_RESOURCES_JSON"])
edge_repo = os.environ.get("FUGUE_EDGE_IMAGE_REPOSITORY", "").strip()
edge_tag = os.environ.get("FUGUE_EDGE_IMAGE_TAG", "").strip()
caddy_repo = os.environ.get("FUGUE_EDGE_CADDY_IMAGE_REPOSITORY", "").strip()
caddy_tag = os.environ.get("FUGUE_EDGE_CADDY_IMAGE_TAG", "").strip()
containers = []
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    name = container.get("name")
    patch = {"name": name}
    if name == "edge":
        if edge_repo and edge_tag:
            patch["image"] = f"{edge_repo}:{edge_tag}"
        patch["resources"] = edge_resources
    elif name == "caddy":
        if caddy_repo and caddy_tag:
            patch["image"] = f"{caddy_repo}:{caddy_tag}"
        patch["resources"] = caddy_resources
    else:
        continue
    containers.append(patch)
if not containers:
    raise SystemExit("worker daemonset has no edge/caddy containers")
patch = {
    "spec": {
        "updateStrategy": {
            "type": "OnDelete",
            "rollingUpdate": None,
        },
        "template": {
            "metadata": {
                "annotations": {
                    "fugue.io/public-data-plane-release-id": release_id,
                    "fugue.io/public-data-plane-release-mode": "node-local-blue-green-worker",
                },
            },
            "spec": {
                "containers": containers,
            },
        },
    },
}
print(json.dumps(patch, separators=(",", ":")))
'
}

patch_inactive_worker() {
  local daemonset_name="$1"
  local patch

  patch="$(container_patch_for_worker "${daemonset_name}")"
  log "patching inactive worker ${daemonset_name} template"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    printf '%s\n' "${patch}"
    return 0
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" patch "ds/${daemonset_name}" --type=strategic -p "${patch}" >/dev/null
  wait_daemonset_observed "${daemonset_name}"
}

delete_worker_pods() {
  local daemonset_name="$1"
  local pods=()
  local pod

  while IFS= read -r pod; do
    pod="$(trim_field "${pod}")"
    [[ -n "${pod}" ]] || continue
    pods+=("${pod}")
  done < <(pod_names_for_daemonset "${daemonset_name}")
  if (( ${#pods[@]} == 0 )); then
    log "inactive worker ${daemonset_name} has no pods to replace"
    return 0
  fi
  log "deleting inactive worker pods for ${daemonset_name}: ${pods[*]}"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" delete pod "${pods[@]}" --wait=true --timeout="${FUGUE_PUBLIC_DATA_PLANE_POD_DELETE_TIMEOUT:-120s}" >/dev/null
}

wait_daemonset_ready() {
  local daemonset_name="$1"
  local timeout="${FUGUE_PUBLIC_DATA_PLANE_ROLLOUT_TIMEOUT:-180s}"
  log "waiting for ${daemonset_name} to be ready"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" rollout status "ds/${daemonset_name}" --timeout="${timeout}"
}

active_slot_from_front() {
  local front_daemonset="$1"
  local pod
  pod="$(ready_pods_for_daemonset "${front_daemonset}" | head -1 || true)"
  if [[ -z "$(trim_field "${pod}")" ]]; then
    return 1
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" exec "${pod}" -c edge-front -- \
    /bin/sh -ec 'cat "$1" 2>/dev/null || true' sh "${FUGUE_EDGE_BLUE_GREEN_ACTIVE_SLOT_FILE}" 2>/dev/null || true
}

active_slot_from_record() {
  local release_record_name="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_RECORD_NAME:-${FUGUE_RELEASE_FULLNAME}-public-data-plane-release}"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "configmap/${release_record_name}" -o jsonpath='{.data.active_slot}' 2>/dev/null || true
}

current_active_slot() {
  local front_daemonset="$1"
  local slot
  slot="$(trim_field "$(active_slot_from_front "${front_daemonset}")")"
  case "${slot}" in
    a|b) printf '%s' "${slot}"; return 0 ;;
  esac
  slot="$(trim_field "$(active_slot_from_record)")"
  case "${slot}" in
    a|b) printf '%s' "${slot}"; return 0 ;;
  esac
  printf '%s' "${FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT}"
}

other_slot() {
  case "$1" in
    a) printf 'b' ;;
    b) printf 'a' ;;
    *) fail "invalid slot $1" ;;
  esac
}

write_front_active_slot() {
  local front_daemonset="$1"
  local slot="$2"
  local pod pods=()

  while IFS= read -r pod; do
    pod="$(trim_field "${pod}")"
    [[ -n "${pod}" ]] || continue
    pods+=("${pod}")
  done < <(ready_pods_for_daemonset "${front_daemonset}")
  if (( ${#pods[@]} == 0 )); then
    fail "front daemonset ${front_daemonset} has no ready pods"
  fi
  log "switching ${front_daemonset} active slot to ${slot} on ${#pods[@]} node(s)"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  for pod in "${pods[@]}"; do
    kubectl_cmd -n "${FUGUE_NAMESPACE}" exec "${pod}" -c edge-front -- \
      /bin/sh -ec 'slot="$1"; file="$2"; mkdir -p "$(dirname "$file")"; tmp="${file}.tmp"; printf "%s\n" "$slot" >"$tmp"; mv "$tmp" "$file"' \
      sh "${slot}" "${FUGUE_EDGE_BLUE_GREEN_ACTIVE_SLOT_FILE}" >/dev/null
  done
}

worker_https_port() {
  local daemonset_name="$1"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    if container.get("name") != "caddy":
        continue
    for port in container.get("ports") or []:
        if port.get("name") == "https-worker":
            print(port.get("hostPort") or port.get("containerPort") or "")
            raise SystemExit(0)
'
}

check_worker_tcp() {
  local daemonset_name="$1"
  local port="$2"
  local host_ip
  [[ -n "$(trim_field "${port}")" ]] || fail "worker ${daemonset_name} has no https-worker hostPort"
  while IFS= read -r host_ip; do
    host_ip="$(trim_field "${host_ip}")"
    [[ -n "${host_ip}" ]] || continue
    log "checking inactive worker TCP ${host_ip}:${port}"
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
      continue
    fi
    python3 -c '
import socket
import sys

host, port, timeout = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
with socket.create_connection((host, port), timeout=timeout):
    pass
' "${host_ip}" "${port}" "${FUGUE_PUBLIC_DATA_PLANE_TCP_TIMEOUT_SECONDS:-5}"
  done < <(node_ips_for_daemonset "${daemonset_name}")
}

run_bluegreen_release() {
  local bases=()
  local base front_ds active inactive inactive_ds active_ds inactive_port
  local protected_before protected_after

  enable_bluegreen_chart_mode
  while IFS= read -r base; do
    base="$(trim_field "${base}")"
    [[ -n "${base}" ]] || continue
    bases+=("${base}")
  done < <(bluegreen_worker_bases)
  if (( ${#bases[@]} == 0 )); then
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" && "${FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN}" == "true" ]]; then
      log "dry-run: blue/green DaemonSets are not present yet because the isolated Helm enable step was not applied"
      return 0
    fi
    fail "no edge blue/green worker DaemonSets found; enable edge.blueGreen.enabled through the isolated public data-plane release first"
  fi

  log "blue/green release_id=${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID} namespace=${FUGUE_NAMESPACE} bases=${bases[*]}"
  for base in "${bases[@]}"; do
    front_ds="$(bluegreen_front_daemonset_name "${base}")"
    wait_daemonset_ready "${front_ds}"
    active="$(current_active_slot "${front_ds}")"
    inactive="$(other_slot "${active}")"
    active_ds="$(bluegreen_worker_daemonset_name "${base}" "${active}")"
    inactive_ds="$(bluegreen_worker_daemonset_name "${base}" "${inactive}")"
    log "${base}: active=${active} inactive=${inactive}"

    protected_before="$(capture_daemonset_pods "${front_ds}" "${active_ds}")"
    patch_inactive_worker "${inactive_ds}"
    delete_worker_pods "${inactive_ds}"
    wait_daemonset_ready "${inactive_ds}"
    inactive_port="$(worker_https_port "${inactive_ds}")"
    check_worker_tcp "${inactive_ds}" "${inactive_port}"
    protected_after="$(capture_daemonset_pods "${front_ds}" "${active_ds}")"
    if [[ "${protected_before}" != "${protected_after}" ]]; then
      printf '%s\n' "${protected_before}" >/tmp/fugue-public-data-plane-protected-before.txt
      printf '%s\n' "${protected_after}" >/tmp/fugue-public-data-plane-protected-after.txt
      diff -u /tmp/fugue-public-data-plane-protected-before.txt /tmp/fugue-public-data-plane-protected-after.txt || true
      fail "front or active worker pod set changed while upgrading inactive worker ${inactive_ds}"
    fi
    write_front_active_slot "${front_ds}" "${inactive}"
    wait_daemonset_ready "${front_ds}"
    FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOT="${inactive}"
  done
}

write_release_record() {
  local daemonsets_csv="$1"
  local release_record_name="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_RECORD_NAME:-${FUGUE_RELEASE_FULLNAME}-public-data-plane-release}"

  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    log "dry-run: skipping release record ${release_record_name}"
    return 0
  fi

  kubectl_cmd -n "${FUGUE_NAMESPACE}" create configmap "${release_record_name}" \
    --from-literal=release_id="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID}" \
    --from-literal=mode="${FUGUE_PUBLIC_DATA_PLANE_RECORD_MODE:-edge-template-ondelete}" \
    --from-literal=active_slot="${FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOT:-}" \
    --from-literal=daemonsets="${daemonsets_csv}" \
    --from-literal=edge_resources="${FUGUE_EDGE_RESOURCES_JSON}" \
    --from-literal=caddy_resources="${FUGUE_EDGE_CADDY_RESOURCES_JSON}" \
    --from-literal=git_sha="${GITHUB_SHA:-}" \
    --from-literal=recorded_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --dry-run=client -o yaml |
    kubectl_cmd apply -f - >/dev/null
  kubectl_cmd -n "${FUGUE_NAMESPACE}" label "configmap/${release_record_name}" \
    app.kubernetes.io/instance="${FUGUE_RELEASE_INSTANCE}" \
    app.kubernetes.io/component=public-data-plane-release \
    fugue.io/rollout-subsystem=public-data-plane \
    --overwrite >/dev/null
  log "wrote release record ${release_record_name}"
}

run_smoke_urls() {
  local urls="${FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS:-}"
  local url

  [[ -n "$(trim_field "${urls}")" ]] || return 0
  while IFS= read -r url; do
    url="$(trim_field "${url}")"
    [[ -n "${url}" ]] || continue
    log "smoke ${url}"
    curl -fsS --max-time "${FUGUE_PUBLIC_DATA_PLANE_SMOKE_TIMEOUT_SECONDS:-10}" "${url}" >/dev/null
  done < <(printf '%s\n' "${urls}" | tr ',' '\n')
}

main() {
  local default_edge_resources
  local default_caddy_resources
  local daemonsets_csv
  local daemonset_name
  local edge_daemonsets=()

  default_edge_resources='{"requests":{"cpu":"25m","memory":"128Mi","ephemeral-storage":"32Mi"},"limits":{"memory":"1Gi","ephemeral-storage":"256Mi"}}'
  default_caddy_resources='{"requests":{"cpu":"25m","memory":"128Mi","ephemeral-storage":"32Mi"},"limits":{"memory":"1Gi","ephemeral-storage":"256Mi"}}'

  FUGUE_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
  FUGUE_RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"
  FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-fugue-fugue}"
  FUGUE_RELEASE_INSTANCE="${FUGUE_RELEASE_INSTANCE:-${FUGUE_RELEASE_NAME:-fugue}}"
  FUGUE_HELM_CHART_PATH="${FUGUE_HELM_CHART_PATH:-./deploy/helm/fugue}"
  FUGUE_HELM_TIMEOUT="${FUGUE_HELM_TIMEOUT:-10m}"
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN:-false}"
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY:-blue-green}"
  FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN="${FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN:-false}"
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID:-pdp-$(date -u +%Y%m%dT%H%M%SZ)-${GITHUB_SHA:-local}}"
  FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT="${FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT:-a}"
  FUGUE_EDGE_BLUE_GREEN_ACTIVE_SLOT_FILE="${FUGUE_EDGE_BLUE_GREEN_ACTIVE_SLOT_FILE:-/var/lib/fugue/edge-blue-green/active-slot}"
  FUGUE_EDGE_RESOURCES_JSON="$(compact_json_object FUGUE_EDGE_RESOURCES_JSON "${FUGUE_EDGE_RESOURCES_JSON:-${default_edge_resources}}")"
  FUGUE_EDGE_CADDY_RESOURCES_JSON="$(compact_json_object FUGUE_EDGE_CADDY_RESOURCES_JSON "${FUGUE_EDGE_CADDY_RESOURCES_JSON:-${default_caddy_resources}}")"
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID FUGUE_EDGE_RESOURCES_JSON FUGUE_EDGE_CADDY_RESOURCES_JSON
  export FUGUE_EDGE_IMAGE_REPOSITORY FUGUE_EDGE_IMAGE_TAG FUGUE_EDGE_CADDY_IMAGE_REPOSITORY FUGUE_EDGE_CADDY_IMAGE_TAG

  case "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" in
    true|false) ;;
    *) fail "FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN must be true or false" ;;
  esac
  case "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" in
    blue-green|legacy-template-ondelete) ;;
    *) fail "FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY must be blue-green or legacy-template-ondelete" ;;
  esac
  case "${FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN}" in
    true|false) ;;
    *) fail "FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN must be true or false" ;;
  esac
  case "${FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT}" in
    a|b) ;;
    *) fail "FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT must be a or b" ;;
  esac

  command_exists python3 || fail "python3 is required"
  command_exists curl || fail "curl is required"
  detect_kubectl

  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" == "blue-green" ]]; then
    FUGUE_PUBLIC_DATA_PLANE_RECORD_MODE="node-local-blue-green"
    run_bluegreen_release
    daemonsets_csv="$(bluegreen_worker_bases | paste -sd, -)"
    write_release_record "${daemonsets_csv}"
    run_smoke_urls
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
      log "dry-run complete; inactive edge workers would be upgraded and switched with front pods preserved"
      return 0
    fi
    log "public edge blue/green release complete; front and previous active workers were not restarted"
    return 0
  fi

  while IFS= read -r daemonset_name; do
    daemonset_name="$(trim_field "${daemonset_name}")"
    [[ -n "${daemonset_name}" ]] || continue
    edge_daemonsets+=("${daemonset_name}")
  done < <(edge_daemonset_names)
  if (( ${#edge_daemonsets[@]} == 0 )); then
    fail "no public edge DaemonSets found in namespace ${FUGUE_NAMESPACE}"
  fi

  log "release_id=${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID} namespace=${FUGUE_NAMESPACE} daemonsets=${edge_daemonsets[*]}"
  before="$(capture_daemonset_pods "${edge_daemonsets[@]}")"

  for daemonset_name in "${edge_daemonsets[@]}"; do
    patch_daemonset_ondelete "${daemonset_name}"
  done
  for daemonset_name in "${edge_daemonsets[@]}"; do
    patch_daemonset_resources "${daemonset_name}"
  done

  after="$(capture_daemonset_pods "${edge_daemonsets[@]}")"
  if [[ "${before}" != "${after}" ]]; then
    printf '%s\n' "${before}" >/tmp/fugue-public-data-plane-before.txt
    printf '%s\n' "${after}" >/tmp/fugue-public-data-plane-after.txt
    diff -u /tmp/fugue-public-data-plane-before.txt /tmp/fugue-public-data-plane-after.txt || true
    fail "edge pod set or restart counts changed during template patch"
  fi

  daemonsets_csv="$(IFS=,; printf '%s' "${edge_daemonsets[*]}")"
  write_release_record "${daemonsets_csv}"
  run_smoke_urls
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    log "dry-run complete; no edge pods would be deleted or restarted"
    return 0
  fi
  log "public edge DaemonSet templates patched; no edge pods were deleted or restarted"
}

main "$@"
