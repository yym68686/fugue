#!/usr/bin/env bash

set -euo pipefail

log() {
  printf '[fugue-public-data-plane] %s\n' "$*"
}

error() {
  printf '[fugue-public-data-plane] ERROR: %s\n' "$*" >&2
}

fail() {
  error "$*"
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
DNS_MANIFEST_LAST_CONTROLLED_POD_COHORT=""
DNS_MANIFEST_LAST_RESTORED_POD_COHORT=""

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
import hashlib
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

public_daemonset_names() {
  : "${FUGUE_RELEASE_INSTANCE:?FUGUE_RELEASE_INSTANCE is required to select public daemonsets}"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get ds -o json | python3 -c '
import json
import sys

release_instance = sys.argv[1]
doc = json.load(sys.stdin)
names = []
for item in doc.get("items", []):
    labels = item.get("metadata", {}).get("labels") or {}
    component = (labels.get("app.kubernetes.io/component") or "").strip()
    subsystem = (labels.get("fugue.io/rollout-subsystem") or "").strip()
    if subsystem != "public-data-plane" or labels.get("app.kubernetes.io/instance") != release_instance:
        continue
    if component in ("edge", "dns") or component.startswith("edge-") or component.startswith("dns-"):
        names.append(item.get("metadata", {}).get("name", ""))
for name in sorted(n for n in names if n):
    print(name)
' "${FUGUE_RELEASE_INSTANCE}"
}

dns_daemonset_names() {
  : "${FUGUE_RELEASE_INSTANCE:?FUGUE_RELEASE_INSTANCE is required to select DNS daemonsets}"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get ds -o json | python3 -c '
import json
import sys

release_instance = sys.argv[1]
doc = json.load(sys.stdin)
names = []
for item in doc.get("items", []):
    labels = item.get("metadata", {}).get("labels") or {}
    component = (labels.get("app.kubernetes.io/component") or "").strip()
    subsystem = (labels.get("fugue.io/rollout-subsystem") or "").strip()
    if subsystem != "public-data-plane" or labels.get("app.kubernetes.io/instance") != release_instance:
        continue
    if component == "dns" or component.startswith("dns-"):
        names.append(item.get("metadata", {}).get("name", ""))
for name in sorted(n for n in names if n):
    print(name)
' "${FUGUE_RELEASE_INSTANCE}"
}

bluegreen_worker_bases() {
  : "${FUGUE_RELEASE_INSTANCE:?FUGUE_RELEASE_INSTANCE is required to select blue-green workers}"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get ds -o json | python3 -c '
import json
import re
import sys

release_instance = sys.argv[1]
doc = json.load(sys.stdin)
bases = {}
for item in doc.get("items", []):
    meta = item.get("metadata", {})
    labels = meta.get("labels") or {}
    if (
        labels.get("fugue.io/rollout-subsystem") != "public-data-plane"
        or labels.get("app.kubernetes.io/instance") != release_instance
    ):
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
' "${FUGUE_RELEASE_INSTANCE}"
}

bluegreen_all_bases() {
  : "${FUGUE_RELEASE_INSTANCE:?FUGUE_RELEASE_INSTANCE is required to select blue-green daemonsets}"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get ds -o json | python3 -c '
import json
import re
import sys

release_instance = sys.argv[1]
doc = json.load(sys.stdin)
bases = set()
for item in doc.get("items", []):
    meta = item.get("metadata", {})
    labels = meta.get("labels") or {}
    if (
        labels.get("fugue.io/rollout-subsystem") != "public-data-plane"
        or labels.get("app.kubernetes.io/instance") != release_instance
    ):
        continue
    mode = labels.get("fugue.io/rollout-mode")
    name = meta.get("name", "")
    if mode == "node-local-blue-green-front" and name.endswith("-front"):
        bases.add(name[:-len("-front")])
        continue
    if mode == "node-local-blue-green-worker":
        match = re.match(r"^(.*)-worker-([ab])$", name)
        if match:
            bases.add(match.group(1))
for base in sorted(bases):
    print(base)
' "${FUGUE_RELEASE_INSTANCE}"
}

bluegreen_all_bases_array() {
  local base
  while IFS= read -r base; do
    base="$(trim_field "${base}")"
    [[ -n "${base}" ]] || continue
    printf '%s\n' "${base}"
  done < <(bluegreen_all_bases)
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

live_front_pod_image() {
  : "${FUGUE_RELEASE_INSTANCE:?FUGUE_RELEASE_INSTANCE is required to select live front Pods}"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods \
    -l "fugue.io/rollout-mode=node-local-blue-green-front,app.kubernetes.io/instance=${FUGUE_RELEASE_INSTANCE}" \
    -o json | python3 -c '
import json
import sys

release_instance = sys.argv[1]
doc = json.load(sys.stdin)
fallback = ""
for pod in sorted(doc.get("items", []), key=lambda item: item.get("metadata", {}).get("name", "")):
    labels = (pod.get("metadata") or {}).get("labels") or {}
    component = labels.get("app.kubernetes.io/component") or ""
    if (
        labels.get("app.kubernetes.io/instance") != release_instance
        or labels.get("fugue.io/rollout-subsystem") != "public-data-plane"
        or labels.get("fugue.io/rollout-mode") != "node-local-blue-green-front"
        or not (component == "edge-front" or (component.startswith("edge-") and component.endswith("-front")))
    ):
        continue
    image = ""
    for container in pod.get("spec", {}).get("containers", []):
        if container.get("name") == "edge-front":
            image = container.get("image", "")
            break
    if not image:
        continue
    if not fallback:
        fallback = image
    status = pod.get("status", {})
    if status.get("phase") != "Running":
        continue
    ready = False
    for condition in status.get("conditions") or []:
        if condition.get("type") == "Ready" and condition.get("status") == "True":
            ready = True
            break
    if ready:
        print(image)
        raise SystemExit(0)
if fallback:
    print(fallback)
' "${FUGUE_RELEASE_INSTANCE}"
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

daemonset_exists() {
  local daemonset_name="$1"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" >/dev/null 2>&1
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

daemonset_pod_uids() {
  local daemonset_name="$1"
  capture_daemonset_pods "${daemonset_name}" | awk -F'|' '{print $3}'
}

daemonset_ready_counts() {
  local daemonset_name="$1"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.status.desiredNumberScheduled}{"\t"}{.status.numberReady}{"\t"}{.status.numberUnavailable}' 2>/dev/null || true
}

daemonset_desired_count() {
  local daemonset_name="$1"
  local desired
  IFS=$'\t' read -r desired _ <<<"$(daemonset_ready_counts "${daemonset_name}")"
  desired="${desired:-0}"
  printf '%s' "${desired}"
}

wait_daemonset_observed() {
  local daemonset_name="$1"
  local timeout_seconds="${FUGUE_PUBLIC_DATA_PLANE_OBSERVED_TIMEOUT_SECONDS:-120}"
  local started_at generation observed

  started_at="$(date +%s)"
  while true; do
    generation="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.metadata.generation}')" || return $?
    observed="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.status.observedGeneration}')" || return $?
    if [[ -n "${generation}" && "${generation}" == "${observed}" ]]; then
      return 0
    fi
    if (( $(date +%s) - started_at >= timeout_seconds )); then
      error "daemonset ${daemonset_name} did not observe generation ${generation}; observed=${observed}"
      return 1
    fi
    sleep 2
  done
}

duration_to_seconds() {
  local duration="${1:-600s}"
  local amount=""
  local unit=""

  if [[ "${duration}" =~ ^([0-9]+)$ ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
    return
  fi
  if [[ "${duration}" =~ ^([0-9]+)(ms|s|m|h)$ ]]; then
    amount="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2]}"
    case "${unit}" in
      ms)
        printf '1'
        ;;
      s)
        printf '%s' "${amount}"
        ;;
      m)
        printf '%s' "$((amount * 60))"
        ;;
      h)
        printf '%s' "$((amount * 3600))"
        ;;
    esac
    return
  fi

  printf '600'
}

image_ref_without_digest() {
  local image_ref="$1"
  printf '%s' "${image_ref%%@*}"
}

image_ref_repository() {
  local image_ref no_digest last
  image_ref="$(trim_field "$1")"
  no_digest="$(image_ref_without_digest "${image_ref}")"
  last="${no_digest##*/}"
  if [[ "${last}" == *:* ]]; then
    printf '%s' "${no_digest%:*}"
  else
    printf '%s' "${no_digest}"
  fi
}

image_ref_tag() {
  local image_ref no_digest last
  image_ref="$(trim_field "$1")"
  no_digest="$(image_ref_without_digest "${image_ref}")"
  last="${no_digest##*/}"
  if [[ "${last}" == *:* ]]; then
    printf '%s' "${last##*:}"
  else
    printf 'latest'
  fi
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
dns_resources = json.loads(os.environ["FUGUE_DNS_RESOURCES_JSON"])
containers = []
dns_container_count = 0
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    name = container.get("name")
    command = container.get("command") or []
    env_names = {entry.get("name") for entry in container.get("env") or []}
    if command == ["/usr/local/bin/fugue-dns"] and "FUGUE_DNS_ZONE" in env_names:
        dns_container_count += 1
        containers.append({"name": name, "resources": dns_resources})
    elif name == "edge":
        containers.append({"name": "edge", "resources": edge_resources})
    elif name == "caddy":
        containers.append({"name": "caddy", "resources": caddy_resources})
if dns_container_count > 1:
    raise SystemExit("daemonset has more than one semantic fugue-dns container")
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
    log "skipping ${daemonset_name}; no edge/caddy/dns containers found"
    return 0
  fi
  log "patching ${daemonset_name} public data-plane template resources without deleting pods"
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

helm_bluegreen_upgrade() {
  local phase="$1"
  local keep_legacy_direct="$2"
  local front_public_hostports="$3"
  local legacy_public_hostports="$4"
  local args=()
  local front_image_ref
  local front_image_repository
  local front_image_tag

  command_exists helm || [[ -n "${HELM:-}" ]] || fail "helm is required when FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN=true"

  args=(
    upgrade "${FUGUE_RELEASE_NAME}" "${FUGUE_HELM_CHART_PATH}"
    -n "${FUGUE_NAMESPACE}"
    --reuse-values
    --history-max 20
    --timeout "${FUGUE_HELM_TIMEOUT}"
    --set edge.blueGreen.enabled=true
    --set "edge.blueGreen.migration.keepLegacyDirect=${keep_legacy_direct}"
    --set "edge.blueGreen.front.publicHostPorts.enabled=${front_public_hostports}"
    --set edge.caddy.enabled=true
    --set "edge.caddy.publicHostPorts.enabled=${legacy_public_hostports}"
  )
  if [[ -n "$(trim_field "${FUGUE_EDGE_IMAGE_REPOSITORY:-}")" ]]; then
    args+=(--set-string "edge.image.repository=${FUGUE_EDGE_IMAGE_REPOSITORY}")
  fi
  if [[ -n "$(trim_field "${FUGUE_EDGE_IMAGE_TAG:-}")" ]]; then
    args+=(--set-string "edge.image.tag=${FUGUE_EDGE_IMAGE_TAG}")
  fi
  if ! front_image_ref="$(live_front_pod_image)"; then
    fail "could not read the live front Pod image for release instance ${FUGUE_RELEASE_INSTANCE}"
  fi
  front_image_ref="$(trim_field "${front_image_ref}")"
  if [[ -n "${front_image_ref}" ]]; then
    front_image_repository="$(image_ref_repository "${front_image_ref}")"
    front_image_tag="$(image_ref_tag "${front_image_ref}")"
    if [[ -n "${front_image_repository}" && -n "${front_image_tag}" ]]; then
      args+=(--set-string "edge.blueGreen.front.image.repository=${front_image_repository}")
      args+=(--set-string "edge.blueGreen.front.image.tag=${front_image_tag}")
      log "preserving live front image during worker release: ${front_image_repository}:${front_image_tag}"
    fi
  elif [[ -n "$(trim_field "${FUGUE_EDGE_IMAGE_REPOSITORY:-}")" && -n "$(trim_field "${FUGUE_EDGE_IMAGE_TAG:-}")" ]]; then
    args+=(--set-string "edge.blueGreen.front.image.repository=${FUGUE_EDGE_IMAGE_REPOSITORY}")
    args+=(--set-string "edge.blueGreen.front.image.tag=${FUGUE_EDGE_IMAGE_TAG}")
    log "no live front pods found; using requested edge image for initial front prewarm"
  fi
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    log "dry-run: would run blue/green Helm phase ${phase}"
    printf 'helm'
    printf ' %q' "${args[@]}"
    printf '\n'
    return 0
  fi
  log "running blue/green Helm phase ${phase}"
  helm_cmd "${args[@]}"
}

delete_daemonset_pods_no_wait() {
  local daemonset_name="$1"
  local display_name="$2"
  local pods=()
  local pod

  while IFS= read -r pod; do
    pod="$(trim_field "${pod}")"
    [[ -n "${pod}" ]] || continue
    pods+=("${pod}")
  done < <(pod_names_for_daemonset "${daemonset_name}")
  if (( ${#pods[@]} == 0 )); then
    log "${display_name} daemonset ${daemonset_name} has no pods to replace"
    return 0
  fi
  log "deleting ${display_name} pods for ${daemonset_name}: ${pods[*]}"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" delete pod "${pods[@]}" --wait=false >/dev/null
}

replace_daemonset_pods_one_at_a_time() {
  local daemonset_name="$1"
  local display_name="$2"
  local smoke_front="${3:-false}"
  local rows
  local ds pod uid created phase restarts

  rows="$(capture_daemonset_pods "${daemonset_name}")"
  if [[ -z "$(trim_field "${rows}")" ]]; then
    log "${display_name} daemonset ${daemonset_name} has no pods to replace"
    return 0
  fi

  while IFS='|' read -r ds pod uid created phase restarts; do
    pod="$(trim_field "${pod}")"
    uid="$(trim_field "${uid}")"
    [[ -n "${pod}" && -n "${uid}" ]] || continue
    log "deleting ${display_name} pod for ${daemonset_name}: ${pod}"
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" != "true" ]]; then
      kubectl_cmd -n "${FUGUE_NAMESPACE}" delete pod "${pod}" --wait=false >/dev/null
      wait_daemonset_replaced_and_ready "${daemonset_name}" "${uid}"
    fi
    if [[ "${smoke_front}" == "true" ]]; then
      check_public_smoke_on_front_nodes "${daemonset_name}"
    fi
  done <<<"${rows}"
}

wait_daemonset_replaced_and_ready() {
  local daemonset_name="$1"
  local before_uids="$2"
  local timeout="${FUGUE_PUBLIC_DATA_PLANE_ROLLOUT_TIMEOUT:-180s}"
  local timeout_seconds
  local started_at
  local current_uids
  local desired
  local ready
  local unavailable
  local uid
  local old_uid_present

  log "waiting for ${daemonset_name} pods to be replaced and ready"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  timeout_seconds="$(duration_to_seconds "${timeout}")"
  started_at="$(date +%s)"
  while true; do
    current_uids="$(daemonset_pod_uids "${daemonset_name}" || true)"
    old_uid_present="false"
    while IFS= read -r uid; do
      uid="$(trim_field "${uid}")"
      [[ -n "${uid}" ]] || continue
      if printf '%s\n' "${current_uids}" | grep -Fxq "${uid}"; then
        old_uid_present="true"
        break
      fi
    done <<<"${before_uids}"
    IFS=$'\t' read -r desired ready unavailable <<<"$(daemonset_ready_counts "${daemonset_name}")"
    desired="${desired:-0}"
    ready="${ready:-0}"
    unavailable="${unavailable:-0}"
    if [[ "${old_uid_present}" == "false" && "${desired}" != "0" && "${desired}" == "${ready}" && "${unavailable}" == "0" ]]; then
      log "daemonset ${daemonset_name} replacement ready: desired=${desired} ready=${ready}"
      return 0
    fi
    if (( $(date +%s) - started_at >= timeout_seconds )); then
      error "daemonset ${daemonset_name} replacement not ready: old_uid_present=${old_uid_present} desired=${desired} ready=${ready} unavailable=${unavailable}"
      return 1
    fi
    sleep 2
  done
}

require_bluegreen_base_complete() {
  local base="$1"
  local front_ds
  local worker_a
  local worker_b

  front_ds="$(bluegreen_front_daemonset_name "${base}")"
  worker_a="$(bluegreen_worker_daemonset_name "${base}" a)"
  worker_b="$(bluegreen_worker_daemonset_name "${base}" b)"
  for daemonset_name in "${front_ds}" "${worker_a}" "${worker_b}"; do
    if ! daemonset_exists "${daemonset_name}"; then
      fail "blue/green base ${base} is incomplete; missing daemonset ${daemonset_name}"
    fi
  done
}

wait_bluegreen_base_ready() {
  local base="$1"
  wait_daemonset_ready "$(bluegreen_front_daemonset_name "${base}")"
  wait_daemonset_ready "$(bluegreen_worker_daemonset_name "${base}" a)"
  wait_daemonset_ready "$(bluegreen_worker_daemonset_name "${base}" b)"
}

migrate_legacy_direct_to_bluegreen_front() {
  local bases=("$@")
  local base
  local front_ds
  local legacy_ds
  local before_uids
  local migrated_any="false"

  if (( ${#bases[@]} == 0 )); then
    return 0
  fi

  for base in "${bases[@]}"; do
    require_bluegreen_base_complete "${base}"
    wait_bluegreen_base_ready "${base}"
  done

  helm_bluegreen_upgrade "bind-public-front-hostports" "true" "true" "false"

  for base in "${bases[@]}"; do
    front_ds="$(bluegreen_front_daemonset_name "${base}")"
    legacy_ds="${base}"
    if ! daemonset_exists "${legacy_ds}"; then
      log "legacy direct edge daemonset ${legacy_ds} already absent"
      continue
    fi
    migrated_any="true"
    log "migrating public hostPorts from legacy ${legacy_ds} to front ${front_ds}"
    before_uids="$(daemonset_pod_uids "${front_ds}")"
    delete_daemonset_pods_no_wait "${front_ds}" "front"
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" != "true" ]]; then
      kubectl_cmd -n "${FUGUE_NAMESPACE}" delete "ds/${legacy_ds}" --wait=false >/dev/null
    fi
    wait_daemonset_replaced_and_ready "${front_ds}" "${before_uids}"
  done

  helm_bluegreen_upgrade "finalize-blue-green-without-legacy-direct" "false" "true" "false"

  if [[ "${migrated_any}" == "true" ]]; then
    log "legacy direct edge daemonsets were migrated to node-local front pods one group at a time"
  fi
}

enable_bluegreen_chart_mode() {
  local bases=()
  local base
  local has_legacy="false"

  [[ "${FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN}" == "true" ]] || return 0

  while IFS= read -r base; do
    bases+=("${base}")
  done < <(bluegreen_all_bases_array)

  if (( ${#bases[@]} == 0 )); then
    helm_bluegreen_upgrade "prewarm-front-and-workers" "true" "false" "true"
    while IFS= read -r base; do
      bases+=("${base}")
    done < <(bluegreen_all_bases_array)
    if (( ${#bases[@]} == 0 )); then
      if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
        log "dry-run: blue/green DaemonSets are not present yet because the isolated Helm enable step was not applied"
        return 0
      fi
      fail "no edge blue/green worker DaemonSets found after enabling edge.blueGreen.enabled=true"
    fi
  elif [[ "${FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN}" == "true" ]]; then
    helm_bluegreen_upgrade "reconcile-front-and-workers" "false" "true" "false"
    bases=()
    while IFS= read -r base; do
      bases+=("${base}")
    done < <(bluegreen_all_bases_array)
  fi

  for base in "${bases[@]}"; do
    require_bluegreen_base_complete "${base}"
  done

  for base in "${bases[@]}"; do
    if daemonset_exists "${base}"; then
      has_legacy="true"
    fi
  done
  if [[ "${has_legacy}" == "true" ]]; then
    migrate_legacy_direct_to_bluegreen_front "${bases[@]}"
  else
    log "edge blue/green chart mode is already enabled without legacy direct daemonsets"
  fi
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

  if ! patch="$(container_patch_for_worker "${daemonset_name}")"; then
    return 1
  fi
  log "patching inactive worker ${daemonset_name} template"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    printf '%s\n' "${patch}"
    return 0
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" patch "ds/${daemonset_name}" --type=strategic -p "${patch}" >/dev/null || return $?
  wait_daemonset_observed "${daemonset_name}" || return $?
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
  kubectl_cmd -n "${FUGUE_NAMESPACE}" delete pod "${pods[@]}" --wait=true --timeout="${FUGUE_PUBLIC_DATA_PLANE_POD_DELETE_TIMEOUT:-120s}" >/dev/null || return $?
}

wait_daemonset_ready() {
  local daemonset_name="$1"
  local timeout="${FUGUE_PUBLIC_DATA_PLANE_ROLLOUT_TIMEOUT:-180s}"
  local strategy
  local timeout_seconds
  local started_at
  local status
  local generation
  local observed_generation
  local desired
  local ready
  local unavailable

  log "waiting for ${daemonset_name} to be ready"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  strategy="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.spec.updateStrategy.type}' 2>/dev/null || true)"
  strategy="${strategy:-RollingUpdate}"
  if [[ "${strategy}" == "RollingUpdate" ]]; then
    kubectl_cmd -n "${FUGUE_NAMESPACE}" rollout status "ds/${daemonset_name}" --timeout="${timeout}" || return $?
    return 0
  fi

  timeout_seconds="$(duration_to_seconds "${timeout}")"
  started_at="$(date +%s)"
  while true; do
    status="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.metadata.generation}{"\t"}{.status.observedGeneration}{"\t"}{.status.desiredNumberScheduled}{"\t"}{.status.numberReady}{"\t"}{.status.numberUnavailable}' 2>/dev/null || true)"
    IFS=$'\t' read -r generation observed_generation desired ready unavailable <<<"${status}"
    generation="${generation:-0}"
    observed_generation="${observed_generation:-0}"
    desired="${desired:-0}"
    ready="${ready:-0}"
    unavailable="${unavailable:-0}"

    if [[ "${generation}" == "${observed_generation}" && "${desired}" == "${ready}" && "${unavailable}" == "0" ]]; then
      log "daemonset ${daemonset_name} ready: generation=${generation} desired=${desired} ready=${ready}"
      return 0
    fi

    if (( $(date +%s) - started_at >= timeout_seconds )); then
      error "daemonset ${daemonset_name} not ready: generation=${generation} observed=${observed_generation} desired=${desired} ready=${ready} unavailable=${unavailable}"
      return 1
    fi
    sleep 2
  done
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
  local base="$1"
  local release_record_name="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_RECORD_NAME:-${FUGUE_RELEASE_FULLNAME}-public-data-plane-release}"
  local active_slots
  local legacy_slot

  active_slots="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "configmap/${release_record_name}" -o jsonpath='{.data.active_slots}' 2>/dev/null || true)"
  active_slots="$(trim_field "${active_slots}")"
  if [[ -n "${active_slots}" ]]; then
    python3 -c '
import json
import sys

base = sys.argv[1]
try:
    value = json.loads(sys.argv[2])
except Exception:
    raise SystemExit(0)
if isinstance(value, dict):
    slot = value.get(base, "")
    if slot in ("a", "b"):
        print(slot)
' "${base}" "${active_slots}" || true
    return 0
  fi

  legacy_slot="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "configmap/${release_record_name}" -o jsonpath='{.data.active_slot}' 2>/dev/null || true)"
  legacy_slot="$(trim_field "${legacy_slot}")"
  [[ -n "${legacy_slot}" ]] || return 0
}

current_active_slot() {
  local base="$1"
  local front_daemonset="$2"
  local default_slot
  local slot
  slot="$(trim_field "$(active_slot_from_front "${front_daemonset}")")"
  case "${slot}" in
    a|b) printf '%s' "${slot}"; return 0 ;;
  esac
  slot="$(trim_field "$(active_slot_from_record "${base}")")"
  case "${slot}" in
    a|b) printf '%s' "${slot}"; return 0 ;;
  esac
  default_slot="$(trim_field "${FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT:-a}")"
  case "${default_slot}" in
    a|b) printf '%s' "${default_slot}"; return 0 ;;
  esac
  fail "could not determine active slot for ${base}; front slot file is unreadable and no per-base release record exists"
}

record_active_slot_json() {
  local active_slots="$1"
  local base="$2"
  local slot="$3"
  python3 -c '
import json
import sys

try:
    value = json.loads(sys.argv[1] or "{}")
except Exception:
    value = {}
if not isinstance(value, dict):
    value = {}
value[sys.argv[2]] = sys.argv[3]
print(json.dumps(value, separators=(",", ":"), sort_keys=True))
' "${active_slots}" "${base}" "${slot}"
}

release_record_active_slots_json() {
  local release_record_name="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_RECORD_NAME:-${FUGUE_RELEASE_FULLNAME}-public-data-plane-release}"
  local record_json

  if ! record_json="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "configmap/${release_record_name}" \
    --ignore-not-found=true -o json 2>/dev/null)"; then
    error "could not read the existing public data-plane release record; refusing to overwrite active slots"
    return 1
  fi
  record_json="$(trim_field "${record_json}")"
  [[ -n "${record_json}" ]] || {
    printf '{}'
    return 0
  }
  RECORD_JSON="${record_json}" python3 -c '
import json
import os
import sys

try:
    record = json.loads(os.environ["RECORD_JSON"])
except Exception:
    raise SystemExit(1)
if not isinstance(record, dict):
    raise SystemExit(1)
raw = (record.get("data") or {}).get("active_slots") or "{}"
try:
    value = json.loads(raw)
except Exception:
    raise SystemExit(1)
if not isinstance(value, dict):
    raise SystemExit(1)
print(json.dumps(value, separators=(",", ":"), sort_keys=True))
'
}

collect_current_active_slots_json() {
  local bases=("$@")
  local active_slots="{}"
  local base
  local front_ds
  local slot

  for base in "${bases[@]}"; do
    front_ds="$(bluegreen_front_daemonset_name "${base}")"
    if [[ "$(daemonset_desired_count "${front_ds}")" == "0" ]]; then
      log "skipping active slot collection for ${base}; front desired=0"
      continue
    fi
    slot="$(current_active_slot "${base}" "${front_ds}")"
    active_slots="$(record_active_slot_json "${active_slots}" "${base}" "${slot}")"
  done
  printf '%s' "${active_slots}"
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
    error "front daemonset ${front_daemonset} has no ready pods"
    return 1
  fi
  log "switching ${front_daemonset} active slot to ${slot} on ${#pods[@]} node(s)"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  for pod in "${pods[@]}"; do
    kubectl_cmd -n "${FUGUE_NAMESPACE}" exec "${pod}" -c edge-front -- \
      /bin/sh -ec 'slot="$1"; file="$2"; mkdir -p "$(dirname "$file")"; tmp="${file}.tmp"; printf "%s\n" "$slot" >"$tmp"; mv "$tmp" "$file"' \
      sh "${slot}" "${FUGUE_EDGE_BLUE_GREEN_ACTIVE_SLOT_FILE}" >/dev/null || return $?
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
    if ! python3 -c '
import socket
import sys

host, port, timeout = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
with socket.create_connection((host, port), timeout=timeout):
    pass
' "${host_ip}" "${port}" "${FUGUE_PUBLIC_DATA_PLANE_TCP_TIMEOUT_SECONDS:-5}"; then
      return 1
    fi
  done < <(node_ips_for_daemonset "${daemonset_name}")
}

worker_smoke_targets() {
  public_data_plane_smoke_urls | python3 -c '
import sys
from urllib.parse import urlsplit

for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    parsed = urlsplit(raw)
    if parsed.scheme != "https" or not parsed.hostname:
        continue
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    print(parsed.hostname + "\t" + path)
'
}

public_data_plane_smoke_urls() {
  local urls="${FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS:-}"
  [[ -n "$(trim_field "${urls}")" ]] || return 0
  printf '%s\n' "${urls}" | tr ',;' '\n'
}

validate_representative_smoke_configuration() {
  local minimum_hosts="${FUGUE_PUBLIC_DATA_PLANE_MIN_SMOKE_HOSTS:-2}"
  local distinct_hosts

  [[ "${minimum_hosts}" =~ ^[1-9][0-9]*$ ]] || {
    error "FUGUE_PUBLIC_DATA_PLANE_MIN_SMOKE_HOSTS must be a positive integer"
    return 1
  }
  if (( minimum_hosts < 2 )); then
    error "FUGUE_PUBLIC_DATA_PLANE_MIN_SMOKE_HOSTS cannot be lower than the safety floor of 2"
    return 1
  fi
  if ! distinct_hosts="$(public_data_plane_smoke_urls | python3 -c '
import sys
from urllib.parse import urlsplit

hosts = set()
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    parsed = urlsplit(raw)
    if parsed.scheme != "https" or not parsed.hostname:
        raise SystemExit(f"invalid public data-plane smoke URL (HTTPS hostname required): {raw}")
    hosts.add(parsed.hostname.lower())
print(len(hosts))
')"; then
    error "public data-plane smoke URL validation failed"
    return 1
  fi
  if (( distinct_hosts < minimum_hosts )); then
    error "public data-plane release requires at least ${minimum_hosts} distinct HTTPS smoke hostnames; found ${distinct_hosts}"
    return 1
  fi
  log "representative smoke configuration validated: distinct_https_hosts=${distinct_hosts} minimum=${minimum_hosts}"
}

authoritative_dns_hostnames() {
  public_data_plane_smoke_urls | python3 -c '
import sys
from urllib.parse import urlsplit

hosts = set()
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    parsed = urlsplit(raw)
    if parsed.scheme == "https" and parsed.hostname:
        hosts.add(parsed.hostname.lower())
for host in sorted(hosts):
    print(host)
'
}

dns_zone_for_daemonset() {
  local daemonset_name="$1"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
matches = []
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    command = container.get("command") or []
    env_names = {entry.get("name") for entry in container.get("env") or []}
    if command != ["/usr/local/bin/fugue-dns"] or "FUGUE_DNS_ZONE" not in env_names:
        continue
    matches.append(container)
if len(matches) != 1:
    raise SystemExit("DNS daemonset must have exactly one semantic fugue-dns container")
for container in matches:
    for entry in container.get("env") or []:
        if entry.get("name") == "FUGUE_DNS_ZONE":
            print((entry.get("value") or "").strip())
            raise SystemExit(0)
'
}

authoritative_dns_targets_for_daemonset() {
  local daemonset_name="$1"
  local daemonset_document
  local nodes_document
  local selector

  daemonset_document="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json)" || return $?
  nodes_document="$(kubectl_cmd get nodes -o json)" || return $?
  selector="$(daemonset_selector "${daemonset_name}")" || return $?
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -l "${selector}" -o json | python3 -c '
import hashlib
import ipaddress
import json
import sys

daemonset = json.loads(sys.argv[1])
nodes_document = json.loads(sys.argv[2])
requested_name = sys.argv[3]
expected_namespace = sys.argv[4]
daemonset_metadata = daemonset.get("metadata") or {}
daemonset_status = daemonset.get("status") or {}
daemonset_name = daemonset_metadata.get("name") or ""
daemonset_uid = daemonset_metadata.get("uid") or ""
generation = daemonset_metadata.get("generation")
observed_generation = daemonset_status.get("observedGeneration")
desired = daemonset_status.get("desiredNumberScheduled") or 0
ready_count = daemonset_status.get("numberReady") or 0
available = daemonset_status.get("numberAvailable") or 0
unavailable = daemonset_status.get("numberUnavailable") or 0
misscheduled = daemonset_status.get("numberMisscheduled") or 0
if (
    not daemonset_name
    or not daemonset_uid
    or daemonset.get("apiVersion") != "apps/v1"
    or daemonset.get("kind") != "DaemonSet"
    or daemonset_name != requested_name
    or daemonset_metadata.get("namespace") != expected_namespace
    or generation != observed_generation
    or desired <= 0
    or ready_count != desired
    or available != desired
    or unavailable != 0
    or misscheduled != 0
):
    raise SystemExit("authoritative DNS daemonSet is not a stable, fully Ready identity")
daemonset_identity = {
    "apiVersion": daemonset.get("apiVersion"),
    "kind": daemonset.get("kind"),
    "metadata": {
        "generation": generation,
        "name": daemonset_name,
        "namespace": daemonset_metadata.get("namespace") or "",
        "uid": daemonset_uid,
    },
    "spec": daemonset.get("spec") or {},
    "status": daemonset_status,
}
daemonset_identity_hash = "sha256:" + hashlib.sha256(
    json.dumps(daemonset_identity, separators=(",", ":"), sort_keys=True).encode()
).hexdigest()

semantic = []
for container in (((daemonset.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []:
    command = container.get("command") or []
    env_names = {entry.get("name") for entry in container.get("env") or []}
    if command == ["/usr/local/bin/fugue-dns"] and "FUGUE_DNS_ZONE" in env_names:
        semantic.append(container)
if len(semantic) != 1:
    raise SystemExit("authoritative DNS daemonSet must have exactly one semantic fugue-dns container")
ports = {}
for port in semantic[0].get("ports") or []:
    name = port.get("name")
    if not isinstance(name, str) or name in ports:
        raise SystemExit("authoritative DNS daemonSet has an unnamed or duplicate semantic port")
    ports[name] = port
if "dns-udp" not in ports or "dns-tcp" not in ports:
    raise SystemExit("authoritative DNS daemonSet is missing its public UDP/TCP ports")
udp = ports["dns-udp"]
tcp = ports["dns-tcp"]

def exact_port(entry, protocol):
    container_port = entry.get("containerPort")
    host_port = entry.get("hostPort")
    host_ip = str(entry.get("hostIP") or "").strip()
    if (
        entry.get("protocol") != protocol
        or isinstance(container_port, bool)
        or not isinstance(container_port, int)
        or not 1 <= container_port <= 65535
        or isinstance(host_port, bool)
        or not isinstance(host_port, int)
        or not 1 <= host_port <= 65535
    ):
        raise SystemExit(f"authoritative DNS daemonSet has an invalid {protocol} public port")
    try:
        parsed = ipaddress.ip_address(host_ip)
    except ValueError:
        raise SystemExit(f"authoritative DNS daemonSet has an invalid {protocol} hostIP")
    if parsed.version != 4 or str(parsed) != host_ip:
        raise SystemExit(f"authoritative DNS daemonSet {protocol} hostIP is not canonical IPv4")
    return container_port, host_port, host_ip

udp_container_port, udp_host_port, udp_host_ip = exact_port(udp, "UDP")
tcp_container_port, tcp_host_port, tcp_host_ip = exact_port(tcp, "TCP")
if udp_host_ip != tcp_host_ip:
    raise SystemExit("authoritative DNS UDP/TCP public ports do not share one hostIP")

nodes = {}
for node in nodes_document.get("items") or []:
    metadata = node.get("metadata") or {}
    name = str(metadata.get("name") or "").strip()
    if not name or name in nodes:
        raise SystemExit("authoritative DNS node inventory has an unnamed or duplicate Node")
    nodes[name] = node

def node_query_identity(node, expected_name):
    metadata = node.get("metadata") or {}
    if (
        node.get("apiVersion") != "v1"
        or node.get("kind") != "Node"
        or metadata.get("name") != expected_name
        or not metadata.get("uid")
    ):
        raise SystemExit("authoritative DNS target Node identity is invalid")
    external_ipv4 = set()
    for address in (node.get("status") or {}).get("addresses") or []:
        if address.get("type") != "ExternalIP":
            continue
        raw = str(address.get("address") or "").strip()
        try:
            parsed = ipaddress.ip_address(raw)
        except ValueError:
            continue
        if parsed.version == 4:
            external_ipv4.add(str(parsed))
    if len(external_ipv4) != 1:
        raise SystemExit(
            f"authoritative DNS target Node {expected_name} has {len(external_ipv4)} ExternalIPv4 addresses; expected 1"
        )
    identity = {
        "apiVersion": node.get("apiVersion"),
        "kind": node.get("kind"),
        "metadata": {
            "generation": metadata.get("generation"),
            "name": metadata.get("name"),
            "uid": metadata.get("uid"),
        },
        "spec": node.get("spec") or {},
        "status": node.get("status") or {},
    }
    identity_hash = "sha256:" + hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    return str(metadata.get("uid")), identity_hash, next(iter(external_ipv4))

def pod_query_identity(pod):
    metadata = pod.get("metadata") or {}
    identity = {
        "apiVersion": pod.get("apiVersion"),
        "kind": pod.get("kind"),
        "metadata": {
            "deletionTimestamp": metadata.get("deletionTimestamp"),
            "generation": metadata.get("generation"),
            "labels": metadata.get("labels") or {},
            "name": metadata.get("name"),
            "namespace": metadata.get("namespace"),
            "ownerReferences": metadata.get("ownerReferences") or [],
            "uid": metadata.get("uid"),
        },
        "spec": pod.get("spec") or {},
        "status": pod.get("status") or {},
    }
    return "sha256:" + hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()

doc = json.load(sys.stdin)
rows = []
seen_external_ips = set()
seen_pod_host_ips = set()
seen_names = set()
seen_uids = set()
seen_nodes = set()
for pod in doc.get("items") or []:
    metadata = pod.get("metadata") or {}
    status = pod.get("status") or {}
    name = metadata.get("name") or ""
    uid = metadata.get("uid") or ""
    node_name = (pod.get("spec") or {}).get("nodeName") or ""
    pod_host_ip = status.get("hostIP") or ""
    revision = (metadata.get("labels") or {}).get("controller-revision-hash") or ""
    controllers = [
        owner
        for owner in metadata.get("ownerReferences") or []
        if isinstance(owner, dict) and owner.get("controller") is True
    ]
    owned = (
        len(controllers) == 1
        and controllers[0].get("apiVersion") == "apps/v1"
        and controllers[0].get("kind") == "DaemonSet"
        and controllers[0].get("name") == daemonset_name
        and controllers[0].get("uid") == daemonset_uid
    )
    ready = any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in status.get("conditions") or []
    )
    if (
        not name
        or not uid
        or not node_name
        or not pod_host_ip
        or pod.get("apiVersion") != "v1"
        or pod.get("kind") != "Pod"
        or metadata.get("namespace") != expected_namespace
        or not revision
        or status.get("phase") != "Running"
        or metadata.get("deletionTimestamp")
        or not owned
        or not ready
        or name in seen_names
        or uid in seen_uids
        or node_name in seen_nodes
        or pod_host_ip in seen_pod_host_ips
    ):
        raise SystemExit(
            "DNS daemonSet target contains a duplicated, unowned, unready, "
            "or identity-less Pod"
        )
    seen_names.add(name)
    seen_uids.add(uid)
    seen_nodes.add(node_name)
    seen_pod_host_ips.add(pod_host_ip)
    node = nodes.get(node_name)
    if node is None:
        raise SystemExit(f"authoritative DNS Pod {name} references an absent Node {node_name}")
    node_uid, node_identity_hash, external_ip = node_query_identity(node, node_name)
    if external_ip != udp_host_ip or external_ip != tcp_host_ip:
        raise SystemExit(
            f"authoritative DNS Pod {name} Node ExternalIPv4 does not match both public hostIP mappings"
        )
    if external_ip in seen_external_ips:
        raise SystemExit("authoritative DNS target ExternalIPv4 is duplicated across exact Pods")
    seen_external_ips.add(external_ip)
    rows.append((
        external_ip,
        str(udp_host_port),
        str(udp_container_port),
        str(tcp_host_port),
        str(tcp_container_port),
        name,
        uid,
        pod_host_ip,
        pod_query_identity(pod),
        node_name,
        node_uid,
        node_identity_hash,
        revision,
        daemonset_uid,
        daemonset_identity_hash,
    ))
if len(rows) != desired:
    raise SystemExit(
        f"authoritative DNS daemonSet has {len(rows)} exact Pod targets; expected {desired}"
    )
for row in sorted(rows):
    print("\t".join(row))
' "${daemonset_document}" "${nodes_document}" "${daemonset_name}" "${FUGUE_NAMESPACE}"
}

authoritative_dns_signing_keyring_for_pod() (
  set -euo pipefail
  local pod_json="$1"
  local container_name="$2"
  local work_dir="$3"
  local slot=""
  local descriptor=""
  local source=""
  local secret_name=""
  local secret_key=""
  local optional=""
  local secret_file=""

  python3 - "${pod_json}" "${container_name}" >"${work_dir}/signing-refs.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    pod = json.load(handle)
container_name = sys.argv[2]
matches = [
    container
    for container in (pod.get("spec") or {}).get("containers") or []
    if container.get("name") == container_name
]
if len(matches) != 1:
    raise SystemExit("authoritative DNS signing keyring container identity is ambiguous")
entries = {
    entry.get("name"): entry
    for entry in matches[0].get("env") or []
    if isinstance(entry, dict) and isinstance(entry.get("name"), str)
}

def direct(name, required=False):
    entry = entries.get(name) or {}
    value = entry.get("value")
    if not isinstance(value, str):
        if required:
            raise SystemExit(f"authoritative DNS {name} must be a literal Pod env value")
        return ""
    value = value.strip()
    if required and not value:
        raise SystemExit(f"authoritative DNS {name} is empty")
    return value

def key_source(name, required):
    entry = entries.get(name) or {}
    value = entry.get("value")
    if isinstance(value, str) and value.strip():
        return {"source": "value", "value": value}
    reference = ((entry.get("valueFrom") or {}).get("secretKeyRef") or {})
    secret_name = str(reference.get("name") or "").strip()
    secret_key = str(reference.get("key") or "").strip()
    optional = reference.get("optional") is True
    if secret_name and secret_key:
        return {
            "optional": optional,
            "secretKey": secret_key,
            "secretName": secret_name,
            "source": "secret",
        }
    if required:
        raise SystemExit(f"authoritative DNS {name} has no literal or Secret key source")
    return {"source": "none"}

print(json.dumps({
    "previous": {
        **key_source("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY", False),
        "keyID": direct("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID"),
    },
    "primary": {
        **key_source("FUGUE_BUNDLE_SIGNING_KEY", True),
        "keyID": direct("FUGUE_BUNDLE_SIGNING_KEY_ID", True),
    },
    "revokedKeyIDs": direct("FUGUE_BUNDLE_REVOKED_KEY_IDS"),
}, separators=(",", ":"), sort_keys=True))
PY
  chmod 600 "${work_dir}/signing-refs.json"

  for slot in primary previous; do
    descriptor="$(python3 - "${work_dir}/signing-refs.json" "${slot}" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as handle:
    ref = (json.load(handle).get(sys.argv[2]) or {})
print("|".join([
    str(ref.get("source") or "none"),
    str(ref.get("secretName") or ""),
    str(ref.get("secretKey") or ""),
    "true" if ref.get("optional") is True else "false",
]))
PY
)"
    IFS='|' read -r source secret_name secret_key optional <<<"${descriptor}"
    secret_file="${work_dir}/${slot}-signing-secret.json"
    if [[ "${source}" == "secret" ]]; then
      if ! kubectl_cmd -n "${FUGUE_NAMESPACE}" get "secret/${secret_name}" -o json >"${secret_file}"; then
        if [[ "${optional}" != "true" ]]; then
          return 1
        fi
        printf '%s\n' '{"data":{}}' >"${secret_file}"
      fi
    else
      printf '%s\n' '{"data":{}}' >"${secret_file}"
    fi
    chmod 600 "${secret_file}"
  done

  python3 - "${work_dir}/signing-refs.json" \
    "${work_dir}/primary-signing-secret.json" \
    "${work_dir}/previous-signing-secret.json" <<'PY'
import base64
import json
import re
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    refs = json.load(handle)

def resolve(slot, secret_path):
    ref = refs.get(slot) or {}
    source = ref.get("source")
    if source == "value":
        key = str(ref.get("value") or "").strip()
    elif source == "secret":
        with open(secret_path, encoding="utf-8") as handle:
            secret = json.load(handle)
        encoded = str((secret.get("data") or {}).get(ref.get("secretKey")) or "").strip()
        if not encoded:
            if ref.get("optional") is True:
                key = ""
            else:
                raise SystemExit(f"authoritative DNS {slot} signing Secret key is absent")
        else:
            try:
                key = base64.b64decode(encoded, validate=True).decode("utf-8").strip()
            except (ValueError, UnicodeDecodeError) as exc:
                raise SystemExit(f"authoritative DNS {slot} signing Secret key is invalid: {exc}")
    else:
        key = ""
    key_id = str(ref.get("keyID") or "").strip()
    return key, key_id

primary_key, primary_key_id = resolve("primary", sys.argv[2])
previous_key, previous_key_id = resolve("previous", sys.argv[3])
if not primary_key or not primary_key_id:
    raise SystemExit("authoritative DNS primary signing keyring identity is incomplete")
if previous_key and not previous_key_id:
    raise SystemExit("authoritative DNS previous signing key has no key ID")
revoked = sorted({
    value.lower()
    for value in re.split(r"[\s,;]+", str(refs.get("revokedKeyIDs") or ""))
    if value
})
print(json.dumps({
    "previousKey": previous_key,
    "previousKeyID": previous_key_id,
    "primaryKey": primary_key,
    "primaryKeyID": primary_key_id,
    "revokedKeyIDs": revoked,
}, separators=(",", ":"), sort_keys=True))
PY
)

authoritative_dns_publication_snapshot_for_pod() (
  set -euo pipefail
  local pod_name="$1"
  local expected_zone="$2"
  local hostnames="$3"
  local expected_query_server="$4"
  local expected_host_port="$5"
  local expected_container_port="$6"
  local expected_transport="$7"
  local expected_pod_uid="$8"
  local expected_pod_host_ip="$9"
  local expected_pod_identity="${10}"
  local expected_node_name="${11}"
  local expected_node_uid="${12}"
  local expected_node_identity="${13}"
  local expected_revision="${14}"
  local expected_daemonset_name="${15}"
  local expected_daemonset_uid="${16}"
  local expected_daemonset_identity="${17}"
  local work_dir
  local runtime
  local container_name
  local cache_path
  local health_port
  local proxy_path

  umask 077
  work_dir="$(mktemp -d "${TMPDIR:-/tmp}/fugue-authoritative-publication.XXXXXX")"
  trap 'rm -rf "${work_dir}"' EXIT
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${expected_daemonset_name}" -o json >"${work_dir}/daemonset-before.json"
  kubectl_cmd get "node/${expected_node_name}" -o json >"${work_dir}/node-before.json"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "pod/${pod_name}" -o json >"${work_dir}/pod-before.json"
  runtime="$(python3 - "${work_dir}/pod-before.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    pod = json.load(handle)
matches = []
for container in (pod.get("spec") or {}).get("containers") or []:
    env = {
        item.get("name"): item.get("value")
        for item in container.get("env") or []
        if isinstance(item, dict) and isinstance(item.get("value"), str)
    }
    if container.get("command") == ["/usr/local/bin/fugue-dns"] and env.get("FUGUE_DNS_ZONE", "").strip():
        matches.append((container, env))
if len(matches) != 1:
    raise SystemExit("authoritative DNS Pod must have exactly one semantic fugue-dns container")
container, env = matches[0]
cache_path = env.get("FUGUE_DNS_CACHE_PATH", "").strip()
health_ports = [
    int(port.get("containerPort") or 0)
    for port in container.get("ports") or []
    if port.get("name") == "http" and int(port.get("containerPort") or 0) > 0
]
if not container.get("name") or not cache_path or len(health_ports) != 1:
    raise SystemExit("authoritative DNS Pod is missing its container, cache path, or health port")
print("\t".join([container["name"], cache_path, str(health_ports[0])]))
PY
)"
  IFS=$'\t' read -r container_name cache_path health_port <<<"${runtime}"
  [[ -n "${container_name}" && -n "${cache_path}" && "${health_port}" =~ ^[1-9][0-9]*$ ]] || return 1
  authoritative_dns_signing_keyring_for_pod \
    "${work_dir}/pod-before.json" "${container_name}" "${work_dir}" >"${work_dir}/keyring.json"
  chmod 600 "${work_dir}/keyring.json"
  proxy_path="/api/v1/namespaces/${FUGUE_NAMESPACE}/pods/${pod_name}:${health_port}/proxy/healthz"
  kubectl_cmd get --raw="${proxy_path}" >"${work_dir}/health-before.json"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" exec "${pod_name}" -c "${container_name}" -- cat "${cache_path}" >"${work_dir}/cache.json"
  kubectl_cmd get --raw="${proxy_path}" >"${work_dir}/health-after.json"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "pod/${pod_name}" -o json >"${work_dir}/pod-after.json"
  kubectl_cmd get "node/${expected_node_name}" -o json >"${work_dir}/node-after.json"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${expected_daemonset_name}" -o json >"${work_dir}/daemonset-after.json"
  printf '%s\n' "${hostnames}" >"${work_dir}/hostnames"

  python3 - "${work_dir}" "${pod_name}" "${expected_zone}" "${container_name}" "${cache_path}" "${health_port}" \
    "${expected_query_server}" "${expected_host_port}" "${expected_container_port}" "${expected_transport}" \
    "${expected_pod_uid}" "${expected_pod_host_ip}" "${expected_pod_identity}" \
    "${expected_node_name}" "${expected_node_uid}" "${expected_node_identity}" "${expected_revision}" \
    "${expected_daemonset_name}" "${expected_daemonset_uid}" "${expected_daemonset_identity}" <<'PY'
import datetime
import base64
import hashlib
import hmac
import ipaddress
import json
import re
import sys

(
    work_dir,
    pod_name,
    expected_zone,
    container_name,
    cache_path,
    health_port,
    expected_query_server,
    expected_host_port,
    expected_container_port,
    expected_transport,
    expected_pod_uid,
    expected_pod_host_ip,
    expected_pod_identity,
    expected_node_name,
    expected_node_uid,
    expected_node_identity,
    expected_revision,
    expected_daemonset_name,
    expected_daemonset_uid,
    expected_daemonset_identity,
) = sys.argv[1:21]

def read_json(name):
    with open(f"{work_dir}/{name}", encoding="utf-8") as handle:
        return json.load(handle)

def fqdn(value):
    value = str(value or "").strip().rstrip(".").lower()
    return value + "." if value else ""

def parse_time(value):
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    return datetime.datetime.fromisoformat(raw).astimezone(datetime.timezone.utc)

def compact_raw_json(raw):
    output = []
    in_string = False
    escaped = False
    for char in raw:
        if in_string:
            output.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
            output.append(char)
        elif char not in " \t\r\n":
            output.append(char)
    return "".join(output)

def top_level_raw_value(raw, wanted):
    decoder = json.JSONDecoder()
    index = 0
    length = len(raw)
    while index < length and raw[index].isspace():
        index += 1
    if index >= length or raw[index] != "{":
        raise SystemExit("authoritative DNS cache envelope is not a JSON object")
    index += 1
    while True:
        while index < length and raw[index].isspace():
            index += 1
        if index < length and raw[index] == "}":
            break
        key, index = decoder.raw_decode(raw, index)
        if not isinstance(key, str):
            raise SystemExit("authoritative DNS cache envelope has a non-string key")
        while index < length and raw[index].isspace():
            index += 1
        if index >= length or raw[index] != ":":
            raise SystemExit("authoritative DNS cache envelope is malformed")
        index += 1
        while index < length and raw[index].isspace():
            index += 1
        start = index
        _, index = decoder.raw_decode(raw, index)
        if key == wanted:
            return raw[start:index]
        while index < length and raw[index].isspace():
            index += 1
        if index < length and raw[index] == ",":
            index += 1
            continue
        if index < length and raw[index] == "}":
            break
        raise SystemExit("authoritative DNS cache envelope has an invalid member boundary")
    raise SystemExit(f"authoritative DNS cache envelope has no {wanted}")

def daemonset_identity(document):
    metadata = document.get("metadata") or {}
    status = document.get("status") or {}
    generation = metadata.get("generation")
    desired = status.get("desiredNumberScheduled") or 0
    if (
        (document.get("apiVersion") or "apps/v1") != "apps/v1"
        or (document.get("kind") or "DaemonSet") != "DaemonSet"
        or metadata.get("name") != expected_daemonset_name
        or metadata.get("uid") != expected_daemonset_uid
        or generation != status.get("observedGeneration")
        or desired <= 0
        or (status.get("numberReady") or 0) != desired
        or (status.get("numberAvailable") or 0) != desired
        or (status.get("numberUnavailable") or 0) != 0
        or (status.get("numberMisscheduled") or 0) != 0
    ):
        raise SystemExit("authoritative DNS daemonSet identity or stable status drifted")
    identity = {
        "apiVersion": document.get("apiVersion") or "apps/v1",
        "kind": document.get("kind") or "DaemonSet",
        "metadata": {
            "generation": generation,
            "name": metadata.get("name") or "",
            "namespace": metadata.get("namespace") or "",
            "uid": metadata.get("uid") or "",
        },
        "spec": document.get("spec") or {},
        "status": status,
    }
    return "sha256:" + hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()

def daemonset_query_port(document):
    semantic = []
    for container in ((((document.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []):
        command = container.get("command") or []
        env_names = {entry.get("name") for entry in container.get("env") or []}
        if command == ["/usr/local/bin/fugue-dns"] and "FUGUE_DNS_ZONE" in env_names:
            semantic.append(container)
    if len(semantic) != 1:
        raise SystemExit("authoritative DNS daemonSet semantic container drifted")
    by_name = {}
    for port in semantic[0].get("ports") or []:
        name = port.get("name")
        if not isinstance(name, str) or name in by_name:
            raise SystemExit("authoritative DNS daemonSet port identity is ambiguous")
        by_name[name] = port
    if "dns-udp" not in by_name or "dns-tcp" not in by_name:
        raise SystemExit("authoritative DNS daemonSet public transport ports drifted")
    udp = by_name["dns-udp"]
    tcp = by_name["dns-tcp"]
    if udp.get("hostIP") != tcp.get("hostIP") or udp.get("hostIP") != expected_query_server:
        raise SystemExit("authoritative DNS daemonSet UDP/TCP hostIP drifted")
    selected = udp if expected_transport == "udp" else tcp
    expected_protocol = expected_transport.upper()
    try:
        host_port = int(expected_host_port)
        container_port = int(expected_container_port)
    except ValueError:
        raise SystemExit("authoritative DNS query port identity is not numeric")
    if (
        not 1 <= host_port <= 65535
        or not 1 <= container_port <= 65535
        or selected.get("protocol") != expected_protocol
        or selected.get("hostPort") != host_port
        or selected.get("containerPort") != container_port
        or selected.get("hostIP") != expected_query_server
    ):
        raise SystemExit("authoritative DNS daemonSet query transport mapping drifted")
    return host_port, container_port

def node_query_identity(document):
    metadata = document.get("metadata") or {}
    if (
        document.get("apiVersion") != "v1"
        or document.get("kind") != "Node"
        or metadata.get("name") != expected_node_name
        or metadata.get("uid") != expected_node_uid
    ):
        raise SystemExit("authoritative DNS query Node identity drifted")
    external_ipv4 = set()
    for address in (document.get("status") or {}).get("addresses") or []:
        if address.get("type") != "ExternalIP":
            continue
        raw = str(address.get("address") or "").strip()
        try:
            parsed = ipaddress.ip_address(raw)
        except ValueError:
            continue
        if parsed.version == 4:
            external_ipv4.add(str(parsed))
    if external_ipv4 != {expected_query_server}:
        raise SystemExit("authoritative DNS query Node ExternalIPv4 drifted")
    identity = {
        "apiVersion": document.get("apiVersion"),
        "kind": document.get("kind"),
        "metadata": {
            "generation": metadata.get("generation"),
            "name": metadata.get("name"),
            "uid": metadata.get("uid"),
        },
        "spec": document.get("spec") or {},
        "status": document.get("status") or {},
    }
    return "sha256:" + hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()

def pod_query_identity(document):
    metadata = document.get("metadata") or {}
    identity = {
        "apiVersion": document.get("apiVersion"),
        "kind": document.get("kind"),
        "metadata": {
            "deletionTimestamp": metadata.get("deletionTimestamp"),
            "generation": metadata.get("generation"),
            "labels": metadata.get("labels") or {},
            "name": metadata.get("name"),
            "namespace": metadata.get("namespace"),
            "ownerReferences": metadata.get("ownerReferences") or [],
            "uid": metadata.get("uid"),
        },
        "spec": document.get("spec") or {},
        "status": document.get("status") or {},
    }
    return "sha256:" + hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()

def raw_array_values(raw):
    decoder = json.JSONDecoder()
    values = []
    index = 0
    length = len(raw)
    while index < length and raw[index].isspace():
        index += 1
    if index >= length or raw[index] != "[":
        raise SystemExit("authoritative DNS bundle signatures are not a JSON array")
    index += 1
    while True:
        while index < length and raw[index].isspace():
            index += 1
        if index < length and raw[index] == "]":
            return values
        start = index
        _, index = decoder.raw_decode(raw, index)
        values.append(raw[start:index])
        while index < length and raw[index].isspace():
            index += 1
        if index < length and raw[index] == ",":
            index += 1
            continue
        if index < length and raw[index] == "]":
            return values
        raise SystemExit("authoritative DNS bundle signatures have an invalid boundary")

def raw_bundle_signing_payload(bundle, bundle_raw, key_id_raw, valid_until_raw):
    members = []
    for source, target in (
        ("schema_version", "schema_version"),
        ("version", "version"),
        ("generation", "generation"),
        ("previous_generation", "previous_generation"),
        ("generated_at", "generated_at"),
    ):
        if bundle.get(source) not in (None, ""):
            members.append(json.dumps(target) + ":" + compact_raw_json(top_level_raw_value(bundle_raw, source)))
    members.append('"valid_until":' + compact_raw_json(valid_until_raw))
    if bundle.get("issuer") not in (None, ""):
        members.append('"issuer":' + compact_raw_json(top_level_raw_value(bundle_raw, "issuer")))
    members.append('"key_id":' + compact_raw_json(key_id_raw))
    if bundle.get("dns_node_id") not in (None, ""):
        members.append('"edge_id":' + compact_raw_json(top_level_raw_value(bundle_raw, "dns_node_id")))
    if bundle.get("edge_group_id") not in (None, ""):
        members.append('"edge_group_id":' + compact_raw_json(top_level_raw_value(bundle_raw, "edge_group_id")))
    members.append('"records":' + compact_raw_json(top_level_raw_value(bundle_raw, "records")))
    return ("{" + ",".join(members) + "}").encode("utf-8")

def verify_bundle_signature(bundle, bundle_raw, keyring):
    schema = str(bundle.get("schema_version") or "").strip()
    if schema and schema.split(".", 1)[0] != "1":
        raise SystemExit("authoritative DNS bundle schema version is unsupported")
    keys = {
        str(keyring.get("primaryKeyID") or "").strip().lower(): str(keyring.get("primaryKey") or "").strip(),
        str(keyring.get("previousKeyID") or "").strip().lower(): str(keyring.get("previousKey") or "").strip(),
    }
    revoked = {str(value).strip().lower() for value in keyring.get("revokedKeyIDs") or [] if str(value).strip()}
    candidates = [(bundle, bundle_raw)]
    signatures = bundle.get("signatures") or []
    if signatures:
        signatures_raw = top_level_raw_value(bundle_raw, "signatures")
        raw_candidates = raw_array_values(signatures_raw)
        if len(raw_candidates) != len(signatures):
            raise SystemExit("authoritative DNS bundle signature identities are inconsistent")
        candidates.extend(zip(signatures, raw_candidates))
    for candidate, candidate_raw in candidates:
        if not isinstance(candidate, dict):
            continue
        key_id = str(candidate.get("key_id") or "").strip()
        signature = str(candidate.get("signature") or "").strip()
        valid_until = candidate.get("valid_until")
        if not key_id or not signature or not valid_until or key_id.lower() in revoked:
            continue
        key = keys.get(key_id.lower()) or ""
        if not key:
            continue
        key_id_raw = top_level_raw_value(candidate_raw, "key_id")
        valid_until_raw = top_level_raw_value(candidate_raw, "valid_until")
        payload = raw_bundle_signing_payload(bundle, bundle_raw, key_id_raw, valid_until_raw)
        expected = base64.urlsafe_b64encode(
            hmac.new(key.encode(), payload, hashlib.sha256).digest()
        ).rstrip(b"=").decode()
        if hmac.compare_digest(signature, expected):
            return
    raise SystemExit("authoritative DNS published bundle HMAC did not verify against the declared keyring")

pod_before = read_json("pod-before.json")
pod_after = read_json("pod-after.json")
node_before = read_json("node-before.json")
node_after = read_json("node-after.json")
daemonset_before = read_json("daemonset-before.json")
daemonset_after = read_json("daemonset-after.json")
health_before = read_json("health-before.json")
health_after = read_json("health-after.json")
cache_doc = read_json("cache.json")
keyring = read_json("keyring.json")
with open(f"{work_dir}/cache.json", encoding="utf-8") as handle:
    cache_raw = handle.read()
with open(f"{work_dir}/hostnames", encoding="utf-8") as handle:
    hostnames = sorted({fqdn(line) for line in handle if fqdn(line)})

if expected_transport not in {"udp", "tcp"}:
    raise SystemExit("authoritative DNS query transport identity is invalid")
try:
    query_server_ip = ipaddress.ip_address(expected_query_server)
except ValueError:
    raise SystemExit("authoritative DNS query server is not an IP address")
if query_server_ip.version != 4 or str(query_server_ip) != expected_query_server:
    raise SystemExit("authoritative DNS query server is not canonical ExternalIPv4")

daemonset_before_identity = daemonset_identity(daemonset_before)
daemonset_after_identity = daemonset_identity(daemonset_after)
if (
    daemonset_before_identity != expected_daemonset_identity
    or daemonset_after_identity != expected_daemonset_identity
):
    raise SystemExit("authoritative DNS daemonSet spec/status changed during publication capture")
query_host_port, query_container_port = daemonset_query_port(daemonset_before)
if daemonset_query_port(daemonset_after) != (query_host_port, query_container_port):
    raise SystemExit("authoritative DNS daemonSet query port changed during publication capture")
node_before_identity = node_query_identity(node_before)
node_after_identity = node_query_identity(node_after)
if node_before_identity != expected_node_identity or node_after_identity != expected_node_identity:
    raise SystemExit("authoritative DNS Node spec/status changed during publication capture")

def validate_pod(document):
    metadata = document.get("metadata") or {}
    status = document.get("status") or {}
    controllers = [
        owner
        for owner in metadata.get("ownerReferences") or []
        if isinstance(owner, dict) and owner.get("controller") is True
    ]
    ready = any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in status.get("conditions") or []
    )
    identity = pod_query_identity(document)
    if (
        document.get("apiVersion") != "v1"
        or document.get("kind") != "Pod"
        or metadata.get("name") != pod_name
        or metadata.get("uid") != expected_pod_uid
        or (document.get("spec") or {}).get("nodeName") != expected_node_name
        or status.get("hostIP") != expected_pod_host_ip
        or (metadata.get("labels") or {}).get("controller-revision-hash") != expected_revision
        or status.get("phase") != "Running"
        or metadata.get("deletionTimestamp")
        or not ready
        or len(controllers) != 1
        or controllers[0].get("apiVersion") != "apps/v1"
        or controllers[0].get("kind") != "DaemonSet"
        or controllers[0].get("name") != expected_daemonset_name
        or controllers[0].get("uid") != expected_daemonset_uid
        or identity != expected_pod_identity
    ):
        raise SystemExit("authoritative DNS publication Pod identity or owner drifted")
    return identity

pod_before_identity = validate_pod(pod_before)
pod_after_identity = validate_pod(pod_after)
if pod_before_identity != pod_after_identity:
    raise SystemExit("authoritative DNS Pod changed during publication capture")
pod = pod_after

if health_before != health_after:
    raise SystemExit("authoritative DNS publication changed while its cache snapshot was captured")
if not health_after.get("healthy"):
    raise SystemExit("authoritative DNS Pod health is not healthy")

payload = cache_doc.get("payload") if isinstance(cache_doc, dict) else None
if payload is not None:
    payload_raw = top_level_raw_value(cache_raw, "payload")
    computed_content_hash = "sha256:" + hashlib.sha256(
        compact_raw_json(payload_raw).encode("utf-8")
    ).hexdigest()
    if (
        cache_doc.get("schema_version") != "1.0"
        or cache_doc.get("kind") != "dns_answer_bundle"
        or not str(cache_doc.get("generation") or "").strip()
        or str(cache_doc.get("content_hash") or "").strip().lower() != computed_content_hash
        or not isinstance(payload, dict)
    ):
        raise SystemExit("authoritative DNS cache envelope identity is invalid")
    cache_content_hash = computed_content_hash
    cached_raw = payload_raw
    cached = payload
else:
    cache_content_hash = "sha256:" + hashlib.sha256(
        compact_raw_json(cache_raw).encode("utf-8")
    ).hexdigest()
    cached_raw = cache_raw
    cached = cache_doc
if not isinstance(cached, dict) or cached.get("version") != 1:
    raise SystemExit("authoritative DNS cache payload version is invalid")
bundle = cached.get("bundle")
if not isinstance(bundle, dict):
    raise SystemExit("authoritative DNS cache contains no published bundle")
bundle_raw = top_level_raw_value(cached_raw, "bundle")

zone = fqdn(expected_zone)
bundle_zone = fqdn(bundle.get("zone"))
health_zone = fqdn(health_after.get("zone"))
bundle_version = str(bundle.get("version") or "").strip()
bundle_generation = str(bundle.get("generation") or bundle_version).strip()
health_version = str(health_after.get("bundle_version") or "").strip()
health_generation = str(health_after.get("serving_generation") or health_version).strip()
if (
    not zone
    or bundle_zone != zone
    or health_zone != zone
    or not bundle_version
    or bundle_version != health_version
    or bundle_generation != health_generation
    or str(health_after.get("cache_path") or "").strip() != cache_path
):
    raise SystemExit("authoritative DNS Pod health/cache/config identity is inconsistent")
if payload is not None and str(cache_doc.get("generation") or "").strip() != bundle_generation:
    raise SystemExit("authoritative DNS cache envelope generation does not match the served bundle")
verify_bundle_signature(bundle, bundle_raw, keyring)
try:
    if parse_time(bundle.get("valid_until")) <= datetime.datetime.now(datetime.timezone.utc):
        raise SystemExit("authoritative DNS published bundle is expired")
except (TypeError, ValueError):
    raise SystemExit("authoritative DNS published bundle has an invalid valid_until")

semantic = []
for container in (pod.get("spec") or {}).get("containers") or []:
    env = {
        item.get("name"): item.get("value")
        for item in container.get("env") or []
        if isinstance(item, dict) and isinstance(item.get("value"), str)
    }
    if container.get("name") == container_name and container.get("command") == ["/usr/local/bin/fugue-dns"]:
        semantic.append(env)
if len(semantic) != 1 or fqdn(semantic[0].get("FUGUE_DNS_ZONE")) != zone:
    raise SystemExit("authoritative DNS Pod runtime zone drifted from the published bundle")
env = semantic[0]
nameservers = [
    fqdn(value)
    for value in re.split(r"[\s,;]+", str(env.get("FUGUE_DNS_NAMESERVERS") or ""))
    if fqdn(value)
]
soa_mname = nameservers[0] if nameservers else fqdn("ns1." + zone.rstrip("."))
soa_rname = fqdn("hostmaster." + zone.rstrip("."))
try:
    minimum = int(str(env.get("FUGUE_DNS_TTL") or "60").strip())
except ValueError:
    raise SystemExit("authoritative DNS Pod has an invalid TTL")
if minimum <= 0:
    raise SystemExit("authoritative DNS Pod TTL must be positive")

records = bundle.get("records") or []
if not isinstance(records, list) or int(health_after.get("record_count") or 0) != len(records):
    raise SystemExit("authoritative DNS health record count does not match the published bundle")

def matching_records(name):
    exact = [record for record in records if isinstance(record, dict) and fqdn(record.get("name")) == name]
    if exact:
        return exact
    labels = name.rstrip(".").split(".")
    if len(labels) < 2:
        return []
    wildcard = fqdn("*." + ".".join(labels[1:]))
    return [record for record in records if isinstance(record, dict) and fqdn(record.get("name")) == wildcard]

def candidate_eligible(candidate, policy):
    if candidate.get("max_stale_exceeded") is True:
        return False
    cache_status = str(candidate.get("cache_status") or "").strip().lower()
    if any(marker in cache_status for marker in ("error", "invalid", "corrupt", "expired", "max_stale")):
        return False
    if policy.get("health_required") is True and candidate.get("healthy") is not True:
        return False
    if policy.get("route_ready_required") is True and candidate.get("route_ready") is not True:
        return False
    if str(policy.get("policy_kind") or "").strip() != "disabled" and candidate.get("tls_ready") is not True:
        return False
    return True

answers = set()
for hostname in hostnames:
    if not hostname.endswith(zone):
        raise SystemExit(f"representative hostname {hostname} is outside authoritative zone {zone}")
    matches = matching_records(hostname)
    for record in matches:
        record_type = str(record.get("type") or "").strip().upper()
        if record_type not in {"A", "AAAA", "CNAME"}:
            continue
        raw_values = record.get("values") or []
        if not isinstance(raw_values, list):
            raise SystemExit("authoritative DNS published record values are malformed")
        values = list(raw_values)
        if record_type in {"A", "AAAA"}:
            candidates = record.get("candidates") or []
            if not isinstance(candidates, list):
                raise SystemExit("authoritative DNS published candidates are malformed")
            if candidates:
                policy = record.get("answer_policy") or {}
                if not isinstance(policy, dict):
                    raise SystemExit("authoritative DNS published answer policy is malformed")
                values = [
                candidate.get("ip")
                    for candidate in candidates
                    if isinstance(candidate, dict) and candidate_eligible(candidate, policy)
                ]
        for raw in values:
            if record_type == "CNAME":
                value = fqdn(raw)
                if value:
                    answers.add((hostname, record_type, value))
                continue
            try:
                parsed = ipaddress.ip_address(str(raw or "").strip())
            except ValueError:
                continue
            if (record_type == "A" and parsed.version == 4) or (record_type == "AAAA" and parsed.version == 6):
                answers.add((hostname, record_type, str(parsed)))
    if not any(owner == hostname and record_type in {"A", "CNAME"} for owner, record_type, _ in answers):
        raise SystemExit(f"published DNS bundle has no allowed A/CNAME answer for {hostname}")

metadata = pod.get("metadata") or {}
pod_uid = str(metadata.get("uid") or "").strip()
if not pod_uid:
    raise SystemExit("authoritative DNS Pod has no UID")
result = {
    "answers": [
        {"name": owner, "type": record_type, "value": value}
        for owner, record_type, value in sorted(answers)
    ],
    "identity": {
        "bundleVersion": bundle_version,
        "cacheContentHash": cache_content_hash,
        "clientScope": "ecs-unmapped-global-fallback",
        "containerName": container_name,
        "daemonSetIdentity": expected_daemonset_identity,
        "daemonSetName": expected_daemonset_name,
        "daemonSetUID": expected_daemonset_uid,
        "healthPort": int(health_port),
        "nodeIdentity": expected_node_identity,
        "nodeName": expected_node_name,
        "nodeUID": expected_node_uid,
        "podHostIP": expected_pod_host_ip,
        "podIdentity": expected_pod_identity,
        "podName": pod_name,
        "podUID": pod_uid,
        "queryContainerPort": query_container_port,
        "queryHostPort": query_host_port,
        "queryServer": expected_query_server,
        "queryTransport": expected_transport,
        "revision": expected_revision,
        "servingGeneration": bundle_generation,
        "zone": zone,
    },
    "soa": {
        "expire": 3600,
        "minimum": minimum,
        "mname": soa_mname,
        "owner": zone,
        "refresh": 300,
        "retry": 60,
        "rname": soa_rname,
    },
    "zone": zone,
}
print(json.dumps(result, separators=(",", ":"), sort_keys=True))
PY
)

validate_authoritative_dns_response() {
  local response_file="$1"
  local expected_config="$2"
  local expected_name="$3"
  local expected_type="$4"

  python3 - "${response_file}" "${expected_config}" "${expected_name}" "${expected_type}" <<'PY'
import ipaddress
import json
import re
import sys

path, config_text, expected_name, expected_type = sys.argv[1:5]
config = json.loads(config_text)

def fqdn(value):
    value = str(value or "").strip().rstrip(".").lower()
    return value + "." if value else ""

expected_name = fqdn(expected_name)
expected_type = expected_type.strip().upper()
if expected_type not in {"SOA", "A", "AAAA", "CNAME"} or not expected_name:
    raise SystemExit("invalid authoritative DNS response expectation")

with open(path, encoding="utf-8", errors="replace") as handle:
    lines = [line.rstrip("\n") for line in handle]
headers = []
flag_rows = []
questions = []
answers = []
got_answer_rows = 0
opt_sections = 0
question_sections = 0
answer_sections = 0
edns_rows = []
ecs_rows = []
section = ""
for line in lines:
    stripped = line.strip()
    if not stripped:
        continue
    if stripped == ";; Got answer:":
        if section or got_answer_rows:
            raise SystemExit("authoritative DNS answer preamble is duplicated or out of order")
        got_answer_rows += 1
        section = "metadata"
        continue
    header = re.fullmatch(r";; ->>HEADER<<- opcode: QUERY, status: ([A-Z]+), id: [0-9]+", stripped)
    if header:
        if section != "metadata" or got_answer_rows != 1 or headers or flag_rows:
            raise SystemExit("authoritative DNS header is duplicated or out of order")
        headers.append(header.group(1))
        continue
    flags = re.fullmatch(
        r";; flags: ([^;]*); QUERY: ([0-9]+), ANSWER: ([0-9]+), AUTHORITY: ([0-9]+), ADDITIONAL: ([0-9]+)",
        stripped,
    )
    if flags:
        if section != "metadata" or len(headers) != 1 or flag_rows:
            raise SystemExit("authoritative DNS flags/count row is duplicated or out of order")
        flag_rows.append((set(flags.group(1).split()), *(int(flags.group(index)) for index in range(2, 6))))
        continue
    if stripped == ";; OPT PSEUDOSECTION:":
        if section != "metadata" or len(flag_rows) != 1 or opt_sections:
            raise SystemExit("authoritative DNS OPT PSEUDOSECTION is duplicated or out of order")
        opt_sections += 1
        section = "opt"
        continue
    edns = re.fullmatch(r"; EDNS: version: ([0-9]+), flags:([^;]*); udp: ([0-9]+)", stripped)
    if edns:
        if section != "opt" or edns_rows or ecs_rows:
            raise SystemExit("authoritative DNS EDNS metadata is duplicated or out of order")
        edns_rows.append((int(edns.group(1)), edns.group(2).strip(), int(edns.group(3))))
        continue
    ecs = re.fullmatch(r"; CLIENT-SUBNET: (.+)", stripped)
    if ecs:
        if section != "opt" or len(edns_rows) != 1 or ecs_rows:
            raise SystemExit("authoritative DNS ECS metadata is duplicated or out of order")
        ecs_rows.append(ecs.group(1).strip())
        continue
    if stripped == ";; QUESTION SECTION:":
        if section != "opt" or len(edns_rows) != 1 or len(ecs_rows) != 1 or question_sections:
            raise SystemExit("authoritative DNS question section is duplicated or out of order")
        question_sections += 1
        section = "question"
        continue
    if stripped == ";; ANSWER SECTION:":
        if section != "question" or len(questions) != 1 or answer_sections:
            raise SystemExit("authoritative DNS answer section is duplicated or out of order")
        answer_sections += 1
        section = "answer"
        continue
    if section == "question" and stripped.startswith(";"):
        fields = stripped[1:].split()
        if len(fields) == 3:
            questions.append((fqdn(fields[0]), fields[1].upper(), fields[2].upper()))
        else:
            raise SystemExit("malformed authoritative DNS question")
    elif section == "answer":
        fields = stripped.split(None, 4)
        if len(fields) != 5:
            raise SystemExit("malformed authoritative DNS answer")
        try:
            ttl = int(fields[1])
        except ValueError:
            raise SystemExit("authoritative DNS answer TTL is invalid")
        if ttl < 0:
            raise SystemExit("authoritative DNS answer TTL is negative")
        answers.append((fqdn(fields[0]), fields[2].upper(), fields[3].upper(), fields[4].strip()))
    else:
        raise SystemExit(f"unexpected authoritative DNS output line: {stripped}")

if len(headers) != 1 or headers[0] != "NOERROR":
    raise SystemExit("authoritative DNS response status is not exactly NOERROR")
if got_answer_rows != 1 or len(flag_rows) != 1:
    raise SystemExit("authoritative DNS response has no unique flags/count row")
flags, query_count, answer_count, authority_count, additional_count = flag_rows[0]
if (
    flags != {"qr", "aa"}
    or query_count != 1
    or answer_count != len(answers)
    or authority_count != 0
    or additional_count != 1
    or additional_count != opt_sections
):
    raise SystemExit("authoritative DNS response is non-authoritative or has inconsistent counts")
if (
    opt_sections != 1
    or question_sections != 1
    or answer_sections != 1
    or len(edns_rows) != 1
    or edns_rows[0][0] != 0
    or edns_rows[0][1] != ""
    or not 512 <= edns_rows[0][2] <= 65535
    or ecs_rows != ["0.0.0.0/0/0"]
):
    raise SystemExit("authoritative DNS OPT/EDNS/ECS metadata is not the exact unmapped IPv4 scope")
if questions != [(expected_name, "IN", expected_type)]:
    raise SystemExit(f"authoritative DNS question drifted: {questions}")
if not answers:
    raise SystemExit("authoritative DNS response contains no answer")

if expected_type == "SOA":
    soa = config.get("soa") or {}
    if len(answers) != 1:
        raise SystemExit("authoritative SOA response must contain exactly one answer")
    owner, answer_class, answer_type, rdata = answers[0]
    fields = rdata.split()
    if len(fields) != 7:
        raise SystemExit("authoritative SOA RDATA is malformed")
    try:
        serial, refresh, retry, expire, minimum = (int(value) for value in fields[2:])
    except ValueError:
        raise SystemExit("authoritative SOA numeric RDATA is malformed")
    if (
        owner != fqdn(soa.get("owner"))
        or owner != expected_name
        or answer_class != "IN"
        or answer_type != "SOA"
        or fqdn(fields[0]) != fqdn(soa.get("mname"))
        or fqdn(fields[1]) != fqdn(soa.get("rname"))
        or not 0 <= serial <= 4294967295
        or refresh != soa.get("refresh")
        or retry != soa.get("retry")
        or expire != soa.get("expire")
        or minimum != soa.get("minimum")
    ):
        raise SystemExit("authoritative SOA owner or RDATA is not bound to the expected zone config")
    raise SystemExit(0)

allowed = set()
for item in config.get("answers") or []:
    if not isinstance(item, dict):
        continue
    owner = fqdn(item.get("name"))
    record_type = str(item.get("type") or "").strip().upper()
    raw_value = str(item.get("value") or "").strip()
    if record_type in {"A", "AAAA"}:
        try:
            value = str(ipaddress.ip_address(raw_value))
        except ValueError:
            continue
    elif record_type == "CNAME":
        value = fqdn(raw_value)
    else:
        continue
    allowed.add((owner, record_type, value))

permitted_types = {"CNAME", expected_type}
observed_types = set()
observed = set()
for owner, answer_class, answer_type, raw_value in answers:
    if answer_class != "IN" or owner != expected_name or answer_type not in permitted_types:
        raise SystemExit("authoritative address answer owner, class, or type is invalid")
    if answer_type in {"A", "AAAA"}:
        try:
            value = str(ipaddress.ip_address(raw_value))
        except ValueError:
            raise SystemExit("authoritative address answer contains an invalid IP")
        if (answer_type == "A" and ipaddress.ip_address(value).version != 4) or (
            answer_type == "AAAA" and ipaddress.ip_address(value).version != 6
        ):
            raise SystemExit("authoritative address answer has the wrong IP family")
    else:
        value = fqdn(raw_value)
    record = (owner, answer_type, value)
    if record not in allowed or record in observed:
        raise SystemExit("authoritative DNS answer is absent from the pinned published allow-set")
    observed.add(record)
    observed_types.add(answer_type)
if "CNAME" in observed_types and len(observed_types) != 1:
    raise SystemExit("authoritative DNS response unexpectedly mixes CNAME and address answers")
PY
}

authoritative_dns_query_batch_with_retry() {
  local daemonset_name="$1"
  local pod_name="$2"
  local server="$3"
  local server_port="$4"
  local container_port="$5"
  local zone="$6"
  local hostnames="$7"
  local transport="$8"
  local expected_pod_uid="$9"
  local expected_pod_host_ip="${10}"
  local expected_pod_identity="${11}"
  local expected_node_name="${12}"
  local expected_node_uid="${13}"
  local expected_node_identity="${14}"
  local expected_revision="${15}"
  local expected_daemonset_uid="${16}"
  local expected_daemonset_identity="${17}"
  local attempts="${FUGUE_PUBLIC_DATA_PLANE_DNS_QUERY_ATTEMPTS:-6}"
  local delay_seconds="${FUGUE_PUBLIC_DATA_PLANE_DNS_QUERY_RETRY_DELAY_SECONDS:-2}"
  local timeout_seconds="${FUGUE_PUBLIC_DATA_PLANE_DNS_QUERY_TIMEOUT_SECONDS:-3}"
  local attempt=1
  local publication_snapshot=""
  local publication_after=""
  local hostname=""
  local work_dir=""
  local output_file=""
  local failed="false"
  local dig_args=()
  local label="${daemonset_name} pod=${pod_name} server=${server}:${server_port} transport=${transport}"

  [[ "${attempts}" =~ ^[1-9][0-9]*$ ]] || fail "FUGUE_PUBLIC_DATA_PLANE_DNS_QUERY_ATTEMPTS must be a positive integer"
  [[ "${delay_seconds}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "FUGUE_PUBLIC_DATA_PLANE_DNS_QUERY_RETRY_DELAY_SECONDS must be a non-negative number"
  [[ "${timeout_seconds}" =~ ^[1-9][0-9]*$ ]] || fail "FUGUE_PUBLIC_DATA_PLANE_DNS_QUERY_TIMEOUT_SECONDS must be a positive integer"
  [[ "${server_port}" =~ ^[1-9][0-9]*$ && "${server_port}" -le 65535 ]] || fail "authoritative DNS query hostPort is invalid"
  [[ "${container_port}" =~ ^[1-9][0-9]*$ && "${container_port}" -le 65535 ]] || fail "authoritative DNS query containerPort is invalid"
  [[ -n "${expected_pod_uid}" && -n "${expected_pod_host_ip}" && -n "${expected_pod_identity}" && -n "${expected_node_name}" && -n "${expected_node_uid}" && -n "${expected_node_identity}" && -n "${expected_revision}" && -n "${expected_daemonset_uid}" && -n "${expected_daemonset_identity}" ]] ||
    fail "authoritative DNS query target identity is incomplete"
  dig_args=("@${server}" -p "${server_port}" "+time=${timeout_seconds}" "+tries=1" "+norecurse" "+subnet=0.0.0.0/0" "+noall" "+comments" "+question" "+answer")
  case "${transport}" in
    udp) ;;
    tcp) dig_args+=(+tcp) ;;
    *) fail "authoritative DNS transport must be udp or tcp" ;;
  esac

  work_dir="$(mktemp -d "${TMPDIR:-/tmp}/fugue-authoritative-query.XXXXXX")" || return 1
  while (( attempt <= attempts )); do
    failed="false"
    publication_snapshot="$(authoritative_dns_publication_snapshot_for_pod \
      "${pod_name}" "${zone}" "${hostnames}" "${server}" "${server_port}" "${container_port}" "${transport}" \
      "${expected_pod_uid}" "${expected_pod_host_ip}" "${expected_pod_identity}" \
      "${expected_node_name}" "${expected_node_uid}" "${expected_node_identity}" "${expected_revision}" "${daemonset_name}" \
      "${expected_daemonset_uid}" "${expected_daemonset_identity}")" || failed="true"
    output_file="${work_dir}/soa-${attempt}.txt"
    if [[ "${failed}" == "false" ]]; then
      if ! dig "${dig_args[@]}" "${zone}" SOA >"${output_file}" 2>&1; then
        failed="true"
      elif ! validate_authoritative_dns_response "${output_file}" "${publication_snapshot}" "${zone}" SOA; then
        failed="true"
      fi
    fi
    if [[ "${failed}" == "false" ]]; then
      while IFS= read -r hostname; do
        hostname="$(trim_field "${hostname}")"
        [[ -n "${hostname}" ]] || continue
        output_file="${work_dir}/a-${attempt}-$(printf '%s' "${hostname}" | tr -c '[:alnum:].-' '_').txt"
        if ! dig "${dig_args[@]}" "${hostname}" A >"${output_file}" 2>&1 ||
          ! validate_authoritative_dns_response "${output_file}" "${publication_snapshot}" "${hostname}" A; then
          failed="true"
          break
        fi
      done <<<"${hostnames}"
    fi
    if [[ "${failed}" == "false" ]]; then
      publication_after="$(authoritative_dns_publication_snapshot_for_pod \
        "${pod_name}" "${zone}" "${hostnames}" "${server}" "${server_port}" "${container_port}" "${transport}" \
        "${expected_pod_uid}" "${expected_pod_host_ip}" "${expected_pod_identity}" \
        "${expected_node_name}" "${expected_node_uid}" "${expected_node_identity}" "${expected_revision}" "${daemonset_name}" \
        "${expected_daemonset_uid}" "${expected_daemonset_identity}")" || failed="true"
    fi
    if [[ "${failed}" == "false" && "${publication_after}" == "${publication_snapshot}" ]]; then
      rm -rf "${work_dir}"
      return 0
    fi
    if [[ "${failed}" == "false" && "${publication_after}" != "${publication_snapshot}" ]]; then
      error "authoritative DNS canonical publication changed during query batch: ${label}"
    fi
    if (( attempt == attempts )); then
      error "authoritative DNS query batch failed after ${attempts} attempt(s): ${label}"
      [[ -z "${output_file}" || ! -f "${output_file}" ]] || sed -n '1,12p' "${output_file}" >&2 || true
      rm -rf "${work_dir}"
      return 1
    fi
    log "authoritative DNS query batch attempt ${attempt}/${attempts} failed; retrying in ${delay_seconds}s: ${label}"
    sleep "${delay_seconds}"
    attempt=$((attempt + 1))
  done
}

check_authoritative_dns_on_nodes() {
  local daemonset_name="$1"
  local zone
  local targets
  local hostnames
  local external_ip
  local udp_host_port
  local udp_container_port
  local tcp_host_port
  local tcp_container_port
  local server_port
  local container_port
  local pod_name
  local pod_uid
  local pod_host_ip
  local pod_identity
  local node_name
  local node_uid
  local node_identity
  local revision
  local daemonset_uid
  local daemonset_identity
  local hostname
  local checked_nodes=0
  local checked_hosts=0
  local transport
  local transport_label

  if ! zone="$(dns_zone_for_daemonset "${daemonset_name}")"; then
    error "could not read DNS zone from ${daemonset_name}"
    return 1
  fi
  zone="$(trim_field "${zone}")"
  if [[ -z "${zone}" ]]; then
    error "DNS daemonset ${daemonset_name} has no FUGUE_DNS_ZONE value"
    return 1
  fi
  if ! targets="$(authoritative_dns_targets_for_daemonset "${daemonset_name}")"; then
    error "could not read exact Pod/node targets for DNS daemonset ${daemonset_name}"
    return 1
  fi
  if [[ -z "$(trim_field "${targets}")" ]]; then
    error "DNS daemonset ${daemonset_name} has no Pod/node targets to validate"
    return 1
  fi
  if ! hostnames="$(authoritative_dns_hostnames)"; then
    error "could not derive representative hostnames for authoritative DNS validation"
    return 1
  fi
  if [[ -z "$(trim_field "${hostnames}")" ]]; then
    error "DNS authoritative validation requires at least one representative hostname"
    return 1
  fi
  while IFS=$'\t' read -r external_ip udp_host_port udp_container_port tcp_host_port tcp_container_port pod_name pod_uid pod_host_ip pod_identity node_name node_uid node_identity revision daemonset_uid daemonset_identity; do
    external_ip="$(trim_field "${external_ip}")"
    udp_host_port="$(trim_field "${udp_host_port}")"
    udp_container_port="$(trim_field "${udp_container_port}")"
    tcp_host_port="$(trim_field "${tcp_host_port}")"
    tcp_container_port="$(trim_field "${tcp_container_port}")"
    pod_name="$(trim_field "${pod_name}")"
    pod_uid="$(trim_field "${pod_uid}")"
    pod_host_ip="$(trim_field "${pod_host_ip}")"
    pod_identity="$(trim_field "${pod_identity}")"
    node_name="$(trim_field "${node_name}")"
    node_uid="$(trim_field "${node_uid}")"
    node_identity="$(trim_field "${node_identity}")"
    revision="$(trim_field "${revision}")"
    daemonset_uid="$(trim_field "${daemonset_uid}")"
    daemonset_identity="$(trim_field "${daemonset_identity}")"
    if [[ -z "${external_ip}" || -z "${udp_host_port}" || -z "${udp_container_port}" || -z "${tcp_host_port}" || -z "${tcp_container_port}" || -z "${pod_name}" || -z "${pod_uid}" || -z "${pod_host_ip}" || -z "${pod_identity}" || -z "${node_name}" || -z "${node_uid}" || -z "${node_identity}" || -z "${revision}" || -z "${daemonset_uid}" || -z "${daemonset_identity}" ]]; then
      error "DNS daemonset ${daemonset_name} returned an incomplete exact Pod target identity"
      return 1
    fi
    checked_nodes=$((checked_nodes + 1))
    for transport in udp tcp; do
      if [[ "${transport}" == "udp" ]]; then
        server_port="${udp_host_port}"
        container_port="${udp_container_port}"
      else
        server_port="${tcp_host_port}"
        container_port="${tcp_container_port}"
      fi
      transport_label="$(printf '%s' "${transport}" | tr '[:lower:]' '[:upper:]')"
      log "checking authoritative DNS ${transport_label} published answers server=${external_ip}:${server_port} pod=${pod_name} node=${node_name} podHostIP=${pod_host_ip} zone=${zone}"
      if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" != "true" ]]; then
        authoritative_dns_query_batch_with_retry \
          "${daemonset_name}" "${pod_name}" "${external_ip}" "${server_port}" "${container_port}" \
          "${zone}" "${hostnames}" "${transport}" "${pod_uid}" "${pod_host_ip}" "${pod_identity}" \
          "${node_name}" "${node_uid}" "${node_identity}" "${revision}" "${daemonset_uid}" "${daemonset_identity}" || return $?
      fi
      checked_hosts=0
      while IFS= read -r hostname; do
        hostname="$(trim_field "${hostname}")"
        [[ -n "${hostname}" ]] || continue
        checked_hosts=$((checked_hosts + 1))
        log "validated authoritative DNS ${transport_label} A/CNAME server=${external_ip}:${server_port} hostname=${hostname} against pinned publication"
      done <<<"${hostnames}"
      if (( checked_hosts == 0 )); then
        error "DNS authoritative validation requires at least one representative hostname"
        return 1
      fi
    done
  done <<<"${targets}"
  if (( checked_nodes == 0 )); then
    error "DNS daemonset ${daemonset_name} has no node IPs to validate"
    return 1
  fi
}

smoke_curl_with_retry() {
  local label="$1"
  shift
  local attempts="${FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS:-18}"
  local delay_seconds="${FUGUE_PUBLIC_DATA_PLANE_SMOKE_RETRY_DELAY_SECONDS:-5}"
  local attempt=1

  [[ "${attempts}" =~ ^[1-9][0-9]*$ ]] || fail "FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS must be a positive integer"
  [[ "${delay_seconds}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "FUGUE_PUBLIC_DATA_PLANE_SMOKE_RETRY_DELAY_SECONDS must be a non-negative number"
  while (( attempt <= attempts )); do
    if curl "$@"; then
      return 0
    fi
    if (( attempt == attempts )); then
      log "smoke failed after ${attempts} attempt(s): ${label}"
      return 1
    fi
    log "smoke attempt ${attempt}/${attempts} failed; retrying in ${delay_seconds}s: ${label}"
    sleep "${delay_seconds}"
    attempt=$((attempt + 1))
  done
}

check_worker_https_smoke() {
  local daemonset_name="$1"
  local port="$2"
  local host
  local path
  local host_ip

  while IFS=$'\t' read -r host path; do
    host="$(trim_field "${host}")"
    [[ -n "${host}" ]] || continue
    path="${path:-/}"
    while IFS= read -r host_ip; do
      host_ip="$(trim_field "${host_ip}")"
      [[ -n "${host_ip}" ]] || continue
      log "checking inactive worker HTTPS smoke ${host_ip}:${port} host=${host} path=${path}"
      if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
        continue
      fi
      smoke_curl_with_retry "inactive worker ${host_ip}:${port} host=${host} path=${path}" \
        -fsS --max-time "${FUGUE_PUBLIC_DATA_PLANE_SMOKE_TIMEOUT_SECONDS:-10}" \
        --resolve "${host}:${port}:${host_ip}" \
        "https://${host}:${port}${path}" >/dev/null || return $?
    done < <(node_ips_for_daemonset "${daemonset_name}")
  done < <(worker_smoke_targets)
}

container_patch_for_front() {
  local daemonset_name="$1"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json | python3 -c '
import json
import os
import sys

doc = json.load(sys.stdin)
release_id = os.environ["FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID"]
edge_repo = os.environ.get("FUGUE_EDGE_IMAGE_REPOSITORY", "").strip()
edge_tag = os.environ.get("FUGUE_EDGE_IMAGE_TAG", "").strip()
if not edge_repo or not edge_tag:
    raise SystemExit("FUGUE_EDGE_IMAGE_REPOSITORY and FUGUE_EDGE_IMAGE_TAG are required for front-ondelete")

containers = []
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    if container.get("name") != "edge-front":
        continue
    containers.append({
        "name": "edge-front",
        "image": f"{edge_repo}:{edge_tag}",
    })
if not containers:
    raise SystemExit("front daemonset has no edge-front container")
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
                    "fugue.io/public-data-plane-release-mode": "node-local-blue-green-front",
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

patch_front_template() {
  local daemonset_name="$1"
  local patch

  patch="$(container_patch_for_front "${daemonset_name}")"
  log "patching front ${daemonset_name} template"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    printf '%s\n' "${patch}"
    return 0
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" patch "ds/${daemonset_name}" --type=strategic -p "${patch}" >/dev/null
  wait_daemonset_observed "${daemonset_name}"
}

container_patch_for_dns() {
  local daemonset_name="$1"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json | python3 -c '
import json
import os
import sys

doc = json.load(sys.stdin)
release_id = os.environ["FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID"]
dns_resources = json.loads(os.environ["FUGUE_DNS_RESOURCES_JSON"])
edge_repo = os.environ.get("FUGUE_EDGE_IMAGE_REPOSITORY", "").strip()
edge_tag = os.environ.get("FUGUE_EDGE_IMAGE_TAG", "").strip()
if not edge_repo or not edge_tag:
    raise SystemExit("FUGUE_EDGE_IMAGE_REPOSITORY and FUGUE_EDGE_IMAGE_TAG are required for dns-ondelete")

containers = []
for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
    command = container.get("command") or []
    env_names = {entry.get("name") for entry in container.get("env") or []}
    if command != ["/usr/local/bin/fugue-dns"] or "FUGUE_DNS_ZONE" not in env_names:
        continue
    containers.append({
        "name": container.get("name"),
        "image": f"{edge_repo}:{edge_tag}",
        "resources": dns_resources,
    })
if len(containers) != 1 or not containers[0].get("name"):
    raise SystemExit("dns daemonset must have exactly one semantic fugue-dns container")
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
                    "fugue.io/public-data-plane-release-mode": "dns-ondelete",
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

patch_dns_template() {
  local daemonset_name="$1"
  local patch

  if ! patch="$(container_patch_for_dns "${daemonset_name}")"; then
    return 1
  fi
  log "patching dns ${daemonset_name} template"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    printf '%s\n' "${patch}"
    return 0
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" patch "ds/${daemonset_name}" --type=strategic -p "${patch}" >/dev/null || return $?
  wait_daemonset_observed "${daemonset_name}" || return $?
}

dns_manifest_snapshot_query() {
  local snapshot_file="$1"
  local action="$2"
  local daemonset_name="${3:-}"

  python3 -c '
import json
import re
import sys

path, action, requested_name = sys.argv[1:4]
try:
    with open(path, encoding="utf-8") as handle:
        doc = json.load(handle)
except Exception as exc:
    raise SystemExit(f"could not read DNS manifest snapshot {path}: {exc}")

if doc.get("apiVersion") != "fugue.io/v1alpha1":
    raise SystemExit("DNS manifest snapshot apiVersion must be fugue.io/v1alpha1")
if doc.get("kind") != "DNSManifestOnDeleteSnapshot":
    raise SystemExit("DNS manifest snapshot kind must be DNSManifestOnDeleteSnapshot")
namespace = doc.get("namespace")
if not isinstance(namespace, str) or not namespace.strip():
    raise SystemExit("DNS manifest snapshot namespace is required")
items = doc.get("daemonSets")
if not isinstance(items, list) or not items:
    raise SystemExit("DNS manifest snapshot must contain at least one daemonSet")

by_name = {}
for item in items:
    if not isinstance(item, dict):
        raise SystemExit("DNS manifest snapshot daemonSet entry must be an object")
    name = item.get("name")
    if not isinstance(name, str) or not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", name):
        raise SystemExit(f"invalid DNS manifest snapshot daemonSet name: {name!r}")
    if name in by_name:
        raise SystemExit(f"duplicate DNS manifest snapshot daemonSet: {name}")
    daemonset_uid = item.get("uid")
    if not isinstance(daemonset_uid, str) or not daemonset_uid:
        raise SystemExit(f"DNS manifest snapshot daemonSet {name} has no UID")
    update_strategy = item.get("updateStrategy")
    desired_spec = item.get("desiredSpec")
    serving_revision = item.get("servingRevision")
    serving_controller_revision = item.get("servingControllerRevision")
    desired_template = item.get("desiredTemplate")
    serving_template = item.get("servingTemplate")
    pods = item.get("pods")
    if not isinstance(update_strategy, dict):
        raise SystemExit(f"DNS manifest snapshot daemonSet {name} has no updateStrategy object")
    if not isinstance(desired_spec, dict):
        raise SystemExit(f"DNS manifest snapshot daemonSet {name} has no desired spec object")
    if not isinstance(serving_revision, str) or not serving_revision:
        raise SystemExit(f"DNS manifest snapshot daemonSet {name} has no servingRevision")
    if not isinstance(serving_controller_revision, dict):
        raise SystemExit(f"DNS manifest snapshot daemonSet {name} has no serving ControllerRevision identity")
    controller_revision_name = serving_controller_revision.get("name")
    controller_revision_uid = serving_controller_revision.get("uid")
    controller_revision_number = serving_controller_revision.get("revision")
    if (
        not isinstance(controller_revision_name, str)
        or controller_revision_name != f"{name}-{serving_revision}"
        or not isinstance(controller_revision_uid, str)
        or not controller_revision_uid
        or isinstance(controller_revision_number, bool)
        or not isinstance(controller_revision_number, int)
        or controller_revision_number < 0
    ):
        raise SystemExit(
            f"DNS manifest snapshot daemonSet {name} has an invalid serving ControllerRevision identity"
        )
    if not isinstance(desired_template, dict) or not isinstance(desired_template.get("spec"), dict):
        raise SystemExit(f"DNS manifest snapshot daemonSet {name} has no desired pod template")
    if not isinstance(serving_template, dict) or not isinstance(serving_template.get("spec"), dict):
        raise SystemExit(f"DNS manifest snapshot daemonSet {name} has no serving pod template")
    if desired_spec.get("template") != desired_template:
        raise SystemExit(f"DNS manifest snapshot daemonSet {name} desired spec/template is inconsistent")
    if (desired_spec.get("updateStrategy") or {"type": "RollingUpdate"}) != update_strategy:
        raise SystemExit(f"DNS manifest snapshot daemonSet {name} desired spec/updateStrategy is inconsistent")
    strategy_type = update_strategy.get("type") or "RollingUpdate"
    if strategy_type != "OnDelete":
        raise SystemExit(
            f"DNS manifest snapshot daemonSet {name} uses {strategy_type}; "
            "the exact DNS transaction supports only an existing OnDelete strategy"
        )
    if not isinstance(pods, list) or not pods:
        raise SystemExit(f"DNS manifest snapshot daemonSet {name} has no pods")
    seen_pod_names = set()
    seen_pod_uids = set()
    seen_node_names = set()
    for pod in pods:
        if not isinstance(pod, dict):
            raise SystemExit(f"DNS manifest snapshot daemonSet {name} has an invalid pod entry")
        pod_name = pod.get("name")
        pod_uid = pod.get("uid")
        node_name = pod.get("nodeName")
        pod_revision = pod.get("revision")
        restart_counts = pod.get("restartCounts")
        if (
            not isinstance(pod_name, str)
            or not pod_name
            or not isinstance(pod_uid, str)
            or not pod_uid
            or not isinstance(node_name, str)
            or not node_name
            or pod_revision != serving_revision
        ):
            raise SystemExit(
                f"DNS manifest snapshot daemonSet {name} has a pod without matching name/UID/node/revision"
            )
        if not isinstance(restart_counts, list) or not restart_counts:
            raise SystemExit(f"DNS manifest snapshot daemonSet {name} pod {pod_name} has no restart baseline")
        seen_containers = set()
        for restart in restart_counts:
            if not isinstance(restart, dict):
                raise SystemExit(f"DNS manifest snapshot daemonSet {name} pod {pod_name} has an invalid restart entry")
            container_name = restart.get("name")
            restart_count = restart.get("restartCount")
            if (
                not isinstance(container_name, str)
                or not container_name
                or container_name in seen_containers
                or isinstance(restart_count, bool)
                or not isinstance(restart_count, int)
                or restart_count != 0
            ):
                raise SystemExit(
                    f"DNS manifest snapshot daemonSet {name} pod {pod_name} has a duplicate, invalid, or nonzero restart baseline"
                )
            seen_containers.add(container_name)
        if pod_name in seen_pod_names or pod_uid in seen_pod_uids or node_name in seen_node_names:
            raise SystemExit(f"DNS manifest snapshot daemonSet {name} has duplicate pod identity")
        seen_pod_names.add(pod_name)
        seen_pod_uids.add(pod_uid)
        seen_node_names.add(node_name)
    by_name[name] = item

if action == "validate":
    raise SystemExit(0)
if action == "namespace":
    print(namespace)
    raise SystemExit(0)
if action == "names":
    for name in sorted(by_name):
        print(name)
    raise SystemExit(0)
if requested_name not in by_name:
    raise SystemExit(f"DNS manifest snapshot has no daemonSet {requested_name}")
item = by_name[requested_name]
if action == "uids":
    for pod in sorted(item["pods"], key=lambda value: value["name"]):
        print(pod["uid"])
elif action == "uid":
    print(item["uid"])
elif action == "nodes":
    for pod in sorted(item["pods"], key=lambda value: value["nodeName"]):
        print(pod["nodeName"])
elif action == "serving-revision":
    print(item["servingRevision"])
elif action == "pod-cohort":
    records = []
    for pod in sorted(item["pods"], key=lambda value: value["nodeName"]):
        records.append({
            "name": pod["name"],
            "nodeName": pod["nodeName"],
            "restartCounts": sorted(pod["restartCounts"], key=lambda value: value["name"]),
            "revision": pod["revision"],
            "uid": pod["uid"],
        })
    print(json.dumps(records, separators=(",", ":"), sort_keys=True))
elif action == "restore-serving-ondelete-patch":
    serving_spec = dict(item["desiredSpec"])
    serving_spec["updateStrategy"] = {"type": "OnDelete"}
    serving_spec["template"] = item["servingTemplate"]
    patch = [
        {"op": "test", "path": "/metadata/uid", "value": item["uid"]},
        {"op": "replace", "path": "/spec", "value": serving_spec},
    ]
    print(json.dumps(patch, separators=(",", ":")))
elif action == "restore-desired-patch":
    patch = [
        {"op": "test", "path": "/metadata/uid", "value": item["uid"]},
        {"op": "replace", "path": "/spec", "value": item["desiredSpec"]},
    ]
    print(json.dumps(patch, separators=(",", ":")))
else:
    raise SystemExit(f"unsupported DNS manifest snapshot query: {action}")
' "${snapshot_file}" "${action}" "${daemonset_name}"
}

capture_dns_manifest_snapshot_live_pod_cohort() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  local expected_revision="$3"

  kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -o json | python3 -c '
import json
import sys

snapshot_path, name, expected_revision = sys.argv[1:4]
with open(snapshot_path, encoding="utf-8") as handle:
    snapshot = json.load(handle)
entry = next((item for item in snapshot.get("daemonSets") or [] if item.get("name") == name), None)
if entry is None:
    raise SystemExit(f"snapshot has no DNS daemonSet {name}")
if expected_revision != entry.get("servingRevision"):
    raise SystemExit(f"DNS daemonSet {name} live cohort capture received the wrong serving revision")
expected_uid = entry.get("uid")
selector = ((entry.get("desiredSpec") or {}).get("selector") or {}).get("matchLabels")
if not isinstance(selector, dict) or not selector or any(not str(key) or not str(value) for key, value in selector.items()):
    raise SystemExit(f"DNS daemonSet {name} snapshot has no exact matchLabels selector")

doc = json.load(sys.stdin)
pods = []
for pod in doc.get("items") or []:
    labels = (pod.get("metadata") or {}).get("labels") or {}
    if all(labels.get(key) == value for key, value in selector.items()):
        pods.append(pod)

records = []
seen_names = set()
seen_uids = set()
seen_nodes = set()
for pod in pods:
    metadata = pod.get("metadata") or {}
    status = pod.get("status") or {}
    pod_name = metadata.get("name") or ""
    display_name = pod_name or "<unknown>"
    pod_uid = metadata.get("uid") or ""
    node_name = (pod.get("spec") or {}).get("nodeName") or ""
    pod_revision = (metadata.get("labels") or {}).get("controller-revision-hash") or ""
    controllers = [
        owner
        for owner in metadata.get("ownerReferences") or []
        if isinstance(owner, dict) and owner.get("controller") is True
    ]
    owner_matches = (
        len(controllers) == 1
        and controllers[0].get("apiVersion") == "apps/v1"
        and controllers[0].get("kind") == "DaemonSet"
        and controllers[0].get("name") == name
        and controllers[0].get("uid") == expected_uid
    )
    ready = any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in status.get("conditions") or []
    )
    restart_counts = []
    seen_containers = set()
    for container in status.get("containerStatuses") or []:
        container_name = container.get("name")
        restart_count = container.get("restartCount")
        if (
            not isinstance(container_name, str)
            or not container_name
            or container_name in seen_containers
            or isinstance(restart_count, bool)
            or not isinstance(restart_count, int)
            or restart_count < 0
        ):
            raise SystemExit(
                f"DNS daemonSet {name} pod {display_name} has invalid live restart state"
            )
        seen_containers.add(container_name)
        restart_counts.append({"name": container_name, "restartCount": restart_count})
    restart_counts.sort(key=lambda value: value["name"])
    if (
        not pod_name
        or not pod_uid
        or not node_name
        or status.get("phase") != "Running"
        or metadata.get("deletionTimestamp")
        or not owner_matches
        or not ready
        or pod_revision != expected_revision
        or not restart_counts
        or pod_name in seen_names
        or pod_uid in seen_uids
        or node_name in seen_nodes
    ):
        raise SystemExit(
            f"DNS daemonSet {name} live snapshot cohort contains an invalid, duplicated, unowned, "
            f"unready, or wrong-revision pod {display_name}"
        )
    seen_names.add(pod_name)
    seen_uids.add(pod_uid)
    seen_nodes.add(node_name)
    records.append({
        "name": pod_name,
        "nodeName": node_name,
        "restartCounts": restart_counts,
        "revision": pod_revision,
        "uid": pod_uid,
    })
records.sort(key=lambda value: value["nodeName"])
print(json.dumps(records, separators=(",", ":"), sort_keys=True))
' "${snapshot_file}" "${daemonset_name}" "${expected_revision}"
}

dns_manifest_serving_controllerrevision_matches_snapshot() {
  local snapshot_file="$1"
  local daemonset_name="$2"

  kubectl_cmd -n "${FUGUE_NAMESPACE}" get controllerrevisions -o json | python3 -c '
import copy
import json
import sys

snapshot_path, name = sys.argv[1:3]
with open(snapshot_path, encoding="utf-8") as handle:
    snapshot = json.load(handle)
entry = next((item for item in snapshot.get("daemonSets") or [] if item.get("name") == name), None)
if entry is None:
    raise SystemExit(f"snapshot has no DNS daemonSet {name}")
expected = entry.get("servingControllerRevision") or {}
expected_name = expected.get("name")
expected_uid = expected.get("uid")
expected_number = expected.get("revision")
expected_hash = entry.get("servingRevision")
daemonset_uid = entry.get("uid")
namespace = snapshot.get("namespace")

doc = json.load(sys.stdin)
matches = [
    item
    for item in doc.get("items") or []
    if (item.get("metadata") or {}).get("name") == expected_name
]
if len(matches) != 1:
    raise SystemExit(
        f"DNS daemonSet {name} serving ControllerRevision {expected_name} has {len(matches)} live objects"
    )
revision = matches[0]
metadata = revision.get("metadata") or {}
controllers = [
    owner
    for owner in metadata.get("ownerReferences") or []
    if isinstance(owner, dict) and owner.get("controller") is True
]
if (
    str(revision.get("apiVersion") or "apps/v1") != "apps/v1"
    or str(revision.get("kind") or "ControllerRevision") != "ControllerRevision"
    or str(metadata.get("namespace") or namespace) != namespace
    or metadata.get("uid") != expected_uid
    or (metadata.get("labels") or {}).get("controller-revision-hash") != expected_hash
    or revision.get("revision") != expected_number
    or len(controllers) != 1
    or controllers[0].get("apiVersion") != "apps/v1"
    or controllers[0].get("kind") != "DaemonSet"
    or controllers[0].get("name") != name
    or controllers[0].get("uid") != daemonset_uid
):
    raise SystemExit(f"DNS daemonSet {name} serving ControllerRevision identity or owner drifted")
template = copy.deepcopy((((revision.get("data") or {}).get("spec") or {}).get("template")))
if not isinstance(template, dict) or template.pop("$patch", None) != "replace":
    raise SystemExit(f"DNS daemonSet {name} serving ControllerRevision lost its replacement template")
if template != entry.get("servingTemplate"):
    raise SystemExit(f"DNS daemonSet {name} serving ControllerRevision template drifted")
' "${snapshot_file}" "${daemonset_name}"
}

capture_dns_manifest_snapshot() {
  local snapshot_file="$1"
  local snapshot_dir
  local work_dir
  local output_file
  local daemonset_name
  local snapshot_names_text
  local live_pod_cohort
  local snapshot_pod_cohort
  local snapshot_revision

  snapshot_file="$(trim_field "${snapshot_file}")"
  [[ -n "${snapshot_file}" ]] || {
    error "DNS manifest snapshot path is required"
    return 1
  }
  snapshot_dir="$(dirname "${snapshot_file}")"
  mkdir -p "${snapshot_dir}" || return $?
  work_dir="$(mktemp -d "${snapshot_dir}/.fugue-dns-manifest-snapshot.XXXXXX")" || return $?
  output_file="${work_dir}/snapshot.json"

  if ! kubectl_cmd -n "${FUGUE_NAMESPACE}" get ds -o json >"${work_dir}/daemonsets.json"; then
    rm -rf "${work_dir}"
    return 1
  fi
  if ! kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -o json >"${work_dir}/pods.json"; then
    rm -rf "${work_dir}"
    return 1
  fi
  if ! kubectl_cmd -n "${FUGUE_NAMESPACE}" get controllerrevisions -o json >"${work_dir}/controllerrevisions.json"; then
    rm -rf "${work_dir}"
    return 1
  fi
  : "${FUGUE_RELEASE_INSTANCE:?FUGUE_RELEASE_INSTANCE is required to capture a DNS manifest snapshot}"
  if ! python3 - "${FUGUE_NAMESPACE}" "${FUGUE_RELEASE_INSTANCE}" "${work_dir}/daemonsets.json" "${work_dir}/pods.json" "${work_dir}/controllerrevisions.json" >"${output_file}" <<'PY'
import copy
import datetime
import json
import sys

namespace, release_instance, daemonsets_path, pods_path, revisions_path = sys.argv[1:6]
with open(daemonsets_path, encoding="utf-8") as handle:
    daemonsets_doc = json.load(handle)
with open(pods_path, encoding="utf-8") as handle:
    pods_doc = json.load(handle)
with open(revisions_path, encoding="utf-8") as handle:
    revisions_doc = json.load(handle)

def is_dns_daemonset(item):
    labels = item.get("metadata", {}).get("labels") or {}
    component = (labels.get("app.kubernetes.io/component") or "").strip()
    return (
        labels.get("fugue.io/rollout-subsystem") == "public-data-plane"
        and labels.get("app.kubernetes.io/instance") == release_instance
        and (component == "dns" or component.startswith("dns-"))
    )

def pod_ready(pod):
    status = pod.get("status") or {}
    if status.get("phase") != "Running" or pod.get("metadata", {}).get("deletionTimestamp"):
        return False
    return any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in status.get("conditions") or []
    )

daemonsets = []
all_pods = pods_doc.get("items") or []
all_revisions = revisions_doc.get("items") or []
for item in sorted(
    (value for value in daemonsets_doc.get("items") or [] if is_dns_daemonset(value)),
    key=lambda value: value.get("metadata", {}).get("name", ""),
):
    metadata = item.get("metadata") or {}
    spec = item.get("spec") or {}
    status = item.get("status") or {}
    name = metadata.get("name") or ""
    uid = metadata.get("uid") or ""
    selector = (spec.get("selector") or {}).get("matchLabels") or {}
    desired_spec = copy.deepcopy(spec)
    desired_template = spec.get("template")
    update_strategy = spec.get("updateStrategy") or {"type": "RollingUpdate"}
    if not name or not uid or not selector or not isinstance(desired_template, dict):
        raise SystemExit(f"DNS daemonSet {name or '<unknown>'} is missing UID, selector, or template")
    generation = metadata.get("generation")
    observed = status.get("observedGeneration")
    desired = status.get("desiredNumberScheduled") or 0
    ready = status.get("numberReady") or 0
    available = status.get("numberAvailable") or 0
    unavailable = status.get("numberUnavailable") or 0
    misscheduled = status.get("numberMisscheduled") or 0
    if (
        generation != observed
        or desired <= 0
        or ready != desired
        or available != desired
        or unavailable != 0
        or misscheduled != 0
    ):
        raise SystemExit(
            f"DNS daemonSet {name} is not stable: generation={generation} observed={observed} "
            f"desired={desired} ready={ready} available={available} "
            f"unavailable={unavailable} misscheduled={misscheduled}"
        )
    matching_pods = []
    for pod in all_pods:
        labels = pod.get("metadata", {}).get("labels") or {}
        if all(labels.get(key) == value for key, value in selector.items()):
            matching_pods.append(pod)
    if len(matching_pods) != desired:
        raise SystemExit(f"DNS daemonSet {name} has {len(matching_pods)} pods; expected {desired}")
    pod_entries = []
    serving_revisions = set()
    serving_nodes = set()
    for pod in sorted(matching_pods, key=lambda value: value.get("metadata", {}).get("name", "")):
        pod_metadata = pod.get("metadata") or {}
        pod_name = pod_metadata.get("name") or ""
        pod_uid = pod_metadata.get("uid") or ""
        node_name = (pod.get("spec") or {}).get("nodeName") or ""
        pod_labels = pod_metadata.get("labels") or {}
        pod_revision = pod_labels.get("controller-revision-hash") or ""
        container_statuses = (pod.get("status") or {}).get("containerStatuses") or []
        restart_counts = []
        seen_container_names = set()
        for container_status in container_statuses:
            container_name = container_status.get("name")
            restart_count = container_status.get("restartCount")
            if (
                not isinstance(container_name, str)
                or not container_name
                or container_name in seen_container_names
                or isinstance(restart_count, bool)
                or not isinstance(restart_count, int)
                or restart_count != 0
            ):
                raise SystemExit(
                    f"DNS daemonSet {name} pod {pod_name or '<unknown>'} has a duplicate, invalid, or nonzero restart baseline"
                )
            seen_container_names.add(container_name)
            restart_counts.append({"name": container_name, "restartCount": restart_count})
        restart_counts.sort(key=lambda value: value["name"])
        owner_matches = any(
            owner.get("apiVersion") == "apps/v1"
            and owner.get("kind") == "DaemonSet"
            and owner.get("name") == name
            and owner.get("uid") == uid
            and owner.get("controller") is True
            for owner in pod_metadata.get("ownerReferences") or []
        )
        if (
            not pod_name
            or not pod_uid
            or not node_name
            or not pod_ready(pod)
            or not owner_matches
            or not pod_revision
            or not restart_counts
        ):
            raise SystemExit(
                f"DNS daemonSet {name} has a pod that is not owned, scheduled, Ready, and revision-pinned"
            )
        if node_name in serving_nodes:
            raise SystemExit(f"DNS daemonSet {name} has more than one serving pod on node {node_name}")
        serving_nodes.add(node_name)
        serving_revisions.add(pod_revision)
        pod_entries.append({
            "name": pod_name,
            "uid": pod_uid,
            "nodeName": node_name,
            "revision": pod_revision,
            "restartCounts": restart_counts,
        })
    if len(serving_revisions) != 1:
        raise SystemExit(
            f"DNS daemonSet {name} has a mixed serving revision cohort: {sorted(serving_revisions)}"
        )
    serving_revision = next(iter(serving_revisions))
    matching_histories = []
    for history in all_revisions:
        history_metadata = history.get("metadata") or {}
        history_labels = history_metadata.get("labels") or {}
        owner_matches = any(
            owner.get("apiVersion") == "apps/v1"
            and owner.get("kind") == "DaemonSet"
            and owner.get("name") == name
            and owner.get("uid") == uid
            and owner.get("controller") is True
            for owner in history_metadata.get("ownerReferences") or []
        )
        if owner_matches and history_labels.get("controller-revision-hash") == serving_revision:
            matching_histories.append(history)
    if len(matching_histories) != 1:
        raise SystemExit(
            f"DNS daemonSet {name} serving revision {serving_revision} has "
            f"{len(matching_histories)} owned ControllerRevisions; expected 1"
        )
    serving_template = copy.deepcopy(
        ((matching_histories[0].get("data") or {}).get("spec") or {}).get("template")
    )
    if not isinstance(serving_template, dict) or not isinstance(serving_template.get("spec"), dict):
        raise SystemExit(
            f"DNS daemonSet {name} serving revision {serving_revision} has no pod template"
        )
    if serving_template.pop("$patch", None) != "replace":
        raise SystemExit(
            f"DNS daemonSet {name} serving revision {serving_revision} "
            "does not contain an exact replacement pod template"
        )
    serving_history_metadata = matching_histories[0].get("metadata") or {}
    serving_history_name = serving_history_metadata.get("name") or ""
    serving_history_uid = serving_history_metadata.get("uid") or ""
    serving_history_revision = matching_histories[0].get("revision")
    if (
        serving_history_name != f"{name}-{serving_revision}"
        or not serving_history_uid
        or isinstance(serving_history_revision, bool)
        or not isinstance(serving_history_revision, int)
        or serving_history_revision < 0
    ):
        raise SystemExit(
            f"DNS daemonSet {name} serving revision {serving_revision} has an invalid ControllerRevision identity"
        )
    strategy_type = update_strategy.get("type") or "RollingUpdate"
    if strategy_type != "OnDelete":
        raise SystemExit(
            f"DNS daemonSet {name} uses {strategy_type}; "
            "the exact DNS transaction supports only an existing OnDelete strategy"
        )
    daemonsets.append({
        "name": name,
        "uid": uid,
        "updateStrategy": update_strategy,
        "desiredSpec": desired_spec,
        "servingRevision": serving_revision,
        "servingControllerRevision": {
            "name": serving_history_name,
            "uid": serving_history_uid,
            "revision": serving_history_revision,
        },
        "desiredTemplate": desired_template,
        "servingTemplate": serving_template,
        "pods": pod_entries,
    })

if not daemonsets:
    raise SystemExit(f"no DNS public data-plane daemonSets found in namespace {namespace}")
snapshot = {
    "apiVersion": "fugue.io/v1alpha1",
    "kind": "DNSManifestOnDeleteSnapshot",
    "namespace": namespace,
    "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "daemonSets": daemonsets,
}
json.dump(snapshot, sys.stdout, separators=(",", ":"), sort_keys=True)
sys.stdout.write("\n")
PY
  then
    rm -rf "${work_dir}"
    return 1
  fi

  chmod 600 "${output_file}" || {
    rm -rf "${work_dir}"
    return 1
  }
  if ! dns_manifest_snapshot_query "${output_file}" validate; then
    rm -rf "${work_dir}"
    return 1
  fi
  snapshot_names_text="$(dns_manifest_snapshot_query "${output_file}" names)" || {
    rm -rf "${work_dir}"
    return 1
  }
  [[ -n "$(trim_field "${snapshot_names_text}")" ]] || {
    error "captured DNS manifest snapshot contains no daemonsets"
    rm -rf "${work_dir}"
    return 1
  }
  while IFS= read -r daemonset_name; do
    daemonset_name="$(trim_field "${daemonset_name}")"
    [[ -n "${daemonset_name}" ]] || continue
    snapshot_revision="$(dns_manifest_snapshot_query "${output_file}" serving-revision "${daemonset_name}")" || {
      rm -rf "${work_dir}"
      return 1
    }
    snapshot_pod_cohort="$(dns_manifest_snapshot_query "${output_file}" pod-cohort "${daemonset_name}")" || {
      rm -rf "${work_dir}"
      return 1
    }
    live_pod_cohort="$(capture_dns_manifest_snapshot_live_pod_cohort \
      "${output_file}" "${daemonset_name}" "${snapshot_revision}")" || {
      error "DNS daemonset ${daemonset_name} live Pod cohort became invalid while its pre-Helm snapshot was being captured"
      rm -rf "${work_dir}"
      return 1
    }
    if [[ "${live_pod_cohort}" != "${snapshot_pod_cohort}" ]]; then
      error "DNS daemonset ${daemonset_name} name/UID/node/revision/restart cohort changed while its pre-Helm snapshot was being captured"
      rm -rf "${work_dir}"
      return 1
    fi
    if ! dns_manifest_daemonset_matches_snapshot "${output_file}" "${daemonset_name}"; then
      error "DNS daemonset ${daemonset_name} changed while its pre-Helm snapshot was being captured"
      rm -rf "${work_dir}"
      return 1
    fi
    if ! dns_manifest_serving_controllerrevision_matches_snapshot "${output_file}" "${daemonset_name}"; then
      error "DNS daemonset ${daemonset_name} serving ControllerRevision changed while its pre-Helm snapshot was being captured"
      rm -rf "${work_dir}"
      return 1
    fi
  done <<<"${snapshot_names_text}"
  if ! mv "${output_file}" "${snapshot_file}"; then
    rm -rf "${work_dir}"
    return 1
  fi
  rm -rf "${work_dir}"
  log "captured pre-Helm DNS manifest snapshot at ${snapshot_file}"
}

dns_manifest_daemonset_state() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json | python3 -c '
import json
import sys

snapshot_path, name = sys.argv[1:3]
with open(snapshot_path, encoding="utf-8") as handle:
    snapshot = json.load(handle)
live = json.load(sys.stdin)
entry = next((item for item in snapshot.get("daemonSets") or [] if item.get("name") == name), None)
if entry is None:
    raise SystemExit(f"snapshot has no DNS daemonSet {name}")
if live.get("metadata", {}).get("uid") != entry.get("uid"):
    raise SystemExit(f"DNS daemonSet {name} UID changed after the pre-Helm snapshot")
spec = live.get("spec") or {}
desired_spec = entry.get("desiredSpec") or {}
template_changed = spec.get("template") != entry.get("desiredTemplate")
strategy_changed = (spec.get("updateStrategy") or {"type": "RollingUpdate"}) != entry.get("updateStrategy")
static_spec = dict(spec)
static_spec.pop("template", None)
static_spec.pop("updateStrategy", None)
desired_static_spec = dict(desired_spec)
desired_static_spec.pop("template", None)
desired_static_spec.pop("updateStrategy", None)
if static_spec != desired_static_spec:
    print("unsupported-spec-changed")
elif template_changed:
    print("template-changed")
elif strategy_changed:
    print("strategy-only")
else:
    print("unchanged")
' "${snapshot_file}" "${daemonset_name}"
}

dns_manifest_daemonset_matches_snapshot() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  [[ "$(dns_manifest_daemonset_state "${snapshot_file}" "${daemonset_name}")" == "unchanged" ]]
}

dns_manifest_daemonset_uid_matches_snapshot() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  local expected_uid
  local live_uid

  expected_uid="$(dns_manifest_snapshot_query "${snapshot_file}" uid "${daemonset_name}")" || return $?
  live_uid="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.metadata.uid}')" || return $?
  if [[ -z "${expected_uid}" || "${live_uid}" != "${expected_uid}" ]]; then
    error "DNS daemonset ${daemonset_name} UID changed from snapshot ${expected_uid:-<empty>} to ${live_uid:-<empty>}"
    return 1
  fi
}

dns_manifest_daemonset_serving_revision_matches_snapshot() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  local snapshot_revision

  snapshot_revision="$(dns_manifest_snapshot_query "${snapshot_file}" serving-revision "${daemonset_name}")" || return $?
  [[ -n "${snapshot_revision}" ]] || return 1
  dns_manifest_serving_controllerrevision_matches_snapshot "${snapshot_file}" "${daemonset_name}" || return $?
  verify_dns_manifest_daemonset_cohort "${snapshot_file}" "${daemonset_name}" "${snapshot_revision}"
}

dns_manifest_daemonset_target_identity() {
  local daemonset_name="$1"
  local daemonset_json

  daemonset_json="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json)" || return $?
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get controllerrevisions -o json | python3 -c '
import copy
import json
import sys

name, daemonset_text = sys.argv[1:3]
doc = json.loads(daemonset_text)
histories = json.load(sys.stdin)
metadata = doc.get("metadata") or {}
spec = doc.get("spec") or {}
uid = metadata.get("uid")
generation = metadata.get("generation")
template = spec.get("template")
strategy = spec.get("updateStrategy") or {"type": "RollingUpdate"}
if not isinstance(uid, str) or not uid:
    raise SystemExit("DNS daemonSet target identity has no UID")
if not isinstance(generation, int):
    raise SystemExit("DNS daemonSet target identity has no generation")
if not isinstance(template, dict) or not isinstance(strategy, dict):
    raise SystemExit("DNS daemonSet target identity has no template/updateStrategy")
matches = []
for history in histories.get("items") or []:
    history_metadata = history.get("metadata") or {}
    owner_matches = any(
        owner.get("apiVersion") == "apps/v1"
        and owner.get("kind") == "DaemonSet"
        and owner.get("name") == name
        and owner.get("uid") == uid
        and owner.get("controller") is True
        for owner in history_metadata.get("ownerReferences") or []
    )
    if not owner_matches:
        continue
    history_template = copy.deepcopy(((history.get("data") or {}).get("spec") or {}).get("template"))
    if not isinstance(history_template, dict):
        continue
    if history_template.pop("$patch", None) != "replace":
        continue
    if history_template != template:
        continue
    revision_number = history.get("revision")
    revision_hash = (history_metadata.get("labels") or {}).get("controller-revision-hash")
    history_uid = history_metadata.get("uid")
    if (
        isinstance(revision_number, bool)
        or not isinstance(revision_number, int)
        or revision_number < 0
        or not isinstance(revision_hash, str)
        or not revision_hash
        or not isinstance(history_uid, str)
        or not history_uid
    ):
        raise SystemExit(f"DNS daemonSet {name} has an invalid matching ControllerRevision")
    matches.append((revision_number, revision_hash, history_uid))
if len(matches) != 1:
    raise SystemExit(
        f"DNS daemonSet {name} desired template has {len(matches)} owned matching ControllerRevisions; expected 1"
    )
revision_number, revision_hash, history_uid = matches[0]
identity = {
    "generation": generation,
    "revision": revision_hash,
    "controllerRevision": {
        "uid": history_uid,
        "revision": revision_number,
    },
    "spec": spec,
    "template": template,
    "uid": uid,
    "updateStrategy": strategy,
}
print(json.dumps(identity, separators=(",", ":"), sort_keys=True))
' "${daemonset_name}" "${daemonset_json}"
}

wait_dns_manifest_daemonset_target_identity() {
  local daemonset_name="$1"
  local timeout_seconds="${FUGUE_PUBLIC_DATA_PLANE_OBSERVED_TIMEOUT_SECONDS:-120}"
  local started_at
  local identity

  wait_daemonset_observed "${daemonset_name}" || return $?
  started_at="$(date +%s)"
  while true; do
    if identity="$(dns_manifest_daemonset_target_identity "${daemonset_name}" 2>/dev/null)" && [[ -n "${identity}" ]]; then
      printf '%s\n' "${identity}"
      return 0
    fi
    if (( $(date +%s) - started_at >= timeout_seconds )); then
      error "DNS daemonset ${daemonset_name} target ControllerRevision was not observable within ${timeout_seconds}s"
      return 1
    fi
    sleep 2
  done
}

dns_manifest_target_identity_revision() {
  local target_identity="$1"

  python3 -c '
import json
import sys

identity = json.loads(sys.argv[1])
revision = identity.get("revision")
if not isinstance(revision, str) or not revision:
    raise SystemExit("DNS daemonSet target identity has no ControllerRevision hash")
print(revision)
' "${target_identity}"
}

dns_manifest_target_identity_uid() {
  local target_identity="$1"

  python3 -c '
import json
import sys

identity = json.loads(sys.argv[1])
uid = identity.get("uid")
if not isinstance(uid, str) or not uid:
    raise SystemExit("DNS daemonSet target identity has no UID")
print(uid)
' "${target_identity}"
}

dns_manifest_daemonset_matches_target_identity() {
  local daemonset_name="$1"
  local expected_identity="$2"
  local live_identity

  live_identity="$(dns_manifest_daemonset_target_identity "${daemonset_name}")" || return $?
  if [[ "${live_identity}" != "${expected_identity}" ]]; then
    error "DNS daemonset ${daemonset_name} target spec/ControllerRevision identity drifted during the transaction"
    return 1
  fi
}

dns_manifest_daemonset_uses_ondelete() {
  local daemonset_name="$1"
  local strategy
  strategy="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.spec.updateStrategy.type}' 2>/dev/null || true)"
  [[ "${strategy:-RollingUpdate}" == "OnDelete" ]]
}

verify_daemonset_pods_at_revision() {
  local daemonset_name="$1"
  local expected_revision="$2"
  local expected_uid="$3"
  local expected_nodes="$4"
  local selector
  local daemonset_json

  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  expected_revision="$(trim_field "${expected_revision}")"
  expected_uid="$(trim_field "${expected_uid}")"
  [[ -n "${expected_revision}" && -n "${expected_uid}" && -n "$(trim_field "${expected_nodes}")" ]] || {
    error "DNS daemonset ${daemonset_name} cohort verification is missing revision, UID, or nodes"
    return 1
  }
  daemonset_json="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json)" || return $?
  selector="$(daemonset_selector "${daemonset_name}")" || return $?
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -l "${selector}" -o json | python3 -c '
import json
import sys

revision, expected_uid, expected_nodes_text, name, daemonset_text = sys.argv[1:6]
expected_nodes = sorted(line.strip() for line in expected_nodes_text.splitlines() if line.strip())
if not expected_nodes or len(expected_nodes) != len(set(expected_nodes)):
    raise SystemExit(f"DNS daemonSet {name} expected node cohort is empty or duplicated")
daemonset = json.loads(daemonset_text)
metadata = daemonset.get("metadata") or {}
status = daemonset.get("status") or {}
if metadata.get("uid") != expected_uid:
    raise SystemExit(
        f"DNS daemonSet {name} UID changed: expected={expected_uid} actual={metadata.get('uid')}"
    )
desired = status.get("desiredNumberScheduled") or 0
ready = status.get("numberReady") or 0
available = status.get("numberAvailable") or 0
unavailable = status.get("numberUnavailable") or 0
misscheduled = status.get("numberMisscheduled") or 0
generation = metadata.get("generation")
observed = status.get("observedGeneration")
if (
    generation != observed
    or desired != len(expected_nodes)
    or ready != len(expected_nodes)
    or available != len(expected_nodes)
    or unavailable != 0
    or misscheduled != 0
):
    raise SystemExit(
        f"DNS daemonSet {name} status no longer matches the pinned node cohort: "
        f"generation={generation} observed={observed} "
        f"desired={desired} ready={ready} available={available} "
        f"unavailable={unavailable} misscheduled={misscheduled} expected={len(expected_nodes)}"
    )
doc = json.load(sys.stdin)
pods = doc.get("items") or []
if len(pods) != len(expected_nodes):
    raise SystemExit(f"DNS daemonSet {name} has {len(pods)} pods; expected {len(expected_nodes)}")
actual_nodes = sorted((pod.get("spec") or {}).get("nodeName") or "" for pod in pods)
if actual_nodes != expected_nodes:
    raise SystemExit(
        f"DNS daemonSet {name} node cohort changed: expected={expected_nodes} actual={actual_nodes}"
    )
for pod in pods:
    metadata = pod.get("metadata") or {}
    status = pod.get("status") or {}
    pod_name = metadata.get("name") or "<unknown>"
    pod_revision = (metadata.get("labels") or {}).get("controller-revision-hash")
    owner_matches = any(
        owner.get("apiVersion") == "apps/v1"
        and owner.get("kind") == "DaemonSet"
        and owner.get("name") == name
        and owner.get("uid") == expected_uid
        and owner.get("controller") is True
        for owner in metadata.get("ownerReferences") or []
    )
    ready = any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in status.get("conditions") or []
    )
    if (
        status.get("phase") != "Running"
        or metadata.get("deletionTimestamp")
        or not owner_matches
        or not ready
        or pod_revision != revision
    ):
        raise SystemExit(
            f"DNS daemonSet {name} pod {pod_name} is not owned and Ready at revision {revision}: "
            f"phase={status.get('phase')} ready={ready} owner={owner_matches} revision={pod_revision}"
        )
' "${expected_revision}" "${expected_uid}" "${expected_nodes}" "${daemonset_name}" "${daemonset_json}"
}

verify_dns_manifest_daemonset_cohort() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  local expected_revision="$3"
  local expected_uid
  local expected_nodes

  expected_uid="$(dns_manifest_snapshot_query "${snapshot_file}" uid "${daemonset_name}")" || return $?
  expected_nodes="$(dns_manifest_snapshot_query "${snapshot_file}" nodes "${daemonset_name}")" || return $?
  verify_daemonset_pods_at_revision "${daemonset_name}" "${expected_revision}" "${expected_uid}" "${expected_nodes}"
}

daemonset_pod_record_on_node_at_revision() {
  local daemonset_name="$1"
  local node_name="$2"
  local expected_revision="$3"
  local expected_uid="$4"
  local expected_pod_uid="${5:-}"
  local selector
  local live_uid

  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  node_name="$(trim_field "${node_name}")"
  expected_revision="$(trim_field "${expected_revision}")"
  expected_uid="$(trim_field "${expected_uid}")"
  selector="$(daemonset_selector "${daemonset_name}")" || return $?
  live_uid="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o jsonpath='{.metadata.uid}')" || return $?
  [[ -n "${node_name}" && -n "${expected_revision}" && -n "${expected_uid}" && "${live_uid}" == "${expected_uid}" ]] || {
    error "DNS daemonset ${daemonset_name} replacement verification is missing node, revision, or UID"
    return 1
  }
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -l "${selector}" -o json | python3 -c '
import json
import sys

revision, node_name, name, daemonset_uid, expected_pod_uid = sys.argv[1:6]
doc = json.load(sys.stdin)
pods = [pod for pod in doc.get("items") or [] if (pod.get("spec") or {}).get("nodeName") == node_name]
if len(pods) != 1:
    raise SystemExit(f"DNS daemonSet {name} has {len(pods)} pods on {node_name}; expected 1")
pod = pods[0]
metadata = pod.get("metadata") or {}
status = pod.get("status") or {}
pod_name = metadata.get("name") or "<unknown>"
pod_uid = metadata.get("uid") or ""
pod_revision = (metadata.get("labels") or {}).get("controller-revision-hash")
owner_matches = any(
    owner.get("apiVersion") == "apps/v1"
    and owner.get("kind") == "DaemonSet"
    and owner.get("name") == name
    and owner.get("uid") == daemonset_uid
    and owner.get("controller") is True
    for owner in metadata.get("ownerReferences") or []
)
ready = any(
    condition.get("type") == "Ready" and condition.get("status") == "True"
    for condition in status.get("conditions") or []
)
restart_counts = []
seen_container_names = set()
for container in status.get("containerStatuses") or []:
    container_name = container.get("name")
    restart_count = container.get("restartCount")
    if (
        not isinstance(container_name, str)
        or not container_name
        or container_name in seen_container_names
        or isinstance(restart_count, bool)
        or not isinstance(restart_count, int)
        or restart_count != 0
    ):
        raise SystemExit(
            f"DNS daemonSet {name} replacement pod {pod_name} on {node_name} "
            "has duplicate, invalid, or nonzero container restart state"
        )
    seen_container_names.add(container_name)
    restart_counts.append({"name": container_name, "restartCount": restart_count})
restart_counts.sort(key=lambda value: value["name"])
if (
    status.get("phase") != "Running"
    or metadata.get("deletionTimestamp")
    or not owner_matches
    or not ready
    or not pod_uid
    or (expected_pod_uid and pod_uid != expected_pod_uid)
    or not restart_counts
    or pod_revision != revision
):
    raise SystemExit(
        f"DNS daemonSet {name} replacement pod {pod_name} on {node_name} failed revision verification: "
        f"phase={status.get('phase')} ready={ready} owner={owner_matches} "
        f"uid={pod_uid} expected_uid={expected_pod_uid or '<capture>'} "
        f"restart_counts={restart_counts} revision={pod_revision} expected={revision}"
    )
print(json.dumps({
    "name": pod_name,
    "nodeName": node_name,
    "restartCounts": restart_counts,
    "revision": pod_revision,
    "uid": pod_uid,
}, separators=(",", ":"), sort_keys=True))
' "${expected_revision}" "${node_name}" "${daemonset_name}" "${expected_uid}" "${expected_pod_uid}"
}

daemonset_pod_uid_on_node_at_revision() {
  local record

  record="$(daemonset_pod_record_on_node_at_revision "$@")" || return $?
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  python3 -c '
import json
import sys

record = json.loads(sys.argv[1])
uid = record.get("uid")
if not isinstance(uid, str) or not uid:
    raise SystemExit("DNS daemonSet replacement record has no Pod UID")
print(uid)
' "${record}"
}

verify_daemonset_pod_on_node_at_revision() {
  daemonset_pod_uid_on_node_at_revision "$@" >/dev/null
}

canonicalize_dns_manifest_pod_records() {
  local daemonset_name="$1"
  local expected_revision="$2"
  local expected_nodes="$3"

  python3 -c '
import json
import sys

name, revision, expected_nodes_text = sys.argv[1:4]
expected_nodes = sorted(line.strip() for line in expected_nodes_text.splitlines() if line.strip())
display_name = name or "<unknown>"
if not name or not revision or not expected_nodes or len(expected_nodes) != len(set(expected_nodes)):
    raise SystemExit(f"DNS daemonSet {display_name} controlled cohort has invalid identity or nodes")
records = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    value = json.loads(line)
    if isinstance(value, list):
        records.extend(value)
    else:
        records.append(value)
if not records:
    raise SystemExit(f"DNS daemonSet {name} controlled cohort has no verified Pod records")
seen_names = set()
seen_nodes = set()
seen_uids = set()
canonical = []
for record in records:
    if not isinstance(record, dict):
        raise SystemExit(f"DNS daemonSet {name} controlled cohort contains a non-object record")
    pod_name = record.get("name")
    node_name = record.get("nodeName")
    pod_uid = record.get("uid")
    pod_revision = record.get("revision")
    restart_counts = record.get("restartCounts")
    if (
        not isinstance(pod_name, str)
        or not pod_name
        or not isinstance(node_name, str)
        or not node_name
        or not isinstance(pod_uid, str)
        or not pod_uid
        or pod_revision != revision
        or not isinstance(restart_counts, list)
        or not restart_counts
        or pod_name in seen_names
        or node_name in seen_nodes
        or pod_uid in seen_uids
    ):
        raise SystemExit(f"DNS daemonSet {name} controlled cohort contains an invalid or duplicate Pod record")
    seen_container_names = set()
    canonical_restarts = []
    for container in restart_counts:
        if not isinstance(container, dict):
            raise SystemExit(f"DNS daemonSet {name} controlled cohort has an invalid restart record")
        container_name = container.get("name")
        restart_count = container.get("restartCount")
        if (
            not isinstance(container_name, str)
            or not container_name
            or container_name in seen_container_names
            or isinstance(restart_count, bool)
            or not isinstance(restart_count, int)
            or restart_count != 0
        ):
            raise SystemExit(f"DNS daemonSet {name} controlled cohort has duplicate, invalid, or nonzero restart state")
        seen_container_names.add(container_name)
        canonical_restarts.append({"name": container_name, "restartCount": restart_count})
    canonical_restarts.sort(key=lambda value: value["name"])
    seen_names.add(pod_name)
    seen_nodes.add(node_name)
    seen_uids.add(pod_uid)
    canonical.append({
        "name": pod_name,
        "nodeName": node_name,
        "restartCounts": canonical_restarts,
        "revision": pod_revision,
        "uid": pod_uid,
    })
canonical.sort(key=lambda value: value["nodeName"])
actual_nodes = [record["nodeName"] for record in canonical]
if actual_nodes != expected_nodes:
    raise SystemExit(
        f"DNS daemonSet {name} controlled cohort node set changed: "
        f"expected={expected_nodes} actual={actual_nodes}"
    )
print(json.dumps(canonical, separators=(",", ":"), sort_keys=True))
' "${daemonset_name}" "${expected_revision}" "${expected_nodes}"
}

capture_dns_manifest_daemonset_pod_cohort() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  local expected_revision="$3"
  local expected_uid
  local expected_nodes
  local selector

  expected_uid="$(dns_manifest_snapshot_query "${snapshot_file}" uid "${daemonset_name}")" || return $?
  expected_nodes="$(dns_manifest_snapshot_query "${snapshot_file}" nodes "${daemonset_name}")" || return $?
  verify_daemonset_pods_at_revision \
    "${daemonset_name}" "${expected_revision}" "${expected_uid}" "${expected_nodes}" || return $?
  selector="$(daemonset_selector "${daemonset_name}")" || return $?
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -l "${selector}" -o json | python3 -c '
import json
import sys

revision, expected_uid, expected_nodes_text, name = sys.argv[1:5]
expected_nodes = sorted(line.strip() for line in expected_nodes_text.splitlines() if line.strip())
doc = json.load(sys.stdin)
records = []
for pod in doc.get("items") or []:
    metadata = pod.get("metadata") or {}
    status = pod.get("status") or {}
    node_name = (pod.get("spec") or {}).get("nodeName") or ""
    pod_name = metadata.get("name") or ""
    pod_uid = metadata.get("uid") or ""
    pod_revision = (metadata.get("labels") or {}).get("controller-revision-hash")
    owner_matches = any(
        owner.get("apiVersion") == "apps/v1"
        and owner.get("kind") == "DaemonSet"
        and owner.get("name") == name
        and owner.get("uid") == expected_uid
        and owner.get("controller") is True
        for owner in metadata.get("ownerReferences") or []
    )
    ready = any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in status.get("conditions") or []
    )
    statuses = status.get("containerStatuses") or []
    restart_counts = []
    for container in statuses:
        container_name = container.get("name")
        restart_count = container.get("restartCount")
        if (
            not isinstance(container_name, str)
            or not container_name
            or isinstance(restart_count, bool)
            or not isinstance(restart_count, int)
            or restart_count != 0
        ):
            raise SystemExit(
                f"DNS daemonSet {name} pod {pod_name or '<unknown>'} has an invalid or nonzero restart baseline"
            )
        restart_counts.append({"name": container_name, "restartCount": restart_count})
    restart_counts.sort(key=lambda value: value["name"])
    if (
        not node_name
        or not pod_name
        or not pod_uid
        or status.get("phase") != "Running"
        or metadata.get("deletionTimestamp")
        or not owner_matches
        or not ready
        or not restart_counts
        or pod_revision != revision
    ):
        raise SystemExit(
            f"DNS daemonSet {name} pod {pod_name or '<unknown>'} is not a stable zero-restart member of revision {revision}"
        )
    records.append({
        "name": pod_name,
        "nodeName": node_name,
        "restartCounts": restart_counts,
        "revision": pod_revision,
        "uid": pod_uid,
    })
records.sort(key=lambda value: value["nodeName"])
if [record["nodeName"] for record in records] != expected_nodes:
    raise SystemExit(
        f"DNS daemonSet {name} exact pod cohort changed nodes: "
        f"expected={expected_nodes} actual={[record['nodeName'] for record in records]}"
    )
print(json.dumps(records, separators=(",", ":"), sort_keys=True))
' "${expected_revision}" "${expected_uid}" "${expected_nodes}" "${daemonset_name}"
}

verify_dns_manifest_daemonset_pod_cohort() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  local expected_revision="$3"
  local expected_cohort="$4"
  local live_cohort

  [[ -n "$(trim_field "${expected_cohort}")" ]] || {
    error "DNS daemonset ${daemonset_name} has no pinned replacement Pod cohort"
    return 1
  }
  live_cohort="$(capture_dns_manifest_daemonset_pod_cohort \
    "${snapshot_file}" "${daemonset_name}" "${expected_revision}")" || return $?
  if [[ "${live_cohort}" != "${expected_cohort}" ]]; then
    error "DNS daemonset ${daemonset_name} Pod UID/restart cohort drifted after controlled replacement"
    return 1
  fi
}

write_dns_manifest_target_state() {
  local snapshot_file="$1"
  local target_file="$2"
  shift 2
  local target_dir
  local work_dir
  local records_file
  local output_file
  local daemonset_name
  local state
  local target_identity
  local pod_cohort

  [[ -n "$(trim_field "${target_file}")" && $(( $# % 4 )) -eq 0 && $# -gt 0 ]] || {
    error "DNS manifest target-state handoff requires a path and complete daemonset records"
    return 1
  }
  target_dir="$(dirname "${target_file}")"
  mkdir -p "${target_dir}" || return $?
  work_dir="$(mktemp -d "${target_dir}/.fugue-dns-target-state.XXXXXX")" || return $?
  chmod 700 "${work_dir}" || {
    rm -rf "${work_dir}"
    return 1
  }
  records_file="${work_dir}/records.jsonl"
  output_file="${work_dir}/target-state.json"
  : >"${records_file}" || {
    rm -rf "${work_dir}"
    return 1
  }
  chmod 600 "${records_file}" || {
    rm -rf "${work_dir}"
    return 1
  }
  while (( $# > 0 )); do
    daemonset_name="$1"
    state="$2"
    target_identity="$3"
    pod_cohort="$4"
    shift 4
    if ! python3 -c '
import json
import sys

name, state, identity_text, cohort_text = sys.argv[1:5]
identity = json.loads(identity_text)
cohort = json.loads(cohort_text)
if not name or state not in {"unchanged", "strategy-only", "template-changed"}:
    raise SystemExit("invalid DNS target-state daemonset identity")
if not isinstance(identity, dict) or not isinstance(cohort, list) or not cohort:
    raise SystemExit(f"DNS target-state record for {name} is incomplete")
print(json.dumps({
    "name": name,
    "podCohort": cohort,
    "state": state,
    "targetIdentity": identity,
}, separators=(",", ":"), sort_keys=True))
' "${daemonset_name}" "${state}" "${target_identity}" "${pod_cohort}" >>"${records_file}"; then
      rm -rf "${work_dir}"
      return 1
    fi
  done
  if ! python3 - "${snapshot_file}" "${FUGUE_NAMESPACE}" "${FUGUE_RELEASE_INSTANCE}" \
    "${FUGUE_RELEASE_FULLNAME}" "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID}" "${records_file}" >"${output_file}" <<'PY'
import hashlib
import json
import sys

snapshot_path, namespace, release_instance, release_fullname, release_id, records_path = sys.argv[1:7]
snapshot_bytes = open(snapshot_path, "rb").read()
with open(snapshot_path, encoding="utf-8") as handle:
    snapshot = json.load(handle)
records = []
with open(records_path, encoding="utf-8") as handle:
    for line in handle:
        if line.strip():
            records.append(json.loads(line))
snapshot_names = sorted(item.get("name") for item in snapshot.get("daemonSets") or [])
record_names = sorted(item.get("name") for item in records)
if not records or record_names != snapshot_names or len(record_names) != len(set(record_names)):
    raise SystemExit("DNS target-state daemonset set does not match the rollback snapshot")
target = {
    "apiVersion": "fugue.io/v1alpha1",
    "kind": "DNSManifestTargetState",
    "namespace": namespace,
    "releaseInstance": release_instance,
    "releaseFullname": release_fullname,
    "releaseID": release_id,
    "snapshotSHA256": hashlib.sha256(snapshot_bytes).hexdigest(),
    "daemonSets": sorted(records, key=lambda value: value["name"]),
}
json.dump(target, sys.stdout, separators=(",", ":"), sort_keys=True)
sys.stdout.write("\n")
PY
  then
    rm -rf "${work_dir}"
    return 1
  fi
  chmod 600 "${output_file}" || {
    rm -rf "${work_dir}"
    return 1
  }
  mv "${output_file}" "${target_file}" || {
    rm -rf "${work_dir}"
    return 1
  }
  rm -rf "${work_dir}"
  log "wrote exact DNS manifest target-state handoff to ${target_file}"
}

dns_manifest_target_state_rows() {
  local snapshot_file="$1"
  local target_file="$2"

  python3 - "${snapshot_file}" "${target_file}" "${FUGUE_NAMESPACE}" "${FUGUE_RELEASE_INSTANCE}" \
    "${FUGUE_RELEASE_FULLNAME}" "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID}" <<'PY'
import base64
import hashlib
import json
import os
import stat
import sys

snapshot_path, target_path, namespace, release_instance, release_fullname, release_id = sys.argv[1:7]
for path in (snapshot_path, target_path):
    metadata = os.lstat(path)
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise SystemExit("DNS manifest handoff inputs must be regular non-symlink files")
    if metadata.st_size <= 0 or metadata.st_size > 8 * 1024 * 1024:
        raise SystemExit("DNS manifest handoff input size is outside the safety bound")
    if stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_uid != os.geteuid():
        raise SystemExit("DNS manifest handoff inputs must be owner-only files owned by the release process")
snapshot_bytes = open(snapshot_path, "rb").read()
with open(snapshot_path, encoding="utf-8") as handle:
    snapshot = json.load(handle)
with open(target_path, encoding="utf-8") as handle:
    target = json.load(handle)
if target.get("apiVersion") != "fugue.io/v1alpha1" or target.get("kind") != "DNSManifestTargetState":
    raise SystemExit("invalid DNS manifest target-state schema")
if (
    target.get("namespace") != namespace
    or target.get("releaseInstance") != release_instance
    or target.get("releaseFullname") != release_fullname
    or target.get("releaseID") != release_id
):
    raise SystemExit("DNS manifest target-state scope does not match the active release")
if target.get("snapshotSHA256") != hashlib.sha256(snapshot_bytes).hexdigest():
    raise SystemExit("DNS manifest target-state is not bound to the active rollback snapshot")
snapshot_names = sorted(item.get("name") for item in snapshot.get("daemonSets") or [])
records = target.get("daemonSets") or []
record_names = sorted(item.get("name") for item in records)
if not records or record_names != snapshot_names or len(record_names) != len(set(record_names)):
    raise SystemExit("DNS manifest target-state daemonset set does not match the rollback snapshot")
for record in sorted(records, key=lambda value: value.get("name") or ""):
    name = record.get("name")
    state = record.get("state")
    identity = record.get("targetIdentity")
    cohort = record.get("podCohort")
    if (
        not isinstance(name, str)
        or not name
        or state not in {"unchanged", "strategy-only", "template-changed"}
        or not isinstance(identity, dict)
        or not isinstance(cohort, list)
        or not cohort
    ):
        raise SystemExit("DNS manifest target-state contains an incomplete daemonset record")
    encode = lambda value: base64.urlsafe_b64encode(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    ).decode()
    print("|".join((name, state, encode(identity), encode(cohort))))
PY
}

decode_dns_manifest_target_state_field() {
  python3 -c '
import base64
import sys

print(base64.urlsafe_b64decode(sys.argv[1].encode()).decode())
' "$1"
}

dns_manifest_handoff_identity() {
  local snapshot_file="$1"
  local target_file="$2"

  python3 - "${snapshot_file}" "${target_file}" <<'PY'
import hashlib
import os
import stat
import sys

parts = []
for path in sys.argv[1:]:
    path_metadata = os.lstat(path)
    if not stat.S_ISREG(path_metadata.st_mode) or stat.S_ISLNK(path_metadata.st_mode):
        raise SystemExit("DNS manifest handoff inputs must be regular non-symlink files")
    with open(path, "rb") as handle:
        metadata = os.fstat(handle.fileno())
        if (metadata.st_dev, metadata.st_ino) != (path_metadata.st_dev, path_metadata.st_ino):
            raise SystemExit("DNS manifest handoff input changed while it was opened")
        if metadata.st_size <= 0 or metadata.st_size > 8 * 1024 * 1024:
            raise SystemExit("DNS manifest handoff input size is outside the safety bound")
        if stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_uid != os.geteuid():
            raise SystemExit("DNS manifest handoff inputs must be owner-only files owned by the release process")
        digest = hashlib.sha256(handle.read()).hexdigest()
    parts.append(":".join((
        str(metadata.st_dev),
        str(metadata.st_ino),
        str(metadata.st_size),
        str(metadata.st_mtime_ns),
        str(metadata.st_ctime_ns),
        digest,
    )))
print("|".join(parts))
PY
}

verify_dns_manifest_target_state_snapshot() {
  local snapshot_file="$1"
  local target_file="$2"
  local expected_rows="$3"
  local expected_handoff_identity="$4"
  local rows
  local handoff_identity
  local daemonset_name
  local state
  local encoded_identity
  local encoded_cohort
  local target_identity
  local pod_cohort
  local verified_count=0
  local rows_after

  [[ -n "${target_file}" && -s "${target_file}" ]] || {
    error "DNS manifest target-state handoff is missing or empty"
    return 1
  }
  [[ -n "$(trim_field "${expected_rows}")" && -n "$(trim_field "${expected_handoff_identity}")" ]] || return 1
  handoff_identity="$(dns_manifest_handoff_identity "${snapshot_file}" "${target_file}")" || return $?
  [[ "${handoff_identity}" == "${expected_handoff_identity}" ]] || {
    error "DNS manifest snapshot or target-state file identity changed during final verification"
    return 1
  }
  rows="$(dns_manifest_target_state_rows "${snapshot_file}" "${target_file}")" || return $?
  [[ "${rows}" == "${expected_rows}" ]] || {
    error "DNS manifest target-state records changed during final verification"
    return 1
  }
  [[ -n "$(trim_field "${rows}")" ]] || {
    error "DNS manifest target-state handoff contains no daemonsets"
    return 1
  }
  validate_dns_manifest_snapshot_live_set "${snapshot_file}" || return $?
  while IFS='|' read -r daemonset_name state encoded_identity encoded_cohort; do
    [[ -n "${daemonset_name}" && -n "${state}" && -n "${encoded_identity}" && -n "${encoded_cohort}" ]] || return 1
    target_identity="$(decode_dns_manifest_target_state_field "${encoded_identity}")" || return $?
    pod_cohort="$(decode_dns_manifest_target_state_field "${encoded_cohort}")" || return $?
    validate_dns_manifest_target_transport "${snapshot_file}" "${daemonset_name}" || return $?
    verify_dns_manifest_daemonset_target_state \
      "${snapshot_file}" "${daemonset_name}" "${state}" "${target_identity}" "${pod_cohort}" || return $?
    verified_count=$((verified_count + 1))
  done <<<"${rows}"
  (( verified_count > 0 )) || return 1
  validate_dns_manifest_snapshot_live_set "${snapshot_file}" || return $?
  handoff_identity="$(dns_manifest_handoff_identity "${snapshot_file}" "${target_file}")" || return $?
  [[ "${handoff_identity}" == "${expected_handoff_identity}" ]] || {
    error "DNS manifest snapshot or target-state file identity changed during final verification"
    return 1
  }
  rows_after="$(dns_manifest_target_state_rows "${snapshot_file}" "${target_file}")" || return $?
  [[ "${rows_after}" == "${expected_rows}" ]] || {
    error "DNS manifest snapshot or target-state handoff changed during final verification"
    return 1
  }
}

verify_dns_manifest_target_state_snapshot_file() {
  local snapshot_file="$1"
  local target_file="$2"
  local rows
  local handoff_identity

  handoff_identity="$(dns_manifest_handoff_identity "${snapshot_file}" "${target_file}")" || return $?
  rows="$(dns_manifest_target_state_rows "${snapshot_file}" "${target_file}")" || return $?
  [[ "$(dns_manifest_handoff_identity "${snapshot_file}" "${target_file}")" == "${handoff_identity}" ]] || {
    error "DNS manifest handoff changed while its quick verification baseline was pinned"
    return 1
  }
  verify_dns_manifest_target_state_snapshot \
    "${snapshot_file}" "${target_file}" "${rows}" "${handoff_identity}"
}

write_dns_manifest_handoff_identity_file() {
  local identity_file="$1"
  local handoff_identity="$2"
  local identity_dir
  local temporary_file

  [[ -n "$(trim_field "${identity_file}")" && -n "$(trim_field "${handoff_identity}")" ]] || return 1
  identity_dir="$(dirname "${identity_file}")"
  [[ -d "${identity_dir}" && ! -L "${identity_dir}" ]] || return 1
  temporary_file="$(mktemp "${identity_dir}/.fugue-dns-handoff-identity.XXXXXX")" || return $?
  chmod 600 "${temporary_file}" || {
    rm -f "${temporary_file}"
    return 1
  }
  printf '%s\n' "${handoff_identity}" >"${temporary_file}" || {
    rm -f "${temporary_file}"
    return 1
  }
  mv "${temporary_file}" "${identity_file}" || {
    rm -f "${temporary_file}"
    return 1
  }
}

verify_dns_manifest_target_state_snapshot_with_smoke() {
  local snapshot_file="$1"
  local target_file="$2"
  local identity_file="$3"
  local rows
  local handoff_identity

  handoff_identity="$(dns_manifest_handoff_identity "${snapshot_file}" "${target_file}")" || return $?
  rows="$(dns_manifest_target_state_rows "${snapshot_file}" "${target_file}")" || return $?
  [[ "$(dns_manifest_handoff_identity "${snapshot_file}" "${target_file}")" == "${handoff_identity}" ]] || {
    error "DNS manifest handoff changed while its smoke handoff baseline was pinned"
    return 1
  }
  verify_dns_manifest_target_state_snapshot \
    "${snapshot_file}" "${target_file}" "${rows}" "${handoff_identity}" || return $?
  run_smoke_urls || return $?
  verify_dns_manifest_target_state_snapshot \
    "${snapshot_file}" "${target_file}" "${rows}" "${handoff_identity}" || return $?
  write_dns_manifest_handoff_identity_file "${identity_file}" "${handoff_identity}"
}

verify_dns_manifest_target_state_file() {
  local snapshot_file="$1"
  local target_file="$2"
  local rows
  local handoff_identity
  local daemonset_name

  handoff_identity="$(dns_manifest_handoff_identity "${snapshot_file}" "${target_file}")" || return $?
  rows="$(dns_manifest_target_state_rows "${snapshot_file}" "${target_file}")" || return $?
  [[ "$(dns_manifest_handoff_identity "${snapshot_file}" "${target_file}")" == "${handoff_identity}" ]] || {
    error "DNS manifest handoff changed while its final verification baseline was pinned"
    return 1
  }
  verify_dns_manifest_target_state_snapshot \
    "${snapshot_file}" "${target_file}" "${rows}" "${handoff_identity}" || return $?
  while IFS='|' read -r daemonset_name _; do
    [[ -n "${daemonset_name}" ]] || return 1
    check_authoritative_dns_on_nodes "${daemonset_name}" || return $?
  done <<<"${rows}"
  [[ "$(dns_manifest_handoff_identity "${snapshot_file}" "${target_file}")" == "${handoff_identity}" ]] || {
    error "DNS manifest handoff changed during authoritative verification"
    return 1
  }
  run_smoke_urls || return $?
  verify_dns_manifest_target_state_snapshot \
    "${snapshot_file}" "${target_file}" "${rows}" "${handoff_identity}" || return $?
  log "exact DNS manifest target state passed the final commit verification"
}

delete_pod_by_uid_no_wait() {
  local pod_name="$1"
  local pod_uid="$2"
  local delete_uri

  pod_name="$(trim_field "${pod_name}")"
  pod_uid="$(trim_field "${pod_uid}")"
  [[ "${pod_name}" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ && "${pod_uid}" =~ ^[A-Za-z0-9_-]+$ ]] || {
    error "refusing DNS Pod delete without a valid name and UID precondition"
    return 1
  }
  delete_uri="/api/v1/namespaces/${FUGUE_NAMESPACE}/pods/${pod_name}"
  python3 -c '
import json
import sys
print(json.dumps({
    "apiVersion": "v1",
    "kind": "DeleteOptions",
    "preconditions": {"uid": sys.argv[1]},
}, separators=(",", ":")))
' "${pod_uid}" | kubectl_cmd delete --raw="${delete_uri}" -f - >/dev/null
}

replace_dns_manifest_daemonset() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  local target_identity="$3"
  local target_revision
  local target_uid
  local snapshot_uid
  local snapshot_nodes
  local original_uids
  local rows
  local ds pod uid created phase restarts
  local node_name
  local replacement_uid
  local replacement_record
  local verified_record
  local controlled_records=""
  local controlled_cohort

  DNS_MANIFEST_LAST_CONTROLLED_POD_COHORT=""

  [[ -n "${target_identity}" ]] || {
    error "DNS daemonset ${daemonset_name} has no pinned post-Helm target identity"
    return 1
  }
  dns_manifest_daemonset_matches_target_identity "${daemonset_name}" "${target_identity}" || return $?
  target_revision="$(dns_manifest_target_identity_revision "${target_identity}")" || return $?
  target_uid="$(dns_manifest_target_identity_uid "${target_identity}")" || return $?
  snapshot_uid="$(dns_manifest_snapshot_query "${snapshot_file}" uid "${daemonset_name}")" || return $?
  snapshot_nodes="$(dns_manifest_snapshot_query "${snapshot_file}" nodes "${daemonset_name}")" || return $?
  [[ -n "${snapshot_uid}" && "${target_uid}" == "${snapshot_uid}" ]] || {
    error "DNS daemonset ${daemonset_name} target UID does not match its pre-Helm snapshot"
    return 1
  }
  original_uids="$(dns_manifest_snapshot_query "${snapshot_file}" uids "${daemonset_name}")" || return $?
  rows="$(capture_daemonset_pods "${daemonset_name}")" || return $?
  [[ -n "$(trim_field "${rows}")" ]] || {
    error "DNS daemonset ${daemonset_name} has no pods to replace"
    return 1
  }
  while IFS='|' read -r ds pod uid created phase restarts; do
    pod="$(trim_field "${pod}")"
    uid="$(trim_field "${uid}")"
    [[ -n "${pod}" && -n "${uid}" ]] || continue
    if ! printf '%s\n' "${original_uids}" | grep -Fxq "${uid}"; then
      error "DNS daemonset ${daemonset_name} pod ${pod} no longer has its pre-Helm UID"
      return 1
    fi
    dns_manifest_daemonset_matches_target_identity "${daemonset_name}" "${target_identity}" || return $?
    node_name="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "pod/${pod}" -o jsonpath='{.spec.nodeName}')" || return $?
    [[ -n "${node_name}" ]] || {
      error "DNS daemonset ${daemonset_name} pod ${pod} has no pinned node identity"
      return 1
    }
    log "deleting DNS manifest pod for ${daemonset_name}: ${pod}"
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" != "true" ]]; then
      delete_pod_by_uid_no_wait "${pod}" "${uid}" || return $?
      wait_daemonset_replaced_and_ready "${daemonset_name}" "${uid}" || return $?
      replacement_uid="$(daemonset_pod_uid_on_node_at_revision \
        "${daemonset_name}" "${node_name}" "${target_revision}" "${snapshot_uid}")" || return $?
      [[ -n "${replacement_uid}" && "${replacement_uid}" != "${uid}" ]] || {
        error "DNS daemonset ${daemonset_name} did not produce a distinct replacement Pod UID on ${node_name}"
        return 1
      }
    fi
    check_authoritative_dns_on_nodes "${daemonset_name}" || return $?
    run_smoke_urls || return $?
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" != "true" ]]; then
      replacement_record="$(daemonset_pod_record_on_node_at_revision \
        "${daemonset_name}" "${node_name}" "${target_revision}" "${snapshot_uid}" "${replacement_uid}")" || return $?
      verified_record="$(daemonset_pod_record_on_node_at_revision \
        "${daemonset_name}" "${node_name}" "${target_revision}" "${snapshot_uid}" "${replacement_uid}")" || return $?
      if [[ "${replacement_record}" != "${verified_record}" ]]; then
        error "DNS daemonset ${daemonset_name} replacement Pod record drifted while it was being pinned on ${node_name}"
        return 1
      fi
      controlled_records+="${verified_record}"$'\n'
      dns_manifest_daemonset_matches_target_identity "${daemonset_name}" "${target_identity}" || return $?
    fi
  done <<<"${rows}"
  dns_manifest_daemonset_matches_target_identity "${daemonset_name}" "${target_identity}" || return $?
  verify_daemonset_pods_at_revision "${daemonset_name}" "${target_revision}" "${snapshot_uid}" "${snapshot_nodes}" || return $?
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" != "true" ]]; then
    controlled_cohort="$(printf '%s' "${controlled_records}" | canonicalize_dns_manifest_pod_records \
      "${daemonset_name}" "${target_revision}" "${snapshot_nodes}")" || return $?
    verify_dns_manifest_daemonset_pod_cohort \
      "${snapshot_file}" "${daemonset_name}" "${target_revision}" "${controlled_cohort}" || return $?
    DNS_MANIFEST_LAST_CONTROLLED_POD_COHORT="${controlled_cohort}"
  fi
}

restore_dns_manifest_daemonset() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  local original_uids
  local serving_patch
  local desired_patch
  local snapshot_revision
  local snapshot_uid
  local snapshot_nodes
  local restored_revision
  local restored_uid
  local restored_identity
  local rows
  local ds pod uid created phase restarts
  local node_name
  local replacement_uid
  local replacement_record
  local verified_record
  local trusted_records=""
  local restored_pod_cohort
  local snapshot_pod_cohort

  DNS_MANIFEST_LAST_RESTORED_POD_COHORT=""

  original_uids="$(dns_manifest_snapshot_query "${snapshot_file}" uids "${daemonset_name}")" || return $?
  snapshot_revision="$(dns_manifest_snapshot_query "${snapshot_file}" serving-revision "${daemonset_name}")" || return $?
  snapshot_uid="$(dns_manifest_snapshot_query "${snapshot_file}" uid "${daemonset_name}")" || return $?
  snapshot_nodes="$(dns_manifest_snapshot_query "${snapshot_file}" nodes "${daemonset_name}")" || return $?
  dns_manifest_daemonset_uid_matches_snapshot "${snapshot_file}" "${daemonset_name}" || return $?
  if dns_manifest_daemonset_matches_snapshot "${snapshot_file}" "${daemonset_name}"; then
    snapshot_pod_cohort="$(dns_manifest_snapshot_query "${snapshot_file}" pod-cohort "${daemonset_name}")" || return $?
    restored_pod_cohort="$(printf '%s\n' "${snapshot_pod_cohort}" | canonicalize_dns_manifest_pod_records \
      "${daemonset_name}" "${snapshot_revision}" "${snapshot_nodes}")" || return $?
    if verify_dns_manifest_daemonset_pod_cohort \
      "${snapshot_file}" "${daemonset_name}" "${snapshot_revision}" "${restored_pod_cohort}"; then
      log "DNS daemonset ${daemonset_name} already matches its exact pre-Helm snapshot cohort"
      wait_daemonset_ready "${daemonset_name}" || return $?
      check_authoritative_dns_on_nodes "${daemonset_name}" || return $?
      verify_dns_manifest_daemonset_pod_cohort \
        "${snapshot_file}" "${daemonset_name}" "${snapshot_revision}" "${restored_pod_cohort}" || return $?
      DNS_MANIFEST_LAST_RESTORED_POD_COHORT="${restored_pod_cohort}"
      return 0
    fi
  fi

  serving_patch="$(dns_manifest_snapshot_query "${snapshot_file}" restore-serving-ondelete-patch "${daemonset_name}")" || return $?
  desired_patch="$(dns_manifest_snapshot_query "${snapshot_file}" restore-desired-patch "${daemonset_name}")" || return $?
  log "restoring DNS daemonset ${daemonset_name} pre-Helm serving template under OnDelete"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" patch "ds/${daemonset_name}" --type=json -p "${serving_patch}" >/dev/null || return $?
  wait_daemonset_observed "${daemonset_name}" || return $?
  restored_identity="$(wait_dns_manifest_daemonset_target_identity "${daemonset_name}")" || return $?
  restored_revision="$(dns_manifest_target_identity_revision "${restored_identity}")" || return $?
  restored_uid="$(dns_manifest_target_identity_uid "${restored_identity}")" || return $?
  [[ -n "${restored_revision}" && "${restored_revision}" == "${snapshot_revision}" && "${restored_uid}" == "${snapshot_uid}" ]] || {
    error "DNS daemonset ${daemonset_name} restored serving revision ${restored_revision:-<empty>} does not match snapshot ${snapshot_revision}"
    return 1
  }

  rows="$(capture_daemonset_pods "${daemonset_name}")" || return $?
  while IFS='|' read -r ds pod uid created phase restarts; do
    pod="$(trim_field "${pod}")"
    uid="$(trim_field "${uid}")"
    [[ -n "${pod}" && -n "${uid}" ]] || continue
    node_name="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "pod/${pod}" -o jsonpath='{.spec.nodeName}')" || return $?
    [[ -n "${node_name}" ]] || {
      error "DNS daemonset ${daemonset_name} rollback pod ${pod} has no pinned node identity"
      return 1
    }
    if printf '%s\n' "${original_uids}" | grep -Fxq "${uid}"; then
      replacement_record="$(daemonset_pod_record_on_node_at_revision \
        "${daemonset_name}" "${node_name}" "${snapshot_revision}" "${snapshot_uid}" "${uid}")" || return $?
      trusted_records+="${replacement_record}"$'\n'
      continue
    fi
    log "replacing changed DNS pod during rollback for ${daemonset_name}: ${pod} uid=${uid}"
    dns_manifest_daemonset_matches_target_identity "${daemonset_name}" "${restored_identity}" || return $?
    delete_pod_by_uid_no_wait "${pod}" "${uid}" || return $?
    wait_daemonset_replaced_and_ready "${daemonset_name}" "${uid}" || return $?
    replacement_uid="$(daemonset_pod_uid_on_node_at_revision \
      "${daemonset_name}" "${node_name}" "${snapshot_revision}" "${snapshot_uid}")" || return $?
    [[ -n "${replacement_uid}" && "${replacement_uid}" != "${uid}" ]] || {
      error "DNS daemonset ${daemonset_name} rollback did not produce a distinct replacement Pod UID on ${node_name}"
      return 1
    }
    check_authoritative_dns_on_nodes "${daemonset_name}" || return $?
    run_smoke_urls || return $?
    replacement_record="$(daemonset_pod_record_on_node_at_revision \
      "${daemonset_name}" "${node_name}" "${snapshot_revision}" "${snapshot_uid}" "${replacement_uid}")" || return $?
    verified_record="$(daemonset_pod_record_on_node_at_revision \
      "${daemonset_name}" "${node_name}" "${snapshot_revision}" "${snapshot_uid}" "${replacement_uid}")" || return $?
    if [[ "${replacement_record}" != "${verified_record}" ]]; then
      error "DNS daemonset ${daemonset_name} rollback Pod record drifted while it was being pinned on ${node_name}"
      return 1
    fi
    trusted_records+="${verified_record}"$'\n'
    dns_manifest_daemonset_matches_target_identity "${daemonset_name}" "${restored_identity}" || return $?
  done <<<"${rows}"

  dns_manifest_daemonset_matches_target_identity "${daemonset_name}" "${restored_identity}" || return $?
  wait_daemonset_ready "${daemonset_name}" || return $?
  restored_pod_cohort="$(printf '%s' "${trusted_records}" | canonicalize_dns_manifest_pod_records \
    "${daemonset_name}" "${snapshot_revision}" "${snapshot_nodes}")" || return $?
  verify_dns_manifest_daemonset_pod_cohort \
    "${snapshot_file}" "${daemonset_name}" "${snapshot_revision}" "${restored_pod_cohort}" || return $?
  dns_manifest_daemonset_uid_matches_snapshot "${snapshot_file}" "${daemonset_name}" || return $?
  kubectl_cmd -n "${FUGUE_NAMESPACE}" patch "ds/${daemonset_name}" --type=json -p "${desired_patch}" >/dev/null || return $?
  wait_daemonset_observed "${daemonset_name}" || return $?
  dns_manifest_daemonset_matches_snapshot "${snapshot_file}" "${daemonset_name}" || {
    error "DNS daemonset ${daemonset_name} does not match its pre-Helm snapshot after restore"
    return 1
  }
  verify_dns_manifest_daemonset_pod_cohort \
    "${snapshot_file}" "${daemonset_name}" "${snapshot_revision}" "${restored_pod_cohort}" || return $?
  wait_daemonset_ready "${daemonset_name}" || return $?
  dns_manifest_daemonset_matches_snapshot "${snapshot_file}" "${daemonset_name}" || return $?
  verify_dns_manifest_daemonset_pod_cohort \
    "${snapshot_file}" "${daemonset_name}" "${snapshot_revision}" "${restored_pod_cohort}" || return $?
  check_authoritative_dns_on_nodes "${daemonset_name}" || return $?
  verify_dns_manifest_daemonset_pod_cohort \
    "${snapshot_file}" "${daemonset_name}" "${snapshot_revision}" "${restored_pod_cohort}" || return $?
  DNS_MANIFEST_LAST_RESTORED_POD_COHORT="${restored_pod_cohort}"
}

validate_dns_manifest_snapshot_live_set() {
  local snapshot_file="$1"
  local snapshot_names
  local live_names

  snapshot_names="$(dns_manifest_snapshot_query "${snapshot_file}" names)" || return $?
  live_names="$(dns_daemonset_names)" || return $?
  if [[ "${snapshot_names}" != "${live_names}" ]]; then
    error "live DNS daemonset set does not match the pre-Helm snapshot"
    error "snapshot daemonsets: $(printf '%s' "${snapshot_names}" | tr '\n' ' ')"
    error "live daemonsets: $(printf '%s' "${live_names}" | tr '\n' ' ')"
    return 1
  fi
}

restore_dns_manifest_snapshot() {
  local snapshot_file="$1"
  local snapshot_namespace
  local snapshot_names=()
  local restored_cohorts=()
  local daemonset_name
  local index
  local snapshot_revision
  local snapshot_names_text
  local restore_failed=false

  dns_manifest_snapshot_query "${snapshot_file}" validate || return $?
  snapshot_namespace="$(dns_manifest_snapshot_query "${snapshot_file}" namespace)" || return $?
  if [[ "${snapshot_namespace}" != "${FUGUE_NAMESPACE}" ]]; then
    error "DNS manifest snapshot namespace ${snapshot_namespace} does not match ${FUGUE_NAMESPACE}"
    return 1
  fi
  validate_dns_manifest_snapshot_live_set "${snapshot_file}" || return $?
  snapshot_names_text="$(dns_manifest_snapshot_query "${snapshot_file}" names)" || return $?
  [[ -n "$(trim_field "${snapshot_names_text}")" ]] || {
    error "DNS manifest snapshot contains no daemonsets to restore"
    return 1
  }
  while IFS= read -r daemonset_name; do
    daemonset_name="$(trim_field "${daemonset_name}")"
    [[ -n "${daemonset_name}" ]] || continue
    snapshot_names+=("${daemonset_name}")
    restored_cohorts+=("")
  done <<<"${snapshot_names_text}"
  (( ${#snapshot_names[@]} > 0 )) || {
    error "DNS manifest snapshot daemonset enumeration produced an empty restore cohort"
    return 1
  }
  for (( index=${#snapshot_names[@]} - 1; index >= 0; index-=1 )); do
    daemonset_name="${snapshot_names[${index}]}"
    if ! restore_dns_manifest_daemonset "${snapshot_file}" "${daemonset_name}"; then
      error "failed to restore DNS daemonset ${daemonset_name} from the pre-Helm snapshot"
      restore_failed=true
    elif [[ -z "$(trim_field "${DNS_MANIFEST_LAST_RESTORED_POD_COHORT}")" ]]; then
      error "DNS daemonset ${daemonset_name} restore did not pin its exact Pod cohort"
      restore_failed=true
    else
      restored_cohorts[${index}]="${DNS_MANIFEST_LAST_RESTORED_POD_COHORT}"
    fi
  done
  if ! run_smoke_urls; then
    error "public smoke failed after restoring the pre-Helm DNS manifest snapshot"
    restore_failed=true
  fi
  for index in "${!snapshot_names[@]}"; do
    daemonset_name="${snapshot_names[${index}]}"
    if ! dns_manifest_daemonset_matches_snapshot "${snapshot_file}" "${daemonset_name}"; then
      error "DNS daemonset ${daemonset_name} drifted after snapshot restore"
      restore_failed=true
      continue
    fi
    if ! dns_manifest_daemonset_serving_revision_matches_snapshot "${snapshot_file}" "${daemonset_name}"; then
      error "DNS daemonset ${daemonset_name} serving cohort drifted after snapshot restore"
      restore_failed=true
      continue
    fi
    snapshot_revision="$(dns_manifest_snapshot_query "${snapshot_file}" serving-revision "${daemonset_name}")" || {
      error "could not read restored revision for DNS daemonset ${daemonset_name}"
      restore_failed=true
      continue
    }
    if ! verify_dns_manifest_daemonset_pod_cohort \
      "${snapshot_file}" "${daemonset_name}" "${snapshot_revision}" "${restored_cohorts[${index}]}"; then
      error "DNS daemonset ${daemonset_name} exact Pod cohort drifted during rollback smoke"
      restore_failed=true
    fi
  done
  [[ "${restore_failed}" == "false" ]] || return 1
  log "pre-Helm DNS manifest snapshot restored and verified"
}

abort_dns_manifest_release() {
  local reason="$1"
  local snapshot_file="$2"

  error "${reason}; restoring the complete pre-Helm DNS manifest snapshot"
  if ! restore_dns_manifest_snapshot "${snapshot_file}"; then
    error "DNS manifest rollback verification failed; release requires operator review"
    return 1
  fi
  error "DNS manifest release aborted; old templates, authoritative answers, and public smoke were restored"
  return 1
}

dns_rollback_patch_for_daemonset() {
  local daemonset_name="$1"
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
spec = doc.get("spec") or {}
template = spec.get("template")
if not isinstance(template, dict):
    raise SystemExit("DNS daemonset has no pod template")
update_strategy = spec.get("updateStrategy")
if not isinstance(update_strategy, dict):
    update_strategy = {"type": "RollingUpdate"}
patch = [
    {"op": "replace", "path": "/spec/updateStrategy", "value": update_strategy},
    {"op": "replace", "path": "/spec/template", "value": template},
]
print(json.dumps(patch, separators=(",", ":")))
'
}

restore_dns_daemonset() {
  local daemonset_name="$1"
  local rollback_patch="$2"
  local original_uids="$3"
  local current_uids
  local replacement_uids

  log "restoring DNS daemonset ${daemonset_name} to its pinned pre-release template"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    return 0
  fi
  kubectl_cmd -n "${FUGUE_NAMESPACE}" patch "ds/${daemonset_name}" --type=json -p "${rollback_patch}" >/dev/null || return $?
  wait_daemonset_observed "${daemonset_name}" || return $?
  if ! current_uids="$(daemonset_pod_uids "${daemonset_name}")"; then
    return 1
  fi
  if [[ -n "$(trim_field "${original_uids}")" && "${current_uids}" == "${original_uids}" ]]; then
    log "DNS daemonset ${daemonset_name} still has its original pod UID set; rollback will not restart it"
    wait_daemonset_ready "${daemonset_name}" || return $?
    check_authoritative_dns_on_nodes "${daemonset_name}" || return $?
    return 0
  fi
  replacement_uids="${current_uids}"
  delete_daemonset_pods_no_wait "${daemonset_name}" "DNS rollback" || return $?
  wait_daemonset_replaced_and_ready "${daemonset_name}" "${replacement_uids}" || return $?
  check_authoritative_dns_on_nodes "${daemonset_name}" || return $?
}

rollback_dns_daemonsets() {
  local rollback_state=("$@")
  local rollback_failed=false
  local index
  local daemonset_name
  local rollback_patch
  local original_uids

  if (( ${#rollback_state[@]} % 3 != 0 )); then
    error "invalid DNS rollback state"
    return 1
  fi
  log "restoring $(( ${#rollback_state[@]} / 3 )) DNS daemonset(s) in reverse order"
  for (( index=${#rollback_state[@]} - 3; index >= 0; index-=3 )); do
    daemonset_name="${rollback_state[${index}]}"
    rollback_patch="${rollback_state[$((index + 1))]}"
    original_uids="${rollback_state[$((index + 2))]}"
    if ! restore_dns_daemonset "${daemonset_name}" "${rollback_patch}" "${original_uids}"; then
      error "failed to restore DNS daemonset ${daemonset_name}"
      rollback_failed=true
    fi
  done
  if ! run_smoke_urls; then
    error "public smoke failed after restoring DNS daemonsets"
    rollback_failed=true
  fi
  if [[ "${rollback_failed}" == "true" ]]; then
    return 1
  fi
  log "DNS daemonset rollback completed and public smoke recovered"
}

abort_dns_release() {
  local reason="$1"
  shift

  error "${reason}; aborting DNS release and restoring pinned templates"
  if ! rollback_dns_daemonsets "$@"; then
    error "DNS rollback verification failed; release remains failed and requires operator review"
    return 1
  fi
  error "DNS release aborted; authoritative answers and public smoke were restored"
  return 1
}

verify_dns_manifest_daemonset_target_state() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  local expected_state="$3"
  local target_identity="$4"
  local expected_replacement_cohort="${5:-}"
  local live_state
  local expected_revision
  local expected_pod_cohort

  dns_manifest_daemonset_matches_target_identity "${daemonset_name}" "${target_identity}" || return $?
  live_state="$(dns_manifest_daemonset_state "${snapshot_file}" "${daemonset_name}")" || return $?
  [[ "${live_state}" == "${expected_state}" ]] || {
    error "DNS daemonset ${daemonset_name} manifest state drifted from ${expected_state} to ${live_state}"
    return 1
  }
  if [[ "${expected_state}" == "template-changed" ]]; then
    expected_revision="$(dns_manifest_target_identity_revision "${target_identity}")" || return $?
    expected_pod_cohort="${expected_replacement_cohort}"
  else
    expected_revision="$(dns_manifest_snapshot_query "${snapshot_file}" serving-revision "${daemonset_name}")" || return $?
    expected_pod_cohort="${expected_replacement_cohort}"
    if [[ -z "$(trim_field "${expected_pod_cohort}")" ]]; then
      expected_pod_cohort="$(dns_manifest_snapshot_query "${snapshot_file}" pod-cohort "${daemonset_name}")" || return $?
    fi
  fi
  verify_dns_manifest_daemonset_pod_cohort \
    "${snapshot_file}" "${daemonset_name}" "${expected_revision}" "${expected_pod_cohort}"
}

validate_dns_manifest_target_transport() {
  local snapshot_file="$1"
  local daemonset_name="$2"
  local expected_nodes
  local node_name
  local daemonset_json
  local node_json

  : "${FUGUE_DNS_CONTAINER_NAME:?FUGUE_DNS_CONTAINER_NAME is required for DNS manifest validation}"
  : "${FUGUE_DNS_UDP_CONTAINER_PORT:?FUGUE_DNS_UDP_CONTAINER_PORT is required for DNS manifest validation}"
  : "${FUGUE_DNS_TCP_CONTAINER_PORT:?FUGUE_DNS_TCP_CONTAINER_PORT is required for DNS manifest validation}"
  : "${FUGUE_DNS_UDP_ADDR:?FUGUE_DNS_UDP_ADDR is required for DNS manifest validation}"
  : "${FUGUE_DNS_TCP_ADDR:?FUGUE_DNS_TCP_ADDR is required for DNS manifest validation}"
  expected_nodes="$(dns_manifest_snapshot_query "${snapshot_file}" nodes "${daemonset_name}")" || return $?
  if [[ "$(printf '%s\n' "${expected_nodes}" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')" != "1" ]]; then
    error "DNS daemonset ${daemonset_name} transport validation requires exactly one pinned node"
    return 1
  fi
  node_name="$(trim_field "${expected_nodes}")"
  daemonset_json="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "ds/${daemonset_name}" -o json)" || return $?
  node_json="$(kubectl_cmd get "node/${node_name}" -o json)" || return $?
  DAEMONSET_JSON="${daemonset_json}" NODE_JSON="${node_json}" python3 - \
    "${daemonset_name}" "${node_name}" "${FUGUE_DNS_CONTAINER_NAME}" \
    "${FUGUE_DNS_UDP_CONTAINER_PORT}" "${FUGUE_DNS_TCP_CONTAINER_PORT}" \
    "${FUGUE_DNS_UDP_ADDR}" "${FUGUE_DNS_TCP_ADDR}" <<'PY'
import ipaddress
import json
import os
import sys

ds_name, node_name, expected_name, udp_text, tcp_text, udp_addr, tcp_addr = sys.argv[1:8]
udp_port = int(udp_text)
tcp_port = int(tcp_text)
daemonset = json.loads(os.environ["DAEMONSET_JSON"])
node = json.loads(os.environ["NODE_JSON"])
external_ipv4s = []
for address in (node.get("status") or {}).get("addresses") or []:
    if address.get("type") != "ExternalIP":
        continue
    value = str(address.get("address") or "").strip()
    try:
        parsed = ipaddress.ip_address(value)
    except ValueError:
        continue
    if parsed.version == 4:
        external_ipv4s.append(value)
external_ipv4s = sorted(set(external_ipv4s))
if len(external_ipv4s) != 1:
    raise SystemExit(
        f"DNS daemonSet {ds_name} node {node_name} has {len(external_ipv4s)} ExternalIPv4 addresses; expected 1"
    )
expected_host_ip = external_ipv4s[0]
containers = ((daemonset.get("spec") or {}).get("template") or {}).get("spec", {}).get("containers") or []
semantic = []
for container in containers:
    command = container.get("command") or []
    env_names = {entry.get("name") for entry in container.get("env") or []}
    if command == ["/usr/local/bin/fugue-dns"] and "FUGUE_DNS_ZONE" in env_names:
        semantic.append(container)
if len(semantic) != 1:
    raise SystemExit(f"DNS daemonSet {ds_name} must have exactly one semantic fugue-dns container")
container = semantic[0]
if container.get("name") != expected_name:
    raise SystemExit(
        f"DNS daemonSet {ds_name} container name mismatch: expected={expected_name} actual={container.get('name')}"
    )
ports = container.get("ports") or []
by_name = {}
for port in ports:
    name = port.get("name")
    if not isinstance(name, str) or name in by_name:
        raise SystemExit(f"DNS daemonSet {ds_name} has an unnamed or duplicate container port")
    by_name[name] = port
if set(by_name) != {"http", "dns-udp", "dns-tcp"}:
    raise SystemExit(f"DNS daemonSet {ds_name} must expose exactly http, dns-udp, and dns-tcp")
http = by_name["http"]
udp = by_name["dns-udp"]
tcp = by_name["dns-tcp"]
if http.get("containerPort") != 7834 or http.get("protocol") != "TCP":
    raise SystemExit(f"DNS daemonSet {ds_name} has an invalid HTTP health port")
for label, entry, protocol, container_port in (
    ("UDP", udp, "UDP", udp_port),
    ("TCP", tcp, "TCP", tcp_port),
):
    if (
        entry.get("protocol") != protocol
        or entry.get("containerPort") != container_port
        or entry.get("hostPort") != 53
        or entry.get("hostIP") != expected_host_ip
    ):
        raise SystemExit(
            f"DNS daemonSet {ds_name} {label} mapping mismatch: "
            f"expected={expected_host_ip}:53->{container_port}/{protocol} actual={entry}"
        )
container_ports = [http.get("containerPort"), udp.get("containerPort"), tcp.get("containerPort")]
legacy = expected_name == "dns" and udp_port == 53 and tcp_port == 53
if not legacy and len(container_ports) != len(set(container_ports)):
    raise SystemExit(f"DNS daemonSet {ds_name} container ports are not unique")
env_values = {}
for entry in container.get("env") or []:
    name = entry.get("name")
    if name in {"FUGUE_DNS_UDP_ADDR", "FUGUE_DNS_TCP_ADDR"}:
        if name in env_values or not isinstance(entry.get("value"), str):
            raise SystemExit(f"DNS daemonSet {ds_name} has a duplicate or non-literal {name}")
        env_values[name] = entry["value"]
expected_env = {"FUGUE_DNS_UDP_ADDR": udp_addr, "FUGUE_DNS_TCP_ADDR": tcp_addr}
if env_values != expected_env:
    raise SystemExit(
        f"DNS daemonSet {ds_name} bind address mismatch: expected={expected_env} actual={env_values}"
    )
PY
}

check_public_smoke_on_front_nodes() {
  local front_daemonset="$1"
  local urls="${FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS:-}"
  local url
  local host
  local path
  local host_ip
  local active_attempts="${FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SMOKE_ATTEMPTS:-${FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS:-3}}"
  local active_delay_seconds="${FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SMOKE_RETRY_DELAY_SECONDS:-${FUGUE_PUBLIC_DATA_PLANE_SMOKE_RETRY_DELAY_SECONDS:-2}}"

  [[ -n "$(trim_field "${urls}")" ]] || return 0
  while IFS= read -r url; do
    url="$(trim_field "${url}")"
    [[ -n "${url}" ]] || continue
    host="$(python3 -c 'from urllib.parse import urlsplit; import sys; print(urlsplit(sys.argv[1]).hostname or "")' "${url}")"
    path="$(python3 -c 'from urllib.parse import urlsplit; import sys; p=urlsplit(sys.argv[1]); path=p.path or "/"; print(path + (("?" + p.query) if p.query else ""))' "${url}")"
    if [[ -z "$(trim_field "${host}")" ]]; then
      error "front smoke URL must include a hostname: ${url}"
      return 1
    fi
    while IFS= read -r host_ip; do
      host_ip="$(trim_field "${host_ip}")"
      [[ -n "${host_ip}" ]] || continue
      log "checking front HTTPS smoke ${host_ip}:443 host=${host} path=${path}"
      if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
        continue
      fi
      FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS="${active_attempts}" \
        FUGUE_PUBLIC_DATA_PLANE_SMOKE_RETRY_DELAY_SECONDS="${active_delay_seconds}" \
        smoke_curl_with_retry "front ${host_ip}:443 host=${host} path=${path}" \
        -fsS --max-time "${FUGUE_PUBLIC_DATA_PLANE_SMOKE_TIMEOUT_SECONDS:-10}" \
        --resolve "${host}:443:${host_ip}" \
        "https://${host}${path}" >/dev/null || return $?
    done < <(node_ips_for_daemonset "${front_daemonset}")
  done < <(public_data_plane_smoke_urls)
}

rollback_bluegreen_fronts() {
  local switch_state=("$@")
  local rollback_failed=false
  local index
  local front_ds
  local original_slot
  local front_count

  if (( ${#switch_state[@]} % 2 != 0 )); then
    error "invalid blue/green rollback state"
    return 1
  fi
  front_count=$(( ${#switch_state[@]} / 2 ))
  log "restoring ${front_count} blue/green front slot(s) in reverse order"
  for (( index=${#switch_state[@]} - 2; index >= 0; index-=2 )); do
    front_ds="${switch_state[${index}]}"
    original_slot="${switch_state[$((index + 1))]}"
    if ! write_front_active_slot "${front_ds}" "${original_slot}"; then
      error "failed to restore ${front_ds} to slot ${original_slot}"
      rollback_failed=true
      continue
    fi
    if ! wait_daemonset_ready "${front_ds}"; then
      error "front ${front_ds} did not become ready after restoring slot ${original_slot}"
      rollback_failed=true
    fi
  done
  for (( index=0; index < ${#switch_state[@]}; index+=2 )); do
    front_ds="${switch_state[${index}]}"
    original_slot="${switch_state[$((index + 1))]}"
    if ! check_public_smoke_on_front_nodes "${front_ds}"; then
      error "front smoke failed for ${front_ds} after restoring slot ${original_slot}"
      rollback_failed=true
    fi
  done
  if [[ "${rollback_failed}" == "true" ]]; then
    return 1
  fi
  log "blue/green front slot restore completed"
}

abort_bluegreen_release() {
  local reason="$1"
  shift
  local rollback_failed=false

  error "${reason}; aborting blue/green release and restoring original slots"
  if ! rollback_bluegreen_fronts "$@"; then
    rollback_failed=true
  fi
  if ! run_smoke_urls; then
    error "public smoke still fails after blue/green slot restore"
    rollback_failed=true
  fi
  if [[ "${rollback_failed}" == "true" ]]; then
    error "blue/green rollback verification failed; release remains failed and requires operator review"
  else
    log "blue/green release aborted; original slots and public smoke were restored"
  fi
  return 1
}

run_bluegreen_release() {
  local bases=()
  local prepared_bases=()
  local prepared_fronts=()
  local prepared_original_slots=()
  local prepared_slots=()
  local rollback_state=()
  local base front_ds active inactive inactive_ds active_ds inactive_port
  local protected_before protected_after
  local active_slots_json="{}"
  local index

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
    if [[ "$(daemonset_desired_count "${front_ds}")" == "0" ]]; then
      log "skipping ${base}; front desired=0"
      continue
    fi
    active="$(current_active_slot "${base}" "${front_ds}")"
    inactive="$(other_slot "${active}")"
    active_ds="$(bluegreen_worker_daemonset_name "${base}" "${active}")"
    inactive_ds="$(bluegreen_worker_daemonset_name "${base}" "${inactive}")"
    log "${base}: active=${active} inactive=${inactive}"

    protected_before="$(capture_daemonset_pods "${front_ds}" "${active_ds}")"
    patch_inactive_worker "${inactive_ds}" || return $?
    delete_worker_pods "${inactive_ds}" || return $?
    wait_daemonset_ready "${inactive_ds}" || return $?
    inactive_port="$(worker_https_port "${inactive_ds}")"
    check_worker_tcp "${inactive_ds}" "${inactive_port}" || return $?
    check_worker_https_smoke "${inactive_ds}" "${inactive_port}" || return $?
    protected_after="$(capture_daemonset_pods "${front_ds}" "${active_ds}")"
    if [[ "${protected_before}" != "${protected_after}" ]]; then
      printf '%s\n' "${protected_before}" >/tmp/fugue-public-data-plane-protected-before.txt
      printf '%s\n' "${protected_after}" >/tmp/fugue-public-data-plane-protected-after.txt
      diff -u /tmp/fugue-public-data-plane-protected-before.txt /tmp/fugue-public-data-plane-protected-after.txt || true
      fail "front or active worker pod set changed while upgrading inactive worker ${inactive_ds}"
    fi
    prepared_bases+=("${base}")
    prepared_fronts+=("${front_ds}")
    prepared_original_slots+=("${active}")
    prepared_slots+=("${inactive}")
  done

  log "all ${#prepared_bases[@]} scheduled blue/green candidate(s) passed pre-switch validation"
  for index in "${!prepared_bases[@]}"; do
    base="${prepared_bases[${index}]}"
    front_ds="${prepared_fronts[${index}]}"
    active="${prepared_original_slots[${index}]}"
    inactive="${prepared_slots[${index}]}"
    rollback_state+=("${front_ds}" "${active}")
    if ! write_front_active_slot "${front_ds}" "${inactive}"; then
      abort_bluegreen_release "failed to switch ${front_ds} to slot ${inactive}" "${rollback_state[@]}"
      return 1
    fi
    if ! wait_daemonset_ready "${front_ds}"; then
      abort_bluegreen_release "front ${front_ds} was not ready after switching to slot ${inactive}" "${rollback_state[@]}"
      return 1
    fi
    if ! check_public_smoke_on_front_nodes "${front_ds}"; then
      abort_bluegreen_release "front smoke failed for ${front_ds} on slot ${inactive}" "${rollback_state[@]}"
      return 1
    fi
    if ! active_slots_json="$(record_active_slot_json "${active_slots_json}" "${base}" "${inactive}")"; then
      abort_bluegreen_release "could not record proposed active slot for ${base}" "${rollback_state[@]}"
      return 1
    fi
  done
  if ! run_smoke_urls; then
    abort_bluegreen_release "public smoke failed after all front slot switches" "${rollback_state[@]}"
    return 1
  fi
  FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOTS_JSON="${active_slots_json}"
}

run_front_ondelete_release() {
  local bases=()
  local active_bases=()
  local base
  local front_ds

  enable_bluegreen_chart_mode
  while IFS= read -r base; do
    base="$(trim_field "${base}")"
    [[ -n "${base}" ]] || continue
    bases+=("${base}")
  done < <(bluegreen_all_bases_array)
  if (( ${#bases[@]} == 0 )); then
    fail "no edge blue/green front DaemonSets found; enable edge.blueGreen.enabled first"
  fi
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_FRONT_RESTART_CONFIRM}" != "true" ]]; then
    fail "front-ondelete requires FUGUE_PUBLIC_DATA_PLANE_FRONT_RESTART_CONFIRM=true because front pods own public 80/443"
  fi

  log "front-ondelete release_id=${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID} namespace=${FUGUE_NAMESPACE} bases=${bases[*]}"
  for base in "${bases[@]}"; do
    require_bluegreen_base_complete "${base}"
    wait_bluegreen_base_ready "${base}"
    front_ds="$(bluegreen_front_daemonset_name "${base}")"
    if [[ "$(daemonset_desired_count "${front_ds}")" == "0" ]]; then
      log "skipping front-ondelete for ${base}; front desired=0"
      continue
    fi
    active_bases+=("${base}")
    patch_front_template "${front_ds}"
    replace_daemonset_pods_one_at_a_time "${front_ds}" "front" "true"
  done
  FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOTS_JSON="$(collect_current_active_slots_json "${active_bases[@]}")"
}

run_dns_ondelete_release() {
  local daemonsets=()
  local daemonset_name
  local before_uids
  local rollback_patch
  local prepared_state=()
  local rollback_state=()
  local index

  while IFS= read -r daemonset_name; do
    daemonset_name="$(trim_field "${daemonset_name}")"
    [[ -n "${daemonset_name}" ]] || continue
    daemonsets+=("${daemonset_name}")
  done < <(dns_daemonset_names)
  if (( ${#daemonsets[@]} == 0 )); then
    fail "no DNS public data-plane DaemonSets found in namespace ${FUGUE_NAMESPACE}"
  fi

  log "dns-ondelete release_id=${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID} namespace=${FUGUE_NAMESPACE} daemonsets=${daemonsets[*]}"
  for daemonset_name in "${daemonsets[@]}"; do
    if ! wait_daemonset_ready "${daemonset_name}"; then
      error "DNS daemonset ${daemonset_name} failed preflight readiness; no DNS mutation was attempted"
      return 1
    fi
  done
  for daemonset_name in "${daemonsets[@]}"; do
    if ! rollback_patch="$(dns_rollback_patch_for_daemonset "${daemonset_name}")"; then
      error "could not pin DNS rollback template for ${daemonset_name}; no DNS mutation was attempted"
      return 1
    fi
    if ! before_uids="$(daemonset_pod_uids "${daemonset_name}")"; then
      error "could not pin DNS pod UID set for ${daemonset_name}; no DNS mutation was attempted"
      return 1
    fi
    prepared_state+=("${daemonset_name}" "${rollback_patch}" "${before_uids}")
  done
  log "pinned rollback templates and pod UID sets for all ${#daemonsets[@]} DNS daemonset(s) before mutation"
  for index in "${!daemonsets[@]}"; do
    daemonset_name="${prepared_state[$((index * 3))]}"
    rollback_patch="${prepared_state[$((index * 3 + 1))]}"
    before_uids="${prepared_state[$((index * 3 + 2))]}"
    rollback_state+=("${daemonset_name}" "${rollback_patch}" "${before_uids}")
    if ! patch_dns_template "${daemonset_name}"; then
      abort_dns_release "failed to patch DNS template ${daemonset_name}" "${rollback_state[@]}" || return 1
    fi
    if ! delete_daemonset_pods_no_wait "${daemonset_name}" "dns"; then
      abort_dns_release "failed to replace DNS pods for ${daemonset_name}" "${rollback_state[@]}" || return 1
    fi
    if ! wait_daemonset_replaced_and_ready "${daemonset_name}" "${before_uids}"; then
      abort_dns_release "DNS daemonset ${daemonset_name} did not become ready" "${rollback_state[@]}" || return 1
    fi
    if ! check_authoritative_dns_on_nodes "${daemonset_name}"; then
      abort_dns_release "authoritative DNS validation failed for ${daemonset_name}" "${rollback_state[@]}" || return 1
    fi
  done
  if ! run_smoke_urls; then
    abort_dns_release "public smoke failed after all DNS daemonsets were replaced" "${rollback_state[@]}" || return 1
  fi
}

run_dns_manifest_ondelete_release() {
  local snapshot_file="${1:-${FUGUE_PUBLIC_DATA_PLANE_DNS_SNAPSHOT_FILE:-}}"
  local snapshot_namespace
  local daemonsets=()
  local daemonset_states=()
  local daemonset_target_identities=()
  local daemonset_pod_cohorts=()
  local replacement_daemonsets=()
  local daemonset_name
  local state
  local target_identity
  local target_revision
  local controlled_cohort
  local original_uids
  local current_uids
  local snapshot_names_text

  snapshot_file="$(trim_field "${snapshot_file}")"
  [[ -n "${snapshot_file}" ]] || {
    error "FUGUE_PUBLIC_DATA_PLANE_DNS_SNAPSHOT_FILE is required for dns-manifest-ondelete"
    return 1
  }
  dns_manifest_snapshot_query "${snapshot_file}" validate || return $?
  snapshot_namespace="$(dns_manifest_snapshot_query "${snapshot_file}" namespace)" || return $?
  if [[ "${snapshot_namespace}" != "${FUGUE_NAMESPACE}" ]]; then
    error "DNS manifest snapshot namespace ${snapshot_namespace} does not match ${FUGUE_NAMESPACE}"
    return 1
  fi
  validate_dns_manifest_snapshot_live_set "${snapshot_file}" || return $?
  snapshot_names_text="$(dns_manifest_snapshot_query "${snapshot_file}" names)" || return $?
  [[ -n "$(trim_field "${snapshot_names_text}")" ]] || {
    error "DNS manifest snapshot contains no daemonsets to reconcile"
    return 1
  }
  while IFS= read -r daemonset_name; do
    daemonset_name="$(trim_field "${daemonset_name}")"
    [[ -n "${daemonset_name}" ]] || continue
    daemonsets+=("${daemonset_name}")
  done <<<"${snapshot_names_text}"
  (( ${#daemonsets[@]} > 0 )) || {
    error "DNS manifest snapshot daemonset enumeration produced an empty release cohort"
    return 1
  }

  log "dns-manifest-ondelete release_id=${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID} namespace=${FUGUE_NAMESPACE} daemonsets=${daemonsets[*]}"
  for daemonset_name in "${daemonsets[@]}"; do
    if ! target_identity="$(wait_dns_manifest_daemonset_target_identity "${daemonset_name}")"; then
      error "could not pin DNS daemonset ${daemonset_name} post-Helm target identity"
      return 1
    fi
    if ! state="$(dns_manifest_daemonset_state "${snapshot_file}" "${daemonset_name}")"; then
      error "could not compare DNS daemonset ${daemonset_name} with its pre-Helm snapshot"
      return 1
    fi
    if ! dns_manifest_daemonset_matches_target_identity "${daemonset_name}" "${target_identity}"; then
      error "DNS daemonset ${daemonset_name} changed while its post-Helm target identity was being pinned"
      return 1
    fi
    if ! validate_dns_manifest_target_transport "${snapshot_file}" "${daemonset_name}"; then
      error "DNS daemonset ${daemonset_name} post-Helm transport does not match the intended migration target"
      return 1
    fi
    case "${state}" in
      unchanged)
        log "DNS daemonset ${daemonset_name} is unchanged by Helm; its pods will not be replaced"
        ;;
      strategy-only)
        log "DNS daemonset ${daemonset_name} changed only updateStrategy; its pods will not be replaced"
        ;;
      template-changed)
        replacement_daemonsets+=("${daemonset_name}")
        ;;
      *)
        error "invalid DNS daemonset manifest comparison state for ${daemonset_name}: ${state}"
        return 1
        ;;
    esac
    daemonset_states+=("${state}")
    daemonset_target_identities+=("${target_identity}")
    daemonset_pod_cohorts+=("")
  done

  local index
  for index in "${!daemonsets[@]}"; do
    daemonset_name="${daemonsets[${index}]}"
    if ! wait_daemonset_ready "${daemonset_name}"; then
      abort_dns_manifest_release "DNS daemonset ${daemonset_name} failed post-Helm readiness" "${snapshot_file}" || return 1
    fi
    if ! dns_manifest_daemonset_matches_target_identity "${daemonset_name}" "${daemonset_target_identities[${index}]}"; then
      abort_dns_manifest_release "DNS daemonset ${daemonset_name} post-Helm target identity drifted during transaction preflight" "${snapshot_file}" || return 1
    fi
    state="$(dns_manifest_daemonset_state "${snapshot_file}" "${daemonset_name}")" || {
      abort_dns_manifest_release "DNS daemonset ${daemonset_name} changed during transaction preflight" "${snapshot_file}" || return 1
    }
    if [[ "${state}" != "${daemonset_states[${index}]}" ]]; then
      abort_dns_manifest_release "DNS daemonset ${daemonset_name} manifest changed during transaction preflight" "${snapshot_file}" || return 1
    fi
    if ! dns_manifest_daemonset_matches_target_identity "${daemonset_name}" "${daemonset_target_identities[${index}]}"; then
      abort_dns_manifest_release "DNS daemonset ${daemonset_name} post-Helm target identity drifted during transaction preflight" "${snapshot_file}" || return 1
    fi
    if [[ "${state}" != "unchanged" ]] && ! dns_manifest_daemonset_uses_ondelete "${daemonset_name}"; then
      abort_dns_manifest_release "DNS daemonset ${daemonset_name} is not OnDelete after Helm" "${snapshot_file}" || return 1
    fi
    original_uids="$(dns_manifest_snapshot_query "${snapshot_file}" uids "${daemonset_name}")" || {
      abort_dns_manifest_release "could not read pre-Helm pod UIDs for ${daemonset_name}" "${snapshot_file}" || return 1
    }
    current_uids="$(daemonset_pod_uids "${daemonset_name}")" || {
      abort_dns_manifest_release "could not read live pod UIDs for ${daemonset_name}" "${snapshot_file}" || return 1
    }
    if [[ "${current_uids}" != "${original_uids}" ]]; then
      abort_dns_manifest_release "DNS daemonset ${daemonset_name} pods changed before the controlled OnDelete replacement" "${snapshot_file}" || return 1
    fi
  done

  for index in "${!daemonsets[@]}"; do
    [[ "${daemonset_states[${index}]}" == "template-changed" ]] || continue
    daemonset_name="${daemonsets[${index}]}"
    if ! replace_dns_manifest_daemonset "${snapshot_file}" "${daemonset_name}" "${daemonset_target_identities[${index}]}"; then
      abort_dns_manifest_release "DNS manifest replacement or verification failed for ${daemonset_name}" "${snapshot_file}" || return 1
    fi
    target_revision="$(dns_manifest_target_identity_revision "${daemonset_target_identities[${index}]}")" || {
      abort_dns_manifest_release "could not read controlled target revision for ${daemonset_name}" "${snapshot_file}" || return 1
    }
    controlled_cohort="${DNS_MANIFEST_LAST_CONTROLLED_POD_COHORT}"
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" != "true" && -z "$(trim_field "${controlled_cohort}")" ]]; then
      abort_dns_manifest_release "controlled replacement did not return a pinned Pod cohort for ${daemonset_name}" "${snapshot_file}" || return 1
    fi
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" != "true" ]] &&
      ! verify_dns_manifest_daemonset_pod_cohort \
        "${snapshot_file}" "${daemonset_name}" "${target_revision}" "${controlled_cohort}"; then
      abort_dns_manifest_release "controlled replacement Pod cohort drifted before transaction handoff for ${daemonset_name}" "${snapshot_file}" || return 1
    fi
    daemonset_pod_cohorts[${index}]="${controlled_cohort}"
  done
  for index in "${!daemonsets[@]}"; do
    [[ -z "$(trim_field "${daemonset_pod_cohorts[${index}]}")" ]] || continue
    daemonset_name="${daemonsets[${index}]}"
    target_revision="$(dns_manifest_snapshot_query "${snapshot_file}" serving-revision "${daemonset_name}")" || {
      abort_dns_manifest_release "could not read the pinned serving revision for ${daemonset_name}" "${snapshot_file}" || return 1
    }
    controlled_cohort="$(dns_manifest_snapshot_query "${snapshot_file}" pod-cohort "${daemonset_name}")" || {
      abort_dns_manifest_release "could not read the pinned pre-Helm Pod cohort for ${daemonset_name}" "${snapshot_file}" || return 1
    }
    controlled_cohort="$(printf '%s\n' "${controlled_cohort}" | canonicalize_dns_manifest_pod_records \
      "${daemonset_name}" "${target_revision}" \
      "$(dns_manifest_snapshot_query "${snapshot_file}" nodes "${daemonset_name}")")" || {
      abort_dns_manifest_release "could not canonicalize the pinned pre-Helm Pod cohort for ${daemonset_name}" "${snapshot_file}" || return 1
    }
    daemonset_pod_cohorts[${index}]="${controlled_cohort}"
  done
  validate_dns_manifest_snapshot_live_set "${snapshot_file}" || {
    abort_dns_manifest_release "DNS daemonset set drifted before final verification" "${snapshot_file}" || return 1
  }
  for index in "${!daemonsets[@]}"; do
    daemonset_name="${daemonsets[${index}]}"
    if ! verify_dns_manifest_daemonset_target_state "${snapshot_file}" "${daemonset_name}" \
      "${daemonset_states[${index}]}" "${daemonset_target_identities[${index}]}" \
      "${daemonset_pod_cohorts[${index}]}"; then
      abort_dns_manifest_release "DNS daemonset ${daemonset_name} target state failed final verification" "${snapshot_file}" || return 1
    fi
    if ! check_authoritative_dns_on_nodes "${daemonset_name}"; then
      abort_dns_manifest_release "final authoritative DNS validation failed for ${daemonset_name}" "${snapshot_file}" || return 1
    fi
  done
  if ! run_smoke_urls; then
    abort_dns_manifest_release "final public smoke failed after DNS manifest reconcile" "${snapshot_file}" || return 1
  fi
  validate_dns_manifest_snapshot_live_set "${snapshot_file}" || {
    abort_dns_manifest_release "DNS daemonset set drifted after final public smoke" "${snapshot_file}" || return 1
  }
  for index in "${!daemonsets[@]}"; do
    daemonset_name="${daemonsets[${index}]}"
    if ! verify_dns_manifest_daemonset_target_state "${snapshot_file}" "${daemonset_name}" \
      "${daemonset_states[${index}]}" "${daemonset_target_identities[${index}]}" \
      "${daemonset_pod_cohorts[${index}]}"; then
      abort_dns_manifest_release "DNS daemonset ${daemonset_name} drifted during final public smoke" "${snapshot_file}" || return 1
    fi
  done
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" != "true" &&
    -n "$(trim_field "${FUGUE_PUBLIC_DATA_PLANE_DNS_TARGET_STATE_FILE:-}")" ]]; then
    local target_state_args=()
    for index in "${!daemonsets[@]}"; do
      target_state_args+=(
        "${daemonsets[${index}]}"
        "${daemonset_states[${index}]}"
        "${daemonset_target_identities[${index}]}"
        "${daemonset_pod_cohorts[${index}]}"
      )
    done
    if ! write_dns_manifest_target_state "${snapshot_file}" \
      "${FUGUE_PUBLIC_DATA_PLANE_DNS_TARGET_STATE_FILE}" "${target_state_args[@]}"; then
      abort_dns_manifest_release "could not write the exact DNS target-state handoff" "${snapshot_file}" || return 1
    fi
  fi
  log "DNS manifest OnDelete transaction completed; replaced_daemonsets=${#replacement_daemonsets[@]}"
}

write_release_record() {
  local daemonsets_csv="$1"
  local release_record_name="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_RECORD_NAME:-${FUGUE_RELEASE_FULLNAME}-public-data-plane-release}"
  local active_slots_json="${FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOTS_JSON:-}"
  local record_json
  local labeled_record_json

  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    log "dry-run: skipping release record ${release_record_name}"
    return 0
  fi
  if [[ -z "$(trim_field "${active_slots_json}")" ]]; then
    if ! active_slots_json="$(release_record_active_slots_json)"; then
      return 1
    fi
  fi

  record_json="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" create configmap "${release_record_name}" \
    --from-literal=release_id="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID}" \
    --from-literal=mode="${FUGUE_PUBLIC_DATA_PLANE_RECORD_MODE:-edge-template-ondelete}" \
    --from-literal=active_slots="${active_slots_json}" \
    --from-literal=daemonsets="${daemonsets_csv}" \
    --from-literal=edge_resources="${FUGUE_EDGE_RESOURCES_JSON}" \
    --from-literal=caddy_resources="${FUGUE_EDGE_CADDY_RESOURCES_JSON}" \
    --from-literal=git_sha="${GITHUB_SHA:-}" \
    --from-literal=recorded_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --dry-run=client -o json)" || return $?
  labeled_record_json="$(RECORD_JSON="${record_json}" RELEASE_INSTANCE="${FUGUE_RELEASE_INSTANCE}" python3 -c '
import json
import os

record = json.loads(os.environ["RECORD_JSON"])
metadata = record.setdefault("metadata", {})
labels = metadata.setdefault("labels", {})
labels.update({
    "app.kubernetes.io/instance": os.environ["RELEASE_INSTANCE"],
    "app.kubernetes.io/component": "public-data-plane-release",
    "fugue.io/rollout-subsystem": "public-data-plane",
})
print(json.dumps(record, separators=(",", ":"), sort_keys=True))
')" || return $?
  kubectl_cmd apply -f - <<<"${labeled_record_json}" >/dev/null || return $?
  log "wrote release record ${release_record_name}"
}

run_smoke_urls() {
  local urls="${FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS:-}"
  local url
  local active_attempts="${FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SMOKE_ATTEMPTS:-${FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS:-3}}"
  local active_delay_seconds="${FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SMOKE_RETRY_DELAY_SECONDS:-${FUGUE_PUBLIC_DATA_PLANE_SMOKE_RETRY_DELAY_SECONDS:-2}}"

  [[ -n "$(trim_field "${urls}")" ]] || return 0
  while IFS= read -r url; do
    url="$(trim_field "${url}")"
    [[ -n "${url}" ]] || continue
    log "smoke ${url}"
    FUGUE_PUBLIC_DATA_PLANE_SMOKE_ATTEMPTS="${active_attempts}" \
      FUGUE_PUBLIC_DATA_PLANE_SMOKE_RETRY_DELAY_SECONDS="${active_delay_seconds}" \
      smoke_curl_with_retry "public ${url}" \
      -fsS --max-time "${FUGUE_PUBLIC_DATA_PLANE_SMOKE_TIMEOUT_SECONDS:-10}" "${url}" >/dev/null || return $?
  done < <(public_data_plane_smoke_urls)
}

main() {
  local default_edge_resources
  local default_caddy_resources
  local default_dns_resources
  local daemonsets_csv
  local daemonset_name
  local public_daemonsets=()

  default_edge_resources='{"requests":{"cpu":"25m","memory":"128Mi","ephemeral-storage":"32Mi"},"limits":{"memory":"1Gi","ephemeral-storage":"256Mi"}}'
  default_caddy_resources='{"requests":{"cpu":"25m","memory":"128Mi","ephemeral-storage":"32Mi"},"limits":{"memory":"1Gi","ephemeral-storage":"256Mi"}}'
  default_dns_resources='{"requests":{"cpu":"25m","memory":"64Mi","ephemeral-storage":"32Mi"},"limits":{"memory":"256Mi","ephemeral-storage":"256Mi"}}'

  FUGUE_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
  FUGUE_RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"
  FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-fugue-fugue}"
  FUGUE_RELEASE_INSTANCE="${FUGUE_RELEASE_INSTANCE:-${FUGUE_RELEASE_NAME:-fugue}}"
  FUGUE_HELM_CHART_PATH="${FUGUE_HELM_CHART_PATH:-./deploy/helm/fugue}"
  FUGUE_HELM_TIMEOUT="${FUGUE_HELM_TIMEOUT:-10m}"
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN:-false}"
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY:-blue-green}"
  FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN="${FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN:-false}"
  FUGUE_PUBLIC_DATA_PLANE_FRONT_RESTART_CONFIRM="${FUGUE_PUBLIC_DATA_PLANE_FRONT_RESTART_CONFIRM:-false}"
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID:-pdp-$(date -u +%Y%m%dT%H%M%SZ)-${GITHUB_SHA:-local}}"
  FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT="${FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT:-a}"
  FUGUE_EDGE_BLUE_GREEN_ACTIVE_SLOT_FILE="${FUGUE_EDGE_BLUE_GREEN_ACTIVE_SLOT_FILE:-/var/lib/fugue/edge-blue-green/active-slot}"
  FUGUE_EDGE_RESOURCES_JSON="$(compact_json_object FUGUE_EDGE_RESOURCES_JSON "${FUGUE_EDGE_RESOURCES_JSON:-${default_edge_resources}}")"
  FUGUE_EDGE_CADDY_RESOURCES_JSON="$(compact_json_object FUGUE_EDGE_CADDY_RESOURCES_JSON "${FUGUE_EDGE_CADDY_RESOURCES_JSON:-${default_caddy_resources}}")"
  FUGUE_DNS_RESOURCES_JSON="$(compact_json_object FUGUE_DNS_RESOURCES_JSON "${FUGUE_DNS_RESOURCES_JSON:-${default_dns_resources}}")"
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID FUGUE_EDGE_RESOURCES_JSON FUGUE_EDGE_CADDY_RESOURCES_JSON FUGUE_DNS_RESOURCES_JSON
  export FUGUE_EDGE_IMAGE_REPOSITORY FUGUE_EDGE_IMAGE_TAG FUGUE_EDGE_CADDY_IMAGE_REPOSITORY FUGUE_EDGE_CADDY_IMAGE_TAG

  case "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" in
    true|false) ;;
    *) fail "FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN must be true or false" ;;
  esac
  case "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" in
    blue-green|front-ondelete|dns-ondelete|dns-manifest-ondelete|legacy-template-ondelete) ;;
    *) fail "FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY must be blue-green, front-ondelete, dns-ondelete, dns-manifest-ondelete, or legacy-template-ondelete" ;;
  esac
  case "${FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN}" in
    true|false) ;;
    *) fail "FUGUE_PUBLIC_DATA_PLANE_ENABLE_BLUE_GREEN must be true or false" ;;
  esac
  case "${FUGUE_PUBLIC_DATA_PLANE_FRONT_RESTART_CONFIRM}" in
    true|false) ;;
    *) fail "FUGUE_PUBLIC_DATA_PLANE_FRONT_RESTART_CONFIRM must be true or false" ;;
  esac
  case "${FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT}" in
    a|b) ;;
    *) fail "FUGUE_EDGE_BLUE_GREEN_DEFAULT_ACTIVE_SLOT must be a or b" ;;
  esac

  command_exists python3 || fail "python3 is required"
  command_exists curl || fail "curl is required"
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" == "dns-ondelete" || "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" == "dns-manifest-ondelete" ]]; then
    command_exists dig || fail "dig is required for authoritative DNS release validation"
  fi
  detect_kubectl

  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" == "blue-green" ]]; then
    FUGUE_PUBLIC_DATA_PLANE_RECORD_MODE="node-local-blue-green"
    validate_representative_smoke_configuration
    run_bluegreen_release
    daemonsets_csv="$(bluegreen_worker_bases | paste -sd, -)"
    write_release_record "${daemonsets_csv}"
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
      log "dry-run complete; inactive edge workers would be upgraded and switched with front pods preserved"
      return 0
    fi
    log "public edge blue/green release complete; front and previous active workers were not restarted"
    return 0
  fi

  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" == "front-ondelete" ]]; then
    FUGUE_PUBLIC_DATA_PLANE_RECORD_MODE="node-local-blue-green-front"
    run_front_ondelete_release
    daemonsets_csv="$(bluegreen_all_bases | while IFS= read -r base; do printf '%s\n' "$(bluegreen_front_daemonset_name "${base}")"; done | paste -sd, -)"
    write_release_record "${daemonsets_csv}"
    run_smoke_urls
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
      log "dry-run complete; front pods would be replaced only because front-ondelete was explicitly selected"
      return 0
    fi
    log "public edge front release complete; use only for explicit front changes"
    return 0
  fi

  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" == "dns-ondelete" ]]; then
    FUGUE_PUBLIC_DATA_PLANE_RECORD_MODE="dns-ondelete"
    validate_representative_smoke_configuration
    run_dns_ondelete_release
    daemonsets_csv="$(dns_daemonset_names | paste -sd, -)"
    write_release_record "${daemonsets_csv}"
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
      log "dry-run complete; DNS pods would be replaced one DaemonSet at a time"
      return 0
    fi
    log "public DNS release complete; DNS DaemonSets were replaced one at a time"
    return 0
  fi

  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" == "dns-manifest-ondelete" ]]; then
    FUGUE_PUBLIC_DATA_PLANE_RECORD_MODE="dns-manifest-ondelete"
    validate_representative_smoke_configuration
    run_dns_manifest_ondelete_release "${FUGUE_PUBLIC_DATA_PLANE_DNS_SNAPSHOT_FILE:-}"
    daemonsets_csv="$(dns_manifest_snapshot_query "${FUGUE_PUBLIC_DATA_PLANE_DNS_SNAPSHOT_FILE:-}" names | paste -sd, -)"
    if [[ -n "$(trim_field "${FUGUE_PUBLIC_DATA_PLANE_DNS_TARGET_STATE_FILE:-}")" ]]; then
      log "deferring the DNS manifest release record until the outer control-plane transaction commits"
    else
      write_release_record "${daemonsets_csv}"
    fi
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
      log "dry-run complete; only DNS daemonsets whose Helm template changed would be replaced"
      return 0
    fi
    log "public DNS manifest release complete; changed DNS DaemonSets were reconciled one at a time"
    return 0
  fi

  while IFS= read -r daemonset_name; do
    daemonset_name="$(trim_field "${daemonset_name}")"
    [[ -n "${daemonset_name}" ]] || continue
    public_daemonsets+=("${daemonset_name}")
  done < <(public_daemonset_names)
  if (( ${#public_daemonsets[@]} == 0 )); then
    fail "no public data-plane DaemonSets found in namespace ${FUGUE_NAMESPACE}"
  fi

  log "release_id=${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID} namespace=${FUGUE_NAMESPACE} daemonsets=${public_daemonsets[*]}"
  before="$(capture_daemonset_pods "${public_daemonsets[@]}")"

  for daemonset_name in "${public_daemonsets[@]}"; do
    patch_daemonset_ondelete "${daemonset_name}"
  done
  for daemonset_name in "${public_daemonsets[@]}"; do
    patch_daemonset_resources "${daemonset_name}"
  done

  after="$(capture_daemonset_pods "${public_daemonsets[@]}")"
  if [[ "${before}" != "${after}" ]]; then
    printf '%s\n' "${before}" >/tmp/fugue-public-data-plane-before.txt
    printf '%s\n' "${after}" >/tmp/fugue-public-data-plane-after.txt
    diff -u /tmp/fugue-public-data-plane-before.txt /tmp/fugue-public-data-plane-after.txt || true
    fail "public data-plane pod set or restart counts changed during template patch"
  fi

  daemonsets_csv="$(IFS=,; printf '%s' "${public_daemonsets[*]}")"
  write_release_record "${daemonsets_csv}"
  run_smoke_urls
  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    log "dry-run complete; no public data-plane pods would be deleted or restarted"
    return 0
  fi
  log "public data-plane DaemonSet templates patched; no public pods were deleted or restarted"
}

if [[ "${FUGUE_PUBLIC_DATA_PLANE_LIB_ONLY:-false}" != "true" ]]; then
  main "$@"
fi
