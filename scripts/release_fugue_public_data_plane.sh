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

bluegreen_all_bases() {
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get ds -o json | python3 -c '
import json
import re
import sys

doc = json.load(sys.stdin)
bases = set()
for item in doc.get("items", []):
    meta = item.get("metadata", {})
    labels = meta.get("labels") or {}
    if labels.get("fugue.io/rollout-subsystem") != "public-data-plane":
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
'
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
  kubectl_cmd -n "${FUGUE_NAMESPACE}" get pods -l fugue.io/rollout-mode=node-local-blue-green-front -o json | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
fallback = ""
for pod in sorted(doc.get("items", []), key=lambda item: item.get("metadata", {}).get("name", "")):
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
  front_image_ref="$(trim_field "$(live_front_pod_image || true)")"
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
      fail "daemonset ${daemonset_name} replacement not ready: old_uid_present=${old_uid_present} desired=${desired} ready=${ready} unavailable=${unavailable}"
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
    kubectl_cmd -n "${FUGUE_NAMESPACE}" rollout status "ds/${daemonset_name}" --timeout="${timeout}"
    return
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
      fail "daemonset ${daemonset_name} not ready: generation=${generation} observed=${observed_generation} desired=${desired} ready=${ready} unavailable=${unavailable}"
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
  local slot
  slot="$(trim_field "$(active_slot_from_front "${front_daemonset}")")"
  case "${slot}" in
    a|b) printf '%s' "${slot}"; return 0 ;;
  esac
  slot="$(trim_field "$(active_slot_from_record "${base}")")"
  case "${slot}" in
    a|b) printf '%s' "${slot}"; return 0 ;;
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
  local active_slots

  active_slots="$(kubectl_cmd -n "${FUGUE_NAMESPACE}" get "configmap/${release_record_name}" -o jsonpath='{.data.active_slots}' 2>/dev/null || true)"
  active_slots="$(trim_field "${active_slots}")"
  if [[ -n "${active_slots}" ]]; then
    python3 -c '
import json
import sys

try:
    value = json.loads(sys.argv[1])
except Exception:
    raise SystemExit(1)
if not isinstance(value, dict):
    raise SystemExit(1)
print(json.dumps(value, separators=(",", ":"), sort_keys=True))
' "${active_slots}" && return 0
  fi
  printf '{}'
}

collect_current_active_slots_json() {
  local bases=("$@")
  local active_slots="{}"
  local base
  local front_ds
  local slot

  for base in "${bases[@]}"; do
    front_ds="$(bluegreen_front_daemonset_name "${base}")"
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

worker_smoke_target() {
  local urls="${FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS:-}"
  [[ -n "$(trim_field "${urls}")" ]] || return 0
  python3 -c '
import sys
from urllib.parse import urlsplit

for raw in sys.argv[1].split(","):
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
    break
' "${urls}"
}

check_worker_https_smoke() {
  local daemonset_name="$1"
  local port="$2"
  local target
  local host
  local path
  local host_ip

  target="$(worker_smoke_target)"
  [[ -n "$(trim_field "${target}")" ]] || return 0
  IFS=$'\t' read -r host path <<<"${target}"
  [[ -n "$(trim_field "${host}")" ]] || return 0
  path="${path:-/}"
  while IFS= read -r host_ip; do
    host_ip="$(trim_field "${host_ip}")"
    [[ -n "${host_ip}" ]] || continue
    log "checking inactive worker HTTPS smoke ${host_ip}:${port} host=${host} path=${path}"
    if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
      continue
    fi
    curl -fsS --max-time "${FUGUE_PUBLIC_DATA_PLANE_SMOKE_TIMEOUT_SECONDS:-10}" \
      --resolve "${host}:${port}:${host_ip}" \
      "https://${host}:${port}${path}" >/dev/null
  done < <(node_ips_for_daemonset "${daemonset_name}")
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

check_public_smoke_on_front_nodes() {
  local front_daemonset="$1"
  local urls="${FUGUE_PUBLIC_DATA_PLANE_SMOKE_URLS:-}"
  local url
  local host
  local path
  local host_ip

  [[ -n "$(trim_field "${urls}")" ]] || return 0
  while IFS= read -r url; do
    url="$(trim_field "${url}")"
    [[ -n "${url}" ]] || continue
    host="$(python3 -c 'from urllib.parse import urlsplit; import sys; print(urlsplit(sys.argv[1]).hostname or "")' "${url}")"
    path="$(python3 -c 'from urllib.parse import urlsplit; import sys; p=urlsplit(sys.argv[1]); path=p.path or "/"; print(path + (("?" + p.query) if p.query else ""))' "${url}")"
    [[ -n "$(trim_field "${host}")" ]] || fail "front smoke URL must include a hostname: ${url}"
    while IFS= read -r host_ip; do
      host_ip="$(trim_field "${host_ip}")"
      [[ -n "${host_ip}" ]] || continue
      log "checking front HTTPS smoke ${host_ip}:443 host=${host} path=${path}"
      if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
        continue
      fi
      curl -fsS --max-time "${FUGUE_PUBLIC_DATA_PLANE_SMOKE_TIMEOUT_SECONDS:-10}" \
        --resolve "${host}:443:${host_ip}" \
        "https://${host}${path}" >/dev/null
    done < <(node_ips_for_daemonset "${front_daemonset}")
  done < <(printf '%s\n' "${urls}" | tr ',' '\n')
}

run_bluegreen_release() {
  local bases=()
  local base front_ds active inactive inactive_ds active_ds inactive_port
  local protected_before protected_after
  local active_slots_json="{}"

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
    active="$(current_active_slot "${base}" "${front_ds}")"
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
    check_worker_https_smoke "${inactive_ds}" "${inactive_port}"
    protected_after="$(capture_daemonset_pods "${front_ds}" "${active_ds}")"
    if [[ "${protected_before}" != "${protected_after}" ]]; then
      printf '%s\n' "${protected_before}" >/tmp/fugue-public-data-plane-protected-before.txt
      printf '%s\n' "${protected_after}" >/tmp/fugue-public-data-plane-protected-after.txt
      diff -u /tmp/fugue-public-data-plane-protected-before.txt /tmp/fugue-public-data-plane-protected-after.txt || true
      fail "front or active worker pod set changed while upgrading inactive worker ${inactive_ds}"
    fi
    write_front_active_slot "${front_ds}" "${inactive}"
    wait_daemonset_ready "${front_ds}"
    active_slots_json="$(record_active_slot_json "${active_slots_json}" "${base}" "${inactive}")"
  done
  FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOTS_JSON="${active_slots_json}"
}

run_front_ondelete_release() {
  local bases=()
  local base
  local front_ds
  local before_uids

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
    patch_front_template "${front_ds}"
    before_uids="$(daemonset_pod_uids "${front_ds}")"
    delete_daemonset_pods_no_wait "${front_ds}" "front"
    wait_daemonset_replaced_and_ready "${front_ds}" "${before_uids}"
    check_public_smoke_on_front_nodes "${front_ds}"
  done
  FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOTS_JSON="$(collect_current_active_slots_json "${bases[@]}")"
}

write_release_record() {
  local daemonsets_csv="$1"
  local release_record_name="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_RECORD_NAME:-${FUGUE_RELEASE_FULLNAME}-public-data-plane-release}"
  local active_slots_json="${FUGUE_PUBLIC_DATA_PLANE_ACTIVE_SLOTS_JSON:-}"

  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    log "dry-run: skipping release record ${release_record_name}"
    return 0
  fi
  if [[ -z "$(trim_field "${active_slots_json}")" ]]; then
    active_slots_json="$(release_record_active_slots_json)"
  fi

  kubectl_cmd -n "${FUGUE_NAMESPACE}" create configmap "${release_record_name}" \
    --from-literal=release_id="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID}" \
    --from-literal=mode="${FUGUE_PUBLIC_DATA_PLANE_RECORD_MODE:-edge-template-ondelete}" \
    --from-literal=active_slots="${active_slots_json}" \
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
  FUGUE_PUBLIC_DATA_PLANE_FRONT_RESTART_CONFIRM="${FUGUE_PUBLIC_DATA_PLANE_FRONT_RESTART_CONFIRM:-false}"
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
    blue-green|front-ondelete|legacy-template-ondelete) ;;
    *) fail "FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY must be blue-green, front-ondelete, or legacy-template-ondelete" ;;
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
