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

write_release_record() {
  local daemonsets_csv="$1"
  local release_record_name="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_RECORD_NAME:-${FUGUE_RELEASE_FULLNAME}-public-data-plane-release}"

  if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" == "true" ]]; then
    log "dry-run: skipping release record ${release_record_name}"
    return 0
  fi

  kubectl_cmd -n "${FUGUE_NAMESPACE}" create configmap "${release_record_name}" \
    --from-literal=release_id="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID}" \
    --from-literal=mode="edge-template-ondelete" \
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
  FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-fugue-fugue}"
  FUGUE_RELEASE_INSTANCE="${FUGUE_RELEASE_INSTANCE:-${FUGUE_RELEASE_NAME:-fugue}}"
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN:-false}"
  FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID="${FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID:-pdp-$(date -u +%Y%m%dT%H%M%SZ)-${GITHUB_SHA:-local}}"
  FUGUE_EDGE_RESOURCES_JSON="$(compact_json_object FUGUE_EDGE_RESOURCES_JSON "${FUGUE_EDGE_RESOURCES_JSON:-${default_edge_resources}}")"
  FUGUE_EDGE_CADDY_RESOURCES_JSON="$(compact_json_object FUGUE_EDGE_CADDY_RESOURCES_JSON "${FUGUE_EDGE_CADDY_RESOURCES_JSON:-${default_caddy_resources}}")"
  export FUGUE_PUBLIC_DATA_PLANE_RELEASE_ID FUGUE_EDGE_RESOURCES_JSON FUGUE_EDGE_CADDY_RESOURCES_JSON

  case "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" in
    true|false) ;;
    *) fail "FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN must be true or false" ;;
  esac

  command_exists python3 || fail "python3 is required"
  command_exists curl || fail "curl is required"
  detect_kubectl

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
