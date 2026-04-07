#!/usr/bin/env bash

set -euo pipefail

REGISTRY_ROOT="${FUGUE_REGISTRY_ROOT:-/var/lib/fugue/registry}"
REGISTRY_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
REGISTRY_DEPLOYMENT="${FUGUE_REGISTRY_DEPLOYMENT_NAME:-fugue-fugue-registry}"
REGISTRY_IMAGE="${FUGUE_REGISTRY_IMAGE:-docker.io/library/registry:2.8.3}"
UPLOAD_STALE_MINUTES="${FUGUE_REGISTRY_UPLOAD_STALE_MINUTES:-1440}"
RUN_IMAGE_PRUNE="${FUGUE_RUN_IMAGE_PRUNE:-true}"
SCALE_REGISTRY_DEPLOYMENT="${FUGUE_SCALE_REGISTRY_DEPLOYMENT:-true}"

log() {
  printf '[fugue-registry-cleanup] %s\n' "$*"
}

if command -v k3s >/dev/null 2>&1; then
  KUBECTL=(k3s kubectl)
  CTR=(k3s ctr)
  CRICTL=(k3s crictl)
elif command -v kubectl >/dev/null 2>&1 && command -v ctr >/dev/null 2>&1 && command -v crictl >/dev/null 2>&1; then
  KUBECTL=(kubectl)
  CTR=(ctr)
  CRICTL=(crictl)
else
  log "requires either k3s or kubectl/ctr/crictl on the host"
  exit 1
fi

show_usage() {
  df -h /
  du -sh "${REGISTRY_ROOT}" 2>/dev/null || true
}

registry_exists() {
  "${KUBECTL[@]}" -n "${REGISTRY_NAMESPACE}" get "deploy/${REGISTRY_DEPLOYMENT}" >/dev/null 2>&1
}

scale_registry() {
  local replicas="$1"
  "${KUBECTL[@]}" -n "${REGISTRY_NAMESPACE}" scale "deploy/${REGISTRY_DEPLOYMENT}" --replicas="${replicas}" >/dev/null
  if [[ "${replicas}" == "0" ]]; then
    "${KUBECTL[@]}" -n "${REGISTRY_NAMESPACE}" wait \
      --for=delete pod \
      -l app.kubernetes.io/component=registry \
      --timeout=180s >/dev/null 2>&1 || true
    return
  fi
  "${KUBECTL[@]}" -n "${REGISTRY_NAMESPACE}" rollout status "deploy/${REGISTRY_DEPLOYMENT}" --timeout=300s >/dev/null
}

prune_images() {
  if [[ "${RUN_IMAGE_PRUNE}" != "true" ]]; then
    return
  fi
  if "${CRICTL[@]}" rmi --prune >/tmp/fugue-registry-cleanup-image-prune.log 2>&1; then
    log "unused container images pruned"
  else
    log "image prune returned non-zero; continuing"
  fi
}

find_stale_upload_dirs() {
  local repositories_root="${REGISTRY_ROOT}/docker/registry/v2/repositories"
  [[ -d "${repositories_root}" ]] || return 0
  while IFS= read -r uploads_root; do
    find "${uploads_root}" -mindepth 1 -maxdepth 1 -type d -mmin "+${UPLOAD_STALE_MINUTES}" -print
  done < <(find "${repositories_root}" -type d -name '_uploads' -print)
}

purge_stale_uploads() {
  local path=""
  local removed=0
  while IFS= read -r path; do
    [[ -n "${path}" ]] || continue
    rm -rf -- "${path}"
    removed=$((removed + 1))
    log "removed stale upload directory ${path}"
  done < <(find_stale_upload_dirs)
  log "removed ${removed} stale upload directories older than ${UPLOAD_STALE_MINUTES} minutes"
}

ensure_registry_image() {
  if "${CTR[@]}" images ls | awk 'NR > 1 {print $1}' | grep -Fxq "${REGISTRY_IMAGE}"; then
    return
  fi
  log "pulling ${REGISTRY_IMAGE} for offline GC"
  "${CTR[@]}" images pull "${REGISTRY_IMAGE}" >/dev/null
}

run_offline_gc() {
  log "running offline registry garbage-collect against ${REGISTRY_ROOT}"
  timeout 600s "${CTR[@]}" run --rm \
    --mount type=bind,src="${REGISTRY_ROOT}",dst=/var/lib/registry,options=rbind:rw \
    "${REGISTRY_IMAGE}" "fugue-registry-gc-$(date +%s)" \
    registry garbage-collect --delete-untagged /etc/docker/registry/config.yml \
    </dev/null >/dev/null
}

log "filesystem usage before cleanup"
show_usage

registry_present="false"
if registry_exists; then
  registry_present="true"
fi

prune_images

if [[ "${registry_present}" == "true" && "${SCALE_REGISTRY_DEPLOYMENT}" == "true" ]]; then
  log "scaling ${REGISTRY_NAMESPACE}/${REGISTRY_DEPLOYMENT} to 0 for offline cleanup"
  scale_registry 0
fi

purge_stale_uploads
ensure_registry_image
run_offline_gc

if [[ "${registry_present}" == "true" && "${SCALE_REGISTRY_DEPLOYMENT}" == "true" ]]; then
  log "scaling ${REGISTRY_NAMESPACE}/${REGISTRY_DEPLOYMENT} back to 1"
  scale_registry 1
fi

log "filesystem usage after cleanup"
show_usage
