#!/usr/bin/env bash

set -euo pipefail

NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
REGISTRY_DEPLOYMENT="${FUGUE_REGISTRY_DEPLOYMENT_NAME:-fugue-fugue-registry}"
REGISTRY_LABEL_SELECTOR="${FUGUE_REGISTRY_LABEL_SELECTOR:-app.kubernetes.io/component=registry}"
REGISTRY_PVC="${FUGUE_REGISTRY_PVC_NAME:-fugue-fugue-registry-data}"
REGISTRY_PVC_SIZE="${FUGUE_REGISTRY_PVC_SIZE:-200Gi}"
REGISTRY_PVC_STORAGE_CLASS="${FUGUE_REGISTRY_PVC_STORAGE_CLASS:-fugue-workspace-rwo}"
REGISTRY_SOURCE_PATH="${FUGUE_REGISTRY_SOURCE_PATH:-}"
MIGRATION_IMAGE="${FUGUE_REGISTRY_MIGRATION_IMAGE:-busybox:1.36}"
RESTORE_REGISTRY_AFTER_MIGRATION="${FUGUE_RESTORE_REGISTRY_AFTER_MIGRATION:-false}"
JOB_NAME="${FUGUE_REGISTRY_MIGRATION_JOB_NAME:-fugue-registry-migrate-$(date +%s)}"

log() {
  printf '[fugue-registry-migrate] %s\n' "$*"
}

if command -v kubectl >/dev/null 2>&1; then
  KUBECTL=(kubectl)
elif command -v k3s >/dev/null 2>&1; then
  KUBECTL=(k3s kubectl)
else
  log "requires kubectl or k3s on PATH"
  exit 1
fi

registry_pod="$("${KUBECTL[@]}" -n "${NAMESPACE}" get pods \
  -l "${REGISTRY_LABEL_SELECTOR}" \
  -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' | head -n 1)"

if [[ -z "${registry_pod}" ]]; then
  log "no running registry pod found in namespace ${NAMESPACE} with selector ${REGISTRY_LABEL_SELECTOR}"
  exit 1
fi

registry_node="$("${KUBECTL[@]}" -n "${NAMESPACE}" get pod "${registry_pod}" -o jsonpath='{.spec.nodeName}')"
if [[ -z "${registry_node}" ]]; then
  log "registry pod ${registry_pod} has no assigned node"
  exit 1
fi

if [[ -z "${REGISTRY_SOURCE_PATH}" ]]; then
  REGISTRY_SOURCE_PATH="$("${KUBECTL[@]}" -n "${NAMESPACE}" get pod "${registry_pod}" \
    -o jsonpath='{range .spec.volumes[?(@.name=="registry-data")]}{.hostPath.path}{end}')"
fi

if [[ -z "${REGISTRY_SOURCE_PATH}" ]]; then
  log "registry pod ${registry_pod} is not using a hostPath registry-data volume; nothing to migrate"
  exit 0
fi

replicas="$("${KUBECTL[@]}" -n "${NAMESPACE}" get deploy "${REGISTRY_DEPLOYMENT}" -o jsonpath='{.spec.replicas}')"
if [[ -z "${replicas}" ]]; then
  replicas="1"
fi

log "source pod: ${NAMESPACE}/${registry_pod}"
log "source node: ${registry_node}"
log "source hostPath: ${REGISTRY_SOURCE_PATH}"
log "target PVC: ${NAMESPACE}/${REGISTRY_PVC}"

pvc_manifest="$(mktemp)"
trap 'rm -f "${pvc_manifest}" "${job_manifest:-}"' EXIT

{
  printf 'apiVersion: v1\n'
  printf 'kind: PersistentVolumeClaim\n'
  printf 'metadata:\n'
  printf '  name: %s\n' "${REGISTRY_PVC}"
  printf '  namespace: %s\n' "${NAMESPACE}"
  printf 'spec:\n'
  printf '  accessModes:\n'
  printf '    - ReadWriteOnce\n'
  printf '  resources:\n'
  printf '    requests:\n'
  printf '      storage: %s\n' "${REGISTRY_PVC_SIZE}"
  if [[ -n "${REGISTRY_PVC_STORAGE_CLASS}" ]]; then
    printf '  storageClassName: %s\n' "${REGISTRY_PVC_STORAGE_CLASS}"
  fi
} >"${pvc_manifest}"

"${KUBECTL[@]}" apply -f "${pvc_manifest}" >/dev/null

log "scaling ${NAMESPACE}/${REGISTRY_DEPLOYMENT} to 0 so the hostPath copy is stable"
"${KUBECTL[@]}" -n "${NAMESPACE}" scale "deploy/${REGISTRY_DEPLOYMENT}" --replicas=0 >/dev/null
"${KUBECTL[@]}" -n "${NAMESPACE}" wait \
  --for=delete pod \
  -l "${REGISTRY_LABEL_SELECTOR}" \
  --timeout=180s >/dev/null 2>&1 || true

job_manifest="$(mktemp)"
{
  printf 'apiVersion: batch/v1\n'
  printf 'kind: Job\n'
  printf 'metadata:\n'
  printf '  name: %s\n' "${JOB_NAME}"
  printf '  namespace: %s\n' "${NAMESPACE}"
  printf 'spec:\n'
  printf '  backoffLimit: 0\n'
  printf '  template:\n'
  printf '    spec:\n'
  printf '      restartPolicy: Never\n'
  printf '      nodeName: %s\n' "${registry_node}"
  printf '      containers:\n'
  printf '        - name: migrate\n'
  printf '          image: %s\n' "${MIGRATION_IMAGE}"
  printf '          command: ["/bin/sh", "-ec"]\n'
  printf '          args:\n'
  printf '            - |\n'
  printf '              test -d /source/docker/registry/v2\n'
  printf '              mkdir -p /target\n'
  printf '              cd /source\n'
  printf '              tar cf - . | (cd /target && tar xpf -)\n'
  printf '              test -d /target/docker/registry/v2\n'
  printf '              du -sh /target 2>/dev/null || true\n'
  printf '          volumeMounts:\n'
  printf '            - name: source\n'
  printf '              mountPath: /source\n'
  printf '              readOnly: true\n'
  printf '            - name: target\n'
  printf '              mountPath: /target\n'
  printf '      volumes:\n'
  printf '        - name: source\n'
  printf '          hostPath:\n'
  printf '            path: %s\n' "${REGISTRY_SOURCE_PATH}"
  printf '            type: Directory\n'
  printf '        - name: target\n'
  printf '          persistentVolumeClaim:\n'
  printf '            claimName: %s\n' "${REGISTRY_PVC}"
} >"${job_manifest}"

"${KUBECTL[@]}" apply -f "${job_manifest}" >/dev/null
log "waiting for migration job ${NAMESPACE}/${JOB_NAME}"
"${KUBECTL[@]}" -n "${NAMESPACE}" wait "job/${JOB_NAME}" --for=condition=complete --timeout=3600s
"${KUBECTL[@]}" -n "${NAMESPACE}" logs "job/${JOB_NAME}" || true

if [[ "${RESTORE_REGISTRY_AFTER_MIGRATION}" == "true" ]]; then
  log "restoring ${NAMESPACE}/${REGISTRY_DEPLOYMENT} to ${replicas} replicas"
  "${KUBECTL[@]}" -n "${NAMESPACE}" scale "deploy/${REGISTRY_DEPLOYMENT}" --replicas="${replicas}" >/dev/null
  "${KUBECTL[@]}" -n "${NAMESPACE}" rollout status "deploy/${REGISTRY_DEPLOYMENT}" --timeout=300s >/dev/null
else
  log "left ${NAMESPACE}/${REGISTRY_DEPLOYMENT} scaled to 0; deploy the PVC-backed chart values before restoring registry traffic"
fi

log "migration copy completed; old hostPath data was not deleted: ${registry_node}:${REGISTRY_SOURCE_PATH}"
