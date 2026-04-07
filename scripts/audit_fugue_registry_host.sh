#!/usr/bin/env bash

set -euo pipefail

REGISTRY_ROOT="${FUGUE_REGISTRY_ROOT:-/var/lib/fugue/registry}"
FAIL_ON_INCONSISTENCY="${FUGUE_REGISTRY_AUDIT_FAIL_ON_INCONSISTENCY:-true}"

log() {
  printf '[fugue-registry-audit] %s\n' "$*"
}

repositories_root="${REGISTRY_ROOT}/docker/registry/v2/repositories"
blobs_root="${REGISTRY_ROOT}/docker/registry/v2/blobs/sha256"

if [[ ! -d "${repositories_root}" ]]; then
  log "repositories root ${repositories_root} does not exist; nothing to audit"
  exit 0
fi

scanned=0
inconsistencies=0

while IFS= read -r current_link; do
  [[ -n "${current_link}" ]] || continue
  relative_path="${current_link#${repositories_root}/}"
  repo="${relative_path%%/_manifests/tags/*}"
  tag_part="${relative_path#${repo}/_manifests/tags/}"
  tag="${tag_part%%/current/link}"
  digest="$(tr -d '[:space:]' < "${current_link}")"
  scanned=$((scanned + 1))

  if [[ ! "${digest}" =~ ^sha256:[a-f0-9]{64}$ ]]; then
    log "invalid manifest link for ${repo}:${tag}: ${digest:-<empty>}"
    inconsistencies=$((inconsistencies + 1))
    continue
  fi

  hash="${digest#sha256:}"
  revision_link="${repositories_root}/${repo}/_manifests/revisions/sha256/${hash}/link"
  blob_path="${blobs_root}/${hash:0:2}/${hash}/data"

  if [[ ! -f "${revision_link}" ]]; then
    log "missing revision link for ${repo}:${tag} -> ${digest}"
    inconsistencies=$((inconsistencies + 1))
  fi
  if [[ ! -f "${blob_path}" ]]; then
    log "missing blob for ${repo}:${tag} -> ${digest}"
    inconsistencies=$((inconsistencies + 1))
  fi
done < <(find "${repositories_root}" -type f -path '*/_manifests/tags/*/current/link' | sort)

if [[ "${inconsistencies}" -gt 0 ]]; then
  log "found ${inconsistencies} registry inconsistencies while scanning ${scanned} tag links"
  if [[ "${FAIL_ON_INCONSISTENCY}" == "true" ]]; then
    exit 1
  fi
else
  log "registry metadata audit passed across ${scanned} tag links"
fi
