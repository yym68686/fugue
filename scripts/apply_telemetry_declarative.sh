#!/usr/bin/env bash

set -euo pipefail
umask 077

readonly FORWARD_MANIFEST="${1:-}"
readonly LKG_MANIFEST="${2:-}"
readonly NAMESPACE="${FUGUE_TELEMETRY_NAMESPACE:-}"
readonly HELM_RELEASE="${FUGUE_TELEMETRY_HELM_RELEASE:-}"
readonly HELM_CHART="${FUGUE_TELEMETRY_HELM_CHART:-}"
readonly DEPLOYMENT="${FUGUE_TELEMETRY_DEPLOYMENT:-}"
readonly SERVICE="${FUGUE_TELEMETRY_SERVICE:-}"
readonly IMAGE_REPOSITORY="${FUGUE_TELEMETRY_IMAGE_REPOSITORY:-}"
readonly FORWARD_DIGEST="${FUGUE_TELEMETRY_IMAGE_DIGEST:-}"
readonly LKG_DIGEST="${FUGUE_TELEMETRY_LKG_IMAGE_DIGEST:-}"
readonly SOURCE_SHA="${FUGUE_TELEMETRY_SOURCE_SHA:-}"
readonly LKG_SOURCE_SHA="${FUGUE_TELEMETRY_LKG_SOURCE_SHA:-}"
readonly OCI_REVISION="${FUGUE_TELEMETRY_OCI_REVISION:-}"
readonly LKG_OCI_REVISION="${FUGUE_TELEMETRY_LKG_OCI_REVISION:-}"
readonly FIELD_MANAGER="fugue-telemetry-declarative"

fail() {
  printf 'telemetry-declarative:%s\n' "$1" >&2
  exit 1
}

for command in helm kubectl python3 awk sha256sum; do
  command -v "${command}" >/dev/null 2>&1 || fail "missing-command:${command}"
done

[[ -n "${NAMESPACE}" && -n "${HELM_RELEASE}" && -n "${HELM_CHART}" && -n "${DEPLOYMENT}" && -n "${SERVICE}" ]] ||
  fail 'resource-identity-missing'
[[ -d "${HELM_CHART}" && ! -L "${HELM_CHART}" ]] || fail 'helm-chart-invalid'
[[ "${IMAGE_REPOSITORY}" =~ ^[a-z0-9.-]+/[a-z0-9._/-]+$ ]] || fail 'image-repository-invalid'
[[ "${FORWARD_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail 'forward-digest-invalid'
[[ "${LKG_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail 'lkg-digest-invalid'
[[ "${SOURCE_SHA}" =~ ^[0-9a-f]{40}$ ]] || fail 'source-sha-invalid'
[[ "${LKG_SOURCE_SHA}" =~ ^[0-9a-f]{40}$ ]] || fail 'lkg-source-sha-invalid'
[[ "${OCI_REVISION}" =~ ^[0-9a-f]{40}$ ]] || fail 'oci-revision-invalid'
[[ "${LKG_OCI_REVISION}" =~ ^[0-9a-f]{40}$ ]] || fail 'lkg-oci-revision-invalid'

validate_input_file() {
  local path="$1"
  local label="$2"
  [[ -f "${path}" && ! -L "${path}" ]] || fail "${label}-manifest-not-regular"
  local size
  python3 - "${path}" <<'PY' || fail "${label}-manifest-permissions"
import os
from pathlib import Path
import stat
import sys

info = Path(sys.argv[1]).lstat()
if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
    raise SystemExit(1)
if stat.S_IMODE(info.st_mode) & 0o022:
    raise SystemExit(1)
PY
  size="$(wc -c <"${path}" | tr -d '[:space:]')"
  [[ "${size}" =~ ^[1-9][0-9]*$ && "${size}" -le 2097152 ]] || fail "${label}-manifest-size"
}

validate_input_file "${FORWARD_MANIFEST}" forward
validate_input_file "${LKG_MANIFEST}" lkg

readonly WORK_DIR="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/fugue-telemetry-ssa.XXXXXX")"
cleanup() {
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

helm_revision() {
  local output="$1"
  helm status "${HELM_RELEASE}" --namespace "${NAMESPACE}" --output json >"${output}"
  python3 - "${output}" <<'PY'
import json
from pathlib import Path
import sys

raw = Path(sys.argv[1]).read_bytes()
if not raw or len(raw) > 1024 * 1024:
    raise SystemExit("helm-status-size-invalid")
value = json.loads(raw)
if type(value) is not dict or value.get("info", {}).get("status") != "deployed":
    raise SystemExit("helm-release-not-deployed")
revision = value.get("version")
if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
    raise SystemExit("helm-revision-invalid")
print(revision)
PY
}

helm_manifest_contains_deployment() {
  local manifest="$1"
  awk -v wanted="${DEPLOYMENT}" '
    function reset_doc() { kind=""; name=""; in_metadata=0 }
    function finish_doc() { if (kind == "Deployment" && name == wanted) found=1 }
    BEGIN { reset_doc() }
    /^---[[:space:]]*$/ { finish_doc(); reset_doc(); next }
    /^kind:[[:space:]]*/ {
      kind=$0
      sub(/^kind:[[:space:]]*/, "", kind)
      sub(/[[:space:]]*$/, "", kind)
      next
    }
    /^metadata:[[:space:]]*$/ { in_metadata=1; next }
    in_metadata && /^[^[:space:]]/ { in_metadata=0 }
    in_metadata && /^  name:[[:space:]]*/ {
      name=$0
      sub(/^  name:[[:space:]]*/, "", name)
      sub(/[[:space:]]*$/, "", name)
      gsub(/^"|"$/, "", name)
    }
    END { finish_doc(); exit(found ? 0 : 1) }
  ' "${manifest}"
}

capture_helm_desired() {
  local prefix="$1"
  local status_file="${WORK_DIR}/${prefix}-helm-status.json"
  local manifest_file="${WORK_DIR}/${prefix}-helm-manifest.yaml"
  local revision
  revision="$(helm_revision "${status_file}")" || fail 'helm-status-invalid'
  helm get manifest "${HELM_RELEASE}" --namespace "${NAMESPACE}" --revision "${revision}" >"${manifest_file}"
  local size
  size="$(wc -c <"${manifest_file}" | tr -d '[:space:]')"
  [[ "${size}" =~ ^[1-9][0-9]*$ && "${size}" -le 4194304 ]] || fail 'helm-manifest-size-invalid'
  local contains=false
  if helm_manifest_contains_deployment "${manifest_file}"; then
    contains=true
  fi
  printf '%s\t%s\t%s\n' "${revision}" "$(sha256sum "${manifest_file}" | awk '{print $1}')" "${contains}"
}

validate_live() {
  local path="$1"
  local expected_digest="$2"
  local expected_owner="$3"
  local expected_oci_revision="$4"
  python3 - "${path}" "${NAMESPACE}" "${DEPLOYMENT}" "${IMAGE_REPOSITORY}" "${expected_digest}" "${expected_owner}" "${expected_oci_revision}" "${FIELD_MANAGER}" <<'PY'
import json
from pathlib import Path
import sys

path, namespace, name, repository, digest, expected_owner, expected_oci_revision, manager = sys.argv[1:]
raw = Path(path).read_bytes()
if not raw or len(raw) > 2 * 1024 * 1024:
    raise SystemExit("live-deployment-size-invalid")
value = json.loads(raw)
metadata = value.get("metadata") if type(value) is dict else None
spec = value.get("spec") if type(value) is dict else None
status = value.get("status") if type(value) is dict else None
if value.get("apiVersion") != "apps/v1" or value.get("kind") != "Deployment":
    raise SystemExit("live-deployment-kind-invalid")
if type(metadata) is not dict or metadata.get("namespace") != namespace or metadata.get("name") != name:
    raise SystemExit("live-deployment-identity-invalid")
if type(spec) is not dict or spec.get("replicas") != 1:
    raise SystemExit("live-deployment-replicas-invalid")
containers = spec.get("template", {}).get("spec", {}).get("containers")
if type(containers) is not list or len(containers) != 1 or containers[0].get("name") != "telemetry-agent":
    raise SystemExit("live-deployment-container-invalid")
if containers[0].get("image") != f"{repository}@{digest}":
    raise SystemExit("live-deployment-image-invalid")
pod_annotations = spec.get("template", {}).get("metadata", {}).get("annotations") or {}
pod_source_commit = pod_annotations.get("fugue.pro/source-commit")
if expected_owner in {"legacy", "handoff"}:
    if pod_source_commit is not None and pod_source_commit != expected_oci_revision:
        raise SystemExit("live-deployment-pre-ssa-pod-source-commit-invalid")
elif expected_owner == "declarative":
    if pod_source_commit != expected_oci_revision:
        raise SystemExit("live-deployment-pod-source-commit-invalid")
annotations = metadata.get("annotations") or {}
labels = metadata.get("labels") or {}
owner = annotations.get("fugue.pro/telemetry-ownership")
if expected_owner == "legacy":
    if owner is not None or annotations.get("helm.sh/resource-policy") is not None:
        raise SystemExit("legacy-helm-owner-invalid")
elif expected_owner == "handoff":
    if owner != "helm" or annotations.get("helm.sh/resource-policy") != "keep":
        raise SystemExit("helm-keep-handoff-not-proven")
elif expected_owner == "declarative":
    if owner != "declarative" or labels.get("app.kubernetes.io/managed-by") != manager:
        raise SystemExit("declarative-owner-invalid")
    managed = metadata.get("managedFields") or []
    if not any(type(item) is dict and item.get("manager") == manager for item in managed):
        raise SystemExit("declarative-field-manager-missing")
else:
    raise SystemExit("expected-owner-invalid")
if type(status) is not dict or any(status.get(field) != 1 for field in ("replicas", "readyReplicas", "availableReplicas", "updatedReplicas")):
    raise SystemExit("live-deployment-not-one-of-one")
resource_version = metadata.get("resourceVersion")
if not isinstance(resource_version, str) or not resource_version:
    raise SystemExit("live-resource-version-invalid")
print(resource_version)
PY
}

live_continuity_fingerprint() {
  local path="$1"
  python3 - "${path}" "${NAMESPACE}" "${DEPLOYMENT}" "${IMAGE_REPOSITORY}" "${LKG_DIGEST}" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

path, namespace, name, repository, digest = sys.argv[1:]
raw = Path(path).read_bytes()
if not raw or len(raw) > 2 * 1024 * 1024:
    raise SystemExit("telemetry-live-size-invalid")
value = json.loads(raw)
metadata = value.get("metadata") if type(value) is dict else None
spec = value.get("spec") if type(value) is dict else None
status = value.get("status") if type(value) is dict else None
if value.get("apiVersion") != "apps/v1" or value.get("kind") != "Deployment":
    raise SystemExit("telemetry-live-kind-invalid")
if type(metadata) is not dict or metadata.get("namespace") != namespace or metadata.get("name") != name:
    raise SystemExit("telemetry-live-identity-invalid")
uid = metadata.get("uid")
if not isinstance(uid, str) or not uid:
    raise SystemExit("telemetry-live-uid-invalid")
if type(spec) is not dict or spec.get("replicas") != 1:
    raise SystemExit("telemetry-live-replicas-invalid")
containers = spec.get("template", {}).get("spec", {}).get("containers")
if type(containers) is not list or len(containers) != 1 or containers[0].get("name") != "telemetry-agent":
    raise SystemExit("telemetry-live-container-invalid")
image = containers[0].get("image")
if image != f"{repository}@{digest}":
    raise SystemExit("telemetry-live-lkg-image-invalid")
if type(status) is not dict or any(status.get(field) != 1 for field in ("replicas", "readyReplicas", "availableReplicas", "updatedReplicas")):
    raise SystemExit("telemetry-live-not-one-of-one")
payload = {
    "uid": uid,
    "template": spec.get("template"),
    "image": image,
    "replicas": 1,
    "readyReplicas": 1,
    "availableReplicas": 1,
    "updatedReplicas": 1,
}
canonical = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii")
print("sha256:" + hashlib.sha256(canonical).hexdigest())
PY
}

render_helm_stage() {
  local ownership="$1"
  local output="$2"
  local release_json="${WORK_DIR}/helm-${ownership}-dry-run.json"
  helm upgrade "${HELM_RELEASE}" "${HELM_CHART}" \
    --namespace "${NAMESPACE}" \
    --reuse-values \
    --set-string "observability.agent.ownership=${ownership}" \
    --dry-run=server \
    --hide-notes \
    --output json >"${release_json}"
  python3 - "${release_json}" "${output}" <<'PY'
import json
import os
from pathlib import Path
import sys

source, destination = map(Path, sys.argv[1:])
raw = source.read_bytes()
if not raw or len(raw) > 8 * 1024 * 1024:
    raise SystemExit("helm-dry-run-size-invalid")
value = json.loads(raw)
manifest = value.get("manifest") if type(value) is dict else None
if not isinstance(manifest, str) or not manifest.strip() or len(manifest.encode()) > 4 * 1024 * 1024:
    raise SystemExit("helm-dry-run-manifest-invalid")
destination.write_text(manifest, encoding="utf-8")
os.chmod(destination, 0o600)
PY
}

compare_helm_handoff_manifests() {
  local current_manifest="$1"
  local stage1_manifest="$2"
  local stage2_manifest="$3"
  local current_mode="$4"
  python3 - "${current_manifest}" "${stage1_manifest}" "${stage2_manifest}" "${current_mode}" "${DEPLOYMENT}" <<'PY'
from pathlib import Path
import re
import sys

current_path, stage1_path, stage2_path, mode, deployment = sys.argv[1:]

def documents(path):
    raw = Path(path).read_text(encoding="utf-8")
    if not raw or len(raw.encode()) > 4 * 1024 * 1024:
        raise SystemExit("helm-handoff-manifest-size-invalid")
    values = []
    for document in re.split(r"(?m)^---[ \t]*\r?\n", raw):
        if not document.strip():
            continue
        values.append(document.replace("\r\n", "\n").rstrip("\n") + "\n")
    if not values:
        raise SystemExit("helm-handoff-manifest-empty")
    return values

def is_telemetry(document):
    return (
        "# Source: fugue/templates/telemetry-agent-deployment.yaml\n" in document
        and "\nkind: Deployment\n" in "\n" + document
        and f"\n  name: {deployment}\n" in "\n" + document
    )

current = documents(current_path)
stage1 = documents(stage1_path)
stage2 = documents(stage2_path)
current_telemetry = [item for item in current if is_telemetry(item)]
stage1_telemetry = [item for item in stage1 if is_telemetry(item)]
stage2_telemetry = [item for item in stage2 if is_telemetry(item)]
if len(current_telemetry) != 1 or len(stage1_telemetry) != 1 or stage2_telemetry:
    raise SystemExit("helm-handoff-telemetry-inventory-invalid")
annotation_block = (
    "  annotations:\n"
    "    helm.sh/resource-policy: keep\n"
    "    fugue.pro/telemetry-ownership: helm\n"
)
if stage1_telemetry[0].count(annotation_block) != 1:
    raise SystemExit("helm-handoff-stage1-annotations-invalid")
if mode == "legacy":
    normalized = stage1_telemetry[0].replace(annotation_block, "", 1)
    if normalized != current_telemetry[0]:
        raise SystemExit("helm-handoff-stage1-deployment-diff-invalid")
elif mode == "handoff":
    if stage1_telemetry[0] != current_telemetry[0]:
        raise SystemExit("helm-handoff-resume-manifest-invalid")
else:
    raise SystemExit("helm-handoff-current-mode-invalid")
current_other = [item for item in current if not is_telemetry(item)]
stage1_other = [item for item in stage1 if not is_telemetry(item)]
if current_other != stage1_other:
    raise SystemExit("helm-handoff-stage1-other-object-diff")
if stage2 != stage1_other:
    raise SystemExit("helm-handoff-stage2-other-object-diff")
PY
}

compare_helm_manifest_bytes() {
  local left="$1"
  local right="$2"
  python3 - "${left}" "${right}" <<'PY'
from pathlib import Path
import re
import sys

def normalized(path):
    raw = Path(path).read_text(encoding="utf-8").replace("\r\n", "\n")
    return [part.rstrip("\n") + "\n" for part in re.split(r"(?m)^---[ \t]*\n", raw) if part.strip()]

if normalized(sys.argv[1]) != normalized(sys.argv[2]):
    raise SystemExit("helm-committed-manifest-differs-from-proven-dry-run")
PY
}

run_helm_stage() {
  local ownership="$1"
  helm upgrade "${HELM_RELEASE}" "${HELM_CHART}" \
    --namespace "${NAMESPACE}" \
    --reuse-values \
    --set-string "observability.agent.ownership=${ownership}" \
    --atomic \
    --cleanup-on-fail \
    --wait \
    --timeout 180s >"${WORK_DIR}/helm-${ownership}-upgrade.log" 2>&1
}

assert_live_continuity() {
  local expected="$1"
  local label="$2"
  local output="${WORK_DIR}/${label}-continuity-live.json"
  kubectl get deployment "${DEPLOYMENT}" --namespace "${NAMESPACE}" --output json >"${output}" ||
    fail "${label}-deleted-telemetry-deployment"
  local observed
  observed="$(live_continuity_fingerprint "${output}")" || fail "${label}-live-continuity-invalid"
  [[ "${observed}" == "${expected}" ]] || fail "${label}-changed-live-workload"
}

bootstrap_helm_ownership() {
  local current_owner="$1"
  local current_manifest="${WORK_DIR}/initial-helm-manifest.yaml"
  local stage1_manifest="${WORK_DIR}/helm-stage1-rendered.yaml"
  local stage2_manifest="${WORK_DIR}/helm-stage2-rendered.yaml"
  local baseline_fingerprint="$2"

  case "${current_owner}" in
    legacy|handoff) ;;
    declarative) fail 'double-writer-helm-still-desires-telemetry-deployment' ;;
    *) fail 'helm-bootstrap-live-owner-invalid' ;;
  esac
  render_helm_stage helm "${stage1_manifest}" || fail 'helm-stage1-dry-run-failed'
  render_helm_stage declarative "${stage2_manifest}" || fail 'helm-stage2-dry-run-failed'
  compare_helm_handoff_manifests \
    "${current_manifest}" "${stage1_manifest}" "${stage2_manifest}" "${current_owner}" ||
    fail 'helm-handoff-render-proof-failed'

  if [[ "${current_owner}" == legacy ]]; then
    stage1_rc=0
    run_helm_stage helm || stage1_rc=$?
    stage1_observed="$(capture_helm_desired stage1-observed)" ||
      fail 'helm-stage1-commit-unknown-reconcile-unavailable'
    kubectl get deployment "${DEPLOYMENT}" --namespace "${NAMESPACE}" --output json >"${WORK_DIR}/stage1-live.json" ||
      fail 'helm-stage1-commit-unknown-live-unavailable'
    if compare_helm_manifest_bytes "${stage1_manifest}" "${WORK_DIR}/stage1-observed-helm-manifest.yaml" &&
      validate_live "${WORK_DIR}/stage1-live.json" "${LKG_DIGEST}" handoff "${LKG_OCI_REVISION}" >/dev/null; then
      assert_live_continuity "${baseline_fingerprint}" stage1
    elif compare_helm_manifest_bytes "${current_manifest}" "${WORK_DIR}/stage1-observed-helm-manifest.yaml" &&
      validate_live "${WORK_DIR}/stage1-live.json" "${LKG_DIGEST}" legacy "${LKG_OCI_REVISION}" >/dev/null; then
      assert_live_continuity "${baseline_fingerprint}" stage1-safe-old-owner
      fail "helm-stage1-not-committed:rc=${stage1_rc}"
    else
      fail "helm-stage1-commit-unknown-unreconciled:rc=${stage1_rc}"
    fi
  fi

  stage2_rc=0
  run_helm_stage declarative || stage2_rc=$?
  stage2_capture="$(capture_helm_desired stage2-observed)" ||
    fail 'helm-stage2-commit-unknown-reconcile-unavailable'
  kubectl get deployment "${DEPLOYMENT}" --namespace "${NAMESPACE}" --output json >"${WORK_DIR}/stage2-live.json" ||
    fail 'helm-stage2-commit-unknown-live-unavailable'
  if compare_helm_manifest_bytes "${stage2_manifest}" "${WORK_DIR}/stage2-observed-helm-manifest.yaml" &&
    [[ "${stage2_capture##*$'\t'}" == false ]] &&
    validate_live "${WORK_DIR}/stage2-live.json" "${LKG_DIGEST}" handoff "${LKG_OCI_REVISION}" >/dev/null; then
    assert_live_continuity "${baseline_fingerprint}" stage2
  elif compare_helm_manifest_bytes "${stage1_manifest}" "${WORK_DIR}/stage2-observed-helm-manifest.yaml" &&
    [[ "${stage2_capture##*$'\t'}" == true ]] &&
    validate_live "${WORK_DIR}/stage2-live.json" "${LKG_DIGEST}" handoff "${LKG_OCI_REVISION}" >/dev/null; then
    assert_live_continuity "${baseline_fingerprint}" stage2-safe-helm-owner
    fail "helm-stage2-not-committed:rc=${stage2_rc}"
  else
    fail "helm-stage2-commit-unknown-unreconciled:rc=${stage2_rc}"
  fi
}

validate_rendered_manifest() {
  local manifest="$1"
  local label="$2"
  local digest="$3"
  local source_sha="$4"
  local oci_revision="$5"
  local output="${WORK_DIR}/${label}-dry-run.json"
  kubectl apply --dry-run=server --server-side --force-conflicts \
    --field-manager="${FIELD_MANAGER}" --filename "${manifest}" --output json >"${output}"
  python3 - "${output}" "${NAMESPACE}" "${DEPLOYMENT}" "${IMAGE_REPOSITORY}" "${digest}" "${source_sha}" "${oci_revision}" "${FIELD_MANAGER}" <<'PY'
import json
from pathlib import Path
import sys

path, namespace, name, repository, digest, source_sha, oci_revision, manager = sys.argv[1:]
raw = Path(path).read_bytes()
if not raw or len(raw) > 2 * 1024 * 1024:
    raise SystemExit("rendered-manifest-size-invalid")
value = json.loads(raw)
metadata = value.get("metadata") if type(value) is dict else None
spec = value.get("spec") if type(value) is dict else None
if value.get("apiVersion") != "apps/v1" or value.get("kind") != "Deployment":
    raise SystemExit("rendered-manifest-kind-invalid")
if type(metadata) is not dict or metadata.get("namespace") != namespace or metadata.get("name") != name:
    raise SystemExit("rendered-manifest-identity-invalid")
if metadata.get("ownerReferences") or metadata.get("finalizers"):
    raise SystemExit("rendered-manifest-controller-ownership-invalid")
annotations = metadata.get("annotations") or {}
labels = metadata.get("labels") or {}
if annotations.get("fugue.pro/telemetry-ownership") != "declarative":
    raise SystemExit("rendered-manifest-owner-invalid")
if annotations.get("fugue.pro/telemetry-manifest-revision") != source_sha:
    raise SystemExit("rendered-manifest-revision-invalid")
if labels.get("app.kubernetes.io/managed-by") != manager:
    raise SystemExit("rendered-manifest-manager-invalid")
if type(spec) is not dict or spec.get("replicas") != 1:
    raise SystemExit("rendered-manifest-replicas-invalid")
selector = spec.get("selector", {}).get("matchLabels")
pod_labels = spec.get("template", {}).get("metadata", {}).get("labels")
if type(selector) is not dict or selector.get("app.kubernetes.io/component") != "telemetry-agent":
    raise SystemExit("rendered-manifest-selector-invalid")
if type(pod_labels) is not dict or any(pod_labels.get(key) != item for key, item in selector.items()):
    raise SystemExit("rendered-manifest-pod-labels-invalid")
containers = spec.get("template", {}).get("spec", {}).get("containers")
if type(containers) is not list or len(containers) != 1 or containers[0].get("name") != "telemetry-agent":
    raise SystemExit("rendered-manifest-container-invalid")
container = containers[0]
if container.get("image") != f"{repository}@{digest}":
    raise SystemExit("rendered-manifest-image-invalid")
pod_annotations = spec.get("template", {}).get("metadata", {}).get("annotations") or {}
if pod_annotations.get("fugue.pro/source-commit") != oci_revision:
    raise SystemExit("rendered-manifest-pod-source-commit-invalid")
if container.get("readinessProbe", {}).get("httpGet", {}).get("path") != "/readyz":
    raise SystemExit("rendered-manifest-ready-probe-invalid")
if container.get("livenessProbe", {}).get("httpGet", {}).get("path") != "/healthz":
    raise SystemExit("rendered-manifest-health-probe-invalid")
PY
}

validate_transition_proof() {
  local live_path="$1"
  local label="$2"
  python3 - \
    "${live_path}" \
    "${WORK_DIR}/forward-dry-run.json" \
    "${WORK_DIR}/lkg-dry-run.json" \
    "${IMAGE_REPOSITORY}" \
    "${FORWARD_DIGEST}" \
    "${LKG_DIGEST}" \
    "${OCI_REVISION}" \
    "${LKG_OCI_REVISION}" \
    "${label}" <<'PY'
import copy
import json
from pathlib import Path
import sys

live_path, forward_path, lkg_path, repository, forward_digest, lkg_digest, forward_oci, lkg_oci, label = sys.argv[1:]

def load(path):
    raw = Path(path).read_bytes()
    if not raw or len(raw) > 2 * 1024 * 1024:
        raise SystemExit(f"{label}-transition-document-size-invalid")
    value = json.loads(raw)
    if type(value) is not dict or value.get("apiVersion") != "apps/v1" or value.get("kind") != "Deployment":
        raise SystemExit(f"{label}-transition-document-kind-invalid")
    return value

def normalized_metadata(value):
    metadata = copy.deepcopy(value.get("metadata"))
    if type(metadata) is not dict:
        raise SystemExit(f"{label}-transition-metadata-invalid")
    for field in ("resourceVersion", "managedFields", "generation"):
        metadata.pop(field, None)
    annotations = metadata.get("annotations")
    if annotations is not None:
        if type(annotations) is not dict:
            raise SystemExit(f"{label}-transition-annotations-invalid")
        for field in (
            "fugue.pro/telemetry-ownership",
            "fugue.pro/telemetry-manifest-revision",
            "helm.sh/resource-policy",
        ):
            annotations.pop(field, None)
        if not annotations:
            metadata.pop("annotations", None)
    labels = metadata.get("labels")
    if labels is not None:
        if type(labels) is not dict:
            raise SystemExit(f"{label}-transition-labels-invalid")
        labels.pop("app.kubernetes.io/managed-by", None)
        if not labels:
            metadata.pop("labels", None)
    return metadata

def normalized_forward_spec(value, expected_digest, expected_oci):
    spec = copy.deepcopy(value.get("spec"))
    if type(spec) is not dict:
        raise SystemExit(f"{label}-transition-spec-invalid")
    containers = spec.get("template", {}).get("spec", {}).get("containers")
    if type(containers) is not list or len(containers) != 1 or containers[0].get("name") != "telemetry-agent":
        raise SystemExit(f"{label}-transition-container-invalid")
    if containers[0].get("image") != f"{repository}@{expected_digest}":
        raise SystemExit(f"{label}-transition-image-invalid")
    containers[0]["image"] = f"{repository}@sha256:" + "0" * 64
    annotations = spec.get("template", {}).get("metadata", {}).get("annotations")
    if type(annotations) is not dict or annotations.get("fugue.pro/source-commit") != expected_oci:
        raise SystemExit(f"{label}-transition-pod-source-commit-invalid")
    annotations["fugue.pro/source-commit"] = "0" * 40
    return spec

def normalized_lkg_live_spec(value, expected_oci, live_document):
    spec = copy.deepcopy(value.get("spec"))
    if type(spec) is not dict:
        raise SystemExit(f"{label}-transition-spec-invalid")
    template_metadata = spec.get("template", {}).get("metadata")
    if type(template_metadata) is not dict:
        raise SystemExit(f"{label}-transition-template-metadata-invalid")
    annotations = template_metadata.get("annotations")
    if annotations is None:
        annotations = {}
        template_metadata["annotations"] = annotations
    if type(annotations) is not dict:
        raise SystemExit(f"{label}-transition-template-annotations-invalid")
    source_commit = annotations.get("fugue.pro/source-commit")
    if live_document:
        metadata = value.get("metadata")
        if type(metadata) is not dict:
            raise SystemExit(f"{label}-transition-live-metadata-invalid")
        ownership = (metadata.get("annotations") or {}).get("fugue.pro/telemetry-ownership")
        if ownership not in {None, "helm", "declarative"}:
            raise SystemExit(f"{label}-transition-live-owner-invalid")
        if source_commit is None and ownership in {None, "helm"}:
            source_commit = expected_oci
        if source_commit != expected_oci:
            raise SystemExit(f"{label}-transition-live-source-commit-invalid")
    elif source_commit != expected_oci:
        raise SystemExit(f"{label}-transition-lkg-source-commit-invalid")
    annotations["fugue.pro/source-commit"] = "0" * 40
    return spec

live = load(live_path)
forward = load(forward_path)
lkg = load(lkg_path)
if normalized_lkg_live_spec(lkg, lkg_oci, False) != normalized_lkg_live_spec(live, lkg_oci, True):
    raise SystemExit(f"{label}-lkg-live-spec-drift")
if normalized_forward_spec(forward, forward_digest, forward_oci) != normalized_forward_spec(lkg, lkg_digest, lkg_oci):
    raise SystemExit(f"{label}-forward-lkg-non-image-spec-drift")
if normalized_metadata(lkg) != normalized_metadata(live):
    raise SystemExit(f"{label}-lkg-live-metadata-drift")
if normalized_metadata(forward) != normalized_metadata(lkg):
    raise SystemExit(f"{label}-forward-lkg-metadata-drift")
PY
}

health_check() {
  local health ready metrics
  local proxy="/api/v1/namespaces/${NAMESPACE}/services/http:${SERVICE}:http/proxy"
  health="$(kubectl get --raw="${proxy}/healthz")" || return 1
  ready="$(kubectl get --raw="${proxy}/readyz")" || return 1
  metrics="$(kubectl get --raw="${proxy}/metrics")" || return 1
  HEALTH="${health}" READY="${ready}" python3 - <<'PY' || return 1
import json
import os

health = json.loads(os.environ["HEALTH"])
ready = json.loads(os.environ["READY"])
if type(health) is not dict or health.get("status") != "ok":
    raise SystemExit("healthz-invalid")
if type(ready) is not dict or ready.get("status") != "ok":
    raise SystemExit("readyz-invalid")
PY
  grep -Fx 'fugue_telemetry_agent_ready 1' <<<"${metrics}" >/dev/null || return 1
}

verify_rollout() {
  local digest="$1"
  local label="$2"
  local oci_revision="$3"
  local live_file="${WORK_DIR}/${label}-live.json"
  kubectl rollout status "deployment/${DEPLOYMENT}" --namespace "${NAMESPACE}" --timeout=120s || return 1
  kubectl get deployment "${DEPLOYMENT}" --namespace "${NAMESPACE}" --output json >"${live_file}" || return 1
  validate_live "${live_file}" "${digest}" declarative "${oci_revision}" >/dev/null || return 1
  health_check || return 1
}

initial_helm="$(capture_helm_desired initial)" || fail 'initial-helm-desired-invalid'
kubectl get deployment "${DEPLOYMENT}" --namespace "${NAMESPACE}" --output json >"${WORK_DIR}/initial-live.json"

validate_rendered_manifest "${FORWARD_MANIFEST}" forward "${FORWARD_DIGEST}" "${SOURCE_SHA}" "${OCI_REVISION}" ||
  fail 'forward-manifest-invalid'
validate_rendered_manifest "${LKG_MANIFEST}" lkg "${LKG_DIGEST}" "${LKG_SOURCE_SHA}" "${LKG_OCI_REVISION}" ||
  fail 'lkg-manifest-invalid'
validate_transition_proof "${WORK_DIR}/initial-live.json" initial ||
  fail 'initial-transition-proof-failed'

initial_owner="$(python3 - "${WORK_DIR}/initial-live.json" <<'PY'
import json
from pathlib import Path
import sys
value = json.loads(Path(sys.argv[1]).read_text())
print((value.get("metadata", {}).get("annotations") or {}).get("fugue.pro/telemetry-ownership", ""))
PY
)"
case "${initial_owner}" in
  '') expected_initial_owner=legacy ; initial_owner_mode=legacy ;;
  helm) expected_initial_owner=handoff ;;
  declarative) expected_initial_owner=declarative ; initial_owner_mode=declarative ;;
  *) fail 'live-owner-invalid' ;;
esac
if [[ "${initial_owner}" == helm ]]; then
  initial_owner_mode=handoff
fi
initial_resource_version="$(validate_live "${WORK_DIR}/initial-live.json" "${LKG_DIGEST}" "${expected_initial_owner}" "${LKG_OCI_REVISION}")" ||
  fail 'live-lkg-not-proven'
baseline_fingerprint="$(live_continuity_fingerprint "${WORK_DIR}/initial-live.json")" ||
  fail 'live-continuity-baseline-invalid'

if [[ "${initial_helm##*$'\t'}" == true ]]; then
  bootstrap_helm_ownership "${initial_owner_mode}" "${baseline_fingerprint}"
  initial_helm="$(capture_helm_desired post-bootstrap)" || fail 'post-bootstrap-helm-desired-invalid'
  [[ "${initial_helm##*$'\t'}" == false ]] || fail 'post-bootstrap-double-writer'
  kubectl get deployment "${DEPLOYMENT}" --namespace "${NAMESPACE}" --output json >"${WORK_DIR}/initial-live.json"
  initial_resource_version="$(validate_live "${WORK_DIR}/initial-live.json" "${LKG_DIGEST}" handoff "${LKG_OCI_REVISION}")" ||
    fail 'post-bootstrap-live-invalid'
  expected_initial_owner=handoff
elif [[ "${initial_owner_mode}" == legacy ]]; then
  fail 'helm-omitted-deployment-without-keep-handoff'
fi
readonly INITIAL_HELM="${initial_helm}"
readonly INITIAL_RESOURCE_VERSION="${initial_resource_version}"

final_helm="$(capture_helm_desired prewrite)" || fail 'prewrite-helm-desired-invalid'
readonly FINAL_HELM="${final_helm}"
[[ "${FINAL_HELM}" == "${INITIAL_HELM}" ]] || fail 'helm-desired-changed-before-apply'
kubectl get deployment "${DEPLOYMENT}" --namespace "${NAMESPACE}" --output json >"${WORK_DIR}/prewrite-live.json"
prewrite_resource_version="$(validate_live "${WORK_DIR}/prewrite-live.json" "${LKG_DIGEST}" "${expected_initial_owner}" "${LKG_OCI_REVISION}")" ||
  fail 'prewrite-live-recapture-invalid'
readonly PREWRITE_RESOURCE_VERSION="${prewrite_resource_version}"
[[ "${PREWRITE_RESOURCE_VERSION}" == "${INITIAL_RESOURCE_VERSION}" ]] || fail 'live-deployment-changed-before-apply'
validate_transition_proof "${WORK_DIR}/prewrite-live.json" prewrite ||
  fail 'prewrite-transition-proof-failed'

apply_status=0
kubectl apply --server-side --force-conflicts --field-manager="${FIELD_MANAGER}" \
  --filename "${FORWARD_MANIFEST}" >/dev/null || apply_status=$?
if [[ "${apply_status}" -ne 0 ]] || ! verify_rollout "${FORWARD_DIGEST}" forward "${OCI_REVISION}"; then
  printf 'telemetry-declarative:forward-failed; restoring exact Git LKG\n' >&2
  kubectl apply --server-side --force-conflicts --field-manager="${FIELD_MANAGER}" \
    --filename "${LKG_MANIFEST}" >/dev/null || fail 'lkg-rollback-apply-failed'
  verify_rollout "${LKG_DIGEST}" lkg "${LKG_OCI_REVISION}" || fail 'lkg-rollback-verification-failed'
  fail 'forward-rollout-failed-lkg-restored'
fi

printf 'telemetry-declarative:success source=%s image=%s@%s\n' \
  "${SOURCE_SHA}" "${IMAGE_REPOSITORY}" "${FORWARD_DIGEST}"
