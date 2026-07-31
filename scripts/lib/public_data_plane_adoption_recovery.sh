#!/usr/bin/env bash

# Durable, bounded recovery WAL for the dedicated public data-plane Helm
# adoption. Callers must already hold the shared coordination Lease.

public_data_plane_adoption_recovery_now() {
  python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"))'
}

public_data_plane_adoption_recovery_cm_name() {
  printf '%s-public-data-plane-adoption-recovery' "${RELEASE_FULLNAME}"
}

public_data_plane_adoption_verify_owned_lease() {
  local expected_recovery="${1:-true}" lease_json
  lease_json="$(${KUBECTL} -n "${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE}" \
    get "lease/${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME}" -o json)" || return 1
  LEASE_JSON="${lease_json}" EXPECTED_OWNER="${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER}" \
    EXPECTED_TOKEN="${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN}" EXPECTED_RECOVERY="${expected_recovery}" python3 - <<'PY'
import json, os
value = json.loads(os.environ["LEASE_JSON"])
metadata = value.get("metadata") or {}
annotations = metadata.get("annotations") or {}
spec = value.get("spec") or {}
assert spec.get("holderIdentity") == os.environ["EXPECTED_OWNER"]
assert annotations.get("fugue.pro/coordination-token") == os.environ["EXPECTED_TOKEN"]
if os.environ["EXPECTED_RECOVERY"] == "true":
    assert annotations.get("fugue.pro/recovery-required") == "true"
else:
    assert "fugue.pro/recovery-required" not in annotations
assert not metadata.get("deletionTimestamp")
assert metadata.get("uid") and metadata.get("resourceVersion")
PY
}

public_data_plane_adoption_extract_recovery_configmap() {
  local configmap_json="$1" output_dir="$2"
  CONFIGMAP_JSON="${configmap_json}" OUTPUT_DIR="${output_dir}" \
    EXPECTED_NAME="$(public_data_plane_adoption_recovery_cm_name)" \
    EXPECTED_NAMESPACE="${RELEASE_NAMESPACE}" python3 - <<'PY'
import json, os, pathlib
value = json.loads(os.environ["CONFIGMAP_JSON"])
metadata = value.get("metadata") or {}
labels = metadata.get("labels") or {}
assert value.get("apiVersion") == "v1" and value.get("kind") == "ConfigMap"
assert metadata.get("name") == os.environ["EXPECTED_NAME"]
assert metadata.get("namespace") == os.environ["EXPECTED_NAMESPACE"]
assert not metadata.get("deletionTimestamp")
assert metadata.get("uid") and metadata.get("resourceVersion")
assert labels == {
    "app.kubernetes.io/instance": os.environ.get("RELEASE_NAME", "fugue"),
    "app.kubernetes.io/component": "public-data-plane-adoption-recovery",
    "fugue.io/recovery-policy": "public-data-plane-helm-adoption-v1",
}
data = value.get("data") or {}
assert set(data) == {"wal.json", "transaction.json", "restore.json"}
root = pathlib.Path(os.environ["OUTPUT_DIR"])
root.mkdir(mode=0o700, parents=True, exist_ok=True)
for name, content in data.items():
    assert isinstance(content, str) and 0 < len(content.encode()) <= 98304
    path = root / name
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)
(root / "configmap-resource-version").write_text(metadata["resourceVersion"], encoding="utf-8")
(root / "configmap-resource-version").chmod(0o600)
PY
}

public_data_plane_adoption_persist_recovery_wal() {
  local cm_name transaction restore wal payload created readback
  cm_name="$(public_data_plane_adoption_recovery_cm_name)"
  transaction="${EVIDENCE_DIR}/transaction.json"
  restore="${EVIDENCE_DIR}/restore.json"
  wal="${EVIDENCE_DIR}/recovery-wal.json"
  public_data_plane_adoption_verify_owned_lease false || return 1
  if ${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o name >/dev/null 2>&1; then
    return 1
  fi
  "${ADOPTION_TOOL}" wal-init \
    --transaction "${transaction}" --restore "${restore}" --wal "${wal}" \
    --lease-namespace "${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE}" \
    --lease-name "${FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME}" \
    --lease-owner "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER}" \
    --lease-token "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN}" \
    --origin-run-id "${GITHUB_RUN_ID}" --origin-run-attempt "${GITHUB_RUN_ATTEMPT:-1}" \
    --at "$(public_data_plane_adoption_recovery_now)" >/dev/null || return 1
  payload="${EVIDENCE_DIR}/recovery-configmap.json"
  TRANSACTION="${transaction}" RESTORE="${restore}" WAL="${wal}" OUTPUT="${payload}" \
    NAME="${cm_name}" NAMESPACE="${RELEASE_NAMESPACE}" RELEASE_NAME="${RELEASE_NAME}" python3 - <<'PY'
import json, os, pathlib
read = lambda key: pathlib.Path(os.environ[key]).read_text(encoding="utf-8")
value = {
  "apiVersion": "v1", "kind": "ConfigMap",
  "metadata": {
    "name": os.environ["NAME"], "namespace": os.environ["NAMESPACE"],
    "labels": {
      "app.kubernetes.io/instance": os.environ["RELEASE_NAME"],
      "app.kubernetes.io/component": "public-data-plane-adoption-recovery",
      "fugue.io/recovery-policy": "public-data-plane-helm-adoption-v1",
    },
  },
  "immutable": False,
  "data": {"wal.json": read("WAL"), "transaction.json": read("TRANSACTION"), "restore.json": read("RESTORE")},
}
encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
assert len(encoded) <= 128 * 1024
pathlib.Path(os.environ["OUTPUT"]).write_bytes(encoded)
pathlib.Path(os.environ["OUTPUT"]).chmod(0o600)
PY
  created="$(${KUBECTL} create -f "${payload}" -o json)" || return 1
  readback="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)" || return 1
  rm -rf "${EVIDENCE_DIR}/wal-readback"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap "${readback}" "${EVIDENCE_DIR}/wal-readback" || return 1
  cmp -s "${transaction}" "${EVIDENCE_DIR}/wal-readback/transaction.json" || return 1
  cmp -s "${restore}" "${EVIDENCE_DIR}/wal-readback/restore.json" || return 1
  cmp -s "${wal}" "${EVIDENCE_DIR}/wal-readback/wal.json" || return 1
  "${ADOPTION_TOOL}" wal-verify --transaction "${transaction}" --restore "${restore}" --wal "${wal}" >/dev/null || return 1
  public_data_plane_adoption_verify_owned_lease false
}

public_data_plane_adoption_advance_recovery_wal() {
  local phase="$1" baseline_digest="${2:-}" cm_name current rv patch updated readback
  local -a baseline_args=()
  cm_name="$(public_data_plane_adoption_recovery_cm_name)"
  public_data_plane_adoption_verify_owned_lease || return 1
  current="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)" || return 1
  rm -rf "${EVIDENCE_DIR}/wal-current"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap "${current}" "${EVIDENCE_DIR}/wal-current" || return 1
  rv="$(cat "${EVIDENCE_DIR}/wal-current/configmap-resource-version")"
  if [[ -n "${baseline_digest}" ]]; then
    baseline_args=(--baseline-digest "${baseline_digest}")
  fi
  "${ADOPTION_TOOL}" wal-advance \
    --transaction "${EVIDENCE_DIR}/wal-current/transaction.json" \
    --restore "${EVIDENCE_DIR}/wal-current/restore.json" \
    --wal "${EVIDENCE_DIR}/wal-current/wal.json" \
    --lease-owner "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_OWNER}" \
    --lease-token "${CONTROL_PLANE_BACKUP_COORDINATION_LEASE_TOKEN}" \
    --phase "${phase}" --at "$(public_data_plane_adoption_recovery_now)" \
    "${baseline_args[@]}" >/dev/null || return 1
  cp "${EVIDENCE_DIR}/wal-current/wal.json" "${EVIDENCE_DIR}/recovery-wal.json"
  updated="$(cat "${EVIDENCE_DIR}/recovery-wal.json")"
  patch="$(RV="${rv}" WAL="${updated}" python3 - <<'PY'
import json, os
print(json.dumps([
  {"op":"test","path":"/metadata/resourceVersion","value":os.environ["RV"]},
  {"op":"replace","path":"/data/wal.json","value":os.environ["WAL"]},
], separators=(",", ":")))
PY
)" || return 1
  ${KUBECTL} -n "${RELEASE_NAMESPACE}" patch "configmap/${cm_name}" --type=json -p "${patch}" >/dev/null || return 1
  readback="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)" || return 1
  rm -rf "${EVIDENCE_DIR}/wal-readback"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap "${readback}" "${EVIDENCE_DIR}/wal-readback" || return 1
  cmp -s "${EVIDENCE_DIR}/recovery-wal.json" "${EVIDENCE_DIR}/wal-readback/wal.json" || return 1
  "${ADOPTION_TOOL}" wal-verify \
    --transaction "${EVIDENCE_DIR}/wal-readback/transaction.json" \
    --restore "${EVIDENCE_DIR}/wal-readback/restore.json" \
    --wal "${EVIDENCE_DIR}/wal-readback/wal.json" >/dev/null || return 1
  public_data_plane_adoption_verify_owned_lease
}

public_data_plane_adoption_delete_terminal_wal() {
  local cm_name current rv
  cm_name="$(public_data_plane_adoption_recovery_cm_name)"
  current="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)" || return 1
  rm -rf "${EVIDENCE_DIR}/wal-terminal"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap "${current}" "${EVIDENCE_DIR}/wal-terminal" || return 1
  "${ADOPTION_TOOL}" wal-verify \
    --transaction "${EVIDENCE_DIR}/wal-terminal/transaction.json" \
    --restore "${EVIDENCE_DIR}/wal-terminal/restore.json" \
    --wal "${EVIDENCE_DIR}/wal-terminal/wal.json" >/dev/null || return 1
  WAL="${EVIDENCE_DIR}/wal-terminal/wal.json" python3 - <<'PY'
import json, os
with open(os.environ["WAL"], encoding="utf-8") as source: value=json.load(source)
assert value["phase"] in {"baseline-finalized", "restore-succeeded"}
PY
  rv="$(cat "${EVIDENCE_DIR}/wal-terminal/configmap-resource-version")"
  ${KUBECTL} -n "${RELEASE_NAMESPACE}" delete "configmap/${cm_name}" --resource-version="${rv}" --wait=false >/dev/null
}

public_data_plane_adoption_delete_unarmed_wal() {
  local expected_recovery="${1:-false}" cm_name current rv
  cm_name="$(public_data_plane_adoption_recovery_cm_name)"
  public_data_plane_adoption_verify_owned_lease "${expected_recovery}" || return 1
  current="$(${KUBECTL} -n "${RELEASE_NAMESPACE}" get "configmap/${cm_name}" -o json)" || return 1
  rm -rf "${EVIDENCE_DIR}/wal-unarmed"
  RELEASE_NAME="${RELEASE_NAME}" public_data_plane_adoption_extract_recovery_configmap "${current}" "${EVIDENCE_DIR}/wal-unarmed" || return 1
  "${ADOPTION_TOOL}" wal-verify \
    --transaction "${EVIDENCE_DIR}/wal-unarmed/transaction.json" \
    --restore "${EVIDENCE_DIR}/wal-unarmed/restore.json" \
    --wal "${EVIDENCE_DIR}/wal-unarmed/wal.json" >/dev/null || return 1
  WAL="${EVIDENCE_DIR}/wal-unarmed/wal.json" python3 - <<'PY'
import json, os
with open(os.environ["WAL"], encoding="utf-8") as source: value=json.load(source)
assert value["phase"] == "lease-acquired"
PY
  rv="$(cat "${EVIDENCE_DIR}/wal-unarmed/configmap-resource-version")"
  ${KUBECTL} -n "${RELEASE_NAMESPACE}" delete "configmap/${cm_name}" --resource-version="${rv}" --wait=false >/dev/null
}
