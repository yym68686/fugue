#!/usr/bin/env bash

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SCRIPT="${REPO_ROOT}/scripts/apply_telemetry_declarative.sh"
readonly BASE_MANIFEST="${REPO_ROOT}/deploy/kustomize/telemetry/deployment.json"
readonly FORWARD_DIGEST="sha256:1111111111111111111111111111111111111111111111111111111111111111"
readonly LKG_DIGEST="sha256:2222222222222222222222222222222222222222222222222222222222222222"
readonly SOURCE_SHA="1111111111111111111111111111111111111111"
readonly LKG_SOURCE_SHA="2222222222222222222222222222222222222222"
readonly LKG_OCI_REVISION="3333333333333333333333333333333333333333"
readonly REPOSITORY="ghcr.io/example/fugue-telemetry-agent"
readonly CONTROLLER_SOURCE_SHA="d1e7ed9cdedbaa09db9bd78b4e433b94c7357510"
readonly CONTROLLER_IMAGE="ghcr.io/yym68686/fugue-controller@sha256:e636b35fe8718e1f20895c0a290924a0d48a6cb7d1072d741612df18483fa13d"

FIXTURE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/fugue-telemetry-declarative-test.XXXXXX")"
cleanup() {
  rm -rf "${FIXTURE_ROOT}"
}
trap cleanup EXIT

mkdir -p "${FIXTURE_ROOT}/bin"

cat >"${FIXTURE_ROOT}/bin/helm" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-} ${2:-}" in
  "status fugue")
    count="$(( $(cat "${FIXTURE_ROOT}/helm-status-count") + 1 ))"
    printf '%s\n' "${count}" >"${FIXTURE_ROOT}/helm-status-count"
    revision="$(cat "${FIXTURE_ROOT}/helm-revision")"
    if [[ "${HELM_REVISION_DRIFT:-false}" == true && "${count}" -eq 2 ]]; then
      revision="$((revision + 1))"
    fi
    printf '{"info":{"status":"deployed"},"version":%s}\n' "${revision}"
    ;;
  "get manifest")
    revision=''
    while (($#)); do
      case "$1" in
        --revision)
          revision="$2"
          shift 2
          ;;
        *) shift ;;
      esac
    done
    if [[ "${revision}" == 822 ]]; then
      cat "${FIXTURE_ROOT}/legacy-base-manifest.yaml"
    else
      cat "${FIXTURE_ROOT}/helm-manifest.yaml"
    fi
    ;;
  "upgrade fugue")
    ownership=''
    dry_run=false
    post_renderer=''
    while (($#)); do
      case "$1" in
        --set-string)
          ownership="${2##*=}"
          shift 2
          ;;
        --dry-run=server)
          dry_run=true
          shift
          ;;
        --post-renderer)
          post_renderer="$2"
          shift 2
          ;;
        *) shift ;;
      esac
    done
    [[ "${ownership}" == helm || "${ownership}" == declarative ]]
    rendered="${FIXTURE_ROOT}/rendered-${ownership}.yaml"
    if [[ -n "${post_renderer}" ]]; then
      [[ -x "${post_renderer}" ]]
      printf 'renderer-use:%s:%s:%s:%s\n' "${ownership}" "${dry_run}" \
        "$(sha256sum "${post_renderer}" | awk '{print $1}')" "${post_renderer}" >>"${FIXTURE_ROOT}/renderer.log"
      "${post_renderer}" <"${FIXTURE_ROOT}/stage-${ownership}-manifest.yaml" >"${rendered}"
    else
      cp "${FIXTURE_ROOT}/stage-${ownership}-manifest.yaml" "${rendered}"
    fi
    if [[ "${dry_run}" == true ]]; then
      python3 - "${rendered}" <<'PY'
import json
from pathlib import Path
import sys
print(json.dumps({"manifest": Path(sys.argv[1]).read_text()}, separators=(",", ":")))
PY
      exit 0
    fi
    printf 'helm-stage:%s\n' "${ownership}" >>"${FIXTURE_ROOT}/commands.log"
    if [[ "${ownership}" == helm ]]; then
      if [[ "${HELM_STAGE1_FAIL_MODE:-}" == old ]]; then
        exit 41
      fi
      cp "${rendered}" "${FIXTURE_ROOT}/helm-manifest.yaml"
      python3 - "${FIXTURE_ROOT}/live.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
value = json.loads(path.read_text())
metadata = value["metadata"]
metadata.setdefault("annotations", {})["helm.sh/resource-policy"] = "keep"
metadata["annotations"]["fugue.pro/telemetry-ownership"] = "helm"
metadata.setdefault("labels", {})["app.kubernetes.io/managed-by"] = "Helm"
path.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True))
PY
      printf '%s\n' 823 >"${FIXTURE_ROOT}/helm-revision"
      [[ "${HELM_STAGE1_FAIL_MODE:-}" != committed ]] || exit 42
    else
      if [[ "${HELM_STAGE2_FAIL_MODE:-}" == old ]]; then
        exit 43
      fi
      cp "${rendered}" "${FIXTURE_ROOT}/helm-manifest.yaml"
      printf '%s\n' 824 >"${FIXTURE_ROOT}/helm-revision"
      [[ "${HELM_STAGE2_FAIL_MODE:-}" != committed ]] || exit 44
    fi
    ;;
  *)
    printf 'unexpected helm command: %q ' "$@" >&2
    exit 91
    ;;
esac
SH

cat >"${FIXTURE_ROOT}/bin/kubectl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "get" && "${2:-}" == "daemonsets" ]]; then
  count="$(( $(cat "${FIXTURE_ROOT}/daemonset-read-count") + 1 ))"
  printf '%s\n' "${count}" >"${FIXTURE_ROOT}/daemonset-read-count"
  if [[ "${EDGE_CHECKSUM_DRIFT:-false}" == true && "${count}" -ge 2 ]]; then
    python3 - "${FIXTURE_ROOT}/daemonsets.json" <<'PY'
import json
from pathlib import Path
import sys
value = json.loads(Path(sys.argv[1]).read_text())
annotations = value["items"][0]["spec"]["template"]["metadata"]["annotations"]
key = next(iter(annotations))
annotations[key] = "f" * 64
print(json.dumps(value, separators=(",", ":"), sort_keys=True))
PY
  else
    cat "${FIXTURE_ROOT}/daemonsets.json"
  fi
  exit 0
fi
if [[ "${1:-}" == "get" && "${2:-}" == "deployment" ]]; then
  if [[ "${3:-}" == "fugue-fugue-controller" ]]; then
    count="$(( $(cat "${FIXTURE_ROOT}/controller-read-count") + 1 ))"
    printf '%s\n' "${count}" >"${FIXTURE_ROOT}/controller-read-count"
    if [[ "${CONTROLLER_TEMPLATE_DRIFT:-false}" == true && "${count}" -ge 2 ]]; then
      python3 - "${FIXTURE_ROOT}/controller.json" <<'PY'
import json
from pathlib import Path
import sys
value = json.loads(Path(sys.argv[1]).read_text())
value["spec"]["template"]["metadata"].setdefault("annotations", {})["synthetic.test/drift"] = "true"
print(json.dumps(value, separators=(",", ":"), sort_keys=True))
PY
    else
      cat "${FIXTURE_ROOT}/controller.json"
    fi
    exit 0
  fi
  show_managed_fields=false
  for argument in "$@"; do
    if [[ "${argument}" == "--show-managed-fields=true" ]]; then
      show_managed_fields=true
    fi
  done
  printf 'deployment-read:%s\n' "${show_managed_fields}" >>"${FIXTURE_ROOT}/live-reads.log"
  if [[ "${show_managed_fields}" == true ]]; then
    cat "${FIXTURE_ROOT}/live.json"
  else
    python3 - "${FIXTURE_ROOT}/live.json" <<'PY'
import json
from pathlib import Path
import sys

value = json.loads(Path(sys.argv[1]).read_text())
value.get("metadata", {}).pop("managedFields", None)
print(json.dumps(value, separators=(",", ":"), sort_keys=True))
PY
  fi
  exit 0
fi
if [[ "${1:-}" == "get" && "${2:-}" == --raw=* ]]; then
  if [[ "${HEALTH_FAIL:-false}" == "true" && "$(cat "${FIXTURE_ROOT}/state")" == "forward" ]]; then
    exit 1
  fi
  case "${2}" in
    */healthz) printf '%s\n' '{"status":"ok"}' ;;
    */readyz) printf '%s\n' '{"status":"ok"}' ;;
    */metrics) printf '%s\n' 'fugue_telemetry_agent_ready 1' ;;
    *) exit 92 ;;
  esac
  exit 0
fi
if [[ "${1:-}" == "rollout" && "${2:-}" == "status" ]]; then
  exit 0
fi
if [[ "${1:-}" == "apply" ]]; then
  manifest=''
  dry_run=false
  while (($#)); do
    case "$1" in
      --filename)
        manifest="$2"
        shift 2
        ;;
      --dry-run=server)
        dry_run=true
        shift
        ;;
      *) shift ;;
    esac
  done
  [[ -n "${manifest}" ]]
  if [[ "${dry_run}" == "true" ]]; then
    printf 'dry-run:%s\n' "$(basename "${manifest}")" >>"${FIXTURE_ROOT}/commands.log"
    python3 - "${manifest}" "${FIXTURE_ROOT}/live.json" <<'PY'
import json
from pathlib import Path
import sys

desired = json.loads(Path(sys.argv[1]).read_text())
live = json.loads(Path(sys.argv[2]).read_text())
desired_metadata = desired["metadata"]
live_metadata = live["metadata"]
desired_metadata["annotations"] = {
    **(live_metadata.get("annotations") or {}),
    **(desired_metadata.get("annotations") or {}),
}
desired_metadata["labels"] = {
    **(live_metadata.get("labels") or {}),
    **(desired_metadata.get("labels") or {}),
}
for field in ("uid", "resourceVersion", "generation", "creationTimestamp", "managedFields"):
    if field in live_metadata:
        desired_metadata[field] = live_metadata[field]
if "status" in live:
    desired["status"] = live["status"]
print(json.dumps(desired, separators=(",", ":"), sort_keys=True))
PY
    exit 0
  fi
  case "$(basename "${manifest}")" in
    forward.json)
      printf '%s\n' 'apply:forward' >>"${FIXTURE_ROOT}/commands.log"
      cp "${FIXTURE_ROOT}/forward-live.json" "${FIXTURE_ROOT}/live.json"
      printf '%s\n' forward >"${FIXTURE_ROOT}/state"
      ;;
    lkg.json)
      printf '%s\n' 'apply:lkg' >>"${FIXTURE_ROOT}/commands.log"
      cp "${FIXTURE_ROOT}/lkg-live.json" "${FIXTURE_ROOT}/live.json"
      printf '%s\n' lkg >"${FIXTURE_ROOT}/state"
      ;;
    *) exit 93 ;;
  esac
  exit 0
fi
printf 'unexpected kubectl command: %q ' "$@" >&2
exit 94
SH

chmod 0700 "${FIXTURE_ROOT}/bin/helm" "${FIXTURE_ROOT}/bin/kubectl"

write_manifest() {
  local destination="$1"
  local image="$2"
  local source_sha="$3"
  local oci_revision="$4"
  local owner="$5"
  local live="$6"
  local keep="$7"
  python3 - "${BASE_MANIFEST}" "${destination}" "${image}" "${source_sha}" "${oci_revision}" "${owner}" "${live}" "${keep}" <<'PY'
import json
import os
from pathlib import Path
import sys

base, destination, image, source_sha, oci_revision, owner, live, keep = sys.argv[1:]
value = json.loads(Path(base).read_text())
metadata = value["metadata"]
metadata["annotations"] = (
    {} if owner == "legacy" else {"fugue.pro/telemetry-ownership": owner}
)
metadata["labels"]["app.kubernetes.io/managed-by"] = (
    "fugue-telemetry-declarative" if owner == "declarative" else "Helm"
)
if source_sha:
    metadata["annotations"]["fugue.pro/telemetry-manifest-revision"] = source_sha
if keep == "true":
    metadata["annotations"]["helm.sh/resource-policy"] = "keep"
value["spec"]["template"]["spec"]["containers"][0]["image"] = image
value["spec"]["template"]["metadata"]["annotations"]["fugue.pro/source-commit"] = oci_revision
if live == "true":
    metadata["resourceVersion"] = "101"
    metadata["uid"] = "telemetry-uid-1"
    if owner == "declarative":
        metadata["managedFields"] = [{"manager": "fugue-telemetry-declarative"}]
    value["status"] = {
        "replicas": 1,
        "readyReplicas": 1,
        "availableReplicas": 1,
        "updatedReplicas": 1,
    }
path = Path(destination)
path.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True))
os.chmod(path, 0o600)
PY
}

write_handoff_support_fixtures() {
  FIXTURE_ROOT="${FIXTURE_ROOT}" python3 - "${CONTROLLER_SOURCE_SHA}" "${CONTROLLER_IMAGE}" <<'PY'
import json
import os
from pathlib import Path
import sys

root = Path(os.environ["FIXTURE_ROOT"])
controller_source, controller_image = sys.argv[1:]
controller_template = {
    "metadata": {
        "annotations": {
            "fugue.pro/source-commit": controller_source,
            "kubectl.kubernetes.io/restartedAt": "2026-01-01T00:00:00Z",
        },
        "labels": {"app.kubernetes.io/component": "controller"},
    },
    "spec": {
        "containers": [
            {
                "env": [{"name": "SYNTHETIC_STABLE", "value": "true"}],
                "image": controller_image,
                "name": "controller",
            }
        ],
        "serviceAccountName": "synthetic-controller",
    },
}
controller = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {"name": "fugue-fugue-controller", "namespace": "fugue-system"},
    "spec": {"template": controller_template},
}
(root / "controller.json").write_text(
    json.dumps(controller, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
    encoding="ascii",
)

items = []
preserved = []
drifted = []
for index in range(9):
    name = f"synthetic-public-edge-{index + 1}"
    mode = "node-local-blue-green-front" if index % 3 == 0 else "node-local-blue-green-worker"
    key = "checksum/edge-blue-green-front" if mode.endswith("front") else "checksum/edge-blue-green-worker"
    checksum = f"{index + 1:064x}"
    items.append(
        {
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": {
                "labels": {
                    "app.kubernetes.io/instance": "fugue",
                    "fugue.io/rollout-mode": mode,
                    "fugue.io/rollout-subsystem": "public-data-plane",
                },
                "name": name,
                "namespace": "fugue-system",
            },
            "spec": {"template": {"metadata": {"annotations": {key: checksum}}}},
        }
    )
    preserved.append(
        "\n".join(
            [
                "---",
                "apiVersion: apps/v1",
                "kind: DaemonSet",
                "metadata:",
                f"  name: {name}",
                "spec:",
                "  template:",
                "    metadata:",
                "      annotations:",
                f"        {key}: {checksum}",
            ]
        )
    )
    drifted.append(
        "\n".join(
            [
                "---",
                "apiVersion: apps/v1",
                "kind: DaemonSet",
                "metadata:",
                f"  name: {name}",
                "spec:",
                "  template:",
                "    metadata:",
                "      annotations:",
                f"        {key}: {'e' * 64}",
                "        synthetic.test/unreleased-template: true",
            ]
        )
    )
(root / "daemonsets.json").write_text(
    json.dumps({"apiVersion": "v1", "items": items, "kind": "List"}, separators=(",", ":"), sort_keys=True),
    encoding="ascii",
)

template_json = json.dumps(controller_template, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
preserved.append(
    "\n".join(
        [
            "---",
            "apiVersion: apps/v1",
            "kind: Deployment",
            "metadata:",
            "  name: fugue-fugue-controller",
            "spec:",
            "  replicas: 2",
            f"  template: {template_json}",
        ]
    )
)
drifted.append(
    "\n".join(
        [
            "---",
            "apiVersion: apps/v1",
            "kind: Deployment",
            "metadata:",
            "  name: fugue-fugue-controller",
            "spec:",
            "  replicas: 2",
            "  template:",
            "    metadata:",
            "      annotations:",
            f"        fugue.pro/source-commit: {controller_source}",
            "        synthetic.test/unreleased-template: true",
            "    spec:",
            "      containers:",
            "        - name: controller",
            f"          image: {controller_image}",
        ]
    )
)
for name, documents in (("preserved-other.yaml", preserved), ("drifted-other.yaml", drifted)):
    (root / name).write_text("\n".join(documents) + "\n", encoding="ascii")
PY
}

reset_fixture() {
  : >"${FIXTURE_ROOT}/commands.log"
  : >"${FIXTURE_ROOT}/live-reads.log"
  : >"${FIXTURE_ROOT}/renderer.log"
  printf '%s\n' initial >"${FIXTURE_ROOT}/state"
  printf '%s\n' 824 >"${FIXTURE_ROOT}/helm-revision"
  printf '%s\n' 0 >"${FIXTURE_ROOT}/helm-status-count"
  printf '%s\n' 0 >"${FIXTURE_ROOT}/daemonset-read-count"
  printf '%s\n' 0 >"${FIXTURE_ROOT}/controller-read-count"
  write_handoff_support_fixtures
  cat >"${FIXTURE_ROOT}/helm-manifest.yaml" <<'YAML'
---
# Source: fugue/templates/telemetry-agent-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: fugue-fugue-telemetry-agent
YAML
  {
    cat "${FIXTURE_ROOT}/helm-manifest.yaml"
    cat "${FIXTURE_ROOT}/drifted-other.yaml"
    cat <<'YAML'
---
# Source: fugue/templates/telemetry-agent-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fugue-fugue-telemetry-agent
  annotations:
    helm.sh/resource-policy: keep
    fugue.pro/telemetry-ownership: helm
  labels:
    app.kubernetes.io/component: telemetry-agent
YAML
  } >"${FIXTURE_ROOT}/stage-helm-manifest.yaml"
  {
    cat "${FIXTURE_ROOT}/helm-manifest.yaml"
    cat "${FIXTURE_ROOT}/drifted-other.yaml"
  } >"${FIXTURE_ROOT}/stage-declarative-manifest.yaml"
  {
    cat "${FIXTURE_ROOT}/helm-manifest.yaml"
    cat "${FIXTURE_ROOT}/preserved-other.yaml"
    cat <<'YAML'
---
# Source: fugue/templates/telemetry-agent-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fugue-fugue-telemetry-agent
  labels:
    app.kubernetes.io/component: telemetry-agent
YAML
  } >"${FIXTURE_ROOT}/legacy-base-manifest.yaml"
  python3 - "${FIXTURE_ROOT}/legacy-base-manifest.yaml" "${FIXTURE_ROOT}/stage-helm-preserved-manifest.yaml" <<'PY'
from pathlib import Path
import sys

source, destination = map(Path, sys.argv[1:])
needle = "metadata:\n  name: fugue-fugue-telemetry-agent\n  labels:\n"
replacement = (
    "metadata:\n"
    "  name: fugue-fugue-telemetry-agent\n"
    "  annotations:\n"
    "    helm.sh/resource-policy: keep\n"
    "    fugue.pro/telemetry-ownership: helm\n"
    "  labels:\n"
)
value = source.read_text()
if value.count(needle) != 1:
    raise SystemExit("telemetry fixture identity invalid")
destination.write_text(value.replace(needle, replacement, 1))
PY
  write_manifest "${FIXTURE_ROOT}/forward.json" "${REPOSITORY}@${FORWARD_DIGEST}" "${SOURCE_SHA}" "${SOURCE_SHA}" declarative false false
  write_manifest "${FIXTURE_ROOT}/lkg.json" "${REPOSITORY}@${LKG_DIGEST}" "${LKG_SOURCE_SHA}" "${LKG_OCI_REVISION}" declarative false false
  write_manifest "${FIXTURE_ROOT}/forward-live.json" "${REPOSITORY}@${FORWARD_DIGEST}" "${SOURCE_SHA}" "${SOURCE_SHA}" declarative true false
  write_manifest "${FIXTURE_ROOT}/lkg-live.json" "${REPOSITORY}@${LKG_DIGEST}" "${LKG_SOURCE_SHA}" "${LKG_OCI_REVISION}" declarative true false
  write_manifest "${FIXTURE_ROOT}/live.json" "${REPOSITORY}@${LKG_DIGEST}" "" "${LKG_OCI_REVISION}" helm true true
}

run_apply() {
  env \
    PATH="${FIXTURE_ROOT}/bin:${PATH}" \
    FIXTURE_ROOT="${FIXTURE_ROOT}" \
    FUGUE_TELEMETRY_NAMESPACE=fugue-system \
    FUGUE_TELEMETRY_HELM_RELEASE=fugue \
    FUGUE_TELEMETRY_HELM_CHART="${REPO_ROOT}/deploy/helm/fugue" \
    FUGUE_TELEMETRY_DEPLOYMENT=fugue-fugue-telemetry-agent \
    FUGUE_TELEMETRY_SERVICE=fugue-fugue-telemetry-agent \
    FUGUE_TELEMETRY_IMAGE_REPOSITORY="${REPOSITORY}" \
    FUGUE_TELEMETRY_IMAGE_DIGEST="${FORWARD_DIGEST}" \
    FUGUE_TELEMETRY_LKG_IMAGE_DIGEST="${TEST_LKG_DIGEST:-${LKG_DIGEST}}" \
    FUGUE_TELEMETRY_SOURCE_SHA="${SOURCE_SHA}" \
    FUGUE_TELEMETRY_LKG_SOURCE_SHA="${LKG_SOURCE_SHA}" \
    FUGUE_TELEMETRY_OCI_REVISION="${SOURCE_SHA}" \
    FUGUE_TELEMETRY_LKG_OCI_REVISION="${TEST_LKG_OCI_REVISION:-${LKG_OCI_REVISION}}" \
    bash "${SCRIPT}" "${FIXTURE_ROOT}/forward.json" "${FIXTURE_ROOT}/lkg.json"
}

reset_fixture
cp "${FIXTURE_ROOT}/lkg-live.json" "${FIXTURE_ROOT}/live.json"
default_live="$(env PATH="${FIXTURE_ROOT}/bin:${PATH}" FIXTURE_ROOT="${FIXTURE_ROOT}" \
  kubectl get deployment fugue-fugue-telemetry-agent --namespace fugue-system --output json)"
shown_live="$(env PATH="${FIXTURE_ROOT}/bin:${PATH}" FIXTURE_ROOT="${FIXTURE_ROOT}" \
  kubectl get deployment fugue-fugue-telemetry-agent --namespace fugue-system \
    --show-managed-fields=true --output json)"
DEFAULT_LIVE="${default_live}" SHOWN_LIVE="${shown_live}" python3 - <<'PY'
import json
import os

default = json.loads(os.environ["DEFAULT_LIVE"])
shown = json.loads(os.environ["SHOWN_LIVE"])
if default.get("metadata", {}).get("managedFields"):
    raise SystemExit("default kubectl output unexpectedly exposed managedFields")
managers = {
    item.get("manager")
    for item in shown.get("metadata", {}).get("managedFields") or []
    if isinstance(item, dict)
}
if "fugue-telemetry-declarative" not in managers:
    raise SystemExit("explicit managedFields output omitted declarative manager")
PY

reset_fixture
cp "${FIXTURE_ROOT}/lkg-live.json" "${FIXTURE_ROOT}/live.json"
run_apply >"${FIXTURE_ROOT}/declarative-resume.log"
[[ "$(cat "${FIXTURE_ROOT}/commands.log")" == $'dry-run:forward.json\ndry-run:lkg.json\napply:forward' ]]
[[ "$(grep -c '^deployment-read:true$' "${FIXTURE_ROOT}/live-reads.log")" -eq 3 ]]
! grep -Fq '^deployment-read:false$' "${FIXTURE_ROOT}/live-reads.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"

reset_fixture
cp "${FIXTURE_ROOT}/lkg-live.json" "${FIXTURE_ROOT}/live.json"
python3 - "${FIXTURE_ROOT}/live.json" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
value = json.loads(path.read_text())
value.get("metadata", {}).pop("managedFields", None)
path.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True))
PY
if run_apply >"${FIXTURE_ROOT}/declarative-missing-manager.log" 2>&1; then
  printf 'declarative live missing field manager was not rejected\n' >&2
  exit 1
fi
grep -Fq 'live-lkg-not-proven' "${FIXTURE_ROOT}/declarative-missing-manager.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

reset_fixture
run_apply >"${FIXTURE_ROOT}/success.log"
[[ "$(cat "${FIXTURE_ROOT}/commands.log")" == $'dry-run:forward.json\ndry-run:lkg.json\napply:forward' ]]

reset_fixture
cp "${FIXTURE_ROOT}/stage-helm-preserved-manifest.yaml" "${FIXTURE_ROOT}/helm-manifest.yaml"
cp "${FIXTURE_ROOT}/lkg-live.json" "${FIXTURE_ROOT}/live.json"
if run_apply >"${FIXTURE_ROOT}/double-writer.log" 2>&1; then
  printf 'double writer was not rejected\n' >&2
  exit 1
fi
grep -Fq 'double-writer-helm-still-desires-telemetry-deployment' "${FIXTURE_ROOT}/double-writer.log"
! grep -Fq 'apply:' "${FIXTURE_ROOT}/commands.log"

reset_fixture
write_manifest "${FIXTURE_ROOT}/live.json" "${REPOSITORY}@${LKG_DIGEST}" "" "${LKG_OCI_REVISION}" helm true false
if run_apply >"${FIXTURE_ROOT}/deletion-risk.log" 2>&1; then
  printf 'Helm deletion risk was not rejected\n' >&2
  exit 1
fi
grep -Fq 'live-lkg-not-proven' "${FIXTURE_ROOT}/deletion-risk.log"
! grep -Fq 'apply:' "${FIXTURE_ROOT}/commands.log"

reset_fixture
write_manifest "${FIXTURE_ROOT}/forward.json" "${REPOSITORY}:mutable" "${SOURCE_SHA}" "${SOURCE_SHA}" declarative false false
if run_apply >"${FIXTURE_ROOT}/wrong-ref.log" 2>&1; then
  printf 'mutable or wrong image ref was not rejected\n' >&2
  exit 1
fi
grep -Fq 'forward-manifest-invalid' "${FIXTURE_ROOT}/wrong-ref.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

reset_fixture
python3 - "${FIXTURE_ROOT}/lkg.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
value = json.loads(path.read_text())
value["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = ":9999"
path.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True))
PY
if run_apply >"${FIXTURE_ROOT}/lkg-env-drift.log" 2>&1; then
  printf 'LKG env drift was not rejected before write\n' >&2
  exit 1
fi
grep -Fq 'initial-transition-proof-failed' "${FIXTURE_ROOT}/lkg-env-drift.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

reset_fixture
if HEALTH_FAIL=true run_apply >"${FIXTURE_ROOT}/rollback.log" 2>&1; then
  printf 'failed health gate incorrectly reported success\n' >&2
  exit 1
fi
grep -Fq 'forward-rollout-failed-lkg-restored' "${FIXTURE_ROOT}/rollback.log"
[[ "$(tail -n 2 "${FIXTURE_ROOT}/commands.log")" == $'apply:forward\napply:lkg' ]]
cmp -s "${FIXTURE_ROOT}/live.json" "${FIXTURE_ROOT}/lkg-live.json"
python3 - "${FIXTURE_ROOT}/live.json" "${LKG_OCI_REVISION}" <<'PY'
import json
from pathlib import Path
import sys

value = json.loads(Path(sys.argv[1]).read_text())
observed = value["spec"]["template"]["metadata"]["annotations"].get("fugue.pro/source-commit")
if observed != sys.argv[2]:
    raise SystemExit("rollback did not restore exact LKG Pod provenance")
PY

prepare_legacy_bootstrap() {
  reset_fixture
  cp "${FIXTURE_ROOT}/legacy-base-manifest.yaml" "${FIXTURE_ROOT}/helm-manifest.yaml"
  printf '%s\n' 822 >"${FIXTURE_ROOT}/helm-revision"
  write_manifest "${FIXTURE_ROOT}/live.json" "${REPOSITORY}@${LKG_DIGEST}" "" "${LKG_OCI_REVISION}" legacy true false
  python3 - "${FIXTURE_ROOT}/live.json" <<'PY'
import json
from pathlib import Path
import sys
path = Path(sys.argv[1])
value = json.loads(path.read_text())
value["spec"]["template"]["metadata"].pop("annotations", None)
path.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True))
PY
}

prepare_legacy_bootstrap
[[ "$(grep -c 'synthetic.test/unreleased-template: true' "${FIXTURE_ROOT}/stage-helm-manifest.yaml")" -eq 10 ]]
run_apply >"${FIXTURE_ROOT}/bootstrap.log"
[[ "$(grep -c '^helm-stage:helm$' "${FIXTURE_ROOT}/commands.log")" -eq 1 ]]
[[ "$(grep -c '^helm-stage:declarative$' "${FIXTURE_ROOT}/commands.log")" -eq 1 ]]
grep -Fxq 'apply:forward' "${FIXTURE_ROOT}/commands.log"
python3 - "${FIXTURE_ROOT}/bootstrap.log" <<'PY'
import json
from pathlib import Path
import re
import sys

prefix = "telemetry-declarative:handoff-seal="
lines = [line for line in Path(sys.argv[1]).read_text().splitlines() if line.startswith(prefix)]
if len(lines) != 1:
    raise SystemExit("handoff seal receipt cardinality invalid")
raw = lines[0][len(prefix):]
value = json.loads(raw)
if raw != json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True):
    raise SystemExit("handoff seal receipt is not canonical")
if value.get("kind") != "TelemetryHelmHandoffSealReceipt" or value.get("status") != "sealed-before-write":
    raise SystemExit("handoff seal receipt identity invalid")
if value.get("helmBaseRevision") != 822 or len(value.get("publicDataPlane") or []) != 9:
    raise SystemExit("handoff seal receipt inventory invalid")
for key in ("controllerTemplateDigest", "helmBaseManifestDigest", "publicDataPlaneDigest", "rendererDigest"):
    if re.fullmatch(r"sha256:[0-9a-f]{64}", value.get(key, "")) is None:
        raise SystemExit(f"handoff seal receipt {key} invalid")
PY
python3 - "${FIXTURE_ROOT}/renderer.log" <<'PY'
from pathlib import Path
import sys

lines = Path(sys.argv[1]).read_text().splitlines()
if len(lines) != 4:
    raise SystemExit("sealed renderer was not reused exactly four times")
records = [line.split(":", 4) for line in lines]
expected = [
    ["renderer-use", "helm", "true"],
    ["renderer-use", "declarative", "true"],
    ["renderer-use", "helm", "false"],
    ["renderer-use", "declarative", "false"],
]
if [record[:3] for record in records] != expected:
    raise SystemExit("sealed renderer use order invalid")
if len({record[3] for record in records}) != 1 or len({record[4] for record in records}) != 1:
    raise SystemExit("renderer digest or path changed between Helm stages")
PY
grep -Fq 'FUGUE_UPGRADE_LIB_ONLY=true source "${UPGRADE_HELPER}"' "${SCRIPT}"
! grep -Fq 'CONFIG_B64 =' "${SCRIPT}"

prepare_legacy_bootstrap
if EDGE_CHECKSUM_DRIFT=true run_apply >"${FIXTURE_ROOT}/edge-drift.log" 2>&1; then
  printf 'Edge checksum drift was not rejected before Helm write\n' >&2
  exit 1
fi
grep -Fq 'helm-handoff-sealed-input-drift-before-stage1' "${FIXTURE_ROOT}/edge-drift.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

prepare_legacy_bootstrap
if CONTROLLER_TEMPLATE_DRIFT=true run_apply >"${FIXTURE_ROOT}/controller-drift.log" 2>&1; then
  printf 'Controller template drift was not rejected before Helm write\n' >&2
  exit 1
fi
grep -Fq 'helm-handoff-sealed-input-drift-before-stage1' "${FIXTURE_ROOT}/controller-drift.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

prepare_legacy_bootstrap
if HELM_REVISION_DRIFT=true run_apply >"${FIXTURE_ROOT}/helm-revision-drift.log" 2>&1; then
  printf 'Helm revision drift was not rejected before Helm write\n' >&2
  exit 1
fi
grep -Fq 'helm-handoff-sealed-input-drift-before-stage1' "${FIXTURE_ROOT}/helm-revision-drift.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

prepare_legacy_bootstrap
python3 - "${FIXTURE_ROOT}/live.json" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
value = json.loads(path.read_text())
value["spec"]["template"].setdefault("metadata", {}).setdefault("annotations", {})["fugue.pro/source-commit"] = "4" * 40
path.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True))
PY
if run_apply >"${FIXTURE_ROOT}/legacy-wrong-source.log" 2>&1; then
  printf 'legacy live wrong non-empty source was not rejected\n' >&2
  exit 1
fi
grep -Fq 'initial-transition-proof-failed' "${FIXTURE_ROOT}/legacy-wrong-source.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

prepare_legacy_bootstrap
python3 - "${FIXTURE_ROOT}/live.json" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
value = json.loads(path.read_text())
value["spec"]["template"]["spec"]["containers"][0]["image"] = "ghcr.io/example/fugue-telemetry-agent@sha256:" + "9" * 64
path.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True))
PY
if run_apply >"${FIXTURE_ROOT}/legacy-wrong-digest.log" 2>&1; then
  printf 'legacy live wrong digest was not rejected\n' >&2
  exit 1
fi
grep -Fq 'initial-transition-proof-failed' "${FIXTURE_ROOT}/legacy-wrong-digest.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

prepare_legacy_bootstrap
if TEST_LKG_OCI_REVISION=5555555555555555555555555555555555555555 run_apply >"${FIXTURE_ROOT}/legacy-wrong-registry-source.log" 2>&1; then
  printf 'legacy wrong registry source evidence was not rejected\n' >&2
  exit 1
fi
grep -Fq 'lkg-manifest-invalid' "${FIXTURE_ROOT}/legacy-wrong-registry-source.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

reset_fixture
cp "${FIXTURE_ROOT}/stage-helm-preserved-manifest.yaml" "${FIXTURE_ROOT}/helm-manifest.yaml"
python3 - "${FIXTURE_ROOT}/live.json" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
value = json.loads(path.read_text())
value["spec"]["template"]["metadata"].pop("annotations", None)
path.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True))
PY
run_apply >"${FIXTURE_ROOT}/handoff-empty-resume.log"
[[ "$(grep -c '^helm-stage:helm$' "${FIXTURE_ROOT}/commands.log")" -eq 0 ]]
[[ "$(grep -c '^helm-stage:declarative$' "${FIXTURE_ROOT}/commands.log")" -eq 1 ]]
grep -Fxq 'apply:forward' "${FIXTURE_ROOT}/commands.log"

reset_fixture
cp "${FIXTURE_ROOT}/lkg-live.json" "${FIXTURE_ROOT}/live.json"
python3 - "${FIXTURE_ROOT}/live.json" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
value = json.loads(path.read_text())
value["spec"]["template"]["metadata"].pop("annotations", None)
path.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True))
PY
if run_apply >"${FIXTURE_ROOT}/declarative-empty-source.log" 2>&1; then
  printf 'declarative live empty source was not rejected\n' >&2
  exit 1
fi
grep -Fq 'initial-transition-proof-failed' "${FIXTURE_ROOT}/declarative-empty-source.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

prepare_legacy_bootstrap
python3 - "${FIXTURE_ROOT}/stage-helm-manifest.yaml" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
value = path.read_text().replace(
    "kind: Service\nmetadata:\n  name: fugue-fugue-telemetry-agent\n",
    "kind: Service\nmetadata:\n  name: fugue-fugue-telemetry-agent\n  annotations:\n    unexpected: drift\n",
    1,
)
path.write_text(value)
PY
if run_apply >"${FIXTURE_ROOT}/stage1-extra-diff.log" 2>&1; then
  printf 'stage1 extra object diff was not rejected\n' >&2
  exit 1
fi
grep -Fq 'helm-handoff-render-proof-failed' "${FIXTURE_ROOT}/stage1-extra-diff.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

prepare_legacy_bootstrap
python3 - "${FIXTURE_ROOT}/stage-declarative-manifest.yaml" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
value = path.read_text().replace(
    "kind: Service\nmetadata:\n  name: fugue-fugue-telemetry-agent\n",
    "kind: Service\nmetadata:\n  name: fugue-fugue-telemetry-agent\n  annotations:\n    unexpected: drift\n",
    1,
)
path.write_text(value)
PY
if run_apply >"${FIXTURE_ROOT}/stage2-extra-diff.log" 2>&1; then
  printf 'stage2 extra object diff was not rejected\n' >&2
  exit 1
fi
grep -Fq 'helm-handoff-render-proof-failed' "${FIXTURE_ROOT}/stage2-extra-diff.log"
! grep -Fq '^helm-stage:' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"

prepare_legacy_bootstrap
if HELM_STAGE1_FAIL_MODE=old run_apply >"${FIXTURE_ROOT}/stage1-old.log" 2>&1; then
  printf 'uncommitted stage1 incorrectly continued\n' >&2
  exit 1
fi
grep -Fq 'helm-stage1-not-committed:rc=41' "${FIXTURE_ROOT}/stage1-old.log"
[[ "$(grep -c '^helm-stage:helm$' "${FIXTURE_ROOT}/commands.log")" -eq 1 ]]
! grep -Fq 'helm-stage:declarative' "${FIXTURE_ROOT}/commands.log"
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"
test -s "${FIXTURE_ROOT}/live.json"

prepare_legacy_bootstrap
HELM_STAGE1_FAIL_MODE=committed run_apply >"${FIXTURE_ROOT}/stage1-committed.log"
[[ "$(grep -c '^helm-stage:helm$' "${FIXTURE_ROOT}/commands.log")" -eq 1 ]]
[[ "$(grep -c '^helm-stage:declarative$' "${FIXTURE_ROOT}/commands.log")" -eq 1 ]]
grep -Fxq 'apply:forward' "${FIXTURE_ROOT}/commands.log"

prepare_legacy_bootstrap
if HELM_STAGE2_FAIL_MODE=old run_apply >"${FIXTURE_ROOT}/stage2-old.log" 2>&1; then
  printf 'uncommitted stage2 incorrectly continued\n' >&2
  exit 1
fi
grep -Fq 'helm-stage2-not-committed:rc=43' "${FIXTURE_ROOT}/stage2-old.log"
[[ "$(grep -c '^helm-stage:helm$' "${FIXTURE_ROOT}/commands.log")" -eq 1 ]]
[[ "$(grep -c '^helm-stage:declarative$' "${FIXTURE_ROOT}/commands.log")" -eq 1 ]]
! grep -Fq '^apply:' "${FIXTURE_ROOT}/commands.log"
grep -Fq 'fugue.pro/telemetry-ownership: helm' "${FIXTURE_ROOT}/helm-manifest.yaml"
test -s "${FIXTURE_ROOT}/live.json"

prepare_legacy_bootstrap
HELM_STAGE2_FAIL_MODE=committed run_apply >"${FIXTURE_ROOT}/stage2-committed.log"
[[ "$(grep -c '^helm-stage:helm$' "${FIXTURE_ROOT}/commands.log")" -eq 1 ]]
[[ "$(grep -c '^helm-stage:declarative$' "${FIXTURE_ROOT}/commands.log")" -eq 1 ]]
grep -Fxq 'apply:forward' "${FIXTURE_ROOT}/commands.log"

printf 'telemetry declarative apply tests passed\n'
