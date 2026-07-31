#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "${TMP_ROOT}"' EXIT

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

assert_eq() {
  local actual="$1"
  local expected="$2"
  local context="$3"
  [[ "${actual}" == "${expected}" ]] || fail "${context}: expected '${expected}', got '${actual}'"
}

output_value() {
  local file="$1"
  local key="$2"
  awk -F= -v key="${key}" '$1 == key { print substr($0, length(key) + 2); exit }' "${file}"
}

multiline_output_value() {
  local file="$1"
  local key="$2"
  awk -v key="${key}" '
    substr($0, 1, length(key) + 2) == key "<<" {
      delimiter = substr($0, length(key) + 3)
      capture = 1
      next
    }
    capture && $0 == delimiter { exit }
    capture { print }
  ' "${file}"
}

kubectl_call_count() {
  local output_file="$1"
  local target="$2"
  awk -v target="${target}" '$0 == target { count++ } END { print count + 0 }' "${output_file}.kubectl-calls"
}

FAKE_BIN="${TMP_ROOT}/bin"
mkdir -p "${FAKE_BIN}"
cat >"${FAKE_BIN}/kubectl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

target=""
previous=""
for argument in "$@"; do
  if [[ "${previous}" == "get" ]]; then
    target="${argument}"
    break
  fi
  previous="${argument}"
done
[[ -n "${target}" ]]
printf '%s\n' "${target}" >>"${FAKE_KUBECTL_CALLS_FILE}"
exec 3>&1
[[ "$(python3 -c 'import os, stat; print(oct(stat.S_IMODE(os.fstat(3).st_mode))[2:])')" == "600" ]] || {
  printf 'kubectl snapshot output file is not mode 0600\n' >&2
  exit 16
}
exec 3>&-
python3 -c '
import json
import sys

fixture, target = sys.argv[1:3]
with open(fixture, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
if target == "daemonsets,configmaps" and target not in objects and "ds" in objects:
    daemonsets = objects["ds"] or {}
    if isinstance(daemonsets, dict) and "__raw_nul_json__" in daemonsets:
        raw = json.dumps({"apiVersion": "v1", "kind": "List", "items": daemonsets["__raw_nul_json__"].get("items") or []}, separators=(",", ":")).encode("utf-8")
        sys.stdout.buffer.write(b"{\x00" + raw[1:])
        raise SystemExit(0)
    if isinstance(daemonsets, dict) and "__error__" in daemonsets:
        print(str(daemonsets["__error__"]), file=sys.stderr)
        raise SystemExit(13)
    if isinstance(daemonsets, dict) and "__raw__" in daemonsets:
        print(str(daemonsets["__raw__"]))
        raise SystemExit(0)
    items = list(daemonsets.get("items") or []) if isinstance(daemonsets, dict) else []
    for key, item in objects.items():
        if key.startswith("configmap/") and isinstance(item, dict):
            items.append(item)
    print(json.dumps({"apiVersion": "v1", "kind": "List", "items": items}, separators=(",", ":")))
    raise SystemExit(0)
if target not in objects:
    raise SystemExit(0)
value = objects[target]
if isinstance(value, dict) and "__raw_nul_json__" in value:
    raw = json.dumps(value["__raw_nul_json__"], separators=(",", ":")).encode("utf-8")
    if not raw.startswith(b"{"):
        raise SystemExit(15)
    sys.stdout.buffer.write(b"{\x00" + raw[1:])
    raise SystemExit(0)
if isinstance(value, dict) and "__error__" in value:
    print(str(value["__error__"]), file=sys.stderr)
    raise SystemExit(13)
if isinstance(value, dict) and "__raw__" in value:
    print(str(value["__raw__"]))
    raise SystemExit(0)
if isinstance(value, dict) and "__sequence__" in value:
    sequence = value["__sequence__"]
    if not isinstance(sequence, list) or not sequence:
        raise SystemExit(14)
    with open(sys.argv[3], "r", encoding="utf-8") as handle:
        occurrence = sum(1 for line in handle if line.rstrip("\n") == target)
    value = sequence[min(occurrence - 1, len(sequence) - 1)]
print(json.dumps(value, separators=(",", ":")))
' "${FAKE_KUBE_OBJECTS_FILE}" "${target}" "${FAKE_KUBECTL_CALLS_FILE}"
SH
cat >"${FAKE_BIN}/helm" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

printf 'call\n' >>"${FAKE_HELM_CALLS_FILE}"
[[ "${FAKE_HELM_MODE:-success}" == "success" ]] || exit 1
[[ "$*" == "get values fugue -n fugue-system --all -o json" ]] || {
  printf 'unexpected Helm values query: %s\n' "$*" >&2
  exit 1
}
exec 3>&1
[[ "$(python3 -c 'import os, stat; print(oct(stat.S_IMODE(os.fstat(3).st_mode))[2:])')" == "600" ]] || {
  printf 'Helm values output file is not mode 0600\n' >&2
  exit 1
}
exec 3>&-
cat "${FAKE_HELM_VALUES_FILE}"
SH
chmod +x "${FAKE_BIN}/kubectl" "${FAKE_BIN}/helm"

run_resolver() {
  local objects_file="$1"
  local values_file="$2"
  local output_file="$3"
  local stdout_file="$4"
  local stderr_file="$5"
  local calls_file="$6"

  : >"${output_file}"
  : >"${stdout_file}"
  : >"${stderr_file}"
  : >"${calls_file}"
  : >"${output_file}.kubectl-calls"
  PATH="${FAKE_BIN}:${PATH}" \
    GITHUB_OUTPUT="${output_file}" \
    FUGUE_RELEASE_NAME=fugue \
    FUGUE_NAMESPACE=fugue-system \
    FUGUE_RELEASE_FULLNAME=fugue-fugue \
    FUGUE_IMAGE_TAG="${RESOLVER_FALLBACK_TAG-fallback-target}" \
    FUGUE_RESOLVE_HELM_IMAGE_BASELINES="${FUGUE_RESOLVE_HELM_IMAGE_BASELINES:-false}" \
    GITHUB_SHA="${RESOLVER_GITHUB_SHA:-}" \
    FAKE_KUBE_OBJECTS_FILE="${objects_file}" \
    FAKE_KUBECTL_CALLS_FILE="${output_file}.kubectl-calls" \
    FAKE_HELM_VALUES_FILE="${values_file}" \
    FAKE_HELM_CALLS_FILE="${calls_file}" \
    FAKE_HELM_MODE="${FAKE_HELM_MODE:-success}" \
    "${REPO_ROOT}/scripts/resolve_control_plane_live_images.sh" >"${stdout_file}" 2>"${stderr_file}"
}

DIGEST_A="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
DIGEST_B="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
DIGEST_C="sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
DIGEST_D="sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
DIGEST_E="sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
DIGEST_F="sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

TAG_OBJECTS="${TMP_ROOT}/tag-objects.json"
TAG_VALUES="${TMP_ROOT}/tag-values.json"
TAG_OUTPUT="${TMP_ROOT}/tag-output"
TAG_STDOUT="${TMP_ROOT}/tag-stdout"
TAG_STDERR="${TMP_ROOT}/tag-stderr"
TAG_CALLS="${TMP_ROOT}/tag-calls"
cat >"${TAG_OBJECTS}" <<JSON
{
  "deploy/fugue-fugue-api": {"spec":{"template":{"spec":{"containers":[{"name":"api","image":"registry.example:5000/team/api:api-live"}]}}}},
  "deploy/fugue-fugue-controller": {"spec":{"template":{"spec":{"containers":[{"name":"controller","image":"ghcr.io/acme/controller:controller-live","env":[
    {"name":"FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY","value":"ghcr.io/acme/drain"},
    {"name":"FUGUE_DRAIN_AGENT_IMAGE_TAG","value":"drain-live"},
    {"name":"FUGUE_DRAIN_AGENT_IMAGE_DIGEST"}
  ]}]}}}},
  "ds/fugue-fugue-image-cache": {"spec":{"template":{"spec":{"containers":[{"name":"image-cache","image":"ghcr.io/acme/cache:cache-live"}]}}}},
  "ds/fugue-fugue-edge": {"spec":{"template":{"spec":{"containers":[{"name":"edge","image":"ghcr.io/acme/edge:edge-live"}]}}}},
  "ds": {"items":[
    {"metadata":{"name":"fugue-fugue-edge-front","labels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"edge-front","fugue.io/rollout-subsystem":"public-data-plane","fugue.io/rollout-mode":"node-local-blue-green-front"}},"spec":{"template":{"spec":{"containers":[{"name":"edge-front","image":"registry.example:5000/team/front:front-live@${DIGEST_A}"}]}}}},
    {"metadata":{"name":"fugue-fugue-edge-ssh-front","labels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"fugue-ssh-front","fugue.io/rollout-subsystem":"public-data-plane","fugue.io/rollout-mode":"node-local-blue-green-front"}},"spec":{"template":{"spec":{"containers":[{"name":"ssh-front","image":"ghcr.io/acme/ssh:ignored"}]}}}}
  ]}
}
JSON
printf '%s\n' '{"bootstrapAdminKey":"must-not-be-read"}' >"${TAG_VALUES}"
run_resolver "${TAG_OBJECTS}" "${TAG_VALUES}" "${TAG_OUTPUT}" "${TAG_STDOUT}" "${TAG_STDERR}" "${TAG_CALLS}"
assert_eq "$(wc -l <"${TAG_CALLS}" | tr -d ' ')" "0" "tag-only references must not read Helm values"
assert_eq "$(output_value "${TAG_OUTPUT}" api_image_repository)" "registry.example:5000/team/api" "registry port repository"
assert_eq "$(output_value "${TAG_OUTPUT}" api_image_source_tag)" "api-live" "tag source"
assert_eq "$(output_value "${TAG_OUTPUT}" api_image_template_ref)" "registry.example:5000/team/api:api-live" "tag template ref"
assert_eq "$(output_value "${TAG_OUTPUT}" telemetry_agent_image_baseline_ref)" "" "absent optional live baseline"
assert_eq "$(output_value "${TAG_OUTPUT}" telemetry_agent_image_template_ref)" "ghcr.io/yym68686/fugue-telemetry-agent:fallback-target" "absent optional fallback"
assert_eq "$(kubectl_call_count "${TAG_OUTPUT}" deploy/fugue-fugue-telemetry-agent)" "1" "true NotFound is queried once and may use fallback"
assert_eq "$(output_value "${TAG_OUTPUT}" drain_agent_image_source_tag)" "drain-live" "drain source tag"
assert_eq "$(kubectl_call_count "${TAG_OUTPUT}" deploy/fugue-fugue-controller)" "1" "controller image and drain metadata share one Deployment GET"
assert_eq "$(output_value "${TAG_OUTPUT}" public_cohort_image_count)" "1" "SSH front exclusion"
assert_eq "$(multiline_output_value "${TAG_OUTPUT}" public_cohort_image_template_refs)" "fugue-fugue-edge-front|registry.example:5000/team/front:front-live@${DIGEST_A}" "tag+digest cohort ref"
assert_eq "$(multiline_output_value "${TAG_OUTPUT}" public_cohort_image_source_tags)" "fugue-fugue-edge-front|front-live" "tag+digest cohort source tag"
assert_eq "$(multiline_output_value "${TAG_OUTPUT}" release_baseline_tags)" $'api-live\ncontroller-live' "legacy release baselines"

TAG_HELM_BASELINE_VALUES="${TMP_ROOT}/tag-helm-baseline-values.json"
cat >"${TAG_HELM_BASELINE_VALUES}" <<JSON
{
  "api":{"image":{"repository":"registry.example:5000/team/api","tag":"api-live","digest":""}},
  "controller":{"image":{"repository":"ghcr.io/acme/controller","tag":"controller-helm","digest":""}},
  "runtime":{"strictDrain":{"agent":{"image":{"repository":"ghcr.io/acme/drain","tag":"drain-live","digest":""}}}},
  "observability":{"agent":{"image":{"repository":"ghcr.io/acme/telemetry","tag":"telemetry-live","digest":""}}},
  "imageCache":{"image":{"repository":"ghcr.io/acme/cache","tag":"cache-live","digest":""}},
  "edge":{"image":{"repository":"ghcr.io/acme/edge","tag":"edge-live","digest":""}}
}
JSON
TAG_HELM_BASELINE_OUTPUT="${TMP_ROOT}/tag-helm-baseline-output"
FUGUE_RESOLVE_HELM_IMAGE_BASELINES=true run_resolver \
  "${TAG_OBJECTS}" \
  "${TAG_HELM_BASELINE_VALUES}" \
  "${TAG_HELM_BASELINE_OUTPUT}" \
  "${TMP_ROOT}/tag-helm-baseline-stdout" \
  "${TMP_ROOT}/tag-helm-baseline-stderr" \
  "${TMP_ROOT}/tag-helm-baseline-calls"
assert_eq "$(wc -l <"${TMP_ROOT}/tag-helm-baseline-calls" | tr -d ' ')" "1" "Helm baseline mode uses one values snapshot"
assert_eq "$(output_value "${TAG_HELM_BASELINE_OUTPUT}" api_image_baseline_ref)" "api-live" "matching API Helm baseline"
assert_eq "$(output_value "${TAG_HELM_BASELINE_OUTPUT}" api_image_helm_drift)" "false" "matching API Helm drift"
assert_eq "$(output_value "${TAG_HELM_BASELINE_OUTPUT}" controller_image_tag)" "controller-live" "drift recovery preserves the observed live tag"
assert_eq "$(output_value "${TAG_HELM_BASELINE_OUTPUT}" controller_image_baseline_ref)" "controller-helm" "drift recovery trusts the Helm revision source tag"
assert_eq "$(output_value "${TAG_HELM_BASELINE_OUTPUT}" controller_image_helm_drift)" "true" "controller Helm/live drift"
assert_eq "$(multiline_output_value "${TAG_HELM_BASELINE_OUTPUT}" release_baseline_tags)" $'api-live\ncontroller-helm' "release baselines follow the Helm revision"
grep -Fq 'controller image drift: live=ghcr.io/acme/controller:controller-live helm=ghcr.io/acme/controller:controller-helm' \
  "${TMP_ROOT}/tag-helm-baseline-stderr" || fail "controller Helm/live drift must be observable"

if FUGUE_RESOLVE_HELM_IMAGE_BASELINES=invalid run_resolver \
  "${TAG_OBJECTS}" \
  "${TAG_HELM_BASELINE_VALUES}" \
  "${TMP_ROOT}/invalid-helm-baseline-output" \
  "${TMP_ROOT}/invalid-helm-baseline-stdout" \
  "${TMP_ROOT}/invalid-helm-baseline-stderr" \
  "${TMP_ROOT}/invalid-helm-baseline-calls"; then
  fail "invalid Helm baseline mode must fail closed"
fi
assert_eq "$(wc -c <"${TMP_ROOT}/invalid-helm-baseline-output" | tr -d ' ')" "0" "invalid Helm baseline mode emits no outputs"

EXPLICIT_EMPTY_DIGEST_OBJECTS="${TMP_ROOT}/explicit-empty-digest-objects.json"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
container = objects["deploy/fugue-fugue-controller"]["spec"]["template"]["spec"]["containers"][0]
for item in container["env"]:
    if item["name"] == "FUGUE_DRAIN_AGENT_IMAGE_DIGEST":
        item["value"] = ""
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${TAG_OBJECTS}" "${EXPLICIT_EMPTY_DIGEST_OBJECTS}"
run_resolver "${EXPLICIT_EMPTY_DIGEST_OBJECTS}" "${TAG_VALUES}" "${TMP_ROOT}/explicit-empty-digest-output" "${TMP_ROOT}/explicit-empty-digest-stdout" "${TMP_ROOT}/explicit-empty-digest-stderr" "${TMP_ROOT}/explicit-empty-digest-calls"
assert_eq "$(output_value "${TMP_ROOT}/explicit-empty-digest-output" drain_agent_image_source_tag)" "drain-live" "explicit empty drain digest equals the Kubernetes name-only representation"
assert_eq "$(output_value "${TMP_ROOT}/explicit-empty-digest-output" drain_agent_image_digest)" "" "explicit empty drain digest remains empty"

REVERSE_EDGE_RACE_OBJECTS="${TMP_ROOT}/reverse-edge-race-objects.json"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
objects.pop("ds/fugue-fugue-edge", None)
objects["ds"]["items"].append({
    "metadata": {"name": "fugue-fugue-edge-worker-a", "labels": {"app.kubernetes.io/instance": "fugue", "app.kubernetes.io/component": "edge-worker-a", "fugue.io/rollout-subsystem": "public-data-plane", "fugue.io/rollout-mode": "node-local-blue-green-worker", "fugue.io/edge-slot": "a"}},
    "spec": {"template": {"spec": {"containers": [{"name": "edge", "image": "ghcr.io/acme/edge:appeared-live"}]}}},
})
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${TAG_OBJECTS}" "${REVERSE_EDGE_RACE_OBJECTS}"
if run_resolver "${REVERSE_EDGE_RACE_OBJECTS}" "${TAG_VALUES}" "${TMP_ROOT}/reverse-edge-race-output" "${TMP_ROOT}/reverse-edge-race-stdout" "${TMP_ROOT}/reverse-edge-race-stderr" "${TMP_ROOT}/reverse-edge-race-calls"; then
  fail "public worker appearing after both exact edge GETs were NotFound must fail closed"
fi
assert_eq "$(wc -c <"${TMP_ROOT}/reverse-edge-race-output" | tr -d ' ')" "0" "reverse edge snapshot race must not emit partial outputs"

EOF_TAG_OBJECTS="${TMP_ROOT}/eof-tag-objects.json"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
objects["deploy/fugue-fugue-api"]["spec"]["template"]["spec"]["containers"][0]["image"] = "registry.example:5000/team/api:EOF"
objects["deploy/fugue-fugue-controller"]["spec"]["template"]["spec"]["containers"][0]["image"] = "ghcr.io/acme/controller:FUGUE_release_baseline_tags_EOF"
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${TAG_OBJECTS}" "${EOF_TAG_OBJECTS}"
run_resolver "${EOF_TAG_OBJECTS}" "${TAG_VALUES}" "${TMP_ROOT}/eof-tag-output" "${TMP_ROOT}/eof-tag-stdout" "${TMP_ROOT}/eof-tag-stderr" "${TMP_ROOT}/eof-tag-calls"
assert_eq "$(output_value "${TMP_ROOT}/eof-tag-output" api_image_source_tag)" "EOF" "EOF is a legal source tag"
assert_eq "$(multiline_output_value "${TMP_ROOT}/eof-tag-output" release_baseline_tags)" $'EOF\nFUGUE_release_baseline_tags_EOF' "dynamic output delimiter preserves EOF and delimiter-shaped baseline lines"
grep -Fqx 'release_baseline_tags<<FUGUE_release_baseline_tags_EOF_1' "${TMP_ROOT}/eof-tag-output" || fail "multiline delimiter collision must select a non-conflicting suffix"
if grep -Fqx 'release_baseline_tags<<EOF' "${TMP_ROOT}/eof-tag-output"; then
  fail "multiline output must not use a fixed EOF delimiter"
fi

ROGUE_COHORT_OBJECTS="${TMP_ROOT}/rogue-cohort-objects.json"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
objects["ds"]["items"].append({
    "metadata": {
        "name": "rogue-edge-front",
        "labels": {
            "app.kubernetes.io/instance": "fugue",
            "app.kubernetes.io/component": "edge-country-us-front",
            "fugue.io/rollout-subsystem": "public-data-plane",
            "fugue.io/rollout-mode": "node-local-blue-green-front",
        },
    },
    "spec": {"template": {"spec": {"containers": [{"name": "edge-front", "image": "ghcr.io/acme/rogue:live"}]}}},
})
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${TAG_OBJECTS}" "${ROGUE_COHORT_OBJECTS}"
if run_resolver "${ROGUE_COHORT_OBJECTS}" "${TAG_VALUES}" "${TMP_ROOT}/rogue-output" "${TMP_ROOT}/rogue-stdout" "${TMP_ROOT}/rogue-stderr" "${TMP_ROOT}/rogue-calls"; then
  fail "public cohort name/component mismatch must fail closed"
fi
assert_eq "$(wc -c <"${TMP_ROOT}/rogue-output" | tr -d ' ')" "0" "rogue cohort failure must not emit partial outputs"

FULL_TAG_OBJECTS="${TMP_ROOT}/full-tag-objects.json"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
objects["deploy/fugue-fugue-telemetry-agent"] = {
    "spec": {"template": {"spec": {"containers": [{"name": "telemetry-agent", "image": "ghcr.io/acme/telemetry:telemetry-live"}]}}}
}
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${TAG_OBJECTS}" "${FULL_TAG_OBJECTS}"
RESOLVER_FALLBACK_TAG="" run_resolver "${FULL_TAG_OBJECTS}" "${TAG_VALUES}" "${TMP_ROOT}/no-fallback-live-output" "${TMP_ROOT}/no-fallback-live-stdout" "${TMP_ROOT}/no-fallback-live-stderr" "${TMP_ROOT}/no-fallback-live-calls"
assert_eq "$(output_value "${TMP_ROOT}/no-fallback-live-output" telemetry_agent_image_source_tag)" "telemetry-live" "complete live inventory does not require a fallback tag"
EMPTY_COHORT_OBJECTS="${TMP_ROOT}/empty-cohort-objects.json"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
objects["ds"] = {"items": []}
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${FULL_TAG_OBJECTS}" "${EMPTY_COHORT_OBJECTS}"
run_resolver "${EMPTY_COHORT_OBJECTS}" "${TAG_VALUES}" "${TMP_ROOT}/empty-cohort-output" "${TMP_ROOT}/empty-cohort-stdout" "${TMP_ROOT}/empty-cohort-stderr" "${TMP_ROOT}/empty-cohort-calls"
assert_eq "$(output_value "${TMP_ROOT}/empty-cohort-output" public_cohort_image_count)" "0" "only an explicit empty items array is an empty public cohort"
if RESOLVER_FALLBACK_TAG="" run_resolver "${TAG_OBJECTS}" "${TAG_VALUES}" "${TMP_ROOT}/no-fallback-missing-output" "${TMP_ROOT}/no-fallback-missing-stdout" "${TMP_ROOT}/no-fallback-missing-stderr" "${TMP_ROOT}/no-fallback-missing-calls"; then
  fail "missing live workload without an explicit fallback tag must fail closed"
fi
assert_eq "$(wc -c <"${TMP_ROOT}/no-fallback-missing-output" | tr -d ' ')" "0" "missing fallback failure must not emit partial outputs"

NO_COHORT_OBJECTS="${TMP_ROOT}/no-cohort-objects.json"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
objects.pop("ds", None)
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${FULL_TAG_OBJECTS}" "${NO_COHORT_OBJECTS}"
if run_resolver "${NO_COHORT_OBJECTS}" "${TAG_VALUES}" "${TMP_ROOT}/no-cohort-output" "${TMP_ROOT}/no-cohort-stdout" "${TMP_ROOT}/no-cohort-stderr" "${TMP_ROOT}/no-cohort-calls"; then
  fail "unreadable public cohort inventory must fail closed"
fi
assert_eq "$(wc -c <"${TMP_ROOT}/no-cohort-output" | tr -d ' ')" "0" "cohort read failure must not emit partial outputs"

for resource_failure in forbidden invalid_json missing_container controller_missing invalid_controller_env nul_image newline_controller value_from_controller value_and_value_from_controller null_value_controller number_value_controller duplicate_digest_controller tab_public pipe_controller raw_nul_resource raw_nul_public; do
  FAILURE_OBJECTS="${TMP_ROOT}/resource-${resource_failure}-objects.json"
  python3 -c '
import json
import sys

source, target, mode = sys.argv[1:4]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
if mode == "forbidden":
    objects["deploy/fugue-fugue-telemetry-agent"] = {"__error__": "Forbidden"}
elif mode == "invalid_json":
    objects["deploy/fugue-fugue-api"] = {"__raw__": "{"}
elif mode == "missing_container":
    objects["ds/fugue-fugue-image-cache"] = {"spec": {"template": {"spec": {"containers": []}}}}
elif mode == "controller_missing":
    objects["deploy/fugue-fugue-controller"] = {"spec": {"template": {"spec": {"containers": [{"name": "not-controller", "image": "ghcr.io/acme/other:live"}]}}}}
elif mode == "invalid_controller_env":
    objects["deploy/fugue-fugue-controller"]["spec"]["template"]["spec"]["containers"][0]["env"] = {}
elif mode == "nul_image":
    objects["deploy/fugue-fugue-telemetry-agent"]["spec"]["template"]["spec"]["containers"][0]["image"] = "ghcr.io/acme/telemetry:\u0000live"
elif mode in {"newline_controller", "pipe_controller"}:
    container = objects["deploy/fugue-fugue-controller"]["spec"]["template"]["spec"]["containers"][0]
    for item in container["env"]:
        if item["name"] == "FUGUE_DRAIN_AGENT_IMAGE_TAG":
            item["value"] = "drain\nlive" if mode == "newline_controller" else "drain|live"
elif mode in {"value_from_controller", "value_and_value_from_controller", "null_value_controller", "number_value_controller", "duplicate_digest_controller"}:
    container = objects["deploy/fugue-fugue-controller"]["spec"]["template"]["spec"]["containers"][0]
    for item in container["env"]:
        if item["name"] == "FUGUE_DRAIN_AGENT_IMAGE_DIGEST":
            if mode in {"value_from_controller", "value_and_value_from_controller"}:
                item["valueFrom"] = {"secretKeyRef": {"name": "drain-image", "key": "digest"}}
            if mode == "value_and_value_from_controller":
                item["value"] = ""
            elif mode == "null_value_controller":
                item["value"] = None
            elif mode == "number_value_controller":
                item["value"] = 7
            elif mode == "duplicate_digest_controller":
                container["env"].append(dict(item))
            break
elif mode == "tab_public":
    objects["ds"]["items"][0]["spec"]["template"]["spec"]["containers"][0]["image"] = "ghcr.io/acme/front:\tlive"
elif mode == "raw_nul_resource":
    live = objects["deploy/fugue-fugue-telemetry-agent"]
    objects["deploy/fugue-fugue-telemetry-agent"] = {"__raw_nul_json__": live}
elif mode == "raw_nul_public":
    live = objects["ds"]
    objects["ds"] = {"__raw_nul_json__": live}
else:
    raise SystemExit(2)
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${FULL_TAG_OBJECTS}" "${FAILURE_OBJECTS}" "${resource_failure}"
  FAILURE_OUTPUT="${TMP_ROOT}/resource-${resource_failure}-output"
  if run_resolver "${FAILURE_OBJECTS}" "${TAG_VALUES}" "${FAILURE_OUTPUT}" "${TMP_ROOT}/resource-${resource_failure}-stdout" "${TMP_ROOT}/resource-${resource_failure}-stderr" "${TMP_ROOT}/resource-${resource_failure}-calls"; then
    fail "Kubernetes resource failure ${resource_failure} must fail closed"
  fi
  assert_eq "$(wc -c <"${FAILURE_OUTPUT}" | tr -d ' ')" "0" "${resource_failure} failure must not emit partial outputs"
  if [[ "${resource_failure}" == "controller_missing" ]]; then
    assert_eq "$(kubectl_call_count "${FAILURE_OUTPUT}" deploy/fugue-fugue-controller)" "1" "invalid controller is not re-read or treated as absent"
  fi
  case "${resource_failure}" in
    value_from_controller|value_and_value_from_controller|null_value_controller|number_value_controller)
      grep -Fq 'FUGUE_DRAIN_AGENT_IMAGE_DIGEST must use one literal string value' "${TMP_ROOT}/resource-${resource_failure}-stderr" ||
        fail "${resource_failure} must report the rejected drain digest carrier"
      ;;
    duplicate_digest_controller)
      grep -Fq 'duplicate FUGUE_DRAIN_AGENT_IMAGE_DIGEST values' "${TMP_ROOT}/resource-${resource_failure}-stderr" ||
        fail "duplicate drain digest values must report the duplicate carrier"
      ;;
  esac
done

CONTROLLER_SEQUENCE_OBJECTS="${TMP_ROOT}/controller-sequence-objects.json"
python3 -c '
import copy
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
first = objects["deploy/fugue-fugue-controller"]
second = copy.deepcopy(first)
container = second["spec"]["template"]["spec"]["containers"][0]
container["image"] = "ghcr.io/acme/controller:second-read"
for item in container["env"]:
    if item["name"] == "FUGUE_DRAIN_AGENT_IMAGE_TAG":
        item["value"] = "second-read"
objects["deploy/fugue-fugue-controller"] = {"__sequence__": [first, second]}
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${FULL_TAG_OBJECTS}" "${CONTROLLER_SEQUENCE_OBJECTS}"
run_resolver "${CONTROLLER_SEQUENCE_OBJECTS}" "${TAG_VALUES}" "${TMP_ROOT}/controller-sequence-output" "${TMP_ROOT}/controller-sequence-stdout" "${TMP_ROOT}/controller-sequence-stderr" "${TMP_ROOT}/controller-sequence-calls"
assert_eq "$(kubectl_call_count "${TMP_ROOT}/controller-sequence-output" deploy/fugue-fugue-controller)" "1" "controller snapshot must be fetched exactly once"
assert_eq "$(output_value "${TMP_ROOT}/controller-sequence-output" controller_image_source_tag)" "controller-live" "controller image comes from first atomic snapshot"
assert_eq "$(output_value "${TMP_ROOT}/controller-sequence-output" drain_agent_image_source_tag)" "drain-live" "drain metadata cannot mix a later controller snapshot"

SPARSE_TAG_OBJECTS="${TMP_ROOT}/sparse-tag-objects.json"
SPARSE_TAG_VALUES="${TMP_ROOT}/sparse-tag-values.json"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
objects["ds"]["items"] = [
    {
        "metadata": {"name": "fugue-fugue-edge-front", "labels": {"app.kubernetes.io/instance": "fugue", "app.kubernetes.io/component": "edge-front", "fugue.io/rollout-subsystem": "public-data-plane", "fugue.io/rollout-mode": "node-local-blue-green-front"}},
        "spec": {"template": {"spec": {"containers": [{"name": "edge-front", "image": "ghcr.io/acme/edge:root-front-tag"}]}}},
    },
    {
        "metadata": {"name": "fugue-fugue-edge-worker-a", "labels": {"app.kubernetes.io/instance": "fugue", "app.kubernetes.io/component": "edge-worker-a", "fugue.io/rollout-subsystem": "public-data-plane", "fugue.io/rollout-mode": "node-local-blue-green-worker", "fugue.io/edge-slot": "a"}},
        "spec": {"template": {"spec": {"containers": [{"name": "edge", "image": "ghcr.io/acme/edge-alt:edge-source"}]}}},
    },
    {
        "metadata": {"name": "fugue-fugue-edge-country-de-worker-a", "labels": {"app.kubernetes.io/instance": "fugue", "app.kubernetes.io/component": "edge-country-de-worker-a", "fugue.io/rollout-subsystem": "public-data-plane", "fugue.io/rollout-mode": "node-local-blue-green-worker", "fugue.io/edge-slot": "a"}},
        "spec": {"template": {"spec": {"containers": [{"name": "edge", "image": "ghcr.io/acme/edge-de:edge-source"}]}}},
    },
    {
        "metadata": {"name": "fugue-fugue-edge-country-us-worker-a", "labels": {"app.kubernetes.io/instance": "fugue", "app.kubernetes.io/component": "edge-country-us-worker-a", "fugue.io/rollout-subsystem": "public-data-plane", "fugue.io/rollout-mode": "node-local-blue-green-worker", "fugue.io/edge-slot": "a"}},
        "spec": {"template": {"spec": {"containers": [{"name": "edge", "image": "ghcr.io/acme/edge:group-us-tag"}]}}},
    },
]
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${FULL_TAG_OBJECTS}" "${SPARSE_TAG_OBJECTS}"
cat >"${SPARSE_TAG_VALUES}" <<JSON
{
  "edge": {
    "image": {"repository": "ghcr.io/acme/edge", "tag": "edge-source", "digest": "${DIGEST_F}"},
    "blueGreen": {
      "front": {"image": {"repository": "", "tag": "root-front-tag", "digest": ""}},
      "slots": {
        "a": {"image": {"repository": "ghcr.io/acme/edge-alt", "tag": "", "digest": ""}},
        "b": {"image": {"repository": "", "tag": "", "digest": ""}}
      }
    },
    "dynamic": {"blueGreen": {"enabled": true}},
    "groups": [
      {"name": "country_de", "image": {"repository": "ghcr.io/acme/edge-de", "digest": ""}},
      {"name": "country_us", "image": {"tag": "group-us-tag"}}
    ]
  }
}
JSON
run_resolver "${SPARSE_TAG_OBJECTS}" "${SPARSE_TAG_VALUES}" "${TMP_ROOT}/sparse-tag-output" "${TMP_ROOT}/sparse-tag-stdout" "${TMP_ROOT}/sparse-tag-stderr" "${TMP_ROOT}/sparse-tag-calls"
assert_eq "$(wc -l <"${TMP_ROOT}/sparse-tag-calls" | tr -d ' ')" "0" "tag templates must not re-inherit a Helm digest"
sparse_templates="$(multiline_output_value "${TMP_ROOT}/sparse-tag-output" public_cohort_image_template_refs)"
grep -Fqx 'fugue-fugue-edge-front|ghcr.io/acme/edge:root-front-tag' <<<"${sparse_templates}" || fail "root tag-only override template missing"
grep -Fqx 'fugue-fugue-edge-worker-a|ghcr.io/acme/edge-alt:edge-source' <<<"${sparse_templates}" || fail "root repository-only sparse override template missing"
grep -Fqx 'fugue-fugue-edge-country-de-worker-a|ghcr.io/acme/edge-de:edge-source' <<<"${sparse_templates}" || fail "group repository-only sparse override template missing"
grep -Fqx 'fugue-fugue-edge-country-us-worker-a|ghcr.io/acme/edge:group-us-tag' <<<"${sparse_templates}" || fail "group tag-only sparse override template missing"
sparse_digests="$(multiline_output_value "${TMP_ROOT}/sparse-tag-output" public_cohort_image_digests)"
while IFS= read -r sparse_digest; do
  [[ "${sparse_digest}" == *'|' ]] || fail "sparse tag override inherited an old digest: ${sparse_digest}"
done <<<"${sparse_digests}"
for slot_label_case in worker_mismatch front_present; do
  SLOT_LABEL_OBJECTS="${TMP_ROOT}/slot-label-${slot_label_case}-objects.json"
  python3 -c '
import json
import sys

source, target, mode = sys.argv[1:4]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
for item in objects["ds"]["items"]:
    labels = item["metadata"]["labels"]
    component = labels["app.kubernetes.io/component"]
    if mode == "worker_mismatch" and component == "edge-worker-a":
        labels["fugue.io/edge-slot"] = "b"
    if mode == "front_present" and component == "edge-front":
        labels["fugue.io/edge-slot"] = "a"
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${SPARSE_TAG_OBJECTS}" "${SLOT_LABEL_OBJECTS}" "${slot_label_case}"
  if run_resolver "${SLOT_LABEL_OBJECTS}" "${SPARSE_TAG_VALUES}" "${TMP_ROOT}/slot-label-${slot_label_case}-output" "${TMP_ROOT}/slot-label-${slot_label_case}-stdout" "${TMP_ROOT}/slot-label-${slot_label_case}-stderr" "${TMP_ROOT}/slot-label-${slot_label_case}-calls"; then
    fail "public cohort edge-slot case ${slot_label_case} must fail closed"
  fi
  assert_eq "$(wc -c <"${TMP_ROOT}/slot-label-${slot_label_case}-output" | tr -d ' ')" "0" "edge-slot ${slot_label_case} failure must not emit partial outputs"
done

DIGEST_OBJECTS="${TMP_ROOT}/digest-objects.json"
DIGEST_VALUES="${TMP_ROOT}/digest-values.json"
DIGEST_OUTPUT="${TMP_ROOT}/digest-output"
DIGEST_STDOUT="${TMP_ROOT}/digest-stdout"
DIGEST_STDERR="${TMP_ROOT}/digest-stderr"
DIGEST_CALLS="${TMP_ROOT}/digest-calls"
cat >"${DIGEST_OBJECTS}" <<JSON
{
  "deploy/fugue-fugue-api": {"spec":{"template":{"spec":{"containers":[{"name":"api","image":"ghcr.io/acme/api@${DIGEST_A}"}]}}}},
  "deploy/fugue-fugue-controller": {"spec":{"template":{"spec":{"containers":[{"name":"controller","image":"ghcr.io/acme/controller@${DIGEST_B}","env":[
    {"name":"FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY","value":"ghcr.io/acme/drain"},
    {"name":"FUGUE_DRAIN_AGENT_IMAGE_TAG","value":"drain-source"},
    {"name":"FUGUE_DRAIN_AGENT_IMAGE_DIGEST","value":"${DIGEST_C}"}
  ]}]}}}},
  "deploy/fugue-fugue-telemetry-agent": {"spec":{"template":{"spec":{"containers":[{"name":"telemetry-agent","image":"ghcr.io/acme/telemetry@${DIGEST_D}"}]}}}},
  "ds/fugue-fugue-image-cache": {"spec":{"template":{"spec":{"containers":[{"name":"image-cache","image":"ghcr.io/acme/cache@${DIGEST_E}"}]}}}},
  "ds/fugue-fugue-edge-worker-a": {"spec":{"template":{"spec":{"containers":[{"name":"edge","image":"ghcr.io/acme/edge@${DIGEST_F}"}]}}}},
  "ds": {"items":[
    {"metadata":{"name":"fugue-fugue-edge-worker-a","labels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"edge-worker-a","fugue.io/rollout-subsystem":"public-data-plane","fugue.io/rollout-mode":"node-local-blue-green-worker","fugue.io/edge-slot":"a"}},"spec":{"template":{"spec":{"containers":[{"name":"edge","image":"ghcr.io/acme/edge@${DIGEST_F}"}]}}}},
    {"metadata":{"name":"fugue-fugue-edge-front","labels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"edge-front","fugue.io/rollout-subsystem":"public-data-plane","fugue.io/rollout-mode":"node-local-blue-green-front"}},"spec":{"template":{"spec":{"containers":[{"name":"edge-front","image":"ghcr.io/acme/edge@${DIGEST_F}"}]}}}},
    {"metadata":{"name":"fugue-fugue-edge-dynamic-front","labels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"edge-dynamic-front","fugue.io/rollout-subsystem":"public-data-plane","fugue.io/rollout-mode":"node-local-blue-green-front"}},"spec":{"template":{"spec":{"containers":[{"name":"edge-front","image":"ghcr.io/acme/edge@${DIGEST_F}"}]}}}},
    {"metadata":{"name":"fugue-fugue-edge-country-de-front","labels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"edge-country-de-front","fugue.io/rollout-subsystem":"public-data-plane","fugue.io/rollout-mode":"node-local-blue-green-front"}},"spec":{"template":{"spec":{"containers":[{"name":"edge-front","image":"ghcr.io/acme/edge-de@${DIGEST_E}"}]}}}},
    {"metadata":{"name":"fugue-fugue-edge-empty-worker-b","labels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"edge-empty-worker-b","fugue.io/rollout-subsystem":"public-data-plane","fugue.io/rollout-mode":"node-local-blue-green-worker","fugue.io/edge-slot":"b"}},"spec":{"template":{"spec":{"containers":[{"name":"edge","image":"ghcr.io/acme/edge@${DIGEST_F}"}]}}}}
  ]}
}
JSON
cat >"${DIGEST_VALUES}" <<JSON
{
  "bootstrapAdminKey":"SECRET-MUST-NOT-LEAK",
  "api":{"image":{"repository":"ghcr.io/acme/api","tag":"api-source","digest":"${DIGEST_A}"}},
  "controller":{"image":{"repository":"ghcr.io/acme/controller","tag":"controller-source","digest":"${DIGEST_B}"}},
  "runtime":{"strictDrain":{"agent":{"image":{"repository":"ghcr.io/acme/drain","tag":"drain-source","digest":"${DIGEST_C}"}}}},
  "observability":{"agent":{"image":{"repository":"ghcr.io/acme/telemetry","tag":"telemetry-source","digest":"${DIGEST_D}"}}},
  "imageCache":{"image":{"repository":"ghcr.io/acme/cache","tag":"cache-source","digest":"${DIGEST_E}"}},
  "edge":{
    "image":{"repository":"ghcr.io/acme/edge","tag":"edge-source","digest":"${DIGEST_F}"},
    "blueGreen":{
      "slots":{"a":{"image":{"repository":"","tag":"","digest":"${DIGEST_F}"}},"b":{"image":{"repository":"","tag":"","digest":""}}},
      "front":{"image":{"repository":"","tag":"","digest":"${DIGEST_F}"}}
    },
    "dynamic":{"blueGreen":{"front":{"image":{"repository":"ghcr.io/acme/edge","tag":"dynamic-front-source","digest":"${DIGEST_F}"}}}},
    "groups":[
      {"name":"country_de","image":{"repository":"ghcr.io/acme/edge-de","tag":"group-source","digest":"${DIGEST_E}"},"blueGreen":{"front":{"image":{"repository":"ghcr.io/acme/edge-de","tag":"group-front-source","digest":"${DIGEST_E}"}}}},
      {"name":"empty","image":{"repository":"","tag":"","digest":""}}
    ]
  }
}
JSON
run_resolver "${DIGEST_OBJECTS}" "${DIGEST_VALUES}" "${DIGEST_OUTPUT}" "${DIGEST_STDOUT}" "${DIGEST_STDERR}" "${DIGEST_CALLS}"
assert_eq "$(wc -l <"${DIGEST_CALLS}" | tr -d ' ')" "1" "all digest references share one Helm snapshot"
assert_eq "$(output_value "${DIGEST_OUTPUT}" api_image_tag)" "api-source" "digest-only legacy tag output"
assert_eq "$(output_value "${DIGEST_OUTPUT}" api_image_digest)" "${DIGEST_A}" "API digest output"
assert_eq "$(output_value "${DIGEST_OUTPUT}" api_image_template_ref)" "ghcr.io/acme/api@${DIGEST_A}" "API exact template ref"
assert_eq "$(output_value "${DIGEST_OUTPUT}" edge_image_source_tag)" "edge-source" "worker-a digest-only sparse override source tag"
assert_eq "$(output_value "${DIGEST_OUTPUT}" drain_agent_image_source_tag)" "drain-source" "digest drain source tag"
assert_eq "$(multiline_output_value "${DIGEST_OUTPUT}" release_baseline_tags)" $'api-source\ncontroller-source' "digest source baselines"
cohort_sources="$(multiline_output_value "${DIGEST_OUTPUT}" public_cohort_image_source_tags)"
grep -Fqx 'fugue-fugue-edge-front|edge-source' <<<"${cohort_sources}" || fail "base front digest-only sparse override source tag missing"
grep -Fqx 'fugue-fugue-edge-dynamic-front|dynamic-front-source' <<<"${cohort_sources}" || fail "dynamic front Helm source tag missing"
grep -Fqx 'fugue-fugue-edge-country-de-front|group-front-source' <<<"${cohort_sources}" || fail "group front local source tag missing"
grep -Fqx 'fugue-fugue-edge-worker-a|edge-source' <<<"${cohort_sources}" || fail "worker source tag missing"
grep -Fqx 'fugue-fugue-edge-empty-worker-b|edge-source' <<<"${cohort_sources}" || fail "all-empty group placeholder must inherit root identity"
if grep -Fq 'SECRET-MUST-NOT-LEAK' "${DIGEST_OUTPUT}" "${DIGEST_STDOUT}" "${DIGEST_STDERR}"; then
  fail "Helm values secret leaked through resolver outputs or logs"
fi

PUBLIC_FIXTURE_TREE="$(git -C "${REPO_ROOT}" rev-parse HEAD^{tree})"
public_fixture_commit() {
  local message="$1"
  local parent="${2:-}"
  local commit_args=(commit-tree "${PUBLIC_FIXTURE_TREE}")
  [[ -z "${parent}" ]] || commit_args+=(-p "${parent}")
  GIT_AUTHOR_NAME=fugue-test GIT_AUTHOR_EMAIL=fugue-test@example.invalid \
  GIT_AUTHOR_DATE=2026-07-31T00:00:00Z \
  GIT_COMMITTER_NAME=fugue-test GIT_COMMITTER_EMAIL=fugue-test@example.invalid \
  GIT_COMMITTER_DATE=2026-07-31T00:00:00Z \
    git -C "${REPO_ROOT}" "${commit_args[@]}" -m "${message}"
}
PUBLIC_HELM_SHA="$(public_fixture_commit 'resolver Helm slot fixture')"
PUBLIC_INACTIVE_SHA="$(public_fixture_commit 'resolver inactive slot fixture' "${PUBLIC_HELM_SHA}")"
PUBLIC_RELEASE_SHA="$(public_fixture_commit 'resolver release record fixture' "${PUBLIC_INACTIVE_SHA}")"
PUBLIC_TARGET_SHA="$(public_fixture_commit 'resolver target fixture' "${PUBLIC_RELEASE_SHA}")"
PUBLIC_NONANCESTOR_SHA="$(public_fixture_commit 'resolver non-ancestor fixture')"
PUBLIC_RECORD_OBJECTS="${TMP_ROOT}/public-record-objects.json"
PUBLIC_RECORD_VALUES="${TMP_ROOT}/public-record-values.json"
python3 - "${PUBLIC_RECORD_OBJECTS}" "${PUBLIC_RECORD_VALUES}" \
  "${PUBLIC_RELEASE_SHA}" "${PUBLIC_INACTIVE_SHA}" "${PUBLIC_HELM_SHA}" \
  "${DIGEST_F}" "${DIGEST_E}" <<'PY'
import json
import sys

objects_path, values_path, release_sha, inactive_sha, helm_sha, release_digest, old_digest = sys.argv[1:8]
namespace = "fugue-system"
release_name = "fugue"
fullname = "fugue-fugue"
repository = "ghcr.io/acme/edge"
release_id = f"pdp-20260731T132527Z-{release_sha}"
old_release_id = f"pdp-20260701T000000Z-{inactive_sha}"

def labels(component, mode, slot=None):
    value = {
        "app.kubernetes.io/name": "fugue",
        "app.kubernetes.io/instance": release_name,
        "app.kubernetes.io/managed-by": "Helm",
        "helm.sh/chart": "fugue-0.1.0",
        "app.kubernetes.io/component": component,
        "fugue.io/rollout-subsystem": "public-data-plane",
        "fugue.io/rollout-mode": mode,
        "fugue.io/downtime-class": "online-required",
    }
    if slot:
        value["fugue.io/edge-slot"] = slot
    return value

def status(generation, desired=1, updated=True):
    value = {
        "observedGeneration": generation,
        "desiredNumberScheduled": desired,
        "currentNumberScheduled": desired,
        "numberReady": desired,
        "numberMisscheduled": 0,
    }
    if desired:
        value["numberAvailable"] = desired
        value["numberUnavailable"] = 0
    if updated and desired:
        value["updatedNumberScheduled"] = desired
    return value

def daemonset(base, member, index):
    desired = 0 if base.endswith("-dynamic") else 1
    if member == "front":
        name = f"{base}-front"
        component = name[len(fullname) + 1:]
        mode = "node-local-blue-green-front"
        container_name = "edge-front"
        image = f"{repository}:{inactive_sha}"
        annotations = {}
        slot = None
        updated = desired > 0
    else:
        slot = member
        name = f"{base}-worker-{slot}"
        component = name[len(fullname) + 1:]
        mode = "node-local-blue-green-worker"
        container_name = "edge"
        active = slot == "b" and desired > 0
        image = f"{repository}@{release_digest if active else old_digest}"
        annotations = {
            "fugue.io/public-data-plane-release-id": release_id if active else old_release_id,
            "fugue.io/public-data-plane-release-mode": "node-local-blue-green-worker",
        }
        updated = active
    generation = index + 10
    return {
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": f"uid-{index}",
            "resourceVersion": str(1000 + index),
            "generation": generation,
            "labels": labels(component, mode, slot),
        },
        "spec": {"template": {"metadata": {"annotations": annotations}, "spec": {"containers": [{"name": container_name, "image": image}]}}},
        "status": status(generation, desired, updated),
    }

bases = [f"{fullname}-edge", f"{fullname}-edge-country-de", f"{fullname}-edge-dynamic"]
items = []
index = 0
for base in bases:
    for member in ("front", "a", "b"):
        items.append(daemonset(base, member, index))
        index += 1
record = {
    "apiVersion": "v1",
    "kind": "ConfigMap",
    "metadata": {
        "name": f"{fullname}-public-data-plane-release",
        "namespace": namespace,
        "uid": "uid-release-record",
        "resourceVersion": "2000",
        "labels": {
            "app.kubernetes.io/instance": release_name,
            "app.kubernetes.io/component": "public-data-plane-release",
            "fugue.io/rollout-subsystem": "public-data-plane",
        },
    },
    "data": {
        "release_id": release_id,
        "mode": "node-local-blue-green",
        "active_slots": json.dumps({base: "b" for base in bases if not base.endswith("-dynamic")}, separators=(",", ":"), sort_keys=True),
        "daemonsets": ",".join(bases),
        "edge_resources": "{}",
        "caddy_resources": "{}",
        "git_sha": release_sha,
        "recorded_at": "2026-07-31T13:26:24Z",
        "runtime_cgroup_memory_max": "1073741824",
        "runtime_cgroup_patched_at": "2026-05-28T23:36:05Z",
        "runtime_cgroup_patched_nodes": "ovhvps,ovhvpseu",
    },
}
objects = {
    "deploy/fugue-fugue-api": {"spec": {"template": {"spec": {"containers": [{"name": "api", "image": "ghcr.io/acme/api:api-live"}]}}}},
    "deploy/fugue-fugue-controller": {"spec": {"template": {"spec": {"containers": [{"name": "controller", "image": "ghcr.io/acme/controller:controller-live", "env": []}]}}}},
    "deploy/fugue-fugue-telemetry-agent": {"spec": {"template": {"spec": {"containers": [{"name": "telemetry-agent", "image": "ghcr.io/acme/telemetry:telemetry-live"}]}}}},
    "ds/fugue-fugue-image-cache": {"spec": {"template": {"spec": {"containers": [{"name": "image-cache", "image": "ghcr.io/acme/cache:cache-live"}]}}}},
    "ds/fugue-fugue-edge-worker-a": next(item for item in items if item["metadata"]["name"] == "fugue-fugue-edge-worker-a"),
    "ds": {"items": items},
    "configmap/fugue-fugue-public-data-plane-release": record,
}
values = {
    "edge": {
        "image": {"repository": repository, "tag": inactive_sha, "digest": old_digest},
        "blueGreen": {
            "front": {"image": {"repository": "", "tag": "", "digest": ""}},
            "slots": {
                "a": {"image": {"repository": "", "tag": "", "digest": ""}},
                "b": {"image": {"repository": "", "tag": helm_sha, "digest": ""}},
            },
        },
        "dynamic": {"blueGreen": {"slots": {"b": {"image": {"repository": repository, "tag": inactive_sha, "digest": old_digest}}}}},
        "groups": [{
            "name": "country_de",
            "image": {"repository": repository, "tag": inactive_sha, "digest": old_digest},
            "blueGreen": {},
        }],
    },
}
with open(objects_path, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
with open(values_path, "w", encoding="utf-8") as handle:
    json.dump(values, handle)
PY

PUBLIC_RECORD_OUTPUT="${TMP_ROOT}/public-record-output"
FUGUE_EDGE_IMAGE_REPOSITORY=ghcr.io/acme/edge \
RESOLVER_GITHUB_SHA="${PUBLIC_TARGET_SHA}" \
run_resolver "${PUBLIC_RECORD_OBJECTS}" "${PUBLIC_RECORD_VALUES}" "${PUBLIC_RECORD_OUTPUT}" \
  "${TMP_ROOT}/public-record-stdout" "${TMP_ROOT}/public-record-stderr" "${TMP_ROOT}/public-record-calls"
assert_eq "$(output_value "${PUBLIC_RECORD_OUTPUT}" edge_image_source_tag)" "${PUBLIC_INACTIVE_SHA}" \
  "inactive old worker remains bound to its exact Helm source"
public_record_sources="$(multiline_output_value "${PUBLIC_RECORD_OUTPUT}" public_cohort_image_source_tags)"
grep -Fqx "fugue-fugue-edge-worker-a|${PUBLIC_INACTIVE_SHA}" <<<"${public_record_sources}" ||
  fail "inactive old A must not consume the active release record"
grep -Fqx "fugue-fugue-edge-worker-b|${PUBLIC_RELEASE_SHA}" <<<"${public_record_sources}" ||
  fail "active digest-only B must consume the exact release-record source"
grep -Fqx "fugue-fugue-edge-country-de-worker-b|${PUBLIC_RELEASE_SHA}" <<<"${public_record_sources}" ||
  fail "every active serving group must consume the same release-record source"
assert_eq "$(kubectl_call_count "${PUBLIC_RECORD_OUTPUT}" daemonsets,configmaps)" "1" \
  "public DaemonSets and release record share one Kubernetes snapshot"
[[ "${PUBLIC_HELM_SHA}" != "${PUBLIC_INACTIVE_SHA}" && \
  "${PUBLIC_HELM_SHA}" != "${PUBLIC_RELEASE_SHA}" && \
  "${PUBLIC_INACTIVE_SHA}" != "${PUBLIC_RELEASE_SHA}" ]] ||
  fail "public production-shape fixture must use three distinct ordered revisions"
git -C "${REPO_ROOT}" merge-base --is-ancestor "${PUBLIC_HELM_SHA}" "${PUBLIC_RELEASE_SHA}" ||
  fail "public production-shape Helm revision must precede the release record"
git -C "${REPO_ROOT}" merge-base --is-ancestor "${PUBLIC_INACTIVE_SHA}" "${PUBLIC_RELEASE_SHA}" ||
  fail "public production-shape inactive revision must precede the release record"

for public_record_failure in forged_sha nonancestor_sha wrong_active_slot partial_active_slots mixed_digest release_id_drift partial_runtime_extension extra_record_key negative_disabled_count string_disabled_count; do
  FAILURE_PUBLIC_OBJECTS="${TMP_ROOT}/public-record-${public_record_failure}-objects.json"
  python3 - "${PUBLIC_RECORD_OBJECTS}" "${FAILURE_PUBLIC_OBJECTS}" "${public_record_failure}" \
    "${PUBLIC_NONANCESTOR_SHA}" "${DIGEST_D}" <<'PY'
import copy
import json
import sys

source, target, mode, nonancestor_sha, mixed_digest = sys.argv[1:6]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
record = objects["configmap/fugue-fugue-public-data-plane-release"]
data = record["data"]
items = objects["ds"]["items"]
if mode in {"forged_sha", "nonancestor_sha"}:
    sha = "f" * 40 if mode == "forged_sha" else nonancestor_sha
    data["git_sha"] = sha
    data["release_id"] = f"pdp-20260731T132527Z-{sha}"
    for item in items:
        if (item["metadata"]["labels"].get("fugue.io/edge-slot") == "b"):
            item["spec"]["template"]["metadata"]["annotations"]["fugue.io/public-data-plane-release-id"] = data["release_id"]
elif mode == "wrong_active_slot":
    slots = json.loads(data["active_slots"])
    slots["fugue-fugue-edge"] = "a"
    data["active_slots"] = json.dumps(slots, separators=(",", ":"), sort_keys=True)
elif mode == "partial_active_slots":
    slots = json.loads(data["active_slots"])
    slots.pop("fugue-fugue-edge-country-de")
    data["active_slots"] = json.dumps(slots, separators=(",", ":"), sort_keys=True)
elif mode == "mixed_digest":
    for item in items:
        if item["metadata"]["name"] == "fugue-fugue-edge-country-de-worker-b":
            item["spec"]["template"]["spec"]["containers"][0]["image"] = "ghcr.io/acme/edge@" + mixed_digest
elif mode == "release_id_drift":
    for item in items:
        if item["metadata"]["name"] == "fugue-fugue-edge-country-de-worker-b":
            item["spec"]["template"]["metadata"]["annotations"]["fugue.io/public-data-plane-release-id"] = "pdp-20260730T000000Z-" + "e" * 40
elif mode == "partial_runtime_extension":
    data.pop("runtime_cgroup_patched_nodes")
elif mode == "extra_record_key":
    data["untrusted_source_tag"] = "attacker-controlled"
elif mode in {"negative_disabled_count", "string_disabled_count"}:
    dynamic_front = next(item for item in items if item["metadata"]["name"] == "fugue-fugue-edge-dynamic-front")
    dynamic_front["status"]["numberAvailable"] = -1 if mode == "negative_disabled_count" else "0"
else:
    raise SystemExit(2)
objects["ds/fugue-fugue-edge-worker-a"] = copy.deepcopy(next(
    item for item in items if item["metadata"]["name"] == "fugue-fugue-edge-worker-a"
))
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
PY
  FAILURE_PUBLIC_OUTPUT="${TMP_ROOT}/public-record-${public_record_failure}-output"
  if FUGUE_EDGE_IMAGE_REPOSITORY=ghcr.io/acme/edge \
    RESOLVER_GITHUB_SHA="${PUBLIC_TARGET_SHA}" \
    run_resolver "${FAILURE_PUBLIC_OBJECTS}" "${PUBLIC_RECORD_VALUES}" "${FAILURE_PUBLIC_OUTPUT}" \
      "${TMP_ROOT}/public-record-${public_record_failure}-stdout" \
      "${TMP_ROOT}/public-record-${public_record_failure}-stderr" \
      "${TMP_ROOT}/public-record-${public_record_failure}-calls"; then
    fail "public release-record evidence ${public_record_failure} must fail closed"
  fi
  assert_eq "$(wc -c <"${FAILURE_PUBLIC_OUTPUT}" | tr -d ' ')" "0" \
    "public release-record ${public_record_failure} failure emits no partial output"
done

PUBLIC_CONFLICT_VALUES="${TMP_ROOT}/public-record-conflict-values.json"
python3 - "${PUBLIC_RECORD_VALUES}" "${PUBLIC_CONFLICT_VALUES}" "${DIGEST_A}" <<'PY'
import json
import sys

source, target, digest = sys.argv[1:4]
with open(source, "r", encoding="utf-8") as handle:
    values = json.load(handle)
values["edge"]["blueGreen"]["slots"]["b"]["image"]["digest"] = digest
with open(target, "w", encoding="utf-8") as handle:
    json.dump(values, handle)
PY
if FUGUE_EDGE_IMAGE_REPOSITORY=ghcr.io/acme/edge \
  RESOLVER_GITHUB_SHA="${PUBLIC_TARGET_SHA}" \
  run_resolver "${PUBLIC_RECORD_OBJECTS}" "${PUBLIC_CONFLICT_VALUES}" \
    "${TMP_ROOT}/public-record-conflict-output" "${TMP_ROOT}/public-record-conflict-stdout" \
    "${TMP_ROOT}/public-record-conflict-stderr" "${TMP_ROOT}/public-record-conflict-calls"; then
  fail "a conflicting non-empty Helm digest must not use public release-record evidence"
fi

for helm_history_case in current_release later_than_release nonancestor missing_commit; do
  PUBLIC_FALSE_OLD_VALUES="${TMP_ROOT}/public-record-${helm_history_case}-values.json"
  case "${helm_history_case}" in
    current_release) false_old_tag="${PUBLIC_RELEASE_SHA}" ;;
    later_than_release) false_old_tag="${PUBLIC_TARGET_SHA}" ;;
    nonancestor) false_old_tag="${PUBLIC_NONANCESTOR_SHA}" ;;
    missing_commit) false_old_tag="ffffffffffffffffffffffffffffffffffffffff" ;;
  esac
  python3 - "${PUBLIC_RECORD_VALUES}" "${PUBLIC_FALSE_OLD_VALUES}" "${false_old_tag}" <<'PY'
import json
import sys

source, target, source_tag = sys.argv[1:4]
with open(source, "r", encoding="utf-8") as handle:
    values = json.load(handle)
values["edge"]["blueGreen"]["slots"]["b"]["image"]["tag"] = source_tag
with open(target, "w", encoding="utf-8") as handle:
    json.dump(values, handle)
PY
  if FUGUE_EDGE_IMAGE_REPOSITORY=ghcr.io/acme/edge \
    RESOLVER_GITHUB_SHA="${PUBLIC_TARGET_SHA}" \
    run_resolver "${PUBLIC_RECORD_OBJECTS}" "${PUBLIC_FALSE_OLD_VALUES}" \
      "${TMP_ROOT}/public-record-${helm_history_case}-output" \
      "${TMP_ROOT}/public-record-${helm_history_case}-stdout" \
      "${TMP_ROOT}/public-record-${helm_history_case}-stderr" \
      "${TMP_ROOT}/public-record-${helm_history_case}-calls"; then
    fail "unsafe Helm public-edge history ${helm_history_case} must fail closed"
  fi
done

PUBLIC_DRIFT_OBJECTS="${TMP_ROOT}/public-record-cross-read-objects.json"
python3 - "${PUBLIC_RECORD_OBJECTS}" "${PUBLIC_DRIFT_OBJECTS}" <<'PY'
import copy
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
first = {"apiVersion": "v1", "kind": "List", "items": copy.deepcopy(objects["ds"]["items"]) + [copy.deepcopy(objects["configmap/fugue-fugue-public-data-plane-release"])]}
second = copy.deepcopy(first)
record = next(item for item in second["items"] if item.get("kind") == "ConfigMap")
record["metadata"]["uid"] = "uid-release-record-drifted"
record["metadata"]["resourceVersion"] = "9999"
record["data"]["active_slots"] = "{}"
objects["daemonsets,configmaps"] = {"__sequence__": [first, second]}
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
PY
PUBLIC_DRIFT_OUTPUT="${TMP_ROOT}/public-record-cross-read-output"
FUGUE_EDGE_IMAGE_REPOSITORY=ghcr.io/acme/edge \
RESOLVER_GITHUB_SHA="${PUBLIC_TARGET_SHA}" \
run_resolver "${PUBLIC_DRIFT_OBJECTS}" "${PUBLIC_RECORD_VALUES}" "${PUBLIC_DRIFT_OUTPUT}" \
  "${TMP_ROOT}/public-record-cross-read-stdout" "${TMP_ROOT}/public-record-cross-read-stderr" \
  "${TMP_ROOT}/public-record-cross-read-calls"
assert_eq "$(kubectl_call_count "${PUBLIC_DRIFT_OUTPUT}" daemonsets,configmaps)" "1" \
  "release-record UID/RV/content cannot drift across multiple reads"
grep -Fqx "fugue-fugue-edge-worker-b|${PUBLIC_RELEASE_SHA}" \
  <<<"$(multiline_output_value "${PUBLIC_DRIFT_OUTPUT}" public_cohort_image_source_tags)" ||
  fail "the one-shot public release snapshot must not mix a later ConfigMap revision"

for edge_snapshot_case in missing mismatch; do
  EDGE_SNAPSHOT_OBJECTS="${TMP_ROOT}/edge-snapshot-${edge_snapshot_case}-objects.json"
  python3 -c '
import json
import sys

source, target, mode, mismatch_digest = sys.argv[1:5]
with open(source, "r", encoding="utf-8") as handle:
    objects = json.load(handle)
items = objects["ds"]["items"]
if mode == "missing":
    objects["ds"]["items"] = [item for item in items if item["metadata"]["name"] != "fugue-fugue-edge-worker-a"]
elif mode == "mismatch":
    for item in items:
        if item["metadata"]["name"] == "fugue-fugue-edge-worker-a":
            item["spec"]["template"]["spec"]["containers"][0]["image"] = "ghcr.io/acme/edge@" + mismatch_digest
else:
    raise SystemExit(2)
with open(target, "w", encoding="utf-8") as handle:
    json.dump(objects, handle)
' "${DIGEST_OBJECTS}" "${EDGE_SNAPSHOT_OBJECTS}" "${edge_snapshot_case}" "${DIGEST_A}"
  EDGE_SNAPSHOT_OUTPUT="${TMP_ROOT}/edge-snapshot-${edge_snapshot_case}-output"
  if run_resolver "${EDGE_SNAPSHOT_OBJECTS}" "${DIGEST_VALUES}" "${EDGE_SNAPSHOT_OUTPUT}" "${TMP_ROOT}/edge-snapshot-${edge_snapshot_case}-stdout" "${TMP_ROOT}/edge-snapshot-${edge_snapshot_case}-stderr" "${TMP_ROOT}/edge-snapshot-${edge_snapshot_case}-calls"; then
    fail "edge worker exact/cohort snapshot ${edge_snapshot_case} must fail closed"
  fi
  assert_eq "$(wc -c <"${EDGE_SNAPSHOT_OUTPUT}" | tr -d ' ')" "0" "edge snapshot ${edge_snapshot_case} failure must not emit partial outputs"
done

MISMATCH_VALUES="${TMP_ROOT}/mismatch-values.json"
MISMATCH_OUTPUT="${TMP_ROOT}/mismatch-output"
MISMATCH_STDOUT="${TMP_ROOT}/mismatch-stdout"
MISMATCH_STDERR="${TMP_ROOT}/mismatch-stderr"
MISMATCH_CALLS="${TMP_ROOT}/mismatch-calls"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    values = json.load(handle)
values["api"]["image"]["repository"] = "ghcr.io/acme/wrong-api"
with open(target, "w", encoding="utf-8") as handle:
    json.dump(values, handle)
' "${DIGEST_VALUES}" "${MISMATCH_VALUES}"
if run_resolver "${DIGEST_OBJECTS}" "${MISMATCH_VALUES}" "${MISMATCH_OUTPUT}" "${MISMATCH_STDOUT}" "${MISMATCH_STDERR}" "${MISMATCH_CALLS}"; then
  fail "Helm/live repository mismatch must fail closed"
fi
assert_eq "$(wc -c <"${MISMATCH_OUTPUT}" | tr -d ' ')" "0" "failed resolver must not emit partial outputs"
assert_eq "$(wc -l <"${MISMATCH_CALLS}" | tr -d ' ')" "1" "mismatch reads Helm once"

HELM_CONTROL_VALUES="${TMP_ROOT}/helm-control-values.json"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    values = json.load(handle)
values["api"]["image"]["tag"] = "api\u0000source"
with open(target, "w", encoding="utf-8") as handle:
    json.dump(values, handle)
' "${DIGEST_VALUES}" "${HELM_CONTROL_VALUES}"
if run_resolver "${DIGEST_OBJECTS}" "${HELM_CONTROL_VALUES}" "${TMP_ROOT}/helm-control-output" "${TMP_ROOT}/helm-control-stdout" "${TMP_ROOT}/helm-control-stderr" "${TMP_ROOT}/helm-control-calls"; then
  fail "Helm TSV control character must fail before Bash normalization"
fi
assert_eq "$(wc -c <"${TMP_ROOT}/helm-control-output" | tr -d ' ')" "0" "Helm control character failure must not emit partial outputs"

MISSING_TAG_VALUES="${TMP_ROOT}/missing-tag-values.json"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    values = json.load(handle)
values["api"]["image"]["tag"] = ""
with open(target, "w", encoding="utf-8") as handle:
    json.dump(values, handle)
' "${DIGEST_VALUES}" "${MISSING_TAG_VALUES}"
if run_resolver "${DIGEST_OBJECTS}" "${MISSING_TAG_VALUES}" "${TMP_ROOT}/missing-tag-output" "${TMP_ROOT}/missing-tag-stdout" "${TMP_ROOT}/missing-tag-stderr" "${TMP_ROOT}/missing-tag-calls"; then
  fail "digest-only image without Helm source tag must fail closed"
fi

AMBIGUOUS_VALUES="${TMP_ROOT}/ambiguous-values.json"
python3 -c '
import json
import sys

source, target = sys.argv[1:3]
with open(source, "r", encoding="utf-8") as handle:
    values = json.load(handle)
values["edge"]["groups"].append({"name": "country-de", "image": {"repository": "ghcr.io/acme/other", "tag": "other", "digest": sys.argv[3]}})
with open(target, "w", encoding="utf-8") as handle:
    json.dump(values, handle)
' "${DIGEST_VALUES}" "${AMBIGUOUS_VALUES}" "${DIGEST_D}"
if run_resolver "${DIGEST_OBJECTS}" "${AMBIGUOUS_VALUES}" "${TMP_ROOT}/ambiguous-output" "${TMP_ROOT}/ambiguous-stdout" "${TMP_ROOT}/ambiguous-stderr" "${TMP_ROOT}/ambiguous-calls"; then
  fail "normalized public cohort group collision must fail closed"
fi

for reserved_group in dynamic ssh; do
  RESERVED_VALUES="${TMP_ROOT}/reserved-${reserved_group}-values.json"
  python3 -c '
import json
import sys

source, target, reserved, digest = sys.argv[1:5]
with open(source, "r", encoding="utf-8") as handle:
    values = json.load(handle)
values["edge"]["groups"].append({"name": reserved, "image": {"repository": "ghcr.io/acme/reserved", "tag": "reserved", "digest": digest}})
with open(target, "w", encoding="utf-8") as handle:
    json.dump(values, handle)
' "${DIGEST_VALUES}" "${RESERVED_VALUES}" "${reserved_group}" "${DIGEST_D}"
  if run_resolver "${DIGEST_OBJECTS}" "${RESERVED_VALUES}" "${TMP_ROOT}/reserved-${reserved_group}-output" "${TMP_ROOT}/reserved-${reserved_group}-stdout" "${TMP_ROOT}/reserved-${reserved_group}-stderr" "${TMP_ROOT}/reserved-${reserved_group}-calls"; then
    fail "reserved public cohort group ${reserved_group} must fail closed"
  fi
  assert_eq "$(wc -c <"${TMP_ROOT}/reserved-${reserved_group}-output" | tr -d ' ')" "0" "reserved ${reserved_group} failure must not emit partial outputs"
done

FAKE_HELM_MODE=fail
if run_resolver "${DIGEST_OBJECTS}" "${DIGEST_VALUES}" "${TMP_ROOT}/helm-fail-output" "${TMP_ROOT}/helm-fail-stdout" "${TMP_ROOT}/helm-fail-stderr" "${TMP_ROOT}/helm-fail-calls"; then
  fail "unavailable Helm snapshot for digest-only image must fail closed"
fi
unset FAKE_HELM_MODE

printf 'control-plane live image resolver tests passed\n'
