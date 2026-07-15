#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib/release_image_ref.sh
source "${SCRIPT_DIR}/lib/release_image_ref.sh"

HELM_VALUES_ATTEMPTED=false
HELM_VALUES_FILE=""
KUBECTL_SNAPSHOT_FILE=""
OUTPUT_BUFFER_FILE=""

cleanup_resolver_files() {
  [[ -z "${HELM_VALUES_FILE}" ]] || rm -f "${HELM_VALUES_FILE}"
  [[ -z "${KUBECTL_SNAPSHOT_FILE}" ]] || rm -f "${KUBECTL_SNAPSHOT_FILE}"
  [[ -z "${OUTPUT_BUFFER_FILE}" ]] || rm -f "${OUTPUT_BUFFER_FILE}"
}
trap cleanup_resolver_files EXIT

umask 077
OUTPUT_BUFFER_FILE="$(mktemp "${TMPDIR:-/tmp}/fugue-live-image-output.XXXXXX")"
KUBECTL_SNAPSHOT_FILE="$(mktemp "${TMPDIR:-/tmp}/fugue-kubectl-snapshot.XXXXXX")"
chmod 600 "${OUTPUT_BUFFER_FILE}" "${KUBECTL_SNAPSHOT_FILE}"

detect_kubectl() {
  if command -v kubectl >/dev/null 2>&1; then
    printf 'kubectl'
    return 0
  fi
  if command -v k3s >/dev/null 2>&1; then
    printf 'k3s kubectl'
    return 0
  fi
  printf 'kubectl'
}

emit_output() {
  local key="$1"
  local value="$2"
  printf '%s=%s\n' "${key}" "${value}" >>"${OUTPUT_BUFFER_FILE}"
}

output_value_contains_line() {
  local value="$1"
  local candidate="$2"
  local line

  while IFS= read -r line; do
    [[ "${line}" != "${candidate}" ]] || return 0
  done <<<"${value}"
  return 1
}

emit_multiline_output() {
  local key="$1"
  local value="$2"
  local delimiter="FUGUE_${key}_EOF"
  local suffix=0

  while output_value_contains_line "${value}" "${delimiter}"; do
    suffix=$((suffix + 1))
    delimiter="FUGUE_${key}_EOF_${suffix}"
  done

  {
    printf '%s<<%s\n' "${key}" "${delimiter}"
    printf '%s\n' "${value}" | sed '/^[[:space:]]*$/d'
    printf '%s\n' "${delimiter}"
  } >>"${OUTPUT_BUFFER_FILE}"
}

flush_outputs() {
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    cat "${OUTPUT_BUFFER_FILE}" >>"${GITHUB_OUTPUT}"
  else
    cat "${OUTPUT_BUFFER_FILE}"
  fi
}

resource_json() {
  local kind="$1"
  local name="$2"
  local namespace="${FUGUE_NAMESPACE:-fugue-system}"
  local kubectl_cmd="${KUBECTL_CMD:-kubectl}"
  local compact_resource

  : >"${KUBECTL_SNAPSHOT_FILE}"
  if ! ${kubectl_cmd} -n "${namespace}" get "${kind}/${name}" --ignore-not-found=true -o json >"${KUBECTL_SNAPSHOT_FILE}"; then
    printf 'failed to read live Kubernetes resource %s/%s\n' "${kind}" "${name}" >&2
    return 1
  fi
  if [[ ! -s "${KUBECTL_SNAPSHOT_FILE}" ]]; then
    return 0
  fi
  compact_resource="$(python3 -c '
import json
import sys

try:
    with open(sys.argv[1], "r", encoding="utf-8") as handle:
        doc = json.load(handle)
except (TypeError, ValueError) as exc:
    print(f"live Kubernetes resource is invalid JSON: {exc}", file=sys.stderr)
    raise SystemExit(1)
if not isinstance(doc, dict) or not doc:
    print("live Kubernetes resource is not a non-empty JSON object", file=sys.stderr)
    raise SystemExit(1)
print(json.dumps(doc, separators=(",", ":")))
' "${KUBECTL_SNAPSHOT_FILE}")" || return 1
  printf '%s' "${compact_resource}"
}

resource_container_image() {
  local kind="$1"
  local name="$2"
  local container_name="$3"
  local resource

  resource="$(resource_json "${kind}" "${name}")" || return 1
  [[ -n "${resource}" ]] || return 0
  printf '%s' "${resource}" | python3 -c '
import json
import sys

container_name = sys.argv[1]
doc = json.load(sys.stdin)
containers = (((doc.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers")
if not isinstance(containers, list) or any(not isinstance(item, dict) for item in containers):
    print("live Kubernetes resource has an invalid containers field", file=sys.stderr)
    raise SystemExit(1)
matches = [item for item in containers if item.get("name") == container_name]
if len(matches) != 1:
    print(f"live Kubernetes resource must contain exactly one {container_name} container; found {len(matches)}", file=sys.stderr)
    raise SystemExit(1)
image = matches[0].get("image")
if (
    not isinstance(image, str)
    or not image
    or any(character.isspace() for character in image)
    or any(ord(character) < 0x20 or ord(character) == 0x7F for character in image)
    or "|" in image
):
    print(f"live Kubernetes {container_name} image is missing or unsafe", file=sys.stderr)
    raise SystemExit(1)
print(image)
' "${container_name}"
}

controller_image_and_drain_record() {
  local name="$1"
  local resource

  resource="$(resource_json deploy "${name}")" || return 1
  [[ -n "${resource}" ]] || return 0
  printf '%s' "${resource}" | python3 -c '
import json
import sys

doc = json.load(sys.stdin)
containers = (((doc.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers")
if not isinstance(containers, list) or any(not isinstance(item, dict) for item in containers):
    print("live controller Deployment has an invalid containers field", file=sys.stderr)
    raise SystemExit(1)
matches = [item for item in containers if item.get("name") == "controller"]
if len(matches) != 1:
    print(f"live controller Deployment must contain exactly one controller container; found {len(matches)}", file=sys.stderr)
    raise SystemExit(1)
container = matches[0]
image = container.get("image")
if not isinstance(image, str) or not image:
    print("live controller image is missing", file=sys.stderr)
    raise SystemExit(1)
env = container.get("env") or []
if not isinstance(env, list) or any(not isinstance(item, dict) for item in env):
    print("live controller Deployment has an invalid env field", file=sys.stderr)
    raise SystemExit(1)

def env_value(name):
    matches = [item for item in env if item.get("name") == name]
    if len(matches) > 1:
        raise ValueError(f"live controller Deployment has duplicate {name} values")
    if not matches:
        return ""
    value = matches[0].get("value")
    if not isinstance(value, str):
        raise ValueError(f"live controller Deployment {name} must use one literal string value")
    return value

try:
    values = [
        image,
        env_value("FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY"),
        env_value("FUGUE_DRAIN_AGENT_IMAGE_TAG"),
        env_value("FUGUE_DRAIN_AGENT_IMAGE_DIGEST"),
    ]
except ValueError as exc:
    print(str(exc), file=sys.stderr)
    raise SystemExit(1)
if any("|" in value for value in values) or any(
    any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    for value in values
):
    print("live controller image metadata contains an unsafe record delimiter", file=sys.stderr)
    raise SystemExit(1)
if any(any(character.isspace() for character in value) for value in values if value):
    print("live controller image metadata contains whitespace", file=sys.stderr)
    raise SystemExit(1)
print("|".join(values))
'
}

load_helm_values_snapshot() {
  local helm_cmd="${HELM_CMD:-helm}"

  if [[ "${HELM_VALUES_ATTEMPTED}" == "true" ]]; then
    [[ -n "${HELM_VALUES_FILE}" && -s "${HELM_VALUES_FILE}" ]]
    return
  fi
  HELM_VALUES_ATTEMPTED=true
  HELM_VALUES_FILE="$(mktemp "${TMPDIR:-/tmp}/fugue-helm-values.XXXXXX")"
  chmod 600 "${HELM_VALUES_FILE}"
  if ! ${helm_cmd} get values "${FUGUE_RELEASE_NAME}" -n "${FUGUE_NAMESPACE}" --all -o json >"${HELM_VALUES_FILE}"; then
    printf 'failed to read the live Helm values snapshot required by a digest-only image reference\n' >&2
    rm -f "${HELM_VALUES_FILE}"
    HELM_VALUES_FILE=""
    return 1
  fi
  if ! python3 -c '
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    value = json.load(handle)
if not isinstance(value, dict):
    raise SystemExit(1)
' "${HELM_VALUES_FILE}"; then
    printf 'live Helm values snapshot is not a JSON object\n' >&2
    rm -f "${HELM_VALUES_FILE}"
    HELM_VALUES_FILE=""
    return 1
  fi
}

helm_effective_image_record() {
  local selector="$1"
  local source_name="${2:-}"

  load_helm_values_snapshot || return 1
  python3 -c '
import json
import re
import sys

values_path, selector, source_name, fullname = sys.argv[1:5]
with open(values_path, "r", encoding="utf-8") as handle:
    values = json.load(handle)

fields = ("repository", "tag", "digest")

def image_at(path):
    value = values
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            raise ValueError(f"missing Helm image path: {path}")
        value = value[part]
    if not isinstance(value, dict):
        raise ValueError(f"Helm image path is not an object: {path}")
    result = {}
    for field in fields:
        item = value.get(field, "")
        if not isinstance(item, str):
            raise ValueError(f"Helm image field is not a string: {path}.{field}")
        result[field] = item
    return result

def validated_sparse_image(value, context):
    if value is None:
        value = {}
    if not isinstance(value, dict):
        raise ValueError(f"Helm image override is not an object: {context}")
    for field in fields:
        if field in value and not isinstance(value[field], str):
            raise ValueError(f"Helm image field is not a string: {context}.{field}")
    return value

def atomic_override(base, override, identity_keys_define_override, context):
    override = validated_sparse_image(override, context)
    if identity_keys_define_override:
        active = any(field in override for field in fields)
    else:
        active = any(bool(override.get(field, "")) for field in fields)
    if not active:
        return dict(base)
    return {field: override.get(field, "") for field in fields}

def digest_aware_sparse_overlay(base, override, identity_keys_define_override, context):
    override = validated_sparse_image(override, context)
    repository_override = bool(override.get("repository", ""))
    tag_override = bool(override.get("tag", ""))
    if identity_keys_define_override:
        all_identity_placeholders = all(field in override for field in fields) and not any(
            override.get(field, "") for field in fields
        )
        digest_override = "digest" in override and not all_identity_placeholders
    else:
        digest_override = bool(override.get("digest", ""))
    active = repository_override or tag_override or digest_override
    if not active:
        return dict(base)
    # Repository and source tag remain backward-compatible sparse overrides.
    # Digest is different: activating any identity override without a digest
    # explicitly unpins the inherited digest instead of silently retaining it.
    return {
        "repository": override.get("repository", "") if repository_override else base["repository"],
        "tag": override.get("tag", "") if tag_override else base["tag"],
        "digest": override.get("digest", ""),
    }

def normalized_group_name(raw):
    if not isinstance(raw, str) or not raw:
        raise ValueError("edge group name is missing")
    normalized = raw.lower().replace("_", "-")
    encoded = normalized.encode("utf-8")[:30]
    try:
        normalized = encoded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("edge group name truncates inside a UTF-8 character") from exc
    if normalized.endswith("-"):
        normalized = normalized[:-1]
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", normalized or ""):
        raise ValueError(f"edge group name does not map to a safe component: {raw!r}")
    return normalized

def edge_groups():
    edge = values.get("edge")
    if not isinstance(edge, dict):
        raise ValueError("missing Helm edge values")
    groups = edge.get("groups") or []
    if not isinstance(groups, list):
        raise ValueError("Helm edge.groups is not an array")
    mapped = {}
    for group in groups:
        if not isinstance(group, dict):
            raise ValueError("Helm edge.groups contains a non-object")
        normalized = normalized_group_name(group.get("name"))
        if normalized in {"dynamic", "ssh"} or normalized in mapped:
            raise ValueError(f"ambiguous normalized edge group name: {normalized}")
        raw_image = validated_sparse_image(group.get("image"), f"edge.groups[{normalized}].image")
        blue_green = group.get("blueGreen") or {}
        if not isinstance(blue_green, dict):
            raise ValueError(f"edge group blueGreen is not an object: {normalized}")
        mapped[normalized] = {"image": raw_image, "blueGreen": blue_green}
    return mapped

try:
    if selector not in {"edge_legacy", "edge_public"}:
        image = image_at(selector)
    else:
        root = image_at("edge.image")
        if selector == "edge_legacy":
            image = root
        else:
            prefix = fullname + "-edge-"
            if not source_name.startswith(prefix):
                raise ValueError(f"public edge workload name is outside the release: {source_name}")
            suffix = source_name[len(prefix):]
            mode = ""
            slot = ""
            if suffix == "front":
                group_name = ""
                mode = "front"
            elif suffix == "dynamic-front":
                group_name = "dynamic"
                mode = "front"
            elif suffix.endswith("-front"):
                group_name = suffix[:-len("-front")]
                mode = "front"
            elif suffix in {"worker-a", "worker-b"}:
                group_name = ""
                mode = "worker"
                slot = suffix[-1]
            elif suffix in {"dynamic-worker-a", "dynamic-worker-b"}:
                group_name = "dynamic"
                mode = "worker"
                slot = suffix[-1]
            else:
                match = re.fullmatch(r"(.+)-worker-([ab])", suffix)
                if not match:
                    raise ValueError(f"unrecognized public edge workload name: {source_name}")
                group_name, slot = match.groups()
                mode = "worker"

            base = root
            local_blue_green = {}
            if group_name not in {"", "dynamic"}:
                groups = edge_groups()
                if group_name not in groups:
                    raise ValueError(f"public edge workload has no unique Helm group: {source_name}")
                group = groups[group_name]
                base = digest_aware_sparse_overlay(root, group["image"], True, f"edge.groups[{group_name}].image")
                local_blue_green = group["blueGreen"]
            elif group_name == "dynamic":
                # Evaluating groups still proves that a user group cannot alias
                # the reserved dynamic cohort name.
                edge_groups()
                dynamic = (values.get("edge") or {}).get("dynamic") or {}
                if not isinstance(dynamic, dict):
                    raise ValueError("Helm edge.dynamic is not an object")
                local_blue_green = dynamic.get("blueGreen") or {}
                if not isinstance(local_blue_green, dict):
                    raise ValueError("Helm edge.dynamic.blueGreen is not an object")

            if mode == "front":
                root_image = digest_aware_sparse_overlay(
                    base,
                    image_at("edge.blueGreen.front.image"),
                    False,
                    "edge.blueGreen.front.image",
                )
                local_front = local_blue_green.get("front") or {}
                if not isinstance(local_front, dict):
                    raise ValueError("local edge blueGreen.front is not an object")
                image = atomic_override(
                    root_image,
                    local_front.get("image"),
                    True,
                    "local edge blueGreen.front.image",
                )
            else:
                root_image = digest_aware_sparse_overlay(
                    base,
                    image_at(f"edge.blueGreen.slots.{slot}.image"),
                    False,
                    f"edge.blueGreen.slots.{slot}.image",
                )
                local_slots = local_blue_green.get("slots") or {}
                if not isinstance(local_slots, dict):
                    raise ValueError("local edge blueGreen.slots is not an object")
                local_slot = local_slots.get(slot) or {}
                if not isinstance(local_slot, dict):
                    raise ValueError(f"local edge blueGreen slot {slot} is not an object")
                image = atomic_override(
                    root_image,
                    local_slot.get("image"),
                    True,
                    f"local edge blueGreen.slots.{slot}.image",
                )
except (TypeError, ValueError) as exc:
    print(f"cannot recover image source tag from Helm values: {exc}", file=sys.stderr)
    raise SystemExit(1)

if any(
    any(ord(character) < 0x20 or ord(character) == 0x7F for character in image[field])
    for field in fields
):
    print("cannot recover image source tag from Helm values: image identity contains a control character", file=sys.stderr)
    raise SystemExit(1)
print("\t".join(image[field] for field in fields))
' "${HELM_VALUES_FILE}" "${selector}" "${source_name}" "${FUGUE_RELEASE_FULLNAME}"
}

helm_source_tag_for_digest_ref() {
  local image_ref="$1"
  local selector="$2"
  local source_name="${3:-}"
  local record repository source_tag digest
  local expected_repository="${RELEASE_IMAGE_REF_REPOSITORY}"
  local expected_digest="${RELEASE_IMAGE_REF_DIGEST}"

  record="$(helm_effective_image_record "${selector}" "${source_name}")" || return 1
  IFS=$'\t' read -r repository source_tag digest <<<"${record}"
  release_image_ref_valid_repository "${repository}" || {
    printf 'Helm image repository is invalid for %s\n' "${source_name:-${selector}}" >&2
    return 1
  }
  release_image_ref_valid_tag "${source_tag}" || {
    printf 'Helm source tag is missing or invalid for digest-only image %s\n' "${image_ref}" >&2
    return 1
  }
  [[ "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    printf 'Helm digest is missing or invalid for digest-only image %s\n' "${image_ref}" >&2
    return 1
  }
  [[ "${repository}" == "${expected_repository}" ]] || {
    printf 'Helm repository does not match live digest-only image %s\n' "${image_ref}" >&2
    return 1
  }
  [[ "${digest}" == "${expected_digest}" ]] || {
    printf 'Helm digest does not match live digest-only image %s\n' "${image_ref}" >&2
    return 1
  }
  printf '%s' "${source_tag}"
}

resolve_image_ref() {
  local image_ref="$1"
  local fallback_repository="$2"
  local fallback_tag="$3"
  local helm_selector="$4"
  local source_name="${5:-}"
  local observed_source_tag="${6:-}"
  local source_tag=""

  RESOLVED_IMAGE_IS_LIVE=false
  if [[ -n "${image_ref}" ]]; then
    RESOLVED_IMAGE_IS_LIVE=true
    release_image_ref_parse "${image_ref}" || return 1
    if [[ "${RELEASE_IMAGE_REF_FORM}" == "digest" ]]; then
      # Load in the parent shell before command substitution so every component
      # reads the same one-shot snapshot instead of re-running Helm in a
      # subshell.
      load_helm_values_snapshot || return 1
      source_tag="$(helm_source_tag_for_digest_ref "${image_ref}" "${helm_selector}" "${source_name}")" || return 1
      if [[ -n "${observed_source_tag}" && "${observed_source_tag}" != "${source_tag}" ]]; then
        printf 'live source tag disagrees with Helm values for digest-only image %s\n' "${image_ref}" >&2
        return 1
      fi
      release_image_ref_parse "${image_ref}" "${source_tag}" || return 1
    fi
  else
    release_image_ref_parse "${fallback_repository}:${fallback_tag}" || return 1
  fi

  RESOLVED_IMAGE_REPOSITORY="${RELEASE_IMAGE_REF_REPOSITORY}"
  RESOLVED_IMAGE_SOURCE_TAG="${RELEASE_IMAGE_REF_SOURCE_TAG}"
  RESOLVED_IMAGE_DIGEST="${RELEASE_IMAGE_REF_DIGEST}"
  RESOLVED_IMAGE_TEMPLATE_REF="${RELEASE_IMAGE_REF_EXACT_TEMPLATE_REF}"
}

public_edge_cohort_records() {
  local namespace="${FUGUE_NAMESPACE}"
  local kubectl_cmd="${KUBECTL_CMD}"
  : >"${KUBECTL_SNAPSHOT_FILE}"
  if ! ${kubectl_cmd} -n "${namespace}" get ds -o json >"${KUBECTL_SNAPSHOT_FILE}"; then
    printf 'failed to read the public edge DaemonSet cohort\n' >&2
    return 1
  fi
  [[ -s "${KUBECTL_SNAPSHOT_FILE}" ]] || {
    printf 'public edge DaemonSet cohort returned empty output\n' >&2
    return 1
  }
  python3 -c '
import json
import re
import sys

snapshot_path, release_name, fullname = sys.argv[1:4]
try:
    with open(snapshot_path, "r", encoding="utf-8") as handle:
        doc = json.load(handle)
except (TypeError, ValueError) as exc:
    print(f"public edge cohort inventory is invalid JSON: {exc}", file=sys.stderr)
    raise SystemExit(1)
if not isinstance(doc, dict):
    print("public edge cohort inventory is not an object", file=sys.stderr)
    raise SystemExit(1)
if "items" not in doc or not isinstance(doc.get("items"), list):
    print("public edge cohort inventory items is not an array", file=sys.stderr)
    raise SystemExit(1)
items = doc["items"]

records = []
seen = set()
for item in items:
    if not isinstance(item, dict):
        print("public edge cohort contains a non-object DaemonSet", file=sys.stderr)
        raise SystemExit(1)
    metadata = item.get("metadata") or {}
    labels = metadata.get("labels") or {}
    if not isinstance(metadata, dict) or not isinstance(labels, dict):
        print("public edge cohort contains invalid metadata", file=sys.stderr)
        raise SystemExit(1)
    if labels.get("app.kubernetes.io/instance") != release_name:
        continue
    if labels.get("fugue.io/rollout-subsystem") != "public-data-plane":
        continue
    component = str(labels.get("app.kubernetes.io/component") or "")
    rollout_mode = labels.get("fugue.io/rollout-mode")
    if rollout_mode == "node-local-blue-green-front":
        if component != "edge-front" and not re.fullmatch(r"edge-.+-front", component):
            continue
        if "fugue.io/edge-slot" in labels:
            print(f"public edge front {component} must not carry an edge-slot label", file=sys.stderr)
            raise SystemExit(1)
        container_name = "edge-front"
    elif rollout_mode == "node-local-blue-green-worker":
        worker_match = re.fullmatch(r"edge(?:-.+)?-worker-([ab])", component)
        if not worker_match:
            continue
        expected_slot = worker_match.group(1)
        if labels.get("fugue.io/edge-slot") != expected_slot:
            print(f"public edge worker {component} has an invalid edge-slot label", file=sys.stderr)
            raise SystemExit(1)
        container_name = "edge"
    else:
        continue
    name = str(metadata.get("name") or "")
    expected_name = f"{fullname}-{component}"
    if name != expected_name:
        display_name = name or "<empty>"
        print(
            f"public edge DaemonSet name/component binding is invalid: name={display_name} component={component}",
            file=sys.stderr,
        )
        raise SystemExit(1)
    if not name or name in seen:
        print("public edge cohort contains an empty or duplicate DaemonSet name", file=sys.stderr)
        raise SystemExit(1)
    seen.add(name)
    containers = (((item.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []
    if not isinstance(containers, list):
        print(f"public edge DaemonSet {name} has invalid containers", file=sys.stderr)
        raise SystemExit(1)
    images = [container.get("image") for container in containers if isinstance(container, dict) and container.get("name") == container_name]
    if len(images) != 1 or not isinstance(images[0], str) or not images[0]:
        print(f"public edge DaemonSet {name} must have one {container_name} image", file=sys.stderr)
        raise SystemExit(1)
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in images[0]) or "\t" in name:
        print(f"public edge DaemonSet {name} image record contains a control character", file=sys.stderr)
        raise SystemExit(1)
    records.append((name, images[0]))

for name, image in sorted(records):
    print(f"{name}\t{image}")
' "${KUBECTL_SNAPSHOT_FILE}" "${FUGUE_RELEASE_NAME}" "${FUGUE_RELEASE_FULLNAME}"
}

output_image_ref() {
  local prefix="$1"
  local image_ref="$2"
  local fallback_repository="$3"
  local fallback_tag="$4"
  local include_release_baseline="${5:-false}"
  local helm_selector="${6:-}"
  local source_name="${7:-}"
  local observed_source_tag="${8:-}"
  local baseline_ref=""

  resolve_image_ref "${image_ref}" "${fallback_repository}" "${fallback_tag}" "${helm_selector}" "${source_name}" "${observed_source_tag}" || return 1
  if [[ "${RESOLVED_IMAGE_IS_LIVE}" == "true" ]]; then
    baseline_ref="${RESOLVED_IMAGE_SOURCE_TAG}"
    if [[ "${include_release_baseline}" == "true" ]]; then
      RELEASE_BASELINE_TAGS="${RELEASE_BASELINE_TAGS}${RELEASE_BASELINE_TAGS:+$'\n'}${RESOLVED_IMAGE_SOURCE_TAG}"
    fi
  fi
  emit_output "${prefix}_image_repository" "${RESOLVED_IMAGE_REPOSITORY}"
  emit_output "${prefix}_image_tag" "${RESOLVED_IMAGE_SOURCE_TAG}"
  emit_output "${prefix}_image_baseline_ref" "${baseline_ref}"
  emit_output "${prefix}_image_template_ref" "${RESOLVED_IMAGE_TEMPLATE_REF}"
  emit_output "${prefix}_image_digest" "${RESOLVED_IMAGE_DIGEST}"
  emit_output "${prefix}_image_source_tag" "${RESOLVED_IMAGE_SOURCE_TAG}"
  printf '%s resolved image: %s\n' "${prefix}" "${RESOLVED_IMAGE_TEMPLATE_REF}"
}

FUGUE_NAMESPACE="${FUGUE_NAMESPACE:-fugue-system}"
FUGUE_RELEASE_NAME="${FUGUE_RELEASE_NAME:-fugue}"
FUGUE_RELEASE_FULLNAME="${FUGUE_RELEASE_FULLNAME:-fugue-fugue}"
KUBECTL_CMD="$(detect_kubectl)"
RELEASE_BASELINE_TAGS=""

api_image="$(resource_container_image deploy "${FUGUE_RELEASE_FULLNAME}-api" api)" || exit 1
if [[ -z "${api_image}" ]]; then
  api_image="$(resource_container_image deploy "${FUGUE_RELEASE_FULLNAME}" api)" || exit 1
fi
controller_record="$(controller_image_and_drain_record "${FUGUE_RELEASE_FULLNAME}-controller")" || exit 1
controller_image=""
drain_repository=""
drain_tag=""
drain_digest=""
if [[ -n "${controller_record}" ]]; then
  IFS='|' read -r controller_image drain_repository drain_tag drain_digest <<<"${controller_record}"
fi
telemetry_image="$(resource_container_image deploy "${FUGUE_RELEASE_FULLNAME}-telemetry-agent" telemetry-agent)" || exit 1
image_cache_image="$(resource_container_image ds "${FUGUE_RELEASE_FULLNAME}-image-cache" image-cache)" || exit 1
edge_source_name="${FUGUE_RELEASE_FULLNAME}-edge"
edge_image="$(resource_container_image ds "${edge_source_name}" edge)" || exit 1
if [[ -z "${edge_image}" ]]; then
  edge_source_name="${FUGUE_RELEASE_FULLNAME}-edge-worker-a"
  edge_image="$(resource_container_image ds "${edge_source_name}" edge)" || exit 1
fi

drain_image=""
if [[ -n "${drain_repository}" && -n "${drain_digest}" ]]; then
  drain_image="${drain_repository}@${drain_digest}"
elif [[ -n "${drain_repository}" && -n "${drain_tag}" ]]; then
  drain_image="${drain_repository}:${drain_tag}"
elif [[ -n "${drain_repository}${drain_tag}${drain_digest}" ]]; then
  printf 'live drain-agent image metadata is partial\n' >&2
  exit 1
fi

fallback_tag="${FUGUE_IMAGE_TAG:-${GITHUB_SHA:-}}"
output_image_ref api "${api_image}" "${FUGUE_API_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-api}" "${fallback_tag}" true "api.image"
output_image_ref controller "${controller_image}" "${FUGUE_CONTROLLER_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-controller}" "${fallback_tag}" true "controller.image"
output_image_ref telemetry_agent "${telemetry_image}" "${FUGUE_TELEMETRY_AGENT_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-telemetry-agent}" "${fallback_tag}" false "observability.agent.image"
output_image_ref drain_agent "${drain_image}" "${FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-drain-agent}" "${fallback_tag}" false "runtime.strictDrain.agent.image" "${FUGUE_RELEASE_FULLNAME}-controller" "${drain_tag}"
output_image_ref image_cache "${image_cache_image}" "${FUGUE_IMAGE_CACHE_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-image-cache}" "${fallback_tag}" false "imageCache.image"
if [[ "${edge_source_name}" == "${FUGUE_RELEASE_FULLNAME}-edge" ]]; then
  edge_helm_selector="edge_legacy"
else
  edge_helm_selector="edge_public"
fi
output_image_ref edge "${edge_image}" "${FUGUE_EDGE_IMAGE_REPOSITORY:-ghcr.io/yym68686/fugue-edge}" "${fallback_tag}" false "${edge_helm_selector}" "${edge_source_name}"

PUBLIC_COHORT_TEMPLATE_REFS=""
PUBLIC_COHORT_DIGESTS=""
PUBLIC_COHORT_SOURCE_TAGS=""
PUBLIC_COHORT_COUNT=0
EDGE_SOURCE_SEEN_IN_COHORT=false
public_cohort_records="$(public_edge_cohort_records)" || exit 1
while IFS=$'\t' read -r cohort_name cohort_ref; do
  [[ -n "${cohort_name}" ]] || continue
  if [[ -n "${edge_image}" && "${cohort_name}" == "${edge_source_name}" ]]; then
    [[ "${cohort_ref}" == "${edge_image}" ]] || {
      printf 'public edge source changed between exact and cohort snapshots: %s\n' "${cohort_name}" >&2
      exit 1
    }
    EDGE_SOURCE_SEEN_IN_COHORT=true
  fi
  resolve_image_ref "${cohort_ref}" "" "" edge_public "${cohort_name}" || exit 1
  PUBLIC_COHORT_TEMPLATE_REFS="${PUBLIC_COHORT_TEMPLATE_REFS}${PUBLIC_COHORT_TEMPLATE_REFS:+$'\n'}${cohort_name}|${RESOLVED_IMAGE_TEMPLATE_REF}"
  PUBLIC_COHORT_DIGESTS="${PUBLIC_COHORT_DIGESTS}${PUBLIC_COHORT_DIGESTS:+$'\n'}${cohort_name}|${RESOLVED_IMAGE_DIGEST}"
  PUBLIC_COHORT_SOURCE_TAGS="${PUBLIC_COHORT_SOURCE_TAGS}${PUBLIC_COHORT_SOURCE_TAGS:+$'\n'}${cohort_name}|${RESOLVED_IMAGE_SOURCE_TAG}"
  PUBLIC_COHORT_COUNT=$((PUBLIC_COHORT_COUNT + 1))
done <<<"${public_cohort_records}"
if [[ "${edge_source_name}" == "${FUGUE_RELEASE_FULLNAME}-edge-worker-a" ]]; then
  if [[ -n "${edge_image}" && "${EDGE_SOURCE_SEEN_IN_COHORT}" != "true" ]]; then
    printf 'public edge worker-a source is missing from the authoritative cohort snapshot\n' >&2
    exit 1
  fi
  if [[ -z "${edge_image}" && "${PUBLIC_COHORT_COUNT}" -ne 0 ]]; then
    printf 'public edge cohort appeared after the exact edge source was absent\n' >&2
    exit 1
  fi
fi
emit_output public_cohort_image_count "${PUBLIC_COHORT_COUNT}"
emit_multiline_output public_cohort_image_template_refs "${PUBLIC_COHORT_TEMPLATE_REFS}"
emit_multiline_output public_cohort_image_digests "${PUBLIC_COHORT_DIGESTS}"
emit_multiline_output public_cohort_image_source_tags "${PUBLIC_COHORT_SOURCE_TAGS}"
emit_multiline_output release_baseline_tags "$(printf '%s\n' "${RELEASE_BASELINE_TAGS}" | sed '/^[[:space:]]*$/d' | sort -u)"
flush_outputs
