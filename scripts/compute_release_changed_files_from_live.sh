#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="${FUGUE_RELEASE_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
TARGET_REF="${FUGUE_RELEASE_TARGET_REF:-${AFTER_SHA:-HEAD}}"

trim_field() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

emit_multiline_output() {
  local key="$1"
  local file="$2"

  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    {
      printf '%s<<EOF\n' "${key}"
      cat "${file}"
      printf 'EOF\n'
    } >>"${GITHUB_OUTPUT}"
  elif [[ "${key}" == "changed_files" ]]; then
    cat "${file}"
  fi
}

emit_output() {
  local key="$1"
  local value="$2"

  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    printf '%s=%s\n' "${key}" "${value}" >>"${GITHUB_OUTPUT}"
  fi
}

normalized_base_refs() {
  printf '%s\n' "${FUGUE_RELEASE_BASE_REFS:-}" |
    tr ',' '\n' |
    sed 's/^[[:space:]]*//;s/[[:space:]]*$//' |
    sed '/^$/d' |
    sort -u
}

if ! git -C "${REPO_ROOT}" cat-file -e "${TARGET_REF}^{commit}" 2>/dev/null; then
  printf 'release target ref is not a locally available commit: %s\n' "${TARGET_REF}" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

changed_file="${tmp_dir}/changed-files"
base_file="${tmp_dir}/base-refs"
unresolved_file="${tmp_dir}/unresolved-refs"
: >"${changed_file}"
: >"${base_file}"
: >"${unresolved_file}"

while IFS= read -r raw_ref; do
  ref="$(trim_field "${raw_ref}")"
  [[ -n "${ref}" ]] || continue
  if ! git -C "${REPO_ROOT}" cat-file -e "${ref}^{commit}" 2>/dev/null; then
    printf '%s\n' "${ref}" >>"${unresolved_file}"
    continue
  fi
  printf '%s\n' "${ref}" >>"${base_file}"
  if [[ "$(git -C "${REPO_ROOT}" rev-parse "${ref}^{commit}")" == "$(git -C "${REPO_ROOT}" rev-parse "${TARGET_REF}^{commit}")" ]]; then
    continue
  fi
  git -C "${REPO_ROOT}" diff --name-only "${ref}" "${TARGET_REF}" >>"${changed_file}"
done < <(normalized_base_refs)

sort -u "${base_file}" -o "${base_file}"
sort -u "${unresolved_file}" -o "${unresolved_file}"
sort -u "${changed_file}" -o "${changed_file}"

if [[ -s "${unresolved_file}" ]]; then
  printf 'cannot derive a trustworthy live-to-target release diff; unresolved live image refs:\n' >&2
  sed 's/^/  /' "${unresolved_file}" >&2
  exit 1
fi

source="live_image_refs"
if [[ ! -s "${base_file}" ]]; then
  if [[ "${FUGUE_RELEASE_REQUIRE_BASELINE:-false}" == "true" ]]; then
    printf 'cannot derive a trustworthy live-to-target release diff; no live API or controller image baseline was found\n' >&2
    exit 1
  fi
  source="no_live_image_baseline"
  printf 'no live image commit baseline found; emitting an empty change set so image planning fails safe by rebuilding all images\n' >&2
fi

emit_multiline_output changed_files "${changed_file}"
emit_multiline_output baseline_refs "${base_file}"
emit_output source "${source}"
emit_output target_ref "$(git -C "${REPO_ROOT}" rev-parse "${TARGET_REF}^{commit}")"

printf 'release diff source=%s target=%s baselines=%s changed_files=%s\n' \
  "${source}" \
  "$(git -C "${REPO_ROOT}" rev-parse --short "${TARGET_REF}^{commit}")" \
  "$(wc -l <"${base_file}" | tr -d ' ')" \
  "$(wc -l <"${changed_file}" | tr -d ' ')" >&2
