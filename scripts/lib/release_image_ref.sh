#!/usr/bin/env bash

# Strict parser for the image-reference forms used by the release pipeline.
#
# release_image_ref_parse REF [SOURCE_TAG_HINT]
#
# On success, the function populates these canonical fields:
#   RELEASE_IMAGE_REF_REPOSITORY
#   RELEASE_IMAGE_REF_SOURCE_TAG
#   RELEASE_IMAGE_REF_DIGEST
#   RELEASE_IMAGE_REF_EXACT_TEMPLATE_REF
#   RELEASE_IMAGE_REF_FORM          (tag, digest, or tag_digest)
#   RELEASE_IMAGE_REF_PINNED        (true when a digest is present)
#
# SOURCE_TAG_HINT is only useful for a digest-only template reference. Callers
# must obtain it from an authoritative source; the parser never invents a tag.

release_image_ref_fail() {
  printf 'invalid release image reference %q: %s\n' "${1:-}" "${2:-invalid value}" >&2
  return 1
}

release_image_ref_valid_tag() {
  local tag="${1:-}"
  (( ${#tag} <= 128 )) || return 1
  [[ "${tag}" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]*$ ]]
}

release_image_ref_valid_repository_component() {
  local component="${1:-}"
  [[ "${component}" =~ ^[a-z0-9]+(([._]|__|-+)[a-z0-9]+)*$ ]]
}

release_image_ref_valid_registry_host() {
  local host="${1:-}"
  local label
  local -a labels=()

  [[ -n "${host}" && ${#host} -le 253 ]] || return 1
  [[ "${host}" != .* && "${host}" != *. && "${host}" != *..* ]] || return 1
  IFS='.' read -r -a labels <<<"${host}"
  for label in "${labels[@]}"; do
    (( ${#label} >= 1 && ${#label} <= 63 )) || return 1
    [[ "${label}" =~ ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$ ]] || return 1
  done
}

release_image_ref_valid_repository() {
  local repository="${1:-}"
  local first host port component
  local -a components=()

  [[ -n "${repository}" && ${#repository} -le 255 ]] || return 1
  [[ "${repository}" != /* && "${repository}" != */ && "${repository}" != *//* ]] || return 1
  [[ "${repository}" != *[[:space:]@]* ]] || return 1

  IFS='/' read -r -a components <<<"${repository}"
  (( ${#components[@]} > 0 )) || return 1
  first="${components[0]}"
  # Production release repositories use canonical lower-case DNS/IPv4
  # authorities. Bracketed IPv6 authorities stay fail-closed until the whole
  # publish/verification chain has an explicit IPv6 registry contract.
  if [[ "${first}" == *:* ]]; then
    [[ "${first}" != *:*:* ]] || return 1
    (( ${#components[@]} >= 2 )) || return 1
    host="${first%%:*}"
    port="${first##*:}"
    release_image_ref_valid_registry_host "${host}" || return 1
    [[ "${port}" =~ ^[1-9][0-9]{0,4}$ ]] || return 1
    (( 10#${port} >= 1 && 10#${port} <= 65535 )) || return 1
    components=("${components[@]:1}")
  elif (( ${#components[@]} >= 2 )) && [[ "${first}" == "localhost" || "${first}" == *.* ]]; then
    release_image_ref_valid_registry_host "${first}" || return 1
    components=("${components[@]:1}")
  fi

  for component in "${components[@]}"; do
    release_image_ref_valid_repository_component "${component}" || return 1
  done
}

release_image_ref_parse() {
  local image_ref="${1:-}"
  local source_tag_hint="${2:-}"
  local at_chars name_and_tag digest repository tag="" last ref_form pinned exact_ref

  RELEASE_IMAGE_REF_REPOSITORY=""
  RELEASE_IMAGE_REF_SOURCE_TAG=""
  RELEASE_IMAGE_REF_DIGEST=""
  RELEASE_IMAGE_REF_EXACT_TEMPLATE_REF=""
  RELEASE_IMAGE_REF_FORM=""
  RELEASE_IMAGE_REF_PINNED=""

  [[ -n "${image_ref}" ]] || release_image_ref_fail "${image_ref}" "reference is empty" || return 1
  [[ "${image_ref}" != *[[:space:]]* ]] || release_image_ref_fail "${image_ref}" "whitespace is not allowed" || return 1
  [[ "${image_ref}" != *://* ]] || release_image_ref_fail "${image_ref}" "URL schemes are not image references" || return 1

  at_chars="${image_ref//[^@]/}"
  (( ${#at_chars} <= 1 )) || release_image_ref_fail "${image_ref}" "multiple digest separators are not allowed" || return 1

  if [[ "${image_ref}" == *@* ]]; then
    name_and_tag="${image_ref%@*}"
    digest="${image_ref##*@}"
    [[ -n "${name_and_tag}" ]] || release_image_ref_fail "${image_ref}" "repository is missing" || return 1
    [[ "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]] || release_image_ref_fail "${image_ref}" "digest must be lowercase sha256 with 64 hexadecimal characters" || return 1
  else
    name_and_tag="${image_ref}"
    digest=""
  fi

  last="${name_and_tag##*/}"
  if [[ "${last}" == *:* ]]; then
    tag="${last##*:}"
    repository="${name_and_tag%:*}"
    release_image_ref_valid_tag "${tag}" || release_image_ref_fail "${image_ref}" "tag is invalid" || return 1
  else
    repository="${name_and_tag}"
  fi

  release_image_ref_valid_repository "${repository}" || release_image_ref_fail "${image_ref}" "repository is invalid or ambiguous" || return 1
  if [[ -z "${digest}" && -z "${tag}" ]]; then
    release_image_ref_fail "${image_ref}" "an explicit tag or digest is required"
    return 1
  fi

  if [[ -n "${source_tag_hint}" ]]; then
    release_image_ref_valid_tag "${source_tag_hint}" || release_image_ref_fail "${image_ref}" "source tag hint is invalid" || return 1
    if [[ -n "${tag}" && "${tag}" != "${source_tag_hint}" ]]; then
      release_image_ref_fail "${image_ref}" "source tag hint disagrees with the reference tag"
      return 1
    fi
  fi

  if [[ -n "${digest}" && -n "${tag}" ]]; then
    ref_form="tag_digest"
    pinned="true"
    exact_ref="${repository}:${tag}@${digest}"
  elif [[ -n "${digest}" ]]; then
    ref_form="digest"
    pinned="true"
    exact_ref="${repository}@${digest}"
    tag="${source_tag_hint}"
  else
    ref_form="tag"
    pinned="false"
    exact_ref="${repository}:${tag}"
  fi

  RELEASE_IMAGE_REF_REPOSITORY="${repository}"
  RELEASE_IMAGE_REF_SOURCE_TAG="${tag}"
  RELEASE_IMAGE_REF_DIGEST="${digest}"
  RELEASE_IMAGE_REF_EXACT_TEMPLATE_REF="${exact_ref}"
  RELEASE_IMAGE_REF_FORM="${ref_form}"
  RELEASE_IMAGE_REF_PINNED="${pinned}"
}
