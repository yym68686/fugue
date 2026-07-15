#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/release_image_ref.sh
source "${REPO_ROOT}/scripts/lib/release_image_ref.sh"

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

assert_parse() {
  local ref="$1"
  local hint="$2"
  local repository="$3"
  local source_tag="$4"
  local digest="$5"
  local exact_ref="$6"
  local form="$7"
  local pinned="$8"

  release_image_ref_parse "${ref}" "${hint}" || fail "expected valid image reference: ${ref}"
  assert_eq "${RELEASE_IMAGE_REF_REPOSITORY}" "${repository}" "${ref} repository"
  assert_eq "${RELEASE_IMAGE_REF_SOURCE_TAG}" "${source_tag}" "${ref} source tag"
  assert_eq "${RELEASE_IMAGE_REF_DIGEST}" "${digest}" "${ref} digest"
  assert_eq "${RELEASE_IMAGE_REF_EXACT_TEMPLATE_REF}" "${exact_ref}" "${ref} exact ref"
  assert_eq "${RELEASE_IMAGE_REF_FORM}" "${form}" "${ref} form"
  assert_eq "${RELEASE_IMAGE_REF_PINNED}" "${pinned}" "${ref} pinned"
}

assert_rejected() {
  local ref="$1"
  local hint="${2:-}"
  if release_image_ref_parse "${ref}" "${hint}" >/dev/null 2>&1; then
    fail "expected invalid image reference to be rejected: ${ref}"
  fi
}

DIGEST="sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
assert_parse "ghcr.io/acme/fugue-api:release-1" "" "ghcr.io/acme/fugue-api" "release-1" "" "ghcr.io/acme/fugue-api:release-1" tag false
assert_parse "registry.example:5000/team/image:v2" "" "registry.example:5000/team/image" v2 "" "registry.example:5000/team/image:v2" tag false
assert_parse "xn--bcher-kva.example:5000/team/image:v2" "" "xn--bcher-kva.example:5000/team/image" v2 "" "xn--bcher-kva.example:5000/team/image:v2" tag false
assert_parse "ghcr.io/acme/fugue-api@${DIGEST}" release-1 "ghcr.io/acme/fugue-api" release-1 "${DIGEST}" "ghcr.io/acme/fugue-api@${DIGEST}" digest true
assert_parse "registry.example:5000/team/image:v2@${DIGEST}" v2 "registry.example:5000/team/image" v2 "${DIGEST}" "registry.example:5000/team/image:v2@${DIGEST}" tag_digest true

assert_rejected ""
assert_rejected "ghcr.io/acme/image"
assert_rejected "ghcr.io/acme/image:"
assert_rejected "ghcr.io/acme/image@"
assert_rejected "ghcr.io/acme/image@@${DIGEST}"
assert_rejected "ghcr.io/acme/image@sha256:0123"
assert_rejected "ghcr.io/acme/image@sha256:ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789"
assert_rejected "GHCR.io/acme/image:v1"
assert_rejected "ghcr.io/Acme/image:v1"
assert_rejected "https://ghcr.io/acme/image:v1"
assert_rejected "ghcr.io/acme/image:bad tag"
assert_rejected "ghcr.io:0/acme/image:v1"
assert_rejected "ghcr.io:65536/acme/image:v1"
assert_rejected "ghcr.io:notaport/acme/image:v1"
assert_rejected "registry.example:5000:v1"
assert_rejected "registry.example:000080/acme/image:v1"
assert_rejected "registry.example:080/acme/image:v1"
assert_rejected "bad_host.example/acme/image:v1"
assert_rejected "[::1]:5000/acme/image:v1"
assert_rejected "ghcr.io/acme/image:v1@${DIGEST}" v2

printf 'release image reference tests passed\n'
