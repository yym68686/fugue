#!/usr/bin/env sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)
DIST_DIR=${1:-"${REPO_ROOT}/dist/cli"}
GO_CACHE_DIR=${GOCACHE:-"${REPO_ROOT}/.gocache"}
LDFLAGS=${FUGUE_CLI_LDFLAGS:-"-s -w"}
TARGETS=${FUGUE_CLI_TARGETS:-"linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64"}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
    return 0
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
    return 0
  fi

  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$1" | awk '{print $NF}'
    return 0
  fi

  printf 'missing checksum tool (sha256sum, shasum, or openssl)\n' >&2
  return 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

require_command go
require_command tar
require_command zip

mkdir -p "${DIST_DIR}"
rm -f "${DIST_DIR}"/fugue_*.tar.gz "${DIST_DIR}"/fugue_*.zip "${DIST_DIR}"/fugue_checksums.txt

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/fugue-cli-dist.XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT INT TERM HUP

for target in ${TARGETS}; do
  goos=${target%/*}
  goarch=${target#*/}
  package_dir="${WORK_DIR}/${goos}_${goarch}"
  binary_name="fugue"
  archive_path="${DIST_DIR}/fugue_${goos}_${goarch}.tar.gz"

  if [ "${goos}" = "windows" ]; then
    binary_name="fugue.exe"
    archive_path="${DIST_DIR}/fugue_${goos}_${goarch}.zip"
  fi

  mkdir -p "${package_dir}"
  env \
    GOCACHE="${GO_CACHE_DIR}" \
    GOOS="${goos}" \
    GOARCH="${goarch}" \
    CGO_ENABLED=0 \
    go build -trimpath -ldflags="${LDFLAGS}" -o "${package_dir}/${binary_name}" ./cmd/fugue

  if [ "${goos}" = "windows" ]; then
    (
      cd "${package_dir}"
      zip -q "${archive_path}" "${binary_name}"
    )
    continue
  fi

  (
    cd "${package_dir}"
    tar -czf "${archive_path}" "${binary_name}"
  )
done

CHECKSUM_FILE="${DIST_DIR}/fugue_checksums.txt"
: > "${CHECKSUM_FILE}"

for archive_path in "${DIST_DIR}"/fugue_*.tar.gz "${DIST_DIR}"/fugue_*.zip; do
  if [ ! -f "${archive_path}" ]; then
    continue
  fi
  printf '%s  %s\n' "$(sha256_file "${archive_path}")" "$(basename "${archive_path}")" >> "${CHECKSUM_FILE}"
done
