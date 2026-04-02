#!/usr/bin/env sh

set -eu

DEFAULT_REPO="yym68686/fugue"

log() {
  printf '[fugue-install] %s\n' "$*"
}

warn() {
  printf '[fugue-install] warning: %s\n' "$*" >&2
}

fail() {
  printf '[fugue-install] error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Install fugue from GitHub Releases.

Usage:
  curl -fsSL https://raw.githubusercontent.com/yym68686/fugue/main/scripts/install_fugue_cli.sh | sh

Environment variables:
  FUGUE_VERSION           Release to install. Defaults to latest.
  FUGUE_INSTALL_DIR       Install directory for the fugue binary.
  FUGUE_INSTALL_REPO      GitHub repo in owner/name form. Defaults to yym68686/fugue.
  FUGUE_INSTALL_BASE_URL  Full download base URL override.
  FUGUE_INSTALL_DRY_RUN   Set to 1 to print the resolved download URL and exit.
EOF
}

normalize_dir() {
  case "$1" in
    /)
      printf '/\n'
      ;;
    */)
      printf '%s\n' "${1%/}"
      ;;
    *)
      printf '%s\n' "$1"
      ;;
  esac
}

is_on_path() {
  dir=$1
  case ":${PATH:-}:" in
    *":${dir}:"*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

can_install_to() {
  dir=$1
  while [ ! -d "${dir}" ]; do
    parent_dir=$(dirname "${dir}")
    if [ "${parent_dir}" = "${dir}" ]; then
      return 1
    fi
    dir=${parent_dir}
  done

  [ -w "${dir}" ]
}

choose_install_dir() {
  if [ -n "${FUGUE_INSTALL_DIR:-}" ]; then
    normalize_dir "${FUGUE_INSTALL_DIR}"
    return
  fi

  for dir in /opt/homebrew/bin /usr/local/bin "${HOME}/.local/bin" "${HOME}/bin"; do
    if [ -d "${dir}" ] && [ -w "${dir}" ] && is_on_path "${dir}"; then
      printf '%s\n' "${dir}"
      return
    fi
  done

  for dir in /opt/homebrew/bin /usr/local/bin "${HOME}/.local/bin" "${HOME}/bin"; do
    if can_install_to "${dir}"; then
      printf '%s\n' "${dir}"
      return
    fi
  done

  fail "could not find a writable install directory; set FUGUE_INSTALL_DIR"
}

detect_os() {
  case "$(uname -s)" in
    Linux)
      printf 'linux\n'
      ;;
    Darwin)
      printf 'darwin\n'
      ;;
    *)
      fail "unsupported operating system: $(uname -s)"
      ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64)
      printf 'amd64\n'
      ;;
    arm64|aarch64)
      printf 'arm64\n'
      ;;
    *)
      fail "unsupported architecture: $(uname -m)"
      ;;
  esac
}

download() {
  url=$1
  output=$2

  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "${url}" -o "${output}"
    return 0
  fi

  if command -v wget >/dev/null 2>&1; then
    wget -qO "${output}" "${url}"
    return 0
  fi

  fail "missing downloader: install curl or wget"
}

sha256_file() {
  file_path=$1

  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${file_path}" | awk '{print $1}'
    return 0
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${file_path}" | awk '{print $1}'
    return 0
  fi

  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "${file_path}" | awk '{print $NF}'
    return 0
  fi

  fail "missing checksum tool: install sha256sum, shasum, or openssl"
}

resolve_base_url() {
  if [ -n "${FUGUE_INSTALL_BASE_URL:-}" ]; then
    printf '%s\n' "${FUGUE_INSTALL_BASE_URL}"
    return
  fi

  repo=${FUGUE_INSTALL_REPO:-${DEFAULT_REPO}}
  version=${FUGUE_VERSION:-latest}

  case "${version}" in
    latest)
      printf 'https://github.com/%s/releases/latest/download\n' "${repo}"
      ;;
    v*)
      printf 'https://github.com/%s/releases/download/%s\n' "${repo}" "${version}"
      ;;
    *)
      printf 'https://github.com/%s/releases/download/v%s\n' "${repo}" "${version}"
      ;;
  esac
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

for required_command in tar awk dirname mktemp; do
  if ! command -v "${required_command}" >/dev/null 2>&1; then
    fail "missing required command: ${required_command}"
  fi
done

os_name=$(detect_os)
arch_name=$(detect_arch)
install_dir=$(normalize_dir "$(choose_install_dir)")
base_url=$(resolve_base_url)
asset_name="fugue_${os_name}_${arch_name}.tar.gz"
checksums_name="fugue_checksums.txt"

if [ "${FUGUE_INSTALL_DRY_RUN:-0}" = "1" ]; then
  printf 'os=%s\n' "${os_name}"
  printf 'arch=%s\n' "${arch_name}"
  printf 'install_dir=%s\n' "${install_dir}"
  printf 'asset_url=%s/%s\n' "${base_url}" "${asset_name}"
  exit 0
fi

temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/fugue-install.XXXXXX")
trap 'rm -rf "${temp_dir}"' EXIT INT TERM HUP

archive_path="${temp_dir}/${asset_name}"
checksums_path="${temp_dir}/${checksums_name}"

log "downloading ${asset_name}"
download "${base_url}/${asset_name}" "${archive_path}"

if download "${base_url}/${checksums_name}" "${checksums_path}"; then
  expected_checksum=$(awk -v name="${asset_name}" '$2 == name { print $1; exit }' "${checksums_path}")
  if [ -z "${expected_checksum}" ]; then
    warn "checksum entry for ${asset_name} not found; continuing without verification"
  else
    actual_checksum=$(sha256_file "${archive_path}")
    if [ "${actual_checksum}" != "${expected_checksum}" ]; then
      fail "checksum mismatch for ${asset_name}"
    fi
  fi
else
  warn "could not download ${checksums_name}; continuing without verification"
fi

log "extracting ${asset_name}"
tar -xzf "${archive_path}" -C "${temp_dir}"

if [ ! -f "${temp_dir}/fugue" ]; then
  fail "archive did not contain a fugue binary"
fi

mkdir -p "${install_dir}"

if command -v install >/dev/null 2>&1; then
  install -m 0755 "${temp_dir}/fugue" "${install_dir}/fugue"
else
  cp "${temp_dir}/fugue" "${install_dir}/fugue"
  chmod 0755 "${install_dir}/fugue"
fi

log "installed ${install_dir}/fugue"

if ! is_on_path "${install_dir}"; then
  warn "${install_dir} is not on PATH"
fi

log "run 'fugue --help' to get started"
