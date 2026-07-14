#!/usr/bin/env bash

set -euo pipefail

# Prepare the exact DiG executable used by the authoritative DNS release
# attestation without mutating the persistent self-hosted runner. Supported
# runner profiles are deliberately explicit: a new distribution or
# architecture must add a reviewed package version and repository checksum.

authoritative_dns_dig_prerequisite_log() {
  printf '[prepare-authoritative-dns-dig] %s\n' "$*"
}

authoritative_dns_dig_prerequisite_error() {
  printf '[prepare-authoritative-dns-dig] ERROR: %s\n' "$*" >&2
  return 1
}

authoritative_dns_dig_prerequisite_trim() {
  local value="${1:-}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

authoritative_dns_dig_prerequisite_os_release() {
  local release_file="${FUGUE_AUTHORITATIVE_DNS_DIG_OS_RELEASE_FILE:-/etc/os-release}"
  local key=""
  local value=""

  AUTHORITATIVE_DNS_DIG_RUNNER_OS_ID=""
  AUTHORITATIVE_DNS_DIG_RUNNER_OS_CODENAME=""
  AUTHORITATIVE_DNS_DIG_RUNNER_OS_VERSION_ID=""
  [[ -r "${release_file}" ]] || {
    authoritative_dns_dig_prerequisite_error "runner OS metadata is unreadable: ${release_file}"
    return 1
  }
  while IFS='=' read -r key value; do
    case "${key}" in
      ID|VERSION_CODENAME|VERSION_ID)
        value="$(authoritative_dns_dig_prerequisite_trim "${value}")"
        if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
          value="${value#\"}"
          value="${value%\"}"
        elif [[ "${value}" == \'*\' && "${value}" == *\' ]]; then
          value="${value#\'}"
          value="${value%\'}"
        fi
        case "${key}" in
          ID) AUTHORITATIVE_DNS_DIG_RUNNER_OS_ID="${value}" ;;
          VERSION_CODENAME) AUTHORITATIVE_DNS_DIG_RUNNER_OS_CODENAME="${value}" ;;
          VERSION_ID) AUTHORITATIVE_DNS_DIG_RUNNER_OS_VERSION_ID="${value}" ;;
        esac
        ;;
    esac
  done <"${release_file}"
  if [[ -z "${AUTHORITATIVE_DNS_DIG_RUNNER_OS_CODENAME}" &&
        "${AUTHORITATIVE_DNS_DIG_RUNNER_OS_ID}:${AUTHORITATIVE_DNS_DIG_RUNNER_OS_VERSION_ID}" == "debian:13" ]]; then
    AUTHORITATIVE_DNS_DIG_RUNNER_OS_CODENAME="trixie"
  fi
  [[ -n "${AUTHORITATIVE_DNS_DIG_RUNNER_OS_ID}" &&
      -n "${AUTHORITATIVE_DNS_DIG_RUNNER_OS_CODENAME}" ]] || {
    authoritative_dns_dig_prerequisite_error "runner OS id/codename is incomplete in ${release_file}"
    return 1
  }
}

authoritative_dns_dig_prerequisite_select_profile() {
  local os_id="$1"
  local codename="$2"
  local architecture="$3"

  AUTHORITATIVE_DNS_DIG_PACKAGE_NAME=""
  AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION=""
  AUTHORITATIVE_DNS_DIG_PACKAGE_ARCHITECTURE=""
  AUTHORITATIVE_DNS_DIG_PACKAGE_SHA256=""
  AUTHORITATIVE_DNS_DIG_UPSTREAM_VERSION=""
  case "${os_id}:${codename}:${architecture}" in
    debian:trixie:amd64)
      AUTHORITATIVE_DNS_DIG_PACKAGE_NAME="bind9-dnsutils"
      AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION="1:9.20.23-1~deb13u1"
      AUTHORITATIVE_DNS_DIG_PACKAGE_ARCHITECTURE="amd64"
      AUTHORITATIVE_DNS_DIG_PACKAGE_SHA256="75f3120f4811354e72481033652787867f9e2bbd04e19a4fae885e8ad97ded63"
      AUTHORITATIVE_DNS_DIG_UPSTREAM_VERSION="9.20.23"
      ;;
    *)
      authoritative_dns_dig_prerequisite_error \
        "unsupported runner profile ${os_id}:${codename}:${architecture}; add a reviewed package version and SHA-256"
      return 1
      ;;
  esac
}

authoritative_dns_dig_prerequisite_require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    authoritative_dns_dig_prerequisite_error "required runner command is missing: $1"
    return 1
  }
}

authoritative_dns_dig_prerequisite_metadata_value() {
  local metadata="$1"
  local field="$2"

  awk -F ': ' -v field="${field}" \
    '$1 == field {print substr($0, length(field) + 3); exit}' <<<"${metadata}"
}

AUTHORITATIVE_DNS_DIG_PREREQUISITE_WORK_DIR=""
AUTHORITATIVE_DNS_DIG_PREREQUISITE_KEEP_WORK_DIR="false"

cleanup_authoritative_dns_dig_prerequisite() {
  if [[ "${AUTHORITATIVE_DNS_DIG_PREREQUISITE_KEEP_WORK_DIR}" != "true" &&
        -n "${AUTHORITATIVE_DNS_DIG_PREREQUISITE_WORK_DIR}" &&
        -d "${AUTHORITATIVE_DNS_DIG_PREREQUISITE_WORK_DIR}" ]]; then
    rm -rf -- "${AUTHORITATIVE_DNS_DIG_PREREQUISITE_WORK_DIR}"
  fi
}

prepare_authoritative_dns_dig() {
  local architecture=""
  local apt_metadata=""
  local advertised_architecture=""
  local advertised_package=""
  local advertised_sha256=""
  local advertised_version=""
  local actual_sha256=""
  local binary=""
  local binary_dir=""
  local command_name=""
  local deb_file=""
  local deb_files=()
  local extracted_architecture=""
  local extracted_package=""
  local extracted_version=""
  local github_env_file="${GITHUB_ENV:-}"
  local github_path_file="${GITHUB_PATH:-}"
  local runner_temp="${RUNNER_TEMP:-}"
  local version_output=""
  local work_dir=""

  for command_name in apt-cache apt-get awk chmod dpkg dpkg-deb env find mkdir mktemp rm sha256sum; do
    authoritative_dns_dig_prerequisite_require_command "${command_name}" || return 1
  done
  [[ -n "${runner_temp}" && "${runner_temp}" == /* && -d "${runner_temp}" && -w "${runner_temp}" ]] || {
    authoritative_dns_dig_prerequisite_error "RUNNER_TEMP must be an absolute writable directory"
    return 1
  }
  [[ -n "${github_path_file}" && "${github_path_file}" == /* ]] || {
    authoritative_dns_dig_prerequisite_error "GITHUB_PATH must be an absolute path"
    return 1
  }
  [[ -n "${github_env_file}" && "${github_env_file}" == /* ]] || {
    authoritative_dns_dig_prerequisite_error "GITHUB_ENV must be an absolute path"
    return 1
  }
  [[ ! -L "${github_path_file}" &&
      ( ! -e "${github_path_file}" || ( -f "${github_path_file}" && -w "${github_path_file}" ) ) ]] || {
    authoritative_dns_dig_prerequisite_error "GITHUB_PATH must be a writable regular file, not a symlink"
    return 1
  }
  [[ ! -L "${github_env_file}" &&
      ( ! -e "${github_env_file}" || ( -f "${github_env_file}" && -w "${github_env_file}" ) ) ]] || {
    authoritative_dns_dig_prerequisite_error "GITHUB_ENV must be a writable regular file, not a symlink"
    return 1
  }

  authoritative_dns_dig_prerequisite_os_release || return 1
  architecture="$(dpkg --print-architecture)" || {
    authoritative_dns_dig_prerequisite_error "could not read the runner Debian architecture"
    return 1
  }
  architecture="$(authoritative_dns_dig_prerequisite_trim "${architecture}")"
  authoritative_dns_dig_prerequisite_select_profile \
    "${AUTHORITATIVE_DNS_DIG_RUNNER_OS_ID}" \
    "${AUTHORITATIVE_DNS_DIG_RUNNER_OS_CODENAME}" \
    "${architecture}" || return 1

  apt_metadata="$(apt-cache show "${AUTHORITATIVE_DNS_DIG_PACKAGE_NAME}=${AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION}")" || {
    authoritative_dns_dig_prerequisite_error \
      "apt metadata does not contain pinned ${AUTHORITATIVE_DNS_DIG_PACKAGE_NAME}=${AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION}"
    return 1
  }
  advertised_package="$(authoritative_dns_dig_prerequisite_metadata_value "${apt_metadata}" Package)"
  advertised_version="$(authoritative_dns_dig_prerequisite_metadata_value "${apt_metadata}" Version)"
  advertised_architecture="$(authoritative_dns_dig_prerequisite_metadata_value "${apt_metadata}" Architecture)"
  advertised_sha256="$(authoritative_dns_dig_prerequisite_metadata_value "${apt_metadata}" SHA256)"
  [[ "${advertised_package}" == "${AUTHORITATIVE_DNS_DIG_PACKAGE_NAME}" &&
      "${advertised_version}" == "${AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION}" &&
      "${advertised_architecture}" == "${AUTHORITATIVE_DNS_DIG_PACKAGE_ARCHITECTURE}" &&
      "${advertised_sha256}" == "${AUTHORITATIVE_DNS_DIG_PACKAGE_SHA256}" ]] || {
    authoritative_dns_dig_prerequisite_error \
      "apt metadata for the pinned DiG package is absent or drifted"
    return 1
  }

  work_dir="$(mktemp -d "${runner_temp%/}/fugue-authoritative-dns-dig.XXXXXX")" || return 1
  AUTHORITATIVE_DNS_DIG_PREREQUISITE_WORK_DIR="${work_dir}"
  AUTHORITATIVE_DNS_DIG_PREREQUISITE_KEEP_WORK_DIR="false"
  trap cleanup_authoritative_dns_dig_prerequisite EXIT
  mkdir -p "${work_dir}/download" "${work_dir}/root" "${work_dir}/home"
  chmod 700 "${work_dir}" "${work_dir}/download" "${work_dir}/root" "${work_dir}/home"

  authoritative_dns_dig_prerequisite_log \
    "downloading ${AUTHORITATIVE_DNS_DIG_PACKAGE_NAME}=${AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION} for ${AUTHORITATIVE_DNS_DIG_PACKAGE_ARCHITECTURE}"
  if ! (
    cd "${work_dir}/download"
    DEBIAN_FRONTEND=noninteractive apt-get -o Debug::NoLocking=true download \
      "${AUTHORITATIVE_DNS_DIG_PACKAGE_NAME}=${AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION}"
  ); then
    authoritative_dns_dig_prerequisite_error "failed to download the pinned DiG package"
    return 1
  fi
  while IFS= read -r deb_file; do
    deb_files+=("${deb_file}")
  done < <(find "${work_dir}/download" -maxdepth 1 -type f -name '*.deb' -print)
  [[ "${#deb_files[@]}" == "1" ]] || {
    authoritative_dns_dig_prerequisite_error \
      "pinned DiG download produced ${#deb_files[@]} package files; expected exactly one"
    return 1
  }
  deb_file="${deb_files[0]}"
  actual_sha256="$(sha256sum "${deb_file}" | awk '{print $1}')" || return 1
  [[ "${actual_sha256}" == "${AUTHORITATIVE_DNS_DIG_PACKAGE_SHA256}" ]] || {
    authoritative_dns_dig_prerequisite_error \
      "downloaded DiG package SHA-256 mismatch: got ${actual_sha256}"
    return 1
  }

  extracted_package="$(dpkg-deb --field "${deb_file}" Package)" || return 1
  extracted_version="$(dpkg-deb --field "${deb_file}" Version)" || return 1
  extracted_architecture="$(dpkg-deb --field "${deb_file}" Architecture)" || return 1
  extracted_package="$(authoritative_dns_dig_prerequisite_trim "${extracted_package}")"
  extracted_version="$(authoritative_dns_dig_prerequisite_trim "${extracted_version}")"
  extracted_architecture="$(authoritative_dns_dig_prerequisite_trim "${extracted_architecture}")"
  [[ "${extracted_package}" == "${AUTHORITATIVE_DNS_DIG_PACKAGE_NAME}" &&
      "${extracted_version}" == "${AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION}" &&
      "${extracted_architecture}" == "${AUTHORITATIVE_DNS_DIG_PACKAGE_ARCHITECTURE}" ]] || {
    authoritative_dns_dig_prerequisite_error \
      "downloaded DiG package identity does not match its pinned name/version/architecture"
    return 1
  }
  if ! dpkg-deb --extract "${deb_file}" "${work_dir}/root"; then
    authoritative_dns_dig_prerequisite_error "failed to extract the pinned DiG package"
    return 1
  fi

  binary="${work_dir}/root/usr/bin/dig"
  binary_dir="${work_dir}/root/usr/bin"
  [[ -f "${binary}" && ! -L "${binary}" && -x "${binary}" ]] || {
    authoritative_dns_dig_prerequisite_error "the pinned package did not produce an executable regular usr/bin/dig"
    return 1
  }
  version_output="$(env -i HOME="${work_dir}/home" LANG=C LC_ALL=C \
    PATH=/usr/bin:/bin:/usr/sbin:/sbin "${binary}" -v 2>&1)" || {
    authoritative_dns_dig_prerequisite_error "the extracted DiG executable could not run with the controlled release environment"
    return 1
  }
  [[ -n "${version_output}" && "${version_output}" != *$'\n'* &&
      "${version_output}" == "DiG ${AUTHORITATIVE_DNS_DIG_UPSTREAM_VERSION}"* ]] || {
    authoritative_dns_dig_prerequisite_error \
      "the extracted DiG version output is unexpected: ${version_output:-<empty>}"
    return 1
  }

  printf 'FUGUE_AUTHORITATIVE_DNS_DIG_PREPARED_BIN=%s\n' "${binary}" >>"${github_env_file}"
  printf '%s\n' "${binary_dir}" >>"${github_path_file}"
  AUTHORITATIVE_DNS_DIG_PREREQUISITE_KEEP_WORK_DIR="true"
  authoritative_dns_dig_prerequisite_log \
    "prepared ${version_output} at ${binary}; package_sha256=${actual_sha256}"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  prepare_authoritative_dns_dig
fi
