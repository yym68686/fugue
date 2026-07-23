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
  AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_NAME=""
  AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_VERSION=""
  AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_ARCHITECTURE=""
  AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_SHA256=""
  AUTHORITATIVE_DNS_DIG_RUNTIME_LIBRARY_DIR=""
  AUTHORITATIVE_DNS_DIG_UPSTREAM_VERSION=""
  case "${os_id}:${codename}:${architecture}" in
    debian:trixie:amd64)
      AUTHORITATIVE_DNS_DIG_PACKAGE_NAME="bind9-dnsutils"
      AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION="1:9.20.23-1~deb13u1"
      AUTHORITATIVE_DNS_DIG_PACKAGE_ARCHITECTURE="amd64"
      AUTHORITATIVE_DNS_DIG_PACKAGE_SHA256="75f3120f4811354e72481033652787867f9e2bbd04e19a4fae885e8ad97ded63"
      AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_NAME="bind9-libs"
      AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_VERSION="1:9.20.23-1~deb13u1"
      AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_ARCHITECTURE="amd64"
      AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_SHA256="fe98c3e3591da8593cd2c8ab585ad9100b35e780a4b768f1b8180b547a1b4035"
      AUTHORITATIVE_DNS_DIG_RUNTIME_LIBRARY_DIR="usr/lib/x86_64-linux-gnu"
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

authoritative_dns_dig_prerequisite_validate_apt_metadata() {
  local package_name="$1"
  local package_version="$2"
  local package_architecture="$3"
  local package_sha256="$4"
  local package_role="$5"
  local apt_metadata=""
  local advertised_architecture=""
  local advertised_package=""
  local advertised_sha256=""
  local advertised_version=""

  apt_metadata="$(apt-cache show "${package_name}=${package_version}")" || {
    authoritative_dns_dig_prerequisite_error \
      "apt metadata does not contain pinned ${package_name}=${package_version}"
    return 1
  }
  advertised_package="$(authoritative_dns_dig_prerequisite_metadata_value "${apt_metadata}" Package)"
  advertised_version="$(authoritative_dns_dig_prerequisite_metadata_value "${apt_metadata}" Version)"
  advertised_architecture="$(authoritative_dns_dig_prerequisite_metadata_value "${apt_metadata}" Architecture)"
  advertised_sha256="$(authoritative_dns_dig_prerequisite_metadata_value "${apt_metadata}" SHA256)"
  [[ "${advertised_package}" == "${package_name}" &&
      "${advertised_version}" == "${package_version}" &&
      "${advertised_architecture}" == "${package_architecture}" &&
      "${advertised_sha256}" == "${package_sha256}" ]] || {
    authoritative_dns_dig_prerequisite_error \
      "apt metadata for the pinned ${package_role} package is absent or drifted"
    return 1
  }
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
  local actual_sha256=""
  local binary=""
  local command_name=""
  local deb_file=""
  local deb_files=()
  local dig_package_seen="false"
  local expected_sha256=""
  local extracted_architecture=""
  local extracted_package=""
  local extracted_version=""
  local github_env_file="${GITHUB_ENV:-}"
  local github_path_file="${GITHUB_PATH:-}"
  local launcher=""
  local launcher_dir=""
  local library_dir=""
  local payload_manifest=""
  local payload_manifest_sha256=""
  local runner_temp="${RUNNER_TEMP:-}"
  local runtime_package_seen="false"
  local version_output=""
  local work_dir=""

  for command_name in apt-cache apt-get awk chmod dpkg dpkg-deb env find mkdir mktemp rm sha256sum sort; do
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

  authoritative_dns_dig_prerequisite_validate_apt_metadata \
    "${AUTHORITATIVE_DNS_DIG_PACKAGE_NAME}" \
    "${AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION}" \
    "${AUTHORITATIVE_DNS_DIG_PACKAGE_ARCHITECTURE}" \
    "${AUTHORITATIVE_DNS_DIG_PACKAGE_SHA256}" \
    "DiG" || return 1
  authoritative_dns_dig_prerequisite_validate_apt_metadata \
    "${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_NAME}" \
    "${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_VERSION}" \
    "${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_ARCHITECTURE}" \
    "${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_SHA256}" \
    "DiG runtime" || return 1

  work_dir="$(mktemp -d "${runner_temp%/}/fugue-authoritative-dns-dig.XXXXXX")" || return 1
  AUTHORITATIVE_DNS_DIG_PREREQUISITE_WORK_DIR="${work_dir}"
  AUTHORITATIVE_DNS_DIG_PREREQUISITE_KEEP_WORK_DIR="false"
  trap cleanup_authoritative_dns_dig_prerequisite EXIT
  mkdir -p "${work_dir}/download" "${work_dir}/root" "${work_dir}/home" "${work_dir}/bin"
  chmod 700 "${work_dir}" "${work_dir}/download" "${work_dir}/root" "${work_dir}/home" "${work_dir}/bin"

  authoritative_dns_dig_prerequisite_log \
    "downloading ${AUTHORITATIVE_DNS_DIG_PACKAGE_NAME}=${AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION} and ${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_NAME}=${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_VERSION} for ${AUTHORITATIVE_DNS_DIG_PACKAGE_ARCHITECTURE}"
  if ! (
    cd "${work_dir}/download"
    DEBIAN_FRONTEND=noninteractive apt-get -o Debug::NoLocking=true download \
      "${AUTHORITATIVE_DNS_DIG_PACKAGE_NAME}=${AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION}" \
      "${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_NAME}=${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_VERSION}"
  ); then
    authoritative_dns_dig_prerequisite_error "failed to download the pinned DiG package closure"
    return 1
  fi
  while IFS= read -r deb_file; do
    deb_files+=("${deb_file}")
  done < <(find "${work_dir}/download" -maxdepth 1 -type f -name '*.deb' -print)
  [[ "${#deb_files[@]}" == "2" ]] || {
    authoritative_dns_dig_prerequisite_error \
      "pinned DiG download produced ${#deb_files[@]} package files; expected exactly two"
    return 1
  }
  for deb_file in "${deb_files[@]}"; do
    extracted_package="$(dpkg-deb --field "${deb_file}" Package)" || return 1
    extracted_version="$(dpkg-deb --field "${deb_file}" Version)" || return 1
    extracted_architecture="$(dpkg-deb --field "${deb_file}" Architecture)" || return 1
    extracted_package="$(authoritative_dns_dig_prerequisite_trim "${extracted_package}")"
    extracted_version="$(authoritative_dns_dig_prerequisite_trim "${extracted_version}")"
    extracted_architecture="$(authoritative_dns_dig_prerequisite_trim "${extracted_architecture}")"
    case "${extracted_package}" in
      "${AUTHORITATIVE_DNS_DIG_PACKAGE_NAME}")
        [[ "${dig_package_seen}" == "false" ]] || {
          authoritative_dns_dig_prerequisite_error "downloaded DiG package closure contains a duplicate DiG package"
          return 1
        }
        dig_package_seen="true"
        expected_sha256="${AUTHORITATIVE_DNS_DIG_PACKAGE_SHA256}"
        [[ "${extracted_version}" == "${AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION}" &&
            "${extracted_architecture}" == "${AUTHORITATIVE_DNS_DIG_PACKAGE_ARCHITECTURE}" ]] || {
          authoritative_dns_dig_prerequisite_error \
            "downloaded DiG package identity does not match its pinned name/version/architecture"
          return 1
        }
        ;;
      "${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_NAME}")
        [[ "${runtime_package_seen}" == "false" ]] || {
          authoritative_dns_dig_prerequisite_error "downloaded DiG package closure contains a duplicate runtime package"
          return 1
        }
        runtime_package_seen="true"
        expected_sha256="${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_SHA256}"
        [[ "${extracted_version}" == "${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_VERSION}" &&
            "${extracted_architecture}" == "${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_ARCHITECTURE}" ]] || {
          authoritative_dns_dig_prerequisite_error \
            "downloaded DiG runtime package identity does not match its pinned name/version/architecture"
          return 1
        }
        ;;
      *)
        authoritative_dns_dig_prerequisite_error \
          "downloaded DiG package closure contains unexpected package ${extracted_package:-<empty>}"
        return 1
        ;;
    esac
    actual_sha256="$(sha256sum "${deb_file}" | awk '{print $1}')" || return 1
    [[ "${actual_sha256}" == "${expected_sha256}" ]] || {
      authoritative_dns_dig_prerequisite_error \
        "downloaded ${extracted_package} package SHA-256 mismatch: got ${actual_sha256}"
      return 1
    }
    if ! dpkg-deb --extract "${deb_file}" "${work_dir}/root"; then
      authoritative_dns_dig_prerequisite_error "failed to extract the pinned ${extracted_package} package"
      return 1
    fi
  done
  [[ "${dig_package_seen}" == "true" && "${runtime_package_seen}" == "true" ]] || {
    authoritative_dns_dig_prerequisite_error "downloaded DiG package closure is incomplete"
    return 1
  }

  binary="${work_dir}/root/usr/bin/dig"
  library_dir="${work_dir}/root/${AUTHORITATIVE_DNS_DIG_RUNTIME_LIBRARY_DIR}"
  launcher="${work_dir}/bin/dig"
  launcher_dir="${work_dir}/bin"
  payload_manifest="${work_dir}/payload.sha256"
  [[ -f "${binary}" && ! -L "${binary}" && -x "${binary}" ]] || {
    authoritative_dns_dig_prerequisite_error "the pinned package did not produce an executable regular usr/bin/dig"
    return 1
  }
  [[ -d "${library_dir}" && ! -L "${library_dir}" ]] || {
    authoritative_dns_dig_prerequisite_error "the pinned runtime package did not produce the expected library directory"
    return 1
  }
  [[ -n "$(find "${library_dir}" -type f -print -quit)" &&
      -z "$(find "${library_dir}" -type l -print -quit)" ]] || {
    authoritative_dns_dig_prerequisite_error \
      "the pinned runtime package library payload is empty or contains symlinks"
    return 1
  }
  {
    sha256sum "${binary}"
    find "${library_dir}" -type f -exec sha256sum {} +
  } | LC_ALL=C sort >"${payload_manifest}"
  [[ -s "${payload_manifest}" && ! -L "${payload_manifest}" ]] || {
    authoritative_dns_dig_prerequisite_error "could not create the isolated DiG payload manifest"
    return 1
  }
  payload_manifest_sha256="$(sha256sum "${payload_manifest}" | awk '{print $1}')" || return 1
  {
    printf '#!/usr/bin/env bash\n'
    printf 'set -euo pipefail\n'
    printf 'readonly fugue_dig_binary=%q\n' "${binary}"
    printf 'readonly fugue_dig_library_dir=%q\n' "${library_dir}"
    printf 'readonly fugue_dig_payload_manifest=%q\n' "${payload_manifest}"
    printf 'readonly fugue_dig_payload_manifest_sha256=%q\n' "${payload_manifest_sha256}"
    printf '[[ -f "${fugue_dig_binary}" && ! -L "${fugue_dig_binary}" && -x "${fugue_dig_binary}" ]] || exit 126\n'
    printf '[[ -d "${fugue_dig_library_dir}" && ! -L "${fugue_dig_library_dir}" ]] || exit 126\n'
    printf '[[ -f "${fugue_dig_payload_manifest}" && ! -L "${fugue_dig_payload_manifest}" ]] || exit 126\n'
    printf 'fugue_dig_manifest_identity="$(sha256sum "${fugue_dig_payload_manifest}")" || exit 126\n'
    printf '[[ "${fugue_dig_manifest_identity%%%% *}" == "${fugue_dig_payload_manifest_sha256}" ]] || exit 126\n'
    printf 'sha256sum --check --strict --status "${fugue_dig_payload_manifest}" || exit 126\n'
    printf 'export LD_LIBRARY_PATH="${fugue_dig_library_dir}"\n'
    printf 'exec "${fugue_dig_binary}" "$@"\n'
  } >"${launcher}"
  chmod 700 "${launcher}"
  [[ -f "${launcher}" && ! -L "${launcher}" && -x "${launcher}" ]] || {
    authoritative_dns_dig_prerequisite_error "could not create the isolated DiG runtime launcher"
    return 1
  }
  version_output="$(env -i HOME="${work_dir}/home" LANG=C LC_ALL=C \
    PATH=/usr/bin:/bin:/usr/sbin:/sbin "${launcher}" -v 2>&1)" || {
    authoritative_dns_dig_prerequisite_error "the isolated DiG runtime could not run with the controlled release environment"
    return 1
  }
  [[ -n "${version_output}" && "${version_output}" != *$'\n'* &&
      "${version_output}" == "DiG ${AUTHORITATIVE_DNS_DIG_UPSTREAM_VERSION}"* ]] || {
    authoritative_dns_dig_prerequisite_error \
      "the extracted DiG version output is unexpected: ${version_output:-<empty>}"
    return 1
  }

  printf 'FUGUE_AUTHORITATIVE_DNS_DIG_PREPARED_BIN=%s\n' "${launcher}" >>"${github_env_file}"
  printf '%s\n' "${launcher_dir}" >>"${github_path_file}"
  AUTHORITATIVE_DNS_DIG_PREREQUISITE_KEEP_WORK_DIR="true"
  authoritative_dns_dig_prerequisite_log \
    "prepared ${version_output} at ${launcher}; package_sha256=${AUTHORITATIVE_DNS_DIG_PACKAGE_SHA256}; runtime_package_sha256=${AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_SHA256}"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  prepare_authoritative_dns_dig
fi
