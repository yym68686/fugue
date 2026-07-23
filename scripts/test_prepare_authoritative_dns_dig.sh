#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/prepare_authoritative_dns_dig.sh
source "${REPO_ROOT}/scripts/prepare_authoritative_dns_dig.sh"

fail() {
  printf '[test_prepare_authoritative_dns_dig] ERROR: %s\n' "$*" >&2
  exit 1
}

assert_eq() {
  local got="$1"
  local want="$2"
  local label="$3"
  [[ "${got}" == "${want}" ]] || fail "${label}: got ${got}, want ${want}"
}

TEST_ROOT="$(mktemp -d)"
trap 'rm -rf "${TEST_ROOT}"' EXIT
FIXTURE_DIG_DEB_SOURCE="${TEST_ROOT}/bind9-dnsutils-fixture.deb"
FIXTURE_RUNTIME_DEB_SOURCE="${TEST_ROOT}/bind9-libs-fixture.deb"
printf 'synthetic bind9-dnsutils package fixture\n' >"${FIXTURE_DIG_DEB_SOURCE}"
printf 'synthetic bind9-libs package fixture\n' >"${FIXTURE_RUNTIME_DEB_SOURCE}"
FIXTURE_DIG_SHA256="$(sha256sum "${FIXTURE_DIG_DEB_SOURCE}" | awk '{print $1}')"
FIXTURE_RUNTIME_SHA256="$(sha256sum "${FIXTURE_RUNTIME_DEB_SOURCE}" | awk '{print $1}')"
FAKE_BIN="${TEST_ROOT}/bin"
mkdir -p "${FAKE_BIN}"

cat >"${FAKE_BIN}/dpkg" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == "--print-architecture" ]]
printf '%s\n' "${TEST_DPKG_ARCHITECTURE:-amd64}"
SH

cat >"${FAKE_BIN}/apt-cache" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == "show" ]]
[[ "${TEST_APT_CACHE_FAIL:-false}" != "true" ]] || exit 71
requested_package="${2%%=*}"
case "${requested_package}" in
  "${TEST_PACKAGE_NAME}")
    printf 'Package: %s\n' "${TEST_APT_PACKAGE_NAME:-${TEST_PACKAGE_NAME}}"
    printf 'Version: %s\n' "${TEST_APT_VERSION:-${TEST_PACKAGE_VERSION}}"
    printf 'Architecture: %s\n' "${TEST_APT_ARCHITECTURE:-${TEST_PACKAGE_ARCHITECTURE}}"
    printf 'SHA256: %s\n' "${TEST_APT_SHA256:-${TEST_PACKAGE_SHA256}}"
    ;;
  "${TEST_RUNTIME_PACKAGE_NAME}")
    printf 'Package: %s\n' "${TEST_RUNTIME_APT_PACKAGE_NAME:-${TEST_RUNTIME_PACKAGE_NAME}}"
    printf 'Version: %s\n' "${TEST_RUNTIME_APT_VERSION:-${TEST_RUNTIME_PACKAGE_VERSION}}"
    printf 'Architecture: %s\n' "${TEST_RUNTIME_APT_ARCHITECTURE:-${TEST_RUNTIME_PACKAGE_ARCHITECTURE}}"
    printf 'SHA256: %s\n' "${TEST_RUNTIME_APT_SHA256:-${TEST_RUNTIME_PACKAGE_SHA256}}"
    ;;
  *) exit 79 ;;
esac
SH

cat >"${FAKE_BIN}/apt-get" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${TEST_COMMAND_LOG}"
[[ "${TEST_DOWNLOAD_FAIL:-false}" != "true" ]] || exit 72
cp "${TEST_FIXTURE_DIG_DEB_SOURCE}" ./bind9-dnsutils_fixture_amd64.deb
if [[ "${TEST_DOWNLOAD_OMIT_RUNTIME:-false}" != "true" ]]; then
  cp "${TEST_FIXTURE_RUNTIME_DEB_SOURCE}" ./bind9-libs_fixture_amd64.deb
fi
SH

cat >"${FAKE_BIN}/dpkg-deb" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  --field)
    if [[ "$(basename "$2")" == bind9-libs_* ]]; then
      case "$3" in
        Package) printf '%s\n' "${TEST_RUNTIME_EXTRACTED_PACKAGE:-${TEST_RUNTIME_PACKAGE_NAME}}" ;;
        Version) printf '%s\n' "${TEST_RUNTIME_EXTRACTED_VERSION:-${TEST_RUNTIME_PACKAGE_VERSION}}" ;;
        Architecture) printf '%s\n' "${TEST_RUNTIME_EXTRACTED_ARCHITECTURE:-${TEST_RUNTIME_PACKAGE_ARCHITECTURE}}" ;;
        *) exit 73 ;;
      esac
    else
      case "$3" in
        Package) printf '%s\n' "${TEST_EXTRACTED_PACKAGE:-${TEST_PACKAGE_NAME}}" ;;
        Version) printf '%s\n' "${TEST_EXTRACTED_VERSION:-${TEST_PACKAGE_VERSION}}" ;;
        Architecture) printf '%s\n' "${TEST_EXTRACTED_ARCHITECTURE:-${TEST_PACKAGE_ARCHITECTURE}}" ;;
        *) exit 73 ;;
      esac
    fi
    ;;
  --extract)
    [[ "${TEST_EXTRACT_FAIL:-false}" != "true" ]] || exit 74
    if [[ "$(basename "$2")" == bind9-libs_* ]]; then
      [[ "${TEST_RUNTIME_EXTRACT_FAIL:-false}" != "true" ]] || exit 80
      mkdir -p "$3/usr/lib/x86_64-linux-gnu"
      printf 'synthetic libisc\n' >"$3/usr/lib/x86_64-linux-gnu/libisc-fixture.so"
    else
      mkdir -p "$3/usr/bin"
      {
        printf '#!/usr/bin/env bash\n'
        printf 'set -euo pipefail\n'
        printf '[[ -n "${LD_LIBRARY_PATH:-}" && -f "${LD_LIBRARY_PATH}/libisc-fixture.so" ]] || exit 78\n'
        if [[ "${TEST_DIG_EXEC_FAIL:-false}" == "true" ]]; then
          printf 'exit 75\n'
        else
          printf 'if [[ "$1" == "-v" ]]; then\n'
          printf '  printf "%%s\\n" %q\n' "${TEST_DIG_VERSION_OUTPUT:-DiG 9.99.1}"
          printf '  exit 0\n'
          printf 'fi\n'
          printf 'exit 76\n'
        fi
      } >"$3/usr/bin/dig"
      chmod +x "$3/usr/bin/dig"
    fi
    ;;
  *) exit 77 ;;
esac
SH
chmod +x "${FAKE_BIN}/apt-cache" "${FAKE_BIN}/apt-get" "${FAKE_BIN}/dpkg" "${FAKE_BIN}/dpkg-deb"

write_os_release() {
  local directory="$1"
  local id="${2:-debian}"
  local codename="${3:-trixie}"

  cat >"${directory}/os-release" <<EOF
ID=${id}
VERSION_ID="13"
VERSION_CODENAME=${codename}
EOF
}

run_fixture_prepare() (
  set -euo pipefail
  local case_dir="$1"

  export PATH="${FAKE_BIN}:/usr/bin:/bin:/usr/sbin:/sbin"
  export RUNNER_TEMP="${case_dir}/runner-temp"
  export GITHUB_PATH="${case_dir}/github-path"
  export GITHUB_ENV="${case_dir}/github-env"
  export FUGUE_AUTHORITATIVE_DNS_DIG_OS_RELEASE_FILE="${case_dir}/os-release"
  export TEST_COMMAND_LOG="${case_dir}/commands.log"
  export TEST_FIXTURE_DIG_DEB_SOURCE="${FIXTURE_DIG_DEB_SOURCE}"
  export TEST_FIXTURE_RUNTIME_DEB_SOURCE="${FIXTURE_RUNTIME_DEB_SOURCE}"
  export TEST_PACKAGE_NAME="bind9-dnsutils"
  export TEST_PACKAGE_VERSION="1:9.99.1-1~test1"
  export TEST_PACKAGE_ARCHITECTURE="amd64"
  export TEST_PACKAGE_SHA256="${TEST_PROFILE_SHA256:-${FIXTURE_DIG_SHA256}}"
  export TEST_RUNTIME_PACKAGE_NAME="bind9-libs"
  export TEST_RUNTIME_PACKAGE_VERSION="1:9.99.1-1~test1"
  export TEST_RUNTIME_PACKAGE_ARCHITECTURE="amd64"
  export TEST_RUNTIME_PACKAGE_SHA256="${TEST_RUNTIME_PROFILE_SHA256:-${FIXTURE_RUNTIME_SHA256}}"
  mkdir -p "${RUNNER_TEMP}"
  : >"${GITHUB_PATH}"
  : >"${GITHUB_ENV}"
  : >"${TEST_COMMAND_LOG}"

  authoritative_dns_dig_prerequisite_select_profile() {
    [[ "$1:$2:$3" == "debian:trixie:amd64" ]] || return 1
    AUTHORITATIVE_DNS_DIG_PACKAGE_NAME="${TEST_PACKAGE_NAME}"
    AUTHORITATIVE_DNS_DIG_PACKAGE_VERSION="${TEST_PACKAGE_VERSION}"
    AUTHORITATIVE_DNS_DIG_PACKAGE_ARCHITECTURE="${TEST_PACKAGE_ARCHITECTURE}"
    AUTHORITATIVE_DNS_DIG_PACKAGE_SHA256="${TEST_PACKAGE_SHA256}"
    AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_NAME="${TEST_RUNTIME_PACKAGE_NAME}"
    AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_VERSION="${TEST_RUNTIME_PACKAGE_VERSION}"
    AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_ARCHITECTURE="${TEST_RUNTIME_PACKAGE_ARCHITECTURE}"
    AUTHORITATIVE_DNS_DIG_RUNTIME_PACKAGE_SHA256="${TEST_RUNTIME_PACKAGE_SHA256}"
    AUTHORITATIVE_DNS_DIG_RUNTIME_LIBRARY_DIR="usr/lib/x86_64-linux-gnu"
    AUTHORITATIVE_DNS_DIG_UPSTREAM_VERSION="9.99.1"
  }
  prepare_authoritative_dns_dig
)

run_failure_case() {
  local label="$1"
  local variable="$2"
  local value="$3"
  local case_dir="${TEST_ROOT}/${label}"

  mkdir -p "${case_dir}/runner-temp"
  write_os_release "${case_dir}"
  if (
    export "${variable}=${value}"
    run_fixture_prepare "${case_dir}"
  ) >/dev/null 2>&1; then
    fail "${label} must fail closed"
  fi
  [[ ! -s "${case_dir}/github-path" ]] || fail "${label} modified GITHUB_PATH after failure"
  [[ ! -s "${case_dir}/github-env" ]] || fail "${label} modified GITHUB_ENV after failure"
  if find "${case_dir}/runner-temp" -mindepth 1 -print -quit | grep -q .; then
    fail "${label} retained a partial runtime after failure"
  fi
}

# The production selector must remain explicit rather than silently using a
# package observed on an unsupported runner.
if authoritative_dns_dig_prerequisite_select_profile ubuntu noble amd64 >/dev/null 2>&1; then
  fail "unsupported OS profile must fail closed"
fi
if authoritative_dns_dig_prerequisite_select_profile debian trixie arm64 >/dev/null 2>&1; then
  fail "unsupported architecture must fail closed"
fi

success_dir="${TEST_ROOT}/success"
mkdir -p "${success_dir}/runner-temp"
write_os_release "${success_dir}"
run_fixture_prepare "${success_dir}"
prepared_bin_dir="$(cat "${success_dir}/github-path")"
prepared_bin="$(sed -n 's/^FUGUE_AUTHORITATIVE_DNS_DIG_PREPARED_BIN=//p' "${success_dir}/github-env")"
[[ "${prepared_bin_dir}" == "${success_dir}/runner-temp/"*"/bin" ]] ||
  fail "success case did not publish its isolated bin directory"
assert_eq "${prepared_bin}" "${prepared_bin_dir}/dig" "prepared DiG environment identity"
assert_eq "$("${prepared_bin_dir}/dig" -v)" "DiG 9.99.1" "prepared DiG version"
grep -Fq -- 'download bind9-dnsutils=1:9.99.1-1~test1 bind9-libs=1:9.99.1-1~test1' "${success_dir}/commands.log" ||
  fail "success case did not download the exact package closure"
prepared_work_dir="${prepared_bin_dir%/bin}"
raw_dig="${prepared_work_dir}/root/usr/bin/dig"
runtime_library="${prepared_work_dir}/root/usr/lib/x86_64-linux-gnu/libisc-fixture.so"
cp "${raw_dig}" "${success_dir}/raw-dig.backup"
printf 'tampered\n' >>"${raw_dig}"
if "${prepared_bin}" -v >/dev/null 2>&1; then
  fail "prepared DiG launcher must reject a modified raw executable"
fi
cp "${success_dir}/raw-dig.backup" "${raw_dig}"
chmod +x "${raw_dig}"
printf 'tampered\n' >>"${runtime_library}"
if "${prepared_bin}" -v >/dev/null 2>&1; then
  fail "prepared DiG launcher must reject a modified runtime library"
fi

run_failure_case apt_cache_failure TEST_APT_CACHE_FAIL true
run_failure_case download_failure TEST_DOWNLOAD_FAIL true
run_failure_case runner_architecture_mismatch TEST_DPKG_ARCHITECTURE arm64
run_failure_case hash_mismatch TEST_PROFILE_SHA256 "$(printf '0%.0s' {1..64})"
run_failure_case runtime_hash_mismatch TEST_RUNTIME_PROFILE_SHA256 "$(printf '0%.0s' {1..64})"
run_failure_case missing_runtime_package TEST_DOWNLOAD_OMIT_RUNTIME true
run_failure_case advertised_version_mismatch TEST_APT_VERSION 1:9.99.2-1~test1
run_failure_case advertised_architecture_mismatch TEST_APT_ARCHITECTURE arm64
run_failure_case advertised_runtime_version_mismatch TEST_RUNTIME_APT_VERSION 1:9.99.2-1~test1
run_failure_case advertised_runtime_architecture_mismatch TEST_RUNTIME_APT_ARCHITECTURE arm64
run_failure_case extracted_version_mismatch TEST_EXTRACTED_VERSION 1:9.99.2-1~test1
run_failure_case extracted_architecture_mismatch TEST_EXTRACTED_ARCHITECTURE arm64
run_failure_case extracted_runtime_version_mismatch TEST_RUNTIME_EXTRACTED_VERSION 1:9.99.2-1~test1
run_failure_case extracted_runtime_architecture_mismatch TEST_RUNTIME_EXTRACTED_ARCHITECTURE arm64
run_failure_case extraction_failure TEST_EXTRACT_FAIL true
run_failure_case runtime_extraction_failure TEST_RUNTIME_EXTRACT_FAIL true
run_failure_case dig_execution_failure TEST_DIG_EXEC_FAIL true
run_failure_case dig_version_mismatch TEST_DIG_VERSION_OUTPUT 'DiG 9.98.0'

python3 - \
  "${REPO_ROOT}/.github/workflows/deploy-control-plane.yml" \
  "${REPO_ROOT}/.github/workflows/release-public-data-plane.yml" \
  "${REPO_ROOT}/.github/actions/operational-domain-guarded-deploy/action.yml" <<'PY'
from pathlib import Path
import sys

deploy_path, public_path, action_path = map(Path, sys.argv[1:])
deploy = deploy_path.read_text(encoding="utf-8")
public = public_path.read_text(encoding="utf-8")
action = action_path.read_text(encoding="utf-8")
prepare = "run: ./scripts/prepare_authoritative_dns_dig.sh"
guarded_deploy = "uses: ./.github/actions/operational-domain-guarded-deploy"
upgrade = "run: ./scripts/upgrade_fugue_control_plane.sh"

if deploy.count(prepare) != 1:
    raise SystemExit("control-plane deploy workflow must invoke the shared DiG prerequisite exactly once")
if public.count(prepare) != 1:
    raise SystemExit("public data-plane workflow must invoke the shared DiG prerequisite exactly once")
if deploy.count(guarded_deploy) != 1:
    raise SystemExit("control-plane deploy workflow must invoke the guarded deploy action exactly once")
if deploy.index(prepare) > deploy.index(guarded_deploy):
    raise SystemExit("control-plane DiG prerequisite must run before the release mutation entrypoint")
if public.index(prepare) > public.index("run: ./scripts/release_fugue_public_data_plane.sh"):
    raise SystemExit("public data-plane DiG prerequisite must run before the release mutation entrypoint")
if action.count(upgrade) != 2:
    raise SystemExit("guarded deploy action must invoke exactly one prepare and one apply upgrade phase")
prepare_phase = action.index("FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE: prepare")
prepare_run = action.index(upgrade, prepare_phase)
upload = action.index("uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a", prepare_run)
apply_phase = action.index("FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE: apply", upload)
apply_run = action.index(upgrade, apply_phase)
if not prepare_phase < prepare_run < upload < apply_phase < apply_run:
    raise SystemExit("guarded deploy action must order prepare, pinned upload, and apply")
for label, workflow in (("control-plane", deploy), ("public data-plane", public)):
    if "apt-get download" in workflow or "dpkg-deb --extract" in workflow:
        raise SystemExit(f"{label} workflow duplicated DiG provisioning logic instead of using the shared script")
    prepare_index = workflow.index(prepare)
    verify_index = workflow.index("command -v dig", prepare_index)
    if verify_index < prepare_index:
        raise SystemExit(f"{label} workflow did not verify DiG after preparing it")
    if '"$(command -v dig)" == "${FUGUE_AUTHORITATIVE_DNS_DIG_PREPARED_BIN}"' not in workflow:
        raise SystemExit(f"{label} workflow does not bind PATH resolution to the prepared DiG binary")
PY

printf '[test_prepare_authoritative_dns_dig] ok\n'
