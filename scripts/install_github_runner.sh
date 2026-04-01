#!/usr/bin/env bash

set -euo pipefail

log() {
  printf '[github-runner-install] %s\n' "$*"
}

fail() {
  printf '[github-runner-install] ERROR: %s\n' "$*" >&2
  exit 1
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    fail "missing required env ${name}"
  fi
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

need_root() {
  if [[ "$(id -u)" -ne 0 ]]; then
    fail "run this script as root"
  fi
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64)
      printf 'x64'
      ;;
    aarch64|arm64)
      printf 'arm64'
      ;;
    *)
      fail "unsupported architecture: $(uname -m)"
      ;;
  esac
}

install_base_packages() {
  if command_exists apt-get; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y ca-certificates curl git jq tar
    return
  fi
  fail "unsupported package manager; only apt-get hosts are supported for now"
}

install_helm() {
  if command_exists helm; then
    return
  fi
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
}

ensure_runner_user() {
  if id -u "${GITHUB_RUNNER_USER}" >/dev/null 2>&1; then
    return
  fi
  useradd --system --create-home --home-dir "/home/${GITHUB_RUNNER_USER}" --shell /bin/bash "${GITHUB_RUNNER_USER}"
}

download_runner() {
  local arch runner_tgz url
  arch="$(detect_arch)"
  runner_tgz="/tmp/actions-runner-linux-${arch}-${GITHUB_RUNNER_VERSION}.tar.gz"
  url="https://github.com/actions/runner/releases/download/v${GITHUB_RUNNER_VERSION}/actions-runner-linux-${arch}-${GITHUB_RUNNER_VERSION}.tar.gz"

  mkdir -p "${GITHUB_RUNNER_INSTALL_DIR}"
  if [[ ! -f "${runner_tgz}" ]]; then
    curl -fsSL -o "${runner_tgz}" "${url}"
  fi
  tar -xzf "${runner_tgz}" -C "${GITHUB_RUNNER_INSTALL_DIR}"
  if [[ -x "${GITHUB_RUNNER_INSTALL_DIR}/bin/installdependencies.sh" ]]; then
    "${GITHUB_RUNNER_INSTALL_DIR}/bin/installdependencies.sh"
  fi
  chown -R "${GITHUB_RUNNER_USER}:${GITHUB_RUNNER_USER}" "${GITHUB_RUNNER_INSTALL_DIR}"
}

ensure_kubeconfig() {
  local runner_home kube_dir kubeconfig_path
  runner_home="$(getent passwd "${GITHUB_RUNNER_USER}" | cut -d: -f6)"
  kube_dir="${runner_home}/.kube"
  kubeconfig_path="${kube_dir}/config"

  mkdir -p "${kube_dir}"
  if [[ -f /etc/rancher/k3s/k3s.yaml ]]; then
    cp /etc/rancher/k3s/k3s.yaml "${kubeconfig_path}"
    chown -R "${GITHUB_RUNNER_USER}:${GITHUB_RUNNER_USER}" "${kube_dir}"
    chmod 0700 "${kube_dir}"
    chmod 0600 "${kubeconfig_path}"
  fi
}

cleanup_runner_artifacts() {
  local active_bin active_externals runner_home work_dir
  runner_home="$(getent passwd "${GITHUB_RUNNER_USER}" | cut -d: -f6)"
  work_dir="${GITHUB_RUNNER_WORK_DIR:-${runner_home}/actions-runner-work}"
  active_bin="$(readlink -f "${GITHUB_RUNNER_INSTALL_DIR}/bin" 2>/dev/null || true)"
  active_externals="$(readlink -f "${GITHUB_RUNNER_INSTALL_DIR}/externals" 2>/dev/null || true)"

  find "${GITHUB_RUNNER_INSTALL_DIR}" -maxdepth 1 -mindepth 1 -type d -name 'bin.*' 2>/dev/null | while read -r path; do
    if [[ -n "${active_bin}" && "${path}" == "${active_bin}" ]]; then
      continue
    fi
    rm -rf -- "${path}"
  done

  find "${GITHUB_RUNNER_INSTALL_DIR}" -maxdepth 1 -mindepth 1 -type d -name 'externals.*' 2>/dev/null | while read -r path; do
    if [[ -n "${active_externals}" && "${path}" == "${active_externals}" ]]; then
      continue
    fi
    rm -rf -- "${path}"
  done

  if [[ -d "${work_dir}/_update" ]]; then
    rm -rf -- "${work_dir}/_update"
    mkdir -p "${work_dir}/_update"
    chown -R "${GITHUB_RUNNER_USER}:${GITHUB_RUNNER_USER}" "${work_dir}/_update"
  fi
}

configure_runner() {
  local runner_url runner_home work_dir config_command
  runner_url="${GITHUB_RUNNER_URL:-https://github.com/${GITHUB_REPOSITORY}}"
  runner_home="$(getent passwd "${GITHUB_RUNNER_USER}" | cut -d: -f6)"
  work_dir="${GITHUB_RUNNER_WORK_DIR:-${runner_home}/actions-runner-work}"
  mkdir -p "${work_dir}"
  chown -R "${GITHUB_RUNNER_USER}:${GITHUB_RUNNER_USER}" "${work_dir}"

  if [[ -f "${GITHUB_RUNNER_INSTALL_DIR}/.runner" ]]; then
    log "runner already configured; skipping config.sh"
    return
  fi

  printf -v config_command "cd %q && ./config.sh --url %q --token %q --name %q --labels %q --work %q --unattended --replace" \
    "${GITHUB_RUNNER_INSTALL_DIR}" \
    "${runner_url}" \
    "${GITHUB_RUNNER_TOKEN}" \
    "${GITHUB_RUNNER_NAME}" \
    "${GITHUB_RUNNER_LABELS}" \
    "${work_dir}"

  su - "${GITHUB_RUNNER_USER}" -s /bin/bash -c "${config_command}"
}

install_service() {
  local needs_install="false"
  if [[ ! -f "${GITHUB_RUNNER_INSTALL_DIR}/svc.sh" ]]; then
    fail "runner service script not found"
  fi

  if [[ ! -x "${GITHUB_RUNNER_INSTALL_DIR}/runsvc.sh" ]]; then
    needs_install="true"
  fi
  if ! compgen -G "/etc/systemd/system/actions.runner.*.service" >/dev/null; then
    needs_install="true"
  fi

  if [[ "${needs_install}" == "true" ]] && compgen -G "/etc/systemd/system/actions.runner.*.service" >/dev/null; then
    (
      cd "${GITHUB_RUNNER_INSTALL_DIR}"
      ./svc.sh uninstall || true
    )
  fi

  if [[ "${needs_install}" == "true" ]]; then
    (
      cd "${GITHUB_RUNNER_INSTALL_DIR}"
      ./svc.sh install "${GITHUB_RUNNER_USER}"
    )
  fi
  (
    cd "${GITHUB_RUNNER_INSTALL_DIR}"
    ./svc.sh start
  )
}

main() {
  need_root

  require_env GITHUB_REPOSITORY
  require_env GITHUB_RUNNER_TOKEN

  GITHUB_RUNNER_VERSION="${GITHUB_RUNNER_VERSION:-2.326.0}"
  GITHUB_RUNNER_USER="${GITHUB_RUNNER_USER:-github-runner}"
  GITHUB_RUNNER_INSTALL_DIR="${GITHUB_RUNNER_INSTALL_DIR:-/opt/github-runner}"
  GITHUB_RUNNER_NAME="${GITHUB_RUNNER_NAME:-$(hostname -s)}"
  GITHUB_RUNNER_LABELS="${GITHUB_RUNNER_LABELS:-self-hosted,linux,x64,fugue,control-plane}"

  install_base_packages
  install_helm
  ensure_runner_user
  download_runner
  ensure_kubeconfig
  configure_runner
  install_service
  cleanup_runner_artifacts

  log "runner installed"
  log "repository: ${GITHUB_REPOSITORY}"
  log "runner name: ${GITHUB_RUNNER_NAME}"
  log "runner labels: ${GITHUB_RUNNER_LABELS}"
}

main "$@"
