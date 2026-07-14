#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/install_fugue_ha.sh"

require_control_plane_host_alias() {
  local env_name="$1"
  local value="${!env_name:-}"

  [[ -n "${value}" ]] || fail "${env_name} is required to bootstrap the control-plane automation bundle"
  [[ ! "${value}" =~ [[:space:]] ]] || fail "${env_name} must not contain whitespace"
}

require_control_plane_host_inventory() {
  require_control_plane_host_alias FUGUE_NODE1
  require_control_plane_host_alias FUGUE_NODE2
  require_control_plane_host_alias FUGUE_NODE3
  if [[ "${PRIMARY_ALIAS}" == "${SECONDARY_ALIASES[0]}" ||
    "${PRIMARY_ALIAS}" == "${SECONDARY_ALIASES[1]}" ||
    "${SECONDARY_ALIASES[0]}" == "${SECONDARY_ALIASES[1]}" ]]; then
    fail "FUGUE_NODE1, FUGUE_NODE2, and FUGUE_NODE3 must identify three distinct hosts"
  fi
}

main() {
  require_control_plane_host_inventory
  require_cmd ssh
  require_cmd scp
  require_cmd ssh-keygen
  require_cmd ssh-keyscan

  prepare_dist
  check_ssh_and_sudo
  detect_api_ip

  if ! cluster_is_ready; then
    fail "control-plane cluster is not ready on ${PRIMARY_ALIAS}; bootstrap the cluster before publishing automation credentials"
  fi

  setup_control_plane_automation
  log "control-plane automation SSH bundle published to ${NAMESPACE}/${CONTROL_PLANE_AUTOMATION_SECRET_NAME}"
}

main "$@"
