#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/install_fugue_ha.sh"

main() {
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
