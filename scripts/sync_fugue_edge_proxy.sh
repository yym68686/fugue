#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/install_fugue_ha.sh"

main() {
  require_cmd ssh
  require_cmd scp
  require_cmd openssl

  detect_api_ip
  maybe_reuse_existing_edge_tls_ask_token
  [[ -n "${FUGUE_EDGE_TLS_ASK_TOKEN}" ]] || fail "failed to detect FUGUE edge TLS ask token"

  log "syncing Route A edge proxy on ${PRIMARY_ALIAS}"
  log "configured Route A domain: ${FUGUE_DOMAIN}"
  log "configured app base domain: ${FUGUE_APP_BASE_DOMAIN}"
  log "detected API IP: ${K3S_API_IP}"

  install_edge_proxy_on_primary
  verify_edge_proxy_config_on_primary
  check_edge_origin_health

  log "Route A edge proxy sync complete on ${PRIMARY_ALIAS}"
}

main "$@"
