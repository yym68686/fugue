#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

failures=0

check_pattern() {
  local description="$1"
  local pattern="$2"
  shift 2
  local matches=""
  if command -v rg >/dev/null 2>&1; then
    matches="$(rg -n --hidden --glob '!scripts/scan_hardcoded_production_facts.sh' "${pattern}" "$@" || true)"
  else
    matches="$(grep -REn --exclude='scan_hardcoded_production_facts.sh' "${pattern}" "$@" 2>/dev/null || true)"
  fi
  if [[ -n "${matches}" ]]; then
    printf '[hardcoding-scan] %s\n' "${description}" >&2
    printf '%s\n' "${matches}" >&2
    failures=$((failures + 1))
  fi
}

release_surfaces=(
  .github/workflows/deploy-control-plane.yml
  deploy/helm/fugue/values.yaml
  deploy/helm/fugue/templates
  internal/api/discovery.go
  internal/config/config.go
  scripts/install_fugue_ha.sh
  scripts/upgrade_fugue_control_plane.sh
  scripts/render_fugue_edge_systemd_unit.sh
  scripts/render_fugue_dns_systemd_unit.sh
  scripts/issue_fugue_app_wildcard_tls.sh
)

check_pattern "api.fugue.pro must not be a release-time default or hardcoded runtime branch" "api\\.fugue\\.pro" "${release_surfaces[@]}"
check_pattern "registry.fugue.internal must come from DiscoveryBundle, explicit values, or production overlay" "registry\\.fugue\\.internal" "${release_surfaces[@]}"
check_pattern "legacy GCP node aliases must not be shell defaults" "FUGUE_NODE[123]:-gcp[123]|:-gcp[123]" "${release_surfaces[@]}"
check_pattern "old control-plane private GCP ranges must not be fallback defaults" "10\\.128\\.[0-9]+\\.[0-9]+" "${release_surfaces[@]}"
check_pattern "static Kubernetes service ClusterIP defaults must come from explicit overlays" "10\\.43\\.[0-9]+\\.[0-9]+" "${release_surfaces[@]}"
check_pattern "old tailnet Kubernetes API endpoints must not be fallback defaults" "https://100\\.64\\.[0-9]+\\.[0-9]+:6443" "${release_surfaces[@]}"
check_pattern "edge group defaults must be explicit or discovery-derived" "FUGUE_EDGE_GROUP_ID:-edge-group-country-us|edgeGroupID: edge-group-country-us" "${release_surfaces[@]}"
check_pattern "wildcard certificate issuance must not require Cloudflare as the only DNS-01 path" "CLOUDFLARE_DNS_API_TOKEN is required|Cloudflare DNS-01" scripts/issue_fugue_app_wildcard_tls.sh

if (( failures > 0 )); then
  printf '[hardcoding-scan] found %d forbidden production fact(s)\n' "${failures}" >&2
  exit 1
fi

printf '[hardcoding-scan] ok\n'
