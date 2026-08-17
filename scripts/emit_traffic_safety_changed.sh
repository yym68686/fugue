#!/usr/bin/env bash

set -euo pipefail

changed_path_file="${1:-}"
github_output="${2:-}"
base_revision="${3:-}"
head_revision="${4:-HEAD}"
[[ -f "${changed_path_file}" ]] || { printf 'changed path file is required\n' >&2; exit 1; }
[[ -n "${github_output}" ]] || { printf 'GitHub output path is required\n' >&2; exit 1; }

repo_root="${FUGUE_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

workflow_job_at_revision() {
  local revision="$1"
  git -C "${repo_root}" show "${revision}:.github/workflows/ci.yml" | awk '
    /^  traffic_safety_stage0:$/ { active=1 }
    active && seen && /^  [^[:space:]][^:]*:/ { exit }
    active { print; seen=1 }
  '
}

traffic_safety_workflow_changed() {
  [[ -n "${base_revision}" ]] || return 0
  git -C "${repo_root}" rev-parse --verify "${base_revision}^{commit}" >/dev/null 2>&1 || return 0
  git -C "${repo_root}" rev-parse --verify "${head_revision}^{commit}" >/dev/null 2>&1 || return 0
  [[ "$(workflow_job_at_revision "${base_revision}")" != "$(workflow_job_at_revision "${head_revision}")" ]]
}

changed=false
while IFS= read -r path; do
  case "${path}" in
    scripts/apply_fugue_traffic_safety.sh|\
    scripts/export_fugue_traffic_state.sh|\
    scripts/probe_fugue_public_dns.sh)
      changed=true
      break
      ;;
    .github/workflows/ci.yml)
      if traffic_safety_workflow_changed; then
        changed=true
        break
      fi
      ;;
  esac
done <"${changed_path_file}"

printf 'traffic_safety_changed=%s\n' "${changed}" >>"${github_output}"
