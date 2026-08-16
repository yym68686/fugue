#!/usr/bin/env bash

set -euo pipefail

changed_path_file="${1:-}"
github_output="${2:-}"
[[ -f "${changed_path_file}" ]] || { printf 'changed path file is required\n' >&2; exit 1; }
[[ -n "${github_output}" ]] || { printf 'GitHub output path is required\n' >&2; exit 1; }

changed=false
while IFS= read -r path; do
  case "${path}" in
    scripts/apply_fugue_traffic_safety.sh|\
    scripts/export_fugue_traffic_state.sh|\
    scripts/probe_fugue_public_dns.sh|\
    scripts/emit_traffic_safety_changed.sh)
      changed=true
      break
      ;;
  esac
done <"${changed_path_file}"

printf 'traffic_safety_changed=%s\n' "${changed}" >>"${github_output}"
