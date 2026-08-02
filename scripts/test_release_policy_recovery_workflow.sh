#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

fail() {
  printf '[test_release_policy_retirement] ERROR: %s\n' "$*" >&2
  exit 1
}

command -v rg >/dev/null 2>&1 || fail 'rg is required for executable retirement scanning'
command -v ruby >/dev/null 2>&1 || fail 'ruby is required for workflow command parsing'

retired_paths=(
  '.github/workflows/recover-control-plane-release-policy.yml'
  '.github/workflows/watch-control-plane-release-policy-recovery.yml'
  'scripts/recover_control_plane_release_baseline.py'
)

for retired_path in "${retired_paths[@]}"; do
  if [[ -e "${REPO_ROOT}/${retired_path}" || -L "${REPO_ROOT}/${retired_path}" ]]; then
    fail "retired rollback entrypoint still exists: ${retired_path}"
  fi
done

reference_scan_status=0
references="$(
  rg --files-with-matches --fixed-strings \
    --hidden --no-ignore --follow \
    -e 'recover-control-plane-release-policy' \
    -e 'watch-control-plane-release-policy-recovery' \
    -e 'recover_control_plane_release_baseline' \
    "${REPO_ROOT}/.github/workflows" "${REPO_ROOT}/scripts" \
    --glob '!test_release_policy_recovery_workflow.sh'
)" || reference_scan_status=$?
if (( reference_scan_status > 1 )); then
  fail "retired rollback reference scan failed closed: status=${reference_scan_status}"
fi
[[ -z "${references}" ]] || fail "retired rollback entrypoint remains referenced by executable source: ${references}"

umask 077
inventory_file="$(mktemp "${TMPDIR:-/tmp}/fugue-release-policy-retirement.XXXXXX")" ||
  fail 'could not create private capability inventory'
cleanup_inventory() {
  rm -f -- "${inventory_file}"
}
trap cleanup_inventory EXIT
inventory_scan_status=0
rg --files --null --hidden --no-ignore --follow \
  "${REPO_ROOT}/.github/workflows" "${REPO_ROOT}/scripts" \
  >"${inventory_file}" || inventory_scan_status=$?
if (( inventory_scan_status != 0 )); then
  fail "rollback capability inventory failed closed: status=${inventory_scan_status}"
fi

while IFS= read -r -d '' candidate; do
  relative_path="${candidate#"${REPO_ROOT}/"}"
  [[ "${relative_path}" != 'scripts/test_release_policy_recovery_workflow.sh' ]] || continue
  source="$(<"${candidate}")"
  if [[ "${source}" == *'refs/tags/fugue-control-plane-release-baseline'* ]]; then
    fail "legacy mutable release baseline tag remains executable: ${relative_path}"
  fi
done <"${inventory_file}"
cleanup_inventory
trap - EXIT

deploy_workflow="${REPO_ROOT}/.github/workflows/deploy-control-plane.yml"
[[ -f "${deploy_workflow}" ]] || fail 'ordinary deploy workflow is absent'
ruby -ryaml - "${deploy_workflow}" <<'RUBY'
workflow = YAML.load_file(ARGV.fetch(0))
abort("retirement tombstone: workflow-level shell or environment overrides are forbidden") if
  workflow.key?("defaults") || workflow.key?("env")
jobs = workflow.fetch("jobs")
release_gate = jobs.fetch("release-gate")
abort("retirement tombstone: release-gate job execution semantics drifted") unless
  release_gate.keys.sort == ["needs", "permissions", "runs-on", "steps"] &&
  release_gate.fetch("needs") == ["release-input-guard"] &&
  release_gate.fetch("runs-on") == "ubuntu-latest" &&
  release_gate.fetch("permissions") == {"actions" => "read", "contents" => "read"}
steps = Array(release_gate.fetch("steps"))
abort("retirement tombstone: release-gate must contain only the source CI receipt gate") unless steps.length == 1
receipt = steps.fetch(0)
abort("retirement tombstone: source CI receipt step shape drifted") unless
  receipt.keys.sort == ["env", "name", "run"] &&
  receipt.fetch("name") == "Verify exact source CI receipt" &&
  receipt.fetch("env") == {
    "EXPECTED_SHA" => "${{ inputs.expected_sha }}",
    "GH_TOKEN" => "${{ github.token }}",
    "REPOSITORY" => "${{ github.repository }}",
  }
receipt_run = receipt.fetch("run")
[
  "actions/workflows/ci.yml/runs?branch=main&event=push&status=success&per_page=100",
  "select(.head_sha ==",
  '[[ "${ci_attempt}" == \'1\' && "${ci_event}" == \'push\' && "${ci_branch}" == \'main\' ]]',
  '[[ "${ci_sha}" == "${EXPECTED_SHA}" && "${ci_status}" == \'completed\' && "${ci_conclusion}" == \'success\' ]]',
  '[[ "${ci_path}" == \'.github/workflows/ci.yml\' ]]',
].each do |fragment|
  abort("retirement tombstone: source CI receipt gate is missing #{fragment.inspect}") unless receipt_run.include?(fragment)
end
[
  "make generate-openapi-check", "test_release_domain_safety.sh", "test_node_local_dns_release.sh",
  "test_verify_stale_release_recovery.py", "go test ./...",
].each do |forbidden|
  abort("retirement tombstone: deploy release-gate reruns source CI: #{forbidden}") if receipt_run.include?(forbidden)
end
RUBY

printf '[test_release_policy_retirement] retired rollback entrypoints are absent\n'
