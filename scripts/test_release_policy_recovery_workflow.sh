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
  release_gate.keys.sort == ["needs", "runs-on", "steps"] &&
  release_gate.fetch("needs") == ["release-input-guard"] &&
  release_gate.fetch("runs-on") == "ubuntu-latest"
steps = Array(release_gate.fetch("steps"))
expected = "bash scripts/test_release_policy_recovery_workflow.sh"
expected_safety_commands = [
  "bash scripts/test_prepare_authoritative_dns_dig.sh",
  "bash scripts/test_release_domain_workflow.sh",
  expected,
  "bash scripts/test_release_domain_safety.sh",
  "FUGUE_REQUIRE_NODE_LOCAL_DNS_TEST_DOCKER=true bash scripts/test_node_local_dns_release.sh",
  "python3 scripts/test_verify_stale_release_recovery.py",
]
expected_ripgrep_setup = <<~'BASH'
  set -euo pipefail
  [[ "$(uname -s)" == 'Linux' ]]
  [[ "$(uname -m)" == 'x86_64' ]]
  readonly archive_name="ripgrep-${RIPGREP_VERSION}-x86_64-unknown-linux-musl.tar.gz"
  readonly release_url="https://github.com/BurntSushi/ripgrep/releases/download/${RIPGREP_VERSION}/${archive_name}"
  readonly archive_path="${RUNNER_TEMP}/${archive_name}"
  readonly tools_dir="${RUNNER_TEMP}/fugue-release-gate-tools"
  [[ ! -e "${archive_path}" && ! -L "${archive_path}" ]]
  [[ ! -e "${tools_dir}" && ! -L "${tools_dir}" ]]
  install -d -m 0700 "${tools_dir}"
  umask 077
  curl \
    --fail \
    --show-error \
    --silent \
    --location \
    --proto '=https' \
    --tlsv1.2 \
    --output "${archive_path}" \
    "${release_url}"
  [[ -f "${archive_path}" && ! -L "${archive_path}" ]]
  printf '%s  %s\n' "${RIPGREP_ARCHIVE_SHA256}" "${archive_path}" | sha256sum --check --strict -
  tar \
    --extract \
    --gzip \
    --file "${archive_path}" \
    --directory "${tools_dir}" \
    --strip-components 1 \
    "ripgrep-${RIPGREP_VERSION}-x86_64-unknown-linux-musl/rg"
  rm -f -- "${archive_path}"
  chmod 0500 "${tools_dir}/rg"
  [[ -f "${tools_dir}/rg" && ! -L "${tools_dir}/rg" && -x "${tools_dir}/rg" ]]
  [[ "$(stat -c '%a' "${tools_dir}/rg")" == '500' ]]
  [[ "$("${tools_dir}/rg" --version | head -n 1)" == "ripgrep ${RIPGREP_VERSION} (rev ${RIPGREP_BUILD_REV})" ]]
  [[ -f "${GITHUB_PATH}" && ! -L "${GITHUB_PATH}" ]]
  printf '%s\n' "${tools_dir}" >> "${GITHUB_PATH}"
BASH
expected_steps = [
  {
    "name" => "Checkout",
    "uses" => "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
    "with" => {"ref" => "${{ github.sha }}"},
  },
  {
    "name" => "Setup Go",
    "uses" => "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16",
    "with" => {"go-version-file" => "go.mod"},
  },
  {
    "name" => "Setup Helm",
    "uses" => "azure/setup-helm@9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310",
    "with" => {"version" => "v4.2.0"},
  },
  {
    "name" => "Prepare pinned ripgrep for release safety contracts",
    "env" => {
      "RIPGREP_VERSION" => "14.1.1",
      "RIPGREP_BUILD_REV" => "4649aa9700",
      "RIPGREP_ARCHIVE_SHA256" => "4cf9f2741e6c465ffdb7c26f38056a59e2a2544b51f7cc128ef28337eeae4d8e",
    },
    "run" => expected_ripgrep_setup,
  },
  {"name" => "Verify generated OpenAPI artifacts", "run" => "make generate-openapi-check"},
  {
    "name" => "Verify release-domain safety contracts",
    "run" => expected_safety_commands.join("\n") + "\n",
  },
  {"name" => "Run Go tests", "run" => "go test ./..."},
]
abort("retirement tombstone: release-gate complete step specification drifted") unless
  steps == expected_steps
ripgrep_step = steps.fetch(3)
ripgrep_env = ripgrep_step.fetch("env")
official_archive_version = "ripgrep 14.1.1 (rev 4649aa9700)"
configured_archive_version = "ripgrep #{ripgrep_env.fetch("RIPGREP_VERSION")} (rev #{ripgrep_env.fetch("RIPGREP_BUILD_REV")})"
abort("retirement tombstone: pinned ripgrep version contract rejects the official archive") unless
  configured_archive_version == official_archive_version
safety_step = steps.fetch(5)
occurrences = jobs.flat_map do |_job_name, job|
  Array(job["steps"]).flat_map do |step|
    step.fetch("run", "").each_line.map(&:strip).select do |line|
      line.include?("test_release_policy_recovery_workflow.sh")
    end
  end
end
abort("retirement tombstone: deploy must execute exactly one strict tombstone command") unless
  occurrences == [expected]
safety_commands = safety_step.fetch("run", "").each_line.map(&:strip).reject(&:empty?)
abort("retirement tombstone: release-gate safety command sequence drifted") unless
  safety_commands == expected_safety_commands
RUBY

printf '[test_release_policy_retirement] retired rollback entrypoints are absent\n'
