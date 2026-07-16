#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKFLOW_FILE="${REPO_ROOT}/.github/workflows/deploy-control-plane.yml"

ruby - "${WORKFLOW_FILE}" <<'RUBY'
require "yaml"

workflow_path = ARGV.fetch(0)
source = File.read(workflow_path, encoding: "UTF-8")
workflow = YAML.safe_load(source, aliases: false)

def fail_contract(message)
  warn "release-domain workflow contract: #{message}"
  exit 1
end

def needs(job)
  value = job["needs"]
  value.is_a?(Array) ? value : (value.nil? ? [] : [value])
end

def step(job, name)
  matches = Array(job["steps"]).select { |candidate| candidate["name"] == name }
  fail_contract("expected exactly one #{name.inspect} step") unless matches.length == 1
  matches.fetch(0)
end

def assert_equal(actual, expected, message)
  fail_contract("#{message}: got #{actual.inspect}, want #{expected.inspect}") unless actual == expected
end

trigger = workflow["on"] || workflow[true]
fail_contract("workflow trigger is missing") unless trigger.is_a?(Hash)
assert_equal(trigger.keys, ["workflow_dispatch"], "release must be dispatch-only")
dispatch = trigger.fetch("workflow_dispatch")
inputs = dispatch.fetch("inputs")
assert_equal(inputs.keys, ["expected_sha"], "dispatch input set")
expected_sha = inputs.fetch("expected_sha")
assert_equal(expected_sha["required"], true, "expected_sha required flag")
assert_equal(expected_sha["type"], "string", "expected_sha type")
fail_contract("expected_sha must not have a default") if expected_sha.key?("default")

assert_equal(workflow["permissions"], {"contents" => "read"}, "top-level permissions")
jobs = workflow.fetch("jobs")

guard = jobs.fetch("release-input-guard")
assert_equal(needs(guard), [], "input guard dependencies")
guard_step = step(guard, "Guard exact main commit authorization")
{
  "EXPECTED_SHA" => "${{ inputs.expected_sha }}",
  "ACTUAL_SHA" => "${{ github.sha }}",
  "EVENT_NAME" => "${{ github.event_name }}",
  "EVENT_REF" => "${{ github.ref }}",
  "EVENT_REF_NAME" => "${{ github.ref_name }}",
  "EVENT_REF_TYPE" => "${{ github.ref_type }}",
}.each do |name, expected|
  assert_equal(guard_step.fetch("env").fetch(name), expected, "guard #{name} source")
end
for fragment in [
  '"${EVENT_REF}" == "refs/heads/main"',
  '"${EVENT_REF_NAME}" == "main"',
  '"${EVENT_REF_TYPE}" == "branch"',
  '^[0-9a-f]{40}$',
  '"${EXPECTED_SHA}" == "${ACTUAL_SHA}"',
]
  fail_contract("input guard is missing #{fragment.inspect}") unless guard_step.fetch("run").include?(fragment)
end

baseline = jobs.fetch("release-baseline")
assert_equal(needs(baseline), ["release-input-guard"], "release-baseline dependencies")
assert_equal(
  baseline.fetch("outputs").fetch("domain_base_sha"),
  "${{ steps.domain_baseline.outputs.domain_base_sha }}",
  "domain baseline output",
)
assert_equal(
  baseline.fetch("outputs").fetch("baseline_ref_object_sha"),
  "${{ steps.domain_baseline.outputs.baseline_ref_object_sha }}",
  "baseline ref object output",
)
resolver = step(baseline, "Resolve release-domain baseline")
assert_equal(resolver["id"], "domain_baseline", "domain baseline step id")
assert_equal(
  resolver.fetch("env").fetch("FUGUE_CONTROL_PLANE_RELEASE_GENESIS_SHA"),
  "${{ vars.FUGUE_CONTROL_PLANE_RELEASE_GENESIS_SHA }}",
  "genesis repository variable",
)
for fragment in [
  "readonly baseline_tag='fugue-control-plane-release-baseline'",
  "readonly genesis_base_sha='723116882214ae9efeaee0877bb378d0db2dcea7'",
  "readonly genesis_b3_sha='8b4bdc2a2b443be6d1244f9b4739cd0be1313d71'",
  "readonly genesis_parent_sha='4d74c6f963258f9f5c3925613891db9163327330'",
  'git ls-remote --refs --exit-code origin "${baseline_ref}"',
  '"${fetched_ref_object_sha}" == "${remote_object}"',
  '"${remote_object}" == "${domain_base_sha}"',
  'git merge-base --is-ancestor "${domain_base_sha}" "${target_sha}"',
  '"${genesis_sha}" == "${target_sha}"',
  '"${actual_parent}" == "${genesis_parent_sha}"',
  '"${parent_b3}" == "${genesis_b3_sha}"',
  '"${b3_base}" == "${genesis_base_sha}"',
  'domain_base_sha="${genesis_base_sha}"',
]
  fail_contract("baseline resolver is missing #{fragment.inspect}") unless resolver.fetch("run").include?(fragment)
end
changes = step(baseline, "Compute live-to-target release changed files")
assert_equal(
  changes.fetch("env").fetch("FUGUE_RELEASE_BASE_REFS"),
  "${{ steps.live_images.outputs.release_baseline_tags }}",
  "live image build baseline",
)

gate = jobs.fetch("release-gate")
assert_equal(needs(gate), ["release-input-guard"], "release-gate dependencies")
gate_commands = Array(gate["steps"]).map { |candidate| candidate["run"].to_s }.join("\n")
fail_contract("release gate must run the workflow contract test") unless gate_commands.include?("bash scripts/test_release_domain_workflow.sh")

build = jobs.fetch("build")
assert_equal(build["permissions"], {"contents" => "read", "packages" => "write"}, "build permissions")

deploy = jobs.fetch("deploy")
assert_equal(deploy["permissions"], {"actions" => "read", "contents" => "read"}, "deploy permissions")
assert_equal(deploy["continue-on-error"], nil, "deploy continue-on-error")
setup_go = step(deploy, "Setup Go")
assert_equal(setup_go["uses"], "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16", "deploy setup-go pin")
build_tools = step(deploy, "Build private release-domain tools")
for fragment in [
  '${RUNNER_TEMP}/fugue-release-tools',
  'for goarch in amd64 arm64; do',
  'CGO_ENABLED=0',
  'GOARCH="${goarch}"',
  'GOOS=linux',
  'GOFLAGS=-mod=readonly',
  'go list -mod=readonly -buildvcs=false -deps ./cmd/...',
  'go mod verify',
  'GOPROXY=https://proxy.golang.org',
  "'GOVCS=*:off'",
  'git diff --exit-code -- go.mod go.sum',
  './cmd/fugue-release-domain-evidence',
  './cmd/fugue-release-domain-dispatch',
  'chmod 0700',
  "stat -c '%a'",
]
  fail_contract("release tool build is missing #{fragment.inspect}") unless build_tools.fetch("run").include?(fragment)
end
fail_contract("release tool build must not preload unrelated module versions") if build_tools.fetch("run").include?("go mod download all")
fail_contract("release tool cache validation must not disable the module proxy") if build_tools.fetch("run").include?("GOPROXY=off")
preload_index = build_tools.fetch("run").index("go list -mod=readonly -buildvcs=false -deps ./cmd/...")
verify_index = build_tools.fetch("run").index("go mod verify")
evidence_build_index = build_tools.fetch("run").index("go build -trimpath -o \"${tools_dir}/fugue-release-domain-evidence\"")
fail_contract("command dependency graphs must be preloaded, verified, then used to build evidence") unless
  preload_index && verify_index && evidence_build_index && preload_index < verify_index && verify_index < evidence_build_index

genesis = step(deploy, "Write genesis public release evidence")
assert_equal(genesis["if"], "${{ needs.release-baseline.outputs.is_genesis == 'true' }}", "genesis evidence condition")
for fragment in [
  'fugue-release-domain-evidence"',
  "write-genesis-public-evidence",
  '--ownership "${GITHUB_WORKSPACE}/deploy/release-domains/ownership-v1.yaml"',
  '--expected-head-sha "${GENESIS_SHA}"',
  '--evidence-base-sha "${DOMAIN_BASE_SHA}"',
  '--actual-parent-sha "${GENESIS_PARENT_SHA}"',
  '${RUNNER_TEMP}/fugue-release-domain-private',
  '${RUNNER_TEMP}/fugue-release-domain-public',
]
  fail_contract("genesis evidence step is missing #{fragment.inspect}") unless genesis.fetch("run").include?(fragment)
end
genesis_run = genesis.fetch("run")
genesis_run.each_line do |line|
  next unless line.include?("upgrade_fugue_control_plane.sh")
  fail_contract("genesis path must not invoke the upgrade script") unless line.strip.start_with?('--expected-change "')
end

expected_genesis_changes = [
  ".github/workflows/deploy-control-plane.yml",
  "cmd/fugue-release-domain-dispatch/classify_files.go",
  "cmd/fugue-release-domain-dispatch/main.go",
  "cmd/fugue-release-domain-dispatch/main_test.go",
  "cmd/fugue-release-domain-dispatch/public_evidence.go",
  "cmd/fugue-release-domain-dispatch/public_evidence_test.go",
  "cmd/fugue-release-domain-dispatch/secure_files.go",
  "cmd/fugue-release-domain-dispatch/stat_times_darwin.go",
  "cmd/fugue-release-domain-dispatch/stat_times_linux.go",
  "cmd/fugue-release-domain-dispatch/stat_times_other.go",
  "cmd/fugue-release-domain-dispatch/strict_json.go",
  "cmd/fugue-release-domain-evidence/evidence.go",
  "cmd/fugue-release-domain-evidence/evidence_test.go",
  "cmd/fugue-release-domain-evidence/main.go",
  "cmd/fugue-release-domain-evidence/manifest.go",
  "cmd/fugue-release-domain-evidence/manifest_test.go",
  "cmd/fugue-release-domain-plan/main.go",
  "cmd/fugue-release-domain-plan/main_test.go",
  "cmd/fugue-release-domain-plan/output.go",
  "cmd/fugue-release-domain-plan/output_test.go",
  "deploy/release-domains/ownership-v1.yaml",
  "docs/runbooks/release-domain-planner.md",
  "internal/api/topology_labeler_test.go",
  "internal/platformsafety/release_workflow_test.go",
  "internal/releaseadapter/adapter.go",
  "internal/releaseadapter/dispatcher.go",
  "internal/releaseadapter/dispatcher_test.go",
  "internal/releaseadapter/trace.go",
  "internal/releaseadapter/transaction.go",
  "internal/releaseadapter/transaction_test.go",
  "internal/releasedomain/changed_file_evidence.go",
  "internal/releasedomain/changed_file_evidence_test.go",
  "internal/releasedomain/file_classifier_test.go",
  "internal/releasedomain/ownership_test.go",
  "internal/releasedomain/plan_artifacts.go",
  "internal/releasedomain/plan_artifacts_test.go",
  "internal/releasedomain/rendered_classifier_test.go",
  "internal/releasedomain/rollback_ownership.go",
  "internal/releasedomain/rollback_ownership_test.go",
  "internal/releasedomain/transaction_envelope.go",
  "internal/releaseevidence/public.go",
  "internal/releaseevidence/public_test.go",
  "scripts/lib/control_plane_release_domain_production.sh",
  "scripts/lib/control_plane_release_domains.sh",
  "scripts/lib/control_plane_release_render.sh",
  "scripts/test_control_plane_release_domain_production.sh",
  "scripts/test_control_plane_release_main_wiring.sh",
  "scripts/test_control_plane_release_render.sh",
  "scripts/test_release_domain_safety.sh",
  "scripts/test_release_domain_workflow.sh",
  "scripts/test_single_domain_release.sh",
  "scripts/upgrade_fugue_control_plane.sh",
]
fail_contract("genesis expected-change allowlist must contain exactly 52 unique paths") unless
  expected_genesis_changes.length == 52 && expected_genesis_changes.uniq.length == 52
actual_genesis_changes = genesis_run.scan(/^\s*--expected-change "([^"]+)" \\\s*$/).flatten
assert_equal(genesis_run.scan(/--expected-change/).length, 52, "genesis expected-change occurrence count")
assert_equal(actual_genesis_changes, expected_genesis_changes, "genesis expected-change exact allowlist")

genesis_reachable = {
  "Checkout" => "",
  "Setup Go" => "",
  "Build private release-domain tools" => "",
  "Write genesis public release evidence" => "${{ needs.release-baseline.outputs.is_genesis == 'true' }}",
  "Upload release-domain public evidence" => "always()",
}
Array(deploy["steps"]).each do |candidate|
  name = candidate.fetch("name")
  condition = candidate["if"].to_s
  if genesis_reachable.key?(name)
    assert_equal(condition, genesis_reachable.fetch(name), "genesis-reachable #{name} condition")
  elsif !condition.include?("needs.release-baseline.outputs.is_genesis != 'true'")
    fail_contract("unreviewed deploy step #{name.inspect} is reachable from genesis")
  end
end
assert_equal(
  Array(deploy["steps"]).map { |candidate| candidate.fetch("name") }.select { |name| genesis_reachable.key?(name) },
  genesis_reachable.keys,
  "genesis-reachable step allowlist",
)

upgrade = step(deploy, "Upgrade Fugue control plane")
upgrade_env = upgrade.fetch("env")
{
  "FUGUE_RELEASE_DOMAIN_BASE_SHA" => "${{ needs.release-baseline.outputs.domain_base_sha }}",
  "FUGUE_RELEASE_DOMAIN_TARGET_SHA" => "${{ github.sha }}",
  "FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL" => "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-evidence",
  "FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL" => "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-dispatch",
  "FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE" => "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json",
}.each do |name, expected|
  assert_equal(upgrade_env[name], expected, "upgrade #{name}")
end

public_upload = step(deploy, "Upload release-domain public evidence")
assert_equal(public_upload["if"], "always()", "public evidence upload condition")
assert_equal(public_upload["continue-on-error"], nil, "public evidence upload continue-on-error")
assert_equal(
  public_upload.fetch("with").fetch("path"),
  "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json",
  "public evidence upload path",
)
assert_equal(public_upload.fetch("with").fetch("if-no-files-found"), "error", "public evidence missing-file policy")
assert_equal(public_upload.fetch("with").fetch("retention-days"), 90, "public evidence retention")
assert_equal(public_upload.fetch("with").fetch("include-hidden-files"), false, "public evidence hidden-file policy")
assert_equal(public_upload.fetch("with").fetch("overwrite"), false, "public evidence overwrite policy")
deploy_uploads = Array(deploy["steps"]).select { |candidate| candidate["uses"].to_s.start_with?("actions/upload-artifact@") }
assert_equal(deploy_uploads.length, 1, "deploy artifact upload count")

record = jobs.fetch("record-release-baseline")
assert_equal(
  needs(record),
  ["release-input-guard", "release-baseline", "release-gate", "build", "deploy"],
  "record-release-baseline dependencies",
)
assert_equal(record["permissions"], {"contents" => "write"}, "record-release-baseline permissions")
assert_equal(
  record["if"],
  "${{ always() && needs.release-input-guard.result == 'success' && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' && needs.deploy.result == 'success' }}",
  "record-release-baseline success condition",
)
advance = step(record, "Advance dedicated release baseline tag")
assert_equal(
  advance.fetch("env").fetch("EXPECTED_BASE_REF_OBJECT"),
  "${{ needs.release-baseline.outputs.baseline_ref_object_sha }}",
  "record baseline ref-object input",
)
for fragment in [
  "readonly baseline_ref='refs/tags/fugue-control-plane-release-baseline'",
  "readonly genesis_base_sha='723116882214ae9efeaee0877bb378d0db2dcea7'",
  "readonly genesis_b3_sha='8b4bdc2a2b443be6d1244f9b4739cd0be1313d71'",
  "readonly genesis_parent_sha='4d74c6f963258f9f5c3925613891db9163327330'",
  'git ls-remote --refs --exit-code origin "${baseline_ref}"',
  '"${remote_object}" == "${EXPECTED_BASE_REF_OBJECT}"',
  '"${fetched_ref_object_sha}" == "${EXPECTED_BASE_REF_OBJECT}"',
  '"${EXPECTED_BASE_REF_OBJECT}" == "${EXPECTED_BASE_SHA}"',
  '"${current_base_sha}" == "${EXPECTED_BASE_SHA}"',
  '"${EXPECTED_BASE_SHA}" == "${genesis_base_sha}"',
  '"${target_parent}" == "${genesis_parent_sha}"',
  '"${parent_b3}" == "${genesis_b3_sha}"',
  '"${b3_base}" == "${genesis_base_sha}"',
  '--force-with-lease="${lease}"',
  '"${TARGET_SHA}:${baseline_ref}"',
]
  fail_contract("baseline advancement is missing #{fragment.inspect}") unless advance.fetch("run").include?(fragment)
end
assert_equal(advance.fetch("run").scan(/\bgit push\b/).length, 1, "baseline git push count")
fail_contract("baseline advancement must not update a branch") if advance.fetch("run").include?("refs/heads/")

freeze = jobs.fetch("freeze-release-lane-on-failure")
freeze_needs = [
  "release-input-guard", "release-baseline", "release-gate", "build", "deploy", "record-release-baseline",
]
assert_equal(needs(freeze), freeze_needs, "freeze finalizer dependencies")
freeze_needs.each do |job_name|
  fail_contract("freeze condition omits #{job_name}") unless freeze.fetch("if").include?("needs.#{job_name}.result != 'success'")
end
assert_equal(freeze["permissions"], {"actions" => "write", "contents" => "read"}, "freeze permissions")

allowed_permissions = {
  "build" => {"contents" => "read", "packages" => "write"},
  "deploy" => {"actions" => "read", "contents" => "read"},
  "record-release-baseline" => {"contents" => "write"},
  "freeze-release-lane-on-failure" => {"actions" => "write", "contents" => "read"},
}
jobs.each do |name, job|
  assert_equal(job["permissions"], allowed_permissions[name], "#{name} job permissions") if job.key?("permissions") || allowed_permissions.key?(name)
end

all_uploads = jobs.each_with_object([]) do |(job_name, job), uploads|
  Array(job["steps"]).each do |candidate|
    next unless candidate["uses"].to_s.start_with?("actions/upload-artifact@")
    uploads << [job_name, candidate.fetch("with").fetch("path")]
  end
end
allowed_uploads = [
  ["deploy", "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json"],
  ["freeze-release-lane-on-failure", "${{ runner.temp }}/fugue-release-lane-freeze/lane-freeze.json"],
]
assert_equal(all_uploads, allowed_uploads, "public artifact allowlist")
fail_contract("workflow must never upload a private release directory") if source.include?("path: ${{ runner.temp }}/fugue-release\n")
fail_contract("workflow must not enable itself") if source.include?("actions/workflows/${workflow_id}/enable")

puts "release-domain workflow contract passed"
RUBY
