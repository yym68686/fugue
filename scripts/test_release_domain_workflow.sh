#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKFLOW_FILE="${REPO_ROOT}/.github/workflows/deploy-control-plane.yml"
OPERATIONAL_ACTION_FILE="${REPO_ROOT}/.github/actions/operational-domain-guarded-deploy/action.yml"

ruby - "${WORKFLOW_FILE}" "${OPERATIONAL_ACTION_FILE}" <<'RUBY'
require "yaml"

workflow_path = ARGV.fetch(0)
operational_action_path = ARGV.fetch(1)
source = File.read(workflow_path, encoding: "UTF-8")
workflow = YAML.safe_load(source, aliases: false)
operational_action = YAML.safe_load(File.read(operational_action_path, encoding: "UTF-8"), aliases: false)

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

def action_step(action, name)
  matches = Array(action.fetch("runs").fetch("steps")).select { |candidate| candidate["name"] == name }
  fail_contract("expected exactly one composite action #{name.inspect} step") unless matches.length == 1
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
assert_equal(resolver.fetch("env", {}), {}, "forward baseline resolver environment")
for fragment in [
  "readonly baseline_ref='refs/heads/fugue-control-plane-release-baseline'",
  'git ls-remote --refs --exit-code origin "${baseline_ref}"',
  '"${remote_status}" == \'0\'',
  '"${fetched_ref_object_sha}" == "${remote_object}"',
  'commit_identity="$(git rev-list --parents -n 1 FETCH_HEAD)"',
  "metadata_candidate='false'",
  '"${metadata_path}" == \'fugue-runtime-baseline.json\'',
  "metadata_candidate='true'",
  'git cat-file blob "${metadata_blob}"',
  'previous_sha = value.get("previous_baseline_object_sha")',
  'if payload != expected:',
  'sys.stdout.write(runtime_sha + "\\t" + ("null" if previous_sha is None else previous_sha))',
  '"${metadata_parent}" == "${previous_baseline_object_sha}"',
  '[[ -n "${parent_shas:-}" ]] || exit 1',
  'git cat-file -e "${domain_base_sha}^{commit}"',
  'git merge-base --is-ancestor "${domain_base_sha}" "${target_sha}"',
  "printf 'is_genesis=false",
  "printf 'genesis_parent_sha=",
]
  fail_contract("baseline resolver is missing #{fragment.inspect}") unless resolver.fetch("run").include?(fragment)
end
for forbidden in [
  "refs/tags/", "genesis_base_sha", "force-with-lease", "git push",
  "gh api", "curl ", "--method", "updateRefs",
]
  fail_contract("baseline resolver retains legacy transport #{forbidden.inspect}") if resolver.fetch("run").include?(forbidden)
end
resolver.fetch("run").lines.map(&:strip).select { |line| line.start_with?("[[") }.each do |line|
  fail_contract("baseline resolver check is not explicitly fail-closed: #{line.inspect}") unless line.end_with?("|| exit 1")
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
  ".github/actions/operational-domain-guarded-deploy/action.yml",
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
fail_contract("genesis expected-change allowlist must contain exactly 53 unique paths") unless
  expected_genesis_changes.length == 53 && expected_genesis_changes.uniq.length == 53
actual_genesis_changes = genesis_run.scan(/^\s*--expected-change "([^"]+)" \\\s*$/).flatten
assert_equal(genesis_run.scan(/--expected-change/).length, 53, "genesis expected-change occurrence count")
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

upgrade = step(deploy, "Upgrade Fugue control plane through uploaded operational evidence")
assert_equal(upgrade["uses"], "./.github/actions/operational-domain-guarded-deploy", "guarded deploy action")
fail_contract("guarded deploy workflow step must not define a run body") if upgrade.key?("run")
upgrade_env = upgrade.fetch("env")
{
  "FUGUE_RELEASE_DOMAIN_BASE_SHA" => "${{ needs.release-baseline.outputs.domain_base_sha }}",
  "FUGUE_RELEASE_DOMAIN_TARGET_SHA" => "${{ github.sha }}",
  "FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL" => "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-evidence",
  "FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL" => "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-dispatch",
  "FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE" => "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json",
  "FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE" => "${{ runner.temp }}/fugue-release-domain-public/operational-domain-evidence.json",
  "FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR" => "${{ runner.temp }}/fugue-release-domain-public/build-activation-evidence",
  "FUGUE_RELEASE_DOMAIN_VERIFIED_IMAGE_ARTIFACTS_DIGEST" => "${{ needs.build.outputs.verified_image_artifacts_digest }}",
  "FUGUE_RELEASE_DOMAIN_IMAGE_TARGETS" => "${{ needs.build.outputs.image_targets }}",
  "FUGUE_RELEASE_DOMAIN_API_IMAGE_BASE_SHA" => "${{ needs.release-baseline.outputs.api_image_baseline_ref }}",
  "FUGUE_RELEASE_DOMAIN_API_IMAGE_DIGEST" => "${{ needs.build.outputs.api_image_digest }}",
  "FUGUE_RELEASE_DOMAIN_CONTROLLER_IMAGE_BASE_SHA" => "${{ needs.release-baseline.outputs.controller_image_baseline_ref }}",
  "FUGUE_RELEASE_DOMAIN_CONTROLLER_IMAGE_DIGEST" => "${{ needs.build.outputs.controller_image_digest }}",
  "FUGUE_RELEASE_DOMAIN_DRAIN_AGENT_IMAGE_BASE_SHA" => "${{ needs.release-baseline.outputs.drain_agent_image_baseline_ref }}",
  "FUGUE_RELEASE_DOMAIN_DRAIN_AGENT_IMAGE_DIGEST" => "${{ needs.build.outputs.drain_agent_image_digest }}",
  "FUGUE_RELEASE_DOMAIN_TELEMETRY_AGENT_IMAGE_BASE_SHA" => "${{ needs.release-baseline.outputs.telemetry_agent_image_baseline_ref }}",
  "FUGUE_RELEASE_DOMAIN_TELEMETRY_AGENT_IMAGE_DIGEST" => "${{ needs.build.outputs.telemetry_agent_image_digest }}",
  "FUGUE_RELEASE_DOMAIN_IMAGE_CACHE_IMAGE_BASE_SHA" => "${{ needs.release-baseline.outputs.image_cache_image_baseline_ref }}",
  "FUGUE_RELEASE_DOMAIN_IMAGE_CACHE_IMAGE_DIGEST" => "${{ needs.build.outputs.image_cache_image_digest }}",
  "FUGUE_RELEASE_DOMAIN_EDGE_IMAGE_BASE_SHA" => "${{ needs.release-baseline.outputs.edge_image_baseline_ref }}",
  "FUGUE_RELEASE_DOMAIN_EDGE_IMAGE_DIGEST" => "${{ needs.build.outputs.edge_image_digest }}",
  "FUGUE_RELEASE_DOMAIN_APP_SSH_IMAGE_DIGEST" => "${{ needs.build.outputs.app_ssh_image_digest }}",
  "FUGUE_APP_SSH_IMAGE_REPOSITORY" => "${{ needs.build.outputs.app_ssh_image_repository }}",
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
assert_equal(operational_action.fetch("runs").fetch("using"), "composite", "operational action runtime")
action_steps = operational_action.fetch("runs").fetch("steps")
assert_equal(
  action_steps.map { |candidate| candidate.fetch("name") },
  [
    "Prepare operational-domain report-only evidence",
    "Upload operational-domain report-only evidence",
    "Upload build-vs-activation report-only evidence",
    "Apply exact authorized control-plane release",
  ],
  "operational action step order",
)
prepare = action_step(operational_action, "Prepare operational-domain report-only evidence")
assert_equal(prepare.fetch("env").fetch("FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE"), "prepare", "prepare phase")
assert_equal(prepare.fetch("run"), "./scripts/upgrade_fugue_control_plane.sh", "prepare entrypoint")
operational_upload = action_step(operational_action, "Upload operational-domain report-only evidence")
assert_equal(operational_upload["id"], "operational-report-upload", "operational upload id")
assert_equal(operational_upload["if"], "always()", "operational report upload condition")
assert_equal(operational_upload["continue-on-error"], nil, "operational report upload continue-on-error")
assert_equal(
  operational_upload["uses"],
  "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
  "operational report upload pin",
)
assert_equal(
  operational_upload.fetch("with").fetch("path"),
  "${{ env.FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE }}",
  "operational report upload path",
)
assert_equal(operational_upload.fetch("with").fetch("if-no-files-found"), "error", "operational report missing-file policy")
assert_equal(operational_upload.fetch("with").fetch("retention-days"), 90, "operational report retention")
assert_equal(operational_upload.fetch("with").fetch("include-hidden-files"), false, "operational report hidden-file policy")
assert_equal(operational_upload.fetch("with").fetch("overwrite"), false, "operational report overwrite policy")
activation_upload = action_step(operational_action, "Upload build-vs-activation report-only evidence")
assert_equal(activation_upload["id"], "image-activation-report-upload", "build-activation upload id")
assert_equal(activation_upload["if"], "always()", "build-activation upload condition")
assert_equal(activation_upload["continue-on-error"], nil, "build-activation upload continue-on-error")
assert_equal(
  activation_upload["uses"],
  "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
  "build-activation upload pin",
)
assert_equal(
  activation_upload.fetch("with").fetch("path"),
  "${{ env.FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR }}",
  "build-activation upload path",
)
assert_equal(activation_upload.fetch("with").fetch("if-no-files-found"), "error", "build-activation missing-file policy")
assert_equal(activation_upload.fetch("with").fetch("retention-days"), 90, "build-activation retention")
assert_equal(activation_upload.fetch("with").fetch("include-hidden-files"), false, "build-activation hidden-file policy")
assert_equal(activation_upload.fetch("with").fetch("overwrite"), false, "build-activation overwrite policy")
apply = action_step(operational_action, "Apply exact authorized control-plane release")
assert_equal(apply.fetch("env").fetch("FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE"), "apply", "apply phase")
assert_equal(
  apply.fetch("env").fetch("FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_ID"),
  "${{ steps.operational-report-upload.outputs.artifact-id }}",
  "apply artifact id proof",
)
assert_equal(
  apply.fetch("env").fetch("FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_DIGEST"),
  "${{ steps.operational-report-upload.outputs.artifact-digest }}",
  "apply artifact digest proof",
)
assert_equal(
  apply.fetch("env").fetch("FUGUE_RELEASE_DOMAIN_OPERATIONAL_ARTIFACT_URL"),
  "${{ steps.operational-report-upload.outputs.artifact-url }}",
  "apply artifact URL proof",
)
assert_equal(
  apply.fetch("env").fetch("FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_ID"),
  "${{ steps.image-activation-report-upload.outputs.artifact-id }}",
  "apply build-activation artifact id proof",
)
assert_equal(
  apply.fetch("env").fetch("FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_DIGEST"),
  "${{ steps.image-activation-report-upload.outputs.artifact-digest }}",
  "apply build-activation artifact digest proof",
)
assert_equal(
  apply.fetch("env").fetch("FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_ARTIFACT_URL"),
  "${{ steps.image-activation-report-upload.outputs.artifact-url }}",
  "apply build-activation artifact URL proof",
)
assert_equal(apply.fetch("run"), "./scripts/upgrade_fugue_control_plane.sh", "apply entrypoint")
deploy_uploads = Array(deploy["steps"]).select { |candidate| candidate["uses"].to_s.start_with?("actions/upload-artifact@") }
assert_equal(deploy_uploads.length, 1, "outer deploy artifact upload count")

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
assert_equal(record.fetch("steps").length, 2, "record baseline exact step inventory")
record_checkout = record.fetch("steps").first
assert_equal(record_checkout.fetch("name"), "Checkout", "record baseline checkout position")
assert_equal(record_checkout.fetch("with").fetch("persist-credentials"), false, "record baseline checkout credentials")
advance = step(record, "Advance dedicated forward-only release baseline branch")
assert_equal(record.fetch("steps").last.fetch("name"), advance.fetch("name"), "record baseline writer position")
assert_equal(
  advance.fetch("env").fetch("EXPECTED_BASE_REF_OBJECT"),
  "${{ needs.release-baseline.outputs.baseline_ref_object_sha }}",
  "record baseline ref-object input",
)
advance_run = advance.fetch("run")
common_advance_fragments = [
  "readonly baseline_ref='refs/heads/fugue-control-plane-release-baseline'",
  'git ls-remote --refs --exit-code origin "${baseline_ref}"',
  '"${remote_object}" == "${EXPECTED_BASE_REF_OBJECT}"',
  'git merge-base --is-ancestor "${EXPECTED_BASE_SHA}" "${TARGET_SHA}"',
  'beforeOid:$beforeOid',
  'afterOid:$afterOid',
  "-F 'force=false'",
  '-f "beforeOid=${EXPECTED_BASE_REF_OBJECT}"',
  "settled='false'",
  "settled='true'",
  '[[ "${settled}" == \'true\' ]] || exit 1',
  "response_exact='false'",
  '"${mutation_status}" == \'0\' && "${echoed}" == "${mutation_id}"',
]
common_advance_fragments.each do |fragment|
  fail_contract("baseline advancement is missing #{fragment.inspect}") unless advance_run.include?(fragment)
end

carrier_recorder = advance_run.include?("readonly metadata_path='fugue-runtime-baseline.json'")
if carrier_recorder
  carrier_fragments = [
    '"${EXPECTED_BASE_REF_OBJECT}" =~ ^[0-9a-f]{40}$',
    '"${TARGET_SHA}" == "${GITHUB_SHA}"',
    '"${EXPECTED_BASE_REF_OBJECT}" != "${EXPECTED_BASE_SHA}"',
    '"${represented_runtime}" == "${EXPECTED_BASE_SHA}"',
    '"${represented_parent}" == "${represented_previous}"',
    'if payload != expected:',
    'carrier_date="$(git show -s --format=%cI "${TARGET_SHA}"',
    '"previous_baseline_object_sha": sys.argv[1]',
    '"runtime_sha": sys.argv[2]',
    'blob_sha="$(git hash-object -w --stdin',
    '"repos/${GITHUB_REPOSITORY}/git/blobs"',
    '--input "${object_tmp}/blob-request.json"',
    '"repos/${GITHUB_REPOSITORY}/git/blobs/${blob_sha}"',
    'response.get("sha") != sys.argv[3]',
    'tree_sha="$(git mktree',
    '"repos/${GITHUB_REPOSITORY}/git/trees"',
    '--input "${object_tmp}/tree-request.json"',
    '"repos/${GITHUB_REPOSITORY}/git/trees/${tree_sha}"',
    'response.get("truncated") is not False',
    'carrier_message="fugue runtime baseline carrier ${TARGET_SHA}"',
    ').encode("utf-8") + message.encode("utf-8")',
    'carrier_sha="$(git hash-object -t commit --stdin',
    '"repos/${GITHUB_REPOSITORY}/git/commits"',
    '--input "${object_tmp}/commit-request.json"',
    '"repos/${GITHUB_REPOSITORY}/git/commits/${carrier_sha}"',
    'response.get("message") != request["message"]',
    'len(parents) != 1 or parents[0].get("sha") != sys.argv[5]',
    'for field in ("author", "committer"):',
    'before_cas_status=0',
    '"${before_cas_object}" == "${EXPECTED_BASE_REF_OBJECT}" ]] || exit 1',
    'rm -rf "${object_tmp}" || exit 1',
    'trap - EXIT',
    '-f "afterOid=${carrier_sha}"',
    '"${observe_status}" == \'0\' && "${observed}" == "${carrier_sha}"',
    'baseline carrier CAS settled by exact bounded readback',
    '"${response_exact}" "${carrier_sha}" >&2 || true',
  ]
  carrier_fragments.each do |fragment|
    fail_contract("carrier baseline advancement is missing #{fragment.inspect}") unless advance_run.include?(fragment)
  end
  carrier_lines = advance_run.lines.map(&:strip)
  guard_line = "[[ \"${before_cas_status}\" == '0' && \"${before_cas_object}\" == \"${EXPECTED_BASE_REF_OBJECT}\" ]] || exit 1"
  cleanup_line = 'rm -rf "${object_tmp}" || exit 1'
  clear_trap_line = "trap - EXIT"
  mutation_core = '-f "beforeOid=${EXPECTED_BASE_REF_OBJECT}" -f "afterOid=${carrier_sha}" -F \'force=false\''
  mutation_line = mutation_core + ' \\'
  ordered_lines = [guard_line, cleanup_line, clear_trap_line, mutation_line]
  ordered_positions = ordered_lines.map do |expected_line|
    matches = carrier_lines.each_index.select { |index| carrier_lines.fetch(index) == expected_line }
    fail_contract("carrier baseline writer exact executable line #{expected_line.inspect} occurs #{matches.length} times") unless matches.length == 1
    matches.fetch(0)
  end
  fail_contract("carrier baseline writer old-OID guard and scratch cleanup are not strictly before its unique mutation") unless ordered_positions.each_cons(2).all? { |left, right| left < right }
  assert_equal(advance_run.scan("gh api").length, 10, "carrier baseline writer API count")
  assert_equal(advance_run.scan("gh api graphql").length, 2, "carrier baseline writer GraphQL count")
  assert_equal(advance_run.scan("--method POST").length, 3, "carrier object POST count")
  assert_equal(advance_run.scan("updateRefs(").length, 1, "carrier baseline writer mutation count")
  assert_equal(advance_run.scan("-F 'force=false'").length, 1, "carrier baseline writer force policy count")
  for forbidden in [
    '-f "afterOid=${TARGET_SHA}"', "--method PATCH", "--method DELETE",
  ]
    fail_contract("carrier baseline writer contains out-of-scope capability #{forbidden.inspect}") if advance_run.include?(forbidden)
  end
else
  legacy_fragments = [
    '"${EXPECTED_BASE_REF_OBJECT}" == "${EXPECTED_BASE_SHA}"',
    '-f "afterOid=${TARGET_SHA}"',
    '"${observe_status}" == \'0\' && "${observed}" == "${TARGET_SHA}"',
    'baseline CAS settled by exact bounded readback (transport_status=%s response_exact=%s)',
    '"${mutation_status}" "${response_exact}" >&2 || true',
  ]
  legacy_fragments.each do |fragment|
    fail_contract("legacy baseline advancement is missing #{fragment.inspect}") unless advance_run.include?(fragment)
  end
  assert_equal(advance_run.scan("gh api").length, 3, "legacy baseline writer API count")
  assert_equal(advance_run.scan("gh api graphql").length, 2, "legacy baseline writer GraphQL count")
  assert_equal(advance_run.scan("updateRefs(").length, 1, "legacy baseline writer mutation count")
  assert_equal(advance_run.scan("-F 'force=false'").length, 1, "legacy baseline writer force policy count")
  fail_contract("legacy baseline writer must not expose an object POST path") if advance_run.include?("--method")
end

for forbidden in [
  "refs/tags/", "git push", "git update-ref", "--force-with-lease",
  " -X ", "createRef", "deleteRef", "force=true", "curl ", "wget ",
]
  fail_contract("baseline writer contains out-of-scope capability #{forbidden.inspect}") if advance_run.include?(forbidden)
end

success_rearm = jobs.fetch("rearm-release-lane-on-success")
success_rearm_needs = [
  "release-input-guard", "release-baseline", "release-gate", "build", "deploy", "record-release-baseline",
]
assert_equal(needs(success_rearm), success_rearm_needs, "successful lane rearm dependencies")
success_rearm_needs.each do |job_name|
  fail_contract("successful lane rearm condition omits #{job_name}") unless success_rearm.fetch("if").include?("needs.#{job_name}.result == 'success'")
end
assert_equal(success_rearm["permissions"], {"actions" => "write", "contents" => "read"}, "successful lane rearm permissions")
assert_equal(success_rearm.fetch("steps").length, 2, "successful lane rearm exact step inventory")
success_rearm_step = step(success_rearm, "Disable successful release lane with exact readback")
for fragment in [
  '"${EXPECTED_SHA}" == "${GITHUB_SHA}"',
  '"${main_head}" == "${EXPECTED_SHA}"',
  "git/ref/heads/fugue-control-plane-release-baseline",
  "for run_status in queued in_progress waiting pending requested",
  "actions/workflows/${workflow_id}/runs?status=${run_status}",
  '"${state_before}" == \'active\'',
  "actions/workflows/${workflow_id}/disable",
  "mutation_status=$?",
  "for attempt in 1 2 3 4 5",
  '"${state_after}" == \'disabled_manually\'',
  '"${settled}" == \'true\'',
  '"rearm_ref_mutation_attempted": False',
  '"rearm_runtime_mutation_attempted": False',
  '"rearm_cluster_mutation_attempted": False',
  '"rearm_production_write": False',
]
  fail_contract("successful lane rearm is missing #{fragment.inspect}") unless success_rearm_step.fetch("run").include?(fragment)
end
for forbidden in [
  "/enable", "/dispatches", "/cancel", "git push", "git update-ref", "updateRefs", "createRef", "deleteRef",
  "--method POST", "--method PATCH", "--method DELETE", "helm ", "kubectl ", "k3s kubectl", "fugue app ",
]
  fail_contract("successful lane rearm contains out-of-scope capability #{forbidden.inspect}") if success_rearm_step.fetch("run").include?(forbidden)
end
success_rearm_upload = step(success_rearm, "Upload successful release lane rearm evidence")
assert_equal(
  success_rearm_upload.fetch("uses"),
  "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
  "successful lane rearm artifact action",
)
assert_equal(success_rearm_upload.fetch("with").fetch("if-no-files-found"), "error", "successful lane rearm absent artifact policy")

freeze = jobs.fetch("freeze-release-lane-on-failure")
freeze_needs = [
  "release-input-guard", "release-baseline", "release-gate", "build", "deploy", "record-release-baseline",
  "rearm-release-lane-on-success",
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
  "rearm-release-lane-on-success" => {"actions" => "write", "contents" => "read"},
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
all_uploads.insert(
  1,
  ["deploy", operational_upload.fetch("with").fetch("path")],
)
all_uploads.insert(
  2,
  ["deploy", activation_upload.fetch("with").fetch("path")],
)
allowed_uploads = [
  ["deploy", "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json"],
  ["deploy", "${{ env.FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE }}"],
  ["deploy", "${{ env.FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR }}"],
  ["rearm-release-lane-on-success", "${{ runner.temp }}/fugue-release-lane-success-rearm/success-rearm.json"],
  ["freeze-release-lane-on-failure", "${{ runner.temp }}/fugue-release-lane-freeze/lane-freeze.json"],
]
assert_equal(all_uploads, allowed_uploads, "public artifact allowlist")
fail_contract("workflow must never upload a private release directory") if source.include?("path: ${{ runner.temp }}/fugue-release\n")
fail_contract("workflow must not enable itself") if source.include?("actions/workflows/${workflow_id}/enable")

puts "release-domain workflow contract passed"
RUBY
