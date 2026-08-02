#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKFLOW_FILE="${REPO_ROOT}/.github/workflows/deploy-control-plane.yml"
OPERATIONAL_ACTION_FILE="${REPO_ROOT}/.github/actions/operational-domain-guarded-deploy/action.yml"
ADOPTION_WORKFLOW_FILE="${REPO_ROOT}/.github/workflows/adopt-public-data-plane-helm-baseline.yml"
RECOVERY_WORKFLOW_FILE="${REPO_ROOT}/.github/workflows/recover-public-data-plane-helm-adoption.yml"

ruby - "${WORKFLOW_FILE}" "${OPERATIONAL_ACTION_FILE}" "${ADOPTION_WORKFLOW_FILE}" "${RECOVERY_WORKFLOW_FILE}" <<'RUBY'
require "yaml"

workflow_path = ARGV.fetch(0)
operational_action_path = ARGV.fetch(1)
adoption_workflow_path = ARGV.fetch(2)
recovery_workflow_path = ARGV.fetch(3)
source = File.read(workflow_path, encoding: "UTF-8")
workflow = YAML.safe_load(source, aliases: false)
operational_action = YAML.safe_load(File.read(operational_action_path, encoding: "UTF-8"), aliases: false)
adoption_workflow = YAML.safe_load(File.read(adoption_workflow_path, encoding: "UTF-8"), aliases: false)
recovery_workflow = YAML.safe_load(File.read(recovery_workflow_path, encoding: "UTF-8"), aliases: false)

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

def assert_setup_go_before_build(workflow, job_name, build_name, label)
  job = workflow.fetch("jobs").fetch(job_name)
  steps = job.fetch("steps")
  setup_matches = steps.each_index.select { |index| steps.fetch(index)["name"] == "Setup Go" }
  build_matches = steps.each_index.select { |index| steps.fetch(index)["name"] == build_name }
  fail_contract("#{label} must contain exactly one Setup Go step") unless setup_matches.length == 1
  fail_contract("#{label} must contain exactly one #{build_name.inspect} step") unless build_matches.length == 1
  setup_index = setup_matches.fetch(0)
  build_index = build_matches.fetch(0)
  fail_contract("#{label} Setup Go must precede the build") unless setup_index < build_index
  setup = steps.fetch(setup_index)
  assert_equal(setup.keys, ["name", "uses", "with"], "#{label} Setup Go key inventory")
  assert_equal(
    setup.fetch("uses"),
    "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16",
    "#{label} Setup Go action pin",
  )
  assert_equal(
    setup.fetch("with"),
    {"go-version-file" => "go.mod", "cache" => false},
    "#{label} Setup Go inputs",
  )
end

adoption_trigger = adoption_workflow["on"] || adoption_workflow[true]
assert_equal(adoption_trigger.keys, ["workflow_dispatch"], "Stage1 trigger set")
assert_equal(adoption_trigger.fetch("workflow_dispatch").fetch("inputs").keys, ["expected_sha", "dry_run"], "Stage1 input set")
assert_equal(adoption_workflow.fetch("permissions"), {"contents" => "read"}, "Stage1 permissions")
assert_equal(adoption_workflow.fetch("concurrency"), {"group" => "fugue-production-cluster-mutation-v1", "cancel-in-progress" => false}, "Stage1 concurrency")
assert_equal(adoption_workflow.fetch("jobs").keys, ["adopt"], "Stage1 job set")
assert_setup_go_before_build(adoption_workflow, "adopt", "Build typed adoption tools", "Stage1")
stage1_upload = step(adoption_workflow.fetch("jobs").fetch("adopt"), "Publish immutable Stage1 handoff")
assert_equal(stage1_upload.fetch("with").fetch("if-no-files-found"), "error", "Stage1 artifact missing-file policy")

recovery_trigger = recovery_workflow["on"] || recovery_workflow[true]
assert_equal(recovery_trigger.keys, ["workflow_dispatch"], "Stage1 recovery trigger set")
assert_equal(
  recovery_trigger.fetch("workflow_dispatch").fetch("inputs").keys,
  ["expected_source_sha", "expected_wal_digest", "origin_run_id", "confirm_recovery"],
  "Stage1 recovery input set",
)
assert_equal(recovery_workflow.fetch("permissions"), {"contents" => "read"}, "Stage1 recovery top-level permissions")
assert_equal(recovery_workflow.fetch("concurrency"), {"group" => "fugue-production-cluster-mutation-v1", "cancel-in-progress" => false}, "Stage1 recovery concurrency")
assert_equal(recovery_workflow.fetch("jobs").keys, ["recover"], "Stage1 recovery job set")
recovery_job = recovery_workflow.fetch("jobs").fetch("recover")
assert_equal(recovery_job.fetch("permissions"), {"actions" => "read", "contents" => "read"}, "Stage1 recovery job permissions")
assert_equal(recovery_job.fetch("if"), "${{ inputs.confirm_recovery }}", "Stage1 recovery default-off guard")
assert_setup_go_before_build(recovery_workflow, "recover", "Build typed recovery tools", "Stage1 recovery")
recovery_identity = step(recovery_job, "Verify exact recovery identity")
recovery_execute = step(recovery_job, "Recover or finalize the durable Stage1 transaction")
recovery_upload = step(recovery_job, "Publish recovered Stage1 handoff")
assert_equal(recovery_identity.fetch("id"), "identity", "Stage1 recovery identity outcome ID")
fail_contract("Stage1 recovery lineage bound must allow exactly six recovery commits") unless recovery_identity.fetch("run").include?('"${commit_count}" -le 6')
fail_contract("Stage1 recovery lineage retains an obsolete narrower bound") if recovery_identity.fetch("run").match?(/"\$\{commit_count\}" -le [12345]/)
assert_equal(recovery_execute.fetch("id"), "recover", "Stage1 recovery execution outcome ID")
assert_equal(
  recovery_upload.fetch("if"),
  "${{ always() && steps.identity.outcome == 'success' && steps.recover.outcome != 'skipped' }}",
  "Stage1 recovery artifact preflight/run condition",
)
assert_equal(recovery_upload.fetch("with").fetch("if-no-files-found"), "error", "Stage1 recovery artifact missing-file policy")
fail_contract("Stage1 recovery terminal artifact is missing") unless recovery_upload.fetch("with").fetch("path").include?("terminal-wal.json")

trigger = workflow["on"] || workflow[true]
fail_contract("workflow trigger is missing") unless trigger.is_a?(Hash)
assert_equal(trigger.keys, ["workflow_dispatch"], "release must be dispatch-only")
dispatch = trigger.fetch("workflow_dispatch")
inputs = dispatch.fetch("inputs")
assert_equal(
  inputs.keys,
  ["expected_sha", "target_sha", "image_cache_convergence", "convergence_source_run_id", "public_data_plane_adoption_run_id", "public_data_plane_adoption_baseline_digest"],
  "dispatch input set",
)
expected_sha = inputs.fetch("expected_sha")
assert_equal(expected_sha["required"], true, "expected_sha required flag")
assert_equal(expected_sha["type"], "string", "expected_sha type")
fail_contract("expected_sha must not have a default") if expected_sha.key?("default")
target_sha = inputs.fetch("target_sha")
assert_equal(target_sha["required"], true, "target_sha required flag")
assert_equal(target_sha["type"], "string", "target_sha type")
fail_contract("target_sha must not have a default") if target_sha.key?("default")
image_cache_convergence = inputs.fetch("image_cache_convergence")
assert_equal(image_cache_convergence["required"], true, "image-cache convergence required flag")
assert_equal(image_cache_convergence["type"], "boolean", "image-cache convergence type")
assert_equal(image_cache_convergence["default"], false, "image-cache convergence default")
convergence_source = inputs.fetch("convergence_source_run_id")
assert_equal(convergence_source["required"], false, "convergence source required flag")
assert_equal(convergence_source["type"], "string", "convergence source type")
assert_equal(convergence_source["default"], "", "convergence source default")
for name in ["public_data_plane_adoption_run_id", "public_data_plane_adoption_baseline_digest"]
  handoff_input = inputs.fetch(name)
  assert_equal(handoff_input["required"], false, "#{name} required flag")
  assert_equal(handoff_input["type"], "string", "#{name} type")
  assert_equal(handoff_input["default"], "", "#{name} default")
end

assert_equal(workflow["permissions"], {"contents" => "read"}, "top-level permissions")
jobs = workflow.fetch("jobs")

guard = jobs.fetch("release-input-guard")
assert_equal(needs(guard), [], "input guard dependencies")
assert_equal(guard.fetch("permissions"), {"actions" => "read", "contents" => "read"}, "input guard permissions")
stage1_handoff = step(guard, "Download exact public data-plane Stage1 handoff")
assert_equal(stage1_handoff.fetch("if"), "${{ inputs.public_data_plane_adoption_run_id != '' }}", "Stage1 handoff condition")
assert_equal(stage1_handoff.fetch("with").fetch("run-id"), "${{ inputs.public_data_plane_adoption_run_id }}", "Stage1 handoff run")
download_authorization = step(guard, "Download convergence successor authorization")
assert_equal(download_authorization.fetch("if"), "${{ inputs.image_cache_convergence }}", "convergence authorization condition")
assert_equal(
  download_authorization.fetch("uses"),
  "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
  "convergence authorization action pin",
)
assert_equal(
  download_authorization.fetch("with"),
  {
    "name" => "fugue-release-convergence-successor-${{ inputs.convergence_source_run_id }}-1",
    "path" => "${{ runner.temp }}/fugue-release-convergence-authorization",
    "github-token" => "${{ github.token }}",
    "repository" => "${{ github.repository }}",
    "run-id" => "${{ inputs.convergence_source_run_id }}",
  },
  "convergence authorization download contract",
)
guard_step = step(guard, "Guard exact main commit authorization")
{
  "EXPECTED_SHA" => "${{ inputs.expected_sha }}",
  "ACTUAL_SHA" => "${{ github.sha }}",
  "TARGET_SHA" => "${{ inputs.target_sha }}",
  "IMAGE_CACHE_CONVERGENCE" => "${{ inputs.image_cache_convergence && 'true' || 'false' }}",
  "CONVERGENCE_SOURCE_RUN_ID" => "${{ inputs.convergence_source_run_id }}",
  "CONVERGENCE_AUTHORIZATION_FILE" => "${{ runner.temp }}/fugue-release-convergence-authorization/successor.json",
  "GH_TOKEN" => "${{ github.token }}",
  "REPOSITORY" => "${{ github.repository }}",
  "EVENT_NAME" => "${{ github.event_name }}",
  "EVENT_REF" => "${{ github.ref }}",
  "EVENT_REF_NAME" => "${{ github.ref_name }}",
  "EVENT_REF_TYPE" => "${{ github.ref_type }}",
  "PUBLIC_DATA_PLANE_ADOPTION_RUN_ID" => "${{ inputs.public_data_plane_adoption_run_id }}",
  "PUBLIC_DATA_PLANE_ADOPTION_BASELINE_DIGEST" => "${{ inputs.public_data_plane_adoption_baseline_digest }}",
  "PUBLIC_DATA_PLANE_ADOPTION_BASELINE" => "${{ runner.temp }}/public-data-plane-stage1-handoff/stage1-baseline.json",
  "PUBLIC_DATA_PLANE_ADOPTION_TRACE" => "${{ runner.temp }}/public-data-plane-stage1-handoff/execution-trace.json",
}.each do |name, expected|
  assert_equal(guard_step.fetch("env").fetch(name), expected, "guard #{name} source")
end
for fragment in [
  '"${EVENT_REF}" == "refs/heads/main"',
  '"${EVENT_REF_NAME}" == "main"',
  '"${EVENT_REF_TYPE}" == "branch"',
  '^[0-9a-f]{40}$',
  '"${EXPECTED_SHA}" == "${ACTUAL_SHA}"',
  '"${TARGET_SHA}" =~ ^[0-9a-f]{40}$',
  'repos/${REPOSITORY}/git/ref/heads/main',
  '"${remote_main}" == "${EXPECTED_SHA}"',
  'false)',
  '[[ -z "${CONVERGENCE_SOURCE_RUN_ID}" ]]',
  'true)',
  'actions/runs/${CONVERGENCE_SOURCE_RUN_ID}',
  '"${source_status}" == \'completed\' && "${source_conclusion}" == \'success\'',
  'pending_activation_artifacts",',
  'source_image_cache_artifact',
  'source_image_cache_artifacts_digest',
  '"schema_version": 2',
  '"successor_run_id": successor_run_id',
  'if raw != canonical:',
  'actions/runs/${PUBLIC_DATA_PLANE_ADOPTION_RUN_ID}',
  '"${stage1_status}" == completed && "${stage1_conclusion}" == success',
  'events[-1].get("phase") == "lease-released"',
  'if [[ -z "${PUBLIC_DATA_PLANE_ADOPTION_RUN_ID}" && -z "${PUBLIC_DATA_PLANE_ADOPTION_BASELINE_DIGEST}" ]]',
  '"${PUBLIC_DATA_PLANE_ADOPTION_RUN_ID}" =~ ^[1-9][0-9]*$',
  '"${PUBLIC_DATA_PLANE_ADOPTION_BASELINE_DIGEST}" =~ ^sha256:[0-9a-f]{64}$',
]
  fail_contract("input guard is missing #{fragment.inspect}") unless guard_step.fetch("run").include?(fragment)
end

baseline = jobs.fetch("release-baseline")
assert_equal(needs(baseline), ["release-input-guard"], "release-baseline dependencies")
assert_equal(baseline.fetch("permissions"), {"actions" => "read", "contents" => "read"}, "release-baseline permissions")
for job_name in ["release-baseline", "release-gate", "build", "deploy", "record-release-baseline"]
  checkout = step(jobs.fetch(job_name), "Checkout")
  assert_equal(checkout.fetch("with").fetch("ref"), "${{ inputs.target_sha }}", "#{job_name} target checkout")
end
stage1_planner_gate = step(baseline, "Verify Stage1 handoff before release planning")
for fragment in ["canonicalize-secret-free", "verify-stage2"]
  fail_contract("Stage1 planner gate is missing #{fragment.inspect}") unless stage1_planner_gate.fetch("run").include?(fragment)
end
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
  resolver.fetch("env", {}),
  {"SOURCE_SHA" => "${{ inputs.expected_sha }}", "TARGET_SHA" => "${{ inputs.target_sha }}"},
  "forward baseline resolver environment",
)
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
  '[[ "${domain_base_sha}" != "${target_sha}" ]] || exit 1',
  'git merge-base --is-ancestor "${target_sha}" "${SOURCE_SHA}"',
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
assert_equal(build["permissions"], {"actions" => "read", "contents" => "read", "packages" => "write"}, "build permissions")
build_authorization = step(build, "Download convergence image artifact authorization")
assert_equal(build_authorization.fetch("if"), "${{ inputs.image_cache_convergence }}", "build convergence authorization condition")
assert_equal(build_authorization.fetch("uses"), "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c", "build convergence authorization pin")
assert_equal(
  build_authorization.fetch("with"),
  download_authorization.fetch("with"),
  "build convergence authorization download contract",
)
build_plan = step(build, "Compute image build plan")
assert_equal(
  build_plan.fetch("env").fetch("FUGUE_RELEASE_IMAGE_CACHE_CONVERGENCE"),
  "${{ inputs.image_cache_convergence && 'true' || 'false' }}",
  "build convergence plan input",
)
build_provenance = step(build, "Publish verified control-plane image provenance")
{
  "FUGUE_IMAGE_CACHE_IMAGE_BASE_REF" => "${{ needs.release-baseline.outputs.image_cache_image_baseline_ref }}",
  "FUGUE_CONTROL_PLANE_IMAGE_REUSE_AUTHORIZATION_FILE" => "${{ inputs.image_cache_convergence && format('{0}/fugue-release-convergence-authorization/successor.json', runner.temp) || '' }}",
  "FUGUE_CONVERGENCE_SOURCE_RUN_ID" => "${{ inputs.convergence_source_run_id }}",
}.each do |name, expected|
  assert_equal(build_provenance.fetch("env").fetch(name), expected, "build provenance #{name}")
end

deploy = jobs.fetch("deploy")
stage1_deploy_gate = step(deploy, "Reverify Stage1 handoff at deploy prewrite")
for fragment in ["canonicalize-secret-free", "verify-stage2"]
  fail_contract("Stage1 deploy gate is missing #{fragment.inspect}") unless stage1_deploy_gate.fetch("run").include?(fragment)
end
assert_equal(
  needs(deploy),
  ["release-input-guard", "release-baseline", "release-gate", "build"],
  "deploy authorization dependencies",
)
assert_equal(
  deploy.fetch("if"),
  "${{ always() && needs.release-input-guard.result == 'success' && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' }}",
  "deploy authorization condition",
)
assert_equal(deploy["permissions"], {"actions" => "read", "contents" => "read"}, "deploy permissions")
assert_equal(deploy["continue-on-error"], nil, "deploy continue-on-error")
setup_go = step(deploy, "Setup Go")
assert_equal(setup_go["uses"], "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16", "deploy setup-go pin")
assert_equal(setup_go["with"], {"go-version-file" => "go.mod", "cache" => false}, "deploy setup-go persistent-runner cache policy")
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
  './cmd/fugue-public-data-plane-adoption',
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
  "Upload release-domain public evidence" => "${{ always() && (steps.genesis_evidence.outcome == 'success' || steps.guarded_deploy.outcome == 'success') }}",
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
assert_equal(upgrade["id"], "guarded_deploy", "guarded deploy step id")
assert_equal(upgrade["uses"], "./.github/actions/operational-domain-guarded-deploy", "guarded deploy action")
fail_contract("guarded deploy workflow step must not define a run body") if upgrade.key?("run")
genesis_evidence = step(deploy, "Write genesis public release evidence")
assert_equal(genesis_evidence["id"], "genesis_evidence", "genesis evidence id")
upgrade_env = upgrade.fetch("env")
{
  "GITHUB_SHA" => "${{ inputs.target_sha }}",
  "FUGUE_PUBLIC_DATA_PLANE_RELEASE_MODE" => "preserve",
  "FUGUE_RELEASE_DOMAIN_BASE_SHA" => "${{ needs.release-baseline.outputs.domain_base_sha }}",
  "FUGUE_RELEASE_DOMAIN_TARGET_SHA" => "${{ inputs.target_sha }}",
  "FUGUE_RELEASE_DOMAIN_EVIDENCE_TOOL" => "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-evidence",
  "FUGUE_RELEASE_DOMAIN_DISPATCH_TOOL" => "${{ runner.temp }}/fugue-release-tools/fugue-release-domain-dispatch",
  "FUGUE_RELEASE_DOMAIN_PUBLIC_EVIDENCE_FILE" => "${{ runner.temp }}/fugue-release-domain-public/release-domain-evidence.json",
  "FUGUE_RELEASE_DOMAIN_OPERATIONAL_REPORT_FILE" => "${{ runner.temp }}/fugue-release-domain-public/operational-domain-evidence.json",
  "FUGUE_RELEASE_DOMAIN_IMAGE_ACTIVATION_REPORT_DIR" => "${{ runner.temp }}/fugue-release-domain-public/build-activation-evidence",
  "FUGUE_RELEASE_DOMAIN_VERIFIED_IMAGE_ARTIFACTS_DIGEST" => "${{ needs.build.outputs.verified_image_artifacts_digest }}",
  "FUGUE_RELEASE_DOMAIN_IMAGE_TARGETS" => "${{ needs.build.outputs.image_targets }}",
  "FUGUE_RELEASE_IMAGE_CACHE_CONVERGENCE" => "${{ inputs.image_cache_convergence && 'true' || 'false' }}",
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
  "FUGUE_EDGE_ACTIVATION_ENABLED" => "${{ vars.FUGUE_EDGE_ACTIVATION_ENABLED || 'false' }}",
  "FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME" => "${{ vars.FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME || '' }}",
}.each do |name, expected|
  assert_equal(upgrade_env[name], expected, "upgrade #{name}")
end
[
  "FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY",
  "FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY_ID",
  "FUGUE_EDGE_ACTIVATION_PLAN_SIGNING_KEY_GENERATION",
].each do |name|
  fail_contract("deploy workflow must not receive activation key material through #{name}") if upgrade_env.key?(name)
end

public_upload = step(deploy, "Upload release-domain public evidence")
assert_equal(
  public_upload["if"],
  "${{ always() && (steps.genesis_evidence.outcome == 'success' || steps.guarded_deploy.outcome == 'success') }}",
  "public evidence upload condition",
)
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
assert_equal(
  operational_action.fetch("outputs"),
  {
    "image-activation-convergence" => {
      "description" => "complete only when no mandatory image-cache build remains build-only",
      "value" => "${{ steps.image-activation-convergence.outputs.status }}",
    },
    "pending-activation-artifacts" => {
      "description" => "canonical comma-separated mandatory image-cache built-only artifacts",
      "value" => "${{ steps.image-activation-convergence.outputs.pending_artifacts }}",
    },
  },
  "operational action outputs",
)
action_steps = operational_action.fetch("runs").fetch("steps")
assert_equal(
  action_steps.map { |candidate| candidate.fetch("name") },
  [
    "Prepare operational-domain report-only evidence",
    "Upload operational-domain report-only evidence",
    "Upload build-vs-activation report-only evidence",
    "Apply exact authorized control-plane release",
    "Verify image activation convergence",
  ],
  "operational action step order",
)
prepare = action_step(operational_action, "Prepare operational-domain report-only evidence")
assert_equal(prepare["id"], "prepare", "prepare id")
assert_equal(prepare.fetch("env").fetch("FUGUE_RELEASE_DOMAIN_OPERATIONAL_PHASE"), "prepare", "prepare phase")
assert_equal(prepare.fetch("run"), "./scripts/upgrade_fugue_control_plane.sh", "prepare entrypoint")
operational_upload = action_step(operational_action, "Upload operational-domain report-only evidence")
assert_equal(operational_upload["id"], "operational-report-upload", "operational upload id")
assert_equal(
  operational_upload["if"],
  "${{ always() && steps.prepare.outcome == 'success' }}",
  "operational report upload condition",
)
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
assert_equal(
  activation_upload["if"],
  "${{ always() && steps.prepare.outcome == 'success' }}",
  "build-activation upload condition",
)
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
convergence = action_step(operational_action, "Verify image activation convergence")
assert_equal(convergence["id"], "image-activation-convergence", "image convergence step id")
for fragment in [
  'image-activation-convergence',
  '--build-artifact-plan "${evidence_dir}/build-artifact-plan.json"',
  '--image-activation-plan "${evidence_dir}/image-activation-plan.json"',
  '--image-activation-evidence "${evidence_dir}/image-activation-evidence.json"',
  "complete)",
  "pending)",
  "printf 'status=%s\\n'",
  "printf 'pending_artifacts=%s\\n'",
]
  fail_contract("image convergence step is missing #{fragment.inspect}") unless convergence.fetch("run").include?(fragment)
end
deploy_uploads = Array(deploy["steps"]).select { |candidate| candidate["uses"].to_s.start_with?("actions/upload-artifact@") }
assert_equal(deploy_uploads.length, 1, "outer deploy artifact upload count")

continuation = jobs.fetch("continue-release-convergence")
assert_equal(
  needs(continuation),
  ["release-input-guard", "release-baseline", "release-gate", "build", "deploy"],
  "release convergence continuation dependencies",
)
assert_equal(continuation["permissions"], {"actions" => "write", "contents" => "read"}, "release convergence continuation permissions")
for fragment in [
  "needs.deploy.outputs.image_activation_convergence == 'pending'",
  "needs.deploy.result == 'success'",
]
  fail_contract("release convergence continuation condition omits #{fragment.inspect}") unless continuation.fetch("if").include?(fragment)
end
successor = step(continuation, "Dispatch exact release convergence successor")
for fragment in [
  '"${EXPECTED_SHA}" == "${GITHUB_SHA}"',
  '"${PENDING_ACTIVATION_ARTIFACTS}" == \'image_cache\'',
  '"${state}" == \'active\'',
  '"${main_head}" == "${EXPECTED_SHA}"',
  '[[ -z "${before}" ]] || exit 1',
  'actions/workflows/${workflow_id}/dispatches',
  '-f "inputs[expected_sha]=${main_head}"',
  '-f "inputs[target_sha]=${TARGET_SHA}"',
  "-f 'inputs[image_cache_convergence]=true'",
  '-f "inputs[convergence_source_run_id]=${GITHUB_RUN_ID}"',
  'successor_number > GITHUB_RUN_NUMBER',
  '"${successor_sha}" == "${main_head}"',
  '"schema_version": 2',
  '"source_image_cache_artifact": image_cache_artifact',
  '"source_image_cache_artifacts_digest": bound_digest',
  '"baseline_advanced": False',
  '"workflow_dispatch_attempted": True',
]
  fail_contract("release convergence successor is missing #{fragment.inspect}") unless successor.fetch("run").include?(fragment)
end
for forbidden in ["/enable", "/disable", "/cancel", "git push", "updateRefs", "helm ", "kubectl "]
  fail_contract("release convergence successor contains out-of-scope capability #{forbidden.inspect}") if successor.fetch("run").include?(forbidden)
end
continuation_upload = step(continuation, "Upload release convergence successor evidence")
assert_equal(continuation_upload.fetch("with").fetch("if-no-files-found"), "error", "continuation absent artifact policy")

record = jobs.fetch("record-release-baseline")
assert_equal(
  needs(record),
  ["release-input-guard", "release-baseline", "release-gate", "build", "deploy"],
  "record-release-baseline dependencies",
)
assert_equal(record["permissions"], {"contents" => "write"}, "record-release-baseline permissions")
assert_equal(
  record["if"],
  "${{ always() && needs.release-input-guard.result == 'success' && needs.release-baseline.result == 'success' && needs.release-gate.result == 'success' && needs.build.result == 'success' && needs.deploy.result == 'success' && needs.deploy.outputs.image_activation_convergence == 'complete' }}",
  "record-release-baseline success condition",
)
assert_equal(record.fetch("steps").length, 2, "record baseline exact step inventory")
record_checkout = record.fetch("steps").first
assert_equal(record_checkout.fetch("name"), "Checkout", "record baseline checkout position")
assert_equal(record_checkout.fetch("with").fetch("ref"), "${{ inputs.target_sha }}", "record baseline target checkout")
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
    '"${SOURCE_SHA}" =~ ^[0-9a-f]{40}$ && "${SOURCE_SHA}" == "${GITHUB_SHA}"',
    '"${remote_main}" == "${SOURCE_SHA}"',
    'git merge-base --is-ancestor "${TARGET_SHA}" "${SOURCE_SHA}"',
    '"${EXPECTED_BASE_SHA}" != "${TARGET_SHA}"',
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
    'bounded_git_object_readback() {',
    'for attempt in $(seq 1 15)',
    '"${attempt}" == \'15\' ]] || sleep 2',
    'carrier %s readback did not settle after 15 attempts',
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
  assert_equal(advance_run.scan("gh api").length, 8, "carrier baseline writer API count")
  assert_equal(advance_run.scan("gh api graphql").length, 2, "carrier baseline writer GraphQL count")
  assert_equal(advance_run.scan("--method POST").length, 3, "carrier object POST count")
  assert_equal(advance_run.scan("bounded_git_object_readback").length, 4, "carrier bounded object readback count")
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
  '"${main_head}" =~ ^[0-9a-f]{40}$',
  '"main_matches_release_sha": main_matches == "true"',
  '"observed_main_sha": main_head',
  "git/ref/heads/fugue-control-plane-release-baseline",
  "for run_status in queued in_progress waiting pending requested",
  "actions/workflows/${workflow_id}/runs?status=${run_status}",
  'run_number <= current_run_number or attempt != 1',
  'event != "workflow_dispatch" or branch != "main"',
  'workflow_path != ".github/workflows/deploy-control-plane.yml"',
  '"successor_run_count": len(successors)',
  '"successor_runs": successors',
  '"settlement_mode": settlement_mode',
  '"${state_before}" == \'active\' || "${state_before}" == \'disabled_manually\'',
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
  '[[ "${main_head}" == "${EXPECTED_SHA}" ]] || exit 1',
  '[[ -z "${other_runs}" ]] || exit 1',
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
  "release-input-guard", "release-baseline", "release-gate", "build", "deploy", "continue-release-convergence",
  "record-release-baseline", "rearm-release-lane-on-success",
]
assert_equal(needs(freeze), freeze_needs, "freeze finalizer dependencies")
[
  "release-input-guard", "release-baseline", "release-gate", "build", "deploy",
].each do |job_name|
  fail_contract("freeze condition omits #{job_name}") unless freeze.fetch("if").include?("needs.#{job_name}.result != 'success'")
end
for fragment in [
  "needs.deploy.outputs.image_activation_convergence == 'complete'",
  "needs.record-release-baseline.result != 'success'",
  "needs.rearm-release-lane-on-success.result != 'success'",
  "needs.deploy.outputs.image_activation_convergence == 'pending'",
  "needs.continue-release-convergence.result != 'success'",
  "needs.deploy.outputs.image_activation_convergence != 'complete'",
  "needs.deploy.outputs.image_activation_convergence != 'pending'",
]
  fail_contract("freeze convergence condition omits #{fragment.inspect}") unless freeze.fetch("if").include?(fragment)
end
assert_equal(freeze["permissions"], {"actions" => "write", "contents" => "read"}, "freeze permissions")

allowed_permissions = {
  "release-input-guard" => {"actions" => "read", "contents" => "read"},
  "release-baseline" => {"actions" => "read", "contents" => "read"},
  "build" => {"actions" => "read", "contents" => "read", "packages" => "write"},
  "deploy" => {"actions" => "read", "contents" => "read"},
  "continue-release-convergence" => {"actions" => "write", "contents" => "read"},
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
  ["continue-release-convergence", "${{ runner.temp }}/fugue-release-convergence-successor/successor.json"],
  ["rearm-release-lane-on-success", "${{ runner.temp }}/fugue-release-lane-success-rearm/success-rearm.json"],
  ["freeze-release-lane-on-failure", "${{ runner.temp }}/fugue-release-lane-freeze/lane-freeze.json"],
]
assert_equal(all_uploads, allowed_uploads, "public artifact allowlist")
fail_contract("workflow must never upload a private release directory") if source.include?("path: ${{ runner.temp }}/fugue-release\n")
fail_contract("workflow must not enable itself") if source.include?("actions/workflows/${workflow_id}/enable")

puts "release-domain workflow contract passed"
RUBY
