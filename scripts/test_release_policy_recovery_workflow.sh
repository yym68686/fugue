#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

fail() {
  printf '[test_release_policy_recovery_workflow] ERROR: %s\n' "$*" >&2
  exit 1
}

if [[ "${FUGUE_VERIFY_GITHUB_GRAPHQL_REF_CAS_SCHEMA:-false}" == "true" ]]; then
  [[ -n "${GH_TOKEN:-}" ]] || fail "GH_TOKEN is required for GraphQL ref CAS schema verification"
  graphql_schema_output="$(mktemp "${TMPDIR:-/tmp}/fugue-graphql-ref-schema.XXXXXX")"
  schema_python_pid=''
  forward_schema_signal() {
    local signal_name="$1"
    local exit_code="$2"
    if [[ -n "${schema_python_pid}" ]] && kill -0 "${schema_python_pid}" 2>/dev/null; then
      kill -s "${signal_name}" "${schema_python_pid}" 2>/dev/null || true
      wait "${schema_python_pid}" 2>/dev/null || true
    fi
    rm -f "${graphql_schema_output}"
    trap - INT TERM
    exit "${exit_code}"
  }
  trap 'forward_schema_signal INT 130' INT
  trap 'forward_schema_signal TERM 143' TERM
  python3 - >"${graphql_schema_output}" <<'PY' &
import json
import os
import signal
import subprocess

process = None


def terminate_process_group():
    if process is None or process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.communicate(timeout=2)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.communicate(timeout=2)
    except subprocess.TimeoutExpired as exc:
        raise SystemExit("GraphQL schema query process group could not be reaped") from exc


def forward_termination(signum, _frame):
    terminate_process_group()
    raise SystemExit(f"GraphQL schema query received termination signal {signum}")


signal.signal(signal.SIGINT, forward_termination)
signal.signal(signal.SIGTERM, forward_termination)
termination_signals = {signal.SIGINT, signal.SIGTERM}
previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, termination_signals)
try:
    process = subprocess.Popen(
        [
            "gh",
            "api",
            "graphql",
            "-f",
            'query=query { refUpdate: __type(name:"RefUpdate") { inputFields { name description type { kind name ofType { kind name } } } } mutation: __type(name:"Mutation") { fields { name type { kind name ofType { kind name } } args { name type { kind name ofType { kind name } } } } } updateRefsInput: __type(name:"UpdateRefsInput") { inputFields { name type { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } } updateRefsPayload: __type(name:"UpdateRefsPayload") { fields { name type { kind name ofType { kind name } } } } }',
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
finally:
    signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
try:
    stdout, stderr = process.communicate(timeout=10)
except subprocess.TimeoutExpired as exc:
    terminate_process_group()
    raise SystemExit("GraphQL RefUpdate schema query exceeded 10 seconds") from exc
if process.returncode != 0:
    raise SystemExit(stderr.strip() or "GraphQL RefUpdate schema query failed")
document = json.loads(stdout)
data = document["data"]
fields = {field["name"]: field for field in data["refUpdate"]["inputFields"]}
expected_types = {
    "name": ("NON_NULL", None, "SCALAR", "GitRefname"),
    "afterOid": ("NON_NULL", None, "SCALAR", "GitObjectID"),
    "beforeOid": ("SCALAR", "GitObjectID", None, None),
    "force": ("SCALAR", "Boolean", None, None),
}
if not set(expected_types).issubset(fields):
    raise SystemExit(f"required RefUpdate fields are missing: {sorted(fields)}")
for name, expected in expected_types.items():
    field_type = fields[name]["type"]
    observed = (
        field_type["kind"],
        field_type["name"],
        (field_type.get("ofType") or {}).get("kind"),
        (field_type.get("ofType") or {}).get("name"),
    )
    if observed != expected:
        raise SystemExit(f"RefUpdate.{name} type drifted: {observed!r}")
update_refs_matches = [
    field for field in data["mutation"]["fields"] if field["name"] == "updateRefs"
]
if len(update_refs_matches) != 1:
    raise SystemExit("Mutation.updateRefs field is missing or duplicated")
update_refs = update_refs_matches[0]
input_args = [argument for argument in update_refs["args"] if argument["name"] == "input"]
if len(input_args) != 1:
    raise SystemExit("Mutation.updateRefs argument shape drifted")
input_type = input_args[0]["type"]
if (
    input_type["kind"],
    (input_type.get("ofType") or {}).get("kind"),
    (input_type.get("ofType") or {}).get("name"),
) != ("NON_NULL", "INPUT_OBJECT", "UpdateRefsInput"):
    raise SystemExit("Mutation.updateRefs input is not UpdateRefsInput!")
if (update_refs["type"]["kind"], update_refs["type"]["name"]) != (
    "OBJECT",
    "UpdateRefsPayload",
):
    raise SystemExit("Mutation.updateRefs does not return UpdateRefsPayload")
update_input_fields = {
    field["name"]: field["type"] for field in data["updateRefsInput"]["inputFields"]
}
if not {"repositoryId", "refUpdates", "clientMutationId"}.issubset(update_input_fields):
    raise SystemExit("UpdateRefsInput required fields are missing")
repository_id = update_input_fields["repositoryId"]
if (
    repository_id["kind"],
    repository_id["ofType"]["kind"],
    repository_id["ofType"]["name"],
) != ("NON_NULL", "SCALAR", "ID"):
    raise SystemExit("UpdateRefsInput.repositoryId is not ID!")
client_mutation_id = update_input_fields["clientMutationId"]
if (client_mutation_id["kind"], client_mutation_id["name"]) != ("SCALAR", "String"):
    raise SystemExit("UpdateRefsInput.clientMutationId is not String")
ref_updates = update_input_fields["refUpdates"]
if (
    ref_updates["kind"],
    ref_updates["ofType"]["kind"],
    ref_updates["ofType"]["ofType"]["kind"],
    ref_updates["ofType"]["ofType"]["ofType"]["name"],
) != ("NON_NULL", "LIST", "NON_NULL", "RefUpdate"):
    raise SystemExit("UpdateRefsInput.refUpdates is not [RefUpdate!]!")
payload_fields = {
    field["name"]: field["type"] for field in data["updateRefsPayload"]["fields"]
}
if "clientMutationId" not in payload_fields or (
    payload_fields["clientMutationId"]["kind"],
    payload_fields["clientMutationId"]["name"],
) != ("SCALAR", "String"):
    raise SystemExit("UpdateRefsPayload.clientMutationId is not String")
print("verified")
PY
  schema_python_pid=$!
  if ! wait "${schema_python_pid}"; then
    schema_python_pid=''
    trap - INT TERM
    rm -f "${graphql_schema_output}"
    fail "GitHub GraphQL exact ref CAS schema query failed"
  fi
  schema_python_pid=''
  trap - INT TERM
  graphql_ref_update_schema="$(<"${graphql_schema_output}")"
  rm -f "${graphql_schema_output}"
  [[ "${graphql_ref_update_schema}" == 'verified' ]] ||
    fail "GitHub GraphQL exact ref CAS schema verification did not complete"
fi
if [[ "${FUGUE_GRAPHQL_REF_CAS_SCHEMA_ONLY:-false}" == "true" ]]; then
  [[ "${FUGUE_VERIFY_GITHUB_GRAPHQL_REF_CAS_SCHEMA:-false}" == "true" ]] ||
    fail "schema-only mode requires live GraphQL RefUpdate verification"
  exit 0
fi

ruby -ryaml -ropen3 - "${REPO_ROOT}" <<'RUBY'
repo = ARGV.fetch(0)
deploy = YAML.load_file(File.join(repo, ".github/workflows/deploy-control-plane.yml"))
recovery = YAML.load_file(File.join(repo, ".github/workflows/recover-control-plane-release-policy.yml"))
watchdog = YAML.load_file(File.join(repo, ".github/workflows/watch-control-plane-release-policy-recovery.yml"))

def fail_contract(message)
  warn("release-policy recovery workflow contract: #{message}")
  exit(1)
end

trigger = recovery["on"] || recovery[true]
fail_contract("workflow must be dispatch-only") unless trigger.is_a?(Hash) && trigger.keys == ["workflow_dispatch"]
input = trigger.dig("workflow_dispatch", "inputs", "expected_sha")
fail_contract("expected_sha must be a required string") unless input == {
  "description" => "Exact main commit authorized for release-policy recovery",
  "required" => true,
  "type" => "string",
}
fail_contract("top-level permissions must be empty") unless recovery["permissions"] == {}
fail_contract("recovery must share deploy mutation concurrency") unless recovery["concurrency"] == deploy["concurrency"]
fail_contract("recovery concurrency must not cancel a tag transaction") unless recovery.dig("concurrency", "cancel-in-progress") == false

jobs = recovery.fetch("jobs")
fail_contract("unexpected recovery jobs") unless jobs.keys == [
  "recovery-gate",
  "advance-baseline",
  "compensate-baseline-on-failure",
  "freeze-release-lanes-on-failure",
]
gate = jobs.fetch("recovery-gate")
advance = jobs.fetch("advance-baseline")
compensate = jobs.fetch("compensate-baseline-on-failure")
freeze = jobs.fetch("freeze-release-lanes-on-failure")
fail_contract("gate permissions drifted") unless gate["permissions"] == {"actions" => "read", "contents" => "read"}
fail_contract("writer permissions drifted") unless advance["permissions"] == {"actions" => "write", "contents" => "write"}
fail_contract("compensation permissions drifted") unless compensate["permissions"] == {"contents" => "write"}
fail_contract("freeze permissions drifted") unless freeze["permissions"] == {"actions" => "write", "contents" => "read"}
fail_contract("writer must require production environment") unless advance["environment"] == "production"
fail_contract("automatic compensation must not depend on a second environment approval") if compensate.key?("environment")
{
  "recovery-gate" => 180,
  "advance-baseline" => 60,
  "compensate-baseline-on-failure" => 15,
  "freeze-release-lanes-on-failure" => 45,
}.each do |job_name, timeout|
  fail_contract("#{job_name} timeout drifted") unless jobs.dig(job_name, "timeout-minutes") == timeout
end

gate_checkout = gate.fetch("steps").first
fail_contract("gate checkout must not persist credentials") unless gate_checkout.dig("with", "persist-credentials") == false
gate_run = gate.fetch("steps").find { |step| step["name"] == "Guard external authorization and exact recovery diff" }&.fetch("run", "")
test_run = gate.fetch("steps").find { |step| step["name"] == "Verify recovery workflow and release safety contracts" }&.fetch("run", "")
schema_step = gate.fetch("steps").find { |step| step["name"] == "Verify live GraphQL exact ref CAS schema" }
schema_run = schema_step&.fetch("run", "")
prewrite_run = advance.fetch("steps").find { |step| step["name"] == "Revalidate authorization and quiesce duplicate recovery runs" }&.fetch("run", "")
transaction_run = advance.fetch("steps").find { |step| step["name"] == "Advance, rollback, and re-advance dedicated release baseline" }&.fetch("run", "")
quiesce_run = advance.fetch("steps").find { |step| step["name"] == "Disable recovery lane and prove final quiescence" }&.fetch("run", "")
context_run = advance.fetch("steps").find { |step| step["name"] == "Write recovery context evidence" }&.fetch("run", "")
final_proof_run = advance.fetch("steps").find { |step| step["name"] == "Final remote and lane proof" }&.fetch("run", "")
same_runner = advance.fetch("steps").find { |step| step["name"] == "Same-runner compensation after an incomplete writer" }
same_runner_freeze = advance.fetch("steps").find { |step| step["name"] == "Same-runner freeze after an incomplete writer" }
same_runner_upload = advance.fetch("steps").find { |step| step["name"] == "Upload same-runner compensation evidence" }
transaction_upload = advance.fetch("steps").find { |step| step["name"] == "Upload baseline transaction evidence" }
compensate_step = compensate.fetch("steps").find { |step| step["name"] == "Observe and compensate exact intermediate tag state" }
freeze_step = freeze.fetch("steps").find { |step| step["name"] == "Disable both lanes and cancel every other non-terminal run" }
post_freeze = freeze.fetch("steps").find { |step| step["name"] == "Record post-freeze evidence" }
freeze_upload = freeze.fetch("steps").find { |step| step["name"] == "Upload recovery lane freeze evidence" }
[gate_run, test_run, prewrite_run, transaction_run, quiesce_run, context_run, final_proof_run].each do |run|
  fail_contract("required run block is missing") if run.nil? || run.empty?
end

[
  '[[ "${target_sha}" == "${AUTHORIZED_SHA}" ]]',
  '[[ "${target_parent}" == "${AUTHORIZED_BASE_SHA}" ]]',
  '[[ "${baseline_ref_object}" == "${AUTHORIZED_BASE_SHA}" ]]',
  "git diff --check",
  "timeout --kill-after=2s 10s git ls-remote",
  "timeout --kill-after=2s 10s gh api --paginate",
].each do |required|
  fail_contract("gate is missing #{required.inspect}") unless gate_run.include?(required)
end
expected_changes = gate_run.scan(/^\s+'([^']+)'\s*$/).flatten
fail_contract("recovery changed-file allowlist drifted") unless expected_changes == [
  ".github/workflows/recover-control-plane-release-policy.yml",
  "docs/runbooks/release-policy-recovery.md",
  "scripts/recover_control_plane_release_baseline.py",
  "scripts/test_release_policy_recovery_workflow.sh",
]

[
  "make generate-openapi-check",
  "bash scripts/test_release_domain_workflow.sh",
  "bash scripts/test_release_policy_recovery_workflow.sh",
  "bash scripts/test_release_domain_safety.sh",
  "FUGUE_REQUIRE_NODE_LOCAL_DNS_TEST_DOCKER=true bash scripts/test_node_local_dns_release.sh",
  "go test ./...",
].each do |required|
  fail_contract("recovery test gate is missing #{required}") unless test_run.include?(required)
end
fail_contract("GraphQL schema gate is missing or not schema-only") unless schema_run&.include?("FUGUE_VERIFY_GITHUB_GRAPHQL_REF_CAS_SCHEMA=true") && schema_run.include?("FUGUE_GRAPHQL_REF_CAS_SCHEMA_ONLY=true")
fail_contract("GraphQL schema gate does not receive the ephemeral token") unless schema_step.dig("env", "GH_TOKEN") == "${{ github.token }}"
full_test_step = gate.fetch("steps").find { |step| step["name"] == "Verify recovery workflow and release safety contracts" }
fail_contract("full recovery test suite inherits a GitHub token") if full_test_step.fetch("env", {}).key?("GH_TOKEN")
schema_source = File.read(File.join(repo, "scripts/test_release_policy_recovery_workflow.sh"))
[
  '"GitRefname"',
  '"GitObjectID"',
  '"UpdateRefsInput"',
  '"UpdateRefsPayload"',
  '"RefUpdate"',
  "trap 'forward_schema_signal TERM 143' TERM",
  'kill -s "${signal_name}" "${schema_python_pid}"',
  'start_new_session=True',
  'signal.signal(signal.SIGTERM, forward_termination)',
  'signal.pthread_sigmask(signal.SIG_BLOCK, termination_signals)',
  'os.killpg(process.pid, signal.SIGTERM)',
  'os.killpg(process.pid, signal.SIGKILL)',
].each do |required|
  fail_contract("live GraphQL schema gate does not lock #{required}") unless schema_source.include?(required)
end
[
  "deploy_state",
  "pending_runs",
  "cancel_recovery_runs",
  "RECOVERY_WRITER_DEADLINE_EPOCH",
  "timeout --kill-after=2s 10s git ls-remote",
].each do |required|
  fail_contract("pre-write revalidation is missing #{required}") unless prewrite_run.include?(required)
end
[
  "GITHUB_RUN_NUMBER",
  "GITHUB_RUN_ATTEMPT",
  "event=workflow_dispatch",
  'head_sha=${GITHUB_SHA}',
  "history_total_count",
  "observed_unique_run_count",
  '/attempts/${attempt_number}',
  "GITHUB_RUN_ATTEMPT - 1",
  "current_run_observed",
  '[[ "${current_run_observed}" == \'1\' ]]',
  'if [[ "${observed_status}" != \'completed\' || "${observed_conclusion}" != \'success\' ]]; then',
  "RECOVERY_PRIOR_ATTEMPTS_VERIFIED",
].each do |required|
  fail_contract("pre-write historical failure latch is missing #{required}") unless prewrite_run.include?(required)
end
fail_contract("historical failure latch trusts only the latest run attempt") unless prewrite_run.include?('/actions/runs/${run_id}/attempts/${attempt_number}')
fail_contract("historical failure latch skips higher-number exact-SHA runs") if prewrite_run.include?("run_number <= GITHUB_RUN_NUMBER")
fail_contract("historical failure latch does not disable recovery before snapshotting") unless prewrite_run.index('${recovery_workflow}/disable') < prewrite_run.index('history_url=')
fail_contract("historical failure latch drops the API total count") unless prewrite_run.include?('[[ "${observed_run_count}" == "${history_total_count}" ]]') && prewrite_run.include?('[[ "${observed_unique_run_count}" == "${history_total_count}" ]]') && prewrite_run.include?('[[ "${exact_run_count}" == "${history_total_count}" ]]')
fail_contract("historical failure latch is not resource-bounded") unless prewrite_run.scan("too many").length == 2 && prewrite_run.scan(" > 20 ").length == 2
fail_contract("historical failure latch does not retain canonical attempt rows") unless prewrite_run.include?("prior-recovery-attempts.tsv") && prewrite_run.include?("LC_ALL=C sort") && prewrite_run.include?("RECOVERY_PRIOR_ATTEMPTS_DIGEST")
[
  '"verified_prior_recovery_attempts": int(os.environ["RECOVERY_PRIOR_ATTEMPTS_VERIFIED"])',
  '"prior_recovery_attempt_digest": os.environ["RECOVERY_PRIOR_ATTEMPTS_DIGEST"]',
  '"prior_recovery_attempts": prior_attempts',
].each do |required|
  fail_contract("success evidence omits the historical failure latch proof") unless context_run.include?(required)
end
fail_contract("transaction must use the bounded exact-CAS helper") unless transaction_run.include?("recover_control_plane_release_baseline.py transact")
fail_contract("transaction helper timeout drifted") unless transaction_run.include?("--timeout-seconds 20")
fail_contract("production transaction does not use the atomic GraphQL beforeOid CAS") unless transaction_run.include?("--cas-backend github-graphql") && transaction_run.include?('--github-repository "${GITHUB_REPOSITORY}"')
fail_contract("transaction evidence upload must yield immediately to cancellation") unless transaction_upload["if"] == "${{ always() && !cancelled() }}"
fail_contract("transaction evidence upload is not bounded") unless transaction_upload["timeout-minutes"] == 5 && transaction_upload["continue-on-error"] == true
[
  '${recovery_workflow}/disable',
  "pending_runs",
  "deploy-control-plane.yml",
  "refs/tags/fugue-control-plane-release-baseline",
].each do |required|
  fail_contract("success quiescence is missing #{required}") unless quiesce_run.include?(required)
end
fail_contract("compensation condition drifted") unless compensate["if"] == "${{ always() && needs.recovery-gate.result == 'success' && needs.advance-baseline.result != 'success' }}"
fail_contract("compensation must continue to evidence") unless compensate_step["continue-on-error"] == true
fail_contract("independent compensation must survive cancellation") unless compensate.fetch("steps").first["if"] == "always()" && compensate_step["if"] == "always()"
fail_contract("compensation must use exact helper") unless compensate_step.fetch("run").include?("recover_control_plane_release_baseline.py compensate")
fail_contract("independent compensation does not use the atomic GraphQL beforeOid CAS") unless compensate_step.fetch("run").include?("--cas-backend github-graphql")
fail_contract("writer lacks same-runner always compensation") unless same_runner.fetch("if").include?("always()") && same_runner.fetch("run").include?("recover_control_plane_release_baseline.py compensate")
fail_contract("same-runner compensation does not use the atomic GraphQL beforeOid CAS") unless same_runner.fetch("run").include?("--cas-backend github-graphql")
fail_contract("writer cancellation is not routed to same-runner compensation") unless same_runner.fetch("if").include?("cancelled()")
fail_contract("writer job must survive workflow cancellation for same-runner cleanup") unless advance.fetch("if") == "${{ always() && needs.recovery-gate.result == 'success' }}"
fail_contract("writer lacks same-runner lane freeze") unless same_runner_freeze.fetch("if").include?("always()") && same_runner_freeze.fetch("run").include?("pending_runs")
fail_contract("same-runner freeze evidence hard-codes an empty run inventory") if same_runner_freeze.fetch("run").include?('"remaining_other_run_ids": []')
same_runner_paths = same_runner_upload.dig("with", "path").to_s.lines.map(&:strip).reject(&:empty?)
fail_contract("same-runner upload must preserve both transaction and compensation evidence") unless same_runner_paths == [
  "${{ runner.temp }}/fugue-release-policy-recovery",
  "${{ runner.temp }}/fugue-release-policy-recovery-same-runner-compensation",
]
fail_contract("freeze condition drifted") unless freeze["if"].include?("needs.advance-baseline.result != 'success'")
fail_contract("freeze action must continue to evidence") unless freeze_step["if"] == "always()" && freeze_step["continue-on-error"] == true
fail_contract("post-freeze observation must continue to evidence") unless post_freeze["if"] == "always()" && post_freeze["continue-on-error"] == true
fail_contract("freeze upload must always run") unless freeze_upload["if"] == "always()" && freeze_upload["continue-on-error"] == true

[prewrite_run, quiesce_run, context_run, final_proof_run, same_runner_freeze.fetch("run"), freeze_step.fetch("run"), post_freeze.fetch("run")].each do |run|
  fail_contract("pending-run inventory is not fail-fast") unless run.include?('inventory="$(mktemp)"') && run.include?("return 1")
  status_loops = run.scan("for status in queued in_progress requested waiting pending; do").length
  guarded_queries = run.scan("if ! timeout").length
  fail_contract("a status query can bypass explicit failure handling") unless status_loops.positive? && guarded_queries >= status_loops
  fail_contract("pending-run failure is discarded inside a string assertion") if run.include?('[[ -z "$(pending_runs')
end
forward_names = [
  "Record writer deadline with compensation reserve",
  "Checkout exact recovery target with tag-write credentials",
  "Revalidate authorization and quiesce duplicate recovery runs",
  "Advance, rollback, and re-advance dedicated release baseline",
  "Disable recovery lane and prove final quiescence",
  "Write recovery context evidence",
  "Upload release-policy recovery evidence",
  "Final remote and lane proof",
]
forward_names.each do |name|
  step = advance.fetch("steps").find { |candidate| candidate["name"] == name }
  fail_contract("forward step #{name} may run after workflow cancellation") unless step&.fetch("if", "").include?("!cancelled()")
end
success_upload = advance.fetch("steps").find { |step| step["name"] == "Upload release-policy recovery evidence" }
fail_contract("success evidence upload is not bounded") unless success_upload&.fetch("timeout-minutes", nil) == 5
fail_contract("writer deadline/reserve gate is missing") unless advance.fetch("steps").first.fetch("run").include?("started_at + 2400") && prewrite_run.include?("< 1500")
helper_reap_grace = 4 # bounded TERM and KILL process-group reaping
# Transaction has ten normal commands plus an eleventh failure observation; the
# conservative ten-timeout bound still exceeds the exact 11T+8 worst case.
graphql_transact_bound = 10 * (20 + helper_reap_grace)
# Compensation has four normal commands plus a fifth failure observation.
graphql_same_runner_compensation_bound = 5 * (10 + helper_reap_grace)
graphql_independent_compensation_bound = 5 * (20 + helper_reap_grace)
fail_contract("GraphQL transaction timeout budget drifted") unless transaction_run.include?("--timeout-seconds 20") && graphql_transact_bound == 240 && graphql_transact_bound <= 1500
fail_contract("same-runner GraphQL compensation exceeds hard-timeout cushion") unless same_runner.fetch("run").include?("--timeout-seconds 10") && graphql_same_runner_compensation_bound == 70 && graphql_same_runner_compensation_bound <= ((advance.fetch("timeout-minutes") * 60) - 2400)
fail_contract("independent GraphQL compensation exceeds its job timeout") unless compensate_step.fetch("run").include?("--timeout-seconds 20") && graphql_independent_compensation_bound == 120 && graphql_independent_compensation_bound <= (compensate.fetch("timeout-minutes") * 60)
prewrite_timeouts = prewrite_run.scan(/timeout --kill-after=(\d+)s (\d+)s/).map { |kill, timeout| [kill.to_i, timeout.to_i] }
fail_contract("pre-write network timeout shape drifted") unless prewrite_timeouts.any? && prewrite_timeouts.all? { |pair| pair == [2, 10] }
fail_contract("pre-write retry shape drifted") unless prewrite_run.scan("for attempt in 1 2 3 4 5; do").length == 2
prewrite_call_bound = 12
prewrite_pending_inventory_bound = 5 * prewrite_call_bound
prewrite_disable_bound = 5 * ((2 * prewrite_call_bound) + 2)
prewrite_history_bound = (2 * prewrite_call_bound) + (20 * prewrite_call_bound)
prewrite_no_pending_bound = prewrite_call_bound + prewrite_disable_bound + (2 * prewrite_pending_inventory_bound) + prewrite_history_bound + prewrite_call_bound
prewrite_cancel_round_bound = prewrite_pending_inventory_bound + prewrite_call_bound + 2 + prewrite_pending_inventory_bound
prewrite_one_pending_bound = prewrite_no_pending_bound + (5 * prewrite_cancel_round_bound)
fail_contract("pre-write no-pending path exceeds the forward window") unless prewrite_no_pending_bound == 538 && prewrite_no_pending_bound <= (2400 - 1500)
fail_contract("pre-write pending path can cross the internal deadline") unless prewrite_one_pending_bound == 1208 && prewrite_one_pending_bound < 2400
fail_contract("pre-write pending path would not fail the final reserve gate") unless prewrite_one_pending_bound > (2400 - 1500)
fail_contract("final proof deadline is smaller than its bounded query plan") unless final_proof_run.include?("< 240")
fail_contract("freeze deadline/evidence reserve gate is missing") unless freeze.fetch("steps").first.fetch("run").include?("started_at + 2100") && freeze_step.fetch("run").include?("< 360")

# Derive the worst-case bounds from the exact workflow loop and timeout shapes.
# Shared concurrency permits at most one other pending run across both lanes.
api_timeout = 10
kill_grace = 2
api_call_bound = api_timeout + kill_grace
status_count = 5
retry_count = 5
retry_sleep = 2
workflow_count = 2
pending_inventory_bound = status_count * api_call_bound
disable_workflow_bound = retry_count * ((2 * api_call_bound) + retry_sleep)
cancel_round_bound = pending_inventory_bound + api_call_bound + retry_sleep + pending_inventory_bound
quiesce_bound = disable_workflow_bound + pending_inventory_bound + (retry_count * cancel_round_bound) + pending_inventory_bound + (3 * api_call_bound)
final_proof_bound = (2 * api_call_bound) + (2 * pending_inventory_bound) + (2 * api_call_bound)
freeze_action_bound = (workflow_count * disable_workflow_bound) + (workflow_count * pending_inventory_bound) + (retry_count * cancel_round_bound)
post_freeze_bound = (3 * api_call_bound) + (2 * pending_inventory_bound)

{
  "success quiescence" => quiesce_run,
  "final proof" => final_proof_run,
  "freeze action" => freeze_step.fetch("run"),
  "post-freeze evidence" => post_freeze.fetch("run"),
}.each do |label, run|
  observed = run.scan(/timeout --kill-after=(\d+)s (\d+)s/).map { |kill, timeout| [kill.to_i, timeout.to_i] }
  fail_contract("#{label} network timeout shape drifted") unless observed.any? && observed.all? { |pair| pair == [kill_grace, api_timeout] }
  status_lists = run.scan(/for status in ([^;]+); do/).map { |values| values.fetch(0).split }
  fail_contract("#{label} pending status inventory drifted") unless status_lists.all? { |values| values.length == status_count }
end
fail_contract("success quiescence retry shape drifted") unless quiesce_run.scan("for attempt in 1 2 3 4 5; do").length == 2
fail_contract("freeze retry shape drifted") unless freeze_step.fetch("run").scan("for attempt in 1 2 3 4 5; do").length == 2
fail_contract("success quiescence exceeds its reserve") unless quiesce_bound == 956 && quiesce_bound <= 1100
fail_contract("final proof exceeds its reserve") unless final_proof_bound == 168 && final_proof_bound <= 240
fail_contract("freeze action exceeds its evidence reserve") unless freeze_action_bound == 1050 && freeze_action_bound <= (2100 - 360)
fail_contract("post-freeze observation exceeds its reserve") unless post_freeze_bound == 156 && post_freeze_bound <= 360
fail_contract("writer hard timeout lacks bounded artifact cushion") unless (advance.fetch("timeout-minutes") * 60) - 2400 >= 600
fail_contract("freeze hard timeout lacks evidence-upload cushion") unless (freeze.fetch("timeout-minutes") * 60) - 2100 >= 600

watchdog_trigger = watchdog["on"] || watchdog[true]
fail_contract("watchdog trigger drifted") unless watchdog_trigger == {
  "workflow_run" => {
    "workflows" => [recovery.fetch("name")],
    "types" => ["completed"],
  },
}
fail_contract("watchdog top-level permissions must be empty") unless watchdog["permissions"] == {}
fail_contract("watchdog must not cancel its own safety run") unless watchdog.dig("concurrency", "cancel-in-progress") == false
expected_watchdog_group = "fugue-control-plane-release-policy-failure-watchdog-${{ github.event.workflow_run.id }}-${{ github.event.workflow_run.run_attempt }}"
fail_contract("watchdog concurrency coalesces distinct source attempts") unless watchdog.dig("concurrency", "group") == expected_watchdog_group
watchdog_jobs = watchdog.fetch("jobs")
fail_contract("watchdog job set drifted") unless watchdog_jobs.keys == ["freeze-authorized-failure"]
watchdog_job = watchdog_jobs.fetch("freeze-authorized-failure")
watchdog_if = watchdog_job.fetch("if")
[
  "always()",
  "conclusion != 'success'",
  "event == 'workflow_dispatch'",
  "head_branch == 'main'",
  "head_repository.full_name == github.repository",
  "head_sha == vars.FUGUE_CONTROL_PLANE_RELEASE_POLICY_RECOVERY_SHA",
].each do |required|
  fail_contract("watchdog authorization condition is missing #{required}") unless watchdog_if.include?(required)
end
fail_contract("watchdog permissions drifted") unless watchdog_job["permissions"] == {"actions" => "write", "contents" => "read"}
fail_contract("watchdog hard timeout drifted") unless watchdog_job["timeout-minutes"] == 30
watchdog_steps = watchdog_job.fetch("steps")
watchdog_freeze = watchdog_steps.find { |step| step["name"] == "Freeze both release lanes and cancel non-terminal runs" }
watchdog_evidence = watchdog_steps.find { |step| step["name"] == "Record watchdog freeze evidence" }
watchdog_upload = watchdog_steps.find { |step| step["name"] == "Upload watchdog freeze evidence" }
fail_contract("watchdog freeze must precede artifact upload") unless watchdog_steps.index(watchdog_freeze) < watchdog_steps.index(watchdog_upload)
fail_contract("watchdog freeze must survive cancellation") unless watchdog_freeze["if"] == "always()" && watchdog_freeze["continue-on-error"] == true
fail_contract("watchdog upload is not hard-bounded") unless watchdog_upload["if"] == "always()" && watchdog_upload["timeout-minutes"] == 5
fail_contract("watchdog freeze deadline/reserve drifted") unless watchdog_steps.first.fetch("run").include?("started_at + 1500") && watchdog_freeze.fetch("run").include?("< 300")
fail_contract("watchdog pending inventory is not fail-fast") unless [watchdog_freeze, watchdog_evidence].all? { |step| step.fetch("run").include?('inventory="$(mktemp)"') && step.fetch("run").include?("return 1") }
expected_attempt_expression = "${{ github.event.workflow_run.run_attempt }}"
fail_contract("watchdog freeze does not bind the triggering attempt") unless watchdog_freeze.dig("env", "TRIGGER_RUN_ATTEMPT") == expected_attempt_expression && watchdog_freeze.fetch("run").include?('[[ "${TRIGGER_RUN_ATTEMPT}" =~ ^[1-9][0-9]*$ ]]')
fail_contract("watchdog evidence does not bind the triggering attempt") unless watchdog_evidence.dig("env", "TRIGGER_RUN_ATTEMPT") == expected_attempt_expression && watchdog_evidence.fetch("run").include?('"trigger_run_attempt": int(os.environ["TRIGGER_RUN_ATTEMPT"])')
fail_contract("watchdog artifact name does not identify the triggering attempt") unless watchdog_upload.dig("with", "name").include?(expected_attempt_expression)
watchdog_runs = {
  "freeze" => watchdog_freeze.fetch("run"),
  "evidence" => watchdog_evidence.fetch("run"),
}
watchdog_runs.each do |label, run|
  observed = run.scan(/timeout --kill-after=(\d+)s (\d+)s/).map { |kill, timeout| [kill.to_i, timeout.to_i] }
  fail_contract("watchdog #{label} network timeout shape drifted") unless observed.any? && observed.all? { |pair| pair == [2, 10] }
  status_lists = run.scan(/for status in ([^;]+); do/).map { |values| values.fetch(0).split }
  fail_contract("watchdog #{label} pending status inventory drifted") unless status_lists.all? { |values| values.length == 5 }
  fail_contract("watchdog #{label} hides same-run-id rerun attempts") if run.include?('$0 != trigger') || run.include?('-v trigger=')
end
fail_contract("watchdog workflow inventory drifted") unless watchdog_freeze.fetch("run").include?("workflows=(deploy-control-plane.yml recover-control-plane-release-policy.yml)")
fail_contract("watchdog retry shape drifted") unless watchdog_freeze.fetch("run").scan("for attempt in 1 2 3 4 5; do").length == 2
[
  'cancel_run_ids "${deploy_remaining}"',
  'cancel_run_ids "${recovery_remaining}"',
  'deploy_remaining="$(pending_runs deploy-control-plane.yml)"',
  'recovery_remaining="$(pending_runs recover-control-plane-release-policy.yml)"',
].each do |required|
  fail_contract("watchdog does not share one bounded cancellation window across both lanes") unless watchdog_freeze.fetch("run").include?(required)
end
fail_contract("watchdog reverted to per-workflow cancellation retries") if watchdog_freeze.fetch("run").include?("cancel_pending_runs")
watchdog_api_call_bound = 10 + 2
watchdog_pending_inventory_bound = 5 * watchdog_api_call_bound
watchdog_disable_bound = 5 * ((2 * watchdog_api_call_bound) + 2)
# The watchdog is intentionally outside the release concurrency group, so the
# watched group can contain one running and one pending run when it starts.
# Both IDs share each combined retry round across the two workflow inventories.
watchdog_cancel_round_bound = watchdog_pending_inventory_bound + (2 * watchdog_api_call_bound) + 2 + watchdog_pending_inventory_bound
watchdog_action_bound = (2 * watchdog_disable_bound) + (2 * watchdog_pending_inventory_bound) + (5 * watchdog_cancel_round_bound)
watchdog_observation_bound = (4 * watchdog_api_call_bound) + (2 * watchdog_pending_inventory_bound)
fail_contract("watchdog freeze exceeds its evidence reserve") unless watchdog_action_bound == 1110 && watchdog_action_bound <= (1500 - 300)
fail_contract("watchdog observation exceeds its reserve") unless watchdog_observation_bound == 168 && watchdog_observation_bound <= 300
fail_contract("watchdog hard timeout lacks artifact cushion") unless (watchdog_job.fetch("timeout-minutes") * 60) - 1500 >= 300

recovery.fetch("jobs").each_value do |job|
  job.fetch("steps").each do |step|
    next unless step.key?("run")
    _stdout, stderr, status = Open3.capture3("bash", "-n", stdin_data: step.fetch("run"))
    fail_contract("shell syntax failed for #{step.fetch('name', 'unnamed')}: #{stderr}") unless status.success?
  end
end
watchdog.fetch("jobs").each_value do |job|
  job.fetch("steps").each do |step|
    next unless step.key?("run")
    _stdout, stderr, status = Open3.capture3("bash", "-n", stdin_data: step.fetch("run"))
    fail_contract("watchdog shell syntax failed for #{step.fetch('name', 'unnamed')}: #{stderr}") unless status.success?
  end
end

deploy_steps = deploy.fetch("jobs").fetch("deploy").fetch("steps")
origin_step = deploy_steps.find { |step| step["name"] == "Record deploy job budget origin" }
checkout_step = deploy_steps.find { |step| step["name"] == "Checkout" }
origin_run = origin_step&.fetch("run", "")
fail_contract("deadline origin step must run before checkout") unless deploy_steps.index(origin_step) < deploy_steps.index(checkout_step)
[
  "readonly budget_seconds=20400",
  "FUGUE_DEPLOY_JOB_STARTED_AT_EPOCH=%s",
  "CONTROL_PLANE_RELEASE_JOB_DEADLINE_EPOCH=%s",
  '$((started_at + budget_seconds))',
].each do |required|
  fail_contract("deadline origin is missing #{required}") unless origin_run.include?(required)
end
upgrade = deploy_steps.find { |step| step["name"] == "Upgrade Fugue control plane" }
fail_contract("upgrade budget drifted") unless upgrade.dig("env", "FUGUE_DEPLOY_JOB_BUDGET_SECONDS") == "20400"
deploy_test_run = deploy.fetch("jobs").values.flat_map { |job| job.fetch("steps", []) }.map { |step| step["run"] }.compact.find { |run| run.include?("test_node_local_dns_release.sh") }
fail_contract("ordinary deploy gate must require Docker image verification") unless deploy_test_run&.include?("FUGUE_REQUIRE_NODE_LOCAL_DNS_TEST_DOCKER=true")

helper = File.read(File.join(repo, "scripts/recover_control_plane_release_baseline.py"))
fail_contract("CAS helper must suppress ambient tag following") unless helper.include?('"--no-follow-tags"')
fail_contract("CAS helper must suppress submodule recursion") unless helper.include?('"--recurse-submodules=no"')
fail_contract("CAS helper must forward termination to the active process group") unless helper.include?("handle_termination_signal") && helper.include?("terminate_process_group")
fail_contract("CAS helper spawn publication is not signal-masked") unless helper.include?("pthread_sigmask") && helper.include?("SIG_BLOCK")
[
  '"updateRefs(input:{repositoryId:$repositoryId,"',
  '"refUpdates:[{name:$name,beforeOid:$beforeOid,"',
  '"afterOid:$afterOid,force:$force}]',
  '"clientMutationId:$clientMutationId})',
].each do |required|
  fail_contract("GraphQL CAS helper is missing #{required}") unless helper.include?(required)
end
fail_contract("GraphQL CAS backend silently uses the non-CAS REST ref API") if helper.include?("/git/refs/")

puts "release-policy recovery workflow contract passed"
RUBY

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
remote="${tmp_dir}/remote.git"
work="${tmp_dir}/work"
git init --bare "${remote}" >/dev/null
git init "${work}" >/dev/null
git -C "${work}" config user.name recovery-test
git -C "${work}" config user.email recovery-test@example.invalid
git -C "${work}" remote add origin "${remote}"
printf 'base\n' >"${work}/state"
git -C "${work}" add state
git -C "${work}" commit -m base >/dev/null
base_sha="$(git -C "${work}" rev-parse HEAD)"
printf 'target\n' >"${work}/state"
git -C "${work}" add state
git -C "${work}" commit -m target >/dev/null
target_sha="$(git -C "${work}" rev-parse HEAD)"
git -C "${work}" push origin "${target_sha}:refs/heads/main" >/dev/null
git -C "${work}" push origin "${base_sha}:refs/tags/fugue-control-plane-release-baseline" >/dev/null
git -C "${work}" tag -a reachable-extra "${base_sha}" -m reachable-extra
git -C "${work}" config push.followTags true

run_helper() {
  git -C "${work}" --no-pager status --short >/dev/null
  (
    cd "${work}"
    python3 "${REPO_ROOT}/scripts/recover_control_plane_release_baseline.py" "$@"
  )
}
tag_oid() {
  git --git-dir="${remote}" rev-parse refs/tags/fugue-control-plane-release-baseline
}

success_evidence="${tmp_dir}/success.json"
run_helper transact --base-sha "${base_sha}" --target-sha "${target_sha}" --evidence "${success_evidence}" --timeout-seconds 10
[[ "$(tag_oid)" == "${target_sha}" ]] || fail "successful transaction did not finish at target"
if git --git-dir="${remote}" show-ref --verify --quiet refs/tags/reachable-extra; then
  fail "baseline CAS must not follow ambient annotated tags"
fi
git -C "${work}" config push.followTags false
python3 - "${success_evidence}" "${base_sha}" "${target_sha}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    evidence = json.load(handle)
base, target = sys.argv[2:]
assert evidence["pre_oid"] == base
assert evidence["forward_oid"] == target
assert evidence["rollback_oid"] == base
assert evidence["final_oid"] == target
assert evidence["rollback_verification"] == "succeeded"
assert evidence["cluster_mutation_attempted"] is False
PY

compensation_evidence="${tmp_dir}/compensation.json"
run_helper compensate --base-sha "${base_sha}" --target-sha "${target_sha}" --evidence "${compensation_evidence}" --timeout-seconds 10
[[ "$(tag_oid)" == "${base_sha}" ]] || fail "target compensation did not restore base"
run_helper compensate --base-sha "${base_sha}" --target-sha "${target_sha}" --evidence "${tmp_dir}/already-base.json" --timeout-seconds 10

hook="${remote}/hooks/pre-receive"
cat >"${hook}" <<'HOOK'
#!/usr/bin/env bash
set -euo pipefail
counter_file="$(dirname "$0")/recovery-test-counter"
count=0
[[ ! -f "${counter_file}" ]] || count="$(<"${counter_file}")"
count=$((count + 1))
printf '%s\n' "${count}" >"${counter_file}"
if [[ "${count}" == "2" ]]; then
  echo "injected second-push failure" >&2
  exit 1
fi
cat >/dev/null
HOOK
chmod +x "${hook}"
printf '0\n' >"${remote}/hooks/recovery-test-counter"
if run_helper transact --base-sha "${base_sha}" --target-sha "${target_sha}" --evidence "${tmp_dir}/injected-failure.json" --timeout-seconds 10; then
  fail "injected second-push failure unexpectedly succeeded"
fi
[[ "$(tag_oid)" == "${target_sha}" ]] || fail "fault injection did not leave the exact intermediate target"
run_helper compensate --base-sha "${base_sha}" --target-sha "${target_sha}" --evidence "${tmp_dir}/injected-compensation.json" --timeout-seconds 10
[[ "$(tag_oid)" == "${base_sha}" ]] || fail "independent compensation did not restore injected intermediate target"

printf 'unexpected\n' >"${work}/state"
git -C "${work}" add state
git -C "${work}" commit -m unexpected >/dev/null
unexpected_sha="$(git -C "${work}" rev-parse HEAD)"
git -C "${work}" push --force origin "${unexpected_sha}:refs/tags/fugue-control-plane-release-baseline" >/dev/null
if run_helper compensate --base-sha "${base_sha}" --target-sha "${target_sha}" --evidence "${tmp_dir}/unexpected.json" --timeout-seconds 10; then
  fail "compensation must reject an unexpected third-party tag OID"
fi
[[ "$(tag_oid)" == "${unexpected_sha}" ]] || fail "unexpected tag OID was overwritten"

git --git-dir="${remote}" update-ref refs/tags/fugue-control-plane-release-baseline "${base_sha}" "${unexpected_sha}"
graphql_bin="${tmp_dir}/graphql-bin"
mkdir -p "${graphql_bin}"
cat >"${graphql_bin}/gh" <<'PY'
#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path

if sys.argv[1:3] != ["api", "graphql"]:
    raise SystemExit("unexpected fake gh command")
fields = {}
index = 3
while index < len(sys.argv):
    if sys.argv[index] in {"-f", "-F"}:
        key, value = sys.argv[index + 1].split("=", 1)
        fields[key] = value
        index += 2
    else:
        index += 1
query = fields.get("query", "")
if query.startswith("query("):
    print("R_fugue_recovery_test")
    raise SystemExit(0)
if "updateRefs" not in query:
    raise SystemExit("fake gh expected updateRefs mutation")
required = {"repositoryId", "name", "beforeOid", "afterOid", "force", "clientMutationId"}
if not required.issubset(fields) or fields["force"] != "true":
    raise SystemExit("fake gh mutation variables are incomplete")
counter_path = Path(os.environ["GRAPHQL_COUNTER"])
count = int(counter_path.read_text(encoding="utf-8")) if counter_path.exists() else 0
count += 1
counter_path.write_text(f"{count}\n", encoding="utf-8")
if count == int(os.environ.get("GRAPHQL_FAIL_MUTATION", "0")):
    print("remote: injected GraphQL failure context", file=sys.stderr)
    print("remote: injected GraphQL CAS failure detail", file=sys.stderr)
    raise SystemExit(1)
remote = os.environ["GRAPHQL_REMOTE"]
if count == int(os.environ.get("GRAPHQL_RACE_MUTATION", "0")):
    race_oid = os.environ["GRAPHQL_RACE_OID"]
    subprocess.run(
        [
            "git",
            f"--git-dir={remote}",
            "update-ref",
            fields["name"],
            race_oid,
            fields["beforeOid"],
        ],
        check=True,
    )
current = subprocess.check_output(
    ["git", f"--git-dir={remote}", "rev-parse", fields["name"]], text=True
).strip()
if current != fields["beforeOid"]:
    print(
        f"beforeOid mismatch expected={fields['beforeOid']} actual={current}",
        file=sys.stderr,
    )
    raise SystemExit(1)
subprocess.run(
    [
        "git",
        f"--git-dir={remote}",
        "update-ref",
        fields["name"],
        fields["afterOid"],
        fields["beforeOid"],
    ],
    check=True,
)
print(fields["clientMutationId"])
PY
chmod +x "${graphql_bin}/gh"
graphql_counter="${tmp_dir}/graphql-counter"
printf '0\n' >"${graphql_counter}"
graphql_success="${tmp_dir}/graphql-success.json"
PATH="${graphql_bin}:${PATH}" GH_TOKEN=test-token GITHUB_REPOSITORY=synthetic/recovery \
  GRAPHQL_REMOTE="${remote}" GRAPHQL_COUNTER="${graphql_counter}" \
  run_helper transact --cas-backend github-graphql \
    --github-repository synthetic/recovery \
    --base-sha "${base_sha}" --target-sha "${target_sha}" \
    --evidence "${graphql_success}" --timeout-seconds 10
[[ "$(tag_oid)" == "${target_sha}" ]] || fail "GraphQL transaction did not finish at target"
python3 - "${graphql_success}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    evidence = json.load(handle)
assert evidence["cas_backend"] == "github-graphql"
assert evidence["github_repository"] == "synthetic/recovery"
assert evidence["rollback_verification"] == "succeeded"
PY
PATH="${graphql_bin}:${PATH}" GH_TOKEN=test-token GITHUB_REPOSITORY=synthetic/recovery \
  GRAPHQL_REMOTE="${remote}" GRAPHQL_COUNTER="${graphql_counter}" \
  run_helper compensate --cas-backend github-graphql \
    --github-repository synthetic/recovery \
    --base-sha "${base_sha}" --target-sha "${target_sha}" \
    --evidence "${tmp_dir}/graphql-success-compensation.json" --timeout-seconds 10
[[ "$(tag_oid)" == "${base_sha}" ]] || fail "GraphQL compensation did not restore base"

printf '0\n' >"${graphql_counter}"
graphql_failure="${tmp_dir}/graphql-failure.json"
if PATH="${graphql_bin}:${PATH}" GH_TOKEN=test-token GITHUB_REPOSITORY=synthetic/recovery \
  GRAPHQL_REMOTE="${remote}" GRAPHQL_COUNTER="${graphql_counter}" GRAPHQL_FAIL_MUTATION=2 \
  run_helper transact --cas-backend github-graphql \
    --github-repository synthetic/recovery \
    --base-sha "${base_sha}" --target-sha "${target_sha}" \
    --evidence "${graphql_failure}" --timeout-seconds 10; then
  fail "injected GraphQL rollback failure unexpectedly succeeded"
fi
[[ "$(tag_oid)" == "${target_sha}" ]] || fail "GraphQL fault injection did not stop at target"
python3 - "${graphql_failure}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    evidence = json.load(handle)
assert evidence["phase"] == "rollback"
assert "injected GraphQL failure context" in evidence["error"]
assert "injected GraphQL CAS failure detail" in evidence["error"]
PY
PATH="${graphql_bin}:${PATH}" GH_TOKEN=test-token GITHUB_REPOSITORY=synthetic/recovery \
  GRAPHQL_REMOTE="${remote}" GRAPHQL_COUNTER="${graphql_counter}" \
  run_helper compensate --cas-backend github-graphql \
    --github-repository synthetic/recovery \
    --base-sha "${base_sha}" --target-sha "${target_sha}" \
    --evidence "${tmp_dir}/graphql-failure-compensation.json" --timeout-seconds 10
[[ "$(tag_oid)" == "${base_sha}" ]] || fail "GraphQL fault compensation did not restore base"

printf '0\n' >"${graphql_counter}"
graphql_race="${tmp_dir}/graphql-race.json"
if PATH="${graphql_bin}:${PATH}" GH_TOKEN=test-token GITHUB_REPOSITORY=synthetic/recovery \
  GRAPHQL_REMOTE="${remote}" GRAPHQL_COUNTER="${graphql_counter}" \
  GRAPHQL_RACE_MUTATION=1 GRAPHQL_RACE_OID="${unexpected_sha}" \
  run_helper transact --cas-backend github-graphql \
    --github-repository synthetic/recovery \
    --base-sha "${base_sha}" --target-sha "${target_sha}" \
    --evidence "${graphql_race}" --timeout-seconds 10; then
  fail "GraphQL beforeOid race unexpectedly overwrote the competing ref"
fi
[[ "$(tag_oid)" == "${unexpected_sha}" ]] || fail "GraphQL beforeOid race overwrote the competing ref"
python3 - "${graphql_race}" "${unexpected_sha}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    evidence = json.load(handle)
assert evidence["phase"] == "advance"
assert evidence["outcome"] == "failed"
assert evidence["failure_observed_oid"] == sys.argv[2]
assert "beforeOid mismatch" in evidence["error"]
PY
git --git-dir="${remote}" update-ref refs/tags/fugue-control-plane-release-baseline "${base_sha}" "${unexpected_sha}"

schema_signal_dir="${tmp_dir}/schema-signal"
schema_signal_bin="${schema_signal_dir}/bin"
mkdir -p "${schema_signal_bin}"
cat >"${schema_signal_bin}/gh" <<'PY'
#!/usr/bin/env python3
import os
import signal
import time
from pathlib import Path

root = Path(os.environ["SCHEMA_SIGNAL_DIR"])
(root / "started").touch()
signal.signal(signal.SIGTERM, signal.SIG_IGN)
time.sleep(3)
(root / "late-child").touch()
PY
chmod +x "${schema_signal_bin}/gh"
PATH="${schema_signal_bin}:${PATH}" GH_TOKEN=test-token \
  SCHEMA_SIGNAL_DIR="${schema_signal_dir}" \
  FUGUE_VERIFY_GITHUB_GRAPHQL_REF_CAS_SCHEMA=true \
  FUGUE_GRAPHQL_REF_CAS_SCHEMA_ONLY=true \
  bash "${REPO_ROOT}/scripts/test_release_policy_recovery_workflow.sh" \
  >"${schema_signal_dir}/stdout" 2>"${schema_signal_dir}/stderr" &
schema_shell_pid=$!
for _ in $(seq 1 100); do
  [[ ! -e "${schema_signal_dir}/started" ]] || break
  sleep 0.05
done
[[ -e "${schema_signal_dir}/started" ]] || fail "schema cancellation test did not start gh"
kill -TERM "${schema_shell_pid}"
if wait "${schema_shell_pid}"; then
  fail "cancelled GraphQL schema gate unexpectedly succeeded"
fi
sleep 4
[[ ! -e "${schema_signal_dir}/late-child" ]] || fail "cancelled GraphQL schema gate left a late child"

fake_bin="${tmp_dir}/fake-bin"
mkdir -p "${fake_bin}"
cat >"${fake_bin}/git" <<'PY'
#!/usr/bin/env python3
import os
import signal
import sys
import time
from pathlib import Path

root = Path(os.environ["RECOVERY_SIGNAL_TEST_DIR"])
counter = root / "counter"
count = int(counter.read_text(encoding="utf-8")) if counter.exists() else 0
count += 1
counter.write_text(f"{count}\n", encoding="utf-8")
if count == 1:
    (root / "started").touch()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(4)
    (root / "late-marker").touch()
print(f"{os.environ['SIGNAL_TEST_BASE']}\trefs/tags/fugue-control-plane-release-baseline")
PY
chmod +x "${fake_bin}/git"
signal_test_dir="${tmp_dir}/signal-test"
mkdir -p "${signal_test_dir}"
signal_evidence="${signal_test_dir}/evidence.json"
(
  cd "${work}"
  exec env PATH="${fake_bin}:${PATH}" \
    RECOVERY_SIGNAL_TEST_DIR="${signal_test_dir}" \
    SIGNAL_TEST_BASE="${base_sha}" \
    python3 "${REPO_ROOT}/scripts/recover_control_plane_release_baseline.py" transact \
      --base-sha "${base_sha}" --target-sha "${target_sha}" \
      --evidence "${signal_evidence}" --timeout-seconds 30
) &
signal_test_pid=$!
for _ in 1 2 3 4 5 6 7 8 9 10; do
  [[ -f "${signal_test_dir}/started" ]] && break
  sleep 0.1
done
[[ -f "${signal_test_dir}/started" ]] || fail "signal regression did not start its detached child"
kill -TERM "${signal_test_pid}"
if wait "${signal_test_pid}"; then
  fail "terminated recovery helper unexpectedly succeeded"
fi
[[ -f "${signal_evidence}" ]] || fail "terminated recovery helper did not preserve failure evidence"
sleep 5
[[ ! -e "${signal_test_dir}/late-marker" ]] || fail "terminated recovery helper left a late detached child"
python3 - "${signal_evidence}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    evidence = json.load(handle)
assert evidence["outcome"] == "failed"
assert "termination signal" in evidence["error"]
PY

printf '[test_release_policy_recovery_workflow] exact CAS and compensation tests passed\n'
