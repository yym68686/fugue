package platformsafety

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPublicDataPlaneReleaseRequiresPhasedActivationAndSynthetic(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/release-public-data-plane.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	if workflow.On.WorkflowDispatch == nil {
		t.Fatal("public release must remain explicit workflow_dispatch")
	}
	inputs := workflow.On.WorkflowDispatch.Inputs
	for _, name := range []string{"confirm_phased_activation", "responses_synthetic_url", "responses_synthetic_model"} {
		if _, ok := inputs[name]; !ok {
			t.Fatalf("public release missing activation input %q", name)
		}
	}
	var confirm releaseWorkflowDispatchInput
	confirmNode := inputs["confirm_phased_activation"]
	if err := confirmNode.Decode(&confirm); err != nil || !confirm.Required || confirm.Type != "boolean" || confirm.Default != false {
		t.Fatalf("phased activation confirmation drifted: %+v err=%v", confirm, err)
	}
	job := workflow.Jobs["release"]
	if job.Environment != "production" || len(job.Steps) == 0 {
		t.Fatalf("public release job boundary drifted: %+v", job)
	}
	step := job.Steps[len(job.Steps)-1]
	wantEnv := map[string]string{
		"FUGUE_EDGE_ACTIVATION_ENABLED":                     "${{ inputs.confirm_phased_activation && 'true' || 'false' }}",
		"FUGUE_EDGE_ACTIVATION_API_URL":                     "${{ vars.FUGUE_API_URL }}",
		"FUGUE_EDGE_ACTIVATION_API_KEY":                     "${{ secrets.FUGUE_BOOTSTRAP_KEY }}",
		"FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME":         "${{ vars.FUGUE_EDGE_ACTIVATION_SIGNING_SECRET_NAME }}",
		"FUGUE_PUBLIC_DATA_PLANE_RESPONSES_SYNTHETIC_URL":   "${{ inputs.responses_synthetic_url }}",
		"FUGUE_PUBLIC_DATA_PLANE_RESPONSES_SYNTHETIC_MODEL": "${{ inputs.responses_synthetic_model }}",
		"FUGUE_PUBLIC_DATA_PLANE_RESPONSES_SYNTHETIC_TOKEN": "${{ secrets.FUGUE_RESPONSES_SYNTHETIC_TOKEN }}",
	}
	for name, want := range wantEnv {
		if step.Env[name] != want {
			t.Fatalf("public release env %s=%q want %q", name, step.Env[name], want)
		}
	}
}

func TestEdgeActivationReleaseOrderingIsFailClosed(t *testing.T) {
	data, err := os.ReadFile("../../scripts/release_fugue_public_data_plane.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	runStart := strings.Index(text, "run_bluegreen_release()")
	if runStart < 0 {
		t.Fatal("blue-green release function is missing")
	}
	runEndOffset := strings.Index(text[runStart:], "\n}\n")
	if runEndOffset < 0 {
		t.Fatal("blue-green release function is unterminated")
	}
	run := text[runStart : runStart+runEndOffset]
	ordered := []string{
		`prepare_edge_activation_candidate_record "${active_slots_json}"`,
		`patch_inactive_worker "${inactive_ds}"`,
		`collect_edge_activation_candidate_material "${active_slots_json}"`,
		`edge_activation_advance "active-epoch-authoritative"`,
		`edge_activation_wait_all_api_ack "${authority_inventory}"`,
		`write_front_active_slot "${front_ds}" "${inactive}"`,
		`run_responses_synthetic`,
		`edge_activation_complete_cutover_and_soak`,
	}
	last := -1
	for _, marker := range ordered {
		index := strings.Index(run, marker)
		if index <= last {
			t.Fatalf("public activation ordering drifted at %q", marker)
		}
		last = index
	}
	completeStart := strings.Index(text, "edge_activation_complete_cutover_and_soak()")
	completeEnd := strings.Index(text[completeStart:], "\n}\n") + completeStart
	complete := text[completeStart:completeEnd]
	for _, marker := range []string{"run_responses_synthetic", "fence_edge_worker_heartbeat", "edge_activation_advance \"active-epoch-enforced\"", "scale_edge_worker_zero_cas"} {
		if !strings.Contains(complete, marker) {
			t.Fatalf("cutover/retire transaction missing %q", marker)
		}
	}
	if strings.Index(complete, "fence_edge_worker_heartbeat") > strings.Index(complete, "scale_edge_worker_zero_cas") {
		t.Fatal("previous slot must be heartbeat-fenced before scale-to-zero")
	}
	if !strings.Contains(text, "live blue-green release requires phased edge activation; legacy argv is forbidden") || !strings.Contains(text, "soak_seconds < 180") {
		t.Fatal("legacy argv or minimum soak fail-closed contract is missing")
	}
	mainStart := strings.Index(text, "main()")
	if mainStart < 0 {
		t.Fatal("public data-plane main function is missing")
	}
	main := text[mainStart:]
	liveGuard := strings.Index(main, `if [[ "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_STRATEGY}" == "blue-green" && "${FUGUE_PUBLIC_DATA_PLANE_RELEASE_DRY_RUN}" != "true" && "${FUGUE_EDGE_ACTIVATION_ENABLED}" != "true" ]]`)
	dispatch := strings.LastIndex(main, "run_bluegreen_release")
	if liveGuard < 0 || dispatch < 0 || liveGuard >= dispatch {
		t.Fatal("live blue-green must reject disabled phased activation before dispatching any release mutation")
	}
	abortStart := strings.Index(text, "abort_bluegreen_release()")
	abortEnd := strings.Index(text[abortStart:], "\n}\n") + abortStart
	abort := text[abortStart:abortEnd]
	if !(strings.Index(abort, "edge_activation_advance rollback") < strings.Index(abort, "edge_activation_wait_all_api_ack") && strings.Index(abort, "edge_activation_wait_all_api_ack") < strings.Index(abort, "rollback_bluegreen_fronts")) {
		t.Fatal("rollback must restore durable route authority and API acknowledgement before front mutation")
	}
}

func TestEdgeAutoRemediatorIsDefaultOffSingleActionAndSharedMutex(t *testing.T) {
	workflow, err := os.ReadFile("../../.github/workflows/remediate-edge-inactive-slot.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(workflow)
	for _, marker := range []string{
		"FUGUE_EDGE_AUTO_REMEDIATION_ENABLED == 'true'",
		"FUGUE_EDGE_AUTO_REMEDIATION_CREDENTIALS_READY == 'true'",
		"FUGUE_EDGE_AUTO_REMEDIATION_INTERVAL_SECONDS: '20'",
		"FUGUE_EDGE_AUTO_REMEDIATION_DEADLINE_SECONDS: '55'",
		"group: fugue-production-cluster-mutation-v1",
		"cancel-in-progress: false",
		"needs.observe.outputs.actionable == 'true'",
	} {
		if !strings.Contains(text, marker) {
			t.Fatalf("auto-remediator workflow contract missing %q", marker)
		}
	}
	if !reflect.DeepEqual(readWorkflowPermissions(t, workflow), map[string]string{"contents": "read"}) {
		t.Fatal("auto-remediator permissions expanded")
	}
	script, err := os.ReadFile("../../scripts/remediate_edge_inactive_slot.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(script)
	ordered := []string{"edge_remediation_action_advance prepared", "fence_and_scale_inactive_target_once", "edge_remediation_action_advance committed", "fresh_route_synthetic", "edge_remediation_action_advance verified"}
	last := -1
	for _, marker := range ordered {
		index := strings.LastIndex(source, marker)
		if index <= last {
			t.Fatalf("auto-remediator action ordering drifted at %q", marker)
		}
		last = index
	}
	for _, forbidden := range []string{"write_front_active_slot", "patch_front", "rollback_bluegreen_fronts", "FUGUE_BUNDLE_SIGNING_KEY"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("auto-remediator gained forbidden capability %q", forbidden)
		}
	}
}

func readWorkflowPermissions(t *testing.T, data []byte) map[string]string {
	t.Helper()
	var workflow struct {
		Permissions map[string]string `yaml:"permissions"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	return workflow.Permissions
}

func TestEdgeActivationWatchdogIsReportOnlyAndDelayed(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/observe-edge-activation-watchdog.yml")
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		On          map[string]yaml.Node `yaml:"on"`
		Permissions map[string]string    `yaml:"permissions"`
		Concurrency struct {
			Group            string `yaml:"group"`
			CancelInProgress bool   `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
		Jobs map[string]releaseWorkflowJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) || workflow.Concurrency.Group != "fugue-edge-activation-watchdog-report-v1" || workflow.Concurrency.CancelInProgress {
		t.Fatalf("watchdog capability boundary drifted: %+v %+v", workflow.Permissions, workflow.Concurrency)
	}
	job := workflow.Jobs["observe"]
	if job.Environment != "" || len(job.Steps) != 3 {
		t.Fatalf("watchdog must be a three-step report-only job: %+v", job)
	}
	for _, forbidden := range []string{"kubectl", "helm", "workflow_dispatch", "contents: write"} {
		if forbidden == "workflow_dispatch" {
			continue
		}
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("watchdog gained mutation capability %q", forbidden)
		}
	}
	script, err := os.ReadFile("../../scripts/observe_edge_activation_watchdog.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(script), "age < 24*3600") || !strings.Contains(string(script), "active-epoch-enforced") || !strings.Contains(string(script), "/v1/responses") {
		t.Fatal("watchdog delayed identity/synthetic contract drifted")
	}
}
