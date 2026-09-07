package model

import (
	"strings"
	"time"
)

const OperationEvidenceTypeBuildAttempt = "build_attempt"

// BuilderAttemptEvidence is a user-safe projection. Never add raw logs, URLs,
// node names, pod names, credentials or arbitrary exception strings here.
type BuilderAttemptEvidence struct {
	Attempt               int       `json:"attempt"`
	Outcome               string    `json:"outcome"`
	StartedAt             time.Time `json:"started_at"`
	FinishedAt            time.Time `json:"finished_at"`
	Causes                []string  `json:"causes"`
	MissingEvidence       []string  `json:"missing_evidence"`
	MemoryRequestBytes    int64     `json:"memory_request_bytes,omitempty"`
	MemoryLimitBytes      int64     `json:"memory_limit_bytes,omitempty"`
	EphemeralRequestBytes int64     `json:"ephemeral_request_bytes,omitempty"`
	EphemeralLimitBytes   int64     `json:"ephemeral_limit_bytes,omitempty"`
}

// PublicDeploymentCause is an allowlist, not a redactor. Unknown text is never
// echoed: diagnostic output can contain arbitrary credentials and topology.
func PublicDeploymentCause(code string) string {
	switch code {
	case "ephemeral_storage_limit_exceeded":
		return "The build exceeded its temporary storage limit."
	case "builder_disk_pressure":
		return "Build storage capacity was unavailable."
	case "memory_limit_exceeded":
		return "The build exceeded its memory limit."
	case "builder_memory_unavailable":
		return "Available build capacity could not satisfy the memory request."
	case "builder_cpu_unavailable":
		return "Available build capacity could not satisfy the CPU request."
	case "builder_storage_unavailable":
		return "Available build capacity could not satisfy the temporary storage request."
	case "placement_constraints_unsatisfied":
		return "No eligible build capacity satisfied the placement requirements."
	case "image_pull_failed":
		return "A required image could not be retrieved."
	case "build_command_failed":
		return "A build command exited unsuccessfully; inspect the application's build logs."
	case "build_timeout":
		return "The build did not complete before its deadline."
	case "operation_timeout":
		return "The operation did not complete before its deadline."
	case "runtime_memory_limit_exceeded":
		return "The new application version exceeded its memory limit."
	case "runtime_capacity_unavailable":
		return "Available runtime capacity could not satisfy the resource request."
	case "runtime_placement_unsatisfied":
		return "The deployment placement requirements could not be satisfied."
	case "application_startup_failure":
		return "The new application version could not start successfully."
	case "operation_cancelled":
		return "The operation was cancelled."
	case "operation_superseded":
		return "The operation was superseded by a newer request."
	case "evidence_unavailable":
		return "The available evidence does not establish the failure cause."
	default:
		return ""
	}
}

// DeploymentCauseCodes extracts symptoms, never ownership or a speculative root
// cause. The caller must distinguish persisted observations from legacy strings.
func DeploymentCauseCodes(message string) []string {
	text := strings.ToLower(message)
	out := []string{}
	add := func(code string) { out = append(out, code) }
	if strings.Contains(text, "ephemeral local storage usage exceeds") || strings.Contains(text, "ephemeral-storage") && strings.Contains(text, "exceeds") {
		add("ephemeral_storage_limit_exceeded")
	}
	if strings.Contains(text, "diskpressure") || strings.Contains(text, "disk-pressure") || strings.Contains(text, "no space left on device") {
		add("builder_disk_pressure")
	}
	if strings.Contains(text, "oomkilled") {
		add("memory_limit_exceeded")
	}
	if strings.Contains(text, "insufficient memory") {
		add("builder_memory_unavailable")
	}
	if strings.Contains(text, "insufficient cpu") {
		add("builder_cpu_unavailable")
	}
	if strings.Contains(text, "insufficient ephemeral-storage") {
		add("builder_storage_unavailable")
	}
	if strings.Contains(text, "untolerated taint") || strings.Contains(text, "node affinity/selector") || strings.Contains(text, "no eligible builder nodes") {
		add("placement_constraints_unsatisfied")
	}
	if strings.Contains(text, "imagepullbackoff") || strings.Contains(text, "errimagepull") {
		add("image_pull_failed")
	}
	if strings.Contains(text, "exit_code=") && !strings.Contains(text, "exit_code=0") && len(out) == 0 {
		add("build_command_failed")
	}
	if strings.Contains(text, "context deadline exceeded") {
		add("build_timeout")
	}
	return out
}
