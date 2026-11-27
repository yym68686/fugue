package pgimage

import "fugue/internal/model"

// The witness contains identities and hashes, never database credentials or raw
// Kubernetes manifests. It survives a lost PATCH response or controller restart.
type Witness struct {
	Namespace      string            `json:"namespace"`
	Cluster        string            `json:"cluster"`
	ClusterUID     string            `json:"cluster_uid"`
	SpecHash       string            `json:"spec_hash"`
	SystemID       string            `json:"system_id"`
	Pod            string            `json:"pod"`
	PodUID         string            `json:"pod_uid"`
	Node           string            `json:"node"`
	SourceImage    string            `json:"source_image"`
	TargetImage    string            `json:"target_image"`
	ResourcesHash  string            `json:"resources_hash"`
	Volumes        map[string]string `json:"volumes"`
	VerifiedPodUID string            `json:"verified_pod_uid,omitempty"`
	ColdMigration  string            `json:"cold_migration,omitempty"`
	SharedNodeUID  string            `json:"shared_node_uid,omitempty"`
	// Optional for compatibility with image-only witnesses already in flight.
	RestartResources      map[string]any      `json:"restart_resources,omitempty"`
	RestartSpecHash       string              `json:"restart_spec_hash,omitempty"`
	RestartResourceTarget *model.ResourceSpec `json:"restart_resource_target,omitempty"`
	SourceResourcesHash   string              `json:"source_resources_hash,omitempty"`
	SourceResizeID        string              `json:"source_resize_id,omitempty"`
	Drain                 *DrainWitness       `json:"drain,omitempty"`
	DrainRestored         bool                `json:"drain_restored,omitempty"`
	// Nil means the intent predates podPatch compatibility checks.
	BootstrapPodPatch *string `json:"bootstrap_pod_patch,omitempty"`
}

type DrainWitness struct {
	BudgetSeconds   int    `json:"budget_seconds"`
	OriginalSeconds int    `json:"original_seconds"`
	Marker          string `json:"marker"`
	HadPostgresql   bool   `json:"had_postgresql"`
	HadParameters   bool   `json:"had_parameters"`
}

// ValidCompletionProof compares the raw JSON payloads so fields written by
// a different Controller version are not silently discarded.
func ValidCompletionProof(op model.Operation, proofs map[string]map[string]any) bool {
	baseline, intent, verified := proofs[BaselineEvidence], proofs[IntentEvidence], proofs[VerifiedEvidence]
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil || len(baseline) == 0 || Hash(baseline) != Hash(intent) || len(verified) == 0 {
		return false
	}
	uid, _ := verified["verified_pod_uid"].(string)
	sourceUID, _ := intent["pod_uid"].(string)
	clusterUID, _ := intent["cluster_uid"].(string)
	systemID, _ := intent["system_id"].(string)
	if uid == "" || sourceUID == "" || uid == sourceUID || intent["target_image"] != op.DesiredSpec.Postgres.Image || clusterUID == "" || systemID == "" {
		return false
	}
	budget := op.DesiredSpec.Postgres.ImageMigrationDrainBudgetSeconds
	if budget != 0 {
		drain, ok := intent["drain"].(map[string]any)
		if !ValidDrainBudget(budget) || !ok || drain["budget_seconds"] != float64(budget) || verified["drain_restored"] != true {
			return false
		}
	} else if intent["drain"] != nil || verified["drain_restored"] != nil {
		return false
	}
	identity := map[string]any{}
	for k, v := range verified {
		if k != "verified_pod_uid" && k != "drain_restored" {
			identity[k] = v
		}
	}
	if Hash(identity) != Hash(intent) {
		return false
	}
	return true
}
