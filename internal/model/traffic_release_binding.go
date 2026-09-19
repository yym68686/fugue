package model

// TrafficReleaseBinding is immutable provenance carried by a group projection.
// It never declares runtime health or replaces release authorization.
type TrafficReleaseBinding struct {
	Schema                  string   `json:"schema"`
	ReleaseSetID            string   `json:"release_set_id"`
	ReleaseSetDigest        string   `json:"release_set_digest"`
	ReleaseSetGeneration    string   `json:"release_set_generation"`
	RouteArtifactID         string   `json:"route_artifact_id"`
	RouteArtifactDigest     string   `json:"route_artifact_digest"`
	RouteArtifactGeneration string   `json:"route_artifact_generation"`
	RouteArtifactSequence   int64    `json:"route_artifact_sequence"`
	ReleaseID               string   `json:"release_id"`
	ReleaseChannel          string   `json:"release_channel"`
	FencingToken            int64    `json:"fencing_token"`
	ScopeKey                string   `json:"scope_key"`
	IntentDigest            string   `json:"intent_digest"`
	PolicyDigest            string   `json:"policy_digest"`
	InputSnapshotDigest     string   `json:"input_snapshot_digest"`
	CompilerVersion         string   `json:"compiler_version"`
	ProjectionDigest        string   `json:"projection_digest"`
	CanaryRuleRef           string   `json:"canary_rule_ref,omitempty"`
	EdgeGroupIDs            []string `json:"edge_group_ids,omitempty"`
}
